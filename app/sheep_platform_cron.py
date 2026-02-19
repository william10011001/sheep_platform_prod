import argparse
import hashlib
from datetime import datetime, timedelta, timezone

import backtest_panel2 as bt

import sheep_platform_db as db


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def week_bounds_last_completed(now_utc: datetime) -> dict:
    monday = (now_utc - timedelta(days=now_utc.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    week_end = monday
    week_start = monday - timedelta(days=7)
    return {"week_start_ts": _iso(week_start), "week_end_ts": _iso(week_end)}


def run_weekly_check(week_start_ts: str, week_end_ts: str) -> None:
    week_start = _parse_iso(week_start_ts)
    week_end = _parse_iso(week_end_ts)

    conn = db._conn()
    try:
        capital_usdt = float(db.get_setting(conn, "capital_usdt", 0.0))
        payout_rate = float(db.get_setting(conn, "payout_rate", 0.0))
    finally:
        conn.close()

    strategies = db.list_strategies(status="active", limit=1000)
    for s in strategies:
        st_row = db.get_strategy_with_params(int(s["id"]))
        if not st_row:
            continue
        pool = db.get_pool(int(st_row["pool_id"]))
        params = st_row.get("params_json") or {}
        family = str(params.get("family") or pool["family"])
        family_params = dict(params.get("family_params") or {})
        tp = float(params.get("tp"))
        sl = float(params.get("sl"))
        mh = int(params.get("max_hold"))

        csv_main, _ = bt.ensure_bitmart_data(
            symbol=str(pool["symbol"]),
            main_step_min=int(pool["timeframe_min"]),
            years=int(pool.get("years") or 3),
            auto_sync=True,
            force_full=False,
        )
        df = bt.load_and_validate_csv(csv_main)
        dff = df[(df["ts"] >= week_start) & (df["ts"] < week_end)].copy()
        if len(dff) < 100:
            continue

        res = bt.run_backtest(
            dff,
            family,
            family_params,
            tp,
            sl,
            mh,
            fee_side=float((pool.get("risk_spec") or {}).get("fee_side", 0.0002)),
            slippage=float((pool.get("risk_spec") or {}).get("slippage", 0.0)),
            worst_case=bool((pool.get("risk_spec") or {}).get("worst_case", True)),
            reverse_mode=bool((pool.get("risk_spec") or {}).get("reverse_mode", False)),
        )
        ret = float(res.get("total_return_pct") or 0.0)
        dd = float(res.get("max_drawdown_pct") or 0.0)
        trades = int(res.get("trades") or 0)

        eligible = ret > 0.0
        db.create_weekly_check(
            strategy_id=int(s["id"]),
            week_start_ts=week_start_ts,
            week_end_ts=week_end_ts,
            return_pct=ret,
            max_drawdown_pct=dd,
            trades=trades,
            eligible=eligible,
        )

        if not eligible:
            db.set_strategy_status(int(s["id"]), "disqualified")

        if eligible and capital_usdt > 0.0 and payout_rate > 0.0:
            alloc = float(s.get("allocation_pct") or 0.0) / 100.0
            amount = capital_usdt * (ret / 100.0) * alloc * payout_rate
            if amount > 0.0:
                db.create_payout(strategy_id=int(s["id"]), user_id=int(s["user_id"]), week_start_ts=week_start_ts, amount_usdt=float(amount))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["init_db", "cycle_rollover", "weekly_check", "refresh_data_hashes"])
    parser.add_argument("--week-start", default="")
    parser.add_argument("--week-end", default="")
    args = parser.parse_args()

    if args.command == "init_db":
        db.init_db()
        return

    if args.command == "cycle_rollover":
        db.init_db()
        db.ensure_cycle_rollover()
        return

    if args.command == "weekly_check":
        db.init_db()
        db.ensure_cycle_rollover()
        if args.week_start and args.week_end:
            run_weekly_check(args.week_start, args.week_end)
        else:
            b = week_bounds_last_completed(_utc_now())
            run_weekly_check(b["week_start_ts"], b["week_end_ts"])
        return

    if args.command == "refresh_data_hashes":
        db.init_db()
        db.ensure_cycle_rollover()
        cycle = db.get_active_cycle()
        if not cycle:
            return
        pools = db.list_factor_pools(cycle_id=int(cycle["id"]))
        conn = db._conn()
        try:
            for p in pools:
                try:
                    csv_main, _ = bt.ensure_bitmart_data(
                        symbol=str(p["symbol"]),
                        main_step_min=int(p["timeframe_min"]),
                        years=int(p.get("years") or 3),
                        auto_sync=True,
                        force_full=False,
                    )
                    h = _sha256_file(str(csv_main))
                    key = db.data_hash_setting_key(str(p["symbol"]), int(p["timeframe_min"]), int(p.get("years") or 3))
                    ts_key = db.data_hash_ts_setting_key(str(p["symbol"]), int(p["timeframe_min"]), int(p.get("years") or 3))
                    db.set_setting(conn, key, h)
                    db.set_setting(conn, ts_key, db.utc_now_iso())
                except Exception:
                    continue
            conn.commit()
        finally:
            conn.close()
        return


if __name__ == "__main__":
    main()
