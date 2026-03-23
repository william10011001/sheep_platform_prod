import importlib.util
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


import sheep_worker_client


def _reset_db_module():
    sys.modules.pop("sheep_platform_db", None)


def _load_live_trader_module():
    script_path = ROOT / "實盤程式" / "實盤因子池下單程式.py"
    spec = importlib.util.spec_from_file_location("sheep_live_trader_test", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_worker_client_normalizes_legacy_public_base_urls():
    assert sheep_worker_client.normalize_api_base_url("https://sheep123.com/api") == "https://sheep123.com/sheep123"
    assert sheep_worker_client.normalize_api_base_url("https://sheep123.com/api/") == "https://sheep123.com/sheep123"
    assert sheep_worker_client.normalize_api_base_url("https://sheep123.com") == "https://sheep123.com/sheep123"
    assert sheep_worker_client.normalize_api_base_url("http://127.0.0.1:8000") == "http://127.0.0.1:8000"


def test_factor_pool_updater_pauses_cleanly_without_credentials(monkeypatch):
    monkeypatch.delenv("SHEEP_FACTOR_POOL_USER", raising=False)
    monkeypatch.delenv("SHEEP_FACTOR_POOL_PASS", raising=False)

    module = _load_live_trader_module()
    logs = []
    module.log = logs.append
    module.bt = object()

    def _unexpected_run(**kwargs):
        raise AssertionError("run_holy_grail_build should not be called without factor-pool credentials")

    module.run_holy_grail_build = _unexpected_run

    class _DummyVar:
        def __init__(self, value):
            self._value = value

        def get(self):
            return self._value

    class _DummyText:
        def get(self, *_args):
            return '[{"family":"TEMA_Cross","symbol":"ETHUSDT","interval":"30m"}]'

    class _DummyUI:
        global_stake_pct_var = _DummyVar(95.0)
        multi_json_text = _DummyText()

    updater = module.FactorPoolUpdater(_DummyUI())
    updater._build_holy_grail()

    assert any("未設定因子池帳密" in msg for msg in logs)
    assert not any("本輪更新失敗" in msg for msg in logs)


def test_live_trader_accepts_wrapped_multi_strategy_json():
    module = _load_live_trader_module()

    cfg, active_pairs = module.normalize_multi_strategy_entries(
        {
            "schema_version": 1,
            "strategies": [
                {
                    "strategy_key": "tema-long",
                    "family": "TEMA_Cross",
                    "symbol": "ETHUSDT",
                    "direction": "long",
                    "interval": "30m",
                    "family_params": {"fast_len": 12, "slow_len": 55},
                    "tp_pct": 1.2,
                    "sl_pct": 0.8,
                    "max_hold_bars": 36,
                    "stake_pct": 25.0,
                    "enabled": True,
                },
                {
                    "strategy_key": "tema-short",
                    "family": "TEMA_Cross",
                    "symbol": "BTCUSDT",
                    "direction": "short",
                    "interval": "1h",
                    "family_params": {"fast_len": 9, "slow_len": 44},
                    "tp_pct": 1.0,
                    "sl_pct": 0.7,
                    "max_hold_bars": 48,
                    "stake_pct": 20.0,
                    "enabled": True,
                },
            ],
        }
    )

    rows = list(cfg.values())
    assert [row["direction"] for row in rows] == ["long", "short"]
    assert rows[0]["strategy_key"] == "tema-long"
    assert rows[1]["interval"] == "1h"
    assert active_pairs == [("BTCUSDT", "1h"), ("ETHUSDT", "30m")]


def test_leaderboard_stats_include_time_and_points_fallback(monkeypatch, tmp_path):
    db_path = tmp_path / "leaderboard-regression.sqlite3"
    monkeypatch.setenv("SHEEP_DB_URL", "")
    monkeypatch.setenv("SHEEP_DB_PATH", str(db_path))
    _reset_db_module()
    import sheep_platform_db as db

    db.init_db()
    db.ensure_cycle_rollover()
    cycle = db.get_active_cycle()
    user = db.get_user_by_username("sheep")
    if user is None:
        db.create_user("sheep", "test-hash", role="user")
        user = db.get_user_by_username("sheep")
    assert cycle is not None
    assert user is not None

    pool_id = db.create_factor_pool(
        cycle_id=int(cycle["id"]),
        name="Leaderboard Pool",
        symbol="ETH_USDT",
        timeframe_min=60,
        years=2,
        family="trend",
        grid_spec={"alpha": [1, 2]},
        risk_spec={"max_leverage": 2},
        num_partitions=4,
        seed=11,
        active=True,
    )[0]

    now = db._now_iso()
    conn = db._conn()
    try:
        conn.execute(
            """
            INSERT INTO mining_tasks (
                user_id, pool_id, cycle_id, partition_idx, num_partitions,
                status, progress_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(user["id"]),
                int(pool_id),
                int(cycle["id"]),
                0,
                4,
                "completed",
                json.dumps({"combos_done": 321, "elapsed_s": 7200}),
                now,
                now,
            ),
        )
        strategy_cur = conn.execute(
            """
            INSERT INTO strategies (
                submission_id, user_id, pool_id, params_json, status,
                allocation_pct, note, created_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                int(user["id"]),
                int(pool_id),
                json.dumps({"family": "TEMA_Cross"}),
                "active",
                10.0,
                "leaderboard-fallback-test",
                now,
                "2099-12-31T23:59:59+00:00",
            ),
        )
        conn.commit()
        strategy_id = int(strategy_cur.lastrowid)
    finally:
        conn.close()

    db.set_setting("capital_usdt", 100000.0)
    db.set_setting("payout_rate", 0.2)
    db.create_weekly_check(
        strategy_id=strategy_id,
        week_start_ts=now,
        week_end_ts=now,
        return_pct=12.5,
        max_drawdown_pct=3.0,
        trades=20,
        eligible=True,
    )

    stats = db.get_leaderboard_stats(period_hours=720)

    assert stats["time"], stats
    assert float(stats["time"][0]["total_seconds"]) >= 7200.0
    assert stats["points"], stats
    assert round(float(stats["points"][0]["total_usdt"]), 4) == 250.0
    assert stats["qualified_strategies"], stats
    assert int(stats["qualified_strategies"][0]["active_strategy_count"]) >= 1
