# -*- coding: utf-8 -*-
"""深入跨所套利分析:多幣排名 + 手續費敏感度 + 最佳路線。

逐幣做時間格重建(新鮮+共識過濾),算毛價差與「扣不同手續費後」的淨價差,
找出最容易有真價差的幣與路線。
    python arb_deep.py --root data/bbo_focus --minutes 0
"""
import argparse
import glob
import os
import sys
import time
from collections import defaultdict, Counter

import pyarrow.parquet as pq

NS = 1_000_000_000
_L = []
def emit(s=""): _L.append(s)


def analyze_symbol(events, grid_ms, fresh_ms, dev_bps, fee_levels):
    """events: sorted [(ts,ex,bid,ask)]. 回傳 dict 統計。"""
    step = int(grid_ms * 1e6)
    t0, t1 = events[0][0], events[-1][0]
    i = 0
    latest = {}
    grid = t0
    n_grid = 0
    gross_pos = 0
    net_pos = {f: 0 for f in fee_levels}
    max_gross = -1e9
    max_net = -1e9
    sum_good = 0
    routes = Counter()
    while grid <= t1:
        while i < len(events) and events[i][0] <= grid:
            _, ex, b, k = events[i]; latest[ex] = (events[i][0], b, k); i += 1
        fresh = {ex: (b, k) for ex, (ts, b, k) in latest.items() if (grid - ts) / 1e6 <= fresh_ms}
        if len(fresh) >= 2:
            mids = sorted((b + k) / 2 for b, k in fresh.values())
            cons = mids[len(mids) // 2]
            good = {ex: (b, k) for ex, (b, k) in fresh.items()
                    if abs(1e4 * ((b + k) / 2 - cons) / cons) <= dev_bps}
            if len(good) >= 2:
                n_grid += 1; sum_good += len(good)
                bbex = max(good, key=lambda e: good[e][0]); baex = min(good, key=lambda e: good[e][1])
                bb, ba = good[bbex][0], good[baex][1]
                gross = 1e4 * (bb - ba) / ba
                if gross > 0: gross_pos += 1
                max_gross = max(max_gross, gross)
                for f in fee_levels:
                    if gross - 2 * f > 0:
                        net_pos[f] += 1
                        if f == max(fee_levels):
                            routes[f"買{baex}->賣{bbex}"] += 1
                max_net = max(max_net, gross - 2 * max(fee_levels))
        grid += step
    if n_grid == 0:
        return None
    return {"n_grid": n_grid, "gross_pos_pct": 100 * gross_pos / n_grid,
            "net_pos_pct": {f: 100 * net_pos[f] / n_grid for f in fee_levels},
            "max_gross": max_gross, "max_net": max_net,
            "avg_good": sum_good / n_grid, "routes": routes}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.path.join("data", "bbo_focus"))
    ap.add_argument("--minutes", type=float, default=0)
    ap.add_argument("--grid-ms", type=float, default=1000)
    ap.add_argument("--fresh-ms", type=float, default=2000)
    ap.add_argument("--dev-bps", type=float, default=8)
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--out-file", default="arb_deep_report.txt")
    a = ap.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    fee_levels = [0.0, 2.0, 5.0, 7.5]

    files = glob.glob(os.path.join(a.root, "**", "*.parquet"), recursive=True)
    if a.minutes:
        cutoff = time.time() - a.minutes * 60
        files = [f for f in files if os.path.getmtime(f) >= cutoff]
    if not files:
        print(f"找不到資料:{a.root}"); return
    print(f"讀 {len(files):,} 個檔...", flush=True)

    by_sym = defaultdict(list)  # symbol -> [(ts,ex,bid,ask)]
    for j, f in enumerate(files):
        ex = "?"
        for part in f.split(os.sep):
            if part.startswith("exchange="):
                ex = part.split("=", 1)[1]
        try:
            t = pq.read_table(f, columns=["symbol", "bid_px", "ask_px", "ts_local_ns"])
        except Exception:
            continue
        for s, b, k, ts in zip(t.column("symbol").to_pylist(), t.column("bid_px").to_pylist(),
                               t.column("ask_px").to_pylist(), t.column("ts_local_ns").to_pylist()):
            if b and k and b <= k:
                by_sym[s].append((ts, ex, b, k))
        if (j + 1) % 20000 == 0:
            print(f"  ...{j+1:,}/{len(files):,}", flush=True)

    rows = []
    for s, ev in by_sym.items():
        if len(ev) < 30:
            continue
        ev.sort()
        r = analyze_symbol(ev, a.grid_ms, a.fresh_ms, a.dev_bps, fee_levels)
        if r:
            r["symbol"] = s
            rows.append(r)

    span_min = 0
    if by_sym:
        allts = [e[0] for ev in by_sym.values() for e in ev]
        span_min = (max(allts) - min(allts)) / NS / 60

    emit("==================== 深入跨所套利分析 ====================")
    emit(f"資料夾: {a.root}   時間窗: {('最近 '+str(a.minutes)+' 分鐘') if a.minutes else '全部'}   "
         f"資料時長: {span_min:.1f} 分鐘   時間格: {a.grid_ms:.0f}ms")
    emit(f"幣數(有效): {len(rows)}   新鮮門檻 {a.fresh_ms:.0f}ms   離群門檻 {a.dev_bps} 萬分之   "
         f"手續費測試(單邊): {fee_levels} 萬分之")
    emit("")
    emit("【每顆幣淨價差>0 的時間佔比(扣來回手續費)— 依手續費 0 萬分之排名,取最會錯位的前 N】")
    emit(f"{'幣':<14}{'毛>0%':>8}{'淨@0':>8}{'淨@2':>8}{'淨@5':>8}{'淨@7.5':>9}"
         f"{'最大毛bps':>10}{'最大淨bps':>10}{'格數':>8}")
    rows.sort(key=lambda r: (-r["net_pos_pct"][0.0], -r["max_gross"]))
    for r in rows[:a.top]:
        np_ = r["net_pos_pct"]
        emit(f"{r['symbol']:<14}{r['gross_pos_pct']:>8.1f}{np_[0.0]:>8.1f}{np_[2.0]:>8.1f}"
             f"{np_[5.0]:>8.1f}{np_[7.5]:>9.1f}{r['max_gross']:>10.2f}{r['max_net']:>10.2f}{r['n_grid']:>8,}")

    # 整體手續費敏感度
    tg = sum(r["n_grid"] for r in rows) or 1
    emit("\n【整體:不同手續費下,淨價差>0 的時間佔比(全部幣加總)】")
    for f in fee_levels:
        tot = sum(r["net_pos_pct"][f] * r["n_grid"] / 100 for r in rows)
        emit(f"  單邊手續費 {f:>4} 萬分之(來回 {2*f:>4}):  {100*tot/tg:.3f}% 的時間有淨賺")

    # 最佳路線(在最高手續費假設下仍 >0)
    allroutes = Counter()
    for r in rows:
        allroutes.update(r["routes"])
    if allroutes:
        emit(f"\n【扣 {2*max(fee_levels):.0f} 萬分之來回後仍淨賺最多的路線(前 10)】")
        for route, c in allroutes.most_common(10):
            emit(f"  {route:<28} {c:,} 次")
    else:
        emit(f"\n(扣 {2*max(fee_levels):.0f} 萬分之來回後,沒有任何幣有淨賺時間格)")

    emit("\n==================== 報告結束(整段貼給我) ====================")
    report = "\n".join(_L)
    with open(a.out_file, "w", encoding="utf-8") as fh:
        fh.write(report)
    print("\n" + report)
    print(f"\n>>> 已存成 {a.out_file}")


if __name__ == "__main__":
    main()
