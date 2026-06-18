# -*- coding: utf-8 -*-
"""針對某顆幣,做「真的能不能跨所套利」的時間序分析。

逐時間格(預設每 1 秒)重建各交易所最新買賣價(只算新鮮且在共識帶內的),
取「最高買價 - 最低賣價」當毛價差,再扣來回手續費,算淨價差 > 0 的比例。

在 5090 上跑(建議先用「精選幣過濾」重錄一段乾淨資料,會更快更準):
    cd C:\\SheepNode\\bbo
    python arb_analysis.py --symbol BTC/USDT --minutes 120
產生 arb_report.txt;整段貼給我即可。
"""
import argparse
import glob
import os
import sys
import time
from collections import Counter

import pyarrow.parquet as pq
import pyarrow.compute as pc

NS = 1_000_000_000
_LINES = []


def emit(s=""):
    _LINES.append(s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.path.join("data", "bbo"))
    ap.add_argument("--symbol", default="BTC/USDT")
    ap.add_argument("--minutes", type=float, default=120, help="只分析最近 N 分鐘(0=全部,大資料會很慢)")
    ap.add_argument("--grid-ms", type=float, default=1000, help="每幾毫秒取一格")
    ap.add_argument("--fee-bps", type=float, default=7.5, help="單邊手續費(萬分之);來回扣兩次")
    ap.add_argument("--fresh-ms", type=float, default=2000, help="報價多舊就不算")
    ap.add_argument("--dev-bps", type=float, default=5, help="偏離共識超過就當離群剔除")
    ap.add_argument("--out-file", default="arb_report.txt")
    a = ap.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    files = glob.glob(os.path.join(a.root, "**", "*.parquet"), recursive=True)
    if a.minutes:
        cutoff = time.time() - a.minutes * 60
        files = [f for f in files if os.path.getmtime(f) >= cutoff]
    if not files:
        print(f"找不到資料(root={a.root}, 最近 {a.minutes} 分鐘)。確認路徑或加大 --minutes。")
        return
    print(f"掃描 {len(files):,} 個檔,抓 {a.symbol} ...", flush=True)

    events = []  # (ts_ns, exchange, bid, ask)
    for j, f in enumerate(files):
        ex = "?"
        for part in f.split(os.sep):
            if part.startswith("exchange="):
                ex = part.split("=", 1)[1]
        try:
            t = pq.read_table(f, columns=["symbol", "bid_px", "ask_px", "ts_local_ns"])
        except Exception:
            continue
        ft = t.filter(pc.equal(t.column("symbol"), a.symbol))
        if ft.num_rows == 0:
            continue
        for ts, b, k in zip(ft.column("ts_local_ns").to_pylist(),
                            ft.column("bid_px").to_pylist(), ft.column("ask_px").to_pylist()):
            if b and k and b <= k:        # 跳過交叉壞資料
                events.append((ts, ex, b, k))
        if (j + 1) % 20000 == 0:
            print(f"  ...{j+1:,}/{len(files):,} 檔, 已收 {len(events):,} 筆 {a.symbol}", flush=True)

    if len(events) < 10:
        print(f"{a.symbol} 的資料太少({len(events)} 筆),換顆幣或加大 --minutes。")
        return
    events.sort()
    step = int(a.grid_ms * 1e6)
    t0, t1 = events[0][0], events[-1][0]

    n_grid = n_gross = n_net = 0
    sum_good = 0
    gross_list = []
    max_gross = max_net = -1e9
    best_tick = None
    pairs = Counter()
    i = 0
    latest = {}
    grid = t0
    while grid <= t1:
        while i < len(events) and events[i][0] <= grid:
            _, ex, b, k = events[i]
            latest[ex] = (events[i][0], b, k)
            i += 1
        fresh = {ex: (b, k) for ex, (ts, b, k) in latest.items() if (grid - ts) / 1e6 <= a.fresh_ms}
        if len(fresh) >= 2:
            mids = sorted((b + k) / 2 for b, k in fresh.values())
            cons = mids[len(mids) // 2]
            good = {ex: (b, k) for ex, (b, k) in fresh.items()
                    if abs(1e4 * ((b + k) / 2 - cons) / cons) <= a.dev_bps}
            if len(good) >= 2:
                n_grid += 1
                sum_good += len(good)
                bbex = max(good, key=lambda e: good[e][0])
                baex = min(good, key=lambda e: good[e][1])
                bb, ba = good[bbex][0], good[baex][1]
                gross = 1e4 * (bb - ba) / ba
                net = gross - 2 * a.fee_bps
                gross_list.append(gross)
                if gross > 0:
                    n_gross += 1
                if net > 0:
                    n_net += 1
                    pairs[f"買{baex}->賣{bbex}"] += 1
                if gross > max_gross:
                    max_gross = gross
                    best_tick = (grid, baex, ba, bbex, bb)
                max_net = max(max_net, net)
        grid += step

    def pct(xs, q):
        return sorted(xs)[min(len(xs) - 1, int(q * len(xs)))] if xs else 0.0

    emit("==================== 跨所套利分析 ====================")
    emit(f"幣: {a.symbol}   時間窗: 最近 {a.minutes:.0f} 分鐘   時間格: {a.grid_ms:.0f} ms")
    emit(f"手續費假設: 單邊 {a.fee_bps} 萬分之 (來回 {2*a.fee_bps} 萬分之)   "
         f"新鮮門檻 {a.fresh_ms:.0f}ms   離群門檻 {a.dev_bps} 萬分之")
    emit(f"原始 {a.symbol} 筆數: {len(events):,}   有效時間格: {n_grid:,}   平均每格 {sum_good/max(n_grid,1):.1f} 家在共識內")
    emit("")
    emit(f"毛價差 > 0 的時間佔比: {100*n_gross/max(n_grid,1):.2f}%  (代表有跨所價格錯位)")
    emit(f"毛價差中位: {pct(gross_list,0.5):.2f} 萬分之   p90: {pct(gross_list,0.9):.2f}   "
         f"p99: {pct(gross_list,0.99):.2f}   最大: {max_gross:.2f} 萬分之")
    emit("")
    emit(f">>> 扣掉來回手續費後,淨價差 > 0 的時間佔比: {100*n_net/max(n_grid,1):.3f}%   "
         f"(共 {n_net:,} 格)")
    emit(f">>> 最大淨價差: {max_net:.2f} 萬分之  (>0 才有賺頭;負代表手續費吃掉價差)")
    if best_tick:
        g, baex, ba, bbex, bb = best_tick
        emit(f"最大毛價差出現在 {time.strftime('%H:%M:%S', time.localtime(g/NS))}: "
             f"在 {baex} 用 {ba:.2f} 買, 到 {bbex} 用 {bb:.2f} 賣 (毛 {max_gross:.2f} 萬分之)")
    if pairs:
        emit("\n淨賺機會最多的路線(前 8):")
        for route, c in pairs.most_common(8):
            emit(f"  {route:<28} {c:,} 次")
    else:
        emit("\n(在此手續費假設下,沒有任何時間格的淨價差 > 0)")
    emit("\n==================== 報告結束(整段貼給我) ====================")

    report = "\n".join(_LINES)
    with open(a.out_file, "w", encoding="utf-8") as fh:
        fh.write(report)
    print("\n" + report)
    print(f"\n>>> 已存成 {a.out_file} (UTF-8)。")


if __name__ == "__main__":
    main()
