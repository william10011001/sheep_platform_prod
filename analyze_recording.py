# -*- coding: utf-8 -*-
"""把已錄製的 Parquet 資料濃縮成一份「可貼上」的小報告(UTF-8 檔)。

檔案很多時會自動抽樣,幾十秒跑完。面板可繼續開著。
    cd C:\\SheepNode\\bbo
    python analyze_recording.py
產生 bbo_report.txt;用記事本打開、整段複製貼給我即可。
"""
import argparse
import os
import sys
import time
from collections import defaultdict, Counter

import pyarrow.parquet as pq
import pyarrow.compute as pc

NS = 1_000_000_000
_LINES = []


def emit(s=""):
    _LINES.append(s)


def enumerate_files(root):
    """走訪所有 .parquet,順便記每家最新檔(供尾端快照)。邊走邊報進度。"""
    files = []
    latest = {}  # exchange -> (mtime, path)
    print("掃描檔案中...", flush=True)
    for dirpath, _, names in os.walk(root):
        ex = "?"
        for part in dirpath.split(os.sep):
            if part.startswith("exchange="):
                ex = part.split("=", 1)[1]
        for nm in names:
            if not nm.endswith(".parquet"):
                continue
            p = os.path.join(dirpath, nm)
            try:
                mt = os.path.getmtime(p)
            except OSError:
                continue
            files.append((p, ex))
            if ex not in latest or mt > latest[ex][0]:
                latest[ex] = (mt, p)
            if len(files) % 20000 == 0:
                print(f"  ...已找到 {len(files):,} 個檔", flush=True)
    print(f"共 {len(files):,} 個檔。", flush=True)
    return files, latest


def read_cols(p):
    return pq.read_table(p, columns=["symbol", "bid_px", "ask_px", "ts_exchange_ns", "ts_local_ns"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.path.join("data", "bbo"))
    ap.add_argument("--symbol", default="BTC/USDT")
    ap.add_argument("--max-files", type=int, default=4000, help="超過就抽樣這麼多個檔")
    ap.add_argument("--out-file", default="bbo_report.txt")
    a = ap.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    if not os.path.isdir(a.root):
        print(f"找不到資料夾 {a.root}。確認你在 C:\\SheepNode\\bbo 底下。")
        return
    allfiles, latest = enumerate_files(a.root)
    if not allfiles:
        print(f"{a.root} 下沒有 .parquet。")
        return

    total_files = len(allfiles)
    step = max(1, total_files // a.max_files)
    sample = allfiles[::step]
    scale = total_files / len(sample)
    sampled = step > 1
    print(f"讀取 {len(sample):,} 個檔" + (f"(每 {step} 個抽 1 個;統計會等比放大)" if sampled else "(全部)") + "...", flush=True)

    ex_rows = Counter(); ex_syms = defaultdict(set); ex_both = Counter(); ex_crossed = Counter()
    ex_tmin, ex_tmax = {}, {}; ex_lag = defaultdict(list)
    sym_rows = Counter(); sym_ex = defaultdict(set)
    read_rows = total_bytes = bad = 0
    gmin = gmax = None

    for i, (p, ex) in enumerate(sample):
        try:
            t = read_cols(p)
        except Exception:
            bad += 1
            continue
        n = t.num_rows
        if not n:
            continue
        read_rows += n
        total_bytes += os.path.getsize(p)
        ex_rows[ex] += n
        syms = t.column("symbol").to_pylist()
        bid = t.column("bid_px").to_pylist(); ask = t.column("ask_px").to_pylist()
        tex = t.column("ts_exchange_ns").to_pylist(); tlo = t.column("ts_local_ns").to_pylist()
        for s in set(syms):
            ex_syms[ex].add(s); sym_ex[s].add(ex)
        for s in syms:
            sym_rows[s] += 1
        tmn, tmx = min(tlo), max(tlo)
        ex_tmin[ex] = min(ex_tmin.get(ex, tmn), tmn); ex_tmax[ex] = max(ex_tmax.get(ex, tmx), tmx)
        gmin = tmn if gmin is None else min(gmin, tmn); gmax = tmx if gmax is None else max(gmax, tmx)
        for b, k, te, tl in zip(bid, ask, tex, tlo):
            if b is not None and k is not None:
                ex_both[ex] += 1
                if b > k:
                    ex_crossed[ex] += 1
            if te and te > 1_000_000_000_000_000_000 and len(ex_lag[ex]) < 3000:
                lag = (tl - te) / 1e6
                if 0 <= lag < 600000:
                    ex_lag[ex].append(lag)
        if (i + 1) % 500 == 0:
            print(f"  ...已讀 {i+1:,}/{len(sample):,}", flush=True)

    # 尾端快照:讀每家「最新的那個檔」(不受抽樣影響)
    snap = {}
    for ex, (_, p) in latest.items():
        try:
            t = read_cols(p)
        except Exception:
            continue
        ft = t.filter(pc.equal(t.column("symbol"), a.symbol))
        for tl, b, k in zip(ft.column("ts_local_ns").to_pylist(),
                            ft.column("bid_px").to_pylist(), ft.column("ask_px").to_pylist()):
            if b and k and (ex not in snap or tl > snap[ex][0]):
                snap[ex] = (tl, b, k)

    def med(xs):
        return sorted(xs)[len(xs) // 2] if xs else None

    span = (gmax - gmin) / NS if (gmin and gmax) else 0
    est_rows = int(read_rows * scale)
    est_bytes = total_bytes * scale
    emit("==================== BBO 錄製資料報告 ====================")
    emit(f"總檔案數: {total_files:,}" + (f"  (抽樣讀了 {len(sample):,} 個,統計為等比估算)" if sampled else "  (全讀)"))
    emit(f"資料筆數: {'約 ' if sampled else ''}{est_rows:,}   磁碟: {'約 ' if sampled else ''}{est_bytes/1e6:.0f} MB")
    if gmin:
        emit(f"時間範圍: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(gmin/NS))} "
             f"~ {time.strftime('%m-%d %H:%M:%S', time.localtime(gmax/NS))}   共 {span/60:.1f} 分鐘")
    emit(f"幣種總數: {len(sym_ex):,}")

    emit("\n--- 各交易所(每秒筆=真實估算;交叉異常=資料壞掉的筆數,越少越好) ---")
    emit(f"{'交易所':<11}{'估算筆數':>14}{'幣種':>7}{'每秒筆':>10}{'交叉異常':>12}{'中位feedlag(ms)':>16}")
    for ex in sorted(ex_rows, key=lambda e: -ex_rows[e]):
        exspan = (ex_tmax[ex] - ex_tmin[ex]) / NS or 1
        lagm = med(ex_lag[ex])
        emit(f"{ex:<11}{int(ex_rows[ex]*scale):>14,}{len(ex_syms[ex]):>7}{ex_rows[ex]*scale/exspan:>10.0f}"
             f"{int(ex_crossed[ex]*scale):>12,}{(f'{lagm:.0f}' if lagm is not None else '-'):>16}")

    tot_cross, tot_both = sum(ex_crossed.values()), sum(ex_both.values())
    emit(f"\n資料品質: 取樣中有買賣價 {tot_both:,} 筆, 交叉(買>賣)異常 {tot_cross} 筆 "
         f"({100*tot_cross/max(tot_both,1):.4f}%)  -- 越接近 0 越好")

    emit("\n--- 出現在最多交易所的幣(前 20) ---")
    for s, exs in sorted(sym_ex.items(), key=lambda kv: -len(kv[1]))[:20]:
        emit(f"  {s:<14} 在 {len(exs):>2} 家")
    nex = len(ex_rows)
    for k in (nex, max(nex - 1, 1), max(nex * 3 // 4, 1), max(nex // 2, 1)):
        cnt = sum(1 for exs in sym_ex.values() if len(exs) >= k)
        emit(f"  出現在 >= {k}/{nex} 家的幣: {cnt} 種")

    emit(f"\n--- {a.symbol} 尾端跨所快照({len(snap)} 家) ---")
    if snap:
        mids = sorted((b + k) / 2 for _, b, k in snap.values())
        cons = mids[len(mids) // 2]
        emit(f"共識中間價 約 {cons:.2f}")
        for ex in sorted(snap, key=lambda e: -(snap[e][1])):
            tl, b, k = snap[ex]
            mid = (b + k) / 2
            emit(f"  {ex:<11} 買 {b:>12.2f}  賣 {k:>12.2f}  "
                 f"偏離 {1e4*(mid-cons)/cons:>7.2f} 萬分之  距尾端 {(gmax-tl)/1e6 if gmax else 0:>8.0f} ms")
    emit("\n==================== 報告結束(整段貼給我即可) ====================")

    report = "\n".join(_LINES)
    with open(a.out_file, "w", encoding="utf-8") as fh:
        fh.write(report)
    print("\n" + report)
    print(f"\n>>> 已存成 {a.out_file} (UTF-8)。用記事本打開、整段複製貼給我即可。")


if __name__ == "__main__":
    main()
