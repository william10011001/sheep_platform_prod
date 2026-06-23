# -*- coding: utf-8 -*-
"""Extract a small, focused slice (a few coins) from the big recorded dataset,
so it can be shipped (via R2) for lead-lag analysis. One compact Parquet per coin.

    python make_slice.py --root data\\bbo --symbols "BTC/USDT,ETH/USDT,SOL/USDT" --out slice
then tar/zip the slice folder and upload to R2.
"""
import argparse
import glob
import os
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc


def safe(sym):
    return sym.replace("/", "_").replace(":", "_")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.path.join("data", "bbo"))
    ap.add_argument("--symbols", required=True, help="comma list, e.g. BTC/USDT,ETH/USDT,SOL/USDT")
    ap.add_argument("--out", default="slice")
    ap.add_argument("--min-age", type=float, default=5.0)
    a = ap.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    want = set(s.strip() for s in a.symbols.split(",") if s.strip())
    os.makedirs(a.out, exist_ok=True)
    files = glob.glob(os.path.join(a.root, "**", "*.parquet"), recursive=True)
    now = time.time()
    files = [f for f in files if now - os.path.getmtime(f) >= a.min_age]
    print("scanning %d files for %s ..." % (len(files), sorted(want)), flush=True)

    cols = {s: {"exchange": [], "bid_px": [], "ask_px": [], "ts_local_ns": []} for s in want}
    sym_set = pa.array(sorted(want))
    bad = 0
    for j, f in enumerate(files):
        ex = "?"
        for part in f.split(os.sep):
            if part.startswith("exchange="):
                ex = part.split("=", 1)[1]
        try:
            t = pq.read_table(f, columns=["symbol", "bid_px", "ask_px", "ts_local_ns"])
        except Exception:
            bad += 1
            continue
        t = t.filter(pc.is_in(t.column("symbol"), value_set=sym_set))
        if t.num_rows == 0:
            continue
        syms = t.column("symbol").to_pylist()
        bid = t.column("bid_px").to_pylist()
        ask = t.column("ask_px").to_pylist()
        ts = t.column("ts_local_ns").to_pylist()
        for s, b, k, tl in zip(syms, bid, ask, ts):
            if b is None or k is None or b > k:
                continue
            c = cols[s]
            c["exchange"].append(ex)
            c["bid_px"].append(b)
            c["ask_px"].append(k)
            c["ts_local_ns"].append(tl)
        if (j + 1) % 20000 == 0:
            print("  ...%d/%d (read fail %d)" % (j + 1, len(files), bad), flush=True)

    print("\nwriting slice -> %s/" % a.out)
    for s in sorted(want):
        c = cols[s]
        n = len(c["ts_local_ns"])
        if n == 0:
            print("  %-14s 0 rows (not found)" % s)
            continue
        tbl = pa.table({
            "exchange": pa.array(c["exchange"]),
            "bid_px": pa.array(c["bid_px"], type=pa.float64()),
            "ask_px": pa.array(c["ask_px"], type=pa.float64()),
            "ts_local_ns": pa.array(c["ts_local_ns"], type=pa.int64()),
        })
        fn = os.path.join(a.out, safe(s) + ".parquet")
        pq.write_table(tbl, fn, compression="zstd")
        nv = len(set(c["exchange"]))
        print("  %-14s %9d rows  %2d venues  -> %s (%.1f KB)"
              % (s, n, nv, fn, os.path.getsize(fn) / 1024))
    print("\nDONE. tar/zip the '%s' folder and upload to R2." % a.out)


if __name__ == "__main__":
    main()
