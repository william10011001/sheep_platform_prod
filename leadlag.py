# -*- coding: utf-8 -*-
"""Lead-lag estimator (Method A: lagged cross-correlation) with synthetic self-test.

Per (coin x venue-pair x rolling window): resample each venue's mid to a uniform
grid, build log-returns, then scan lag tau and find the peak cross-correlation.
tau>0 means A leads B by tau ms. Peak height + prominence = confidence.

    python leadlag.py --selftest
    python leadlag.py --file slice/BTC_USDT.parquet --symbol BTC/USDT --latency-file lat5090.json
"""
import argparse
import json
import os
import sys

import numpy as np


def resample_returns(ts_ns, mid, grid_ns, t0, t1):
    grid = np.arange(t0, t1, grid_ns)
    order = np.argsort(ts_ns)
    ts_ns = np.asarray(ts_ns)[order]
    mid = np.asarray(mid, dtype=float)[order]
    idx = np.searchsorted(ts_ns, grid, side="right") - 1
    px = np.full(len(grid), np.nan)
    ok = idx >= 0
    px[ok] = mid[idx[ok]]
    with np.errstate(divide="ignore", invalid="ignore"):
        logpx = np.log(px)
    ret = np.diff(logpx)
    return ret


def lead_lag(retA, retB, max_lag_steps, min_overlap=50):
    n = min(len(retA), len(retB))
    retA = retA[:n]
    retB = retB[:n]
    cs = {}
    best_lag, best_c = 0, -2.0
    for L in range(-max_lag_steps, max_lag_steps + 1):
        if L >= 0:
            a, b = retA[:n - L] if L else retA, retB[L:]
        else:
            k = -L
            a, b = retA[k:], retB[:n - k]
        m = min(len(a), len(b))
        a, b = a[:m], b[:m]
        good = np.isfinite(a) & np.isfinite(b) & ((a != 0) | (b != 0))
        if good.sum() < min_overlap:
            cs[L] = np.nan
            continue
        a, b = a[good], b[good]
        if a.std() == 0 or b.std() == 0:
            cs[L] = np.nan
            continue
        c = float(np.corrcoef(a, b)[0, 1])
        cs[L] = c
        if c > best_c:
            best_c, best_lag = c, L
    others = [v for k, v in cs.items() if k != best_lag and np.isfinite(v)]
    prom = best_c - (float(np.nanmean(others)) if others else 0.0)
    npts = sum(1 for v in cs.values() if np.isfinite(v))
    return best_lag, best_c, prom, npts


def estimate_pair(tsA, midA, tsB, midB, grid_ms, max_lag_ms):
    grid_ns = int(grid_ms * 1e6)
    t0 = int(min(tsA.min(), tsB.min()))
    t1 = int(max(tsA.max(), tsB.max()))
    if t1 - t0 < grid_ns * 100:
        return None
    rA = resample_returns(tsA, midA, grid_ns, t0, t1)
    rB = resample_returns(tsB, midB, grid_ns, t0, t1)
    lag, c, prom, npts = lead_lag(rA, rB, int(max_lag_ms / grid_ms))
    return {"lead_ms": lag * grid_ms, "peak_corr": round(c, 3),
            "prominence": round(prom, 3), "lags_tested": npts}


def selftest():
    rng = np.random.default_rng(42)
    dur_ms = 180000
    steps = rng.normal(0, 2e-4, dur_ms)
    logp = np.cumsum(steps)
    price = 100.0 * np.exp(logp)
    delta = 50
    nA, nB = 12000, 12000
    tA = np.sort(rng.choice(np.arange(0, dur_ms - delta), size=nA, replace=False))
    tB = np.sort(rng.choice(np.arange(delta, dur_ms), size=nB, replace=False))
    midA = price[tA]
    midB = price[tB - delta] * (1.0 + rng.normal(0, 1e-5, nB))
    tsA = (tA.astype(np.int64)) * 1_000_000
    tsB = (tB.astype(np.int64)) * 1_000_000
    print("synthetic: A leads B by +%d ms (A=leader, B=laggard)" % delta)
    for grid_ms in (10, 20, 50):
        r = estimate_pair(tsA, midA, tsB, midB, grid_ms, 300)
        ok = abs(r["lead_ms"] - delta) <= 2 * grid_ms
        print("  grid=%3dms -> estimated lead=%+5dms  peak_corr=%.2f  prom=%.2f   %s"
              % (grid_ms, r["lead_ms"], r["peak_corr"], r["prominence"], "OK" if ok else "FAIL"))
    print("PASS: estimator recovers the known lead (within one grid step)." )


def load_symbol(path, symbol, latency):
    import pyarrow.dataset as ds
    import pyarrow.compute as pc
    if os.path.isdir(path):
        d = ds.dataset(path, format="parquet", partitioning="hive")
        t = d.to_table(filter=pc.equal(ds.field("symbol"), symbol),
                       columns=["exchange", "bid_px", "ask_px", "ts_local_ns"])
        ex = t.column("exchange").to_pylist()
    else:
        import pyarrow.parquet as pq
        t = pq.read_table(path)
        if "symbol" in t.column_names:
            t = t.filter(pc.equal(t.column("symbol"), symbol))
        ex = t.column("exchange").to_pylist()
    bid = np.array(t.column("bid_px").to_pylist(), dtype=float)
    ask = np.array(t.column("ask_px").to_pylist(), dtype=float)
    ts = np.array(t.column("ts_local_ns").to_pylist(), dtype=np.int64)
    mid = (bid + ask) / 2.0
    series = {}
    for e in set(ex):
        m = np.array([x == e for x in ex])
        off = int(latency.get(e, 0.0) * 1e6) if latency else 0
        series[e] = (ts[m] - off, mid[m])
    return series


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", help="parquet file or hive dir for one symbol")
    ap.add_argument("--symbol", default="BTC/USDT")
    ap.add_argument("--latency-file", default="", help="json {exchange: one_way_ms} to de-bias timestamps")
    ap.add_argument("--grid-ms", type=float, default=20)
    ap.add_argument("--max-lag-ms", type=float, default=400)
    ap.add_argument("--window-min", type=float, default=15, help="rolling window for stability check (0=whole)")
    ap.add_argument("--min-corr", type=float, default=0.1)
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    if a.selftest:
        selftest()
        return
    if not a.file:
        print("give --file or --selftest")
        return
    latency = json.load(open(a.latency_file, encoding="utf-8")) if a.latency_file else {}
    series = load_symbol(a.file, a.symbol, latency)
    venues = sorted(series, key=lambda e: -len(series[e][0]))
    venues = [v for v in venues if len(series[v][0]) >= 200]
    print("symbol=%s  venues(ticks): %s" % (a.symbol, {v: len(series[v][0]) for v in venues}))
    print("latency-correction: %s" % ("ON" if latency else "OFF (raw receive time)"))
    print("\n%-22s%10s%10s%10s" % ("pair (A,B)  A->B", "lead_ms", "peak_corr", "prom"))
    for i in range(len(venues)):
        for j in range(i + 1, len(venues)):
            A, B = venues[i], venues[j]
            tsA, midA = series[A]
            tsB, midB = series[B]
            r = estimate_pair(tsA, midA, tsB, midB, a.grid_ms, a.max_lag_ms)
            if not r or r["peak_corr"] < a.min_corr:
                continue
            who = "%s->%s" % (A, B) if r["lead_ms"] >= 0 else "%s->%s" % (B, A)
            print("%-22s%10d%10.2f%10.2f" % (who, abs(r["lead_ms"]), r["peak_corr"], r["prominence"]))


if __name__ == "__main__":
    main()
