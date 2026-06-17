# -*- coding: utf-8 -*-
"""Orchestrator: run the BBO collector fleet.

    python -m bbo_collector.main --exchanges all --duration 30 --max-symbols 25
    python -m bbo_collector.main --exchanges binance,okx,kucoin --out data/bbo
"""
from __future__ import annotations
import argparse
import asyncio
import time

import aiohttp

from .collector import Metrics, run_exchange
from .registry import REGISTRY
from .writer import ParquetWriter


async def _stats(metrics: dict, writer: ParquetWriter, stop: asyncio.Event, every: float):
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=every)
        except asyncio.TimeoutError:
            pass
        now = time.monotonic()
        live = sum(1 for m in metrics.values() if now - m.last_record_mono < 5 and m.last_record_mono)
        recs = sum(m.records for m in metrics.values())
        drops = sum(m.drops for m in metrics.values())
        print(f"[{time.strftime('%H:%M:%S')}] live_ex={live}/{len(metrics)} "
              f"records={recs:,} drops={drops:,} parquet_rows={writer.rows_written:,} "
              f"files={writer.files_written}")


async def run(exchanges, duration, max_symbols, out, stats_every, queue_max):
    adapters = [REGISTRY[name] for name in exchanges]
    queue: asyncio.Queue = asyncio.Queue(maxsize=queue_max)
    stop = asyncio.Event()
    metrics = {a.name: Metrics() for a in adapters}
    writer = ParquetWriter(out)

    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        writer_task = asyncio.create_task(writer.run(queue, stop))
        stats_task = asyncio.create_task(_stats(metrics, writer, stop, stats_every))
        ex_tasks = [asyncio.create_task(
            run_exchange(a, queue, stop, session, metrics[a.name], symbol_cap=max_symbols))
            for a in adapters]

        if duration:
            try:
                await asyncio.wait_for(asyncio.gather(*ex_tasks), timeout=duration)
            except asyncio.TimeoutError:
                pass
        else:
            await asyncio.gather(*ex_tasks)

        stop.set()
        for t in ex_tasks:
            t.cancel()
        await asyncio.gather(writer_task, stats_task, return_exceptions=True)

    _summary(metrics, writer)


def _summary(metrics, writer):
    print("\n==================== SUMMARY ====================")
    print(f"{'exchange':<12}{'symbols':>9}{'msgs':>10}{'records':>11}{'drops':>8}{'reconn':>8}{'err':>6}")
    for name, m in sorted(metrics.items()):
        sym = "ALL" if m.symbols == -1 else m.symbols
        print(f"{name:<12}{str(sym):>9}{m.messages:>10,}{m.records:>11,}{m.drops:>8,}{m.reconnects:>8}{m.errors:>6}")
    print("-" * 64)
    print(f"parquet rows written: {writer.rows_written:,} in {writer.files_written} files")
    print("rows per exchange (parquet):")
    for ex, n in sorted(writer.rows_per_exchange.items()):
        print(f"   {ex:<12} {n:,}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--exchanges", default="all", help="comma list or 'all'")
    ap.add_argument("--duration", type=float, default=0, help="seconds (0 = run forever)")
    ap.add_argument("--max-symbols", type=int, default=0, help="cap symbols per venue (0 = no cap)")
    ap.add_argument("--out", default="data/bbo")
    ap.add_argument("--stats-every", type=float, default=5.0)
    ap.add_argument("--queue-max", type=int, default=500_000)
    a = ap.parse_args()
    names = list(REGISTRY) if a.exchanges == "all" else [x.strip() for x in a.exchanges.split(",")]
    bad = [n for n in names if n not in REGISTRY]
    if bad:
        raise SystemExit(f"unknown exchanges: {bad}. known: {list(REGISTRY)}")
    print(f"exchanges: {names}")
    asyncio.run(run(names, a.duration, a.max_symbols or None, a.out, a.stats_every, a.queue_max))


if __name__ == "__main__":
    main()
