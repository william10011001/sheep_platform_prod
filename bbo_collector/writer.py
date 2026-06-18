# -*- coding: utf-8 -*-
"""Async Parquet writer.

Drains a shared asyncio.Queue, buffers BBO records into column arrays, and
flushes partitioned zstd-Parquet files on a size OR time trigger. Partition
layout:  <root>/exchange=<EX>/date=<YYYY-MM-DD>/hour=<HH>/part-<epoch_ns>.parquet

One file per flush keeps the write path append-free (no partial-file corruption,
the failure mode that bit the panel journal before). Files are immutable once
closed and can be shipped to object storage by a separate cron.
"""
from __future__ import annotations
import asyncio
import os
import time
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq

from .schema import BBO_SCHEMA, COLUMNS


class ParquetWriter:
    def __init__(self, root: str, flush_rows: int = 1_000_000, flush_secs: float = 30.0,
                 compression: str = "zstd"):
        self.root = root
        self.flush_rows = flush_rows
        self.flush_secs = flush_secs
        self.compression = compression
        # buffer is a list of tuples (column-major built at flush time)
        self._buf: list[tuple] = []
        self._last_flush = time.monotonic()
        # metrics
        self.rows_written = 0
        self.files_written = 0
        self.rows_per_exchange: dict[str, int] = defaultdict(int)
        os.makedirs(root, exist_ok=True)

    def add(self, rec_tuple: tuple):
        self._buf.append(rec_tuple)

    def _should_flush(self) -> bool:
        return (len(self._buf) >= self.flush_rows or
                (self._buf and time.monotonic() - self._last_flush >= self.flush_secs))

    def flush(self):
        if not self._buf:
            self._last_flush = time.monotonic()
            return
        rows, self._buf = self._buf, []
        self._last_flush = time.monotonic()
        # column-major
        cols = list(zip(*rows))
        table = pa.Table.from_arrays(
            [pa.array(cols[i]) for i in range(len(COLUMNS))],
            schema=BBO_SCHEMA,
        )
        # partition by exchange + date + hour (use ts_local of first row's wall clock)
        # group rows by (exchange, date, hour) so a flush spanning the hour boundary
        # still lands in correct partitions.
        self._write_partitioned(table)
        self.rows_written += table.num_rows

    def _write_partitioned(self, table: pa.Table):
        ex_col = table.column("exchange").to_pylist()
        ts_col = table.column("ts_local_ns").to_pylist()
        # bucket indices
        buckets: dict[tuple, list[int]] = defaultdict(list)
        for i, (ex, ts) in enumerate(zip(ex_col, ts_col)):
            tm = time.gmtime(ts / 1e9)
            key = (ex, time.strftime("%Y-%m-%d", tm), time.strftime("%H", tm))
            buckets[key].append(i)
        for (ex, date, hour), idxs in buckets.items():
            # exchange/date/hour are carried by the partition path (hive layout);
            # drop them from the file body so a hive-partitioned read does not hit
            # a duplicate/type-mismatched column.
            sub = table.take(idxs).drop_columns(["exchange"])
            d = os.path.join(self.root, f"exchange={ex}", f"date={date}", f"hour={hour}")
            os.makedirs(d, exist_ok=True)
            fn = os.path.join(d, f"part-{time.time_ns()}-{len(idxs)}.parquet")
            pq.write_table(sub, fn, compression=self.compression)
            self.files_written += 1
            self.rows_per_exchange[ex] += len(idxs)

    async def run(self, queue: "asyncio.Queue", stop: "asyncio.Event"):
        """Drain queue until stop is set and queue is empty."""
        while not (stop.is_set() and queue.empty()):
            try:
                rec = await asyncio.wait_for(queue.get(), timeout=0.5)
                self.add(rec)
                # opportunistically drain without awaiting to batch hard
                while len(self._buf) < self.flush_rows:
                    try:
                        self.add(queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
            except asyncio.TimeoutError:
                pass
            if self._should_flush():
                self.flush()
        self.flush()
