# -*- coding: utf-8 -*-
"""Canonical best-bid/offer (BBO) record + Arrow schema.

One record = one top-of-book snapshot for one (exchange, symbol) at the instant
the exchange pushed it. We keep TWO timestamps:
  ts_exchange_ns : the venue's own event time (nullable; not every venue sends one)
  ts_local_ns    : when our process received the frame (always set)
Prices/sizes are float64 for v1. NOTE: for strict financial correctness you may
later switch to integer-scaled or decimal; float64 holds ~15-16 significant
digits which is fine for recording/analytics but can round the last digit of
some 18-decimal token prices.
"""
from __future__ import annotations
from dataclasses import dataclass
import pyarrow as pa

BBO_SCHEMA = pa.schema([
    ("exchange",        pa.dictionary(pa.int32(), pa.string())),  # cheap repeated string
    ("symbol",          pa.string()),   # canonical "BASE/QUOTE", e.g. "BTC/USDT"
    ("raw_symbol",      pa.string()),   # the venue's own symbol string
    ("bid_px",          pa.float64()),
    ("bid_qty",         pa.float64()),
    ("ask_px",          pa.float64()),
    ("ask_qty",         pa.float64()),
    ("ts_exchange_ns",  pa.int64()),    # nullable
    ("ts_local_ns",     pa.int64()),
    ("seq",             pa.int64()),    # nullable; venue sequence/update id
])

COLUMNS = [f.name for f in BBO_SCHEMA]


@dataclass(slots=True)
class BBO:
    exchange: str
    symbol: str
    raw_symbol: str
    bid_px: float | None
    bid_qty: float | None
    ask_px: float | None
    ask_qty: float | None
    ts_exchange_ns: int | None
    ts_local_ns: int
    seq: int | None = None

    def as_tuple(self):
        return (self.exchange, self.symbol, self.raw_symbol, self.bid_px, self.bid_qty,
                self.ask_px, self.ask_qty, self.ts_exchange_ns, self.ts_local_ns, self.seq)
