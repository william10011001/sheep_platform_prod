# -*- coding: utf-8 -*-
"""Async per-exchange WebSocket collector.

One asyncio task per connection. Handles: optional auth pre-step, symbol
discovery (REST), sharded subscriptions, app-level + protocol-level keepalive,
gzip/deflate/binary frame decoding, parse -> canonical BBO -> bounded queue,
and reconnect with exponential backoff. The socket read loop NEVER blocks on the
queue: on overflow we drop and count (a recorder must keep draining the socket
or the venue disconnects it)."""
from __future__ import annotations
import asyncio
import gzip
import json
import time
import zlib
from collections import defaultdict, deque

import aiohttp

from .schema import BBO


class Metrics:
    def __init__(self):
        self.messages = 0
        self.records = 0
        self.drops = 0
        self.reconnects = 0
        self.errors = 0
        self.last_record_mono = 0.0
        self.symbols = 0
        self.connections = 0
        # per-message in-process handling time (decode+parse+canon+emit), nanoseconds
        self.proc_ns = deque(maxlen=4096)

    def add_proc(self, ns: int):
        self.proc_ns.append(ns)

    def proc_pctl(self, q: float):
        if not self.proc_ns:
            return 0.0
        xs = sorted(self.proc_ns)
        return xs[min(len(xs) - 1, int(q * len(xs)))] / 1000.0  # -> microseconds


def _inflate(data: bytes) -> bytes:
    # try raw-deflate then zlib-wrapped
    for wbits in (-zlib.MAX_WBITS, zlib.MAX_WBITS, zlib.MAX_WBITS | 32):
        try:
            return zlib.decompress(data, wbits)
        except zlib.error:
            continue
    return data


async def _fetch_json(session: aiohttp.ClientSession, url: str):
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
        return await r.json(content_type=None)


async def discover_symbols(adapter, session) -> list[tuple[str, str]]:
    """Return [(canonical, raw_ws_symbol)]. 'all' mode returns []."""
    if adapter.mode == "all" or not adapter.symbol_rest:
        return []
    j = await _fetch_json(session, adapter.symbol_rest)
    pairs = adapter.symbol_extract(j) if adapter.symbol_extract else []
    # de-dup, keep those that canonicalise
    seen, out = set(), []
    for canonical, raw in pairs:
        if raw and raw not in seen:
            seen.add(raw)
            out.append((canonical, raw))
    return out


class Connection:
    def __init__(self, adapter, raw_symbols, queue, stop, metrics, shard=0, live=None, allow=None):
        self.a = adapter
        self.raw_symbols = raw_symbols       # list of raw ws symbols (per_symbol/list); [] for all
        self.queue = queue
        self.stop = stop
        self.m = metrics
        self.shard = shard
        self.live = live                     # optional LiveState tap (panel)
        self.allow = allow                   # optional (canonical) -> bool symbol allowlist
        self.state = {}                      # per-connection state for stateful adapters

    async def run(self, session):
        backoff = 1.0
        while not self.stop.is_set():
            try:
                url = self.a.ws_url
                if self.a.auth:
                    url = await self.a.auth(session)
                async with session.ws_connect(url, heartbeat=None,
                                              max_msg_size=0,
                                              timeout=aiohttp.ClientTimeout(total=30)) as ws:
                    self.m.connections += 1
                    await self._subscribe(ws)
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    try:
                        await self._recv_loop(ws)
                    finally:
                        ping_task.cancel()
                backoff = 1.0
            except Exception as e:  # noqa
                self.m.errors += 1
                if self.stop.is_set():
                    break
                self.m.reconnects += 1
                await asyncio.sleep(backoff + 0.2 * self.shard)
                backoff = min(backoff * 2, 30.0)

    async def _subscribe(self, ws):
        a = self.a
        if a.url_based or a.subscribe is None:
            return
        if a.mode == "all":
            frames = a.subscribe(None)
        else:
            frames = []
            syms = self.raw_symbols
            for i in range(0, len(syms), a.sub_batch):
                frames.extend(a.subscribe(syms[i:i + a.sub_batch]))
        for fr in frames:
            await ws.send_str(fr)
            await asyncio.sleep(0.2)  # pace subscriptions (venues cap subscribe rate)

    async def _ping_loop(self, ws):
        if not self.a.ping_frame:
            return
        try:
            while True:
                await asyncio.sleep(self.a.ping_interval)
                await ws.send_str(self.a.ping_frame)
        except (asyncio.CancelledError, ConnectionResetError):
            return

    async def _recv_loop(self, ws):
        a = self.a
        async for msg in ws:
            if self.stop.is_set():
                break
            if msg.type == aiohttp.WSMsgType.BINARY and a.raw_parse:
                # binary/protobuf path: bypass json
                self.m.messages += 1
                t = time.perf_counter_ns()
                try:
                    recs = a.raw_parse(msg.data)
                except Exception:  # noqa
                    recs = None
                if recs:
                    self._emit(recs)
                self.m.add_proc(time.perf_counter_ns() - t)
                continue
            t = time.perf_counter_ns()
            if msg.type == aiohttp.WSMsgType.TEXT:
                raw = msg.data
                if a.compression in ("gzip", "deflate"):
                    raw = self._decode_bytes(msg.data.encode("latin-1"))
            elif msg.type == aiohttp.WSMsgType.BINARY:
                raw = self._decode_bytes(msg.data)
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break
            else:
                continue
            self.m.messages += 1
            self._handle(raw, ws)
            self.m.add_proc(time.perf_counter_ns() - t)

    def _decode_bytes(self, data: bytes) -> str:
        a = self.a
        if a.compression == "gzip":
            try:
                data = gzip.decompress(data)
            except OSError:
                data = _inflate(data)
        elif a.compression == "deflate":
            data = _inflate(data)
        if a.decode_text:
            return a.decode_text(data)
        return data.decode("utf-8", "replace")

    def _handle(self, raw: str, ws):
        a = self.a
        try:
            obj = json.loads(raw)
        except (ValueError, TypeError):
            return
        # server-driven ping/heartbeat
        if a.server_ping_reply:
            reply = a.server_ping_reply(obj)
            if reply is not None:
                asyncio.create_task(self._safe_send(ws, reply))
                return
        outer = None
        if a.outer_symbol_from:
            outer = obj.get(a.outer_symbol_from) if isinstance(obj, dict) else None
        try:
            recs = a.parse(obj, outer, self.state)
        except Exception:  # noqa
            return
        if recs:
            self._emit(recs)

    def _emit(self, recs):
        a = self.a
        now_ns = time.time_ns()
        mono = time.monotonic()
        for r in recs:
            rawsym = r.get("raw_symbol")
            if not rawsym:
                continue
            canonical = a.canon(rawsym) if a.canon else None
            if canonical is None:
                continue
            if self.allow is not None and not self.allow(canonical):
                continue
            if r["bid_px"] is None and r["ask_px"] is None:
                continue
            if self.live is not None:
                # LiveState.update signature is (ex, sym, bid, ask, bidq, askq, ts_ex, ts_local)
                self.live.update(a.name, canonical, r["bid_px"], r["ask_px"],
                                 r["bid_qty"], r["ask_qty"], r.get("ts_exchange_ns"), now_ns)
            rec = BBO(
                exchange=a.name, symbol=canonical, raw_symbol=str(rawsym),
                bid_px=r["bid_px"], bid_qty=r["bid_qty"],
                ask_px=r["ask_px"], ask_qty=r["ask_qty"],
                ts_exchange_ns=r.get("ts_exchange_ns"), ts_local_ns=now_ns,
                seq=r.get("seq"),
            ).as_tuple()
            try:
                self.queue.put_nowait(rec)
                self.m.records += 1
                self.m.last_record_mono = mono
            except asyncio.QueueFull:
                self.m.drops += 1

    async def _safe_send(self, ws, text):
        try:
            await ws.send_str(text)
        except Exception:  # noqa
            pass


async def run_exchange(adapter, queue, stop, session, metrics, symbol_cap=None,
                       symbol_filter=None, live=None, allow=None):
    """Discover symbols, shard, and run all connections for one exchange.
    symbol_cap: keep at most N symbols (smoke testing). symbol_filter: keep only
    symbols whose canonical id is in this set. allow: (canonical)->bool allowlist
    (also drops non-allowed symbols before subscribing). live: optional LiveState tap."""
    raw_syms = []
    if adapter.mode != "all":
        try:
            pairs = await discover_symbols(adapter, session)
        except Exception:  # noqa
            metrics.errors += 1
            pairs = []
        if allow is not None:
            pairs = [(c, r) for c, r in pairs if allow(c)]
        if symbol_filter is not None:
            pairs = [(c, r) for c, r in pairs if c in symbol_filter]
        if symbol_cap:
            pairs = pairs[:symbol_cap]
        raw_syms = [raw for _, raw in pairs]
        metrics.symbols = len(raw_syms)
        if not raw_syms:
            return
    else:
        metrics.symbols = -1  # all (unknown count up front)

    # shard
    conns = []
    if adapter.mode == "all":
        conns = [Connection(adapter, [], queue, stop, metrics, 0, live, allow)]
    else:
        cap = adapter.max_symbols_per_conn
        shards = [raw_syms[i:i + cap] for i in range(0, len(raw_syms), cap)] or [[]]
        conns = [Connection(adapter, sh, queue, stop, metrics, i, live, allow) for i, sh in enumerate(shards)]

    await asyncio.gather(*(c.run(session) for c in conns))
