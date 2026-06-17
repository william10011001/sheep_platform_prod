# -*- coding: utf-8 -*-
"""Shared in-memory live state for the panel.

Tapped by every collector connection (hot path: one dict write per update) and
read by the web API. Also holds per-exchange network-RTT probes and reports
pushed by remote device nodes (for cross-device comparison)."""
from __future__ import annotations
import time
from collections import defaultdict

_NS_2001 = 1_000_000_000_000_000_000  # sanity floor for ns timestamps


class LiveState:
    def __init__(self, node_id="local"):
        self.node_id = node_id
        self.started = time.time()
        # (exchange, symbol) -> [bid, ask, bidq, askq, ts_ex_ns, ts_local_ns, x_ms_ewma]
        self.latest: dict[tuple, list] = {}
        self.ex_count = defaultdict(int)       # cumulative updates per exchange
        self._ex_prev = defaultdict(int)
        self.ex_ups = defaultdict(float)       # updates/sec EWMA
        self.ex_lag = defaultdict(float)       # feed lag (ts_local-ts_ex) ms EWMA
        self.rtt: dict[str, dict] = {}         # exchange -> {p50,p99,jitter}
        self.metrics = {}                      # exchange -> Metrics (set by server)
        self.devices: dict[str, dict] = {}     # node_id -> latest report
        self._last_sample = time.monotonic()

    # ---- hot path ----
    def update(self, ex, sym, bid, ask, bidq, askq, ts_ex, ts_local):
        key = (ex, sym)
        prev = self.latest.get(key)
        x = 0.0
        if prev is not None and prev[5]:
            dt = (ts_local - prev[5]) / 1e6
            x = dt if not prev[6] else 0.9 * prev[6] + 0.1 * dt
        self.latest[key] = [bid, ask, bidq, askq, ts_ex, ts_local, x]
        self.ex_count[ex] += 1
        if ts_ex and ts_ex > _NS_2001:
            lag = (ts_local - ts_ex) / 1e6
            if 0 <= lag < 60000:
                e = self.ex_lag[ex]
                self.ex_lag[ex] = lag if e == 0 else 0.9 * e + 0.1 * lag

    # ---- 1 Hz sampler ----
    def sample_rates(self):
        now = time.monotonic()
        dt = max(1e-3, now - self._last_sample)
        self._last_sample = now
        for ex, c in list(self.ex_count.items()):
            rate = (c - self._ex_prev[ex]) / dt
            self._ex_prev[ex] = c
            e = self.ex_ups[ex]
            self.ex_ups[ex] = rate if e == 0 else 0.6 * e + 0.4 * rate

    # ---- read API ----
    def overview(self):
        rows = []
        for ex, m in self.metrics.items():
            age = (time.monotonic() - m.last_record_mono) if m.last_record_mono else 1e9
            rtt = self.rtt.get(ex, {})
            rows.append({
                "exchange": ex,
                "symbols": (m.symbols if m.symbols >= 0 else "ALL"),
                "live": age < 5,
                "age_s": round(age, 1),
                "ups": round(self.ex_ups.get(ex, 0.0), 1),
                "x_ms": round(1000.0 / self.ex_ups[ex], 1) if self.ex_ups.get(ex) else None,
                "feed_lag_ms": round(self.ex_lag.get(ex, 0.0), 1) or None,
                "rtt_p50": rtt.get("p50"), "rtt_p99": rtt.get("p99"), "jitter": rtt.get("jitter"),
                "proc_us_p50": round(m.proc_pctl(0.5), 1), "proc_us_p99": round(m.proc_pctl(0.99), 1),
                "records": m.records, "drops": m.drops, "reconnects": m.reconnects,
            })
        rows.sort(key=lambda r: (not r["live"], -(r["ups"] or 0)))
        return rows

    def totals(self):
        return {
            "records": sum(m.records for m in self.metrics.values()),
            "drops": sum(m.drops for m in self.metrics.values()),
            "ups": round(sum(self.ex_ups.values()), 1),
            "live_ex": sum(1 for m in self.metrics.values()
                           if m.last_record_mono and time.monotonic() - m.last_record_mono < 5),
            "exchanges": len(self.metrics),
            "symbols_tracked": len(self.latest),
        }

    def top_symbols(self, n=80):
        c = defaultdict(int)
        for (ex, sym) in self.latest:
            c[sym] += 1
        return [s for s, _ in sorted(c.items(), key=lambda kv: -kv[1])[:n]]

    def book(self, symbol):
        now_ns = time.time_ns()
        venues = []
        for (ex, sym), v in self.latest.items():
            if sym == symbol and v[0] and v[1]:
                venues.append({
                    "exchange": ex, "bid": v[0], "ask": v[1], "bidq": v[2], "askq": v[3],
                    "age_ms": round((now_ns - v[5]) / 1e6, 1),
                    "x_ms": round(v[6], 2),
                    "feed_lag_ms": round((v[5] - v[4]) / 1e6, 1) if v[4] and v[4] > _NS_2001 else None,
                })
        venues.sort(key=lambda r: r["bid"], reverse=True)
        edge = None
        if len(venues) >= 2:
            best_bid = max(venues, key=lambda r: r["bid"])
            best_ask = min(venues, key=lambda r: r["ask"])
            e = best_bid["bid"] - best_ask["ask"]
            edge = {"best_bid_ex": best_bid["exchange"], "best_bid": best_bid["bid"],
                    "best_ask_ex": best_ask["exchange"], "best_ask": best_ask["ask"],
                    "edge": e, "edge_bps": round(1e4 * e / best_ask["ask"], 2) if best_ask["ask"] else None}
        return {"symbol": symbol, "venues": venues, "edge": edge}

    # ---- multi-device ----
    def self_report(self) -> dict:
        """Standardised report this node sends to a hub (and used for the local
        row in the comparison)."""
        return {"node_id": self.node_id, "ts": time.time(),
                "uptime_s": round(time.time() - self.started),
                "rtt": dict(self.rtt), "totals": self.totals()}

    def ingest_device(self, report: dict):
        nid = report.get("node_id", "unknown")
        report["_received"] = time.time()
        self.devices[nid] = report

    def device_comparison(self):
        devs = dict(self.devices)
        local = self.self_report()
        local["_local"] = True
        local["_received"] = time.time()
        devs[self.node_id] = local
        now = time.time()
        exes = set()
        for d in devs.values():
            exes.update((d.get("rtt") or {}).keys())
        table = []
        for ex in sorted(exes):
            cells, best_dev, best_rtt = {}, None, 1e18
            for nid, d in devs.items():
                r = (d.get("rtt") or {}).get(ex)
                if r and r.get("p50") is not None:
                    cells[nid] = r
                    if r["p50"] < best_rtt:
                        best_rtt, best_dev = r["p50"], nid
            table.append({"exchange": ex, "best_dev": best_dev, "cells": cells})
        nodes = []
        for nid, d in devs.items():
            tot = d.get("totals") or {}
            nodes.append({"node_id": nid, "local": d.get("_local", False),
                          "stale": (now - d.get("_received", 0)) > 30,
                          "uptime_s": d.get("uptime_s"),
                          "ups": tot.get("ups"), "live_ex": tot.get("live_ex"),
                          "exchanges": tot.get("exchanges"), "records": tot.get("records"),
                          "drops": tot.get("drops")})
        nodes.sort(key=lambda n: (not n["local"], n["node_id"]))
        return {"nodes": nodes, "table": table}
