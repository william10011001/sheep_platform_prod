# -*- coding: utf-8 -*-
"""Exchange adapter interface + a data-driven path-based JSON parser factory.

Field paths support dict keys and array indices, e.g. "data.b[0][0]",
"orderbook_units[0].bid_price", "params[0][4]". parse(obj, outer, state):
  outer  -> a symbol pulled from a top-level routing field (KuCoin 'subject',
            Bybit topic) when the record itself lacks the symbol.
  state  -> a per-connection dict for stateful venues (Bitfinex chanId map,
            Poloniex order-book reconstruction). Stateless adapters ignore it.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Optional


def to_float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _tokens(path: str):
    return [t for t in path.replace("[", ".").replace("]", "").split(".") if t != ""]


def get_path(obj, path: Optional[str]):
    """Navigate dict keys and list indices. 'root'/''/None -> obj itself."""
    if path in (None, "", "root"):
        return obj
    node = obj
    for t in _tokens(path):
        if node is None:
            return None
        if t.lstrip("-").isdigit():
            i = int(t)
            if isinstance(node, (list, tuple)) and -len(node) <= i < len(node):
                node = node[i]
            else:
                return None
        elif isinstance(node, dict):
            node = node.get(t)
        else:
            return None
    return node


def records_at(obj, record_path: str):
    node = get_path(obj, (record_path or "root").replace("[]", ""))
    if node is None:
        return []
    return node if isinstance(node, list) else [node]


def json_parser(record_path, f_symbol, f_bid_px, f_bid_qty, f_ask_px, f_ask_qty,
                f_ts="none", ts_scale_to_ns=1e6, outer_symbol=False, px_scale=1.0):
    """Build a stateless parse(obj, outer, state) -> list[dict]."""
    def parse(obj, outer=None, state=None):
        out = []
        for r in records_at(obj, record_path):
            sym = outer if outer_symbol else get_path(r, f_symbol)
            ts = None
            if f_ts and f_ts != "none":
                rt = get_path(r, f_ts)
                fv = to_float(rt)
                ts = int(fv * ts_scale_to_ns) if fv is not None else None
            bpx = to_float(get_path(r, f_bid_px)) if f_bid_px else None
            apx = to_float(get_path(r, f_ask_px)) if f_ask_px else None
            if px_scale != 1.0:
                if bpx is not None:
                    bpx *= px_scale
                if apx is not None:
                    apx *= px_scale
            out.append({
                "raw_symbol": sym,
                "bid_px": bpx,
                "bid_qty": to_float(get_path(r, f_bid_qty)) if f_bid_qty else None,
                "ask_px": apx,
                "ask_qty": to_float(get_path(r, f_ask_qty)) if f_ask_qty else None,
                "ts_exchange_ns": ts,
                "seq": None,
            })
        return out
    return parse


@dataclass
class Adapter:
    name: str
    ws_url: str
    mode: str                        # "all" | "list" | "per_symbol"
    parse: Callable                  # (obj, outer, state) -> list[dict]
    compression: str = "none"        # none | gzip | deflate
    subscribe: Optional[Callable] = None      # (symbols|None) -> list[str frames]
    url_based: bool = False
    symbol_rest: Optional[str] = None
    symbol_extract: Optional[Callable] = None  # (json) -> [(canonical, raw_ws)]
    ping_frame: Optional[str] = None
    ping_interval: float = 20.0
    server_ping_reply: Optional[Callable] = None  # (obj) -> Optional[str]
    auth: Optional[Callable] = None             # async (session) -> ws_url
    decode_text: Optional[Callable] = None      # (bytes) -> str
    raw_parse: Optional[Callable] = None        # (bytes) -> list[dict]  (binary/protobuf)
    outer_symbol_from: Optional[str] = None
    canon: Optional[Callable] = None            # (raw_ws) -> "BASE/QUOTE" | None
    sub_batch: int = 50
    max_symbols_per_conn: int = 10_000
    notes: str = ""
