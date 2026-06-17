# -*- coding: utf-8 -*-
"""Custom parsers for venues that need binary decoding or per-connection state.

All verified against real captured WS frames (see ws-bbo-spec-discovery run)."""
from __future__ import annotations

from .adapter import to_float
from .symbols import canon_from_parts, split_nosep


# ----------------------- MEXC protobuf (generic wire reader) -----------------------
def _read_varint(b, i):
    shift = res = 0
    while True:
        x = b[i]; i += 1
        res |= (x & 0x7F) << shift
        if not x & 0x80:
            return res, i
        shift += 7


def _parse_pb(b: bytes) -> dict:
    """Minimal protobuf wire decoder -> {field_num: value}. last-wins."""
    out, i, n = {}, 0, len(b)
    while i < n:
        tag, i = _read_varint(b, i)
        fn, wt = tag >> 3, tag & 7
        if wt == 0:
            v, i = _read_varint(b, i)
        elif wt == 2:
            ln, i = _read_varint(b, i)
            v = b[i:i + ln]; i += ln
        elif wt == 5:
            v = b[i:i + 4]; i += 4
        elif wt == 1:
            v = b[i:i + 8]; i += 8
        else:
            break
        out[fn] = v
    return out


def _pb_str(x):
    return x.decode("utf-8", "replace") if isinstance(x, (bytes, bytearray)) else None


def mexc_raw_parse(data: bytes):
    """PushDataV3ApiWrapper: f3=symbol, f315=PublicAggreBookTicker
    {f1 bidPx, f2 bidQty, f3 askPx, f4 askQty, f6 time(ms)}."""
    top = _parse_pb(data)
    sym = _pb_str(top.get(3))
    inner = top.get(315)
    if not sym or not isinstance(inner, (bytes, bytearray)):
        return []
    f = _parse_pb(inner)
    t = f.get(6)
    return [{
        "raw_symbol": sym,
        "bid_px": to_float(_pb_str(f.get(1))),
        "bid_qty": to_float(_pb_str(f.get(2))),
        "ask_px": to_float(_pb_str(f.get(3))),
        "ask_qty": to_float(_pb_str(f.get(4))),
        "ts_exchange_ns": int(t) * 1_000_000 if isinstance(t, int) else None,
        "seq": None,
    }]


def mexc_canon(raw):
    return split_nosep(raw)


# ----------------------- Bitfinex (chanId -> symbol state) -----------------------
def bitfinex_parse(obj, outer=None, state=None):
    if state is None:
        return []
    if isinstance(obj, dict):
        if obj.get("event") == "subscribed":
            state[obj.get("chanId")] = obj.get("symbol")
        return []
    if isinstance(obj, list) and len(obj) >= 2:
        payload = obj[1]
        if not isinstance(payload, list) or len(payload) < 4:
            return []           # "hb" heartbeat or malformed
        sym = state.get(obj[0])
        if not sym:
            return []
        return [{
            "raw_symbol": sym,
            "bid_px": to_float(payload[0]), "bid_qty": to_float(payload[1]),
            "ask_px": to_float(payload[2]), "ask_qty": to_float(payload[3]),
            "ts_exchange_ns": None, "seq": None,
        }]
    return []


def bitfinex_canon(raw):
    s = raw[1:] if raw and raw.startswith("t") else raw
    if not s:
        return None
    if ":" in s:
        b, q = s.split(":", 1)
    elif len(s) >= 6:
        b, q = s[:-3], s[-3:]
    else:
        return None
    return canon_from_parts(b, q)   # norm_asset maps UST -> USDT


# ----------------------- Poloniex (book_lv2 snapshot+delta) -----------------------
def poloniex_parse(obj, outer=None, state=None):
    if state is None or not isinstance(obj, dict) or obj.get("channel") != "book_lv2":
        return []
    action = obj.get("action")
    books = state.setdefault("books", {})
    out = []
    for r in obj.get("data", []):
        sym = r.get("symbol")
        if not sym:
            continue
        bk = books.setdefault(sym, {"bids": {}, "asks": {}})
        if action == "snapshot":
            bk["bids"] = {p: q for p, q in r.get("bids", [])}
            bk["asks"] = {p: q for p, q in r.get("asks", [])}
        else:
            for side in ("bids", "asks"):
                for p, q in r.get(side, []):
                    if to_float(q):
                        bk[side][p] = q
                    else:
                        bk[side].pop(p, None)
        if not bk["bids"] or not bk["asks"]:
            continue
        bb = max(bk["bids"], key=float)
        ba = min(bk["asks"], key=float)
        ts = r.get("ts")
        out.append({
            "raw_symbol": sym,
            "bid_px": to_float(bb), "bid_qty": to_float(bk["bids"][bb]),
            "ask_px": to_float(ba), "ask_qty": to_float(bk["asks"][ba]),
            "ts_exchange_ns": int(ts) * 1_000_000 if ts else None, "seq": None,
        })
    return out


# ----------------------- Phemex (scaled-int spot_market24h) -----------------------
def phemex_parse(obj, outer=None, state=None):
    if not isinstance(obj, dict):
        return []
    r = obj.get("spot_market24h")
    if not isinstance(r, dict):
        return []
    be, ae = r.get("bidEp"), r.get("askEp")
    if be is None and ae is None:
        return []
    return [{
        "raw_symbol": r.get("symbol"),
        "bid_px": be / 1e8 if isinstance(be, (int, float)) else None,
        "bid_qty": None,
        "ask_px": ae / 1e8 if isinstance(ae, (int, float)) else None,
        "ask_qty": None,
        "ts_exchange_ns": None, "seq": None,
    }]


def phemex_canon(raw):
    return split_nosep(raw[1:]) if raw and raw.startswith("s") else split_nosep(raw)


# ----------------------- Upbit / Bithumb (QUOTE-BASE order) -----------------------
def krw_style_canon(raw):
    """Upbit/Bithumb code is QUOTE-BASE, e.g. KRW-BTC -> BTC/KRW, USDT-BTC -> BTC/USDT."""
    if not raw or "-" not in raw:
        return None
    quote, base = raw.split("-", 1)
    return canon_from_parts(base, quote)


# ----------------------- Bitstamp (symbol in channel field) -----------------------
def bitstamp_canon(channel):
    if not channel:
        return None
    return split_nosep(channel.replace("order_book_", "").upper())
