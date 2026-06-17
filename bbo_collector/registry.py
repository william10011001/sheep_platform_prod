# -*- coding: utf-8 -*-
"""Exchange adapter registry — 23 spot venues, all WS specs live-verified.

Each adapter declares how to subscribe, parse, and canonicalise. The generic
collector engine drives them. Field paths use adapter.get_path syntax.
"""
from __future__ import annotations
import json
import time

import aiohttp

from .adapter import Adapter, json_parser
from .symbols import make_canon, canon_from_parts, split_sep
from . import custom

REGISTRY: dict[str, Adapter] = {}


def reg(a: Adapter):
    REGISTRY[a.name] = a
    return a


def _sub(d):                      # compact json frame
    return json.dumps(d, separators=(",", ":"))


# ============================== Binance ==============================
reg(Adapter(
    name="binance", ws_url="wss://stream.binance.com:9443/ws", mode="per_symbol",
    parse=json_parser("root", "s", "b", "B", "a", "A", "none"),
    subscribe=lambda syms: [_sub({"method": "SUBSCRIBE",
                                  "params": [f"{s}@bookTicker" for s in syms],
                                  "id": int(time.time() * 1000) % 1_000_000})],
    symbol_rest="https://api.binance.com/api/v3/exchangeInfo",
    symbol_extract=lambda j: [(canon_from_parts(s["baseAsset"], s["quoteAsset"]), s["symbol"].lower())
                              for s in j.get("symbols", [])
                              if s.get("status") == "TRADING" and s.get("isSpotTradingAllowed")],
    canon=make_canon("nosep"), sub_batch=300, max_symbols_per_conn=1000,
    notes="bookTicker raw event over /ws; real-time on best bid/ask change; <=1024 streams/conn",
))

# ================================ OKX ================================
reg(Adapter(
    name="okx", ws_url="wss://ws.okx.com:8443/ws/v5/public", mode="per_symbol",
    parse=json_parser("data", "instId", "bidPx", "bidSz", "askPx", "askSz", "ts", 1e6),
    subscribe=lambda syms: [_sub({"op": "subscribe",
                                  "args": [{"channel": "tickers", "instId": s} for s in syms]})],
    symbol_rest="https://www.okx.com/api/v5/public/instruments?instType=SPOT",
    symbol_extract=lambda j: [(make_canon("dash")(x["instId"]), x["instId"])
                              for x in j.get("data", []) if x.get("state") == "live"],
    canon=make_canon("dash"), sub_batch=50, ping_frame="ping", ping_interval=20,
    notes="tickers channel ~100ms; text ping->pong",
))

# =============================== Bybit ===============================
reg(Adapter(
    name="bybit", ws_url="wss://stream.bybit.com/v5/public/spot", mode="per_symbol",
    parse=json_parser("data", "s", "b[0][0]", "b[0][1]", "a[0][0]", "a[0][1]", "none"),
    subscribe=lambda syms: [_sub({"op": "subscribe", "args": [f"orderbook.1.{s}" for s in syms]})],
    symbol_rest="https://api.bybit.com/v5/market/instruments-info?category=spot",
    symbol_extract=lambda j: [(make_canon("nosep")(x["symbol"]), x["symbol"])
                              for x in j.get("result", {}).get("list", [])
                              if x.get("status") == "Trading"],
    canon=make_canon("nosep"), sub_batch=10, ping_frame=_sub({"op": "ping"}), ping_interval=20,
    notes="spot best bid/ask via orderbook.1 (snapshot each tick); tickers has NO bid/ask",
))

# ============================== Coinbase =============================
reg(Adapter(
    name="coinbase", ws_url="wss://ws-feed.exchange.coinbase.com", mode="list",
    parse=json_parser("root", "product_id", "best_bid", "best_bid_size",
                      "best_ask", "best_ask_size", "none"),
    subscribe=lambda syms: [_sub({"type": "subscribe", "product_ids": syms, "channels": ["ticker"]})],
    symbol_rest="https://api.exchange.coinbase.com/products",
    symbol_extract=lambda j: [(canon_from_parts(p["base_currency"], p["quote_currency"]), p["id"])
                              for p in j if p.get("status") == "online" and not p.get("trading_disabled")],
    canon=make_canon("dash"), sub_batch=60,
    notes="ticker channel carries best_bid/best_ask; one connection enumerates all products",
))

# =============================== Gate.io ============================
reg(Adapter(
    name="gateio", ws_url="wss://api.gateio.ws/ws/v4/", mode="per_symbol",
    parse=json_parser("result", "s", "b", "B", "a", "A", "t", 1e6),
    subscribe=lambda syms: [_sub({"time": int(time.time()), "channel": "spot.book_ticker",
                                  "event": "subscribe", "payload": syms})],
    symbol_rest="https://api.gateio.ws/api/v4/spot/currency_pairs",
    symbol_extract=lambda j: [(make_canon("underscore")(p["id"]), p["id"])
                              for p in j if p.get("trade_status") == "tradable"],
    canon=make_canon("underscore"), sub_batch=100,
    notes="spot.book_ticker real-time top-of-book",
))

# =============================== KuCoin =============================
async def _kucoin_auth(session: aiohttp.ClientSession):
    async with session.post("https://api.kucoin.com/api/v1/bullet-public",
                            timeout=aiohttp.ClientTimeout(total=20)) as r:
        j = await r.json(content_type=None)
    d = j["data"]; srv = d["instanceServers"][0]
    return f'{srv["endpoint"]}?token={d["token"]}&connectId=bbo{int(time.time())}'


reg(Adapter(
    name="kucoin", ws_url="", mode="all", auth=_kucoin_auth,
    parse=json_parser("data", "_", "bestBid", "bestBidSize", "bestAsk", "bestAskSize",
                      "time", 1e6, outer_symbol=True),
    subscribe=lambda _: [_sub({"id": int(time.time() * 1000), "type": "subscribe",
                               "topic": "/market/ticker:all", "response": True})],
    canon=make_canon("dash"), outer_symbol_from="subject",
    ping_frame=_sub({"id": "hb", "type": "ping"}), ping_interval=15,
    notes="token via bullet-public; /market/ticker:all BBO ~100ms; symbol in 'subject'",
))

# ============================= HTX (Huobi) ==========================
reg(Adapter(
    name="htx", ws_url="wss://api.huobi.pro/ws", mode="per_symbol", compression="gzip",
    parse=json_parser("tick", "symbol", "bid", "bidSize", "ask", "askSize", "quoteTime", 1e6),
    subscribe=lambda syms: [_sub({"sub": f"market.{s}.bbo", "id": s}) for s in syms],
    symbol_rest="https://api.huobi.pro/v1/common/symbols",
    symbol_extract=lambda j: [(canon_from_parts(s["base-currency"], s["quote-currency"]), s["symbol"])
                              for s in j.get("data", []) if s.get("state") == "online"],
    canon=make_canon("nosep"), sub_batch=1, max_symbols_per_conn=300,
    server_ping_reply=lambda o: _sub({"pong": o["ping"]}) if isinstance(o, dict) and "ping" in o else None,
    notes="gzip frames; market.<sym>.bbo top-of-book; server {ping}->{pong}; market.tickers has NO bid/ask",
))

# =============================== Bitget =============================
reg(Adapter(
    name="bitget", ws_url="wss://ws.bitget.com/v2/ws/public", mode="per_symbol",
    parse=json_parser("data", "instId", "bidPr", "bidSz", "askPr", "askSz", "ts", 1e6),
    subscribe=lambda syms: [_sub({"op": "subscribe",
                                  "args": [{"instType": "SPOT", "channel": "ticker", "instId": s} for s in syms]})],
    symbol_rest="https://api.bitget.com/api/v2/spot/public/symbols",
    symbol_extract=lambda j: [(make_canon("nosep")(x["symbol"]), x["symbol"])
                              for x in j.get("data", []) if x.get("status") == "online"],
    canon=make_canon("nosep"), sub_batch=50, ping_frame="ping", ping_interval=25,
    notes="ticker channel bidPr/askPr; text ping->pong",
))

# ================================ MEXC ==============================
reg(Adapter(
    name="mexc", ws_url="wss://wbs-api.mexc.com/ws", mode="per_symbol", compression="none",
    parse=lambda obj, outer=None, state=None: [],   # text frames are acks only
    raw_parse=custom.mexc_raw_parse,
    subscribe=lambda syms: [_sub({"method": "SUBSCRIPTION",
                                  "params": [f"spot@public.aggre.bookTicker.v3.api.pb@100ms@{s}" for s in syms]})],
    symbol_rest="https://api.mexc.com/api/v3/exchangeInfo",
    symbol_extract=lambda j: [(custom.mexc_canon(s["symbol"]), s["symbol"])
                              for s in j.get("symbols", [])
                              if s.get("isSpotTradingAllowed") or str(s.get("status")) in ("1", "ENABLED", "TRADING")],
    canon=custom.mexc_canon, sub_batch=30, max_symbols_per_conn=30,
    ping_frame=_sub({"method": "PING"}), ping_interval=25,
    notes="protobuf-only aggre.bookTicker.pb@100ms; 30 subs/conn",
))

# ============================= Crypto.com ===========================
def _cryptocom_hb(o):
    if isinstance(o, dict) and o.get("method") == "public/heartbeat":
        return _sub({"id": o.get("id"), "method": "public/respond-heartbeat"})
    return None


reg(Adapter(
    name="cryptocom", ws_url="wss://stream.crypto.com/exchange/v1/market", mode="per_symbol",
    parse=json_parser("result.data", "i", "b", "bs", "k", "ks", "t", 1e6),
    subscribe=lambda syms: [_sub({"id": 1, "method": "subscribe",
                                  "params": {"channels": [f"ticker.{s}" for s in syms]}})],
    symbol_rest="https://api.crypto.com/exchange/v1/public/get-instruments",
    symbol_extract=lambda j: [(make_canon("underscore")(x["symbol"]), x["symbol"])
                              for x in j.get("result", {}).get("data", [])],
    canon=make_canon("underscore"), sub_batch=100, server_ping_reply=_cryptocom_hb,
    notes="ticker channel b/k = bid/ask; must answer public/heartbeat",
))

# ============================== Bitfinex ============================
reg(Adapter(
    name="bitfinex", ws_url="wss://api-pub.bitfinex.com/ws/2", mode="per_symbol",
    parse=custom.bitfinex_parse, canon=custom.bitfinex_canon,
    subscribe=lambda syms: [_sub({"event": "subscribe", "channel": "ticker", "symbol": s}) for s in syms],
    symbol_rest="https://api-pub.bitfinex.com/v2/conf/pub:list:pair:exchange",
    symbol_extract=lambda j: [(custom.bitfinex_canon("t" + c), "t" + c) for c in (j[0] if j else [])],
    sub_batch=1, max_symbols_per_conn=25,
    notes="ticker = full snapshot each update; symbol bound to chanId (stateful); USDT=UST",
))

# ============================== Bitstamp ============================
reg(Adapter(
    name="bitstamp", ws_url="wss://ws.bitstamp.net", mode="per_symbol",
    parse=json_parser("data", "_", "bids[0][0]", "bids[0][1]", "asks[0][0]", "asks[0][1]",
                      "microtimestamp", 1e3, outer_symbol=True),
    subscribe=lambda syms: [_sub({"event": "bts:subscribe", "data": {"channel": f"order_book_{s}"}}) for s in syms],
    symbol_rest="https://www.bitstamp.net/api/v2/trading-pairs-info/",
    symbol_extract=lambda j: [(split_sep(p["name"], "/"), p["url_symbol"])
                              for p in j if p.get("trading") == "Enabled"],
    canon=custom.bitstamp_canon, outer_symbol_from="channel", sub_batch=1, max_symbols_per_conn=200,
    notes="order_book channel snapshot each tick; symbol only in 'channel' field",
))

# ============================== BitMart =============================
reg(Adapter(
    name="bitmart", ws_url="wss://ws-manager-compress.bitmart.com/api?protocol=1.1", mode="per_symbol",
    parse=json_parser("data", "symbol", "bid_px", "bid_sz", "ask_px", "ask_sz", "ms_t", 1e6),
    subscribe=lambda syms: [_sub({"op": "subscribe", "args": [f"spot/ticker:{s}" for s in syms]})],
    symbol_rest="https://api-cloud.bitmart.com/spot/v1/symbols",
    symbol_extract=lambda j: [(make_canon("underscore")(s), s) for s in j.get("data", {}).get("symbols", [])],
    canon=make_canon("underscore"), sub_batch=50, ping_frame="ping", ping_interval=15,
    notes="spot/ticker bid_px/ask_px; text ping->pong",
))

# =============================== CoinEx =============================
def _coinex_hb(o):
    if isinstance(o, dict) and o.get("method") == "server.ping":
        return _sub({"method": "server.pong", "params": {}, "id": o.get("id")})
    return None


reg(Adapter(
    name="coinex", ws_url="wss://socket.coinex.com/v2/spot", mode="list", compression="gzip",
    parse=json_parser("data", "market", "best_bid_price", "best_bid_size",
                      "best_ask_price", "best_ask_size", "updated_at", 1e6),
    subscribe=lambda syms: [_sub({"method": "bbo.subscribe", "params": {"market_list": syms}, "id": 1})],
    symbol_rest="https://api.coinex.com/v2/spot/market",
    symbol_extract=lambda j: [(make_canon("nosep")(x["market"]), x["market"]) for x in j.get("data", [])],
    canon=make_canon("nosep"), sub_batch=500, server_ping_reply=_coinex_hb,
    notes="gzip frames; bbo.update single-market; enumerate market_list",
))

# ============================== WhiteBIT ============================
def _whitebit_canon(r):
    if not r or str(r).endswith("_PERP"):
        return None
    return split_sep(r, "_")


reg(Adapter(
    name="whitebit", ws_url="wss://api.whitebit.com/ws", mode="all",
    parse=json_parser("params", "2", "4", "5", "6", "7", "1", 1e9),
    subscribe=lambda _: [_sub({"id": 1, "method": "bookTicker_subscribe", "params": []})],
    canon=_whitebit_canon,
    notes="bookTicker_update positional array [bidTs,evTs,sym,uid,bidPx,bidQty,askPx,askQty]; all markets incl PERP",
))

# ============================== Poloniex ============================
reg(Adapter(
    name="poloniex", ws_url="wss://ws.poloniex.com/ws/public", mode="per_symbol",
    parse=custom.poloniex_parse, canon=make_canon("underscore"),
    subscribe=lambda syms: [_sub({"event": "subscribe", "channel": ["book_lv2"], "symbols": syms})],
    symbol_rest="https://api.poloniex.com/markets",
    symbol_extract=lambda j: [(make_canon("underscore")(x["symbol"]), x["symbol"])
                              for x in j if x.get("state") == "NORMAL"],
    sub_batch=50, ping_frame=_sub({"event": "ping"}), ping_interval=20,
    notes="book_lv2 snapshot+delta -> top-of-book reconstructed (stateful)",
))

# =============================== Upbit ==============================
reg(Adapter(
    name="upbit", ws_url="wss://api.upbit.com/websocket/v1", mode="list",
    parse=json_parser("root", "code", "orderbook_units[0].bid_price", "orderbook_units[0].bid_size",
                      "orderbook_units[0].ask_price", "orderbook_units[0].ask_size", "timestamp", 1e6),
    subscribe=lambda syms: [json.dumps([{"ticket": "bbo"}, {"type": "orderbook", "codes": syms}, {"format": "DEFAULT"}])],
    symbol_rest="https://api.upbit.com/v1/market/all?is_details=false",
    symbol_extract=lambda j: [(custom.krw_style_canon(x["market"]), x["market"]) for x in j],
    canon=custom.krw_style_canon, sub_batch=100,
    notes="binary JSON frames; QUOTE-BASE codes (KRW-/USDT-); orderbook snapshot each tick",
))

# ============================== Bithumb =============================
reg(Adapter(
    name="bithumb", ws_url="wss://ws-api.bithumb.com/websocket/v1", mode="list",
    parse=json_parser("root", "code", "orderbook_units[0].bid_price", "orderbook_units[0].bid_size",
                      "orderbook_units[0].ask_price", "orderbook_units[0].ask_size", "timestamp", 1e3),
    subscribe=lambda syms: [json.dumps([{"ticket": "bbo"}, {"type": "orderbook", "codes": syms}])],
    symbol_rest="https://api.bithumb.com/v1/market/all?isDetails=false",
    symbol_extract=lambda j: [(custom.krw_style_canon(x["market"]), x["market"]) for x in j],
    canon=custom.krw_style_canon, sub_batch=100,
    notes="Upbit-clone; binary JSON; KRW-only; timestamp in microseconds",
))

# ================================= XT ===============================
reg(Adapter(
    name="xt", ws_url="wss://stream.xt.com/public", mode="per_symbol",
    parse=json_parser("data", "s", "b[0][0]", "b[0][1]", "a[0][0]", "a[0][1]", "none"),  # XT depth 't' is not push time
    subscribe=lambda syms: [_sub({"method": "subscribe", "params": [f"depth@{s},5" for s in syms], "id": "1"})],
    symbol_rest="https://sapi.xt.com/v4/public/symbol",
    symbol_extract=lambda j: [(make_canon("underscore")(s["symbol"]), s["symbol"])
                              for s in j.get("result", {}).get("symbols", [])
                              if s.get("state") in (None, "ONLINE")],
    canon=make_canon("underscore"), sub_batch=20,
    notes="depth@<sym>,5 full snapshot each tick; ticker@ has NO bid/ask",
))

# ============================== AscendEX ============================
def _ascendex_hb(o):
    if isinstance(o, dict) and o.get("m") == "ping":
        return _sub({"op": "pong"})
    return None


reg(Adapter(
    name="ascendex", ws_url="wss://ascendex.com/api/pro/v1/stream", mode="per_symbol",
    parse=json_parser("root", "symbol", "data.bid[0]", "data.bid[1]", "data.ask[0]", "data.ask[1]", "data.ts", 1e6),
    subscribe=lambda syms: [_sub({"op": "sub", "ch": f"bbo:{s}"}) for s in syms],
    symbol_rest="https://ascendex.com/api/pro/v1/spot/ticker",
    symbol_extract=lambda j: [(make_canon("slash")(x["symbol"]), x["symbol"]) for x in j.get("data", [])],
    canon=make_canon("slash"), sub_batch=1, max_symbols_per_conn=200, server_ping_reply=_ascendex_hb,
    notes="bbo channel snapshot each update; symbol BASE/QUOTE; server {m:ping}->{op:pong}",
))

# =============================== Phemex =============================
reg(Adapter(
    name="phemex", ws_url="wss://ws.phemex.com", mode="all",
    parse=custom.phemex_parse, canon=custom.phemex_canon,
    subscribe=lambda _: [_sub({"id": 1, "method": "spot_market24h.subscribe", "params": []})],
    ping_frame=_sub({"id": 2, "method": "server.ping", "params": []}), ping_interval=25,
    notes="spot_market24h all symbols; scaled-int bidEp/askEp /1e8; symbol s-prefixed; NO bid/ask sizes",
))

# ============================= DigiFinex ============================
reg(Adapter(
    name="digifinex", ws_url="wss://openapi.digifinex.com/ws/v1/", mode="all", compression="deflate",
    parse=json_parser("params", "symbol", "best_bid", "best_bid_size", "best_ask", "best_ask_size", "timestamp", 1e6),
    subscribe=lambda _: [_sub({"id": 1, "method": "all_ticker.subscribe", "params": []})],
    canon=make_canon("underscore"),
    notes="zlib-wrapped frames; all_ticker.update params[] per-symbol best_bid/best_ask",
))

# =============================== LBank =============================
def _lbank_hb(o):
    if isinstance(o, dict) and o.get("action") == "ping":
        return _sub({"action": "pong", "pong": o.get("ping")})
    return None


reg(Adapter(
    name="lbank", ws_url="wss://api.lbkex.com/ws/V2/", mode="per_symbol",
    parse=json_parser("root", "pair", "depth.bids[0][0]", "depth.bids[0][1]",
                      "depth.asks[0][0]", "depth.asks[0][1]", "ds", 1e6),
    subscribe=lambda syms: [_sub({"action": "subscribe", "subscribe": "depth", "depth": "10", "pair": s}) for s in syms],
    symbol_rest="https://api.lbkex.com/v2/currencyPairs.do",
    symbol_extract=lambda j: [(make_canon("underscore")(p), p) for p in j.get("data", [])],
    canon=make_canon("underscore"), sub_batch=1, max_symbols_per_conn=200, server_ping_reply=_lbank_hb,
    notes="depth top-10 snapshot each tick; server {action:ping}->{action:pong}",
))
