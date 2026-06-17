# -*- coding: utf-8 -*-
"""Remote device latency reporter — ZERO dependencies (stdlib only).

Run on any machine (5090, AWS, ...) to feed its per-exchange network RTT into a
running BBO panel for side-by-side device comparison. No aiohttp/pyarrow needed.

    python node_agent.py --panel-url http://<panel-host>:8800 --node-id aws-tokyo
    python node_agent.py --panel-url http://192.168.1.50:8800 --node-id 5090 --interval 15
"""
import argparse
import json
import socket
import time
import urllib.request

HOSTS = {
    "binance": ("stream.binance.com", 9443), "okx": ("ws.okx.com", 8443),
    "bybit": ("stream.bybit.com", 443), "coinbase": ("ws-feed.exchange.coinbase.com", 443),
    "gateio": ("api.gateio.ws", 443), "kucoin": ("ws-api-spot.kucoin.com", 443),
    "htx": ("api.huobi.pro", 443), "bitget": ("ws.bitget.com", 443),
    "mexc": ("wbs-api.mexc.com", 443), "cryptocom": ("stream.crypto.com", 443),
    "bitfinex": ("api-pub.bitfinex.com", 443), "bitstamp": ("ws.bitstamp.net", 443),
    "bitmart": ("ws-manager-compress.bitmart.com", 443), "coinex": ("socket.coinex.com", 443),
    "whitebit": ("api.whitebit.com", 443), "poloniex": ("ws.poloniex.com", 443),
    "upbit": ("api.upbit.com", 443), "bithumb": ("ws-api.bithumb.com", 443),
    "xt": ("stream.xt.com", 443), "ascendex": ("ascendex.com", 443),
    "phemex": ("ws.phemex.com", 443), "digifinex": ("openapi.digifinex.com", 443),
    "lbank": ("api.lbkex.com", 443),
}


def resolve(host):
    try:
        return socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)[0][4][0]
    except Exception:
        return None


def tcp_rtt(ip, port, n, timeout=5.0):
    xs = []
    for _ in range(n):
        t = time.perf_counter()
        try:
            s = socket.create_connection((ip, port), timeout=timeout)
            xs.append((time.perf_counter() - t) * 1000.0)
            s.close()
        except Exception:
            pass
        time.sleep(0.04)
    if not xs:
        return None
    xs.sort()
    p = lambda q: xs[min(len(xs) - 1, int(q * len(xs)))]
    return {"p50": round(p(.5), 1), "p99": round(p(.99), 1), "jitter": round(p(.99) - p(.5), 1), "n": len(xs)}


def measure(samples):
    ips = {ex: resolve(h) for ex, (h, _) in HOSTS.items()}
    out = {}
    for ex, (h, port) in HOSTS.items():
        ip = ips[ex]
        if ip:
            r = tcp_rtt(ip, port, samples)
            if r:
                out[ex] = r
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--panel-url", required=True, help="e.g. http://192.168.1.50:8800")
    ap.add_argument("--node-id", required=True)
    ap.add_argument("--interval", type=float, default=15.0)
    ap.add_argument("--samples", type=int, default=8)
    ap.add_argument("--token", default="", help="panel auth token (if the hub requires one)")
    ap.add_argument("--once", action="store_true")
    a = ap.parse_args()
    url = a.panel_url.rstrip("/") + "/api/ingest"
    headers = {"Content-Type": "application/json"}
    if a.token:
        headers["X-Auth-Token"] = a.token
    started = time.time()
    while True:
        rtt = measure(a.samples)
        body = json.dumps({"node_id": a.node_id, "ts": time.time(),
                           "uptime_s": round(time.time() - started), "rtt": rtt}).encode()
        try:
            req = urllib.request.Request(url, data=body, headers=headers)
            urllib.request.urlopen(req, timeout=10).read()
            best = min(rtt.items(), key=lambda kv: kv[1]["p50"]) if rtt else None
            print(f"[{time.strftime('%H:%M:%S')}] {a.node_id}: reported {len(rtt)} venues"
                  + (f"; fastest {best[0]} {best[1]['p50']}ms" if best else ""))
        except Exception as e:
            print(f"POST failed: {e}")
        if a.once:
            break
        time.sleep(a.interval)


if __name__ == "__main__":
    main()
