# -*- coding: utf-8 -*-
"""Exchange latency benchmark — ZERO dependencies (stdlib only).

Measures, from whatever machine it runs on, the network round-trip to each
exchange's real data-path host:
  - tcp_ms : TCP 3-way handshake time (≈ one pure network RTT, server does ~no work)
  - tls_ms : additional TLS handshake time (the real wss setup cost, ~1-2 more RTT)

DNS is resolved once up front and we connect to the IP, so the numbers are pure
network RTT (not polluted by DNS). Run the SAME script on the laptop, the 5090,
and the AWS box, then compare the tables. Lower median = faster; lower p99 and
p99-p50 (jitter) = more stable. For latency arbitrage, JITTER/p99 matters as much
as the median.

    python latency_bench.py            # all venues, 25 samples
    python latency_bench.py 50 binance,okx,bybit
"""
import socket
import ssl
import statistics
import sys
import time

# data-path host:port actually used by the collector's WebSocket connections
HOSTS = {
    "binance":   ("stream.binance.com", 9443),
    "okx":       ("ws.okx.com", 8443),
    "bybit":     ("stream.bybit.com", 443),
    "coinbase":  ("ws-feed.exchange.coinbase.com", 443),
    "gateio":    ("api.gateio.ws", 443),
    "kucoin":    ("ws-api-spot.kucoin.com", 443),
    "htx":       ("api.huobi.pro", 443),
    "bitget":    ("ws.bitget.com", 443),
    "mexc":      ("wbs-api.mexc.com", 443),
    "cryptocom": ("stream.crypto.com", 443),
    "bitfinex":  ("api-pub.bitfinex.com", 443),
    "bitstamp":  ("ws.bitstamp.net", 443),
    "bitmart":   ("ws-manager-compress.bitmart.com", 443),
    "coinex":    ("socket.coinex.com", 443),
    "whitebit":  ("api.whitebit.com", 443),
    "poloniex":  ("ws.poloniex.com", 443),
    "upbit":     ("api.upbit.com", 443),
    "bithumb":   ("ws-api.bithumb.com", 443),
    "xt":        ("stream.xt.com", 443),
    "ascendex":  ("ascendex.com", 443),
    "phemex":    ("ws.phemex.com", 443),
    "digifinex": ("openapi.digifinex.com", 443),
    "lbank":     ("api.lbkex.com", 443),
}

# the venues that actually matter for arbitrage (deepest real liquidity)
TIER1 = {"binance", "okx", "bybit", "coinbase", "kucoin", "gateio", "htx", "bitget", "mexc", "bitstamp"}

_ctx = ssl.create_default_context()
_ctx.check_hostname = False
_ctx.verify_mode = ssl.CERT_NONE  # we time the handshake, not validate certs


def resolve(host):
    try:
        return socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)[0][4][0]
    except Exception:
        return None


def sample(ip, host, port, timeout=5.0):
    try:
        t0 = time.perf_counter()
        s = socket.create_connection((ip, port), timeout=timeout)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        t1 = time.perf_counter()
        ss = _ctx.wrap_socket(s, server_hostname=host)
        t2 = time.perf_counter()
        ss.close()
        return (t1 - t0) * 1000.0, (t2 - t1) * 1000.0
    except Exception:
        return None, None


def pctl(xs, q):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    i = min(len(xs) - 1, int(q * len(xs)))
    return xs[i]


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    names = (sys.argv[2].split(",") if len(sys.argv) > 2 else list(HOSTS))
    print(f"latency_bench  samples={n}  host={socket.gethostname()}  "
          f"time={time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'exchange':<11}{'host':<34}{'tcp_p50':>8}{'tcp_p99':>8}{'jitter':>8}{'tls_p50':>8}{'ok':>5}")
    print("-" * 90)
    rows = []
    for name in names:
        host, port = HOSTS[name]
        ip = resolve(host)
        if not ip:
            print(f"{name:<11}{host:<34}{'DNS-FAIL':>8}")
            continue
        tcp, tls = [], []
        for _ in range(n):
            a, b = sample(ip, host, port)
            if a is not None:
                tcp.append(a); tls.append(b)
            time.sleep(0.08)
        if not tcp:
            print(f"{name:<11}{host:<34}{'UNREACH':>8}")
            continue
        p50, p99 = pctl(tcp, 0.50), pctl(tcp, 0.99)
        jit = p99 - p50
        tlsp50 = pctl(tls, 0.50)
        rows.append((name, host, p50, p99, jit, tlsp50, len(tcp)))
    rows.sort(key=lambda r: r[2])
    for name, host, p50, p99, jit, tlsp50, ok in rows:
        star = "*" if name in TIER1 else " "
        print(f"{name+star:<11}{host:<34}{p50:>8.1f}{p99:>8.1f}{jit:>8.1f}{tlsp50:>8.1f}{ok:>5}")
    print("-" * 90)
    t1 = [r[2] for r in rows if r[0] in TIER1]
    if t1:
        print(f"TIER-1 venues (*): tcp_p50 best={min(t1):.1f}ms  worst={max(t1):.1f}ms  "
              f"mean={statistics.mean(t1):.1f}ms   (lower & flatter = better for latency arb)")
    print("tcp_p50 = network round-trip; jitter = p99-p50 (stability). Compare across machines.")


if __name__ == "__main__":
    main()
