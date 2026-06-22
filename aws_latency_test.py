import socket
import ssl
import sys
import time
import urllib.request

TCP_SAMPLES = 20
READ_SAMPLES = 6
TCP_TIMEOUT = 5.0
READ_TIMEOUT = 8.0
UA = "Mozilla/5.0 (latency-test)"

VENUES = [
    ("binance", "stream.binance.com", 9443, "https://api.binance.com/api/v3/ticker/bookTicker?symbol=BTCUSDT"),
    ("okx", "ws.okx.com", 8443, "https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT"),
    ("bybit", "stream.bybit.com", 443, "https://api.bybit.com/v5/market/tickers?category=spot&symbol=BTCUSDT"),
    ("coinbase", "ws-feed.exchange.coinbase.com", 443, "https://api.exchange.coinbase.com/products/BTC-USD/ticker"),
    ("gateio", "api.gateio.ws", 443, "https://api.gateio.ws/api/v4/spot/tickers?currency_pair=BTC_USDT"),
    ("kucoin", "ws-api-spot.kucoin.com", 443, "https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=BTC-USDT"),
    ("htx", "api.huobi.pro", 443, "https://api.huobi.pro/market/detail/merged?symbol=btcusdt"),
    ("bitget", "ws.bitget.com", 443, "https://api.bitget.com/api/v2/spot/market/tickers?symbol=BTCUSDT"),
    ("mexc", "wbs-api.mexc.com", 443, "https://api.mexc.com/api/v3/ticker/bookTicker?symbol=BTCUSDT"),
    ("cryptocom", "stream.crypto.com", 443, "https://api.crypto.com/exchange/v1/public/get-tickers?instrument_name=BTC_USDT"),
    ("bitfinex", "api-pub.bitfinex.com", 443, "https://api-pub.bitfinex.com/v2/ticker/tBTCUSD"),
    ("bitstamp", "ws.bitstamp.net", 443, "https://www.bitstamp.net/api/v2/ticker/btcusd/"),
    ("bitmart", "ws-manager-compress.bitmart.com", 443, "https://api-cloud.bitmart.com/spot/quotation/v3/ticker?symbol=BTC_USDT"),
    ("coinex", "socket.coinex.com", 443, "https://api.coinex.com/v2/spot/ticker?market=BTCUSDT"),
    ("whitebit", "api.whitebit.com", 443, "https://whitebit.com/api/v1/public/ticker?market=BTC_USDT"),
    ("poloniex", "ws.poloniex.com", 443, "https://api.poloniex.com/markets/BTC_USDT/ticker24h"),
    ("upbit", "api.upbit.com", 443, "https://api.upbit.com/v1/ticker?markets=KRW-BTC"),
    ("bithumb", "ws-api.bithumb.com", 443, "https://api.bithumb.com/public/ticker/BTC_KRW"),
    ("xt", "stream.xt.com", 443, "https://sapi.xt.com/v4/public/ticker/price?symbol=btc_usdt"),
    ("ascendex", "ascendex.com", 443, "https://ascendex.com/api/pro/v1/spot/ticker?symbol=BTC%2FUSDT"),
    ("phemex", "ws.phemex.com", 443, "https://api.phemex.com/md/spot/ticker/24hr?symbol=sBTCUSDT"),
    ("digifinex", "openapi.digifinex.com", 443, "https://openapi.digifinex.com/v3/ticker?symbol=btc_usdt"),
    ("lbank", "api.lbkex.com", 443, "https://api.lbkex.com/v2/supplement/ticker/price.do?symbol=btc_usdt"),
]

TIER1 = {"binance", "okx", "bybit", "coinbase", "kucoin", "gateio", "htx", "bitget", "mexc", "bitstamp"}

_ctx = ssl.create_default_context()
_ctx.check_hostname = False
_ctx.verify_mode = ssl.CERT_NONE


def resolve(host):
    try:
        return socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)[0][4][0]
    except Exception:
        return None


def tcp_rtt(ip, port):
    out = []
    for _ in range(TCP_SAMPLES):
        t = time.perf_counter()
        try:
            s = socket.create_connection((ip, port), timeout=TCP_TIMEOUT)
            out.append((time.perf_counter() - t) * 1000.0)
            try:
                s.close()
            except Exception:
                pass
        except Exception:
            pass
        time.sleep(0.05)
    return out


def read_rtt(url):
    out = []
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    for _ in range(READ_SAMPLES):
        t = time.perf_counter()
        try:
            r = urllib.request.urlopen(req, timeout=READ_TIMEOUT, context=_ctx)
            r.read()
            if r.status == 200:
                out.append((time.perf_counter() - t) * 1000.0)
        except Exception:
            pass
        time.sleep(0.1)
    return out


def pctl(xs, q):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


def main():
    print("latency_test host=%s tcp_samples=%d read_samples=%d time=%s"
          % (socket.gethostname(), TCP_SAMPLES, READ_SAMPLES, time.strftime("%Y-%m-%d %H:%M:%S")))
    print("%-11s%-34s%9s%9s%9s%10s%6s" % ("exchange", "host", "rtt_p50", "rtt_p99", "jitter", "read_p50", "ok"))
    print("-" * 92)
    rows = []
    for name, host, port, url in VENUES:
        ip = resolve(host)
        if not ip:
            print("%-11s%-34s%9s" % (name, host, "DNS_FAIL"))
            continue
        tcp = tcp_rtt(ip, port)
        rd = read_rtt(url)
        if not tcp:
            print("%-11s%-34s%9s" % (name, host, "UNREACH"))
            continue
        p50 = pctl(tcp, 0.50)
        p99 = pctl(tcp, 0.99)
        jit = p99 - p50
        rp = pctl(rd, 0.50) if rd else float("nan")
        rows.append((name, host, p50, p99, jit, rp, len(rd)))
    rows.sort(key=lambda r: r[2])
    for name, host, p50, p99, jit, rp, okn in rows:
        star = "*" if name in TIER1 else " "
        rps = ("%.1f" % rp) if rp == rp else "-"
        print("%-11s%-34s%9.1f%9.1f%9.1f%10s%6d" % (name + star, host, p50, p99, jit, rps, okn))
    print("-" * 92)
    t1 = [r[2] for r in rows if r[0] in TIER1]
    if t1:
        print("TIER1 venues(*) rtt_p50  best=%.1f  worst=%.1f  mean=%.1f ms"
              % (min(t1), max(t1), sum(t1) / len(t1)))
    print("rtt = TCP round-trip to the live data host (network latency, lower=better).")
    print("read = full HTTPS price-read round-trip. jitter = rtt_p99 - rtt_p50 (stability).")
    print("Run the same script on each machine and compare.")


if __name__ == "__main__":
    main()
