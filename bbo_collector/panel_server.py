# -*- coding: utf-8 -*-
"""Local monitoring + recording panel.

Runs the collector fleet in-process (taps LiveState on the hot path), records
every tick to Parquet for backtest, probes per-exchange network RTT, and serves
a live dashboard with: per-exchange health/latency, cross-exchange BBO for any
coin (arb edge), and a multi-device comparison fed by remote node agents.

    python -m bbo_collector.panel_server --exchanges all --out data/bbo --port 8800
    python -m bbo_collector.panel_server --exchanges binance,okx,bybit --max-symbols 50
"""
from __future__ import annotations
import argparse
import asyncio
import hmac
import os
import time
from urllib.parse import urlsplit

import aiohttp
from aiohttp import web

from .collector import Metrics, run_exchange
from .live_state import LiveState
from .registry import REGISTRY
from .symbols import load_bases, make_allow
from .writer import ParquetWriter

PROBE_EVERY = 30.0
WS_HOST_OVERRIDE = {"kucoin": ("ws-api-spot.kucoin.com", 443)}


def ws_host(adapter):
    if adapter.name in WS_HOST_OVERRIDE:
        return WS_HOST_OVERRIDE[adapter.name]
    u = urlsplit(adapter.ws_url)
    return u.hostname, (u.port or (443 if u.scheme == "wss" else 80))


async def tcp_rtt(host, port, n=8):
    samples = []
    for _ in range(n):
        t = time.perf_counter()
        try:
            r, w = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5)
            samples.append((time.perf_counter() - t) * 1000.0)
            w.close()
            try:
                await asyncio.wait_for(w.wait_closed(), timeout=2)
            except Exception:  # noqa
                pass
        except Exception:  # noqa
            pass
        await asyncio.sleep(0.04)
    if not samples:
        return None
    s = sorted(samples)
    p = lambda q: s[min(len(s) - 1, int(q * len(s)))]
    return {"p50": round(p(.5), 1), "p99": round(p(.99), 1),
            "jitter": round(p(.99) - p(.5), 1), "n": len(s)}


async def _sampler(state, stop):
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass
        state.sample_rates()


async def _rtt_loop(state, hosts, stop):
    while not stop.is_set():
        for ex, (h, p) in hosts.items():
            if stop.is_set() or not h:
                break
            r = await tcp_rtt(h, p)
            if r:
                state.rtt[ex] = r
        try:
            await asyncio.wait_for(stop.wait(), timeout=PROBE_EVERY)
        except asyncio.TimeoutError:
            pass


async def _pusher(state, session, hub_url, token, every, stop):
    """When this node is a spoke, push its standardised report to the hub."""
    url = hub_url.rstrip("/") + "/api/ingest"
    headers = {"X-Auth-Token": token} if token else {}
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=every)
        except asyncio.TimeoutError:
            pass
        try:
            await session.post(url, json=state.self_report(), headers=headers,
                               timeout=aiohttp.ClientTimeout(total=10))
        except Exception:  # noqa
            pass


# ------------------------------- web ------------------------------
def make_app(state: LiveState, writer: ParquetWriter, token: str = ""):

    @web.middleware
    async def auth_mw(request, handler):
        if not token or request.path == "/login":
            return await handler(request)
        supplied = request.headers.get("X-Auth-Token") or request.cookies.get("bbo_token")
        if supplied and hmac.compare_digest(supplied, token):
            return await handler(request)
        # browser navigations -> login page; API -> 401 (token never in URL)
        if request.method == "GET" and "text/html" in request.headers.get("Accept", ""):
            raise web.HTTPFound("/login")
        return web.json_response({"error": "unauthorized"}, status=401)

    app = web.Application(middlewares=[auth_mw])

    async def index(_):
        return web.Response(text=HTML, content_type="text/html")

    async def login_get(request):
        err = "<p style='color:#ff6b6b'>wrong token</p>" if request.query.get("e") else ""
        return web.Response(text=LOGIN_HTML.replace("<!--ERR-->", err), content_type="text/html")

    async def login_post(request):
        data = await request.post()
        if token and hmac.compare_digest(data.get("token", ""), token):
            resp = web.HTTPFound("/")
            resp.set_cookie("bbo_token", token, httponly=True, samesite="Lax", max_age=2592000)
            return resp
        raise web.HTTPFound("/login?e=1")

    async def overview(_):
        return web.json_response({
            "node_id": state.node_id,
            "uptime_s": round(time.time() - state.started),
            "totals": {**state.totals(), "parquet_rows": writer.rows_written if writer else 0,
                       "parquet_files": writer.files_written if writer else 0},
            "exchanges": state.overview(),
        })

    async def book(req):
        def _f(k, d):
            try:
                return float(req.query.get(k, d))
            except ValueError:
                return d
        return web.json_response(state.book(req.query.get("symbol", "BTC/USDT"),
                                            _f("fresh_ms", 2000), _f("dev_bps", 5)))

    async def symbols(_):
        return web.json_response({"symbols": state.top_symbols()})

    async def devices(_):
        return web.json_response(state.device_comparison())

    async def ingest(req):
        try:
            state.ingest_device(await req.json())
            return web.json_response({"ok": True})
        except Exception as e:  # noqa
            return web.json_response({"ok": False, "err": str(e)}, status=400)

    app.add_routes([
        web.get("/", index), web.get("/login", login_get), web.post("/login", login_post),
        web.get("/api/overview", overview),
        web.get("/api/book", book), web.get("/api/symbols", symbols),
        web.get("/api/devices", devices), web.post("/api/ingest", ingest),
    ])
    return app


async def serve(exchanges, out, host, port, max_symbols, node_id, token="", hub_url="",
                push_every=5.0, allow=None):
    state = LiveState(node_id)
    adapters = [REGISTRY[n] for n in exchanges]
    state.metrics = {a.name: Metrics() for a in adapters}
    hosts = {a.name: ws_host(a) for a in adapters}
    queue: asyncio.Queue = asyncio.Queue(maxsize=1_000_000)
    stop = asyncio.Event()
    writer = ParquetWriter(out)

    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30)
    session = aiohttp.ClientSession(connector=connector, timeout=timeout)

    runner = web.AppRunner(make_app(state, writer, token))
    await runner.setup()
    await web.TCPSite(runner, host, port).start()
    shown = "localhost" if host in ("0.0.0.0", "") else host
    print(f"\n  Panel:  http://{shown}:{port}" + (f"/?token=<TOKEN>" if token else "")
          + f"\n  Node:   {node_id}   Exchanges: {len(adapters)}   Recording -> {out}\n"
          f"  Auth:   {'ON (token required)' if token else 'OFF (localhost only — do NOT expose)'}\n"
          + (f"  Pushing metrics -> {hub_url}\n" if hub_url else
             f"  Hub mode: remote nodes push to POST :{port}/api/ingest\n"))

    tasks = [
        asyncio.create_task(writer.run(queue, stop)),
        asyncio.create_task(_sampler(state, stop)),
        asyncio.create_task(_rtt_loop(state, hosts, stop)),
    ]
    if hub_url:
        tasks.append(asyncio.create_task(_pusher(state, session, hub_url, token, push_every, stop)))
    for a in adapters:
        tasks.append(asyncio.create_task(
            run_exchange(a, queue, stop, session, state.metrics[a.name],
                         symbol_cap=max_symbols, live=state, allow=allow)))
    try:
        await stop.wait()
    finally:
        stop.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await session.close()
        await runner.cleanup()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--exchanges", default="all")
    ap.add_argument("--out", default="data/bbo")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8800)
    ap.add_argument("--max-symbols", type=int, default=0)
    ap.add_argument("--node-id", default="local")
    ap.add_argument("--token-file", default=os.environ.get("BBO_PANEL_TOKEN_FILE", ""),
                    help="path to a file containing the auth token (preferred — keeps it out of cmdline/URL)")
    ap.add_argument("--token", default="", help="auth token (avoid on cmdline; prefer --token-file or env BBO_PANEL_TOKEN)")
    ap.add_argument("--hub-url", default="", help="if set, push this node's metrics to that hub")
    ap.add_argument("--push-every", type=float, default=5.0)
    ap.add_argument("--bases", default="", help="comma list of base coins to record (e.g. BTC,ETH,SOL); empty = all")
    ap.add_argument("--bases-file", default="", help="file with one base coin per line (e.g. coins_liquid.txt)")
    ap.add_argument("--quotes", default="USDT,USDC,USD,BTC,ETH", help="quote currencies to keep, or ALL")
    a = ap.parse_args()
    token = a.token or os.environ.get("BBO_PANEL_TOKEN", "")
    if a.token_file:
        token = open(a.token_file, encoding="utf-8").read().strip()
    allow = make_allow(load_bases(a.bases, a.bases_file), a.quotes)
    names = list(REGISTRY) if a.exchanges == "all" else [x.strip() for x in a.exchanges.split(",")]
    bad = [n for n in names if n not in REGISTRY]
    if bad:
        raise SystemExit(f"unknown: {bad}")
    if allow:
        print(f"  Symbol filter: bases={sorted(load_bases(a.bases, a.bases_file))[:8]}... quotes={a.quotes}")
    try:
        asyncio.run(serve(names, a.out, a.host, a.port, a.max_symbols or None, a.node_id,
                          token, a.hub_url, a.push_every, allow))
    except KeyboardInterrupt:
        print("\nstopped.")


LOGIN_HTML = r"""<!doctype html><html><head><meta charset=utf-8><title>BBO Panel — login</title>
<style>body{background:#0b0e14;color:#cdd6e4;font:14px ui-monospace,monospace;display:flex;
height:100vh;margin:0;align-items:center;justify-content:center}
form{background:#0e1320;border:1px solid #1c2330;padding:28px;border-radius:8px;text-align:center}
input{background:#0b0e14;color:#cdd6e4;border:1px solid #1c2330;padding:8px;width:260px;font-family:inherit}
button{background:#5aa9ff;color:#06101f;border:0;padding:8px 16px;margin-top:10px;cursor:pointer;border-radius:4px}
h2{color:#5aa9ff;margin:0 0 14px}</style></head><body>
<form method=post action=/login>
<h2>📡 BBO Panel</h2><!--ERR-->
<input type=password name=token placeholder="enter token" autofocus autocomplete=current-password>
<br><button>unlock</button></form></body></html>"""

HTML = r"""<!doctype html><html><head><meta charset=utf-8>
<title>BBO Panel</title><style>
:root{--bg:#0b0e14;--fg:#cdd6e4;--mut:#7a8699;--line:#1c2330;--grn:#39d98a;--red:#ff6b6b;--acc:#5aa9ff;--warn:#ffcf5c}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:13px/1.4 ui-monospace,Menlo,Consolas,monospace}
h1{font-size:15px;margin:0}h2{font-size:13px;color:var(--acc);margin:18px 12px 6px;border-bottom:1px solid var(--line);padding-bottom:4px}
header{display:flex;gap:18px;align-items:center;padding:10px 12px;background:#0e1320;border-bottom:1px solid var(--line);flex-wrap:wrap}
.kpi{color:var(--mut)}.kpi b{color:var(--fg)}
table{width:calc(100% - 24px);margin:0 12px;border-collapse:collapse}
th,td{text-align:right;padding:3px 8px;border-bottom:1px solid var(--line);white-space:nowrap}
th{color:var(--mut);font-weight:400}td:first-child,th:first-child{text-align:left}
.live{color:var(--grn)}.dead{color:var(--red)}.mut{color:var(--mut)}.grn{color:var(--grn)}.red{color:var(--red)}.warn{color:var(--warn)}
.best{background:rgba(57,217,138,.14)}.stale{opacity:.4}.off{opacity:.6}select{background:#0e1320;color:var(--fg);border:1px solid var(--line);padding:4px;font-family:inherit}
.bar{height:6px;background:var(--acc);border-radius:3px;display:inline-block;vertical-align:middle}
</style></head><body>
<header>
 <h1>📡 BBO Panel</h1>
 <span class=kpi>node <b id=node>–</b></span>
 <span class=kpi>up <b id=up>–</b></span>
 <span class=kpi>live <b id=liveex>–</b></span>
 <span class=kpi>updates/s <b id=ups>–</b></span>
 <span class=kpi>parquet rows <b id=rows>–</b></span>
 <span class=kpi>drops <b id=drops>–</b></span>
 <span class=kpi>symbols <b id=syms>–</b></span>
</header>

<h2>Cross-exchange book &nbsp; <select id=sym></select> <span id=edge class=mut></span></h2>
<table id=booktbl><thead><tr><th>exchange<th>bid<th>bidQty<th>ask<th>askQty<th>spread bps<th>dev bps<th>X (ms)<th>age ms<th>feed-lag ms</tr></thead><tbody></tbody></table>

<h2>Exchanges — health & latency (this device)</h2>
<table id=extbl><thead><tr><th>exchange<th>status<th>upd/s<th>~X ms<th>RTT p50<th>RTT p99<th>jitter<th>feed-lag<th>proc µs p50<th>proc µs p99<th>records<th>drops<th>reconn</tr></thead><tbody></tbody></table>

<h2>Device comparison — network RTT p50 (ms) per exchange &nbsp;<span class=mut>(green = best device)</span></h2>
<table id=devtbl><thead><tr></tr></thead><tbody></tbody></table>

<script>
const $=s=>document.querySelector(s), fmt=(x,d=1)=>x==null?'·':(+x).toFixed(d);
async function j(u){try{return await (await fetch(u)).json()}catch(e){return null}}
let curSym='BTC/USDT';
async function loadSyms(){const d=await j('/api/symbols');if(!d)return;const s=$('#sym');
  s.innerHTML=d.symbols.map(x=>`<option ${x==curSym?'selected':''}>${x}</option>`).join('');
  s.onchange=()=>{curSym=s.value;book()};}
async function overview(){const d=await j('/api/overview');if(!d)return;
  $('#node').textContent=d.node_id;$('#up').textContent=d.uptime_s+'s';
  const t=d.totals;$('#liveex').textContent=t.live_ex+'/'+t.exchanges;
  $('#ups').textContent=t.ups.toLocaleString();$('#rows').textContent=t.parquet_rows.toLocaleString();
  $('#drops').textContent=t.drops.toLocaleString();$('#syms').textContent=t.symbols_tracked.toLocaleString();
  const mx=Math.max(1,...d.exchanges.map(e=>e.ups||0));
  $('#extbl tbody').innerHTML=d.exchanges.map(e=>`<tr>
    <td>${e.exchange}</td>
    <td class=${e.live?'live':'dead'}>${e.live?'● live':'○ '+fmt(e.age_s)+'s'}</td>
    <td>${fmt(e.ups)} <span class=bar style=width:${40*(e.ups||0)/mx}px></span></td>
    <td class=mut>${fmt(e.x_ms)}</td>
    <td>${fmt(e.rtt_p50)}</td><td class=mut>${fmt(e.rtt_p99)}</td>
    <td class=${e.jitter>20?'warn':''}>${fmt(e.jitter)}</td>
    <td>${fmt(e.feed_lag_ms)}</td>
    <td class=grn>${fmt(e.proc_us_p50)}</td><td class=mut>${fmt(e.proc_us_p99)}</td>
    <td>${e.records.toLocaleString()}</td><td class=${e.drops?'red':'mut'}>${e.drops}</td><td>${e.reconnects}</td>
  </tr>`).join('');}
async function book(){const d=await j('/api/book?symbol='+encodeURIComponent(curSym));if(!d)return;
  const bb=d.edge?d.edge.best_bid:null, ba=d.edge?d.edge.best_ask:null;
  $('#booktbl tbody').innerHTML=d.venues.map(v=>{
    const sp=v.ask&&v.bid?1e4*(v.ask-v.bid)/v.ask:null, good=!v.stale&&!v.off;
    const tag=v.stale?' <span class=red>stale</span>':(v.off?' <span class=warn>off</span>':'');
    return `<tr class=${v.stale?'stale':(v.off?'off':'')}>
    <td>${v.exchange}${tag}</td>
    <td class=${good&&d.edge&&v.exchange==d.edge.best_bid_ex?'best grn':''}>${fmt(v.bid,8)}</td>
    <td class=mut>${fmt(v.bidq,4)}</td>
    <td class=${good&&d.edge&&v.exchange==d.edge.best_ask_ex?'best red':''}>${fmt(v.ask,8)}</td>
    <td class=mut>${fmt(v.askq,4)}</td>
    <td>${fmt(sp,2)}</td>
    <td class=${v.off?'warn':'mut'}>${fmt(v.dev_bps,2)}</td>
    <td class=mut>${fmt(v.x_ms,1)}</td>
    <td class=${v.stale?'red':'mut'}>${fmt(v.age_ms)}</td><td>${fmt(v.feed_lag_ms)}</td></tr>`}).join('');
  const cons=d.consensus?` · consensus ${fmt(d.consensus,2)}`:'';
  $('#edge').innerHTML=d.edge?`&nbsp; best bid <b class=grn>${d.edge.best_bid_ex} ${fmt(d.edge.best_bid,2)}</b>`
    +` · best ask <b class=red>${d.edge.best_ask_ex} ${fmt(d.edge.best_ask,2)}</b>`
    +` · edge <b class=${d.edge.edge>0?'grn':'mut'}>${fmt(d.edge.edge,4)} (${fmt(d.edge.edge_bps,2)} bps)</b>${cons}`
    +` · <span class=mut>${d.in_consensus} ok / ${d.fresh_venues} fresh / ${d.venues.length} venues (age&lt;${d.fresh_ms}ms, dev&lt;${d.dev_bps}bps)</span>`
    :`&nbsp;<span class=mut>no edge — ${d.in_consensus} in-consensus of ${d.venues.length} venues${cons}</span>`;}
async function devices(){const d=await j('/api/devices');if(!d)return;
  const ids=d.nodes.map(n=>n.node_id);
  $('#devtbl thead tr').innerHTML='<th>exchange</th>'+d.nodes.map(n=>
    `<th>${n.node_id}${n.local?' ★':''}${n.stale?' <span class=red>(stale)</span>':''}`
    +`<br><span class=mut>${fmt(n.ups,0)} u/s · live ${n.live_ex??'·'}/${n.exchanges??'·'} · up ${n.uptime_s??'·'}s`
    +`${n.drops?` · <span class=red>drop ${n.drops}</span>`:''}</span></th>`).join('');
  $('#devtbl tbody').innerHTML=d.table.map(r=>`<tr><td>${r.exchange}</td>`+
    ids.map(id=>{const c=r.cells[id];if(!c)return '<td class=mut>·</td>';
      return `<td class=${id==r.best_dev?'best grn':''}>${fmt(c.p50)}<span class=mut> ±${fmt(c.jitter)}</span></td>`}).join('')
    +'</tr>').join('');}
function tick(){overview();book();devices();}
loadSyms();tick();setInterval(tick,1000);setInterval(loadSyms,15000);
</script></body></html>"""

if __name__ == "__main__":
    main()
