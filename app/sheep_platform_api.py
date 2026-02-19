import json
import os
import time
import random
import hashlib
import html
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

import sheep_platform_db as db
import backtest_panel2 as bt
from sheep_platform_rate_limit import RateLimiter
from sheep_platform_version import semver_gte


db.init_db()

API_ROOT_PATH = os.environ.get("SHEEP_API_ROOT_PATH", "").strip()

app = FastAPI(title="sheep-platform-api", root_path=API_ROOT_PATH)


# In-memory rate limiters (cheap + good enough for a single API instance)
_token_limiter = RateLimiter(rate_per_minute=600.0, burst=120.0)
_token_touch_cache: Dict[int, float] = {}
_settings_cache: Dict[str, Any] = {"ts": 0.0, "ratelimit_rpm": 600.0, "ratelimit_burst": 120.0, "slow_ms": 800.0, "sample": 0.05}

_token_issue_limiter = RateLimiter(rate_per_minute=30.0, burst=10.0)

# Chat rate limiting: 3 messages / 20s per user (and a second IP guard)
_chat_user_limiter = RateLimiter(rate_per_minute=9.0, burst=3.0)
_chat_ip_limiter = RateLimiter(rate_per_minute=18.0, burst=6.0)


class _ChatHub:
    def __init__(self) -> None:
        self._conns: List[WebSocket] = []

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._conns.append(ws)

    def disconnect(self, ws: WebSocket) -> None:
        try:
            self._conns.remove(ws)
        except Exception:
            pass

    async def broadcast(self, message: Dict[str, Any]) -> None:
        dead: List[WebSocket] = []
        for ws in list(self._conns):
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


_chat_hub = _ChatHub()


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_settings_cached() -> Dict[str, Any]:
    now = time.time()
    if now - float(_settings_cache.get("ts", 0.0)) < 5.0:
        return _settings_cache

    conn = db._conn()  # internal; we cache so OK
    try:
        ratelimit_rpm = float(db.get_setting(conn, "api_ratelimit_rpm", 600.0))
        ratelimit_burst = float(db.get_setting(conn, "api_ratelimit_burst", 120.0))
        slow_ms = float(db.get_setting(conn, "api_slow_ms", 800.0))
        sample = float(db.get_setting(conn, "api_log_sample_rate", 0.05))
    finally:
        conn.close()

    _settings_cache.update(
        {
            "ts": now,
            "ratelimit_rpm": max(0.0, ratelimit_rpm),
            "ratelimit_burst": max(1.0, ratelimit_burst),
            "slow_ms": max(0.0, slow_ms),
            "sample": min(1.0, max(0.0, sample)),
        }
    )
    _token_limiter.configure(_settings_cache["ratelimit_rpm"], _settings_cache["ratelimit_burst"])
    return _settings_cache


def _client_ip(req: Request) -> str:
    """Best-effort client IP.

    In production we sit behind Nginx. Use X-Forwarded-For / X-Real-IP if present,
    otherwise fall back to req.client.host.
    """
    try:
        xff = req.headers.get("x-forwarded-for") or req.headers.get("X-Forwarded-For") or ""
        if xff:
            # First IP is the original client; the rest are proxies.
            ip = xff.split(",", 1)[0].strip()
            if ip:
                return ip

        xri = req.headers.get("x-real-ip") or req.headers.get("X-Real-IP") or ""
        if xri.strip():
            return xri.strip()

        if req.client and req.client.host:
            return str(req.client.host)
    except Exception:
        pass
    return ""


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _auth_ctx(req: Request, authorization: Optional[str]) -> Dict[str, Any]:
    if not authorization:
        raise HTTPException(status_code=401, detail="missing_authorization")

    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="bad_authorization")

    raw = parts[1].strip()
    res = db.verify_api_token(raw)
    if not res:
        raise HTTPException(status_code=401, detail="invalid_or_expired_token")

    user = res["user"]
    tok = res["token"]

    # rate limit by token_id
    cfg = _get_settings_cached()
    allowed, retry_after = _token_limiter.check(str(tok["id"]))
    if not allowed:
        headers = {"Retry-After": str(int(max(1, retry_after or 1.0)))}
        raise HTTPException(status_code=429, detail="rate_limited", headers=headers)

    # touch token at most every ~30s
    tid = int(tok["id"])
    now = time.time()
    last = float(_token_touch_cache.get(tid, 0.0))
    if now - last > 30.0:
        _token_touch_cache[tid] = now
        try:
            db.touch_api_token(tid, ip=_client_ip(req), user_agent=req.headers.get("user-agent"))
        except Exception:
            pass

    # stash for logging
    req.state.user_id = int(user["id"])
    req.state.token_id = int(tok["id"])

    return {"user": user, "token": tok}


def _require_worker(
    req: Request,
    ctx: Dict[str, Any],
    worker_id: Optional[str],
    worker_version: Optional[str],
    worker_protocol: Optional[int],
) -> Dict[str, Any]:
    wid = str(worker_id or "").strip()
    wv = str(worker_version or "").strip()
    try:
        wp = int(worker_protocol) if worker_protocol is not None else 0
    except Exception:
        wp = 0

    if not wid:
        raise HTTPException(status_code=400, detail="missing_x_worker_id")

    conn = db._conn()
    try:
        min_protocol = int(db.get_setting(conn, "worker_min_protocol", 2))
        min_version = str(db.get_setting(conn, "worker_min_version", "2.0.0"))
        latest_version = str(db.get_setting(conn, "worker_latest_version", min_version))
    finally:
        conn.close()

    if wp < min_protocol:
        raise HTTPException(status_code=426, detail={"error": "worker_protocol_too_old", "min_protocol": min_protocol})

    if not semver_gte(wv, min_version):
        raise HTTPException(status_code=426, detail={"error": "worker_version_too_old", "min_version": min_version, "latest": latest_version})

    req.state.worker_id = wid

    try:
        db.upsert_worker(
            worker_id=wid,
            user_id=int(ctx["user"]["id"]),
            version=wv,
            protocol=wp,
            meta={"ua": req.headers.get("user-agent"), "ip": _client_ip(req)},
        )
    except Exception:
        pass

    return {"worker_id": wid, "worker_version": wv, "worker_protocol": wp}


def _metrics_from_bt_result(res: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "total_return_pct": float(res.get("total_return_pct", 0.0)),
        "max_drawdown_pct": float(res.get("max_drawdown_pct", 0.0)),
        "sharpe": float(res.get("sharpe", 0.0)),
        "trades": int(res.get("trades", 0)),
        "win_rate_pct": float(res.get("win_rate_pct", 0.0)),
    }


def _score(metrics: Dict[str, Any]) -> float:
    trades = int(metrics.get("trades", 0) or 0)
    if trades <= 0:
        return -1e18
    ret = float(metrics.get("total_return_pct", 0.0) or 0.0)
    dd = float(metrics.get("max_drawdown_pct", 0.0) or 0.0)
    sh = float(metrics.get("sharpe", 0.0) or 0.0)
    return float(ret + 5.0 * sh - 0.6 * dd)


def _passes_thresholds(metrics: Dict[str, Any], min_trades: int, min_ret: float, max_dd: float, min_sh: float) -> bool:
    return bool(
        int(metrics.get("trades", 0)) >= int(min_trades)
        and float(metrics.get("total_return_pct", 0.0)) >= float(min_ret)
        and float(metrics.get("max_drawdown_pct", 0.0)) <= float(max_dd)
        and float(metrics.get("sharpe", 0.0)) >= float(min_sh)
    )


@app.middleware("http")
async def _log_mw(request: Request, call_next):
    start = time.time()
    ts_iso = _utc_iso()

    status_code = 500
    try:
        resp = await call_next(request)
        status_code = int(getattr(resp, "status_code", 200) or 200)
        return resp
    except HTTPException as he:
        status_code = int(getattr(he, "status_code", 500) or 500)
        raise
    finally:
        dur_ms = (time.time() - start) * 1000.0
        cfg = _get_settings_cached()

        user_id = getattr(request.state, "user_id", None)
        token_id = getattr(request.state, "token_id", None)
        worker_id = getattr(request.state, "worker_id", None)

        # sample successful requests; always log errors and slow requests
        do_log = (status_code >= 400) or (dur_ms >= float(cfg["slow_ms"]))
        if not do_log:
            do_log = random.random() < float(cfg["sample"])

        if do_log:
            try:
                db.log_api_request(
                    ts_iso=ts_iso,
                    user_id=int(user_id) if user_id is not None else None,
                    worker_id=str(worker_id) if worker_id else None,
                    token_id=int(token_id) if token_id is not None else None,
                    method=request.method,
                    path=str(request.url.path),
                    status_code=int(status_code),
                    duration_ms=float(dur_ms),
                    detail={"query": str(request.url.query), "ip": _client_ip(request)},
                )
            except Exception:
                pass


class TokenRequest(BaseModel):
    username: str
    password: str
    ttl_seconds: int = 86400
    name: str = "worker"


class TokenResponse(BaseModel):
    token: str
    token_id: int
    user_id: int
    role: str
    issued_at: str
    expires_at: str


class TaskOut(BaseModel):
    task_id: int
    pool_id: int
    pool_name: str
    symbol: str
    timeframe_min: int
    years: int
    family: str
    partition_idx: int
    num_partitions: int
    seed: int
    grid_spec: Dict[str, Any]
    risk_spec: Dict[str, Any]
    data_hash: str = ""
    data_hash_ts: str = ""
    lease_id: str
    progress: Dict[str, Any]


class ProgressIn(BaseModel):
    lease_id: str
    progress: Dict[str, Any]


class FinishIn(BaseModel):
    lease_id: str
    candidates: List[Dict[str, Any]]
    final_progress: Dict[str, Any]
    data_hash: str = ""


class ReleaseIn(BaseModel):
    lease_id: str
    progress: Dict[str, Any]


@app.get("/healthz")
def healthz():
    return {
        "ok": True,
        "ts": _utc_iso(),
        "git_sha": os.getenv("SHEEP_GIT_SHA", ""),
    }


@app.get("/", response_class=HTMLResponse)
def landing() -> HTMLResponse:
    """Lightweight landing HTML used for link previews (LINE/IG).

    Open Graph crawlers do not execute JavaScript on Streamlit pages. This endpoint serves
    OG meta tags server-side.
    """
    conn = db._conn()
    try:
        title = str(db.get_setting(conn, "og_title", "羊肉爐挖礦分潤平台") or "")
        desc = str(db.get_setting(conn, "og_description", "") or "")
        site = str(db.get_setting(conn, "og_site_name", "") or "")
        img = str(db.get_setting(conn, "og_image_url", "") or "")
        redirect_url = str(db.get_setting(conn, "og_redirect_url", "") or "")
    finally:
        conn.close()

    esc_title = html.escape(title)
    esc_desc = html.escape(desc)
    esc_site = html.escape(site)
    esc_img = html.escape(img)
    esc_redirect = html.escape(redirect_url)

    refresh_meta = ""
    jump_block = ""
    if redirect_url:
        refresh_meta = f'<meta http-equiv="refresh" content="0;url={esc_redirect}">'
        jump_block = f'<p><a href="{esc_redirect}">開啟平台</a></p>'

    html_doc = f"""<!doctype html>
<html lang=\"zh-Hant\">
  <head>
    <meta charset=\"utf-8\">
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
    <title>{esc_title}</title>
    <meta property=\"og:title\" content=\"{esc_title}\">
    <meta property=\"og:description\" content=\"{esc_desc}\">
    <meta property=\"og:site_name\" content=\"{esc_site}\">
    <meta property=\"og:type\" content=\"website\">
    {f'<meta property="og:image" content="{esc_img}">' if img else ''}
    {refresh_meta}
    <meta name=\"twitter:card\" content=\"summary_large_image\">
  </head>
  <body>
    <h1>{esc_title}</h1>
    <p>{esc_desc}</p>
    {jump_block}
  </body>
</html>"""

    return HTMLResponse(content=html_doc, status_code=200)


@app.get("/manifest")
def manifest():
    conn = db._conn()
    try:
        return {
            "server_time": _utc_iso(),
            "worker_min_version": str(db.get_setting(conn, "worker_min_version", "2.0.0")),
            "worker_latest_version": str(db.get_setting(conn, "worker_latest_version", "2.0.0")),
            "worker_min_protocol": int(db.get_setting(conn, "worker_min_protocol", 2)),
            "worker_download_url": str(db.get_setting(conn, "worker_download_url", "")),
        }
    finally:
        conn.close()


class ChatSendIn(BaseModel):
    text: str


@app.get("/chat/recent")
def chat_recent(limit: int = 50) -> Dict[str, Any]:
    msgs = db.list_chat_messages(limit=int(limit))
    return {"ts": _utc_iso(), "messages": msgs}


@app.post("/chat/send")
async def chat_send(
    request: Request,
    body: ChatSendIn,
    authorization: Optional[str] = Header(None),
):
    ctx = _auth_ctx(request, authorization)
    user_id = int(ctx["user"]["id"])
    ip = _client_ip(request) or "ip"

    allowed_u, ra_u = _chat_user_limiter.check(f"u:{user_id}", cost=1.0)
    allowed_ip, ra_ip = _chat_ip_limiter.check(f"ip:{ip}", cost=1.0)
    if not (allowed_u and allowed_ip):
        retry_after = int(max(1, max(float(ra_u or 0.0), float(ra_ip or 0.0))))
        raise HTTPException(status_code=429, detail="chat_rate_limited", headers={"Retry-After": str(retry_after)})

    raw_text = str(body.text or "").strip()
    if not raw_text:
        raise HTTPException(status_code=400, detail="empty_message")

    conn = db._conn()
    try:
        blocked_raw = str(db.get_setting(conn, "chat_blocked_words", "") or "")
        max_len = int(db.get_setting(conn, "chat_max_len", 120) or 120)
    finally:
        conn.close()

    max_len = int(max(1, min(500, max_len)))
    if len(raw_text) > max_len:
        raw_text = raw_text[:max_len]

    blocked = [w.strip() for w in blocked_raw.splitlines() if w.strip()]
    low = raw_text.lower()
    for w in blocked:
        if w.lower() in low:
            raise HTTPException(status_code=400, detail="blocked_word")

    safe_text = html.escape(raw_text, quote=True)

    msg = {
        "type": "chat_message",
        "ts": _utc_iso(),
        "user_id": int(user_id),
        "username": str(ctx["user"].get("username") or ""),
        "text": safe_text,
    }

    try:
        db.insert_worker_event(user_id=int(user_id), worker_id=None, event="chat_message", detail={"text": safe_text, "ip": ip})
    except Exception:
        pass

    await _chat_hub.broadcast(msg)
    return {"ok": True}


@app.websocket("/ws/chat")
async def ws_chat(ws: WebSocket):
    ip = (ws.client.host if ws.client else "") or "ip"
    allowed, _ = _chat_ip_limiter.check(f"ws:{ip}", cost=1.0)
    if not allowed:
        await ws.close(code=1008)
        return

    await _chat_hub.connect(ws)
    try:
        # Send recent messages on connect.
        try:
            recent = db.list_chat_messages(limit=50)
        except Exception:
            recent = []
        await ws.send_json({"type": "recent", "ts": _utc_iso(), "messages": recent})
        while True:
            # We do not accept client->server chat over WebSocket for security; use /chat/send.
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        _chat_hub.disconnect(ws)


@app.post("/token", response_model=TokenResponse)
def issue_token(req: Request, body: TokenRequest):
    # rate limit by IP to avoid brute force
    allowed, retry_after = _token_issue_limiter.check(_client_ip(req) or "ip", cost=1.0)
    if not allowed:
        headers = {"Retry-After": str(int(max(1, retry_after or 1.0)))}
        raise HTTPException(status_code=429, detail="rate_limited", headers=headers)

    user = db.get_user_by_username(body.username)
    if not user:
        raise HTTPException(status_code=401, detail="bad_credentials")

    if not db.verify_user_password(user["id"], body.password):
        raise HTTPException(status_code=401, detail="bad_credentials")

    if int(user.get("disabled") or 0) != 0:
        raise HTTPException(status_code=403, detail="user_disabled")

    token = db.create_api_token(int(user["id"]), ttl_seconds=int(body.ttl_seconds), name=str(body.name or "worker"))

    return TokenResponse(
        token=str(token["token"]),
        token_id=int(token["token_id"]),
        user_id=int(user["id"]),
        role=str(user.get("role") or "user"),
        issued_at=str(token.get("issued_at")),
        expires_at=str(token.get("expires_at")),
    )


@app.get("/flags")
def flags(
    request: Request,
    authorization: Optional[str] = Header(None),
    x_worker_id: Optional[str] = Header(None),
    x_worker_version: Optional[str] = Header(None),
    x_worker_protocol: Optional[int] = Header(None),
):
    ctx = _auth_ctx(request, authorization)
    _require_worker(request, ctx, x_worker_id, x_worker_version, x_worker_protocol)
    user_id = int(ctx["user"]["id"])

    return {"run_enabled": bool(db.get_user_run_enabled(user_id))}


@app.get("/settings/thresholds")
def thresholds(
    request: Request,
    authorization: Optional[str] = Header(None),
    x_worker_id: Optional[str] = Header(None),
    x_worker_version: Optional[str] = Header(None),
    x_worker_protocol: Optional[int] = Header(None),
):
    ctx = _auth_ctx(request, authorization)
    _require_worker(request, ctx, x_worker_id, x_worker_version, x_worker_protocol)

    conn = db._conn()
    try:
        return {
            "min_trades": int(db.get_setting(conn, "min_trades", 30)),
            "min_total_return_pct": float(db.get_setting(conn, "min_total_return_pct", 3.0)),
            "max_drawdown_pct": float(db.get_setting(conn, "max_drawdown_pct", 25.0)),
            "min_sharpe": float(db.get_setting(conn, "min_sharpe", 0.6)),
            "keep_top_n": int(db.get_setting(conn, "candidate_keep_top_n", 30)),
        }
    finally:
        conn.close()


@app.get("/settings/snapshot")
def settings_snapshot(
    request: Request,
    authorization: Optional[str] = Header(None),
    x_worker_id: Optional[str] = Header(None),
    x_worker_version: Optional[str] = Header(None),
    x_worker_protocol: Optional[int] = Header(None),
):
    ctx = _auth_ctx(request, authorization)
    _require_worker(request, ctx, x_worker_id, x_worker_version, x_worker_protocol)
    conn = db._conn()
    try:
        thresholds = {
            "min_trades": int(db.get_setting(conn, "min_trades", 30)),
            "min_total_return_pct": float(db.get_setting(conn, "min_total_return_pct", 3.0)),
            "max_drawdown_pct": float(db.get_setting(conn, "max_drawdown_pct", 25.0)),
            "min_sharpe": float(db.get_setting(conn, "min_sharpe", 0.6)),
            "keep_top_n": int(db.get_setting(conn, "candidate_keep_top_n", 30)),
        }
    finally:
        conn.close()
    return {"ts": _utc_iso(), "thresholds": thresholds}


@app.post("/workers/heartbeat")
def worker_heartbeat_endpoint(
    request: Request,
    authorization: Optional[str] = Header(None),
    x_worker_id: Optional[str] = Header(None),
    x_worker_version: Optional[str] = Header(None),
    x_worker_protocol: Optional[int] = Header(None),
    x_current_task_id: Optional[int] = Header(None),
):
    ctx = _auth_ctx(request, authorization)
    w = _require_worker(request, ctx, x_worker_id, x_worker_version, x_worker_protocol)
    try:
        db.worker_heartbeat(w["worker_id"], int(ctx["user"]["id"]), task_id=int(x_current_task_id) if x_current_task_id else None)
    except Exception:
        pass
    return {"ok": True}


@app.post("/tasks/claim", response_model=Optional[TaskOut])
def claim_task(
    request: Request,
    authorization: Optional[str] = Header(None),
    x_worker_id: Optional[str] = Header(None),
    x_worker_version: Optional[str] = Header(None),
    x_worker_protocol: Optional[int] = Header(None),
):
    ctx = _auth_ctx(request, authorization)
    w = _require_worker(request, ctx, x_worker_id, x_worker_version, x_worker_protocol)

    user_id = int(ctx["user"]["id"])
    if not db.get_user_run_enabled(user_id):
        return None

    task = db.claim_next_task(user_id, w["worker_id"])
    if not task:
        return None

    try:
        progress = json.loads(task.get("progress_json") or "{}")
    except Exception:
        progress = {}

    try:
        grid_spec = json.loads(task.get("grid_spec_json") or "{}")
    except Exception:
        grid_spec = {}
    try:
        risk_spec = json.loads(task.get("risk_spec_json") or "{}")
    except Exception:
        risk_spec = {}

    years = int(task.get("years") or 0) or 3
    dh = {"data_hash": "", "data_hash_ts": ""}
    try:
        dh = db.get_data_hash(str(task.get("symbol") or ""), int(task.get("timeframe_min") or 0), int(years))
    except Exception:
        dh = {"data_hash": "", "data_hash_ts": ""}

    # If the server hasn't recorded a hash yet, compute it once on-demand.
    if not str(dh.get("data_hash") or "").strip():
        try:
            csv_main, _ = bt.ensure_bitmart_data(
                symbol=str(task.get("symbol") or ""),
                main_step_min=int(task.get("timeframe_min") or 0),
                years=int(years),
                auto_sync=True,
                force_full=False,
            )
            local_hash = _sha256_file(csv_main)
            if local_hash:
                db.set_data_hash(str(task.get("symbol") or ""), int(task.get("timeframe_min") or 0), int(years), local_hash, ts=_utc_iso())
                dh = db.get_data_hash(str(task.get("symbol") or ""), int(task.get("timeframe_min") or 0), int(years))
        except Exception:
            pass

    return TaskOut(
        task_id=int(task["id"]),
        pool_id=int(task["pool_id"]),
        pool_name=str(task.get("pool_name") or ""),
        symbol=str(task.get("symbol") or ""),
        timeframe_min=int(task.get("timeframe_min") or 0),
        years=int(years),
        family=str(task.get("family") or ""),
        partition_idx=int(task.get("partition_idx") or 0),
        num_partitions=int(task.get("num_partitions") or 1),
        seed=int(task.get("seed") or 0),
        grid_spec=grid_spec,
        risk_spec=risk_spec,
        data_hash=str(dh.get("data_hash") or ""),
        data_hash_ts=str(dh.get("data_hash_ts") or ""),
        lease_id=str(task.get("lease_id") or ""),
        progress=progress,
    )


@app.post("/tasks/{task_id}/progress")
def update_progress(
    task_id: int,
    request: Request,
    body: ProgressIn,
    authorization: Optional[str] = Header(None),
    x_worker_id: Optional[str] = Header(None),
    x_worker_version: Optional[str] = Header(None),
    x_worker_protocol: Optional[int] = Header(None),
):
    ctx = _auth_ctx(request, authorization)
    w = _require_worker(request, ctx, x_worker_id, x_worker_version, x_worker_protocol)

    ok = db.update_task_progress_with_lease(
        task_id=int(task_id),
        user_id=int(ctx["user"]["id"]),
        worker_id=w["worker_id"],
        lease_id=str(body.lease_id),
        progress=body.progress,
    )
    if not ok:
        raise HTTPException(status_code=409, detail="lease_mismatch_or_task_not_running")
    return {"ok": True}


@app.post("/tasks/{task_id}/release")
def release_task(
    task_id: int,
    request: Request,
    body: ReleaseIn,
    authorization: Optional[str] = Header(None),
    x_worker_id: Optional[str] = Header(None),
    x_worker_version: Optional[str] = Header(None),
    x_worker_protocol: Optional[int] = Header(None),
):
    ctx = _auth_ctx(request, authorization)
    w = _require_worker(request, ctx, x_worker_id, x_worker_version, x_worker_protocol)

    ok = db.release_task_with_lease(
        task_id=int(task_id),
        user_id=int(ctx["user"]["id"]),
        worker_id=w["worker_id"],
        lease_id=str(body.lease_id),
        progress=body.progress,
    )
    if not ok:
        raise HTTPException(status_code=409, detail="lease_mismatch_or_task_not_running")
    return {"ok": True}


@app.post("/tasks/{task_id}/finish")
def finish_task(
    task_id: int,
    request: Request,
    body: FinishIn,
    authorization: Optional[str] = Header(None),
    x_worker_id: Optional[str] = Header(None),
    x_worker_version: Optional[str] = Header(None),
    x_worker_protocol: Optional[int] = Header(None),
):
    ctx = _auth_ctx(request, authorization)
    w = _require_worker(request, ctx, x_worker_id, x_worker_version, x_worker_protocol)

    user_id = int(ctx["user"]["id"])
    worker_id = str(w["worker_id"])
    lease_id = str(body.lease_id or "").strip()

    task_row = db.get_task(int(task_id))
    if not task_row:
        raise HTTPException(status_code=404, detail="not_found")

    # Cheap consistency check before doing any heavy server-side verification.
    if str(task_row.get("status") or "") != "running":
        raise HTTPException(status_code=409, detail="task_not_running")
    if int(task_row.get("user_id") or 0) != int(user_id):
        raise HTTPException(status_code=409, detail="task_not_owned")
    if str(task_row.get("lease_id") or "") != lease_id:
        raise HTTPException(status_code=409, detail="lease_mismatch")
    if str(task_row.get("lease_worker_id") or "") != worker_id:
        raise HTTPException(status_code=409, detail="worker_mismatch")

    years = int(task_row.get("years") or 0) or 3
    symbol = str(task_row.get("symbol") or "")
    tf_min = int(task_row.get("timeframe_min") or 0)

    # Data hash consistency guard: do not ban on mismatches that are due to stale data.
    server_dh = {"data_hash": "", "data_hash_ts": ""}
    try:
        server_dh = db.get_data_hash(symbol, tf_min, years)
    except Exception:
        server_dh = {"data_hash": "", "data_hash_ts": ""}

    worker_dh = str((body.final_progress or {}).get("data_hash") or body.data_hash or "").strip()
    if server_dh.get("data_hash") and worker_dh and str(server_dh.get("data_hash")) != worker_dh:
        try:
            prog = dict(body.final_progress or {})
            prog["last_error"] = "data_hash_mismatch"
            prog["server_data_hash"] = str(server_dh.get("data_hash") or "")
            prog["worker_data_hash"] = str(worker_dh)
            prog["updated_at"] = _utc_iso()
            db.release_task_with_lease(
                task_id=int(task_id),
                user_id=int(user_id),
                worker_id=worker_id,
                lease_id=lease_id,
                progress=prog,
            )
        except Exception:
            pass
        raise HTTPException(status_code=409, detail="data_hash_mismatch")

    # Server-side re-verify to prevent forged metrics.
    verified_candidates: List[Dict[str, Any]] = []
    raw_candidates: List[Dict[str, Any]] = list(body.candidates or [])

    conn = db._conn()
    try:
        min_trades = int(db.get_setting(conn, "min_trades", 40))
        min_ret = float(db.get_setting(conn, "min_total_return_pct", 15.0))
        max_dd = float(db.get_setting(conn, "max_drawdown_pct", 25.0))
        min_sh = float(db.get_setting(conn, "min_sharpe", 0.6))
        keep_top = int(db.get_setting(conn, "candidate_keep_top_n", 30))
        max_verify = int(db.get_setting(conn, "verify_max_candidates", 10))
        tol_ret = float(db.get_setting(conn, "verify_tolerance_return_pct", 0.1))
        tol_dd = float(db.get_setting(conn, "verify_tolerance_drawdown_pct", 0.1))
        tol_sh = float(db.get_setting(conn, "verify_tolerance_sharpe", 0.05))
        tol_tr = int(db.get_setting(conn, "verify_tolerance_trades", 1))
    finally:
        conn.close()

    max_verify = max(0, min(50, int(max_verify)))
    keep_top = max(1, min(200, int(keep_top)))

    try:
        try:
            risk_spec = json.loads(task_row.get("risk_spec_json") or "{}")
        except Exception:
            risk_spec = {}
        family = str(task_row.get("family") or "")
        fee_side = float(risk_spec.get("fee_side", 0.0002))
        slippage = float(risk_spec.get("slippage", 0.0))
        worst_case = bool(risk_spec.get("worst_case", True))
        reverse_mode = bool(risk_spec.get("reverse_mode", False))

        csv_main, _ = bt.ensure_bitmart_data(
            symbol=symbol,
            main_step_min=int(tf_min),
            years=int(years),
            auto_sync=True,
            force_full=False,
        )
        df = bt.load_and_validate_csv(csv_main)

        checked = 0
        for cand in raw_candidates:
            if max_verify and checked >= max_verify:
                break
            if not isinstance(cand, dict):
                continue
            params = cand.get("params") or cand.get("params_json") or {}
            if not isinstance(params, dict):
                continue

            cand_family = str(params.get("family") or family)
            family_params = params.get("family_params")
            if not isinstance(family_params, dict):
                # Backward compatibility: allow flat family params at top-level.
                family_params = {k: v for k, v in params.items() if k not in ("family", "tp", "sl", "max_hold")}

            try:
                tp = float(params.get("tp"))
                sl = float(params.get("sl"))
                mh = int(params.get("max_hold"))
            except Exception:
                continue

            res = bt.run_backtest(
                df,
                cand_family,
                dict(family_params),
                float(tp),
                float(sl),
                int(mh),
                fee_side=fee_side,
                slippage=slippage,
                worst_case=worst_case,
                reverse_mode=reverse_mode,
            )
            server_metrics = _metrics_from_bt_result(res)
            server_score = float(_score(server_metrics))

            reported = cand.get("metrics") or {}
            if isinstance(reported, dict) and reported:
                try:
                    rep_ret = float(reported.get("total_return_pct"))
                    rep_dd = float(reported.get("max_drawdown_pct"))
                    rep_sh = float(reported.get("sharpe"))
                    rep_tr = int(reported.get("trades"))
                    mismatch = bool(
                        abs(float(server_metrics.get("total_return_pct", 0.0)) - rep_ret) > float(tol_ret)
                        or abs(float(server_metrics.get("max_drawdown_pct", 0.0)) - rep_dd) > float(tol_dd)
                        or abs(float(server_metrics.get("sharpe", 0.0)) - rep_sh) > float(tol_sh)
                        or abs(int(server_metrics.get("trades", 0)) - rep_tr) > int(tol_tr)
                    )
                except Exception:
                    mismatch = False

                if mismatch:
                    # Hard stop: likely forged submission.
                    db.set_user_disabled(user_id, True)
                    db.write_audit_log(
                        actor_user_id=int(user_id),
                        action="cheat_detected",
                        detail={
                            "task_id": int(task_id),
                            "worker_id": worker_id,
                            "symbol": symbol,
                            "timeframe_min": int(tf_min),
                            "years": int(years),
                            "server_metrics": server_metrics,
                            "reported_metrics": reported,
                        },
                    )
                    try:
                        prog = dict(body.final_progress or {})
                        prog["last_error"] = "cheat_detected"
                        prog["updated_at"] = _utc_iso()
                        db.release_task_with_lease(
                            task_id=int(task_id),
                            user_id=int(user_id),
                            worker_id=worker_id,
                            lease_id=lease_id,
                            progress=prog,
                        )
                    except Exception:
                        pass
                    raise HTTPException(status_code=403, detail="cheat_detected")

            passed = _passes_thresholds(server_metrics, min_trades, min_ret, max_dd, min_sh)
            if passed:
                verified_candidates.append(
                    {
                        "score": float(server_score),
                        "params": {
                            "family": cand_family,
                            "family_params": dict(family_params),
                            "tp": float(tp),
                            "sl": float(sl),
                            "max_hold": int(mh),
                        },
                        "metrics": server_metrics,
                    }
                )
            checked += 1

    except HTTPException:
        raise
    except Exception as e:
        # Verification unavailable: finish the task but drop unverified candidates.
        verified_candidates = []
        try:
            prog = dict(body.final_progress or {})
            prog["last_error"] = "server_verify_unavailable"
            prog["verify_error"] = str(e)
            prog["updated_at"] = _utc_iso()
            body.final_progress = prog
        except Exception:
            pass

    verified_candidates.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
    if len(verified_candidates) > keep_top:
        verified_candidates = verified_candidates[:keep_top]

    best_id = db.finish_task_with_lease(
        task_id=int(task_id),
        user_id=int(user_id),
        worker_id=worker_id,
        lease_id=str(lease_id),
        candidates=verified_candidates,
        final_progress=dict(body.final_progress or {}),
    )

    if best_id is None and verified_candidates:
        raise HTTPException(status_code=409, detail="lease_mismatch_or_task_not_running")
    return {"ok": True, "best_candidate_id": best_id}


# Legacy endpoint kept on purpose: force upgrade.
@app.get("/tasks/next")
def legacy_next_task():
    raise HTTPException(status_code=426, detail={"error": "endpoint_deprecated", "use": "/tasks/claim"})


@app.get("/tasks/{task_id}")
def get_task(task_id: int, request: Request, authorization: Optional[str] = Header(None)):
    _auth_ctx(request, authorization)
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="not_found")
    return dict(task)
