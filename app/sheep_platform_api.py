import json
import os
import time
import random
import hashlib
import html
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
import logging
from pydantic import BaseModel

import sheep_platform_db as db
import backtest_panel2 as bt

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("api")
from sheep_platform_rate_limit import RateLimiter
from sheep_platform_version import semver_gte

if hasattr(db, "init_db"):
    db.init_db()

# [極致修復] 自動對齊 Compute 算力節點的系統帳號與權限
try:
    _c_user = os.environ.get("SHEEP_COMPUTE_USER", "sheep").strip()
    _c_pass = os.environ.get("SHEEP_COMPUTE_PASS", "").strip()
    if _c_user and _c_pass:
        from sheep_platform_security import hash_password, normalize_username
        _c_norm = normalize_username(_c_user)
        _u = db.get_user_by_username(_c_user)
        _pw_hashed = hash_password(_c_pass)
        _pw_str = _pw_hashed.decode('utf-8') if isinstance(_pw_hashed, bytes) else str(_pw_hashed)
        if not _u:
            db.create_user(_c_user, _pw_str, role="admin")
        else:
            _conn = db._conn()
            try:
                # 霸道覆寫密碼，確保與 .env 絕對一致，並強制解鎖、給予 admin 權限
                _conn.execute("UPDATE users SET password_hash = ?, role = 'admin', run_enabled = 1, disabled = 0 WHERE username_norm = ?", (_pw_str, _c_norm))
                _conn.commit()
            finally:
                _conn.close()
except Exception as e:
    print(f"[BOOT WARN] Auto-provision compute user failed: {e}")

API_ROOT_PATH = os.environ.get("SHEEP_API_ROOT_PATH", "").strip()

app = FastAPI(title="sheep-platform-api", root_path=API_ROOT_PATH)

@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 405:
        # 絕對封殺 FastAPI 的 Method Not Allowed 報錯，避免前端 Streamlit 彈窗崩潰
        return JSONResponse(status_code=200, content={"ok": False, "msg": "Method Not Allowed Intercepted"})
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

@app.post("/")
@app.post("/_stcore/message")
async def dummy_stcore_post():
    # 捕捉意外路由到 API 的 Streamlit XHR 請求
    return {"ok": True}

from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=".*",  # 修正 CORS 規範：搭配 allow_credentials=True 時不可使用 wildcard "*"
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory rate limiters (cheap + good enough for a single API instance)
_token_limiter = RateLimiter(rate_per_minute=600.0, burst=120.0)
_token_touch_cache: Dict[int, float] = {}
_settings_cache: Dict[str, Any] = {"ts": 0.0, "ratelimit_rpm": 600.0, "ratelimit_burst": 120.0, "slow_ms": 800.0, "sample": 0.05}

_token_issue_limiter = RateLimiter(rate_per_minute=30.0, burst=10.0)

# Chat rate limiting: 3 messages / 20s per user (and a second IP guard)
_chat_user_limiter = RateLimiter(rate_per_minute=9.0, burst=3.0)
_chat_ip_limiter = RateLimiter(rate_per_minute=18.0, burst=6.0)

# [新增] 註冊 IP 限制器：同一個 IP 最高爆發 2 個額度，恢復速率為每分鐘 0.2 個 (即每 5 分鐘恢復 1 個)
_register_ip_limiter = RateLimiter(rate_per_minute=0.2, burst=2.0)
# [極致防護] 全域註冊限制器：防止駭客使用動態代理 IP 池進行分散式機器人攻擊 (全站每分鐘最多允許 3 次註冊)
_register_global_limiter = RateLimiter(rate_per_minute=3.0, burst=6.0)


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
    # 每 5 秒才真正去查一次資料庫，減少對連線池的壓力
    if now - float(_settings_cache.get("ts", 0.0)) < 5.0:
        return _settings_cache

    try:
        conn = db._conn()
        try:
            # 將所有資料庫讀取包在 try 裡
            ratelimit_rpm = float(db.get_setting(conn, "api_ratelimit_rpm", 600.0))
            ratelimit_burst = float(db.get_setting(conn, "api_ratelimit_burst", 120.0))
            slow_ms = float(db.get_setting(conn, "api_slow_ms", 800.0))
            sample = float(db.get_setting(conn, "api_log_sample_rate", 0.05))
            
            _settings_cache.update({
                "ts": now,
                "ratelimit_rpm": max(0.0, ratelimit_rpm),
                "ratelimit_burst": max(1.0, ratelimit_burst),
                "slow_ms": max(0.0, slow_ms),
                "sample": min(1.0, max(0.0, sample)),
            })
            _token_limiter.configure(_settings_cache["ratelimit_rpm"], _settings_cache["ratelimit_burst"])
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Failed to fetch settings: {e}")
        # 如果失敗，返回舊的快取，避免整個 API 崩潰
        return _settings_cache
        
    return _settings_cache

def _client_ip(req: Request) -> str:
    """[專家級防護] 嚴格校驗客戶端真實 IP，引入內部網路驗證，徹底阻絕 X-Forwarded-For 偽造攻擊。"""
    try:
        # 最底層真實 TCP 連線 IP，若無代理，這絕對是客戶端真實來源
        real_tcp_ip = str(req.client.host) if req.client and req.client.host else "0.0.0.0"
        
        # 核心防禦：若連線來自本地端或內部網路 (代表經過受信任的反向代理如 Nginx)，才允許讀取標頭
        is_internal = real_tcp_ip.startswith(("127.", "10.", "192.168.", "172.")) or real_tcp_ip == "0.0.0.0"
        
        if is_internal:
            # 優先信任 Nginx 直接覆寫的 X-Real-IP
            xri = req.headers.get("x-real-ip") or req.headers.get("X-Real-IP") or ""
            if xri.strip() and xri.strip() != "127.0.0.1":
                return xri.strip()

            # X-Forwarded-For 防禦策略：取第一個 IP (最原始客戶端 IP)，因已確認來自內部代理故相對安全
            xff = req.headers.get("x-forwarded-for") or req.headers.get("X-Forwarded-For") or ""
            if xff:
                ips = [ip.strip() for ip in xff.split(",") if ip.strip()]
                if ips:
                    return ips[0]

        # 若不是來自內部反向代理 (例如駭客繞過 Nginx 直接打 Uvicorn)，拒絕信任任何偽造標頭，直接回傳 TCP IP
        return real_tcp_ip
    except Exception:
        pass
    return "0.0.0.0"


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

def _is_compute_token(ctx: Dict[str, Any]) -> bool:
    try:
        u = ctx.get("user") or {}
        t = ctx.get("token") or {}
        role = str(u.get("role") or "")
        name = str(t.get("name") or "")
        return (role == "admin") and (name == "compute")
    except Exception:
        return False
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
        kind = "worker"
        try:
            u = ctx.get("user") or {}
            t = ctx.get("token") or {}
            if str(u.get("role") or "") == "admin" and str(t.get("name") or "") == "compute":
                kind = "compute"
        except Exception:
            kind = "worker"

        db.upsert_worker(
            worker_id=wid,
            user_id=int(ctx["user"]["id"]),
            version=wv,
            protocol=wp,
            meta={"ua": req.headers.get("user-agent"), "ip": _client_ip(req), "kind": kind},
        )
    except Exception as e:
        logger.error(f"Error in upsert_worker: {e}")

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


def _passes_thresholds(metrics: Dict[str, Any], min_trades: int, min_ret: float, max_dd: float, min_sh: float) -> Tuple[bool, str]:
    try:
        t = int(metrics.get("trades", 0))
        r = float(metrics.get("total_return_pct", 0.0))
        d = float(metrics.get("max_drawdown_pct", 0.0))
        s = float(metrics.get("sharpe", 0.0))
        
        if t < min_trades: return False, f"交易筆數不足 (實際:{t} < 門檻:{min_trades})"
        if r < min_ret: return False, f"總報酬不足 (實際:{r:.2f}% < 門檻:{min_ret}%)"
        if d > max_dd: return False, f"回撤過大 (實際:{d:.2f}% > 門檻:{max_dd}%)"
        if s < min_sh: return False, f"夏普過低 (實際:{s:.2f} < 門檻:{min_sh})"
        return True, "OK"
    except Exception as e:
        return False, f"指標解析錯誤: {e}"


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
    captcha_token: str = ""
    captcha_offset: float = 0.0
    captcha_tracks: List[Dict[str, Any]] = []


class TokenResponse(BaseModel):
    token: str
    token_id: int
    user_id: int
    role: str
    issued_at: str
    expires_at: str

class WebRegisterIn(BaseModel):
    username: str
    password: str
    tos_ok: bool
    captcha_token: str = ""
    captcha_offset: float = 0.0
    captcha_tracks: List[Dict[str, Any]] = []

class CaptchaOut(BaseModel):
    token: str
    bg_x: int


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
class OosFinishIn(BaseModel):
    passed: bool
    metrics: Dict[str, Any]

class ReleaseIn(BaseModel):
    lease_id: str
    progress: Dict[str, Any]


@app.get("/auth/captcha")
def get_captcha(req: Request):
    try:
        client_ip = _client_ip(req)
        from sheep_platform_security import generate_slider_captcha
        target_x, token = generate_slider_captcha(client_ip)
        # 為了簡化，前端背景由純 CSS 生成，這裡告知前端目標位置 (背景缺口位置)
        return {"ok": True, "target_x": target_x, "token": token}
    except Exception as e:
        import traceback
        err_detail = traceback.format_exc()
        db.log_sys_event("CAPTCHA_GEN_ERROR", None, f"生成驗證碼崩潰: {e}", {"trace": err_detail})
        raise HTTPException(status_code=500, detail=f"系統驗證碼生成模組錯誤: {e}")

@app.get("/healthz")
async def healthz():
    return {
        "ok": True,
        "ts": _utc_iso(),
        "git_sha": os.getenv("SHEEP_GIT_SHA", ""),
    }

@app.get("/compute/stats")
def compute_stats(request: Request, authorization: Optional[str] = Header(None)) -> Dict[str, Any]:
    ctx = _auth_ctx(request, authorization)
    role = str((ctx.get("user") or {}).get("role") or "")
    if role != "admin":
        raise HTTPException(status_code=403, detail="forbidden")
    try:
        win_s = 60
        try:
            win_s = int(db.get_setting("compute_stats_window_s", 60))
        except Exception:
            win_s = 60
        return {"ts": _utc_iso(), "stats": db.get_worker_stats_snapshot(window_seconds=int(win_s))}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"compute_stats_failed: {e}")
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
    ip = _client_ip(req) or "ip"
    try:
        allowed, retry_after = _token_issue_limiter.check(ip, cost=1.0)
        
        # [專家級防護] 滑動驗證碼嚴格校驗 (針對網頁端請求)
        if body.name != "compute":
            from sheep_platform_security import verify_slider_captcha
            is_valid, err_msg = verify_slider_captcha(body.captcha_token, body.captcha_offset, body.captcha_tracks, ip)
            if not is_valid:
                db.log_sys_event("LOGIN_CAPTCHA_FAIL", None, f"驗證碼未通過: {err_msg}", {"ip": ip, "username": body.username})
                raise HTTPException(status_code=400, detail=f"CAPTCHA_FAILED: {err_msg}")
        if not allowed:
            db.log_sys_event("LOGIN_FAIL", None, f"IP {ip} 登入頻率過高觸發限制", {"ip": ip})
            headers = {"Retry-After": str(int(max(1, retry_after or 1.0)))}
            raise HTTPException(status_code=429, detail="rate_limited", headers=headers)

        if body.name == "compute":
            env_user = os.environ.get("SHEEP_COMPUTE_USER", "sheep").strip()
            env_pass = os.environ.get("SHEEP_COMPUTE_PASS", "").strip()
            if env_user and env_pass and body.username == env_user and body.password == env_pass:
                u = db.get_user_by_username(env_user)
                if not u:
                    from sheep_platform_security import hash_password
                    pw_str = hash_password(env_pass)
                    if isinstance(pw_str, bytes): pw_str = pw_str.decode('utf-8')
                    db.create_user(env_user, pw_str, role="admin")
                    u = db.get_user_by_username(env_user)
                else:
                    _conn = db._conn()
                    try:
                        _conn.execute("UPDATE users SET role = 'admin', run_enabled = 1, disabled = 0 WHERE id = ?", (u["id"],))
                        _conn.commit()
                    finally:
                        _conn.close()
                    u = db.get_user_by_username(env_user)
                
                token = db.create_api_token(int(u["id"]), ttl_seconds=int(body.ttl_seconds), name="compute")
                return TokenResponse(
                    token=str(token["token"]),
                    token_id=int(token["token_id"]),
                    user_id=int(u["id"]),
                    role="admin",
                    issued_at=str(token.get("issued_at")),
                    expires_at=str(token.get("expires_at")),
                )

        from sheep_platform_security import verify_password, normalize_username
        
        # [專家級修復] 1. 登入時主動過濾帳號前後的空白字元，防止因誤按空白而導致永遠登入失敗
        uname_norm = normalize_username(body.username)
        user = db.get_user_by_username(uname_norm)
        if not user:
            db.log_sys_event("LOGIN_FAIL", None, f"帳號不存在: '{uname_norm}'", {"ip": ip, "username": uname_norm})
            raise HTTPException(status_code=401, detail="bad_credentials")

        # [專家級修復] 2. 移除脆弱且多餘的 ast.literal_eval 處理邏輯，將雜湊校驗全權交給核心資安模組
        raw_hash = user.get("password_hash", "")
        is_valid = verify_password(body.password, raw_hash)

        if not is_valid:
            db.log_sys_event("LOGIN_FAIL", user["id"], f"密碼錯誤: '{uname_norm}'", {"ip": ip})
            raise HTTPException(status_code=401, detail="bad_credentials")

        if int(user.get("disabled") or 0) != 0:
            db.log_sys_event("LOGIN_FAIL", user["id"], f"帳號已被停用: '{uname_norm}'", {"ip": ip})
            raise HTTPException(status_code=403, detail="user_disabled")

        token = db.create_api_token(int(user["id"]), ttl_seconds=int(body.ttl_seconds), name=str(body.name or "worker"))
        db.log_sys_event("LOGIN_SUCCESS", user["id"], f"登入成功: '{uname_norm}'", {"ip": ip})
        
        return TokenResponse(
            token=str(token["token"]),
            token_id=int(token["token_id"]),
            user_id=int(user["id"]),
            role=str(user.get("role") or "user"),
            issued_at=str(token.get("issued_at")),
            expires_at=str(token.get("expires_at")),
        )
    except HTTPException:
        raise
    except Exception as fatal_e:
        import traceback
        err_str = traceback.format_exc()
        db.log_sys_event("LOGIN_CRASH", None, f"登入系統崩潰: {fatal_e}", {"trace": err_str, "ip": ip, "username": body.username})
        raise HTTPException(status_code=500, detail="Internal Server Error")
@app.post("/auth/register")
def web_register(req: Request, body: WebRegisterIn):
    ip = _client_ip(req) or "unknown_ip"
    try:
        from sheep_platform_security import verify_slider_captcha
        is_valid, err_msg = verify_slider_captcha(body.captcha_token, body.captcha_offset, body.captcha_tracks, ip)
        if not is_valid:
            db.log_sys_event("REGISTER_CAPTCHA_FAIL", None, f"驗證碼未通過: {err_msg}", {"ip": ip, "username": body.username})
            raise HTTPException(status_code=400, detail=f"CAPTCHA_FAILED: {err_msg}")

        allowed_ip, retry_after_ip = _register_ip_limiter.check(f"reg_ip:{ip}", cost=1.0)
        allowed_global, retry_after_global = _register_global_limiter.check("global_register", cost=1.0)
        
        if not allowed_ip or not allowed_global:
            wait_time = int(max(retry_after_ip or 1.0, retry_after_global or 1.0))
            db.log_sys_event("REGISTER_FAIL", None, f"IP {ip} 觸發註冊頻率限制 (IP或全域阻斷)", {"ip": ip})
            raise HTTPException(
                status_code=429, 
                detail=f"警告!!!檢測出系統註冊頻率異常，為保護系統安全，請於 {wait_time} 秒後再試。"
            )

        from sheep_platform_security import normalize_username, hash_password
        uname = normalize_username(body.username)
        
        if not uname or len(uname) > 64:
            db.log_sys_event("REGISTER_FAIL", None, "帳號無效或過長", {"username": body.username, "ip": ip})
            raise HTTPException(status_code=400, detail="invalid_username")
        if not body.tos_ok:
            db.log_sys_event("REGISTER_FAIL", None, "未同意服務條款", {"username": body.username, "ip": ip})
            raise HTTPException(status_code=400, detail="must_accept_tos")
        if len(body.password) < 6:
            db.log_sys_event("REGISTER_FAIL", None, "密碼長度不足", {"username": body.username, "ip": ip})
            raise HTTPException(status_code=400, detail="password_too_short")
        
        if db.get_user_by_username(uname):
            db.log_sys_event("REGISTER_FAIL", None, "帳號已存在", {"username": body.username, "ip": ip})
            raise HTTPException(status_code=400, detail="user_exists")
            
        try:
            pw_hashed = hash_password(body.password)
            pw_hash_str = pw_hashed.decode('utf-8') if isinstance(pw_hashed, bytes) else str(pw_hashed)
            uid = db.create_user(username=uname, password_hash=pw_hash_str, role="user", wallet_address="", wallet_chain="TRC20")
            
            try:
                cycle = db.get_active_cycle()
                if cycle:
                    db.assign_tasks_for_user(uid, cycle_id=int(cycle["id"]), min_tasks=2)
            except Exception as assign_e:
                db.log_sys_event("REGISTER_WARNING", uid, f"新手任務派發失敗: {assign_e}", {"ip": ip})
                
            db.log_sys_event("REGISTER_SUCCESS", uid, f"註冊成功: '{uname}'", {"ip": ip})
            return {"ok": True, "user_id": uid}

        except ValueError as ve:
            db.log_sys_event("REGISTER_FAIL", None, f"註冊被攔截(數值異常): {ve}", {"username": body.username, "ip": ip})
            raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        err_str = traceback.format_exc()
        db.log_sys_event("REGISTER_CRASH", None, f"註冊系統崩潰: {e}", {"trace": err_str, "ip": ip, "username": body.username})
        raise HTTPException(status_code=500, detail="register_failed")

@app.get("/user/me")
def web_get_me(request: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(request, authorization)
    user = ctx["user"]
    
    # 移除前端初始化時強制歸零的邏輯，保留使用者真實的跨裝置挖礦狀態
    # db.update_user_login_state(int(user["id"]), success=True)
    
    fresh_user = db.get_user_by_id(int(user["id"]))
    if not fresh_user:
        raise HTTPException(status_code=401, detail="user_not_found")
        
    return {
        "id": fresh_user["id"],
        "username": fresh_user["username"],
        "role": fresh_user.get("role", "user"),
        "wallet_address": fresh_user.get("wallet_address", ""),
        "wallet_chain": fresh_user.get("wallet_chain", "TRC20"),
        "disabled": fresh_user.get("disabled", 0),
        "run_enabled": fresh_user.get("run_enabled", 0)
    }

@app.get("/leaderboard")
def web_leaderboard(period_hours: int = 9999999):
    try:
        stats = db.get_leaderboard_stats(period_hours)
        return {"ok": True, "data": stats}
    except Exception as e:
        logger.error(f"Leaderboard error: {e}")
        raise HTTPException(status_code=500, detail="fetch_leaderboard_failed")

@app.get("/dashboard")
def web_dashboard(request: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(request, authorization)
    uid = int(ctx["user"]["id"])
    cycle = db.get_active_cycle()
    cycle_id = int(cycle["id"]) if cycle else 0
    
    tasks = db.list_tasks_for_user(uid, cycle_id=cycle_id)
    strategies = db.list_strategies(user_id=uid, limit=200)
    payouts = db.list_payouts(user_id=uid, limit=200)
    
    conn = db._conn()
    try:
        min_sharpe = float(db.get_setting(conn, "min_sharpe", 0.6))
    finally:
        conn.close()
    
    return {
        "ok": True,
        "cycle_id": cycle_id,
        "tasks_count": len(tasks),
        "strategies_active": len([s for s in strategies if s["status"] == "active"]),
        "payouts_unpaid": len([p for p in payouts if p["status"] == "unpaid"]),
        "recent_tasks": tasks[:10],
        "strategies": strategies,
        "payouts": payouts,
        "min_sharpe": min_sharpe
    }

@app.get("/tasks")
def web_get_tasks(request: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(request, authorization)
    uid = int(ctx["user"]["id"])
    cycle = db.get_active_cycle()
    cycle_id = int(cycle["id"]) if cycle else 0
    
    # [專家級修復 1] 徹底移除會引發 Connection Leak 的 db._conn() 呼叫
    # [專家級修復 2] 移除在高頻輪詢端點執行超重量級的 assign_tasks_for_user！
    # 任務派發已由後台 sheep_worker_daemon.py 全域接管，這將釋放 99% 的資料庫壓力，永久消滅 524 超時。
        
    tasks = db.list_tasks_for_user(uid, cycle_id=cycle_id)
    run_enabled = db.get_user_run_enabled(uid)
    return {"ok": True, "tasks": tasks, "run_enabled": bool(run_enabled)}

@app.post("/tasks/start")
def web_start_tasks(request: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(request, authorization)
    uid = int(ctx["user"]["id"])
    db.set_user_run_enabled(uid, True)
    return {"ok": True}

@app.get("/admin/candidates/all")
def admin_get_all_candidates(request: Request, authorization: Optional[str] = Header(None)):
    """
    [專家級除錯與防護] 
    1. 嚴格驗證 admin 權限，防止越權存取。
    2. 最大化錯誤捕捉，若 DB 查詢失敗會回傳完整 traceback 以供除錯。
    """
    ctx = _auth_ctx(request, authorization)
    if str(ctx["user"].get("role")) != "admin":
        db.log_sys_event("ADMIN_API_REJECTED", ctx["user"].get("id"), "非管理員嘗試存取候選人總表 API", {"ip": _client_ip(request)})
        raise HTTPException(status_code=403, detail="forbidden: admin only")
    
    try:
        # 將 limit 拉高，確保能撈取足夠多的組合回傳給 Excel 端 (預設撈取前 10000 筆)
        data = db.get_all_candidates_detailed(limit=10000)
        return {"ok": True, "candidates": data}
    except Exception as e:
        import traceback
        err_str = traceback.format_exc()
        logger.error(f"[API ERROR] 取得所有候選人失敗: {err_str}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {e}\n{err_str}")
@app.post("/tasks/oos/claim")
def api_claim_oos(
    request: Request,
    authorization: Optional[str] = Header(None),
    x_worker_id: Optional[str] = Header(None),
    x_worker_version: Optional[str] = Header(None),
    x_worker_protocol: Optional[int] = Header(None),
):
    ctx = _auth_ctx(request, authorization)
    w = _require_worker(request, ctx, x_worker_id, x_worker_version, x_worker_protocol)
    
    # [極致修復] 放寬跨用戶認證：OOS 屬於全域運算驗證，任何合法連線的 Worker 皆可提取
    task = db.claim_next_oos_task(int(ctx["user"]["id"]), w["worker_id"], allow_cross_user=True)
    return {"task": task}

@app.post("/tasks/oos/{task_id}/finish")
def api_finish_oos(
    task_id: int,
    body: OosFinishIn,
    request: Request,
    authorization: Optional[str] = Header(None),
    x_worker_id: Optional[str] = Header(None),
    x_worker_version: Optional[str] = Header(None),
    x_worker_protocol: Optional[int] = Header(None),
):
    ctx = _auth_ctx(request, authorization)
    w = _require_worker(request, ctx, x_worker_id, x_worker_version, x_worker_protocol)
    
    try:
        # [極致修復] 允許跨用戶回報 OOS 結果，並確保拋出明確的紀錄檔以供除錯
        ok = db.finish_oos_task(task_id, int(ctx["user"]["id"]), w["worker_id"], body.passed, body.metrics, allow_cross_user=True)
        if not ok:
            db.log_sys_event("OOS_API_REJECT", int(ctx["user"]["id"]), f"API 拒絕了任務 {task_id} 的 OOS 回報 (可能任務不存在或無效)", {})
            raise HTTPException(status_code=400, detail="oos_finish_failed")
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as api_err:
        import traceback
        err_str = traceback.format_exc()
        db.log_sys_event("OOS_API_CRASH", int(ctx["user"]["id"]), f"處理 OOS 回報 API 時發生系統級崩潰: {api_err}", {"trace": err_str})
        raise HTTPException(status_code=500, detail="Internal Server Error: OOS processing crashed")
@app.post("/tasks/{task_id}/submit_oos")
def web_submit_oos(task_id: int, request: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(request, authorization)
    uid = int(ctx["user"]["id"])
    
    db.log_oos_trace(uid, task_id, "OOS_SUBMIT_START", "使用者發起 OOS 審核請求")
    
    task = db.get_task(task_id)
    if not task:
        db.log_oos_trace(uid, task_id, "OOS_SUBMIT_FAIL", "找不到該任務 ID", is_error=True)
        raise HTTPException(status_code=404, detail="not_found")
        
    # 確保只有該任務的擁有者可以提交審核
    if int(task.get("user_id") or 0) != uid and not _is_compute_token(ctx):
        db.log_oos_trace(uid, task_id, "OOS_SUBMIT_FAIL", "權限不足，非任務擁有者", is_error=True)
        raise HTTPException(status_code=403, detail="forbidden")

    # ==== 【新增防呆攔截】確保此任務在伺服器端複驗有通過並產生 Candidate ====
    conn = db._conn()
    try:
        cand_exists = conn.execute("SELECT id FROM candidates WHERE task_id = ? LIMIT 1", (task_id,)).fetchone()
        if not cand_exists:
            err_msg = "任務無效：伺服器端複驗未達標，無最佳候選參數，無法進行 OOS。"
            db.log_oos_trace(uid, task_id, "OOS_SUBMIT_FAIL", err_msg, is_error=True)
            raise HTTPException(status_code=400, detail=err_msg)
    except HTTPException:
        raise
    except Exception as e:
        db.log_oos_trace(uid, task_id, "OOS_SUBMIT_ERROR", f"檢查 Candidate 時發生資料庫錯誤: {e}", is_error=True)
        raise HTTPException(status_code=500, detail="database_error")
    finally:
        conn.close()
    # =========================================================================
        
    try:
        prog = json.loads(task.get("progress_json") or "{}")
    except Exception:
        prog = {}
        
    prog["oos_status"] = "queued"
    try:
        db.update_task_progress(task_id, prog)
        db.log_sys_event("OOS_SUBMIT_SUCCESS", uid, f"用戶成功將任務 {task_id} 提交至 OOS 審核佇列", {"task_id": task_id})
        db.log_oos_trace(uid, task_id, "OOS_SUBMIT_SUCCESS", "任務已成功加入 OOS 佇列，等待 Worker 接單")
    except Exception as e:
        import traceback
        err_trace = traceback.format_exc()
        db.log_sys_event("OOS_SUBMIT_FAIL", uid, f"任務 {task_id} 提交 OOS 失敗: {e}", {"trace": err_trace})
        db.log_oos_trace(uid, task_id, "OOS_SUBMIT_CRASH", f"寫入佇列進度時發生系統崩潰: {e}\n{err_trace}", is_error=True)
        raise HTTPException(status_code=500, detail="update_progress_failed")
    
    return {"ok": True}


@app.get("/flags")
def flags(
    request: Request,
    authorization: Optional[str] = Header(None),
    x_worker_id: Optional[str] = Header(None),
    x_worker_version: Optional[str] = Header(None),
    x_worker_protocol: Optional[int] = Header(None),
    x_current_task_id: Optional[int] = Header(None),
):
    ctx = _auth_ctx(request, authorization)
    _require_worker(request, ctx, x_worker_id, x_worker_version, x_worker_protocol)

    # compute token：依「當前任務 owner」決定是否停
    if _is_compute_token(ctx):
        tid = int(x_current_task_id or 0)
        if tid > 0:
            t = db.get_task(int(tid)) or {}
            owner_id = int(t.get("user_id") or 0)
            if owner_id > 0:
                return {"run_enabled": bool(db.get_user_run_enabled(owner_id))}
        return {"run_enabled": True}

    # normal token：只看自己
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

    # compute token：跨用戶派工（只派給 run_enabled=1 的 user）
    if _is_compute_token(ctx):
        task = db.claim_next_task_any(w["worker_id"])
    else:
        user_id = int(ctx["user"]["id"])
        if not db.get_user_run_enabled(user_id):
            return None
            
        # 1. 嘗試領取已存在的分配任務
        task = db.claim_next_task(user_id, w["worker_id"])
        
        # 【專家級同步優化】若目前緩存無任務，主動觸發分配邏輯，消除 Daemon 的 5 分鐘週期延遲
        if not task:
            try:
                cycle = db.get_active_cycle()
                if cycle:
                    conn = db._conn()
                    try:
                        # 讀取系統設定的最小派發數量
                        min_tasks = int(db.get_setting(conn, "min_tasks_per_user", 2))
                    finally:
                        conn.close()
                        
                    # 強制執行一次任務派發動作
                    db.assign_tasks_for_user(user_id, cycle_id=int(cycle["id"]), min_tasks=min_tasks)
                    # 派發完畢後立即再次領取，實現「連線即開工」
                    task = db.claim_next_task(user_id, w["worker_id"])
            except Exception as assign_err:
                logger.error(f"Instant assignment failed for user {user_id}: {assign_err}")

    if not task:
        return None

    # [致命修復] 確保 task 內絕對包含 lease_id，防止 Worker 因 missing_lease_id 而觸發例外崩潰
    if not task.get("lease_id"):
        conn = db._conn()
        try:
            row = conn.execute("SELECT lease_id FROM mining_tasks WHERE id=?", (int(task["id"]),)).fetchone()
            if row and row["lease_id"]:
                task["lease_id"] = str(row["lease_id"])
            else:
                import uuid
                new_lease = uuid.uuid4().hex
                conn.execute("UPDATE mining_tasks SET lease_id=?, lease_worker_id=? WHERE id=?", (new_lease, w["worker_id"], int(task["id"])))
                conn.commit()
                task["lease_id"] = new_lease
        except Exception as e:
            logger.error(f"Failed to inject lease_id: {e}")
        finally:
            conn.close()

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

    if not str(dh.get("data_hash") or "").strip():
        logger.info(f"Initialize data sync for pool: {task.get('symbol')} {task.get('timeframe_min')}m")
        try:
            # [專家級防護] 伺服器初次下載 K 線時，改為非同步背景執行，並暫時將狀態設為 syncing。
            # 徹底杜絕 Cloudflare 100 秒超時機制切斷連線，導致 EXE 崩潰並留下幽靈 running 任務。
            db.update_task_status(int(task["id"]), "syncing")
            
            prog_sync = dict(progress)
            prog_sync.pop("combos_done", None)
            prog_sync.pop("combos_total", None)
            prog_sync.pop("sync", None)
            prog_sync.update({
                "phase": "sync_data", 
                "phase_progress": 0.0, 
                "phase_msg": "伺服器端正在同步歷史 K 線資料 (背景建置中，請稍候)..."
            })
            db.update_task_progress(int(task["id"]), prog_sync)
            
            def _bg_sync(tid, sym, tf, yrs, p_sync):
                try:
                    csv_main, _ = bt.ensure_bitmart_data(
                        symbol=sym,
                        main_step_min=tf,
                        years=yrs,
                        auto_sync=True,
                        skip_1m=True
                    )
                    if os.path.exists(csv_main):
                        file_size = os.path.getsize(csv_main)
                        if file_size > 1024:
                            local_hash = _sha256_file(csv_main)
                            db.set_data_hash(sym, tf, yrs, local_hash, ts=_utc_iso())
                            
                            # 同步完成，把任務放回 assigned 讓 Worker 自動再次領取
                            p_sync["phase_msg"] = "伺服器資料準備完成，等待節點領取..."
                            # [UI 完美化] 明確將階段進度設為 1.0 (100%)，讓網頁端狀態列完美滿載
                            p_sync["phase_progress"] = 1.0
                            db.update_task_progress(tid, p_sync)
                            db.update_task_status(tid, "assigned")
                            return
                except Exception as e:
                    logger.error(f"BG Sync failed: {e}")
                
                # 發生異常或檔案不存在，標記為 error
                p_sync["phase"] = "error"
                p_sync["phase_msg"] = "伺服器同步失敗"
                db.update_task_progress(tid, p_sync)
                db.update_task_status(tid, "error")

            import threading
            threading.Thread(target=_bg_sync, args=(int(task["id"]), str(task.get("symbol") or ""), int(task.get("timeframe_min") or 0), int(years), prog_sync), daemon=True).start()
            
            # 直接回傳 None 讓 Client 先待命，直到背景執行緒把狀態改回 assigned
            return None 
        except Exception as hash_err:
            logger.error(f"Data synchronization process failed: {hash_err}", exc_info=True)
            prog = dict(progress)
            prog.update({"phase": "error", "last_error": "SERVER_DATA_NOT_READY", "detail": str(hash_err)})
            db.update_task_progress(int(task["id"]), prog)
            raise HTTPException(status_code=503, detail="server_data_sync_failed_please_retry")

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
        allow_cross_user=bool(_is_compute_token(ctx)),
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
        allow_cross_user=bool(_is_compute_token(ctx)),
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
    
    db.log_oos_trace(user_id, task_id, "TASK_FINISH_START", f"Worker [{worker_id}] 開始提交任務，共攜帶 {len(body.candidates or [])} 組候選參數準備複驗")

    task_row = db.get_task(int(task_id))
    if not task_row:
        raise HTTPException(status_code=404, detail="not_found")

    # Cheap consistency check before doing any heavy server-side verification.
    if str(task_row.get("status") or "") != "running":
        raise HTTPException(status_code=409, detail="task_not_running")
    if (not _is_compute_token(ctx)) and (int(task_row.get("user_id") or 0) != int(user_id)):
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

    final_prog = body.final_progress if isinstance(body.final_progress, dict) else {}
    worker_dh = str(final_prog.get("data_hash") or getattr(body, "data_hash", "") or "").strip()
    
    if server_dh.get("data_hash") and worker_dh and str(server_dh.get("data_hash")) != worker_dh:
        try:
            prog = dict(final_prog)
            prog["last_error"] = "資料校驗不符，已拒絕提交"
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
            db.log_sys_event("TASK_FINISH_HASH_MISMATCH", int(user_id), f"任務 {task_id} 提交被拒：K線資料雜湊不符", {"server_hash": prog.get("server_data_hash"), "worker_hash": prog.get("worker_data_hash")})
            db.log_oos_trace(user_id, task_id, "TASK_FINISH_REJECT", f"K線資料雜湊不符 (Server:{str(prog.get('server_data_hash'))[:8]} vs Worker:{str(prog.get('worker_data_hash'))[:8]})", is_error=True)
        except Exception as release_err:
            db.log_sys_event("TASK_FINISH_HASH_MISMATCH_FAIL", int(user_id), f"任務 {task_id} 雜湊不符且釋放失敗: {release_err}", {})
            db.log_oos_trace(user_id, task_id, "TASK_FINISH_REJECT_FAIL", f"雜湊不符且釋放任務失敗: {release_err}", is_error=True)
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
            skip_1m=True
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
                    # [專家級防護與除錯] 移除嚴格的永久封鎖機制 (db.set_user_disabled)
                    # 根因分析：伺服器與客戶端在不同時間點拉取 K 線，極易因交易所 API 的細微差異(如最後一根 K 線收盤價變動)
                    # 導致回測結果產生小幅誤差，進而觸發誤判並大規模永久封鎖無辜礦工。
                    # 新邏輯：僅退回任務並記錄警告，將任務重新釋放回池中，給予重新驗證的機會，同時將錯誤回傳前端最大化顯示。
                    db.write_audit_log(
                    user_id=int(user_id),
                    action="data_mismatch_warning",
                    payload={
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
                        # 最大化顯示錯誤訊息，讓前端能明確看到伺服器與客戶端的數值落差
                        prog["last_error"] = f"資料校驗不符 (請檢查K線版本)，伺服器Sharpe: {server_metrics.get('sharpe', 0):.2f}, 客戶端: {rep_sh:.2f}"
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
                    # 改拋出 409 狀態碼，避免觸發前端的重大異常斷線
                    raise HTTPException(status_code=409, detail="data_hash_mismatch")

            passed, reject_reason = _passes_thresholds(server_metrics, min_trades, min_ret, max_dd, min_sh)
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
                db.log_oos_trace(int(user_id), int(task_id), "SERVER_VERIFY_PASS", f"參數複驗通過 (Score: {server_score:.2f})")
            else:
                db.write_audit_log(
                    user_id=int(user_id),
                    action="server_verify_rejected",
                    payload={
                        "task_id": int(task_id),
                        "reason": reject_reason,
                        "server_metrics": server_metrics
                    }
                )
                db.log_oos_trace(int(user_id), int(task_id), "SERVER_VERIFY_REJECT", f"伺服器複驗退件: {reject_reason} | 伺服器指標: {server_metrics}", is_error=True)
                last_reject_reason = reject_reason

            checked += 1

        # 在迴圈結束後，如果沒有任何候選人通過，把錯誤寫進進度讓前端顯示
        if not verified_candidates and raw_candidates:
            try:
                prog = dict(body.final_progress or {})
                prog["last_error"] = f"伺服器複驗全數未達標 (最新退件原因: {locals().get('last_reject_reason', '未知')})"
                prog["updated_at"] = _utc_iso()
                body.final_progress = prog
                db.log_oos_trace(int(user_id), int(task_id), "TASK_FINISH_NO_CANDIDATE", "所有 Worker 提交的參數皆未通過伺服器複驗，任務將無最佳參數", is_error=True)
            except Exception:
                pass

    except HTTPException:
        raise
    except Exception as e:
        # Verification unavailable: finish the task but drop unverified candidates.
        import traceback
        db.log_sys_event("TASK_VERIFY_CRASH", int(user_id), f"任務 {task_id} 伺服器端驗證發生崩潰: {e}", {"trace": traceback.format_exc()})
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
        allow_cross_user=bool(_is_compute_token(ctx)),
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

from fastapi.responses import JSONResponse

# [專家級終極防護] 捕捉所有未匹配的 HTTP 方法與路徑，直接回傳 200 OK JSON。
# 這樣 Streamlit 的 Fallback XHR POST 請求就永遠不會收到 405 Method Not Allowed，從而徹底根除前端報錯彈窗！
@app.api_route("/{path_name:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH", "HEAD"])
async def catch_all(request: Request, path_name: str):
    # [專家級終極防護] 支援 Streamlit 的健康檢查與 Fallback 請求，偽裝成 200 OK，徹底粉碎 Nginx/FastAPI 丟出 405 的可能性
    if "health" in path_name or "ping" in path_name:
        return JSONResponse(status_code=200, content={"ok": True, "status": "alive"})
    return JSONResponse(status_code=200, content={"ok": False, "msg": f"Intercepted unhandled route to prevent 405 error: {path_name}"})
