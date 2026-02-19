import json
import os
import time
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

import sheep_platform_db as db
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
    family: str
    partition_idx: int
    num_partitions: int
    seed: int
    grid_spec: Dict[str, Any]
    risk_spec: Dict[str, Any]
    lease_id: str
    progress: Dict[str, Any]


class ProgressIn(BaseModel):
    lease_id: str
    progress: Dict[str, Any]


class FinishIn(BaseModel):
    lease_id: str
    candidates: List[Dict[str, Any]]
    final_progress: Dict[str, Any]


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
            "keep_top_n": int(db.get_setting(conn, "keep_top_n", 30)),
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
            "keep_top_n": int(db.get_setting(conn, "keep_top_n", 30)),
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

    return TaskOut(
        task_id=int(task["id"]),
        pool_id=int(task["pool_id"]),
        pool_name=str(task.get("pool_name") or ""),
        symbol=str(task.get("symbol") or ""),
        timeframe_min=int(task.get("timeframe_min") or 0),
        family=str(task.get("family") or ""),
        partition_idx=int(task.get("partition_idx") or 0),
        num_partitions=int(task.get("num_partitions") or 1),
        seed=int(task.get("seed") or 0),
        grid_spec=grid_spec,
        risk_spec=risk_spec,
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

    best_id = db.finish_task_with_lease(
        task_id=int(task_id),
        user_id=int(ctx["user"]["id"]),
        worker_id=w["worker_id"],
        lease_id=str(body.lease_id),
        candidates=body.candidates,
        final_progress=body.final_progress,
    )
    if best_id is None and body.candidates:
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
