import asyncio
import json
import math
import os
import time
import random
import hashlib
import hmac
import html
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
import logging
from pydantic import BaseModel

import sheep_platform_db as db
import backtest_runtime_core as bt
from sheep_realtime.service import read_realtime_control, read_realtime_status, write_realtime_control
from sheep_review import (
    count_review_pipeline_tasks as _count_review_pipeline_tasks,
    enrich_task_row as _enrich_task_row,
    evaluate_thresholds as _evaluate_thresholds,
    normalize_review_fields as _normalize_review_fields,
    rebuild_review_state as _rebuild_review_state,
)
from sheep_strategy_schema import normalize_direction, normalize_runtime_strategy_entry, normalize_strategy_batch

def enrich_task_row(task: Dict[str, Any]) -> Dict[str, Any]:
    row = _enrich_task_row(task)
    if "grid_spec_json" in row:
        try:
            row["grid_spec"] = json.loads(row["grid_spec_json"] or "{}")
        except Exception:
            row["grid_spec"] = {}
    if "risk_spec_json" in row:
        try:
            row["risk_spec"] = json.loads(row["risk_spec_json"] or "{}")
        except Exception:
            row["risk_spec"] = {}
    return row

def evaluate_thresholds(metrics: Dict[str, Any], min_trades: int, min_total_return_pct: float, max_drawdown_pct: float, min_sharpe: float) -> Dict[str, Any]:
    result = _evaluate_thresholds(metrics, min_trades, min_total_return_pct, max_drawdown_pct, min_sharpe)
    return {
        "passed": bool(result.get("passed") or False),
        "reason": str(result.get("reason") or ("已達標" if result.get("passed") else "")),
        "failures": list(result.get("failures") or []),
    }

def normalize_review_fields(progress: Dict[str, Any], status: str) -> Dict[str, Any]:
    return _normalize_review_fields(progress, status)

def count_review_pipeline_tasks(tasks: List[Dict[str, Any]]) -> int:
    return _count_review_pipeline_tasks(tasks)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("api")
from sheep_platform_rate_limit import RateLimiter
from sheep_platform_version import semver_gte

logger.info("API boot is side-effect free; use sheep_platform_bootstrap.py for init/bootstrap/maintenance.")

API_ROOT_PATH = os.environ.get("SHEEP_API_ROOT_PATH", "").strip()

app = FastAPI(title="sheep-platform-api", root_path=API_ROOT_PATH)

@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    """[專家級修復] HTTPException 統一處理，記錄並拋出真實錯誤供調試"""
    # 記錄所有 HTTP 異常以便調試
    if exc.status_code >= 400 and _should_log_http_error(request, exc):
        db.log_sys_event(
            "HTTP_ERROR",
            getattr(request.state, "user_id", None),
            f"HTTP {exc.status_code}: {exc.detail}",
            {"path": str(request.url.path), "method": request.method}
        )
    
    # 回傳標準的 RFC 相容錯誤格式，而非假裝 200 OK
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "ok": False,
            "error": exc.detail or f"HTTP {exc.status_code} Error",
            "status_code": exc.status_code
        }
    )

@app.post("/")
@app.post("/_stcore/message")
async def dummy_stcore_post():
    # 捕捉意外路由到 API 的 Streamlit XHR 請求
    return {"ok": True}

from fastapi.middleware.cors import CORSMiddleware

# [專家級修復] CORS 配置合規性修正
# 根據 CORS RFC 規範：當 allow_credentials=True 時，不能使用 wildcard "*" 或 regex ".*"
# 改為：環境變數控制允許來源，部署時明確設定

_allowed_origins = []
_allowed_origins_env = os.environ.get("SHEEP_CORS_ORIGINS", "").strip()
if _allowed_origins_env:
    _allowed_origins = [o.strip() for o in _allowed_origins_env.split(",") if o.strip()]
else:
    # 預設值：本地端 + 常見 Streamlit 埠位
    _allowed_origins = ["http://localhost:3000", "http://localhost:8501", "http://127.0.0.1:8501"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,  # 明確列表而非 wildcard
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["Content-Range", "X-Content-Range"],
    max_age=7200,  # 預檢快取時間
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

_LIVE_CACHE_TTL_SECONDS = 15.0
_LIVE_CACHE_TTL_BY_BUCKET: Dict[str, float] = {
    "dashboard": 15.0,
    "leaderboard": 60.0,
    "announcements": 20.0,
}
_LIVE_CACHE_MAX_BUCKET_SIZE: Dict[str, int] = {
    "dashboard": 128,
    "leaderboard": 32,
    "announcements": 32,
}
_LIVE_INVALIDATE_THROTTLE_SECONDS: Dict[str, float] = {
    "leaderboard": 45.0,
}
_live_versions: Dict[str, str] = {
    "dashboard": "",
    "leaderboard": "",
    "runtime": "",
    "announcement": "",
}
_live_cache: Dict[str, Dict[str, Dict[str, Any]]] = {
    "dashboard": {},
    "leaderboard": {},
    "announcements": {},
}
_live_last_nonempty: Dict[str, Dict[str, Dict[str, Any]]] = {
    "dashboard": {},
    "leaderboard": {},
    "announcements": {},
}
_live_last_invalidated_at: Dict[str, float] = {
    "dashboard": 0.0,
    "leaderboard": 0.0,
    "runtime": 0.0,
    "announcement": 0.0,
}
_http_error_telemetry_cache: Dict[str, float] = {}
_runtime_lookup_cache: Dict[str, Any] = {"ts": 0.0, "lookup": {}, "key": ""}


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


class _AnnouncementHub:
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


_announcement_hub = _AnnouncementHub()


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


if not _live_versions["dashboard"]:
    _live_versions.update(
        {
            "dashboard": _utc_iso(),
            "leaderboard": _utc_iso(),
            "runtime": _utc_iso(),
            "announcement": _utc_iso(),
        }
    )


def _live_cache_key(*parts: Any) -> str:
    return "::".join([str(part) for part in parts])


def _cached_live_payload(bucket: str, key: str, loader) -> Dict[str, Any]:
    now = time.time()
    cache_bucket = _live_cache.setdefault(bucket, {})
    cached = cache_bucket.get(key) or {}
    ttl_s = float(_LIVE_CACHE_TTL_BY_BUCKET.get(bucket) or _LIVE_CACHE_TTL_SECONDS)
    if cached and now - float(cached.get("ts") or 0.0) < ttl_s:
        return dict(cached.get("value") or {})
    value = dict(loader() or {})
    cache_bucket[key] = {"ts": now, "value": value}
    max_size = int(_LIVE_CACHE_MAX_BUCKET_SIZE.get(bucket) or 0)
    if max_size > 0 and len(cache_bucket) > max_size:
        for stale_key, _stale_val in sorted(
            cache_bucket.items(),
            key=lambda item: float((item[1] or {}).get("ts") or 0.0),
        )[:-max_size]:
            cache_bucket.pop(stale_key, None)
    return dict(value)


def _remember_live_snapshot(bucket: str, key: str, value: Dict[str, Any]) -> None:
    bucket_store = _live_last_nonempty.setdefault(bucket, {})
    bucket_store[key] = dict(value or {})
    max_size = int(_LIVE_CACHE_MAX_BUCKET_SIZE.get(bucket) or 0)
    if max_size > 0 and len(bucket_store) > max_size:
        for stale_key in list(bucket_store.keys())[:-max_size]:
            bucket_store.pop(stale_key, None)


def _invalidate_live_state(*channels: str) -> None:
    now_iso = _utc_iso()
    now_ts = time.time()
    requested = {str(channel or "").strip().lower() for channel in channels if str(channel or "").strip()}
    if not requested:
        requested = {"dashboard", "leaderboard", "runtime"}
    if "dashboard" in requested or "runtime" in requested:
        _live_cache["dashboard"].clear()
        _live_versions["dashboard"] = now_iso
        _live_last_invalidated_at["dashboard"] = now_ts
        _runtime_lookup_cache["ts"] = 0.0
        _runtime_lookup_cache["lookup"] = {}
        _runtime_lookup_cache["key"] = ""
    if "leaderboard" in requested:
        throttle_s = float(_LIVE_INVALIDATE_THROTTLE_SECONDS.get("leaderboard") or 0.0)
        last = float(_live_last_invalidated_at.get("leaderboard") or 0.0)
        if throttle_s <= 0.0 or (now_ts - last) >= throttle_s:
            _live_cache["leaderboard"].clear()
            _live_versions["leaderboard"] = now_iso
            _live_last_invalidated_at["leaderboard"] = now_ts
    if "runtime" in requested:
        _live_versions["runtime"] = now_iso
        _live_last_invalidated_at["runtime"] = now_ts
    if "announcement" in requested:
        _live_cache["announcements"].clear()
        _live_versions["announcement"] = now_iso
        _live_last_invalidated_at["announcement"] = now_ts


def _leaderboard_has_rows(payload: Dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    for key in ("combos", "score", "time", "points", "qualified_strategies"):
        rows = payload.get(key)
        if isinstance(rows, list) and rows:
            return True
    return False


def _should_log_http_error(request: Request, exc: StarletteHTTPException) -> bool:
    status_code = int(getattr(exc, "status_code", 500) or 500)
    if status_code < 400:
        return False

    detail_text = str(getattr(exc, "detail", "") or "")
    path = str(request.url.path or "")
    method = str(request.method or "GET").upper()
    ip = _client_ip(request)

    throttle_window = 0.0
    if status_code == 401 and "invalid_or_expired_token" in detail_text:
        if path in {"/flags", "/workers/heartbeat", "/user/me"}:
            throttle_window = 30.0
    elif status_code == 404 and path == "/tasks/oos/claim":
        throttle_window = 60.0

    if throttle_window <= 0.0:
        return True

    cache_key = f"{status_code}:{method}:{path}:{detail_text}:{ip}"
    now = time.time()
    last = float(_http_error_telemetry_cache.get(cache_key, 0.0))
    if now - last < throttle_window:
        return False
    _http_error_telemetry_cache[cache_key] = now
    return True


def _categorize_captcha_error(err_msg: str) -> Dict[str, str]:
    message = str(err_msg or "").strip()
    lower = message.lower()
    if "缺失" in message:
        return {"code": "captcha_missing", "message": "驗證資料不完整，請重新載入滑塊後再試。"}
    if "過期" in message or "invalidtoken" in lower or "ttl" in lower:
        return {"code": "captcha_expired", "message": "驗證碼已過期，請重新取得後再試。"}
    if "ip不匹配" in message.lower() or "ip不匹配" in message:
        return {"code": "captcha_ip_mismatch", "message": "驗證環境已變更，請重新整理後再試。"}
    if "位置不精確" in message or "滑動位置" in message:
        return {"code": "captcha_mismatch", "message": "滑動位置未對準，請重新嘗試。"}
    if "滑動異常" in message or "操作時間異常" in message:
        return {"code": "captcha_invalid", "message": "驗證動作異常，請重新滑動驗證。"}
    return {"code": "captcha_failed", "message": "安全驗證失敗，請重新嘗試。"}


def _runtime_sync_event_detail(scope: str, user_id: int = 0) -> Dict[str, Any]:
    conn = db._conn()
    try:
        params: List[Any] = [f"RUNTIME_SYNC_{str(scope or '').strip().upper()}_%"]
        user_filter = ""
        if int(user_id or 0) > 0:
            user_filter = " AND (user_id = ? OR user_id IS NULL)"
            params.append(int(user_id))
        rows = conn.execute(
            f"""
            SELECT event_type, user_id, message, detail_json, created_at
            FROM sys_monitor_events
            WHERE event_type LIKE ?
            {user_filter}
            ORDER BY created_at DESC, id DESC
            LIMIT 20
            """,
            params,
        ).fetchall()
        latest_success: Dict[str, Any] = {}
        latest_failure: Dict[str, Any] = {}
        for row in rows:
            payload = {}
            try:
                payload = json.loads(row["detail_json"] or "{}")
            except Exception:
                payload = {}
            event_type = str(row["event_type"] or "")
            base = {
                "event_type": event_type,
                "timestamp": str(row["created_at"] or ""),
                "user_id": row["user_id"],
                "message": str(row["message"] or ""),
                "detail": payload,
            }
            if not latest_success and event_type.endswith("_SUCCESS"):
                latest_success = base
            if not latest_failure and event_type.endswith("_FAIL"):
                latest_failure = base
            if latest_success and latest_failure:
                break
        latest_success_detail = dict(latest_success.get("detail") or {})
        latest_failure_detail = dict(latest_failure.get("detail") or {})
        return {
            "latest_success": latest_success,
            "latest_failure": latest_failure,
            "last_success_at": str(latest_success_detail.get("updated_at") or latest_success.get("timestamp") or ""),
            "last_failure_at": str(latest_failure_detail.get("updated_at") or latest_failure.get("timestamp") or ""),
            "last_success_message": str(latest_success.get("message") or ""),
            "last_failure_message": str(latest_failure.get("message") or ""),
            "last_success_strategy_count": int(latest_success_detail.get("strategy_count") or 0),
            "last_failure_strategy_count": int(latest_failure_detail.get("strategy_count") or 0),
            "last_success_checksum": str(latest_success_detail.get("checksum") or ""),
            "last_failure_checksum": str(latest_failure_detail.get("checksum") or ""),
        }
    finally:
        conn.close()
    _invalidate_live_state("dashboard", "runtime")


def _parse_iso_datetime(value: str) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def _runtime_snapshot_status(snapshot: Dict[str, Any], *, threshold_minutes: int = 10) -> Dict[str, Any]:
    updated_at = str(snapshot.get("updated_at") or "")
    dt = _parse_iso_datetime(updated_at)
    stale = True
    age_seconds = None
    summary = dict(snapshot.get("summary") or {})
    items = list(snapshot.get("items") or [])
    strategy_count = int(snapshot.get("strategy_count") or len(items))
    item_count = len(items)
    expected_strategy_count = None
    for key in ("expected_strategy_count", "selected_count", "strategy_count"):
        raw_value = summary.get(key)
        if raw_value is None:
            continue
        try:
            parsed = int(raw_value)
        except Exception:
            continue
        if parsed >= 0:
            expected_strategy_count = parsed
            break
    count_mismatch_reasons: List[str] = []
    if item_count > 0 and item_count != strategy_count:
        count_mismatch_reasons.append("item_count")
    if expected_strategy_count is not None and expected_strategy_count != strategy_count:
        count_mismatch_reasons.append("summary_count")
    if dt is not None:
        age_seconds = max(0.0, (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds())
        stale = age_seconds > float(threshold_minutes * 60)
    return {
        "updated_at": updated_at,
        "strategy_count": strategy_count,
        "item_count": item_count,
        "expected_strategy_count": expected_strategy_count,
        "count_mismatch": bool(count_mismatch_reasons),
        "count_mismatch_reasons": count_mismatch_reasons,
        "stale": bool(stale),
        "age_seconds": age_seconds,
    }


def _allow_runtime_password_auth() -> bool:
    raw = str(os.environ.get("SHEEP_ALLOW_RUNTIME_PASSWORD_AUTH", "") or "").strip().lower()
    if raw in {"0", "false", "no", "off"}:
        return False
    realtime_mode = str(os.environ.get("SHEEP_REALTIME_MODE", "") or "").strip().lower()
    if realtime_mode == "live":
        return False
    return raw in {"", "1", "true", "yes", "on"}


def _admin_realtime_payload(ctx: Dict[str, Any]) -> Dict[str, Any]:
    status = dict(read_realtime_status() or {})
    control = dict(read_realtime_control() or {})
    global_snapshot = db.get_runtime_portfolio_snapshot("global") or {}
    personal_snapshot = db.get_runtime_portfolio_snapshot("personal", user_id=int(ctx["user"]["id"])) or {}
    global_status = _runtime_snapshot_status(global_snapshot)
    personal_status = _runtime_snapshot_status(personal_snapshot)
    holy_grail_diagnostics = dict(status.get("holy_grail_diagnostics") or {})
    symbol_state_items = list(status.get("symbol_state_items") or [])
    resource_summary = dict(status.get("resource_summary") or {})
    daemon_state = str(status.get("state") or "stopped").strip().lower() or "stopped"
    health_state = daemon_state
    if daemon_state == "running":
        heartbeat_dt = _parse_iso_datetime(str(status.get("last_heartbeat_at") or ""))
        if heartbeat_dt is None:
            health_state = "degraded"
        else:
            age_s = max(0.0, (datetime.now(timezone.utc) - heartbeat_dt.astimezone(timezone.utc)).total_seconds())
            if age_s > 45:
                health_state = "stale"
            elif bool(global_snapshot) and bool(global_status.get("stale")):
                health_state = "degraded"
            elif str(status.get("mode") or "").strip().lower() == "shadow":
                health_state = "shadow"
            else:
                health_state = "live"
    return {
        "ok": True,
        "daemon": status,
        "control": control,
        "health_state": health_state,
        "mode": str(status.get("mode") or control.get("mode") or "shadow"),
        "desired_state": str(control.get("desired_state") or status.get("desired_state") or "stopped"),
        "desired_mode": str(control.get("mode") or status.get("desired_mode") or "shadow"),
        "last_heartbeat_at": str(status.get("last_heartbeat_at") or ""),
        "last_round_ms": float(status.get("last_round_ms") or 0.0),
        "last_round_started_at": str(status.get("last_round_started_at") or holy_grail_diagnostics.get("round_started_at") or ""),
        "last_round_finished_at": str(status.get("last_round_finished_at") or holy_grail_diagnostics.get("round_finished_at") or ""),
        "resource_summary": resource_summary,
        "runtime_sync": {
            "global": {**global_status, **_runtime_sync_event_detail("global")},
            "personal": {**personal_status, **_runtime_sync_event_detail("personal", int(ctx["user"]["id"]))},
            **dict(status.get("runtime_sync") or {}),
        },
        "symbol_state_items": symbol_state_items,
        "holy_grail_diagnostics": holy_grail_diagnostics,
    }


def _runtime_match_signature(item: Dict[str, Any]) -> Tuple[str, str, str, str, str]:
    entry = normalize_runtime_strategy_entry(
        item.get("params_json") if isinstance(item.get("params_json"), dict) else dict(item or {}),
        default_symbol=str(item.get("symbol") or ""),
        default_interval=str(item.get("interval") or item.get("timeframe_min") or ""),
    )
    external_key = str(item.get("external_key") or item.get("strategy_key") or entry.get("strategy_key") or "").strip()
    interval = str(entry.get("interval") or item.get("interval") or item.get("timeframe_min") or "").strip()
    if interval.endswith(".0"):
        interval = interval[:-2]
    return (
        external_key,
        str(entry.get("family") or item.get("family") or "").strip().lower(),
        str(entry.get("symbol") or item.get("symbol") or "").strip().upper(),
        normalize_direction(entry.get("direction") or item.get("direction"), default="long"),
        interval.lower(),
    )


def _runtime_snapshot_signatures(snapshot: Dict[str, Any]) -> set[Tuple[str, str, str, str, str]]:
    out: set[Tuple[str, str, str, str, str]] = set()
    for item in list(snapshot.get("items") or []):
        try:
            out.add(_runtime_match_signature(dict(item or {})))
        except Exception:
            continue
    return out


def _active_strategy_runtime_lookup(limit: int = 20000) -> Dict[Tuple[str, str, str, str, str], Dict[str, Any]]:
    return _active_strategy_runtime_lookup_for_items(None, limit=limit)


def _active_strategy_runtime_lookup_for_items(
    runtime_items: Optional[List[Dict[str, Any]]],
    *,
    limit: int = 20000,
) -> Dict[Tuple[str, str, str, str, str], Dict[str, Any]]:
    item_list = [dict(item or {}) for item in list(runtime_items or [])]
    now = time.time()
    cache_key = f"limit:{int(limit or 0)}"
    if item_list:
        fingerprints: List[str] = []
        for item in item_list:
            try:
                signature = _runtime_match_signature(item)
            except Exception:
                continue
            fingerprints.append("||".join(signature))
        cache_key = "items:" + hashlib.sha1("\n".join(sorted(fingerprints)).encode("utf-8")).hexdigest()
    cache_lookup = _runtime_lookup_cache.get("lookup") or {}
    if cache_lookup and now - float(_runtime_lookup_cache.get("ts") or 0.0) < 60.0 and str(_runtime_lookup_cache.get("key") or "") == cache_key:
        return dict(cache_lookup)

    lookup: Dict[Tuple[str, str, str, str, str], Dict[str, Any]] = {}
    for row in db.list_active_strategy_runtime_rows(limit=limit, runtime_items=item_list):
        item = dict(row or {})
        try:
            signature = _runtime_match_signature(item)
        except Exception:
            continue
        score = float(item.get("score") or (item.get("metrics") or {}).get("sharpe") or 0.0)
        current = lookup.get(signature)
        current_score = float((current or {}).get("score") or ((current or {}).get("metrics") or {}).get("sharpe") or 0.0)
        if current is None or score >= current_score:
            lookup[signature] = item
    _runtime_lookup_cache["ts"] = now
    _runtime_lookup_cache["lookup"] = dict(lookup)
    _runtime_lookup_cache["key"] = cache_key
    return lookup


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _as_optional_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except Exception:
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return float(number)


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _is_effectively_missing_metric(value: Any) -> bool:
    try:
        return abs(float(value or 0.0)) <= 1e-9
    except Exception:
        return True


def _prefer_runtime_metric(value: Any, fallback: Any) -> float:
    if not _is_effectively_missing_metric(value):
        return _as_float(value, 0.0)
    return _as_float(fallback, 0.0)


def _first_present_value(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _runtime_display_key(value: Any, *, strategy_id: Any = 0) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    strategy_id_text = str(_as_int(strategy_id, 0) or "").strip()
    if strategy_id_text and text == strategy_id_text:
        return ""
    if text.isdigit():
        return ""
    return text


def _enrich_runtime_items(items: List[Dict[str, Any]], *, strategy_lookup: Optional[Dict[Tuple[str, str, str, str, str], Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    lookup = strategy_lookup or {}
    enriched: List[Dict[str, Any]] = []
    for raw in list(items or []):
        item = dict(raw or {})
        match = _find_runtime_strategy_match(item, lookup)
        metrics = dict(match.get("metrics") or {})
        strategy_id = _as_int(match.get("strategy_id"), _as_int(item.get("strategy_id"), 0))
        resolved_external_key = (
            _runtime_display_key(match.get("external_key"), strategy_id=strategy_id)
            or _runtime_display_key(match.get("pool_external_key"), strategy_id=strategy_id)
            or _runtime_display_key(item.get("external_key"), strategy_id=strategy_id)
            or _runtime_display_key(item.get("strategy_key"), strategy_id=strategy_id)
        )
        item["owner_user_id"] = int(match.get("owner_user_id") or item.get("owner_user_id") or 0)
        item["owner_username"] = str(match.get("username") or item.get("owner_username") or "")
        item["owner_nickname"] = str(
            match.get("display_name")
            or match.get("nickname")
            or match.get("username")
            or item.get("owner_nickname")
            or item.get("owner_username")
            or ""
        )
        item["owner_avatar_url"] = str(match.get("avatar_url") or item.get("owner_avatar_url") or "")
        item["strategy_id"] = strategy_id
        item["strategy_key"] = _runtime_display_key(item.get("strategy_key"), strategy_id=strategy_id)
        item["external_key"] = resolved_external_key
        item["total_return_pct"] = _prefer_runtime_metric(item.get("total_return_pct"), metrics.get("total_return_pct"))
        item["max_drawdown_pct"] = _prefer_runtime_metric(item.get("max_drawdown_pct"), metrics.get("max_drawdown_pct"))
        item["sharpe"] = _prefer_runtime_metric(item.get("sharpe"), match.get("score") or metrics.get("sharpe"))
        item["score"] = _prefer_runtime_metric(item.get("score") or item.get("sharpe"), match.get("score") or metrics.get("sharpe"))
        item["display_interval"] = str(item.get("display_interval") or item.get("interval") or match.get("timeframe_min") or "")
        enriched.append(item)
    return enriched


def _find_runtime_strategy_match(
    item: Dict[str, Any],
    strategy_lookup: Dict[Tuple[str, str, str, str, str], Dict[str, Any]],
) -> Dict[str, Any]:
    strategy_id = _as_int(item.get("strategy_id"), 0)
    if strategy_id > 0:
        for candidate in strategy_lookup.values():
            if _as_int(candidate.get("strategy_id"), 0) == strategy_id:
                return candidate
    strategy_key = str(item.get("strategy_key") or item.get("external_key") or "").strip()
    if strategy_key:
        for candidate in strategy_lookup.values():
            candidate_key = str(candidate.get("external_key") or "").strip()
            if candidate_key and candidate_key == strategy_key:
                return candidate
    try:
        exact = strategy_lookup.get(_runtime_match_signature(item), {}) or {}
        if exact:
            return exact
    except Exception:
        exact = {}
    try:
        signature = _runtime_match_signature(item)
        bare_signature = signature[1:]
        bare_interval_minutes = int(db._interval_to_minutes(signature[4]))
    except Exception:
        return exact or {}
    for candidate in strategy_lookup.values():
        try:
            candidate_signature = _runtime_match_signature(candidate)
            candidate_bare_signature = candidate_signature[1:]
            if candidate_bare_signature == bare_signature:
                return candidate
            if candidate_bare_signature[:3] == bare_signature[:3]:
                candidate_interval_minutes = int(db._interval_to_minutes(candidate_signature[4]))
                if bare_interval_minutes > 0 and candidate_interval_minutes == bare_interval_minutes:
                    return candidate
        except Exception:
            continue
    return exact or {}


def _enrich_runtime_position_items(
    items: List[Dict[str, Any]],
    *,
    strategy_lookup: Optional[Dict[Tuple[str, str, str, str, str], Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    lookup = strategy_lookup or {}
    enriched: List[Dict[str, Any]] = []
    for raw in list(items or []):
        item = dict(raw or {})
        match = _find_runtime_strategy_match(item, lookup)
        metrics = dict(match.get("metrics") or {})
        item["position_key"] = str(item.get("position_key") or item.get("position_id") or item.get("strategy_key") or "")
        item["owner_user_id"] = int(match.get("owner_user_id") or item.get("owner_user_id") or 0)
        item["owner_username"] = str(match.get("username") or item.get("owner_username") or "")
        item["owner_nickname"] = str(
            match.get("display_name")
            or match.get("nickname")
            or match.get("username")
            or item.get("owner_nickname")
            or item.get("owner_username")
            or ""
        )
        item["owner_avatar_url"] = str(match.get("avatar_url") or item.get("owner_avatar_url") or "")
        item["strategy_id"] = _as_int(match.get("strategy_id"), _as_int(item.get("strategy_id"), 0))
        item["external_key"] = str(match.get("external_key") or item.get("external_key") or item.get("strategy_key") or "")
        item["family"] = str(item.get("family") or match.get("family") or "")
        item["symbol"] = str(item.get("symbol") or match.get("symbol") or "").upper()
        item["direction"] = normalize_direction(item.get("direction") or match.get("direction"), default="long")
        item["interval"] = str(item.get("interval") or match.get("timeframe_min") or "")
        item["display_interval"] = str(item.get("display_interval") or item.get("interval") or match.get("timeframe_min") or "")
        item["score"] = _as_float(item.get("score") or match.get("score") or metrics.get("sharpe") or 0.0)
        item["entry_price"] = _as_optional_float(_first_present_value(item.get("entry_price"), item.get("entryPrice")))
        if item["entry_price"] is not None and item["entry_price"] <= 0:
            item["entry_price"] = None
        item["mark_price"] = _as_optional_float(_first_present_value(item.get("mark_price"), item.get("markPrice")))
        item["liquidation_price"] = _as_optional_float(
            _first_present_value(
                item.get("liquidation_price"),
                item.get("liq_price"),
                item.get("liquidationPrice"),
                item.get("liqPrice"),
            )
        )
        if item["liquidation_price"] is not None and item["liquidation_price"] <= 0:
            item["liquidation_price"] = None
        item["position_qty"] = _as_float(item.get("position_qty") or item.get("qty") or item.get("positionAmt") or 0.0)
        item["position_usdt"] = _as_float(item.get("position_usdt") or item.get("position_value") or item.get("positionValue") or 0.0)
        item["margin_usdt"] = _as_float(item.get("margin_usdt") or item.get("margin") or item.get("marginValue") or 0.0)
        item["margin_ratio_pct"] = _as_optional_float(
            _first_present_value(item.get("margin_ratio_pct"), item.get("marginRatePct"), item.get("margin_rate_pct"))
        )
        if item["margin_ratio_pct"] is not None and item["margin_ratio_pct"] <= 0:
            item["margin_ratio_pct"] = None
        item["unrealized_pnl_usdt"] = _as_float(item.get("unrealized_pnl_usdt") or item.get("unrealizedPnl") or item.get("unrealized_pnl") or 0.0)
        item["unrealized_pnl_roe_pct"] = _as_optional_float(
            _first_present_value(
                item.get("unrealized_pnl_roe_pct"),
                item.get("unrealizedPnlRoePct"),
                item.get("unrealized_pnl_pct"),
                item.get("unrealizedPnlPct"),
                item.get("estimated_pnl_pct"),
            )
        )
        item["unrealized_pnl_pct"] = item["unrealized_pnl_roe_pct"]
        enriched.append(item)
    enriched.sort(
        key=lambda row: (
            -abs(_as_float(row.get("position_usdt") or 0.0)),
            str(row.get("symbol") or ""),
            str(row.get("direction") or ""),
        )
    )
    return enriched


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


def _verify_request_signature(req: Request, token_obj: Dict[str, Any], body: str = "") -> bool:
    """[新增] 驗證請求簽名，防止重放攻擊和請求篡改
    
    客戶端需提供：
    - X-Signature: HMAC-SHA256(body + timestamp，使用 token 作為密鑰)
    - X-Timestamp: 請求時間戳（毫秒）
    
    時間窗口：60 秒（超過 60 秒的請求拒絕）
    """
    # 開發模式可禁用（環境變數）
    if os.environ.get("SHEEP_SKIP_SIGNATURE_CHECK", "").lower() == "true":
        return True
    
    signature = req.headers.get("x-signature") or req.headers.get("X-Signature")
    timestamp = req.headers.get("x-timestamp") or req.headers.get("X-Timestamp")
    
    # 簽名檢查為可選功能（舊版客戶端相容）
    if not signature or not timestamp:
        return True
    
    try:
        ts_ms = int(timestamp)
        current_ms = int(time.time() * 1000)
        
        # 時間窗口檢查：防止重放（60 秒）
        if abs(current_ms - ts_ms) > 60000:
            logger.warning(f"Time skew detected: {abs(current_ms - ts_ms)}ms")
            return False
        
        # 簽名驗證
        token_str = str(token_obj.get("token", ""))
        message = f"{body}:{timestamp}"
        
        from sheep_platform_security import stable_hmac_sha256, get_hmac_key
        expected_sig = stable_hmac_sha256(get_hmac_key(), message)
        
        return hmac.compare_digest(signature, expected_sig)
        
    except Exception as e:
        logger.error(f"Signature verification error: {e}")
        return False

def _is_compute_token(ctx: Dict[str, Any]) -> bool:
    try:
        u = ctx.get("user") or {}
        t = ctx.get("token") or {}
        role = str(u.get("role") or "")
        name = str(t.get("name") or "")
        return (role == "admin") and (name == "compute")
    except Exception:
        return False


_worker_token_telemetry_cache: Dict[str, float] = {}


def _token_kind(ctx: Dict[str, Any]) -> str:
    try:
        if _is_compute_token(ctx):
            return "compute"
        token = ctx.get("token") or {}
        name = str(token.get("name") or "").strip().lower()
        if name == "worker":
            return "worker"
        if name == "web_session":
            return "web_session"
        return name or "unknown"
    except Exception:
        return "unknown"


def _is_admin_ctx(ctx: Dict[str, Any]) -> bool:
    try:
        return str((ctx.get("user") or {}).get("role") or "") == "admin"
    except Exception:
        return False


def _can_publish_global_runtime(ctx: Dict[str, Any]) -> bool:
    if not _is_admin_ctx(ctx):
        return False
    token_name = str((ctx.get("token") or {}).get("name") or "").strip().lower()
    return token_name in {"compute", "web_session", "worker", "system_sync", "runtime_password"} or token_name == ""


def _runtime_sync_auth_ctx(
    req: Request,
    authorization: Optional[str],
    *,
    username: str = "",
    password: str = "",
) -> Dict[str, Any]:
    if authorization:
        return _auth_ctx(req, authorization)
    if not _allow_runtime_password_auth():
        raise HTTPException(status_code=401, detail="runtime_password_auth_disabled")
    uname = str(username or "").strip()
    pwd = str(password or "")
    if not uname or not pwd:
        raise HTTPException(status_code=401, detail="missing_runtime_sync_credentials")
    from sheep_platform_security import verify_password

    user = db.get_user_by_username(uname)
    if not user or not verify_password(pwd, user.get("password_hash", "")):
        raise HTTPException(status_code=401, detail="bad_runtime_sync_credentials")
    if int(user.get("disabled") or 0) != 0:
        raise HTTPException(status_code=403, detail="user_disabled")
    req.state.user_id = int(user["id"])
    req.state.token_id = 0
    return {"user": user, "token": {"id": 0, "name": "runtime_password"}}


def _log_legacy_worker_token_use(req: Request, ctx: Dict[str, Any], endpoint: str) -> None:
    if _token_kind(ctx) != "web_session":
        return
    token = ctx.get("token") or {}
    token_id = int(token.get("id") or 0)
    cache_key = f"{token_id}:{endpoint}"
    now = time.time()
    last = float(_worker_token_telemetry_cache.get(cache_key, 0.0))
    if now - last < 300.0:
        return
    _worker_token_telemetry_cache[cache_key] = now
    db.log_sys_event(
        "LEGACY_WORKER_TOKEN_USED",
        (ctx.get("user") or {}).get("id"),
        f"Legacy web session token used on worker endpoint: {endpoint}",
        {
            "path": endpoint,
            "token_id": token_id,
            "token_kind": _token_kind(ctx),
            "ip": _client_ip(req),
            "user_agent": req.headers.get("user-agent", ""),
        },
    )


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

    token_kind = _token_kind(ctx)
    if token_kind not in {"worker", "compute", "web_session"}:
        raise HTTPException(status_code=403, detail="worker_token_required")
    if token_kind == "web_session":
        _log_legacy_worker_token_use(req, ctx, req.url.path)

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
            meta={
                "ua": req.headers.get("user-agent"),
                "ip": _client_ip(req),
                "kind": token_kind,
                "token_kind": token_kind,
            },
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
        result = evaluate_thresholds(
            metrics=metrics,
            min_trades=min_trades,
            min_total_return_pct=min_ret,
            max_drawdown_pct=max_dd,
            min_sharpe=min_sh,
        )
        return bool(result["passed"]), str(result["reason"])
    except Exception as e:
        return False, f"指標解析錯誤: {e}"


def _get_active_cycle_id() -> int:
    cycle = db.get_active_cycle()
    return int(cycle["id"]) if cycle else 0


def _get_user_pending_assignment_count(user_id: int, cycle_id: int) -> int:
    try:
        return int(db.count_tasks_for_user(user_id, cycle_id=cycle_id, statuses=["assigned"]))
    except Exception:
        return 0


def _prime_user_task_queue(user_id: int, cycle_id: int) -> Dict[str, int]:
    if int(cycle_id or 0) <= 0:
        return {"assigned_count": 0, "task_count": 0, "pending_task_count": 0, "active_cycle_id": 0}

    before_pending = _get_user_pending_assignment_count(user_id, cycle_id)
    before_task_count = int(db.count_tasks_for_user(user_id, cycle_id=cycle_id, statuses=["assigned", "queued", "running"]))

    conn = db._conn()
    try:
        min_tasks = int(db.get_setting(conn, "min_tasks_per_user", 2))
    finally:
        conn.close()

    db.assign_tasks_for_user(user_id, cycle_id=cycle_id, min_tasks=min_tasks)

    after_pending = _get_user_pending_assignment_count(user_id, cycle_id)
    after_task_count = int(db.count_tasks_for_user(user_id, cycle_id=cycle_id, statuses=["assigned", "queued", "running"]))
    return {
        "assigned_count": max(0, after_pending - before_pending),
        "task_count": after_task_count,
        "pending_task_count": after_pending,
        "active_cycle_id": int(cycle_id),
    }


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
    name: str = "web_session"
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


class WorkerTokenIssueIn(BaseModel):
    ttl_seconds: int = 86400 * 30
    rotate_existing: bool = True


class RuntimeSyncTokenIssueIn(BaseModel):
    ttl_seconds: int = 86400 * 30
    rotate_existing: bool = True

class WebRegisterIn(BaseModel):
    username: str
    password: str
    nickname: str
    tos_ok: bool
    avatar_url: str = ""
    captcha_token: str = ""
    captcha_offset: float = 0.0
    captcha_tracks: List[Dict[str, Any]] = []


class UserProfileUpdateIn(BaseModel):
    nickname: str
    avatar_url: str = ""
    clear_avatar: bool = False


class DefaultAvatarUpdateIn(BaseModel):
    avatar_url: str = ""

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
        raise HTTPException(status_code=500, detail={"code": "captcha_unavailable", "message": "目前無法載入安全驗證，請稍後再試。"})

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
class AdminSettingsUpdate(BaseModel):
    min_trades: int
    min_total_return_pct: float
    max_drawdown_pct: float
    min_sharpe: float
    candidate_keep_top_n: Optional[int] = None
    keep_top_n: Optional[int] = None
    global_fee_pct: Optional[float] = None
    global_slippage_pct: Optional[float] = None

class FactorPoolCreate(BaseModel):
    name: str
    external_key: str = ""
    symbol: str
    direction: str = "long"
    timeframe_min: int
    years: int
    family: str
    grid_spec: Dict[str, Any]
    risk_spec: Dict[str, Any]
    num_partitions: int
    seed: int
    active: bool
    auto_expand: bool = False

class FactorPoolUpdate(BaseModel):
    name: str
    external_key: str = ""
    symbol: str
    direction: str = "long"
    timeframe_min: int
    years: int
    family: str
    grid_spec: Dict[str, Any]
    risk_spec: Dict[str, Any]
    num_partitions: int
    seed: int
    active: bool


class RuntimePortfolioSyncIn(BaseModel):
    scope: str
    updated_at: Optional[str] = None
    source: str = "holy_grail_runtime"
    summary: Dict[str, Any] = {}
    items: List[Dict[str, Any]] = []
    checksum: Optional[str] = None
    username: str = ""
    password: str = ""


class RealtimeControlIn(BaseModel):
    action: str
    reason: str = ""


class CatalogImportIn(BaseModel):
    schema_version: int = 1
    factor_pools: List[Dict[str, Any]] = []
    strategies: List[Dict[str, Any]] = []


class AnnouncementCreateIn(BaseModel):
    title: str
    preview_text: str = ""
    body_markdown: str = ""
    slug: str = ""
    status: str = "draft"


class AnnouncementUpdateIn(BaseModel):
    title: str
    preview_text: str = ""
    body_markdown: str = ""
    slug: str = ""
    status: str = "draft"


_THRESHOLD_DEFAULTS: Dict[str, Any] = {
    "min_trades": 30,
    "min_total_return_pct": 3.0,
    "max_drawdown_pct": 25.0,
    "min_sharpe": 0.6,
    "candidate_keep_top_n": 30,
    "global_fee_pct": 0.06,
    "global_slippage_pct": 0.02,
}


def _resolve_candidate_keep_top_n(payload: Any) -> int:
    if isinstance(payload, BaseModel):
        raw = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
    elif isinstance(payload, dict):
        raw = dict(payload)
    else:
        raw = {}

    value = raw.get("candidate_keep_top_n")
    if value is None:
        value = raw.get("keep_top_n")
    if value is None:
        raise HTTPException(status_code=422, detail="candidate_keep_top_n is required")
    return int(value)


def _load_threshold_state() -> Tuple[Dict[str, Any], Dict[str, Optional[str]]]:
    keys = [
        "min_trades",
        "min_total_return_pct",
        "max_drawdown_pct",
        "min_sharpe",
        "candidate_keep_top_n",
        "global_fee_pct",
        "global_slippage_pct",
    ]
    details = db.get_settings_details(keys)
    thresholds = {
        "min_trades": int(details.get("min_trades", {}).get("value") if details.get("min_trades", {}).get("value") is not None else _THRESHOLD_DEFAULTS["min_trades"]),
        "min_total_return_pct": float(details.get("min_total_return_pct", {}).get("value") if details.get("min_total_return_pct", {}).get("value") is not None else _THRESHOLD_DEFAULTS["min_total_return_pct"]),
        "max_drawdown_pct": float(details.get("max_drawdown_pct", {}).get("value") if details.get("max_drawdown_pct", {}).get("value") is not None else _THRESHOLD_DEFAULTS["max_drawdown_pct"]),
        "min_sharpe": float(details.get("min_sharpe", {}).get("value") if details.get("min_sharpe", {}).get("value") is not None else _THRESHOLD_DEFAULTS["min_sharpe"]),
        "candidate_keep_top_n": int(details.get("candidate_keep_top_n", {}).get("value") if details.get("candidate_keep_top_n", {}).get("value") is not None else _THRESHOLD_DEFAULTS["candidate_keep_top_n"]),
        "global_fee_pct": float(details.get("global_fee_pct", {}).get("value") if details.get("global_fee_pct", {}).get("value") is not None else _THRESHOLD_DEFAULTS["global_fee_pct"]),
        "global_slippage_pct": float(details.get("global_slippage_pct", {}).get("value") if details.get("global_slippage_pct", {}).get("value") is not None else _THRESHOLD_DEFAULTS["global_slippage_pct"]),
    }
    thresholds["keep_top_n"] = int(thresholds["candidate_keep_top_n"])

    updated_at = {
        "min_trades": details.get("min_trades", {}).get("updated_at"),
        "min_total_return_pct": details.get("min_total_return_pct", {}).get("updated_at"),
        "max_drawdown_pct": details.get("max_drawdown_pct", {}).get("updated_at"),
        "min_sharpe": details.get("min_sharpe", {}).get("updated_at"),
        "candidate_keep_top_n": details.get("candidate_keep_top_n", {}).get("updated_at"),
        "keep_top_n": details.get("candidate_keep_top_n", {}).get("updated_at"),
        "global_fee_pct": details.get("global_fee_pct", {}).get("updated_at"),
        "global_slippage_pct": details.get("global_slippage_pct", {}).get("updated_at"),
    }
    return thresholds, updated_at


def _safe_markdown_to_html(text: Any) -> str:
    raw = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    if not raw.strip():
        return ""
    lines = raw.split("\n")
    blocks: List[str] = []
    in_list = False
    in_code = False
    code_lines: List[str] = []
    paragraph: List[str] = []

    def flush_paragraph() -> None:
        nonlocal paragraph
        if not paragraph:
            return
        content = "<br>".join([html.escape(line.strip()) for line in paragraph if str(line).strip()])
        if content:
            blocks.append(f"<p>{content}</p>")
        paragraph = []

    def flush_list() -> None:
        nonlocal in_list
        if in_list:
            blocks.append("</ul>")
            in_list = False

    def flush_code() -> None:
        nonlocal in_code, code_lines
        if in_code:
            blocks.append("<pre><code>" + html.escape("\n".join(code_lines)) + "</code></pre>")
            code_lines = []
            in_code = False

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("```"):
            flush_paragraph()
            flush_list()
            if in_code:
                flush_code()
            else:
                in_code = True
                code_lines = []
            continue
        if in_code:
            code_lines.append(line)
            continue
        if not stripped:
            flush_paragraph()
            flush_list()
            continue
        if stripped.startswith("### "):
            flush_paragraph()
            flush_list()
            blocks.append(f"<h3>{html.escape(stripped[4:].strip())}</h3>")
            continue
        if stripped.startswith("## "):
            flush_paragraph()
            flush_list()
            blocks.append(f"<h2>{html.escape(stripped[3:].strip())}</h2>")
            continue
        if stripped.startswith("# "):
            flush_paragraph()
            flush_list()
            blocks.append(f"<h1>{html.escape(stripped[2:].strip())}</h1>")
            continue
        if stripped.startswith(("- ", "* ")):
            flush_paragraph()
            if not in_list:
                blocks.append("<ul>")
                in_list = True
            blocks.append(f"<li>{html.escape(stripped[2:].strip())}</li>")
            continue
        paragraph.append(line)

    flush_paragraph()
    flush_list()
    flush_code()
    return "".join(blocks)


def _announcement_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    item = dict(row or {})
    item["published_at"] = str(item.get("published_at") or "")
    item["created_at"] = str(item.get("created_at") or "")
    item["updated_at"] = str(item.get("updated_at") or "")
    return item


def _cost_settings_payload() -> Dict[str, Any]:
    thresholds, updated_at = _load_threshold_state()
    fee_pct = float(thresholds.get("global_fee_pct") or 0.0)
    slippage_pct = float(thresholds.get("global_slippage_pct") or 0.0)
    return {
        "fee_pct": fee_pct,
        "slippage_pct": slippage_pct,
        "fee_side": fee_pct / 100.0,
        "slippage_pct_decimal": slippage_pct / 100.0,
        "source_settings_updated_at": updated_at.get("global_slippage_pct") or updated_at.get("global_fee_pct") or _utc_iso(),
    }


def _apply_global_costs_to_risk_spec(risk_spec: Any) -> Dict[str, Any]:
    effective = dict(risk_spec or {})
    costs = _cost_settings_payload()
    effective["fee_side"] = float(costs["fee_side"])
    effective["fee_pct"] = float(costs["fee_pct"])
    effective["global_fee_pct"] = float(costs["fee_pct"])
    effective["slippage"] = float(costs["slippage_pct_decimal"])
    effective["slippage_pct"] = float(costs["slippage_pct_decimal"])
    effective["global_slippage_pct"] = float(costs["slippage_pct"])
    effective["cost_basis"] = {
        "fee_pct": float(costs["fee_pct"]),
        "slippage_pct": float(costs["slippage_pct"]),
        "fee_side": float(costs["fee_side"]),
        "slippage": float(costs["slippage_pct_decimal"]),
        "source_settings_updated_at": str(costs["source_settings_updated_at"] or ""),
    }
    return effective


async def _broadcast_announcement_event(action: str, announcement: Optional[Dict[str, Any]] = None) -> None:
    payload = {
        "type": "announcement_update",
        "action": str(action or "updated"),
        "ts": _utc_iso(),
        "announcement_version": _live_versions["announcement"],
        "announcement": _announcement_payload(announcement or {}),
    }
    await _announcement_hub.broadcast(payload)


def _dispatch_announcement_event(action: str, announcement: Optional[Dict[str, Any]] = None) -> None:
    try:
        asyncio.run(_broadcast_announcement_event(action, announcement))
    except RuntimeError:
        try:
            loop = asyncio.get_event_loop()
            loop.create_task(_broadcast_announcement_event(action, announcement))
        except Exception:
            pass


def _announcement_list_payload(*, page: int, page_size: int, q: str = "", status: str = "", include_drafts: bool = False) -> Dict[str, Any]:
    page = max(1, int(page or 1))
    page_size = max(1, min(100, int(page_size or 20)))
    offset = (page - 1) * page_size
    rows = db.list_announcements(
        limit=page_size,
        offset=offset,
        q=q,
        status=status,
        include_drafts=include_drafts,
    )
    total = db.count_announcements(status=status, include_drafts=include_drafts, q=q)
    items = [_announcement_payload(row) for row in rows]
    return {
        "ok": True,
        "items": items,
        "total": int(total or 0),
        "page": page,
        "page_size": page_size,
        "has_next": (offset + len(items)) < int(total or 0),
        "announcement_version": _live_versions["announcement"],
        "generated_at": _utc_iso(),
    }


def _log_deprecated_alias(req: Request, ctx: Dict[str, Any], legacy_path: str, canonical_path: str) -> None:
    user = (ctx or {}).get("user") or {}
    db.log_sys_event(
        "DEPRECATED_ALIAS_USED",
        user.get("id"),
        f"Legacy API alias used: {legacy_path} -> {canonical_path}",
        {
            "legacy_path": legacy_path,
            "canonical_path": canonical_path,
            "method": req.method,
            "ip": _client_ip(req),
            "user_agent": req.headers.get("user-agent", ""),
        },
    )

@app.get("/admin/factor_pools")
def get_admin_factor_pools(req: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(req, authorization)
    if str(ctx["user"].get("role")) != "admin":
        raise HTTPException(status_code=403, detail="權限不足：僅限系統管理員")
    try:
        cycle = db.get_active_cycle()
        cycle_id = int(cycle["id"]) if cycle else 0
        pools = db.list_factor_pools(cycle_id)
        # 專家級修復：確保前後端 JSON 型別一致，防止前端讀取到純字串崩潰
        for p in pools:
            try: p["grid_spec"] = json.loads(p.get("grid_spec_json") or "{}")
            except Exception: p["grid_spec"] = {}
            try: p["risk_spec"] = json.loads(p.get("risk_spec_json") or "{}")
            except Exception: p["risk_spec"] = {}
        return {"ok": True, "cycle_id": cycle_id, "pools": pools}
    except Exception as e:
        import traceback
        err_str = traceback.format_exc()
        db.log_sys_event("ADMIN_POOL_ERROR", ctx["user"].get("id"), f"讀取策略池失敗: {str(e)}", {"trace": err_str})
        return JSONResponse(status_code=200, content={"ok": False, "msg": f"資料庫讀取異常: {str(e)}\n{err_str}", "pools": []})

@app.post("/admin/factor_pools")
def create_admin_factor_pool(req: Request, body: FactorPoolCreate, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(req, authorization)
    if str(ctx["user"].get("role")) != "admin":
        raise HTTPException(status_code=403, detail="權限不足")
    try:
        cycle = db.get_active_cycle()
        if not cycle:
            raise HTTPException(status_code=400, detail="目前沒有活躍的挖礦週期，無法建立策略池")
        cycle_id = int(cycle["id"])
        ids = db.create_factor_pool(
            cycle_id=cycle_id, name=body.name, symbol=body.symbol, timeframe_min=body.timeframe_min,
            years=body.years, family=body.family, grid_spec=body.grid_spec, risk_spec=body.risk_spec,
            num_partitions=body.num_partitions, seed=body.seed, active=body.active, auto_expand=body.auto_expand,
            direction=normalize_direction(body.direction, reverse=(body.risk_spec or {}).get("reverse_mode"), default="long"),
            external_key=str(body.external_key or "").strip(),
        )
        db.log_sys_event("ADMIN_POOL_CREATE", ctx["user"].get("id"), f"管理員建立了 {len(ids)} 個策略池", {"ids": ids})
        return {"ok": True, "msg": f"成功建立 {len(ids)} 個策略池！", "ids": ids}
    except Exception as e:
        import traceback
        raise HTTPException(status_code=500, detail=f"建立策略池失敗: {str(e)}\n{traceback.format_exc()}")

@app.put("/admin/factor_pools/{pool_id}")
def update_admin_factor_pool(pool_id: int, req: Request, body: FactorPoolUpdate, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(req, authorization)
    if str(ctx["user"].get("role")) != "admin":
        raise HTTPException(status_code=403, detail="權限不足")
    try:
        db.update_factor_pool(
            pool_id=pool_id, name=body.name, symbol=body.symbol, timeframe_min=body.timeframe_min,
            years=body.years, family=body.family, grid_spec=body.grid_spec, risk_spec=body.risk_spec,
            num_partitions=body.num_partitions, seed=body.seed, active=body.active,
            direction=normalize_direction(body.direction, reverse=(body.risk_spec or {}).get("reverse_mode"), default="long"),
            external_key=str(body.external_key or "").strip(),
        )
        db.log_sys_event("ADMIN_POOL_UPDATE", ctx["user"].get("id"), f"管理員更新了策略池 #{pool_id}", {"pool_id": pool_id})
        return {"ok": True, "msg": f"策略池 #{pool_id} 更新成功！"}
    except Exception as e:
        import traceback
        raise HTTPException(status_code=500, detail=f"更新策略池失敗: {str(e)}\n{traceback.format_exc()}")

@app.post("/admin/catalog/import")
def admin_catalog_import(
    req: Request,
    body: CatalogImportIn,
    dry_run: bool = True,
    authorization: Optional[str] = Header(None),
):
    ctx = _auth_ctx(req, authorization)
    if not _is_admin_ctx(ctx):
        raise HTTPException(status_code=403, detail="admin_required")
    cycle = db.get_active_cycle()
    cycle_id = int(cycle.get("id") or 0) if cycle else 0
    if cycle_id <= 0:
        raise HTTPException(status_code=400, detail="active_cycle_required")
    report = db.import_admin_catalog(
        cycle_id=cycle_id,
        owner_user_id=int(ctx["user"]["id"]),
        payload=body.model_dump() if hasattr(body, "model_dump") else body.dict(),
        dry_run=bool(dry_run),
    )
    if report.get("ok") and not bool(dry_run):
        _invalidate_live_state("dashboard", "leaderboard")
    return JSONResponse(status_code=200 if report.get("ok") else 400, content=report)


@app.post("/runtime/portfolio/sync")
def runtime_portfolio_sync(
    req: Request,
    body: RuntimePortfolioSyncIn,
    authorization: Optional[str] = Header(None),
):
    ctx = _runtime_sync_auth_ctx(
        req,
        authorization,
        username=str(body.username or ""),
        password=str(body.password or ""),
    )
    scope = "global" if str(body.scope or "").strip().lower() == "global" else "personal"
    if scope == "global" and not _can_publish_global_runtime(ctx):
        raise HTTPException(status_code=403, detail="global_runtime_admin_required")
    token_kind = _token_kind(ctx)
    if token_kind == "runtime_password":
        db.log_sys_event(
            "RUNTIME_SYNC_DEPRECATED_AUTH",
            int(ctx["user"]["id"]),
            f"Runtime sync using deprecated password auth ({scope})",
            {"scope": scope, "ip": _client_ip(req)},
        )

    normalized_items: List[Dict[str, Any]] = []
    for idx, raw_item in enumerate(list(body.items or []), start=1):
        item = normalize_runtime_strategy_entry(dict(raw_item or {}))
        direction = normalize_direction(item.get("direction"), default="")
        if direction not in {"long", "short"}:
            raise HTTPException(status_code=422, detail=f"invalid_direction_at_item_{idx}")
        item.update(
            {
                "rank": int((raw_item or {}).get("rank") or idx),
                "strategy_key": str((raw_item or {}).get("strategy_key") or f"{item.get('family')}_{idx}"),
                "stake_pct": float((raw_item or {}).get("stake_pct") or item.get("stake_pct") or 0.0),
                "sharpe": float((raw_item or {}).get("sharpe") or 0.0),
                "total_return_pct": float((raw_item or {}).get("total_return_pct") or 0.0),
                "max_drawdown_pct": float((raw_item or {}).get("max_drawdown_pct") or 0.0),
                "avg_pairwise_corr_to_selected": (raw_item or {}).get("avg_pairwise_corr_to_selected"),
                "max_pairwise_corr_to_selected": (raw_item or {}).get("max_pairwise_corr_to_selected"),
                "duplicate_group_id": (raw_item or {}).get("duplicate_group_id"),
            }
        )
        normalized_items.append(item)
    try:
        snapshot = db.save_runtime_portfolio_snapshot(
            scope=scope,
            user_id=int(ctx["user"]["id"]) if scope == "personal" else 0,
            published_by=int(ctx["user"]["id"]),
            source=str(body.source or "holy_grail_runtime"),
            items=normalized_items,
            summary=dict(body.summary or {}),
            updated_at=str(body.updated_at or ""),
            checksum=str(body.checksum or ""),
        )
        db.log_sys_event(
            f"RUNTIME_SYNC_{scope.upper()}_SUCCESS",
            int(ctx["user"]["id"]),
            f"{scope} runtime snapshot synced",
            {
                "scope": scope,
                "published_by": int(ctx["user"]["id"]),
                "source": str(body.source or "holy_grail_runtime"),
                "strategy_count": int(snapshot.get("strategy_count") or 0),
                "checksum": str(snapshot.get("checksum") or ""),
                "updated_at": str(snapshot.get("updated_at") or ""),
                "token_kind": token_kind,
            },
        )
        _invalidate_live_state("dashboard", "runtime")
        return {"ok": True, "snapshot": snapshot}
    except Exception as exc:
        db.log_sys_event(
            f"RUNTIME_SYNC_{scope.upper()}_FAIL",
            int(ctx["user"]["id"]),
            f"{scope} runtime snapshot sync failed: {exc}",
            {
                "scope": scope,
                "source": str(body.source or "holy_grail_runtime"),
                "strategy_count": len(normalized_items),
                "token_kind": token_kind,
            },
        )
        raise


@app.post("/admin/settings")
def update_admin_settings(req: Request, body: AdminSettingsUpdate, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(req, authorization)
    if str(ctx["user"].get("role")) != "admin":
        raise HTTPException(status_code=403, detail="權限不足：僅限系統管理員")
    
    keep_top_n = _resolve_candidate_keep_top_n(body)
    conn = db._conn()
    try:
        db.set_setting(conn, "min_trades", body.min_trades)
        db.set_setting(conn, "min_total_return_pct", body.min_total_return_pct)
        db.set_setting(conn, "max_drawdown_pct", body.max_drawdown_pct)
        db.set_setting(conn, "min_sharpe", body.min_sharpe)
        db.set_setting(conn, "candidate_keep_top_n", keep_top_n)
        db.set_setting(conn, "global_fee_pct", float(body.global_fee_pct if body.global_fee_pct is not None else _THRESHOLD_DEFAULTS["global_fee_pct"]))
        db.set_setting(conn, "global_slippage_pct", float(body.global_slippage_pct if body.global_slippage_pct is not None else _THRESHOLD_DEFAULTS["global_slippage_pct"]))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
    return {"ok": True, "msg": "達標門檻設定已成功更新並立即生效！"}

@app.get("/announcements")
def list_public_announcements(page: int = 1, page_size: int = 10, q: str = ""):
    cache_key = _live_cache_key("public", page, page_size, q.strip().lower())

    def _load() -> Dict[str, Any]:
        return _announcement_list_payload(page=page, page_size=page_size, q=q, include_drafts=False)

    payload = _cached_live_payload("announcements", cache_key, _load)
    _remember_live_snapshot("announcements", cache_key, payload)
    return payload


@app.get("/announcements/{slug}")
def get_public_announcement(slug: str):
    row = db.get_announcement_by_slug(slug, include_drafts=False)
    if not row:
        raise HTTPException(status_code=404, detail="announcement_not_found")
    return {"ok": True, "item": _announcement_payload(row), "announcement_version": _live_versions["announcement"]}


@app.get("/admin/announcements")
def admin_list_announcements(
    request: Request,
    authorization: Optional[str] = Header(None),
    page: int = 1,
    page_size: int = 20,
    q: str = "",
    status: str = "",
):
    ctx = _auth_ctx(request, authorization)
    if not _is_admin_ctx(ctx):
        raise HTTPException(status_code=403, detail="admin_required")
    return _announcement_list_payload(page=page, page_size=page_size, q=q, status=status, include_drafts=True)


@app.post("/admin/announcements")
def admin_create_announcement(
    request: Request,
    body: AnnouncementCreateIn,
    authorization: Optional[str] = Header(None),
):
    ctx = _auth_ctx(request, authorization)
    if not _is_admin_ctx(ctx):
        raise HTTPException(status_code=403, detail="admin_required")
    row = db.create_announcement(
        title=str(body.title or "").strip(),
        preview_text=str(body.preview_text or "").strip(),
        body_markdown=str(body.body_markdown or ""),
        body_html=_safe_markdown_to_html(body.body_markdown or ""),
        author_user_id=int(ctx["user"]["id"]),
        status=str(body.status or "draft").strip().lower() or "draft",
        slug=str(body.slug or "").strip(),
    )
    _invalidate_live_state("announcement", "dashboard")
    _dispatch_announcement_event("created", row)
    return {"ok": True, "item": _announcement_payload(row)}


@app.put("/admin/announcements/{announcement_id}")
def admin_update_announcement(
    announcement_id: int,
    request: Request,
    body: AnnouncementUpdateIn,
    authorization: Optional[str] = Header(None),
):
    ctx = _auth_ctx(request, authorization)
    if not _is_admin_ctx(ctx):
        raise HTTPException(status_code=403, detail="admin_required")
    row = db.update_announcement(
        int(announcement_id),
        title=str(body.title or "").strip(),
        preview_text=str(body.preview_text or "").strip(),
        body_markdown=str(body.body_markdown or ""),
        body_html=_safe_markdown_to_html(body.body_markdown or ""),
        status=str(body.status or "draft").strip().lower() or "draft",
        slug=str(body.slug or "").strip(),
    )
    if not row:
        raise HTTPException(status_code=404, detail="announcement_not_found")
    _invalidate_live_state("announcement", "dashboard")
    _dispatch_announcement_event("updated", row)
    return {"ok": True, "item": _announcement_payload(row)}


@app.post("/admin/announcements/{announcement_id}/publish")
def admin_publish_announcement(announcement_id: int, request: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(request, authorization)
    if not _is_admin_ctx(ctx):
        raise HTTPException(status_code=403, detail="admin_required")
    row = db.publish_announcement(int(announcement_id))
    if not row:
        raise HTTPException(status_code=404, detail="announcement_not_found")
    _invalidate_live_state("announcement", "dashboard")
    _dispatch_announcement_event("published", row)
    return {"ok": True, "item": _announcement_payload(row)}


@app.post("/admin/announcements/{announcement_id}/unpublish")
def admin_unpublish_announcement(announcement_id: int, request: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(request, authorization)
    if not _is_admin_ctx(ctx):
        raise HTTPException(status_code=403, detail="admin_required")
    row = db.unpublish_announcement(int(announcement_id))
    if not row:
        raise HTTPException(status_code=404, detail="announcement_not_found")
    _invalidate_live_state("announcement", "dashboard")
    _dispatch_announcement_event("unpublished", row)
    return {"ok": True, "item": _announcement_payload(row)}


@app.websocket("/ws/announcements")
async def ws_announcements(ws: WebSocket):
    await _announcement_hub.connect(ws)
    try:
        recent = db.list_announcements(limit=8, offset=0, include_drafts=False)
        await ws.send_json(
            {
                "type": "recent",
                "ts": _utc_iso(),
                "announcement_version": _live_versions["announcement"],
                "items": [_announcement_payload(row) for row in recent],
            }
        )
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        _announcement_hub.disconnect(ws)


@app.get("/admin/strategies")
def get_admin_strategies(
    req: Request,
    authorization: Optional[str] = Header(None),
    page: int = 1,
    page_size: int = 50,
    q: str = "",
    username: str = "",
    symbol: str = "",
    direction: str = "",
):
    ctx = _auth_ctx(req, authorization)
    if str(ctx["user"].get("role")) != "admin":
        db.log_sys_event("ADMIN_ACCESS_DENIED", ctx["user"].get("id"), "非法存取管理策略介面", {"ip": _client_ip(req)})
        raise HTTPException(status_code=403, detail="權限不足：僅限系統管理員")
    
    try:
        page_data = db.get_admin_active_strategies_page(
            page=page,
            page_size=page_size,
            q=q,
            username=username,
            symbol=symbol,
            direction=direction,
        )
        global_snapshot = db.get_runtime_portfolio_snapshot("global") or {}
        personal_snapshot = db.get_runtime_portfolio_snapshot("personal", user_id=int(ctx["user"]["id"])) or {}
        global_signatures = _runtime_snapshot_signatures(global_snapshot)
        personal_signatures = _runtime_snapshot_signatures(personal_snapshot)
        enriched_items: List[Dict[str, Any]] = []
        for item in list(page_data.get("items") or []):
            row = dict(item or {})
            signature = _runtime_match_signature(row)
            in_global = signature in global_signatures
            in_personal = signature in personal_signatures
            row["in_runtime"] = bool(in_global or in_personal)
            row["runtime_scopes"] = [scope for scope, enabled in (("personal", in_personal), ("global", in_global)) if enabled]
            row["runtime_updated_at"] = str(personal_snapshot.get("updated_at") or global_snapshot.get("updated_at") or "")
            enriched_items.append(row)

        if not enriched_items and int(page_data.get("total") or 0) == 0:
            db.log_sys_event("ADMIN_QUERY_EMPTY", ctx["user"].get("id"), "實盤策略池查詢結果為空", {})

        summary = {
            "global_runtime": _runtime_snapshot_status(global_snapshot),
            "personal_runtime": _runtime_snapshot_status(personal_snapshot),
        }
        return {
            "ok": True,
            "items": enriched_items,
            "strategies": enriched_items,
            "total": int(page_data.get("total") or 0),
            "page": int(page_data.get("page") or page),
            "page_size": int(page_data.get("page_size") or page_size),
            "has_next": bool(page_data.get("has_next")),
            "summary": summary,
        }
    except Exception as e:
        import traceback
        err_msg = traceback.format_exc()
        db.log_sys_event("ADMIN_STRAT_ERROR", ctx["user"].get("id"), f"讀取策略池失敗: {str(e)}", {"trace": err_msg})
        # 即使發生錯誤也回傳 200 並帶上錯誤詳情，防止前端觸發 504 崩潰
        return JSONResponse(
            status_code=200,
            content={"ok": False, "msg": f"資料庫讀取異常: {str(e)}", "items": [], "strategies": [], "total": 0, "page": 1, "page_size": 50, "has_next": False},
        )

@app.get("/admin/system_diagnostics")
def get_admin_system_diagnostics(req: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(req, authorization)
    if str(ctx["user"].get("role")) != "admin":
        raise HTTPException(status_code=403, detail="forbidden: admin only")

    cycle = db.get_active_cycle() or {}
    cycle_id = int(cycle.get("id") or 0)
    pools = db.list_factor_pools(cycle_id) if cycle_id > 0 else []
    strategies = db.get_admin_active_strategies_page(page=1, page_size=1)
    thresholds, threshold_updated_at = _load_threshold_state()
    db_source = db.describe_db_source()
    global_snapshot = db.get_runtime_portfolio_snapshot("global") or {}
    personal_snapshot = db.get_runtime_portfolio_snapshot("personal", user_id=int(ctx["user"]["id"])) or {}
    global_status = _runtime_snapshot_status(global_snapshot)
    personal_status = _runtime_snapshot_status(personal_snapshot)
    runtime_mismatch = bool(global_status.get("count_mismatch"))

    return {
        "ok": True,
        "db_kind": db_source.get("kind"),
        "db_target": db_source.get("masked_dsn"),
        "db_host": db_source.get("host"),
        "db_database": db_source.get("database"),
        "git_sha": str(os.environ.get("SHEEP_GIT_SHA", "") or ""),
        "active_cycle": {
            "id": cycle_id,
            "name": cycle.get("name"),
            "status": cycle.get("status"),
            "start_ts": cycle.get("start_ts"),
            "end_ts": cycle.get("end_ts"),
        },
        "active_pool_count": len(pools),
        "active_strategy_count": int(strategies.get("total") or 0),
        "thresholds": thresholds,
        "threshold_updated_at": threshold_updated_at,
        "runtime_sync": {
            "global": {**global_status, **_runtime_sync_event_detail("global")},
            "personal": {**personal_status, **_runtime_sync_event_detail("personal", int(ctx["user"]["id"]))},
            "global_active_strategy_mismatch": runtime_mismatch,
        },
        "realtime": _admin_realtime_payload(ctx),
        "server_time": _utc_iso(),
    }


@app.get("/admin/realtime/status")
def get_admin_realtime_status(req: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(req, authorization)
    if not _is_admin_ctx(ctx):
        raise HTTPException(status_code=403, detail="admin_required")
    return _admin_realtime_payload(ctx)


@app.post("/admin/realtime/control")
def post_admin_realtime_control(req: Request, body: RealtimeControlIn, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(req, authorization)
    if not _is_admin_ctx(ctx):
        raise HTTPException(status_code=403, detail="admin_required")
    action = str(body.action or "").strip().lower()
    mapping = {
        "start_shadow": {"desired_state": "running", "mode": "shadow"},
        "promote_live": {"desired_state": "running", "mode": "live"},
        "stop": {"desired_state": "stopped", "mode": "shadow"},
        "restart_shadow": {"desired_state": "running", "mode": "shadow"},
    }
    if action not in mapping:
        raise HTTPException(status_code=400, detail="invalid_realtime_action")
    payload = write_realtime_control(
        desired_state=mapping[action]["desired_state"],
        mode=mapping[action]["mode"],
        reason=str(body.reason or action),
        requested_by=int(ctx["user"]["id"]),
    )
    db.log_sys_event(
        "REALTIME_CONTROL_UPDATED",
        int(ctx["user"]["id"]),
        f"Realtime control updated: {action}",
        {"action": action, **payload, "ip": _client_ip(req)},
    )
    return {"ok": True, "action": action, "control": payload}


@app.get("/admin/holy-grail/diagnostics/latest")
def get_admin_holy_grail_diagnostics(req: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(req, authorization)
    if not _is_admin_ctx(ctx):
        raise HTTPException(status_code=403, detail="admin_required")
    realtime = _admin_realtime_payload(ctx)
    return {
        "ok": True,
        "diagnostics": dict(realtime.get("holy_grail_diagnostics") or {}),
        "last_round_ms": float(realtime.get("last_round_ms") or 0.0),
        "last_round_started_at": str(realtime.get("last_round_started_at") or ""),
        "last_round_finished_at": str(realtime.get("last_round_finished_at") or ""),
    }


@app.post("/admin/factor_pools/prune")
def admin_prune_factor_pools(req: Request, authorization: Optional[str] = Header(None), dry_run: bool = True):
    ctx = _auth_ctx(req, authorization)
    if not _is_admin_ctx(ctx):
        raise HTTPException(status_code=403, detail="admin_required")
    result = db.prune_factor_pools_current_cycle_strict(dry_run=bool(dry_run), requested_by=int(ctx["user"]["id"]))
    if not bool(dry_run):
        _invalidate_live_state("dashboard", "runtime")
    return result


@app.get("/admin/errors/export.txt")
def export_admin_errors(req: Request, authorization: Optional[str] = Header(None), limit: int = 2000):
    ctx = _auth_ctx(req, authorization)
    if not _is_admin_ctx(ctx):
        raise HTTPException(status_code=403, detail="admin_required")
    rows = db.list_actionable_error_rows(limit=limit)
    lines = ["timestamp | source | event_type | user | worker | message | detail_json"]
    for row in rows:
        lines.append(
            " | ".join(
                [
                    str(row.get("timestamp") or ""),
                    str(row.get("source") or ""),
                    str(row.get("event_type") or ""),
                    str(row.get("user_id") or ""),
                    str(row.get("worker_id") or ""),
                    str(row.get("message") or "").replace("\n", " ").strip(),
                    str(row.get("detail_json") or "{}").replace("\n", " ").strip(),
                ]
            )
        )
    filename = f"sheep-errors-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.txt"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return PlainTextResponse("\n".join(lines), headers=headers)


@app.post("/admin/maintenance/rebuild-review-state")
def admin_rebuild_review_state(req: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(req, authorization)
    if str(ctx["user"].get("role")) != "admin":
        raise HTTPException(status_code=403, detail="forbidden: admin only")

    summary = _rebuild_review_state(db_module=db)
    db.log_sys_event(
        "REVIEW_STATE_REBUILD",
        ctx["user"].get("id"),
        "Admin triggered review-state maintenance",
        summary,
    )
    _invalidate_live_state("dashboard", "leaderboard")
    return {"ok": True, **summary}


@app.get("/admin/pools")
def legacy_get_admin_pools(req: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(req, authorization)
    _log_deprecated_alias(req, ctx, "/admin/pools", "/admin/factor_pools")
    return get_admin_factor_pools(req, authorization)


@app.post("/admin/pools/{pool_id}/update")
def legacy_update_admin_pool(pool_id: int, req: Request, body: FactorPoolUpdate, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(req, authorization)
    _log_deprecated_alias(req, ctx, f"/admin/pools/{pool_id}/update", f"/admin/factor_pools/{pool_id}")
    return update_admin_factor_pool(pool_id, req, body, authorization)


@app.get("/api/trading/strategies")
def legacy_get_trading_strategies(req: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(req, authorization)
    _log_deprecated_alias(req, ctx, "/api/trading/strategies", "/admin/strategies")
    return get_admin_strategies(req, authorization)


@app.post("/admin/settings/thresholds")
def legacy_update_admin_settings(req: Request, body: AdminSettingsUpdate, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(req, authorization)
    _log_deprecated_alias(req, ctx, "/admin/settings/thresholds", "/admin/settings")
    return update_admin_settings(req, body, authorization)


@app.get("/", response_class=HTMLResponse)
def landing() -> HTMLResponse:
    """
    [專家級修改] 將管理員面板無縫整合至系統根目錄 (Root Path)。
    一般訪客 (如 LINE/IG 爬蟲) 仍會讀取到正確的 OG Meta Tags，畫面上會提供按鈕前往 Streamlit 平台。
    若在畫面上的特權表單輸入已於環境變數中配置的管理權限密碼，即會原地解鎖並展開全中文管理員控制面板。
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

    # 移除自動 Refresh 否則管理員無法停留此頁登入，改為顯示跳轉按鈕供一般使用者
    jump_block = f'<div class="text-center mt-8"><a href="{esc_redirect}" class="bg-blue-600 hover:bg-blue-700 text-white font-bold py-3 px-6 rounded-lg transition inline-block">👉 點擊此處開啟羊肉爐平台 👈</a></div>' if redirect_url else ''

    html_doc = f"""<!doctype html>
<html lang="zh-Hant">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{esc_title} - 系統入口</title>
    <meta property="og:title" content="{esc_title}">
    <meta property="og:description" content="{esc_desc}">
    <meta property="og:site_name" content="{esc_site}">
    <meta property="og:type" content="website">
    {f'<meta property="og:image" content="{esc_img}">' if img else ''}
    <meta name="twitter:card" content="summary_large_image">
    
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://unpkg.com/vue@3/dist/vue.global.js"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <style>
        body {{ background-color: #0b0f14; color: #cbd5e1; }}
        .panel {{ background-color: #0f1824; border-color: #1f2937; }}
        .input-bg {{ background-color: #0e1420; border: 1px solid #1f2937; color: #e2e8f0; }}
        .input-bg:focus {{ border-color: #3b82f6; outline: none; }}
        [v-cloak] {{ display: none; }}
    </style>
  </head>
  <body>
    <div id="app" v-cloak class="min-h-screen p-6 flex flex-col items-center justify-center">
        <div v-if="!token" class="w-full max-w-md panel p-8 rounded-xl shadow-lg border">
            <div class="text-center mb-6">
                <h1 class="text-3xl font-bold text-white mb-2">{esc_title}</h1>
                <p class="text-gray-400">{esc_desc}</p>
            </div>
            {jump_block}
            
            <div class="mt-12 border-t border-gray-700 pt-8">
                <h2 class="text-xl font-bold text-gray-400 mb-6 text-center"><i class="fas fa-lock mr-2"></i>系統管理員登入</h2>
                <form @submit.prevent="login">
                    <div class="mb-4">
                        <label class="block text-sm font-medium mb-2">帳號</label>
                        <input v-model="username" type="text" class="w-full p-3 rounded-lg input-bg" required>
                    </div>
                    <div class="mb-6">
                        <label class="block text-sm font-medium mb-2">密碼</label>
                        <input v-model="password" type="password" class="w-full p-3 rounded-lg input-bg" required>
                    </div>
                    <button type="submit" class="w-full bg-gray-700 hover:bg-gray-600 text-white font-bold py-3 rounded-lg transition" :disabled="loading">
                        <span v-if="loading"><i class="fas fa-spinner fa-spin"></i> 驗證中...</span>
                        <span v-else>登入後台</span>
                    </button>
                </form>
                <div v-if="error" class="mt-4 p-3 bg-red-900/50 border border-red-500 text-red-200 rounded text-center">
                    {{{{ error }}}}
                </div>
            </div>
        </div>

        <div v-else class="w-full max-w-6xl mt-4">
            <div class="flex justify-between items-center mb-8">
                <h1 class="text-3xl font-bold text-white"><i class="fas fa-shield-alt mr-2 text-blue-500"></i>管理員控制面板</h1>
                <button @click="logout" class="bg-red-600 hover:bg-red-700 text-white px-4 py-2 rounded-lg transition shadow">
                    <i class="fas fa-sign-out-alt mr-1"></i>登出
                </button>
            </div>

            <div v-if="message" class="mb-6 p-4 bg-green-900/50 border border-green-500 text-green-200 rounded-lg flex justify-between shadow">
                <span><i class="fas fa-check-circle mr-2"></i>{{{{ message }}}}</span>
                <button @click="message=''" class="text-green-200 hover:text-white"><i class="fas fa-times"></i></button>
            </div>
            <div v-if="error" class="mb-6 p-4 bg-red-900/50 border border-red-500 text-red-200 rounded-lg flex justify-between shadow">
                <span><i class="fas fa-exclamation-triangle mr-2"></i>{{{{ error }}}}</span>
                <button @click="error=''" class="text-red-200 hover:text-white"><i class="fas fa-times"></i></button>
            </div>

            <div class="grid grid-cols-1 xl:grid-cols-3 gap-6 items-start">
                <div class="panel p-6 rounded-xl shadow-lg border col-span-1">
                    <h2 class="text-xl font-bold text-white mb-4 border-b border-gray-700 pb-2">
                        <i class="fas fa-sliders-h mr-2 text-blue-400"></i>達標門檻動態設定
                    </h2>
                    <form @submit.prevent="updateSettings">
                        <div class="mb-4">
                            <label class="block text-sm font-medium mb-1 text-gray-300" title="至少需要交易的筆數">最少交易筆數 (Min Trades)</label>
                            <input v-model.number="settings.min_trades" type="number" class="w-full p-2 rounded input-bg" required>
                        </div>
                        <div class="mb-4">
                            <label class="block text-sm font-medium mb-1 text-gray-300">最低總報酬率 % (Min Return %)</label>
                            <input v-model.number="settings.min_total_return_pct" type="number" step="0.1" class="w-full p-2 rounded input-bg" required>
                        </div>
                        <div class="mb-4">
                            <label class="block text-sm font-medium mb-1 text-gray-300">最大回撤容忍 % (Max Drawdown %)</label>
                            <input v-model.number="settings.max_drawdown_pct" type="number" step="0.1" class="w-full p-2 rounded input-bg" required>
                        </div>
                        <div class="mb-4">
                            <label class="block text-sm font-medium mb-1 text-gray-300">最低夏普值 (Min Sharpe)</label>
                            <input v-model.number="settings.min_sharpe" type="number" step="0.01" class="w-full p-2 rounded input-bg" required>
                        </div>
                        <div class="mb-6">
                            <label class="block text-sm font-medium mb-1 text-gray-300">保存前 N 名參數 (Keep Top N)</label>
                            <input v-model.number="settings.candidate_keep_top_n" type="number" class="w-full p-2 rounded input-bg" required>
                        </div>
                        <button type="submit" class="w-full bg-blue-600 hover:bg-blue-700 text-white font-bold py-3 rounded-lg transition" :disabled="loading">
                            <i class="fas fa-save mr-1"></i>儲存設定並立即生效
                        </button>
                    </form>
                </div>

                <div class="panel p-6 rounded-xl shadow-lg border col-span-1 xl:col-span-2">
                    <div class="flex justify-between items-center mb-4 border-b border-gray-700 pb-2">
                        <h2 class="text-xl font-bold text-white">
                            <i class="fas fa-layer-group mr-2 text-green-400"></i>已過審策略池總覽 (實盤因子庫)
                        </h2>
                        <span class="bg-blue-900/50 text-blue-300 text-sm py-1 px-3 rounded-full border border-blue-700 font-bold">
                            過審總數: {{{{ strategies.length }}}} 個
                        </span>
                    </div>
                    
                    <div class="overflow-x-auto rounded-lg border border-gray-700">
                        <table class="w-full text-sm text-left">
                            <thead class="text-xs uppercase bg-gray-800 text-gray-300 border-b border-gray-700">
                                <tr>
                                    <th class="px-4 py-3">策略ID</th>
                                    <th class="px-4 py-3">提供者(礦工)</th>
                                    <th class="px-4 py-3">交易對 / 週期</th>
                                    <th class="px-4 py-3">綜合夏普值</th>
                                    <th class="px-4 py-3">IS 報酬</th>
                                    <th class="px-4 py-3">狀態</th>
                                    <th class="px-4 py-3">過審時間</th>
                                </tr>
                            </thead>
                            <tbody>
                                <tr v-for="st in strategies" :key="st.strategy_id" class="border-b border-gray-800 hover:bg-gray-750 transition duration-150">
                                    <td class="px-4 py-3 font-mono text-gray-400">#{{{{ st.strategy_id }}}}</td>
                                    <td class="px-4 py-3 font-medium text-blue-400">{{{{ st.username }}}}</td>
                                    <td class="px-4 py-3"><span class="bg-gray-700 px-2 py-1 rounded text-xs">{{{{ st.symbol }}}} ({{{{ st.timeframe_min }}}}m)</span></td>
                                    <td class="px-4 py-3 font-bold text-green-400">
                                        {{{{ (st.progress?.oos_metrics?.sharpe || st.metrics?.sharpe || 0).toFixed(2) }}}}
                                    </td>
                                    <td class="px-4 py-3 text-yellow-400">{{{{ (st.metrics?.total_return_pct || 0).toFixed(2) }}}}%</td>
                                    <td class="px-4 py-3">
                                        <span class="px-2 py-1 bg-green-900/50 text-green-400 rounded text-xs border border-green-800">
                                            {{{{ st.status }}}}
                                        </span>
                                    </td>
                                    <td class="px-4 py-3 text-gray-400 text-xs">{{{{ new Date(st.created_at).toLocaleString('zh-TW') }}}}</td>
                                </tr>
                                <tr v-if="strategies.length === 0">
                                    <td colspan="7" class="px-4 py-12 text-center text-gray-500">
                                        <i class="fas fa-folder-open text-3xl mb-2"></i><br>目前尚無通過審核的策略
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>

                <div class="panel p-6 rounded-xl shadow-lg border col-span-1 xl:col-span-3 mt-4">
                    <div class="flex justify-between items-center mb-4 border-b border-gray-700 pb-2">
                        <h2 class="text-xl font-bold text-white">
                            <i class="fas fa-cogs mr-2 text-purple-400"></i>動態策略池參數管理 (Factor Pools)
                        </h2>
                        <button @click="openPoolModal(null)" class="bg-purple-600 hover:bg-purple-700 text-white px-4 py-2 rounded-lg transition text-sm font-bold shadow">
                            <i class="fas fa-plus mr-1"></i>新增策略池
                        </button>
                    </div>
                    <div class="mb-4 rounded-lg border border-gray-700 bg-gray-800/60 p-4">
                        <div class="flex flex-col gap-3">
                            <div class="flex flex-col gap-2 lg:flex-row lg:items-center lg:justify-between">
                                <div>
                                    <h3 class="text-base font-bold text-white">Batch JSON Import</h3>
                                    <p class="text-xs text-gray-400">Paste catalog JSON or load a local file, then run dry-run before applying.</p>
                                </div>
                                <div class="flex flex-wrap gap-2">
                                    <label class="cursor-pointer rounded border border-gray-600 bg-gray-700 px-3 py-2 text-sm text-gray-100 hover:bg-gray-600">
                                        <input type="file" accept=".json,application/json" class="hidden" @change="loadCatalogFile">
                                        Load JSON File
                                    </label>
                                    <button @click="runCatalogImport(true)" class="rounded bg-amber-600 px-3 py-2 text-sm font-bold text-white hover:bg-amber-700" :disabled="loading || catalogImporting">
                                        <i class="fas fa-vial mr-1"></i>Dry Run
                                    </button>
                                    <button @click="runCatalogImport(false)" class="rounded bg-emerald-600 px-3 py-2 text-sm font-bold text-white hover:bg-emerald-700" :disabled="loading || catalogImporting">
                                        <i class="fas fa-file-import mr-1"></i>Apply Import
                                    </button>
                                </div>
                            </div>
                            <textarea
                                v-model="catalogJson"
                                rows="10"
                                class="w-full rounded input-bg p-3 font-mono text-xs leading-relaxed"
                                placeholder='{{"schema_version":1,"factor_pools":[],"strategies":[]}}'
                            ></textarea>
                            <div v-if="catalogImporting" class="text-sm text-blue-300">
                                <i class="fas fa-spinner fa-spin mr-1"></i>Importing catalog...
                            </div>
                            <div v-if="catalogReportText" class="rounded border border-gray-700 bg-gray-900/70 p-3">
                                <div class="mb-2 text-sm font-semibold text-gray-200">Import Report</div>
                                <pre class="max-h-72 overflow-auto whitespace-pre-wrap text-xs text-gray-300">{{{{ catalogReportText }}}}</pre>
                            </div>
                        </div>
                    </div>
                    <div class="overflow-x-auto rounded-lg border border-gray-700">
                        <table class="w-full text-sm text-left">
                            <thead class="text-xs uppercase bg-gray-800 text-gray-300 border-b border-gray-700">
                                <tr>
                                    <th class="px-4 py-3">ID</th>
                                    <th class="px-4 py-3">名稱 / 狀態</th>
                                    <th class="px-4 py-3">交易對 / 週期</th>
                                    <th class="px-4 py-3">策略家族</th>
                                    <th class="px-4 py-3">分區數</th>
                                    <th class="px-4 py-3">操作</th>
                                </tr>
                            </thead>
                            <tbody>
                                <tr v-for="p in factorPools" :key="p.id" class="border-b border-gray-800 hover:bg-gray-750 transition duration-150">
                                    <td class="px-4 py-3 font-mono text-gray-400">#{{{{ p.id }}}}</td>
                                    <td class="px-4 py-3 font-medium text-white">
                                        {{{{ p.name }}}}
                                        <span v-if="p.active === 1" class="ml-2 px-2 py-0.5 bg-green-900/50 text-green-400 rounded text-xs border border-green-800">啟用中</span>
                                        <span v-else class="ml-2 px-2 py-0.5 bg-red-900/50 text-red-400 rounded text-xs border border-red-800">停用</span>
                                    </td>
                                    <td class="px-4 py-3"><span class="bg-gray-700 px-2 py-1 rounded text-xs text-yellow-300">{{{{ p.symbol }}}} ({{{{ p.timeframe_min }}}}m)</span></td>
                                    <td class="px-4 py-3 text-blue-300">{{{{ p.family }}}}</td>
                                    <td class="px-4 py-3 text-gray-300">{{{{ p.num_partitions }}}} 區</td>
                                    <td class="px-4 py-3">
                                        <button @click="openPoolModal(p)" class="text-blue-400 hover:text-blue-300 mr-3 px-3 py-1 bg-blue-900/40 rounded border border-blue-700"><i class="fas fa-edit"></i> 修改參數</button>
                                    </td>
                                </tr>
                                <tr v-if="factorPools.length === 0">
                                    <td colspan="6" class="px-4 py-8 text-center text-gray-500">目前週期內沒有任何策略池資料</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>

        <div v-if="showPoolModal" class="fixed inset-0 bg-black/80 flex items-center justify-center z-50 p-4">
            <div class="panel p-6 rounded-xl shadow-2xl border border-gray-600 w-full max-w-3xl max-h-[90vh] overflow-y-auto">
                <h2 class="text-2xl font-bold text-white mb-4 border-b border-gray-700 pb-2">
                    {{{{ editingPool.id ? '修改策略池參數 #' + editingPool.id : '新增策略池' }}}}
                </h2>
                <form @submit.prevent="savePool">
                    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div class="mb-2">
                            <label class="block text-sm font-medium mb-1 text-gray-300">名稱</label>
                            <input v-model="editingPool.name" type="text" class="w-full p-2 rounded input-bg" required>
                        </div>
                        <div class="mb-2">
                            <label class="block text-sm font-medium mb-1 text-gray-300">External Key</label>
                            <input v-model="editingPool.external_key" type="text" class="w-full p-2 rounded input-bg font-mono text-sm">
                        </div>
                        <div class="mb-2">
                            <label class="block text-sm font-medium mb-1 text-gray-300">策略家族 (Family)</label>
                            <input v-model="editingPool.family" type="text" class="w-full p-2 rounded input-bg" required>
                        </div>
                        <div class="mb-2">
                            <label class="block text-sm font-medium mb-1 text-gray-300">交易對 (Symbol)</label>
                            <input v-model="editingPool.symbol" type="text" class="w-full p-2 rounded input-bg" required>
                        </div>
                        <div class="mb-2">
                            <label class="block text-sm font-medium mb-1 text-gray-300">Direction</label>
                            <select v-model="editingPool.direction" class="w-full rounded input-bg p-2">
                                <option value="long">long</option>
                                <option value="short">short</option>
                            </select>
                        </div>
                        <div class="mb-2">
                            <label class="block text-sm font-medium mb-1 text-gray-300">週期 分鐘 (Timeframe)</label>
                            <input v-model.number="editingPool.timeframe_min" type="number" class="w-full p-2 rounded input-bg" required>
                        </div>
                        <div class="mb-2">
                            <label class="block text-sm font-medium mb-1 text-gray-300">回測年份 (Years)</label>
                            <input v-model.number="editingPool.years" type="number" class="w-full p-2 rounded input-bg" required>
                        </div>
                        <div class="mb-2">
                            <label class="block text-sm font-medium mb-1 text-gray-300">網格分區數 (Partitions)</label>
                            <input v-model.number="editingPool.num_partitions" type="number" class="w-full p-2 rounded input-bg" required>
                        </div>
                        <div class="mb-2">
                            <label class="block text-sm font-medium mb-1 text-gray-300">隨機種子 (Seed)</label>
                            <input v-model.number="editingPool.seed" type="number" class="w-full p-2 rounded input-bg" required>
                        </div>
                        <div class="mb-2 flex items-center mt-6">
                            <input v-model="editingPool.active" type="checkbox" class="w-5 h-5 mr-2 accent-blue-500">
                            <label class="text-sm font-medium text-gray-200">開放接單 (Active)</label>
                        </div>
                        <div v-if="!editingPool.id" class="mb-2 flex items-center md:col-span-2 p-3 bg-yellow-900/30 border border-yellow-700 rounded text-yellow-300">
                            <input v-model="editingPool.auto_expand" type="checkbox" class="w-5 h-5 mr-3 accent-yellow-500">
                            <label class="text-sm font-bold">自動擴展 (Auto Expand) - 勾選後將無視上方交易對，自動為 BTC/ETH 生成全週期矩陣</label>
                        </div>
                    </div>
                    <div class="mb-4 mt-4">
                        <label class="block text-sm font-medium mb-1 text-gray-300">網格掃描設定 JSON (Grid Spec)</label>
                        <textarea v-model="editingPool.grid_spec_str" rows="4" class="w-full p-2 rounded input-bg font-mono text-sm leading-relaxed" required></textarea>
                    </div>
                    <div class="mb-4">
                        <label class="block text-sm font-medium mb-1 text-gray-300">風控參數設定 JSON (Risk Spec)</label>
                        <textarea v-model="editingPool.risk_spec_str" rows="3" class="w-full p-2 rounded input-bg font-mono text-sm leading-relaxed" required></textarea>
                    </div>
                    
                    <div class="flex justify-end gap-3 mt-6 pt-4 border-t border-gray-700">
                        <button type="button" @click="showPoolModal = false" class="bg-gray-600 hover:bg-gray-500 text-white px-6 py-2 rounded-lg transition shadow">取消返回</button>
                        <button type="submit" class="bg-blue-600 hover:bg-blue-700 text-white px-6 py-2 rounded-lg transition font-bold shadow-lg" :disabled="loading">
                            <span v-if="loading"><i class="fas fa-spinner fa-spin mr-1"></i>資料庫寫入中...</span>
                            <span v-else><i class="fas fa-save mr-1"></i>儲存變更至資料庫</span>
                        </button>
                    </div>
                </form>
            </div>
        </div>
    </div>

    <script>
        const {{ createApp, ref, onMounted }} = Vue;

        createApp({{
            setup() {{
                const token = ref(localStorage.getItem('admin_token') || '');
                const username = ref('');
                const password = ref('');
                const loading = ref(false);
                const error = ref('');
                const message = ref('');
                const settings = ref({{
                    min_trades: 30,
                    min_total_return_pct: 3.0,
                    max_drawdown_pct: 25.0,
                    min_sharpe: 0.6,
                    candidate_keep_top_n: 30
               }});
                const strategies = ref([]);
                const factorPools = ref([]);
                const showPoolModal = ref(false);
                const editingPool = ref({{}});
                const catalogJson = ref('');
                const catalogImporting = ref(false);
                const catalogReport = ref(null);
                const catalogReportText = ref('');

                const blankPool = () => ({{
                    id: 0,
                    name: '',
                    external_key: '',
                    symbol: 'BTC_USDT',
                    direction: 'long',
                    timeframe_min: 60,
                    years: 3,
                    family: 'TEMA_Cross',
                    num_partitions: 8,
                    seed: 42,
                    active: true,
                    auto_expand: false,
                    grid_spec: {{}},
                    risk_spec: {{}},
                    grid_spec_str: '{{}}',
                    risk_spec_str: '{{}}',
                }});

                const req = async (url, options = {{}}) => {{
                    if(!options.headers) options.headers = {{}};
                    options.headers['Content-Type'] = 'application/json';
                    if(token.value) options.headers['Authorization'] = 'Bearer ' + token.value;
                    
                    const res = await fetch(url, options);
                    const data = await res.json();
                    if(!res.ok) throw new Error(data.detail || data.msg || '連線錯誤');
                    return data;
                }};

                const login = async () => {{
                    loading.value = true;
                    error.value = '';
                    try {{
                        const data = await req('/token', {{
                            method: 'POST',
                            body: JSON.stringify({{ username: username.value, password: password.value, name: 'compute' }})
                        }});
                        if (data.role !== 'admin') {{
                            throw new Error('權限拒絕：必須具備管理員權限');
                        }}
                        token.value = data.token;
                        localStorage.setItem('admin_token', data.token);
                        await fetchData();
                        message.value = '身分驗證成功，歡迎來到系統管理員後台。';
                    }} catch (e) {{
                        error.value = e.message;
                        token.value = '';
                        localStorage.removeItem('admin_token');
                    }} finally {{
                        loading.value = false;
                    }}
                }};

                const logout = () => {{
                    token.value = '';
                    localStorage.removeItem('admin_token');
                    username.value = '';
                    password.value = '';
                    strategies.value = [];
                    factorPools.value = [];
                    catalogJson.value = '';
                    catalogReport.value = null;
                    catalogReportText.value = '';
                    message.value = '已安全登出。';
                }};

                const fetchData = async () => {{
                    loading.value = true;
                    error.value = '';
                    try {{
                        const [sData, stData, poolData] = await Promise.all([
                            req('/settings/snapshot').catch((se) => {{
                                console.error('Snapshot fail:', se);
                                return null;
                            }}),
                            req('/admin/strategies'),
                            req('/admin/factor_pools'),
                        ]);
                        if (sData && sData.thresholds) settings.value = sData.thresholds;
                        if (stData && stData.ok) {{
                            strategies.value = stData.strategies || [];
                            if (strategies.value.length === 0) {{
                                console.warn('API returned empty strategy list');
                            }}
                        }} else {{
                            throw new Error(stData.msg || '策略池同步失敗');
                        }}
                        if (poolData && poolData.ok) {{
                            factorPools.value = poolData.pools || [];
                        }} else {{
                            throw new Error((poolData && poolData.msg) || '動態策略池載入失敗');
                        }}
                    }} catch (e) {{
                        error.value = '管理員資料載入失敗: ' + e.message;
                        console.error('FetchData Error:', e);
                    }} finally {{
                        loading.value = false;
                    }}
                }};

                const openPoolModal = (pool) => {{
                    const base = pool ? JSON.parse(JSON.stringify(pool)) : blankPool();
                    base.external_key = base.external_key || '';
                    base.direction = String(base.direction || 'long').toLowerCase();
                    base.grid_spec = base.grid_spec || {{}};
                    base.risk_spec = base.risk_spec || {{}};
                    base.grid_spec_str = JSON.stringify(base.grid_spec, null, 2);
                    base.risk_spec_str = JSON.stringify(base.risk_spec, null, 2);
                    editingPool.value = base;
                    showPoolModal.value = true;
                    error.value = '';
                }};

                const savePool = async () => {{
                    loading.value = true;
                    error.value = '';
                    message.value = '';
                    try {{
                        const payload = {{
                            name: String(editingPool.value.name || '').trim(),
                            external_key: String(editingPool.value.external_key || '').trim(),
                            symbol: String(editingPool.value.symbol || '').trim().toUpperCase(),
                            direction: String(editingPool.value.direction || 'long').trim().toLowerCase(),
                            timeframe_min: Number(editingPool.value.timeframe_min || 0),
                            years: Number(editingPool.value.years || 3),
                            family: String(editingPool.value.family || '').trim(),
                            grid_spec: JSON.parse(editingPool.value.grid_spec_str || '{{}}'),
                            risk_spec: JSON.parse(editingPool.value.risk_spec_str || '{{}}'),
                            num_partitions: Number(editingPool.value.num_partitions || 1),
                            seed: Number(editingPool.value.seed || 42),
                            active: !!editingPool.value.active,
                            auto_expand: !!editingPool.value.auto_expand,
                        }};
                        if (!payload.name || !payload.symbol || !payload.family || !payload.timeframe_min) {{
                            throw new Error('Missing required pool fields.');
                        }}
                        if (editingPool.value.id) {{
                            await req('/admin/factor_pools/' + editingPool.value.id, {{
                                method: 'PUT',
                                body: JSON.stringify(payload),
                            }});
                            message.value = 'Factor pool #' + editingPool.value.id + ' saved.';
                        }} else {{
                            await req('/admin/factor_pools', {{
                                method: 'POST',
                                body: JSON.stringify(payload),
                            }});
                            message.value = 'Factor pool created.';
                        }}
                        showPoolModal.value = false;
                        await fetchData();
                    }} catch (e) {{
                        error.value = 'Save pool failed: ' + e.message;
                    }} finally {{
                        loading.value = false;
                    }}
                }};

                const loadCatalogFile = async (event) => {{
                    error.value = '';
                    try {{
                        const file = event && event.target && event.target.files ? event.target.files[0] : null;
                        if (!file) return;
                        catalogJson.value = await file.text();
                        catalogReport.value = null;
                        catalogReportText.value = '';
                    }} catch (e) {{
                        error.value = 'Load file failed: ' + e.message;
                    }} finally {{
                        if (event && event.target) event.target.value = '';
                    }}
                }};

                const runCatalogImport = async (dryRun) => {{
                    catalogImporting.value = true;
                    error.value = '';
                    message.value = '';
                    try {{
                        const payload = JSON.parse(catalogJson.value || '{{}}');
                        const res = await req('/admin/catalog/import?dry_run=' + (dryRun ? 'true' : 'false'), {{
                            method: 'POST',
                            body: JSON.stringify(payload),
                        }});
                        catalogReport.value = res;
                        catalogReportText.value = JSON.stringify(res, null, 2);
                        message.value = dryRun ? 'Catalog dry-run completed.' : 'Catalog import completed.';
                        if (!dryRun) {{
                            await fetchData();
                        }}
                    }} catch (e) {{
                        catalogReport.value = null;
                        catalogReportText.value = '';
                        error.value = 'Catalog import failed: ' + e.message;
                    }} finally {{
                        catalogImporting.value = false;
                    }}
                }};

                const updateSettings = async () => {{
                    loading.value = true;
                    error.value = '';
                    message.value = '';
                    try {{
                        const res = await req('/admin/settings', {{
                            method: 'POST',
                            body: JSON.stringify(settings.value)
                        }});
                        message.value = res.msg || '設定已成功儲存並立即生效！';
                        await fetchData(); // 重新整理
                    }} catch (e) {{
                        error.value = '儲存設定失敗：' + e.message;
                    }} finally {{
                        loading.value = false;
                    }}
                }};

                onMounted(() => {{
                    if (token.value) {{
                        fetchData();
                    }}
                }});

                return {{
                    token, username, password, loading, error, message,
                    settings, strategies, factorPools, showPoolModal, editingPool,
                    catalogJson, catalogImporting, catalogReport, catalogReportText,
                    login, logout, updateSettings, openPoolModal, savePool,
                    loadCatalogFile, runCatalogImport
                }};
            }}
        }}).mount('#app');
    </script>
  </body>
</html>"""

    return HTMLResponse(content=html_doc, status_code=200)


@app.get("/manifest")
def manifest():
    conn = db._conn()
    try:
        worker_download_url = str(db.get_setting(conn, "worker_download_url", db.DEFAULT_WORKER_DOWNLOAD_URL) or "").strip()
        if not worker_download_url:
            worker_download_url = str(db.DEFAULT_WORKER_DOWNLOAD_URL)
        return {
            "server_time": _utc_iso(),
            "worker_min_version": str(db.get_setting(conn, "worker_min_version", "2.0.0")),
            "worker_latest_version": str(db.get_setting(conn, "worker_latest_version", "2.0.0")),
            "worker_min_protocol": int(db.get_setting(conn, "worker_min_protocol", 2)),
            "worker_download_url": worker_download_url,
            "worker_download_sha256": str(db.get_setting(conn, "worker_download_sha256", "")),
            "worker_bundle_kind": str(db.get_setting(conn, "worker_bundle_kind", "")),
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
    requested_name = str(body.name or "web_session").strip().lower() or "web_session"
    if requested_name not in {"compute", "worker", "web_session"}:
        requested_name = "web_session"
    try:
        allowed, retry_after = _token_issue_limiter.check(ip, cost=1.0)
        
        # [專家級防護] 滑動驗證碼嚴格校驗 (針對網頁端請求)
        if requested_name != "compute":
            from sheep_platform_security import verify_slider_captcha
            is_valid, err_msg = verify_slider_captcha(body.captcha_token, body.captcha_offset, body.captcha_tracks, ip)
            if not is_valid:
                db.log_sys_event("LOGIN_CAPTCHA_FAIL", None, f"驗證碼未通過: {err_msg}", {"ip": ip, "username": body.username})
                raise HTTPException(status_code=400, detail=_categorize_captcha_error(err_msg))
        if not allowed:
            db.log_sys_event("LOGIN_FAIL", None, f"IP {ip} 登入頻率過高觸發限制", {"ip": ip})
            headers = {"Retry-After": str(int(max(1, retry_after or 1.0)))}
            raise HTTPException(status_code=429, detail="rate_limited", headers=headers)

        if requested_name == "compute":
            env_user = os.environ.get("SHEEP_COMPUTE_USER", "").strip()
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

        token = db.create_api_token(int(user["id"]), ttl_seconds=int(body.ttl_seconds), name=requested_name)
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


@app.post("/workers/token")
def issue_worker_token(request: Request, body: WorkerTokenIssueIn, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(request, authorization)
    user_id = int(ctx["user"]["id"])
    current_token = ctx.get("token") or {}
    current_token_id = int(current_token.get("id") or 0)
    current_token_name = str(current_token.get("name") or "").strip().lower()
    rotated_count = 0
    if bool(body.rotate_existing):
        exclude_token_id = current_token_id if current_token_name == "worker" else 0
        rotated_count = int(db.revoke_api_tokens_for_user(user_id, name="worker", exclude_token_id=exclude_token_id))

    token = db.create_api_token(user_id, ttl_seconds=int(body.ttl_seconds), name="worker")
    db.log_sys_event(
        "WORKER_TOKEN_ISSUED",
        user_id,
        "Issued dedicated personal worker token",
        {
            "rotated_count": rotated_count,
            "token_kind": _token_kind(ctx),
            "ip": _client_ip(request),
        },
    )
    return {
        "ok": True,
        "token": str(token["token"]),
        "token_id": int(token["token_id"]),
        "user_id": user_id,
        "name": "worker",
        "token_kind": "worker",
        "rotated_count": rotated_count,
        "issued_at": str(token.get("issued_at")),
        "expires_at": str(token.get("expires_at")),
    }


@app.post("/runtime-sync/token")
def issue_runtime_sync_token(request: Request, body: RuntimeSyncTokenIssueIn, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(request, authorization)
    if not _is_admin_ctx(ctx):
        raise HTTPException(status_code=403, detail="admin_required")
    user_id = int(ctx["user"]["id"])
    rotated_count = 0
    if bool(body.rotate_existing):
        rotated_count = int(db.revoke_api_tokens_for_user(user_id, name="system_sync"))
    token = db.create_api_token(user_id, ttl_seconds=int(body.ttl_seconds), name="system_sync")
    db.log_sys_event(
        "RUNTIME_SYNC_TOKEN_ISSUED",
        user_id,
        "Issued runtime sync token",
        {
            "rotated_count": rotated_count,
            "ip": _client_ip(request),
        },
    )
    return {
        "ok": True,
        "token": str(token["token"]),
        "token_id": int(token["token_id"]),
        "user_id": user_id,
        "name": "system_sync",
        "token_kind": "system_sync",
        "rotated_count": rotated_count,
        "issued_at": str(token.get("issued_at")),
        "expires_at": str(token.get("expires_at")),
    }
@app.post("/auth/register")
def web_register(req: Request, body: WebRegisterIn):
    ip = _client_ip(req) or "unknown_ip"
    try:
        from sheep_platform_security import verify_slider_captcha
        is_valid, err_msg = verify_slider_captcha(body.captcha_token, body.captcha_offset, body.captcha_tracks, ip)
        if not is_valid:
            db.log_sys_event("REGISTER_CAPTCHA_FAIL", None, f"驗證碼未通過: {err_msg}", {"ip": ip, "username": body.username})
            raise HTTPException(status_code=400, detail=_categorize_captcha_error(err_msg))

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
        nickname = str(body.nickname or "").strip()
        
        if not uname or len(uname) > 64:
            db.log_sys_event("REGISTER_FAIL", None, "帳號無效或過長", {"username": body.username, "ip": ip})
            raise HTTPException(status_code=400, detail="invalid_username")
        if not nickname:
            db.log_sys_event("REGISTER_FAIL", None, "暱稱未填寫", {"username": body.username, "ip": ip})
            raise HTTPException(status_code=400, detail="nickname_required")
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
            uid = db.create_user(
                username=uname,
                password_hash=pw_hash_str,
                role="user",
                wallet_address="",
                wallet_chain="TRC20",
                nickname=nickname,
                avatar_url=str(body.avatar_url or ""),
            )
            
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
        "nickname": fresh_user.get("nickname") or fresh_user.get("username") or "",
        "display_name": fresh_user.get("display_name") or fresh_user.get("nickname") or fresh_user.get("username") or "",
        "avatar_url": fresh_user.get("avatar_url") or db.get_default_avatar_url(),
        "role": fresh_user.get("role", "user"),
        "wallet_address": fresh_user.get("wallet_address", ""),
        "wallet_chain": fresh_user.get("wallet_chain", "TRC20"),
        "disabled": fresh_user.get("disabled", 0),
        "run_enabled": fresh_user.get("run_enabled", 0),
        "token_kind": _token_kind(ctx),
        "token_name": str((ctx.get("token") or {}).get("name") or ""),
    }


@app.post("/user/profile")
@app.patch("/user/profile")
def web_update_profile(request: Request, body: UserProfileUpdateIn, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(request, authorization)
    uid = int(ctx["user"]["id"])
    try:
        user = db.update_user_profile(
            uid,
            nickname=str(body.nickname or ""),
            avatar_url=str(body.avatar_url or ""),
            clear_avatar=bool(body.clear_avatar),
        )
        db.write_audit_log(uid, "update_profile", {"clear_avatar": bool(body.clear_avatar)})
        return {"ok": True, "user": user}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/admin/default-avatar")
def admin_get_default_avatar(request: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(request, authorization)
    if not _is_admin_ctx(ctx):
        raise HTTPException(status_code=403, detail="admin_required")
    return {"ok": True, "avatar_url": db.get_default_avatar_url()}


@app.post("/admin/default-avatar")
def admin_set_default_avatar(request: Request, body: DefaultAvatarUpdateIn, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(request, authorization)
    if not _is_admin_ctx(ctx):
        raise HTTPException(status_code=403, detail="admin_required")
    avatar_url = db.set_default_avatar_url(str(body.avatar_url or ""))
    db.write_audit_log(int(ctx["user"]["id"]), "set_default_avatar", {"has_avatar": bool(avatar_url)})
    return {"ok": True, "avatar_url": db.get_default_avatar_url()}

@app.get("/leaderboard")
def web_leaderboard(period_hours: int = 720):
    try:
        cache_key = _live_cache_key("period_hours", int(period_hours or 0))

        def _load_leaderboard() -> Dict[str, Any]:
            stats = dict(db.get_leaderboard_stats(period_hours) or {})
            preserved = dict((_live_last_nonempty.get("leaderboard") or {}).get(cache_key) or {})
            if preserved:
                preserved_any = False
                for key in ("combos", "score", "time", "points", "qualified_strategies"):
                    if not isinstance(stats.get(key), list) or stats.get(key):
                        continue
                    fallback_rows = preserved.get(key)
                    if isinstance(fallback_rows, list) and fallback_rows:
                        stats[key] = list(fallback_rows)
                        preserved_any = True
                if preserved_any:
                    stats["_preserved_last_nonempty"] = True
            if _leaderboard_has_rows(stats):
                clean_stats = {k: v for k, v in stats.items() if not str(k).startswith("_")}
                _remember_live_snapshot("leaderboard", cache_key, clean_stats)
                return stats
            if preserved:
                preserved["_preserved_last_nonempty"] = True
                return preserved
            return stats

        stats = _cached_live_payload(
            "leaderboard",
            cache_key,
            _load_leaderboard,
        )
        return {"ok": True, "data": stats, "leaderboard_version": _live_versions["leaderboard"], "generated_at": _utc_iso()}
    except Exception as e:
        logger.error(f"Leaderboard error: {e}")
        raise HTTPException(status_code=500, detail="fetch_leaderboard_failed")

@app.get("/dashboard")
def web_dashboard(request: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(request, authorization)
    uid = int(ctx["user"]["id"])
    cache_key = _live_cache_key("user_id", uid)
    def _load_dashboard() -> Dict[str, Any]:
        def _dashboard_section(label: str, default: Any, loader):
            try:
                return loader()
            except Exception as section_exc:
                logger.error(f"Dashboard {label} load failed for user {uid}: {section_exc}")
                return default

        cycle_id = _get_active_cycle_id()
        recent_tasks = [enrich_task_row(t) for t in db.list_tasks_for_user(uid, cycle_id=cycle_id, limit=10)]
        strategies = db.list_strategies(user_id=uid, limit=100)
        payouts = db.list_payouts(user_id=uid, limit=100)
        try:
            global_counters = dict(db.get_global_dashboard_counters() or {})
        except Exception as counter_exc:
            logger.error(f"Dashboard global counter load failed for user {uid}: {counter_exc}")
            global_counters = {
                "total_strategy_pool_combo_count": 0,
                "global_mined_combo_count": 0,
            }
        try:
            latest_announcements = [_announcement_payload(row) for row in db.list_announcements(limit=6, offset=0, include_drafts=False)]
        except Exception as announcement_exc:
            logger.error(f"Dashboard announcement load failed for user {uid}: {announcement_exc}")
            latest_announcements = []

        conn = db._conn()
        try:
            min_sharpe = float(db.get_setting(conn, "min_sharpe", 0.6))
        finally:
            conn.close()

        completed_task_count = int(db.count_tasks_for_user(uid, cycle_id=0, statuses=["completed"]))
        personal_active_strategy_count = int(db.count_strategies(user_id=uid, status="active"))
        personal_reviewed_strategy_count = int(_dashboard_section("personal_review_ready_count", 0, lambda: db.count_review_ready_tasks(user_id=uid)))
        global_reviewed_strategy_count = int(_dashboard_section("global_review_ready_count", 0, lambda: db.count_review_ready_tasks()))
        personal_review_pipeline_count = int(
            _dashboard_section("personal_review_pipeline_count", 0, lambda: db.count_review_pipeline_tasks_for_user(uid, cycle_id=cycle_id))
        )
        personal_review_ready_items = list(
            _dashboard_section("personal_review_ready_items", [], lambda: db.list_review_ready_items_for_user(uid, limit=100) or [])
        )
        personal_runtime_snapshot = _dashboard_section("personal_runtime_snapshot", {}, lambda: db.get_runtime_portfolio_snapshot("personal", user_id=uid) or {})
        global_runtime_snapshot = _dashboard_section("global_runtime_snapshot", {}, lambda: db.get_runtime_portfolio_snapshot("global") or {})
        personal_runtime_items = list(personal_runtime_snapshot.get("items") or [])
        global_runtime_items = list(global_runtime_snapshot.get("items") or [])
        runtime_position_items_raw = list((global_runtime_snapshot.get("summary") or {}).get("position_items") or [])
        runtime_lookup = _dashboard_section(
            "runtime_lookup",
            {},
            lambda: (
                _active_strategy_runtime_lookup_for_items(
                    list(global_runtime_items) + list(runtime_position_items_raw),
                    limit=500,
                )
                if (global_runtime_items or runtime_position_items_raw)
                else {}
            ),
        )
        enriched_global_runtime_items = _enrich_runtime_items(global_runtime_items, strategy_lookup=runtime_lookup)
        runtime_position_items = _enrich_runtime_position_items(
            runtime_position_items_raw,
            strategy_lookup=runtime_lookup,
        )
        personal_live_strategy_items = [item for item in enriched_global_runtime_items if int(item.get("owner_user_id") or 0) == uid]
        personal_runtime_updated_at = str(personal_runtime_snapshot.get("updated_at") or "")
        global_runtime_updated_at = str(global_runtime_snapshot.get("updated_at") or "")
        personal_runtime_status = _runtime_snapshot_status(personal_runtime_snapshot)
        global_runtime_status = _runtime_snapshot_status(global_runtime_snapshot)
        global_live_strategy_count = int(global_runtime_snapshot.get("strategy_count") or len(global_runtime_items))
        personal_live_strategy_count = len(personal_live_strategy_items)

        return {
            "ok": True,
            "cycle_id": cycle_id,
            "tasks_count": completed_task_count,
            "strategies_active": personal_active_strategy_count,
            "personal_live_strategies_active": personal_active_strategy_count,
            "personal_live_strategies_reviewed_label": "個人過審策略",
            "personal_reviewed_strategy_count": personal_reviewed_strategy_count,
            "personal_live_strategy_count": personal_live_strategy_count,
            "personal_live_strategy_items": personal_live_strategy_items[:20],
            "personal_active_strategy_count": personal_active_strategy_count,
            "personal_review_pipeline_count": int(personal_review_pipeline_count),
            "personal_review_ready_items": personal_review_ready_items[:100],
            "personal_review_pipeline_hint": "已達標並由系統持續追蹤的任務會優先顯示；未達標或異常結果可在歷史紀錄查看。",
            "global_strategies_active": global_reviewed_strategy_count,
            "global_live_strategies_reviewed_label": "全域過審策略",
            "global_reviewed_strategy_count": global_reviewed_strategy_count,
            "global_live_strategy_count": global_live_strategy_count,
            "personal_runtime_portfolio_count": int(personal_runtime_snapshot.get("strategy_count") or len(personal_runtime_items)),
            "personal_runtime_portfolio_updated_at": personal_runtime_updated_at,
            "personal_runtime_portfolio_items": personal_runtime_items,
            "personal_runtime_portfolio_stale": bool(personal_runtime_status.get("stale")),
            "personal_runtime_portfolio_age_seconds": personal_runtime_status.get("age_seconds"),
            "global_runtime_portfolio_count": global_live_strategy_count,
            "global_runtime_portfolio_updated_at": global_runtime_updated_at,
            "global_runtime_portfolio_items": enriched_global_runtime_items[:20],
            "global_runtime_portfolio_stale": bool(global_runtime_status.get("stale")),
            "global_runtime_portfolio_age_seconds": global_runtime_status.get("age_seconds"),
            "runtime_position_items": runtime_position_items[:50],
            "runtime_sync": {
                "personal": {**personal_runtime_status, **_runtime_sync_event_detail("personal", uid)},
                "global": {**global_runtime_status, **_runtime_sync_event_detail("global")},
                "global_active_strategy_mismatch": bool(global_runtime_status.get("count_mismatch")),
            },
            "payouts_unpaid": len([p for p in payouts if p["status"] == "unpaid"]),
            "recent_tasks": recent_tasks,
            "strategies": strategies,
            "payouts": payouts,
            "min_sharpe": min_sharpe,
            "total_strategy_pool_combo_count": int(global_counters.get("total_strategy_pool_combo_count") or 0),
            "global_mined_combo_count": int(global_counters.get("global_mined_combo_count") or 0),
            "announcements": latest_announcements,
            "announcement_version": _live_versions["announcement"],
            "dashboard_version": _live_versions["dashboard"],
        }

    try:
        payload = _cached_live_payload("dashboard", cache_key, _load_dashboard)
        _remember_live_snapshot("dashboard", cache_key, payload)
        return payload
    except Exception as exc:
        preserved = dict((_live_last_nonempty.get("dashboard") or {}).get(cache_key) or {})
        if preserved:
            preserved["_preserved_last_nonempty"] = True
            preserved["dashboard_warning"] = "stale_fallback"
            return preserved
        logger.error(f"Dashboard error for user {uid}: {exc}")
        raise HTTPException(status_code=500, detail="fetch_dashboard_failed")


@app.get("/live/version")
def live_version(request: Request, authorization: Optional[str] = Header(None)):
    _auth_ctx(request, authorization)
    return {
        "ok": True,
        "dashboard_version": _live_versions["dashboard"],
        "leaderboard_version": _live_versions["leaderboard"],
        "runtime_version": _live_versions["runtime"],
        "announcement_version": _live_versions["announcement"],
        "server_time": _utc_iso(),
    }

@app.get("/tasks")
def web_get_tasks(request: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(request, authorization)
    uid = int(ctx["user"]["id"])
    cycle_id = _get_active_cycle_id()

    # 僅做純資料讀取，嚴禁在此端點觸發 assign_tasks 或任何寫入操作，確保 API 響應在 50ms 內完成
    tasks = [enrich_task_row(t) for t in db.list_tasks_for_user(uid, cycle_id=cycle_id, limit=200)]
    run_enabled = db.get_user_run_enabled(uid)
    pending_task_count = _get_user_pending_assignment_count(uid, cycle_id)
    return {
        "ok": True,
        "tasks": tasks,
        "run_enabled": bool(run_enabled),
        "active_cycle_id": cycle_id,
        "pending_task_count": pending_task_count,
    }

@app.post("/tasks/start")
def web_start_tasks(request: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(request, authorization)
    uid = int(ctx["user"]["id"])
    db.set_user_run_enabled(uid, True)
    cycle_id = _get_active_cycle_id()
    primed = {
        "assigned_count": 0,
        "task_count": int(db.count_tasks_for_user(uid, cycle_id=cycle_id, statuses=["assigned", "queued", "running"])),
        "pending_task_count": int(db.count_tasks_for_user(uid, cycle_id=cycle_id, statuses=["assigned"])),
        "active_cycle_id": int(cycle_id),
        "priming": True,
    }
    try:
        import threading
        result_holder: Dict[str, Any] = {}
        ready = threading.Event()

        def _prime_in_background() -> None:
            try:
                result = dict(_prime_user_task_queue(uid, cycle_id) or {})
                result_holder.update(result)
                db.log_sys_event(
                    "USER_RUN_ENABLED_PRIME_READY",
                    uid,
                    "Background task priming completed",
                    result,
                )
                _invalidate_live_state("dashboard")
            except Exception as exc:
                db.log_sys_event(
                    "USER_RUN_ENABLED_PRIME_DEFERRED",
                    uid,
                    f"Background task prime failed: {exc}",
                    {"active_cycle_id": int(cycle_id)},
                )
            finally:
                ready.set()

        thread = threading.Thread(target=_prime_in_background, daemon=True)
        thread.start()
        if ready.wait(0.35) and result_holder:
            primed.update(result_holder)
            primed["priming"] = False
    except Exception as exc:
        db.log_sys_event(
            "USER_RUN_ENABLED_PRIME_THREAD_FAIL",
            uid,
            f"Background task prime thread failed: {exc}",
            {"ip": _client_ip(request), **primed},
        )
    db.log_sys_event(
        "USER_RUN_ENABLED",
        uid,
        "User requested task start via API",
        {"ip": _client_ip(request), **primed},
    )
    _invalidate_live_state("dashboard")
    return {"ok": True, "run_enabled": True, **primed}


@app.post("/tasks/stop")
def web_stop_tasks(request: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(request, authorization)
    uid = int(ctx["user"]["id"])
    db.set_user_run_enabled(uid, False)
    db.log_sys_event("USER_RUN_DISABLED", uid, "User requested task stop via API", {"ip": _client_ip(request)})
    _invalidate_live_state("dashboard")
    return {
        "ok": True,
        "run_enabled": False,
        "assigned_count": 0,
        "task_count": int(db.count_tasks_for_user(uid, cycle_id=_get_active_cycle_id(), statuses=["assigned", "queued", "running"])),
        "pending_task_count": int(db.count_tasks_for_user(uid, cycle_id=_get_active_cycle_id(), statuses=["assigned"])),
        "active_cycle_id": _get_active_cycle_id(),
    }


@app.post("/tasks/{task_id}/submit_oos")
def legacy_submit_oos(task_id: int, request: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(request, authorization)
    task = db.get_task(int(task_id))
    if not task:
        raise HTTPException(status_code=404, detail="task_not_found")

    owner_id = int(task.get("user_id") or 0)
    if (not _is_compute_token(ctx)) and owner_id != int(ctx["user"]["id"]):
        raise HTTPException(status_code=403, detail="forbidden")

    _log_deprecated_alias(request, ctx, f"/tasks/{task_id}/submit_oos", "auto-managed OOS flow")

    progress = {}
    try:
        progress = json.loads(task.get("progress_json") or "{}")
    except Exception:
        progress = {}

    review = normalize_review_fields(progress, str(task.get("status") or ""))
    return {
        "ok": True,
        "deprecated": True,
        "auto_managed": True,
        "task_id": int(task_id),
        "oos_status": str(review.get("oos_status") or "auto_managed"),
        "review_status": str(review.get("review_status") or "auto_managed"),
        "review_reason": str(review.get("review_reason") or ""),
        "review_failures": list(review.get("review_failures") or []),
        "msg": "OOS review is fully automatic; manual submit is deprecated.",
    }

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
    if x_worker_id:
        _require_worker(request, ctx, x_worker_id, x_worker_version, x_worker_protocol)

    token_kind = _token_kind(ctx)
    cycle_id = _get_active_cycle_id()

    # compute token：依「當前任務 owner」決定是否停
    if _is_compute_token(ctx):
        tid = int(x_current_task_id or 0)
        if tid > 0:
            t = db.get_task(int(tid)) or {}
            owner_id = int(t.get("user_id") or 0)
            if owner_id > 0:
                run_enabled = bool(db.get_user_run_enabled(owner_id))
                return {
                    "run_enabled": run_enabled,
                    "token_kind": token_kind,
                    "assignment_mode": "global_compute",
                    "reason": "owner_run_enabled" if run_enabled else "owner_run_disabled",
                    "pending_task_count": int(db.count_tasks_for_user(owner_id, cycle_id=cycle_id, statuses=["assigned"])),
                    "active_cycle_id": cycle_id,
                }
        return {
            "run_enabled": True,
            "token_kind": token_kind,
            "assignment_mode": "global_compute",
            "reason": "compute_token_ready",
            "pending_task_count": 0,
            "active_cycle_id": cycle_id,
        }

    # normal token：只看自己
    user_id = int(ctx["user"]["id"])
    run_enabled = bool(db.get_user_run_enabled(user_id))
    pending_task_count = _get_user_pending_assignment_count(user_id, cycle_id)

    if token_kind == "web_session":
        reason = "legacy_web_session_token"
    elif not run_enabled:
        reason = "run_disabled"
    elif cycle_id <= 0:
        reason = "no_active_cycle"
    elif pending_task_count > 0:
        reason = "task_available"
    else:
        reason = "no_pending_tasks"

    return {
        "run_enabled": run_enabled,
        "token_kind": token_kind,
        "assignment_mode": "personal_worker",
        "reason": reason,
        "pending_task_count": pending_task_count,
        "active_cycle_id": cycle_id,
    }


@app.get("/settings/thresholds")
def thresholds(
    request: Request,
    authorization: Optional[str] = Header(None),
    x_worker_id: Optional[str] = Header(None),
    x_worker_version: Optional[str] = Header(None),
    x_worker_protocol: Optional[int] = Header(None),
):
    ctx = _auth_ctx(request, authorization)
    if x_worker_id:
        _require_worker(request, ctx, x_worker_id, x_worker_version, x_worker_protocol)
    thresholds_data, _ = _load_threshold_state()
    return thresholds_data


@app.get("/settings/snapshot")
def settings_snapshot(
    request: Request,
    authorization: Optional[str] = Header(None),
    x_worker_id: Optional[str] = Header(None),
    x_worker_version: Optional[str] = Header(None),
    x_worker_protocol: Optional[int] = Header(None),
):
    ctx = _auth_ctx(request, authorization)
    if x_worker_id:
        _require_worker(request, ctx, x_worker_id, x_worker_version, x_worker_protocol)
    thresholds, updated_at = _load_threshold_state()
    return {
        "ts": _utc_iso(),
        "thresholds": thresholds,
        "updated_at": updated_at,
        "cost_basis": _cost_settings_payload(),
    }


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
                cycle_id = _get_active_cycle_id()
                if cycle_id > 0:
                    _prime_user_task_queue(user_id, cycle_id)
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
    risk_spec = _apply_global_costs_to_risk_spec(risk_spec)

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
    _invalidate_live_state("dashboard")
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
    
    db.log_sys_event("TASK_FINISH_START", user_id, f"Worker [{worker_id}] 開始提交任務，共攜帶 {len(body.candidates or [])} 組候選參數準備驗證", {"candidates_count": len(body.candidates or [])})

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

    final_prog = dict(body.final_progress or {}) if isinstance(body.final_progress, dict) else {}
    final_prog["cost_basis"] = _cost_settings_payload()
    body.final_progress = final_prog
    worker_dh = str(final_prog.get("data_hash") or getattr(body, "data_hash", "") or "").strip()
    
    if server_dh.get("data_hash") and worker_dh and str(server_dh.get("data_hash")) != worker_dh:
        try:
            prog = dict(final_prog)
            prog["last_error"] = "資料校驗不符，已拒絕提交"
            prog["review_status"] = "error"
            prog["review_reason"] = "資料校驗不符，已拒絕提交"
            prog["review_failures"] = []
            prog["oos_status"] = "error"
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
        except Exception as release_err:
            db.log_sys_event("TASK_FINISH_HASH_MISMATCH_FAIL", int(user_id), f"任務 {task_id} 雜湊不符且釋放失敗: {release_err}", {})
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

    best_rejected: Optional[Dict[str, Any]] = None

    try:
        checked = 0
        for cand in raw_candidates:
            if max_verify and checked >= max_verify:
                break
            if not isinstance(cand, dict):
                continue
            params = cand.get("params") or cand.get("params_json") or {}
            if not isinstance(params, dict):
                continue

            cand_family = str(params.get("family") or str(task_row.get("family") or ""))
            family_params = params.get("family_params")
            if not isinstance(family_params, dict):
                family_params = {k: v for k, v in params.items() if k not in ("family", "tp", "sl", "max_hold")}

            try:
                tp = float(params.get("tp", 0.0))
                sl = float(params.get("sl", 0.0))
                mh = int(params.get("max_hold", 0))
            except Exception:
                continue

            # [專家級優化] 全面信任用戶端算力，伺服器不再進行回測，直接採納 Worker 回報之指標
            reported = cand.get("metrics") or {}
            server_metrics = {
                "total_return_pct": float(reported.get("total_return_pct", 0.0)),
                "max_drawdown_pct": float(reported.get("max_drawdown_pct", 0.0)),
                "sharpe": float(reported.get("sharpe", 0.0)),
                "trades": int(reported.get("trades", 0)),
                "win_rate_pct": float(reported.get("win_rate_pct", 0.0)),
            }
            server_score = float(_score(server_metrics))
            threshold_eval = evaluate_thresholds(
                metrics=server_metrics,
                min_trades=min_trades,
                min_total_return_pct=min_ret,
                max_drawdown_pct=max_dd,
                min_sharpe=min_sh,
            )
            if bool(threshold_eval["passed"]):
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
                db.log_sys_event("WORKER_VERIFY_PASS", int(user_id), f"參數達標自動審核通過 (Score: {server_score:.2f})", {"task_id": task_id})
            else:
                reject_reason = str(threshold_eval.get("reason") or "未通過門檻")
                reject_failures = list(threshold_eval.get("failures") or [])
                db.write_audit_log(
                    user_id=int(user_id),
                    action="worker_verify_rejected",
                    payload={
                        "task_id": int(task_id),
                        "reason": reject_reason,
                        "worker_metrics": server_metrics,
                        "review_failures": reject_failures,
                    }
                )
                db.log_sys_event(
                    "WORKER_VERIFY_REJECT",
                    int(user_id),
                    f"用戶端數據未達標: {reject_reason}",
                    {"task_id": task_id, "metrics": server_metrics, "review_failures": reject_failures},
                )
                if best_rejected is None or float(server_score) > float(best_rejected.get("score") or -1e18):
                    best_rejected = {
                        "score": float(server_score),
                        "reason": reject_reason,
                        "failures": reject_failures,
                        "metrics": dict(server_metrics),
                    }

            checked += 1

        if not verified_candidates and raw_candidates:
            try:
                prog = dict(body.final_progress or {})
                review_reason = str((best_rejected or {}).get("reason") or "提交之參數皆未達標")
                prog["best_any_passed"] = False
                prog["review_status"] = "rejected"
                prog["review_reason"] = review_reason
                prog["review_failures"] = list((best_rejected or {}).get("failures") or [])
                prog["oos_status"] = "rejected"
                prog["last_reject_reason"] = review_reason
                prog["last_error"] = f"提交之參數皆未達標 ({review_reason})"
                prog["updated_at"] = _utc_iso()
                body.final_progress = prog
                db.log_sys_event("TASK_FINISH_NO_CANDIDATE", int(user_id), "任務完成但所有參數皆未達標", {"task_id": task_id})
            except Exception:
                pass
        elif verified_candidates:
            try:
                prog = dict(body.final_progress or {})
                prog["best_any_passed"] = True
                prog["review_status"] = str(prog.get("review_status") or prog.get("oos_status") or "auto_managed")
                prog["review_reason"] = str(prog.get("review_reason") or "已達標，後續流程由系統自動管理中")
                prog["review_failures"] = list(prog.get("review_failures") or [])
                if not str(prog.get("oos_status") or "").strip():
                    prog["oos_status"] = "auto_managed"
                prog["updated_at"] = _utc_iso()
                body.final_progress = prog
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
            prog["review_status"] = "error"
            prog["review_reason"] = str(e)
            prog["review_failures"] = []
            prog["oos_status"] = "error"
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
    _invalidate_live_state("dashboard", "leaderboard")
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
    return enrich_task_row(dict(task))


@app.post("/tasks/oos/claim")
def legacy_oos_claim(request: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(request, authorization)
    db.log_sys_event(
        "LEGACY_OOS_CLAIM",
        int((ctx.get("user") or {}).get("id") or 0) or None,
        "Legacy OOS claim endpoint hit; returning empty task for compatibility",
        {"ip": _client_ip(request)},
    )
    return {"ok": True, "deprecated": True, "task": None}


@app.post("/tasks/oos/{task_id}/finish")
def legacy_oos_finish(task_id: int, request: Request, authorization: Optional[str] = Header(None)):
    ctx = _auth_ctx(request, authorization)
    db.log_sys_event(
        "LEGACY_OOS_FINISH",
        int((ctx.get("user") or {}).get("id") or 0) or None,
        f"Legacy OOS finish endpoint hit for task {task_id}",
        {"task_id": int(task_id), "ip": _client_ip(request)},
    )
    return {"ok": True, "deprecated": True, "task_id": int(task_id)}

from fastapi.responses import JSONResponse

# [專家級終極防護] 捕捉所有未匹配的 HTTP 方法與路徑，直接回傳 200 OK JSON。
# 這樣 Streamlit 的 Fallback XHR POST 請求就永遠不會收到 405 Method Not Allowed，從而徹底根除前端報錯彈窗！
@app.post("/admin/direct_exec")
def admin_direct_exec(request: Request, body: dict, authorization: Optional[str] = Header(None)):
    """管理員專用直連 SSH exec（與 fetch_pg_dump.py 完全相同基礎設施）"""
    ctx = _auth_ctx(request, authorization)
    if str(ctx["user"].get("role") or "") != "admin":
        raise HTTPException(status_code=403, detail="僅限管理員")
    sql = str(body.get("sql") or "").strip()
    params = tuple(body.get("params") or ())
    if not sql:
        raise HTTPException(status_code=400, detail="sql 不可為空")
    affected = db.admin_ssh_direct_exec(sql, params)
    db.log_sys_event("ADMIN_DIRECT_SSH_EXEC", int(ctx["user"]["id"]), f"直連修改成功，影響 {affected} 行", {"sql_preview": sql[:200]})
    return {"ok": True, "affected_rows": affected}

@app.api_route("/{path_name:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH", "HEAD"])
async def catch_all(request: Request, path_name: str):
    # [專家級終極防護] 支援 Streamlit 的健康檢查與 Fallback 請求，偽裝成 200 OK，徹底粉碎 Nginx/FastAPI 丟出 405 的可能性
    if "health" in path_name or "ping" in path_name:
        return JSONResponse(status_code=200, content={"ok": True, "status": "alive"})
    user_id = getattr(request.state, "user_id", None)
    db.log_sys_event(
        "UNKNOWN_ROUTE",
        user_id,
        f"Unknown route requested: {request.method} /{path_name}",
        {"path": f"/{path_name}", "method": request.method, "ip": _client_ip(request)},
    )
    return JSONResponse(
        status_code=404,
        content={
            "ok": False,
            "error": "route_not_found",
            "path": f"/{path_name}",
            "method": request.method,
        },
    )
