import json
import os
import random
import re
import time
import math
import html
import base64
import atexit
import socket
import hashlib
from urllib.parse import urlparse, urlunparse
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import sys as _sys
db = _sys.modules[__name__]
from sheep_platform_security import (
    hash_password,
    verify_password,
    validate_username,
    validate_password_strength,
    validate_wallet_address,
    normalize_username,
    get_fernet,
)
from sheep_strategy_schema import (
    direction_to_reverse,
    normalize_direction,
    normalize_runtime_strategy_entry,
    parse_json_object,
    unwrap_family_params,
)
# ─────────────────────────────────────────────────────────────────────────────
# DB API (sqlite/postgres) — production: Postgres via SHEEP_DB_URL
# ─────────────────────────────────────────────────────────────────────────────
import sqlite3
import threading

try:
    import psycopg2
    import psycopg2.pool
    import psycopg2.extras
except Exception:
    psycopg2 = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


_REVIEW_READY_CACHE: Dict[str, Any] = {"ts": 0.0, "values": {}, "retry_after": {}}


def _env_timeout_ms(name: str, default: int) -> int:
    try:
        value = int(float(os.environ.get(name, str(default)) or str(default)))
    except Exception:
        value = int(default)
    return max(0, min(3600000, int(value)))


def _in_docker() -> bool:
    # 目的：判斷目前程式是否在 docker/container 裡，避免把 compose 專用的 host 規則套到本機直跑
    # [專家級修復] 增加環境變數與 DNS 解析的啟發式判斷，防止較新版本的 container 引擎沒有 /.dockerenv
    try:
        if os.path.exists("/.dockerenv"):
            return True
    except Exception:
        pass
    try:
        import socket
        socket.gethostbyname("db")
        return True
    except Exception:
        pass
    try:
        if os.environ.get("SHEEP_DB_URL", "").find("@db:") != -1:
            return True
    except Exception:
        pass
    try:
        with open("/proc/1/cgroup", "rt", encoding="utf-8", errors="ignore") as f:
            cg = f.read()
        cg_l = cg.lower()
        if ("docker" in cg_l) or ("kubepods" in cg_l) or ("containerd" in cg_l):
            return True
    except Exception:
        pass
    return False


def _mask_dsn(dsn: str) -> str:
    # 只遮密碼，不遮 user/host/dbname，避免你 debug 只看到一團黑
    try:
        u = urlparse(str(dsn or ""))
        if u.password:
            return str(dsn).replace(u.password, "***")
    except Exception:
        pass
    try:
        return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", str(dsn))
    except Exception:
        return str(dsn)


def _rewrite_pg_host(dsn: str, new_host: str) -> str:
    # 將 postgresql://user:pass@HOST:PORT/db 的 HOST 改成 new_host
    try:
        u = urlparse(str(dsn or ""))
        if not u.scheme or not u.netloc:
            return str(dsn)

        netloc = str(u.netloc)
        userinfo = ""
        hostport = netloc

        if "@" in hostport:
            userinfo, hostport = hostport.rsplit("@", 1)

        port = ""

        if hostport.startswith("["):
            m = re.match(r"^\[(?P<h>.+)\](?::(?P<p>\d+))?$", hostport)
            if m:
                port = str(m.group("p") or "")
        else:
            if ":" in hostport:
                _, port = hostport.split(":", 1)

        # IPv6 host 需要加 []
        nh = str(new_host or "").strip()
        if ":" in nh and not nh.startswith("["):
            nh2 = f"[{nh}]"
        else:
            nh2 = nh

        new_hostport = f"{nh2}:{port}" if port else nh2
        new_netloc = f"{userinfo}@{new_hostport}" if userinfo else new_hostport

        u2 = u._replace(netloc=new_netloc)
        return urlunparse(u2)
    except Exception:
        return str(dsn)


def _db_url() -> str:
    # 主來源：SHEEP_DB_URL
    raw_url = os.environ.get("SHEEP_DB_URL")
    
    # [專家級修復] 嚴格攔截 fallback 機制。若 docker-compose 明確將其設為空字串，
    # 代表強制要求退回 SQLite。此時絕對不可再讀取 DATABASE_URL，避免連線到空庫。
    if raw_url == "":
        return ""
        
    u = str(raw_url or "").strip()

    # 次要來源：DATABASE_URL（很多 PaaS 預設用這個）
    if not u:
        u = str(os.environ.get("DATABASE_URL", "") or "").strip()

    if not u:
        return ""

    low = u.lower()
    if low.startswith("postgresql://") or low.startswith("postgres://"):
        # 允許顯式 override host：適用「本機直跑 UI、DB 在另一台/或已 publish port」等情境
        override = str(os.environ.get("SHEEP_PG_HOST", "") or os.environ.get("PGHOST", "") or "").strip()

        # 若你不在 docker/container 裡，還寫 host=db，那幾乎一定炸；直接幫你改成 127.0.0.1（可再被 override 覆蓋）
        try:
            host = str(urlparse(u).hostname or "")
        except Exception:
            host = ""

        if override:
            u = _rewrite_pg_host(u, override)
        else:
            if host == "db" and (not _in_docker()):
                u = _rewrite_pg_host(u, "127.0.0.1")

    return u


def _db_kind() -> str:
    u = _db_url().lower()
    if u.startswith("postgresql://") or u.startswith("postgres://"):
        return "postgres"
    return "sqlite"


def _db_path() -> str:
    p = str(os.environ.get("SHEEP_DB_PATH", "") or "").strip()
    if p:
        if not os.path.isabs(p):
            try:
                base_dir = os.path.dirname(os.path.abspath(__file__))
            except Exception:
                base_dir = os.getcwd()
            p = os.path.join(base_dir, p)
        return p
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    except Exception:
        base_dir = os.getcwd()
    return os.path.join(base_dir, "data", "sheep.db")


_DEFAULT_THRESHOLD_SETTINGS: Dict[str, Any] = {
    "min_trades": 30,
    "min_total_return_pct": 3.0,
    "max_drawdown_pct": 25.0,
    "min_sharpe": 0.6,
    "candidate_keep_top_n": 30,
    "default_avatar_data_url": "",
}

_PROFILE_NICKNAME_MAX_LEN = 16
_AVATAR_DATA_URL_MAX_LEN = 350000


def _sanitize_nickname_text(value: Any, fallback: str = "") -> str:
    raw = str(value or "").strip()
    if not raw:
        raw = str(fallback or "").strip()
    if not raw:
        raw = "未命名用戶"
    return html.escape(raw[:_PROFILE_NICKNAME_MAX_LEN])


def _avatar_initials(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "羊"
    ascii_only = re.sub(r"[^A-Za-z0-9]", "", text)
    if ascii_only:
        return ascii_only[:2].upper()
    return text[:1]


def _build_default_avatar_data_url(label: str) -> str:
    initials = html.escape(_avatar_initials(label))
    svg = (
        "<svg xmlns='http://www.w3.org/2000/svg' width='128' height='128' viewBox='0 0 128 128' fill='none'>"
        "<rect width='128' height='128' rx='64' fill='#2B3036'/>"
        "<circle cx='64' cy='64' r='52' fill='#3B4148'/>"
        "<text x='64' y='74' text-anchor='middle' font-family='Segoe UI, Arial, sans-serif' "
        "font-size='40' font-weight='700' fill='#F4F4F5'>"
        f"{initials}</text></svg>"
    )
    encoded = base64.b64encode(svg.encode("utf-8")).decode("ascii")
    return f"data:image/svg+xml;base64,{encoded}"


def _sanitize_avatar_url(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    lower = text.lower()
    if lower.startswith("data:image/") and len(text) <= _AVATAR_DATA_URL_MAX_LEN:
        return text
    if lower.startswith("https://") or lower.startswith("http://"):
        return text
    return ""


def _default_avatar_url_from_conn(conn: Any = None) -> str:
    owns_conn = not bool(getattr(conn, "_is_db_conn", False))
    if owns_conn:
        conn = _conn()
    try:
        custom = _sanitize_avatar_url(get_setting(conn, "default_avatar_data_url", ""))
        if custom:
            return custom
    except Exception:
        pass
    finally:
        if owns_conn:
            conn.close()
    return _build_default_avatar_data_url("SHEEP")


def _decorate_user_row(row: Any, *, default_avatar_url: str = "") -> Dict[str, Any]:
    data = dict(row or {})
    username = str(data.get("username") or "").strip()
    nickname = _sanitize_nickname_text(data.get("nickname"), username)
    avatar_url = _sanitize_avatar_url(data.get("avatar_url"))
    if not avatar_url:
        avatar_url = str(default_avatar_url or "").strip() or _build_default_avatar_data_url(nickname or username or "S")
    data["nickname"] = nickname
    data["display_name"] = nickname
    data["avatar_url"] = avatar_url
    return data


def _infer_direction(
    *,
    direction: Any = None,
    params_json: Any = None,
    risk_spec_json: Any = None,
    default: str = "long",
) -> str:
    params = parse_json_object(params_json)
    family_params = unwrap_family_params(params.get("family_params") or params)
    risk_spec = parse_json_object(risk_spec_json)
    return normalize_direction(
        direction or params.get("direction") or family_params.get("direction"),
        reverse=(
            params.get("reverse")
            if "reverse" in params
            else family_params.get("reverse")
            if "reverse" in family_params
            else risk_spec.get("reverse_mode")
        ),
        default=default,
    )


def _normalize_risk_spec(direction: Any, risk_spec: Any) -> Dict[str, Any]:
    spec = parse_json_object(risk_spec)
    normalized_direction = normalize_direction(direction, reverse=spec.get("reverse_mode"), default="long")
    spec["reverse_mode"] = bool(direction_to_reverse(normalized_direction))
    return spec


def _normalize_strategy_params_payload(
    params_json: Any,
    *,
    direction: Any = None,
    family: str = "",
    symbol: str = "",
    interval: str = "",
) -> Dict[str, Any]:
    params = parse_json_object(params_json)
    wrapper = parse_json_object(params.get("family_params"))
    family_params = unwrap_family_params(params.get("family_params") or params)
    normalized_direction = _infer_direction(direction=direction, params_json=params, default="long")
    payload = {
        "family": str(family or params.get("family") or wrapper.get("family") or "").strip(),
        "family_params": family_params,
        "tp": params.get("tp", wrapper.get("tp")),
        "sl": params.get("sl", wrapper.get("sl")),
        "max_hold": params.get("max_hold", wrapper.get("max_hold")),
        "direction": normalized_direction,
        "symbol": str(symbol or params.get("symbol") or "").strip(),
        "interval": str(interval or params.get("interval") or "").strip(),
    }
    return {key: value for key, value in payload.items() if value not in (None, "")}


def _normalize_pool_row(row: Any) -> Dict[str, Any]:
    data = dict(row or {})
    direction = _infer_direction(direction=data.get("direction"), risk_spec_json=data.get("risk_spec_json"))
    data["direction"] = direction
    data["external_key"] = str(data.get("external_key") or "")
    data["grid_spec"] = parse_json_object(data.get("grid_spec_json"))
    risk_spec = _normalize_risk_spec(direction, data.get("risk_spec_json"))
    data["risk_spec"] = risk_spec
    data["risk_spec_json"] = json.dumps(risk_spec, ensure_ascii=False)
    return data


def _normalize_strategy_row(row: Any) -> Dict[str, Any]:
    data = dict(row or {})
    direction = _infer_direction(direction=data.get("direction"), params_json=data.get("params_json"))
    data["direction"] = direction
    data["external_key"] = str(data.get("external_key") or "")
    params = _normalize_strategy_params_payload(
        data.get("params_json"),
        direction=direction,
        family=str(data.get("family") or ""),
        symbol=str(data.get("symbol") or ""),
        interval=str(data.get("interval") or data.get("timeframe_min") or ""),
    )
    data["params"] = params
    data["params_json"] = params
    return data


def _normalize_candidate_row(row: Any) -> Dict[str, Any]:
    data = dict(row or {})
    direction = _infer_direction(direction=data.get("direction"), params_json=data.get("params_json"))
    data["direction"] = direction
    params = parse_json_object(data.get("params_json"))
    if params:
        params["direction"] = direction
    data["params_json"] = params
    data["metrics"] = parse_json_object(data.get("metrics_json"))
    return data


def _runtime_snapshot_row(row: Any) -> Dict[str, Any]:
    data = dict(row or {})
    data["summary"] = parse_json_object(data.get("summary_json"))
    return data


def _runtime_item_row(row: Any) -> Dict[str, Any]:
    data = dict(row or {})
    data["direction"] = normalize_direction(data.get("direction"), default="long")
    try:
        data["strategy_id"] = int(data.get("strategy_id") or 0)
    except Exception:
        data["strategy_id"] = 0
    data["stake_pct"] = float(data.get("stake_pct") or 0.0)
    data["sharpe"] = float(data.get("sharpe") or 0.0)
    data["total_return_pct"] = float(data.get("total_return_pct") or 0.0)
    data["max_drawdown_pct"] = float(data.get("max_drawdown_pct") or 0.0)
    data["params"] = parse_json_object(data.get("params_json"))
    corr_stats = parse_json_object(data.get("corr_stats_json"))
    if not corr_stats:
        corr_stats = {
            "avg_pairwise_corr_to_selected": float(data.get("avg_corr") or 0.0),
            "max_pairwise_corr_to_selected": float(data.get("max_corr") or 0.0),
        }
    data["corr_stats"] = corr_stats
    return data


def _stable_json_checksum(payload: Any) -> str:
    normalized = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def describe_db_source() -> Dict[str, Any]:
    kind = _db_kind()
    if kind == "postgres":
        dsn = str(_db_url() or "")
        parsed = urlparse(dsn) if dsn else None
        return {
            "kind": "postgres",
            "masked_dsn": _mask_dsn(dsn),
            "host": str(getattr(parsed, "hostname", "") or ""),
            "database": str((parsed.path or "").lstrip("/") if parsed else ""),
            "path": "",
        }

    db_path = _db_path()
    return {
        "kind": "sqlite",
        "masked_dsn": db_path,
        "host": "",
        "database": os.path.basename(db_path),
        "path": db_path,
    }


def ensure_default_settings(conn: Any = None) -> Dict[str, Any]:
    owns_conn = not bool(getattr(conn, "_is_db_conn", False))
    if owns_conn:
        conn = _conn()

    inserted: List[str] = []
    try:
        for key, value in _DEFAULT_THRESHOLD_SETTINGS.items():
            row = conn.execute("SELECT 1 FROM settings WHERE key = ? LIMIT 1", (str(key),)).fetchone()
            if row:
                continue
            set_setting(conn, str(key), value)
            inserted.append(str(key))

        if owns_conn:
            conn.commit()
        return {"inserted": inserted, "count": len(inserted)}
    except Exception:
        if owns_conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if owns_conn:
            conn.close()


def _backfill_direction_columns(conn: Any) -> None:
    try:
        pool_rows = conn.execute("SELECT id, direction, risk_spec_json FROM factor_pools").fetchall()
    except Exception:
        pool_rows = []

    pool_risk_by_id: Dict[int, Dict[str, Any]] = {}
    for row in pool_rows:
        row = dict(row)
        try:
            pool_id = int(row["id"])
        except Exception:
            continue
        current_direction = _infer_direction(direction=row.get("direction"), risk_spec_json=row.get("risk_spec_json"))
        risk_spec = _normalize_risk_spec(current_direction, row.get("risk_spec_json"))
        pool_risk_by_id[pool_id] = risk_spec
        try:
            conn.execute(
                "UPDATE factor_pools SET direction = ?, risk_spec_json = ?, external_key = COALESCE(external_key, '') WHERE id = ?",
                (current_direction, json.dumps(risk_spec, ensure_ascii=False), pool_id),
            )
        except Exception:
            pass

    try:
        strategy_rows = conn.execute("SELECT id, direction, params_json, pool_id FROM strategies").fetchall()
    except Exception:
        strategy_rows = []
    for row in strategy_rows:
        row = dict(row)
        try:
            pool_id = int(row.get("pool_id") or 0)
        except Exception:
            pool_id = 0
        current_direction = _infer_direction(
            direction=row.get("direction"),
            params_json=row.get("params_json"),
            risk_spec_json=pool_risk_by_id.get(pool_id),
        )
        normalized_params = _normalize_strategy_params_payload(row.get("params_json"), direction=current_direction)
        try:
            conn.execute(
                "UPDATE strategies SET direction = ?, params_json = ?, external_key = COALESCE(external_key, '') WHERE id = ?",
                (current_direction, json.dumps(normalized_params, ensure_ascii=False), int(row["id"])),
            )
        except Exception:
            pass

    try:
        candidate_rows = conn.execute("SELECT id, direction, params_json, pool_id FROM candidates").fetchall()
    except Exception:
        candidate_rows = []
    for row in candidate_rows:
        row = dict(row)
        try:
            pool_id = int(row.get("pool_id") or 0)
        except Exception:
            pool_id = 0
        current_direction = _infer_direction(
            direction=row.get("direction"),
            params_json=row.get("params_json"),
            risk_spec_json=pool_risk_by_id.get(pool_id),
        )
        try:
            conn.execute("UPDATE candidates SET direction = ? WHERE id = ?", (current_direction, int(row["id"])))
        except Exception:
            pass


class _DBResult:
    def __init__(self, cur, rowcount: int = 0, lastrowid: int = 0):
        self._cur = cur
        self.rowcount = int(rowcount or 0)
        self.lastrowid = int(lastrowid or 0)

    def fetchone(self):
        if self._cur is None:
            return None
        try:
            r = self._cur.fetchone()
            return dict(r) if r else None
        finally:
            try:
                self._cur.close()
            except Exception:
                pass
            self._cur = None

    def fetchall(self):
        if self._cur is None:
            return []
        try:
            rows = self._cur.fetchall()
            return [dict(x) for x in rows] if rows else []
        finally:
            try:
                self._cur.close()
            except Exception:
                pass
            self._cur = None


class _DBConn:
    def __init__(self, conn, pool):
        self._c = conn
        self._p = pool
        self._closed = False
        self._is_db_conn = True  # 修復 get_setting 誤判導致的連線無限增生

    def execute(self, sql: str, params: Any = None):
        is_pg = (getattr(self, "kind", "") == "postgres")
        if is_pg and psycopg2 is not None:
            # [極致修復] 移除會導致 UnboundLocalError 的區域 import，直接使用檔案頂部已匯入的全域模組
            cur = self._c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # 僅在 Postgres 模式下替換佔位符，保護 SQLite 原生語法
            sql_fixed = sql.replace("?", "%s")
        else:
            cur = self._c.cursor()
            sql_fixed = sql

        try:
            if params is not None:
                cur.execute(sql_fixed, params)
            else:
                cur.execute(sql_fixed)
            return cur
        except Exception as e:
            # 發生錯誤時必須手動 rollback，否則該連線會失效
            try:
                self._c.rollback()
            except Exception:
                pass
            raise e

    def executescript(self, sql: str):
        """兼容 SQLite 的 executescript 方法，供 init_db 執行 DDL 使用"""
        is_pg = (getattr(self, "kind", "") == "postgres")
        if is_pg and psycopg2 is not None:
            cur = self._c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            self._c.executescript(sql)
            return None
            
        try:
            cur.execute(sql)
            return cur
        except Exception as e:
            try:
                self._c.rollback()
            except Exception:
                pass
            import sys, traceback
            print(f"[FATAL DB ERROR] executescript 執行失敗: {e}\n{traceback.format_exc()}", file=sys.stderr, flush=True)
            raise e

    def commit(self):
        self._c.commit()

    def rollback(self):
        self._c.rollback()

    def close(self):
        if not self._closed:
            try:
                is_pg = (getattr(self, "kind", "") == "postgres")
                if is_pg and self._p:
                    # [專家級防護] 歸還連線前強制 rollback，徹底清除 IDLE IN TRANSACTION 與懸空鎖！
                    # 這是解決 PostgreSQL 中 SELECT 讀寫鎖阻塞 ALTER TABLE (導致全站轉圈圈且無日誌) 的終極解法
                    try:
                        self._c.rollback()
                    except Exception:
                        pass
                    self._p.putconn(self._c)
                elif not is_pg:
                    # [專家級修復] SQLite 必須真實關閉連線，防止伺服器 File Descriptor 記憶體洩漏
                    try:
                        self._c.close()
                    except Exception:
                        pass
            except Exception as e:
                import traceback
                import sys
                print(f"[FATAL DB ERROR] 連線關閉/歸還失敗: {e}\n{traceback.format_exc()}", file=sys.stderr, flush=True)
            finally:
                self._closed = True

    # [專家級補強] 確保物件被垃圾回收時，連線一定會還給連線池，防止 Pool Exhausted
    def __del__(self):
        try:
            if not getattr(self, "_closed", True):
                self.close()
        except Exception:
            pass


_PG_POOL = None
_PG_POOL_LOCK = threading.Lock()


def _close_pg_pool() -> None:
    global _PG_POOL
    p = _PG_POOL
    if p is None:
        return
    try:
        p.closeall()
    except Exception:
        pass
    _PG_POOL = None


try:
    atexit.register(_close_pg_pool)
except Exception:
    pass


def _pg_pool():
    global _PG_POOL
    if _PG_POOL is not None:
        return _PG_POOL

    with _PG_POOL_LOCK:
        if _PG_POOL is not None:
            return _PG_POOL

        url0 = _db_url()
        if not url0:
            raise RuntimeError("SHEEP_DB_URL is empty but postgres backend requested")

        # [專家級修復] 搭配 PgBouncer，將 Python 內部連線池的最大上限拉高到 200，徹底解決 exhausted 報錯
        maxconn = 200
        try:
            maxconn = int(os.environ.get("SHEEP_PG_MAXCONN", "200") or "200")
        except Exception:
            maxconn = 200
        maxconn = max(5, min(500, int(maxconn)))

        # 建立候選 DSN（同一個密碼、同一個 DB，只改 host）
        candidates = []
        detailed_errors = [] # 儲存每個連線的詳細錯誤以便除錯

        # [專家級修復] 強制保留原始環境變數 DSN 作為第一優先。
        # 避免 _in_docker() 誤判導致 "db" 被強制替換為 "127.0.0.1"，造成 Docker 內無法連線。
        raw_url = str(os.environ.get("SHEEP_DB_URL", "")).strip()
        if raw_url and raw_url not in candidates:
            candidates.append(raw_url)

        # 再加入經過 _db_url 處理的 DSN
        if url0 and url0 not in candidates:
            candidates.append(url0)

        # 解析目前首選的 host
        try:
            p0 = urlparse(candidates[0])
            host0 = str(p0.hostname or "")
        except Exception:
            host0 = ""

        # 擴充 fallback 清單，保證所有可能性都被涵蓋
        if host0 == "db":
            for h in ("db", "127.0.0.1", "localhost", "host.docker.internal"):
                u1 = _rewrite_pg_host(candidates[0], h)
                if u1 not in candidates:
                    candidates.append(u1)
        else:
            for h in ("127.0.0.1", "localhost"):
                u1 = _rewrite_pg_host(candidates[0], h)
                if u1 not in candidates:
                    candidates.append(u1)

        last_err = None

        for dsn in candidates:
            try:
                # 先測 DNS（可讀性比 psycopg2 的報錯好很多）
                try:
                    hh = str(urlparse(dsn).hostname or "")
                    if hh:
                        socket.getaddrinfo(hh, None)
                except Exception:
                    pass

                _PG_POOL = psycopg2.pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=maxconn,
                    dsn=dsn,
                )
                return _PG_POOL

            except Exception as e:
                last_err = e
                masked = _mask_dsn(dsn)
                detailed_errors.append(f"嘗試連線 {masked} 失敗: {e}")
                _PG_POOL = None
                continue

        # 全部候選都失敗：把「你需要的根因」與所有嘗試過程完整印出來（但不洩漏密碼）
        error_details_str = "\n".join([f"  - {err}" for err in detailed_errors])
        try:
            import traceback as _tb
            tried = [_mask_dsn(x) for x in candidates]
            print("[DB FATAL] Postgres connection pool init failed.", file=_sys.stderr, flush=True)
            print(f"[DB FATAL] in_docker={_in_docker()}", file=_sys.stderr, flush=True)
            print(f"[DB FATAL] 嘗試連線歷程:\n{error_details_str}", file=_sys.stderr, flush=True)
            print(f"[DB FATAL] hint: if you are running outside docker-compose, do NOT use host=db; set SHEEP_PG_HOST=localhost and publish 5432 if needed.", file=_sys.stderr, flush=True)
        except Exception:
            pass

        raise RuntimeError(
            "Postgres 連線失敗：目前 DSN host 無法解析或無法連線。\n\n"
            f"以下是系統嘗試過的所有連線與對應失敗原因：\n{error_details_str}\n\n"
            "【排解建議】\n"
            "1. 若你使用 docker-compose，請確認 db 容器已成功啟動且 Healthcheck 通過。\n"
            "2. 若你本機直接執行 Python，請把 SHEEP_DB_URL 的 host 改成 localhost/127.0.0.1，或設定 SHEEP_PG_HOST=localhost。"
        ) from last_err


def _release_conn(kind: str, raw) -> None:
    if kind == "postgres":
        # [專家級修復] 歸還連線前強制 rollback，清除殘留的錯誤交易狀態與死鎖，防止連線池被毒化
        try:
            raw.rollback()
        except Exception:
            pass
        try:
            _pg_pool().putconn(raw)
        except Exception:
            try:
                raw.close()
            except Exception:
                pass
        return
    try:
        raw.close()
    except Exception:
        pass


def _conn() -> _DBConn:
    kind = _db_kind()

    if kind == "postgres":
        pool_obj = _pg_pool()
        c = pool_obj.getconn()
        try:
            c.autocommit = False
        except Exception:
            pass
            
        # [專家級防護] 注入連線級別的超時保護，徹底消滅無窮等待(Deadlock/Lock wait)
        try:
            statement_timeout_ms = _env_timeout_ms("SHEEP_PG_STATEMENT_TIMEOUT_MS", 7000)
            lock_timeout_ms = _env_timeout_ms("SHEEP_PG_LOCK_TIMEOUT_MS", 5000)
            with c.cursor() as cur:
                cur.execute(f"SET statement_timeout = {int(statement_timeout_ms)};")
                cur.execute(f"SET lock_timeout = {int(lock_timeout_ms)};")
            c.commit() # 必須 commit，確保設定生效且不留下懸空交易
        except Exception:
            try:
                c.rollback()
            except Exception:
                pass
                
        # 修復：正確的參數順序為 _DBConn(conn, pool)
        conn_obj = _DBConn(c, pool_obj)
        conn_obj.kind = "postgres"  # 保留 kind 屬性供後續方言判斷使用
        return conn_obj

    # sqlite
    # sqlite
    path = _db_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    except Exception as e:
        import traceback
        print(f"[DB ERROR] 無法建立資料庫目錄 {path}, 錯誤詳情: {e}\n{traceback.format_exc()}")

    # [專家級修復] 使用 IMMEDIATE 隔離級別，根除 SQLite 讀寫鎖升級導致的 deadlock 與瞬間 database is locked 錯誤
    raw = sqlite3.connect(path, timeout=30.0, check_same_thread=False, isolation_level="IMMEDIATE")
    raw.row_factory = sqlite3.Row

    try:
        raw.execute("PRAGMA foreign_keys = ON;")
    except Exception:
        pass
    try:
        raw.execute("PRAGMA journal_mode = WAL;")
    except Exception:
        pass
    try:
        raw.execute("PRAGMA synchronous = NORMAL;")
    except Exception:
        pass
    try:
        busy_ms = int(float(os.environ.get("SHEEP_SQLITE_BUSY_TIMEOUT_MS", "15000") or "15000"))
    except Exception:
        busy_ms = 15000
    busy_ms = max(0, min(60000, int(busy_ms)))
    try:
        raw.execute(f"PRAGMA busy_timeout = {busy_ms};")
    except Exception:
        pass
    try:
        raw.execute("PRAGMA mmap_size = 268435456;")
    except Exception:
        pass
    try:
        raw.execute("PRAGMA cache_size = -20000;")
    except Exception:
        pass
    try:
        raw.execute("PRAGMA temp_store = MEMORY;")
    except Exception:
        pass

    # 修復：正確的參數順序為 _DBConn(conn, pool)，SQLite 無 pool 故傳 None
    conn_obj = _DBConn(raw, None)
    conn_obj.kind = "sqlite"
    return conn_obj


def init_db() -> None:
    conn = _conn()
    is_pg = (getattr(conn, "kind", "sqlite") == "postgres")
    
    try:
        if is_pg:
            # ---------------------------------------------------------
            # PostgreSQL 專用 DDL
            # ---------------------------------------------------------
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id BIGSERIAL PRIMARY KEY,
                    username TEXT NOT NULL,
                    username_norm TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL DEFAULT 'user',
                    nickname TEXT DEFAULT '',
                    avatar_url TEXT DEFAULT '',
                    disabled INTEGER NOT NULL DEFAULT 0,
                    run_enabled INTEGER NOT NULL DEFAULT 0,
                    wallet_address TEXT NOT NULL DEFAULT '',
                    wallet_chain TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    last_login_at TEXT,
                    profile_updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS audit_logs (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT,
                    action TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS api_tokens (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    token TEXT NOT NULL UNIQUE,
                    name TEXT,
                    expires_at TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS mining_cycles (
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT,
                    status TEXT,
                    start_ts TEXT,
                    end_ts TEXT
                );

                CREATE TABLE IF NOT EXISTS factor_pools (
                    id BIGSERIAL PRIMARY KEY,
                    cycle_id BIGINT,
                    name TEXT,
                    external_key TEXT DEFAULT '',
                    symbol TEXT,
                    direction TEXT DEFAULT 'long',
                    timeframe_min INTEGER,
                    years INTEGER,
                    family TEXT,
                    grid_spec_json TEXT,
                    risk_spec_json TEXT,
                    num_partitions INTEGER,
                    seed INTEGER,
                    active INTEGER DEFAULT 1,
                    created_at TEXT
                );

                CREATE TABLE IF NOT EXISTS mining_tasks (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT,
                    pool_id BIGINT,
                    cycle_id BIGINT,
                    partition_idx INTEGER,
                    num_partitions INTEGER,
                    status TEXT DEFAULT 'assigned',
                    progress_json TEXT DEFAULT '{}',
                    last_heartbeat TEXT,
                    created_at TEXT,
                    updated_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_mining_tasks_pool_cycle_part ON mining_tasks (pool_id, cycle_id, partition_idx);
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_cycle_status ON mining_tasks (cycle_id, status);
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_updated_status ON mining_tasks (updated_at, status);
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_user_cycle_pool_part ON mining_tasks (user_id, cycle_id, pool_id, partition_idx);
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_user_cycle_id_desc ON mining_tasks (user_id, cycle_id, id DESC);
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_activity_at ON mining_tasks (COALESCE(last_heartbeat, updated_at, created_at), user_id);
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_status_user ON mining_tasks (status, user_id);
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_status_id ON mining_tasks (status, id);
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_user_cycle_status_id ON mining_tasks (user_id, cycle_id, status, id);
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_completed_review_status ON mining_tasks ((COALESCE(progress_json::jsonb->>'review_status', progress_json::jsonb->>'oos_status', ''))) WHERE status = 'completed';
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_user_completed_review_status ON mining_tasks (user_id, (COALESCE(progress_json::jsonb->>'review_status', progress_json::jsonb->>'oos_status', ''))) WHERE status = 'completed';
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_activity_status_user ON mining_tasks (COALESCE(last_heartbeat, updated_at, created_at), status, user_id);
                CREATE INDEX IF NOT EXISTS idx_users_runnable ON users(disabled, run_enabled, id);
                CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at);

                CREATE TABLE IF NOT EXISTS submissions (
                    id BIGSERIAL PRIMARY KEY,
                    candidate_id BIGINT,
                    user_id BIGINT,
                    pool_id BIGINT,
                    status TEXT DEFAULT 'pending',
                    audit_json TEXT DEFAULT '{}',
                    submitted_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_submissions_status_user ON submissions(status, user_id);

                CREATE TABLE IF NOT EXISTS candidates (
                    id BIGSERIAL PRIMARY KEY,
                    task_id BIGINT,
                    user_id BIGINT,
                    pool_id BIGINT,
                    direction TEXT DEFAULT 'long',
                    params_json TEXT,
                    metrics_json TEXT,
                    score DOUBLE PRECISION,
                    is_submitted INTEGER DEFAULT 0,
                    created_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_candidates_created_at ON candidates(created_at);
                CREATE INDEX IF NOT EXISTS idx_candidates_created_user_score ON candidates(created_at, user_id, score);

                CREATE TABLE IF NOT EXISTS strategies (
                    id BIGSERIAL PRIMARY KEY,
                    submission_id BIGINT,
                    user_id BIGINT,
                    pool_id BIGINT,
                    external_key TEXT DEFAULT '',
                    direction TEXT DEFAULT 'long',
                    params_json TEXT,
                    status TEXT DEFAULT 'active',
                    allocation_pct DOUBLE PRECISION,
                    note TEXT,
                    created_at TEXT,
                    expires_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_strategies_status_user ON strategies(status, user_id);
                CREATE INDEX IF NOT EXISTS idx_strategies_user_status_created ON strategies(user_id, status, created_at);

                CREATE TABLE IF NOT EXISTS weekly_checks (
                    id BIGSERIAL PRIMARY KEY,
                    strategy_id BIGINT,
                    week_start_ts TEXT,
                    week_end_ts TEXT,
                    return_pct DOUBLE PRECISION,
                    max_drawdown_pct DOUBLE PRECISION,
                    trades INTEGER,
                    eligible INTEGER,
                    checked_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_weekly_checks_checked_strategy ON weekly_checks(checked_at, strategy_id);

                CREATE TABLE IF NOT EXISTS payouts (
                    id BIGSERIAL PRIMARY KEY,
                    strategy_id BIGINT,
                    user_id BIGINT,
                    week_start_ts TEXT,
                    amount_usdt DOUBLE PRECISION,
                    status TEXT DEFAULT 'unpaid',
                    txid TEXT,
                    created_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_payouts_created_at ON payouts(created_at);
                CREATE INDEX IF NOT EXISTS idx_payouts_created_user ON payouts(created_at, user_id);

                CREATE TABLE IF NOT EXISTS runtime_portfolio_snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    scope TEXT NOT NULL,
                    user_id BIGINT,
                    published_by BIGINT,
                    source TEXT NOT NULL DEFAULT 'holy_grail',
                    updated_at TEXT NOT NULL,
                    strategy_count INTEGER NOT NULL DEFAULT 0,
                    checksum TEXT NOT NULL DEFAULT '',
                    summary_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_runtime_portfolio_scope_user ON runtime_portfolio_snapshots(scope, user_id, updated_at);

                CREATE TABLE IF NOT EXISTS runtime_portfolio_items (
                    id BIGSERIAL PRIMARY KEY,
                    snapshot_id BIGINT NOT NULL,
                    strategy_key TEXT NOT NULL DEFAULT '',
                    rank INTEGER NOT NULL DEFAULT 0,
                    strategy_id BIGINT,
                    family TEXT NOT NULL DEFAULT '',
                    symbol TEXT NOT NULL DEFAULT '',
                    direction TEXT NOT NULL DEFAULT 'long',
                    interval TEXT NOT NULL DEFAULT '',
                    stake_pct DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                    sharpe DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                    total_return_pct DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                    max_drawdown_pct DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                    avg_corr DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                    max_corr DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                    params_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_runtime_portfolio_items_snapshot ON runtime_portfolio_items(snapshot_id, rank);

                CREATE TABLE IF NOT EXISTS sys_monitor_events (
                    id BIGSERIAL PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    user_id BIGINT,
                    message TEXT,
                    detail_json TEXT DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_sys_monitor_events_type_ts ON sys_monitor_events(event_type, created_at);
                """
            )

            # [專家級防護] 立刻 commit 基礎表結構，釋放 AccessExclusiveLock，防止 DDL 長時間鎖定資料庫引發死鎖
            try:
                conn.commit()
            except Exception:
                conn.rollback()

            # [專家級修復] 將所有修改獨立為單筆交易，每執行一步就獨立存檔
            statements = [
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS username_norm TEXT",
                "UPDATE users SET username_norm = lower(username) WHERE username_norm IS NULL OR username_norm = ''",
                "CREATE UNIQUE INDEX IF NOT EXISTS users_username_norm_uq ON users(username_norm)",
                "CREATE INDEX IF NOT EXISTS users_username_norm_idx ON users(username_norm)",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS nickname TEXT DEFAULT ''",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar_url TEXT DEFAULT ''",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS disabled INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS run_enabled INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS wallet_address TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS wallet_chain TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS profile_updated_at TEXT",
                "ALTER TABLE factor_pools ADD COLUMN IF NOT EXISTS external_key TEXT DEFAULT ''",
                "ALTER TABLE factor_pools ADD COLUMN IF NOT EXISTS direction TEXT DEFAULT 'long'",
                "ALTER TABLE candidates ADD COLUMN IF NOT EXISTS direction TEXT DEFAULT 'long'",
                "ALTER TABLE strategies ADD COLUMN IF NOT EXISTS external_key TEXT DEFAULT ''",
                "ALTER TABLE strategies ADD COLUMN IF NOT EXISTS direction TEXT DEFAULT 'long'",
                "ALTER TABLE mining_tasks ADD COLUMN IF NOT EXISTS lease_id TEXT",
                "ALTER TABLE mining_tasks ADD COLUMN IF NOT EXISTS lease_worker_id TEXT",
                "ALTER TABLE mining_tasks ADD COLUMN IF NOT EXISTS lease_expires_at TEXT",
                "ALTER TABLE mining_tasks ADD COLUMN IF NOT EXISTS attempt INTEGER DEFAULT 0",
                """
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id TEXT PRIMARY KEY,
                    user_id BIGINT,
                    kind TEXT NOT NULL DEFAULT 'worker',
                    version TEXT NOT NULL DEFAULT '',
                    protocol INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    last_task_id BIGINT,
                    tasks_done INTEGER NOT NULL DEFAULT 0,
                    tasks_fail INTEGER NOT NULL DEFAULT 0,
                    avg_cps DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                    last_error TEXT NOT NULL DEFAULT '',
                    meta_json TEXT NOT NULL DEFAULT '{}'
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS worker_events (
                    id BIGSERIAL PRIMARY KEY,
                    ts TEXT NOT NULL,
                    user_id BIGINT,
                    worker_id TEXT,
                    event TEXT NOT NULL,
                    detail_json TEXT NOT NULL DEFAULT '{}'
                )
                """,
                "CREATE INDEX IF NOT EXISTS idx_workers_last_seen ON workers(last_seen_at)",
                "CREATE INDEX IF NOT EXISTS idx_worker_events_ts ON worker_events(ts)",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_user_cycle_pool_part ON mining_tasks(user_id, cycle_id, pool_id, partition_idx)",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_user_cycle_id_desc ON mining_tasks(user_id, cycle_id, id DESC)",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_activity_at ON mining_tasks((COALESCE(last_heartbeat, updated_at, created_at)), user_id)",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_status_user ON mining_tasks(status, user_id)",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_status_id ON mining_tasks(status, id)",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_user_cycle_status_id ON mining_tasks(user_id, cycle_id, status, id)",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_completed_review_status ON mining_tasks((COALESCE(progress_json::jsonb->>'review_status', progress_json::jsonb->>'oos_status', ''))) WHERE status = 'completed'",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_user_completed_review_status ON mining_tasks(user_id, (COALESCE(progress_json::jsonb->>'review_status', progress_json::jsonb->>'oos_status', ''))) WHERE status = 'completed'",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_activity_status_user ON mining_tasks((COALESCE(last_heartbeat, updated_at, created_at)), status, user_id)",
                "CREATE INDEX IF NOT EXISTS idx_users_runnable ON users(disabled, run_enabled, id)",
                "CREATE INDEX IF NOT EXISTS idx_submissions_status_user ON submissions(status, user_id)",
                "CREATE INDEX IF NOT EXISTS idx_candidates_created_user_score ON candidates(created_at, user_id, score)",
                "CREATE INDEX IF NOT EXISTS idx_strategies_status_user ON strategies(status, user_id)",
                "CREATE INDEX IF NOT EXISTS idx_strategies_user_status_created ON strategies(user_id, status, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_weekly_checks_checked_strategy ON weekly_checks(checked_at, strategy_id)",
                "CREATE INDEX IF NOT EXISTS idx_payouts_created_user ON payouts(created_at, user_id)",
                "ALTER TABLE runtime_portfolio_items ADD COLUMN IF NOT EXISTS strategy_id BIGINT",
                "ALTER TABLE runtime_portfolio_items ADD COLUMN IF NOT EXISTS total_return_pct DOUBLE PRECISION NOT NULL DEFAULT 0.0",
                "ALTER TABLE runtime_portfolio_items ADD COLUMN IF NOT EXISTS max_drawdown_pct DOUBLE PRECISION NOT NULL DEFAULT 0.0",
            ]
            
            for stmt in statements:
                try:
                    conn.execute(stmt)
                    conn.commit()
                except Exception:
                    conn.rollback()

            ensure_default_settings(conn)
            _backfill_direction_columns(conn)
            conn.commit()
            # 執行完 Postgres 的 DDL 後，直接結束函數，絕對不往下跑 SQLite 的邏圈
            return
            
        else:
            # ---------------------------------------------------------
            # SQLite 專用 DDL
            # ---------------------------------------------------------
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL,
                    username_norm TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL DEFAULT 'user',
                    nickname TEXT DEFAULT '',
                    avatar_url TEXT DEFAULT '',
                    disabled INTEGER NOT NULL DEFAULT 0,
                    run_enabled INTEGER NOT NULL DEFAULT 0,
                    wallet_address TEXT NOT NULL DEFAULT '',
                    wallet_chain TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    last_login_at TEXT,
                    profile_updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS audit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    action TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS api_tokens (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    token TEXT NOT NULL UNIQUE,
                    name TEXT,
                    expires_at TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                
                CREATE TABLE IF NOT EXISTS mining_cycles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    status TEXT,
                    start_ts TEXT,
                    end_ts TEXT
                );
                
                CREATE TABLE IF NOT EXISTS factor_pools (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cycle_id INTEGER,
                    name TEXT,
                    external_key TEXT DEFAULT '',
                    symbol TEXT,
                    direction TEXT DEFAULT 'long',
                    timeframe_min INTEGER,
                    years INTEGER,
                    family TEXT,
                    grid_spec_json TEXT,
                    risk_spec_json TEXT,
                    num_partitions INTEGER,
                    seed INTEGER,
                    active INTEGER DEFAULT 1,
                    created_at TEXT
                );
                
                CREATE TABLE IF NOT EXISTS mining_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    pool_id INTEGER,
                    cycle_id INTEGER,
                    partition_idx INTEGER,
                    num_partitions INTEGER,
                    status TEXT DEFAULT 'assigned',
                    progress_json TEXT DEFAULT '{}',
                    last_heartbeat TEXT,
                    created_at TEXT,
                    updated_at TEXT
                );
                
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_pool_cycle_part ON mining_tasks (pool_id, cycle_id, partition_idx);
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_cycle_status ON mining_tasks (cycle_id, status);
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_user_cycle_pool_part ON mining_tasks (user_id, cycle_id, pool_id, partition_idx);
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_user_cycle_id_desc ON mining_tasks (user_id, cycle_id, id DESC);
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_activity_at ON mining_tasks (COALESCE(last_heartbeat, updated_at, created_at), user_id);
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_status_user ON mining_tasks (status, user_id);
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_status_id ON mining_tasks (status, id);
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_user_cycle_status_id ON mining_tasks (user_id, cycle_id, status, id);
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_completed_review_status ON mining_tasks (COALESCE(json_extract(progress_json, '$.review_status'), json_extract(progress_json, '$.oos_status'), '')) WHERE status = 'completed';
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_user_completed_review_status ON mining_tasks (user_id, COALESCE(json_extract(progress_json, '$.review_status'), json_extract(progress_json, '$.oos_status'), '')) WHERE status = 'completed';
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_activity_status_user ON mining_tasks (COALESCE(last_heartbeat, updated_at, created_at), status, user_id);
                CREATE INDEX IF NOT EXISTS idx_users_runnable ON users(disabled, run_enabled, id);
                
                CREATE TABLE IF NOT EXISTS submissions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    candidate_id INTEGER,
                    user_id INTEGER,
                    pool_id INTEGER,
                    status TEXT DEFAULT 'pending',
                    audit_json TEXT DEFAULT '{}',
                    submitted_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_submissions_status_user ON submissions(status, user_id);

                CREATE TABLE IF NOT EXISTS candidates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER,
                    user_id INTEGER,
                    pool_id INTEGER,
                    direction TEXT DEFAULT 'long',
                    params_json TEXT,
                    metrics_json TEXT,
                    score REAL,
                    is_submitted INTEGER DEFAULT 0,
                    created_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_candidates_created_at ON candidates(created_at);
                CREATE INDEX IF NOT EXISTS idx_candidates_created_user_score ON candidates(created_at, user_id, score);
                
                CREATE TABLE IF NOT EXISTS strategies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    submission_id INTEGER,
                    user_id INTEGER,
                    pool_id INTEGER,
                    external_key TEXT DEFAULT '',
                    direction TEXT DEFAULT 'long',
                    params_json TEXT,
                    status TEXT DEFAULT 'active',
                    allocation_pct REAL,
                    note TEXT,
                    created_at TEXT,
                    expires_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_strategies_status_user ON strategies(status, user_id);
                CREATE INDEX IF NOT EXISTS idx_strategies_user_status_created ON strategies(user_id, status, created_at);

                CREATE TABLE IF NOT EXISTS weekly_checks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id INTEGER,
                    week_start_ts TEXT,
                    week_end_ts TEXT,
                    return_pct REAL,
                    max_drawdown_pct REAL,
                    trades INTEGER,
                    eligible INTEGER,
                    checked_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_weekly_checks_checked_strategy ON weekly_checks(checked_at, strategy_id);

                CREATE TABLE IF NOT EXISTS payouts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id INTEGER,
                    user_id INTEGER,
                    week_start_ts TEXT,
                    amount_usdt REAL,
                    status TEXT DEFAULT 'unpaid',
                    txid TEXT,
                    created_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_payouts_created_at ON payouts(created_at);
                CREATE INDEX IF NOT EXISTS idx_payouts_created_user ON payouts(created_at, user_id);

                CREATE TABLE IF NOT EXISTS runtime_portfolio_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scope TEXT NOT NULL,
                    user_id INTEGER,
                    published_by INTEGER,
                    source TEXT NOT NULL DEFAULT 'holy_grail',
                    updated_at TEXT NOT NULL,
                    strategy_count INTEGER NOT NULL DEFAULT 0,
                    checksum TEXT NOT NULL DEFAULT '',
                    summary_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_runtime_portfolio_scope_user ON runtime_portfolio_snapshots(scope, user_id, updated_at);

                CREATE TABLE IF NOT EXISTS runtime_portfolio_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id INTEGER NOT NULL,
                    strategy_key TEXT NOT NULL DEFAULT '',
                    rank INTEGER NOT NULL DEFAULT 0,
                    strategy_id INTEGER,
                    family TEXT NOT NULL DEFAULT '',
                    symbol TEXT NOT NULL DEFAULT '',
                    direction TEXT NOT NULL DEFAULT 'long',
                    interval TEXT NOT NULL DEFAULT '',
                    stake_pct REAL NOT NULL DEFAULT 0.0,
                    sharpe REAL NOT NULL DEFAULT 0.0,
                    total_return_pct REAL NOT NULL DEFAULT 0.0,
                    max_drawdown_pct REAL NOT NULL DEFAULT 0.0,
                    avg_corr REAL NOT NULL DEFAULT 0.0,
                    max_corr REAL NOT NULL DEFAULT 0.0,
                    params_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_runtime_portfolio_items_snapshot ON runtime_portfolio_items(snapshot_id, rank);

                CREATE TABLE IF NOT EXISTS sys_monitor_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    user_id INTEGER,
                    message TEXT,
                    detail_json TEXT DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_sys_monitor_events_type_ts ON sys_monitor_events(event_type, created_at);
                """
            )
            
            statements_sqlite = [
                "ALTER TABLE users ADD COLUMN nickname TEXT DEFAULT ''",
                "ALTER TABLE users ADD COLUMN avatar_url TEXT DEFAULT ''",
                "ALTER TABLE users ADD COLUMN run_enabled INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE users ADD COLUMN wallet_address TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE users ADD COLUMN wallet_chain TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE users ADD COLUMN profile_updated_at TEXT",
                "ALTER TABLE factor_pools ADD COLUMN external_key TEXT DEFAULT ''",
                "ALTER TABLE factor_pools ADD COLUMN direction TEXT DEFAULT 'long'",
                "ALTER TABLE candidates ADD COLUMN direction TEXT DEFAULT 'long'",
                "ALTER TABLE strategies ADD COLUMN external_key TEXT DEFAULT ''",
                "ALTER TABLE strategies ADD COLUMN direction TEXT DEFAULT 'long'",
                "ALTER TABLE mining_tasks ADD COLUMN lease_id TEXT",
                "ALTER TABLE mining_tasks ADD COLUMN lease_worker_id TEXT",
                "ALTER TABLE mining_tasks ADD COLUMN lease_expires_at TEXT",
                "ALTER TABLE mining_tasks ADD COLUMN attempt INTEGER DEFAULT 0",
                """
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id TEXT PRIMARY KEY,
                    user_id INTEGER,
                    kind TEXT NOT NULL DEFAULT 'worker',
                    version TEXT NOT NULL DEFAULT '',
                    protocol INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    last_task_id INTEGER,
                    tasks_done INTEGER NOT NULL DEFAULT 0,
                    tasks_fail INTEGER NOT NULL DEFAULT 0,
                    avg_cps REAL NOT NULL DEFAULT 0.0,
                    last_error TEXT NOT NULL DEFAULT '',
                    meta_json TEXT NOT NULL DEFAULT '{}'
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS worker_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    user_id INTEGER,
                    worker_id TEXT,
                    event TEXT NOT NULL,
                    detail_json TEXT NOT NULL DEFAULT '{}'
                )
                """,
                "CREATE INDEX IF NOT EXISTS idx_workers_last_seen ON workers(last_seen_at)",
                "CREATE INDEX IF NOT EXISTS idx_worker_events_ts ON worker_events(ts)",
                "CREATE INDEX IF NOT EXISTS idx_worker_events_event_ts ON worker_events(event, ts)",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_user_cycle_status_upd ON mining_tasks(user_id, cycle_id, status, updated_at)",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_user_cycle_pool_part ON mining_tasks(user_id, cycle_id, pool_id, partition_idx)",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_user_cycle_id_desc ON mining_tasks(user_id, cycle_id, id DESC)",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_activity_at ON mining_tasks(COALESCE(last_heartbeat, updated_at, created_at), user_id)",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_status_lease ON mining_tasks(status, lease_expires_at)",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_status_fast ON mining_tasks(status)",
                "CREATE INDEX IF NOT EXISTS idx_users_runnable ON users(disabled, run_enabled, id)",
                "CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at)",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_updated_status ON mining_tasks(updated_at, status)",
                "CREATE INDEX IF NOT EXISTS idx_candidates_created_at ON candidates(created_at)",
                "CREATE INDEX IF NOT EXISTS idx_candidates_created_user_score ON candidates(created_at, user_id, score)",
                "CREATE INDEX IF NOT EXISTS idx_strategies_status_user ON strategies(status, user_id)",
                "CREATE INDEX IF NOT EXISTS idx_strategies_user_status_created ON strategies(user_id, status, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_weekly_checks_checked_strategy ON weekly_checks(checked_at, strategy_id)",
                "CREATE INDEX IF NOT EXISTS idx_payouts_created_at ON payouts(created_at)",
                "CREATE INDEX IF NOT EXISTS idx_payouts_created_user ON payouts(created_at, user_id)",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_status_user ON mining_tasks(status, user_id)",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_status_id ON mining_tasks(status, id)",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_user_cycle_status_id ON mining_tasks(user_id, cycle_id, status, id)",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_completed_review_status ON mining_tasks(COALESCE(json_extract(progress_json, '$.review_status'), json_extract(progress_json, '$.oos_status'), '')) WHERE status = 'completed'",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_user_completed_review_status ON mining_tasks(user_id, COALESCE(json_extract(progress_json, '$.review_status'), json_extract(progress_json, '$.oos_status'), '')) WHERE status = 'completed'",
                "CREATE INDEX IF NOT EXISTS idx_mining_tasks_activity_status_user ON mining_tasks(COALESCE(last_heartbeat, updated_at, created_at), status, user_id)",
                "CREATE INDEX IF NOT EXISTS idx_submissions_status_user ON submissions(status, user_id)",
                "ALTER TABLE runtime_portfolio_items ADD COLUMN strategy_id INTEGER",
                "ALTER TABLE runtime_portfolio_items ADD COLUMN total_return_pct REAL NOT NULL DEFAULT 0.0",
                "ALTER TABLE runtime_portfolio_items ADD COLUMN max_drawdown_pct REAL NOT NULL DEFAULT 0.0",
            ]
            
            for stmt in statements_sqlite:
                try:
                    conn.execute(stmt)
                    conn.commit()
                except Exception:
                    pass # SQLite 不支援 IF NOT EXISTS 的 ALTER TABLE 寫法，若報錯通常代表已存在

            ensure_default_settings(conn)
            _backfill_direction_columns(conn)
            conn.commit()

    finally:
        conn.close()


def get_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    log_sys_event("AUTH_LOGIN_TRACE_1", None, "成功進入 get_user_by_username 函數第一行", {"input": username})
    try:
        raw = str(username or "")
        uname_norm = normalize_username(raw)
        if not uname_norm:
            log_sys_event("AUTH_LOGIN_ERROR", None, "格式化帳號為空", {"raw": raw})
            return None
    except Exception as e:
        log_sys_event("AUTH_LOGIN_CRASH", None, f"normalize 崩潰: {e}", {})
        return None

    log_sys_event("AUTH_LOGIN_TRACE_2", None, "準備請求資料庫連線池", {"uname_norm": uname_norm})
    try:
        conn = _conn()
    except Exception as e:
        log_sys_event("AUTH_LOGIN_CRASH", None, f"請求連線池失敗: {e}", {})
        return None

    log_sys_event("AUTH_LOGIN_TRACE_3", None, "成功取得資料庫連線，準備執行 SQL SELECT", {})
    try:
        try:
            row = conn.execute("SELECT * FROM users WHERE username_norm = ? LIMIT 1", (uname_norm,)).fetchone()
            if row:
                row_dict = _decorate_user_row(row, default_avatar_url=_default_avatar_url_from_conn(conn))
                log_sys_event("AUTH_LOGIN_TRACE_4", None, "SQL 查詢成功並找到用戶", {"id": row_dict.get("id")})
                return row_dict
        except Exception as e:
            conn.rollback()
            log_sys_event("AUTH_LOGIN_WARN", None, f"1階查詢失敗: {e}", {})

        log_sys_event("AUTH_LOGIN_TRACE_5", None, "準備執行2階與3階查詢", {})
        try:
            row2 = conn.execute("SELECT * FROM users WHERE lower(username) = ? OR username = ? LIMIT 1", (uname_norm, raw.strip())).fetchone()
            if row2:
                log_sys_event("AUTH_LOGIN_TRACE_6", None, "2階查詢成功", {})
                return _decorate_user_row(row2, default_avatar_url=_default_avatar_url_from_conn(conn))
        except Exception:
            conn.rollback()
            
        try:
            rows = conn.execute("SELECT * FROM users").fetchall()
            for r in rows:
                if str(r.get("username", "")).lower() == uname_norm or str(r.get("username_norm", "")) == uname_norm:
                    log_sys_event("AUTH_LOGIN_TRACE_7", None, "3階全表掃描成功", {})
                    return _decorate_user_row(r, default_avatar_url=_default_avatar_url_from_conn(conn))
        except Exception:
            conn.rollback()

        log_sys_event("AUTH_LOGIN_NOT_FOUND", None, "資料庫中沒有此用戶", {})
        return None
    finally:
        conn.close()


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    try:
        uid = int(user_id)
    except Exception:
        return None
    import time
    for attempt in range(5):
        try:
            conn = _conn()
            try:
                row = conn.execute("SELECT * FROM users WHERE id = ? LIMIT 1", (uid,)).fetchone()
                return _decorate_user_row(row, default_avatar_url=_default_avatar_url_from_conn(conn)) if row else None
            finally:
                conn.close()
        except Exception:
            if attempt == 4:
                return None
            time.sleep(0.1 * (2 ** attempt))
    return None


def create_user(
    username: str,
    password_hash: str,
    role: str = "user",
    wallet_address: str = "",
    wallet_chain: str = "",
    nickname: str = "",
    avatar_url: str = "",
) -> int:
    log_sys_event("AUTH_REG_TRACE_1", None, "成功進入 create_user 函數第一行", {"input_username": username})
    try:
        uname = str(username or "").strip()
        uname_norm = normalize_username(uname)
        if not uname_norm:
            log_sys_event("AUTH_REG_ERROR", None, "無效的註冊名稱", {})
            raise ValueError("invalid username")
        safe_nickname = _sanitize_nickname_text(nickname, uname)
        safe_avatar_url = _sanitize_avatar_url(avatar_url)
        pw_str = password_hash.decode("utf-8") if isinstance(password_hash, bytes) else str(password_hash or "")
    except Exception as e:
        log_sys_event("AUTH_REG_CRASH", None, f"註冊前置變數處理失敗: {e}", {})
        raise e

    log_sys_event("AUTH_REG_TRACE_2", None, "準備請求資料庫連線", {})
    try:
        conn = _conn()
    except Exception as e:
        log_sys_event("AUTH_REG_CRASH", None, f"註冊取得連線失敗: {e}", {})
        raise e

    log_sys_event("AUTH_REG_TRACE_3", None, "連線取得成功，準備執行 INSERT", {})
    try:
        if getattr(conn, "kind", "sqlite") == "postgres":
            row = conn.execute(
                "INSERT INTO users (username, username_norm, password_hash, role, nickname, avatar_url, disabled, run_enabled, wallet_address, wallet_chain, created_at, profile_updated_at) VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?) RETURNING id",
                (uname, uname_norm, pw_str, str(role or "user"), safe_nickname, safe_avatar_url, str(wallet_address or ""), str(wallet_chain or ""), _now_iso(), _now_iso())
            ).fetchone()
            conn.commit()
            new_id = int((row or {}).get("id") or 0)
            log_sys_event("AUTH_REG_TRACE_4", new_id, "INSERT 成功 (PG)", {})
            return new_id

        cur = conn.execute(
            "INSERT INTO users (username, username_norm, password_hash, role, nickname, avatar_url, disabled, run_enabled, wallet_address, wallet_chain, created_at, profile_updated_at) VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?)",
            (uname, uname_norm, pw_str, str(role or "user"), safe_nickname, safe_avatar_url, str(wallet_address or ""), str(wallet_chain or ""), _now_iso(), _now_iso())
        )
        conn.commit()
        new_id = int(cur.lastrowid)
        log_sys_event("AUTH_REG_TRACE_4", new_id, "INSERT 成功 (SQLite)", {})
        return new_id
    except Exception as e:
        conn.rollback()
        log_sys_event("AUTH_REG_FATAL", None, f"寫入 DB 失敗 (可能是帳號重複): {e}", {})
        raise e
    finally:
        conn.close()


def list_users(limit: int = 500) -> List[Dict[str, Any]]:
    conn = _conn()
    try:
        rows = conn.execute("SELECT * FROM users ORDER BY id DESC LIMIT ?", (int(limit),)).fetchall()
        default_avatar_url = _default_avatar_url_from_conn(conn)
        return [_decorate_user_row(r, default_avatar_url=default_avatar_url) for r in rows]
    finally:
        conn.close()


def list_runnable_users(limit: int = 1000) -> List[Dict[str, Any]]:
    conn = _conn()
    try:
        rows = conn.execute(
            """
            SELECT id, username, nickname, avatar_url, disabled, run_enabled
            FROM users
            WHERE COALESCE(disabled, 0) = 0
              AND COALESCE(run_enabled, 0) = 1
            ORDER BY id ASC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        default_avatar_url = _default_avatar_url_from_conn(conn)
        return [_decorate_user_row(r, default_avatar_url=default_avatar_url) for r in rows]
    finally:
        conn.close()


def set_user_disabled(user_id: int, disabled: bool) -> None:
    conn = _conn()
    try:
        conn.execute("UPDATE users SET disabled = ? WHERE id = ?", (1 if disabled else 0, int(user_id)))
        conn.commit()
    finally:
        conn.close()


def is_user_locked(user_id: int) -> bool:
    row = get_user_by_id(user_id)
    if not row:
        return True
    try:
        return int(row["disabled"] or 0) == 1
    except Exception:
        return False


def update_user_login_state(user_id: int, success: bool = True) -> None:
    log_sys_event("AUTH_STATE_TRACE_1", user_id, "成功進入 update_user_login_state 第一行", {"success": success})
    try:
        conn = _conn()
    except Exception as e:
        log_sys_event("AUTH_STATE_CRASH", user_id, f"取得連線失敗: {e}", {})
        return

    log_sys_event("AUTH_STATE_TRACE_2", user_id, "成功取得連線，準備執行 UPDATE", {})
    try:
        if success:
            now = _now_iso()
            try:
                conn.execute("UPDATE users SET last_login_at = ?, run_enabled = 0 WHERE id = ?", (now, int(user_id)))
                conn.commit()
                log_sys_event("AUTH_STATE_TRACE_3", user_id, "users 表更新成功", {})
            except Exception as e:
                conn.rollback()
                log_sys_event("AUTH_STATE_FATAL", user_id, f"更新 user 表失敗: {e}", {})
            
            log_sys_event("AUTH_STATE_TRACE_4", user_id, "準備釋放幽靈任務", {})
            try:
                conn.execute("UPDATE mining_tasks SET status='assigned', lease_id=NULL, lease_worker_id=NULL, lease_expires_at=NULL, updated_at=? WHERE user_id=? AND status IN ('running', 'queued')", (now, int(user_id)))
                conn.commit()
                log_sys_event("AUTH_STATE_TRACE_5", user_id, "幽靈任務釋放成功", {})
            except Exception as e_lease:
                conn.rollback()
                log_sys_event("AUTH_STATE_WARN", user_id, f"幽靈任務釋放失敗: {e_lease}", {})
    finally:
        conn.close()


def get_user_run_enabled(user_id: int) -> bool:
    row = get_user_by_id(user_id)
    if not row:
        return False
    try:
        return int(row["run_enabled"] or 0) == 1
    except Exception:
        return False


def set_user_run_enabled(user_id: int, enabled: bool) -> None:
    uid = int(user_id or 0)
    if uid <= 0:
        return

    import time, random
    for attempt in range(10):
        try:
            conn = _conn()
            try:
                conn.execute("UPDATE users SET run_enabled = ? WHERE id = ?", (1 if enabled else 0, uid))
                conn.commit() # 先存檔，避免後續報錯導致狀態遺失

                # 關閉時：回收該 user 所有 running/queued 任務，避免卡死或浪費算力
                if not bool(enabled):
                    now = _now_iso()
                    try:
                        conn.execute(
                            """
                            UPDATE mining_tasks
                            SET status='assigned',
                                lease_id=NULL,
                                lease_worker_id=NULL,
                                lease_expires_at=NULL,
                                updated_at=?
                            WHERE user_id=? AND status IN ('running', 'queued')
                            """,
                            (now, uid),
                        )
                        conn.commit()
                    except Exception:
                        conn.rollback() # 清除 Postgres 的交易死鎖狀態
                        conn.execute(
                            "UPDATE mining_tasks SET status='assigned', updated_at=? WHERE user_id=? AND status IN ('running', 'queued')",
                            (now, uid),
                        )
                        conn.commit()
                break
            finally:
                conn.close()
        except Exception as e:
            if attempt == 9:
                import sys
                print(f"[CRITICAL DB ERROR] set_user_run_enabled 放棄重試: {e}", file=sys.stderr)
            time.sleep(random.uniform(0.1, 0.5) * (1.2 ** attempt))


def get_wallet_info(user_id: int) -> Dict[str, str]:
    row = get_user_by_id(user_id)
    if not row:
        return {"wallet_address": "", "wallet_chain": ""}
    return {
        "wallet_address": str(row["wallet_address"] or ""),
        "wallet_chain": str(row["wallet_chain"] or ""),
    }


def get_wallet_address(user_id: int) -> str:
    return get_wallet_info(user_id).get("wallet_address", "")


def set_wallet_address(user_id: int, wallet_address: str, wallet_chain: str = "") -> None:
    conn = _conn()
    try:
        conn.execute(
            "UPDATE users SET wallet_address = ?, wallet_chain = ? WHERE id = ?",
            (str(wallet_address or ""), str(wallet_chain or ""), int(user_id)),
        )
        conn.commit()
    finally:
        conn.close()


def get_setting(arg1: Any, arg2: Any = None, arg3: Any = None) -> Any:
    if bool(getattr(arg1, "_is_db_conn", False)):
        conn = arg1
        k = str(arg2 or "").strip()
        default = arg3
        if not k:
            return default
        row = conn.execute("SELECT value FROM settings WHERE key = ? LIMIT 1", (k,)).fetchone()
        if not row:
            return default
        row_dict = dict(row)
        try:
            return json.loads(row_dict.get("value"))
        except Exception:
            return row_dict.get("value")
    else:
        k = str(arg1 or "").strip()
        default = arg2

    if not k:
        return default
    conn = _conn()
    try:
        row = conn.execute("SELECT value FROM settings WHERE key = ? LIMIT 1", (k,)).fetchone()
        if not row:
            return default
        row_dict = dict(row)
        try:
            return json.loads(row_dict.get("value"))
        except Exception:
            return row_dict.get("value")
    finally:
        conn.close()


def set_setting(arg1: Any, arg2: Any, arg3: Any = None) -> None:
    if bool(getattr(arg1, "_is_db_conn", False)):
        conn = arg1
        k = str(arg2 or "").strip()
        value = arg3
        if not k:
            return
        v_json = json.dumps(value, ensure_ascii=False)
        conn.execute(
            """
            INSERT INTO settings (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            (k, v_json, _now_iso()),
        )
        return

    k = str(arg1 or "").strip()
    value = arg2
    if not k:
        return
    v_json = json.dumps(value, ensure_ascii=False)
    conn = _conn()
    try:
        conn.execute(
            """
            INSERT INTO settings (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            (k, v_json, _now_iso()),
        )
        conn.commit()
    finally:
        conn.close()


def get_settings_details(keys: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    conn = _conn()
    try:
        for raw_key in list(keys or []):
            key = str(raw_key or "").strip()
            if not key:
                continue
            row = conn.execute("SELECT value, updated_at FROM settings WHERE key = ? LIMIT 1", (key,)).fetchone()
            if not row:
                out[key] = {"value": None, "updated_at": None}
                continue

            row_dict = dict(row)
            value = row_dict.get("value")
            try:
                value = json.loads(value)
            except Exception:
                pass

            out[key] = {
                "value": value,
                "updated_at": row_dict.get("updated_at"),
            }
        return out
    finally:
        conn.close()




def write_audit_log(user_id: Optional[int], action: str, payload: Any) -> None:
    conn = _conn()
    try:
        payload_json = json.dumps(payload or {}, ensure_ascii=False)
        conn.execute(
            "INSERT INTO audit_logs (user_id, action, payload_json, created_at) VALUES (?, ?, ?, ?)",
            (int(user_id) if user_id is not None else None, str(action or ""), payload_json, _now_iso()),
        )
        conn.commit()
    finally:
        conn.close()

def create_api_token(user_id: int, ttl_seconds: int, name: str = "worker") -> dict:
    log_sys_event("AUTH_TOKEN_STEP_1", user_id, "進入核發 API Token 函數", {"ttl": ttl_seconds, "name": name})
    import secrets
    token = secrets.token_urlsafe(32)
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
    
    try:
        conn = _conn()
    except Exception as conn_e:
        import traceback
        log_sys_event("AUTH_TOKEN_CRASH", user_id, f"Token 取得連線卡死: {conn_e}", {"trace": traceback.format_exc()})
        return {}

    try:
        if getattr(conn, "kind", "sqlite") == "postgres":
            row = conn.execute(
                "INSERT INTO api_tokens (user_id, token, name, expires_at, created_at) VALUES (?, ?, ?, ?, ?) RETURNING id",
                (user_id, token, name, expires_at, _now_iso())
            ).fetchone()
            conn.commit()
            log_sys_event("AUTH_TOKEN_STEP_2", user_id, "成功核發 API Token (PG)", {})
            return {"token_id": int((row or {}).get("id") or 0), "token": token, "expires_at": expires_at, "issued_at": _now_iso()}

        cur = conn.execute(
            "INSERT INTO api_tokens (user_id, token, name, expires_at, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, token, name, expires_at, _now_iso())
        )
        conn.commit()
        log_sys_event("AUTH_TOKEN_STEP_2", user_id, "成功核發 API Token (SQLite)", {})
        return {"token_id": cur.lastrowid, "token": token, "expires_at": expires_at, "issued_at": _now_iso()}
    except Exception as e:
        conn.rollback()
        import traceback
        log_sys_event("AUTH_TOKEN_ERROR", user_id, f"Token 寫入資料庫失敗: {e}", {"trace": traceback.format_exc()})
        print(f"[DB ERROR] create_api_token: {e}")
        return {}
    finally:
        conn.close()

def revoke_api_token(token_id: int) -> None:
    conn = _conn()
    try:
        conn.execute("DELETE FROM api_tokens WHERE id = ?", (token_id,))
        conn.commit()
    finally:
        conn.close()

def revoke_api_tokens_for_user(user_id: int, name: str = "", exclude_token_id: int = 0) -> int:
    conn = _conn()
    try:
        params: List[Any] = [int(user_id)]
        if str(name or "").strip():
            query = "DELETE FROM api_tokens WHERE user_id = ? AND name = ?"
            params.append(str(name))
        else:
            query = "DELETE FROM api_tokens WHERE user_id = ?"
        if int(exclude_token_id or 0) > 0:
            query += " AND id <> ?"
            params.append(int(exclude_token_id))
        cur = conn.execute(query, params)
        conn.commit()
        return int(cur.rowcount or 0)
    finally:
        conn.close()

def list_api_tokens_for_user(user_id: int, name: str = "") -> list:
    conn = _conn()
    try:
        if str(name or "").strip():
            cur = conn.execute(
                "SELECT * FROM api_tokens WHERE user_id = ? AND name = ? ORDER BY id DESC",
                (int(user_id), str(name)),
            )
        else:
            cur = conn.execute(
                "SELECT * FROM api_tokens WHERE user_id = ? ORDER BY id DESC",
                (int(user_id),),
            )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()

def verify_api_token(token: str) -> Optional[dict]:
    conn = _conn()
    try:
        row = conn.execute("SELECT * FROM api_tokens WHERE token = ? LIMIT 1", (token,)).fetchone()
        if not row: return None
        if _now_iso() > row["expires_at"]: return None
        user = conn.execute("SELECT * FROM users WHERE id = ?", (row["user_id"],)).fetchone()
        if not user: return None
        return {"user": dict(user), "token": dict(row)}
    except Exception as e:
        print(f"[DB ERROR] verify_api_token: {e}")
        return None
    finally:
        conn.close()

def touch_api_token(token_id: int, ip: str = "", user_agent: str = "") -> None:
    conn = _conn()
    try:
        # [專家級修復] 更新最近活動時間，避免使用者活躍期間 Token 無預警過期
        conn.execute(
            "UPDATE api_tokens SET expires_at = ? WHERE id = ?",
            ((datetime.now(timezone.utc) + timedelta(days=7)).isoformat(), token_id)
        )
        conn.commit()
    except Exception as e:
        print(f"[DB ERROR] touch_api_token 發生異常: {e}")
    finally:
        conn.close()



# [專家修復] 已移除重複宣告的 _db_path 與 _conn 函式，防止模組載入時發生路徑解析覆蓋衝突。
from datetime import datetime as _safe_dt, timezone as _safe_tz, timedelta as _safe_td

def ensure_cycle_rollover() -> None:
    conn = _conn()
    try:
        cur = conn.execute("SELECT id, start_ts, end_ts FROM mining_cycles WHERE status = 'active' ORDER BY id DESC LIMIT 1")
        active = cur.fetchone()
        now_dt = _safe_dt.now(_safe_tz.utc)
        now_str = now_dt.isoformat()

        if not active:
            end_ts = (now_dt + _safe_td(days=7)).isoformat()
            conn.execute("INSERT INTO mining_cycles (name, status, start_ts, end_ts) VALUES (?, ?, ?, ?)",
                            ("Cycle 1", "active", now_str, end_ts))
            conn.commit()
        else:
            if now_str > active["end_ts"]:
                conn.execute("UPDATE mining_cycles SET status = 'completed' WHERE id = ?", (active["id"],))
                
                new_end = (now_dt + _safe_td(days=7)).isoformat()
                if getattr(conn, "kind", "sqlite") == "postgres":
                    row_cyc = conn.execute("INSERT INTO mining_cycles (name, status, start_ts, end_ts) VALUES (?, ?, ?, ?) RETURNING id",
                                    (f"Cycle {active['id'] + 1}", "active", now_str, new_end)).fetchone()
                    new_cycle_id = int((row_cyc or {}).get("id") or 0)
                else:
                    cur2 = conn.execute("INSERT INTO mining_cycles (name, status, start_ts, end_ts) VALUES (?, ?, ?, ?)",
                                    (f"Cycle {active['id'] + 1}", "active", now_str, new_end))
                    new_cycle_id = int(cur2.lastrowid)
                
                if new_cycle_id <= 0:
                    raise ValueError(f"無法取得新週期的 ID (new_cycle_id={new_cycle_id})，資料庫方言解析異常")
                
                try:
                    conn.execute("""
                        INSERT INTO factor_pools (
                            cycle_id, name, external_key, symbol, direction, timeframe_min, years, family, 
                            grid_spec_json, risk_spec_json, num_partitions, seed, 
                            active, created_at
                        )
                        SELECT ?, name, COALESCE(external_key, ''), symbol, COALESCE(direction, 'long'), timeframe_min, years, family, 
                               grid_spec_json, risk_spec_json, num_partitions, seed, 
                               1, ?
                        FROM factor_pools src
                        WHERE src.cycle_id = ? AND src.active = 1
                        AND NOT EXISTS (
                            SELECT 1 FROM factor_pools target 
                            WHERE target.cycle_id = ? 
                            AND target.name = src.name 
                            AND target.symbol = src.symbol
                        )
                    """, (new_cycle_id, now_str, active["id"], new_cycle_id))
                    
                    # 4. 審計日誌
                    conn.execute(
                        "INSERT INTO audit_logs (user_id, action, payload_json, created_at) VALUES (NULL, ?, ?, ?)",
                        ("cycle_auto_inheritance", json.dumps({"from": active["id"], "to": new_cycle_id}), now_str)
                    )
                    conn.execute(
                        "INSERT INTO sys_monitor_events (event_type, user_id, message, detail_json, created_at) VALUES (?, ?, ?, ?, ?)",
                        ("CYCLE_ROLLOVER_OK", None, f"成功從週期 {active['id']} 繼承 Pool 至 {new_cycle_id}", json.dumps({"from": active["id"], "to": new_cycle_id}), now_str)
                    )
                except Exception as cycle_fatal:
                    import traceback
                    print(f"[CRITICAL DB ERROR] 週期 Pool 繼承失敗: {cycle_fatal}")
                    conn.execute(
                        "INSERT INTO sys_monitor_events (event_type, user_id, message, detail_json, created_at) VALUES (?, ?, ?, ?, ?)",
                        ("CYCLE_ROLLOVER_FAIL", None, f"週期 Pool 繼承失敗: {cycle_fatal}", json.dumps({"trace": traceback.format_exc()}), now_str)
                    )
                    
                conn.commit()
    except Exception:
        pass
    finally:
        conn.close()

def get_active_cycle() -> dict:
    import time
    for attempt in range(5):
        try:
            conn = _conn()
            try:
                cur = conn.execute("SELECT id, name, status, start_ts, end_ts FROM mining_cycles WHERE status = 'active' ORDER BY id DESC LIMIT 1")
                row = cur.fetchone()
                if row:
                    return dict(row)
                return {}
            finally:
                conn.close()
        except Exception:
            if attempt == 4:
                return {}
            time.sleep(0.1 * (2 ** attempt))
    return {}

def list_factor_pools(cycle_id: int) -> list:
    """專家級 Pool 檢索：具備自動修復與跨週期一致性檢查機制"""
    import time, random
    last_err = None
    for attempt in range(12):
        try:
            conn = _conn()
            try:
                cur = conn.execute("SELECT * FROM factor_pools WHERE cycle_id = ?", (int(cycle_id),))
                rows = [dict(row) for row in cur.fetchall()]
                
                if not rows:
                    last_p_cycle = conn.execute("SELECT cycle_id FROM factor_pools ORDER BY cycle_id DESC LIMIT 1").fetchone()
                    if last_p_cycle and last_p_cycle["cycle_id"] != cycle_id:
                        source_cid = last_p_cycle["cycle_id"]
                        print(f"[DB MAINTENANCE] 偵測到週期 {cycle_id} 缺乏 Pool 資料，啟動從週期 {source_cid} 繼承程序...")
                        try:
                            conn.execute("""
                                INSERT INTO factor_pools (cycle_id, name, external_key, symbol, direction, timeframe_min, years, family, grid_spec_json, risk_spec_json, num_partitions, seed, active, created_at)
                                SELECT ?, name, COALESCE(external_key, ''), symbol, COALESCE(direction, 'long'), timeframe_min, years, family, grid_spec_json, risk_spec_json, num_partitions, seed, active, ?
                                FROM factor_pools WHERE cycle_id = ? AND active = 1
                            """, (cycle_id, _now_iso(), source_cid))
                            conn.commit()
                            cur = conn.execute("SELECT * FROM factor_pools WHERE cycle_id = ?", (cycle_id,))
                            rows = [dict(row) for row in cur.fetchall()]
                        except Exception as rescue_e:
                            import traceback
                            print(f"[FATAL DB ERROR] Pool 跨週期繼承失敗: {rescue_e}\n{traceback.format_exc()}")
                for row in rows:
                    direction = _infer_direction(direction=row.get("direction"), risk_spec_json=row.get("risk_spec_json"))
                    row["direction"] = direction
                    row["external_key"] = str(row.get("external_key") or "")
                    risk_spec = _normalize_risk_spec(direction, row.get("risk_spec_json"))
                    row["risk_spec_json"] = json.dumps(risk_spec, ensure_ascii=False)
                return rows
            finally:
                conn.close()
        except Exception as e:
            last_err = e
            time.sleep(random.uniform(0.2, 0.8) * (1.2 ** attempt))
    
    # [專家級防護] 絕對禁止因鎖死而回傳空陣列，這會導致前端誤判並將所有任務過濾掉變成空表格
    raise RuntimeError(f"資料庫高併發鎖定，無法讀取策略池列表。請稍後再試。({last_err})")

def log_sys_event(event_type: str, user_id: Optional[int], message: str, detail: dict = None) -> None:
    """終極監視：非同步背景獨立執行緒，保證絕對不被主程式的死鎖拖累 (專家級強化：消除靜默失敗)"""
    if event_type == "TASK_ASSIGN_SKIP":
        return

    import sys
    import json
    import threading
    import traceback

    try:
        payload_str = json.dumps(detail or {}, ensure_ascii=False)
    except Exception:
        payload_str = "{}"

    # 第一道防線：直接印到 Docker Console，就算資料庫炸了也看得到
    print(f"[SYS_EVENT] {event_type} | UID:{user_id} | MSG:{message} | DETAIL:{payload_str}", file=sys.stderr, flush=True)

    def _write_event():
        try:
            conn = _conn()
            try:
                conn.execute(
                    "INSERT INTO sys_monitor_events (event_type, user_id, message, detail_json, created_at) VALUES (?, ?, ?, ?, ?)",
                    (event_type, user_id, message, payload_str, _now_iso())
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            # [專家級修復] 絕對禁止吞噬錯誤！若寫入資料庫失敗，必須在 Console 噴出致命錯誤，才能抓出 DBeaver 沒資料的原因
            err_str = traceback.format_exc()
            print(f"[FATAL DB WRITE ERROR] log_sys_event 寫入資料庫失敗! Event: {event_type}\nReason: {e}\n{err_str}", file=sys.stderr, flush=True)

    if event_type in {"UNKNOWN_ROUTE", "DEPRECATED_ALIAS_USED"}:
        _write_event()
        return

    # 第二道防線：丟進背景執行緒，主程式不需等待寫入完成，徹底消滅日誌引發的連環卡死
    t = threading.Thread(target=_write_event, daemon=True)
    t.start()

def assign_tasks_for_user(user_id: int, cycle_id: int = 0, min_tasks: int = 2, max_tasks: int = 6, preferred_family: str = "") -> None:
    """[專家級修復] 原子性任務派發，完全消滅競態條件與重複分配
    
    關鍵改進：
    1. 使用 SELECT ... FOR UPDATE 鎖定用戶記錄
    2. 單一交易完成讀取 → 檢查 → 派發，無中斷窗口
    3. 插入前驗證分區未被佔用（INSERT ... SELECT ... WHERE NOT EXISTS）
    """
    import time, random
    for attempt in range(5):
        try:
            conn = _conn()
            try:
                # [獨家修復] 立即加鎖，防止並發請求在此用戶的派發過程中介入
                if getattr(conn, "kind", "sqlite") == "postgres":
                    # PostgreSQL：行鎖定
                    conn.execute("SELECT 1 FROM users WHERE id = ? FOR UPDATE", (user_id,))
                else:
                    # SQLite：表鎖定
                    try:
                        conn.execute("PRAGMA query_only = FALSE")
                    except Exception:
                        pass
                
                if cycle_id <= 0:
                    cycle_row = conn.execute("SELECT id FROM mining_cycles WHERE status = 'active' ORDER BY id DESC LIMIT 1").fetchone()
                    if not cycle_row: 
                        log_sys_event("TASK_ASSIGN_FAIL", user_id, "找不到 Active 狀態的週期，無法派發", {})
                        return
                    cycle_id = cycle_row["id"]
                
                # [改進] 使用 COUNT 而非 FETCHALL，減少記憶體開銷
                cur = conn.execute("SELECT COUNT(*) as c FROM mining_tasks WHERE user_id = ? AND status IN ('assigned', 'running', 'queued') AND cycle_id = ?", (user_id, cycle_id))
                row = cur.fetchone()
                if row is None:
                    current_tasks = 0
                else:
                    try:
                        current_tasks = int(row["c"] or 0)
                    except Exception:
                        current_tasks = int(row[0] or 0)
                
                if current_tasks >= min_tasks:
                    # 靜謐無聲返回，避免日誌污染
                    break
                    
                needed = min(max_tasks - current_tasks, min_tasks - current_tasks)
                if needed <= 0:
                    break
                
                # 池列表查詢（維持原邏輯）
                pools_query = "SELECT id, num_partitions FROM factor_pools WHERE cycle_id = ? AND active = 1"
                params = [cycle_id]
                if preferred_family:
                    pools_query += " AND family = ?"
                    params.append(preferred_family)
                
                pools = conn.execute(pools_query, params).fetchall()
                if not pools and preferred_family:
                    pools = conn.execute("SELECT id, num_partitions FROM factor_pools WHERE cycle_id = ? AND active = 1", (cycle_id,)).fetchall()
                    
                if not pools:
                    # 緊急繼承邏輯（維持原樣）
                    try:
                        last_p_cycle = conn.execute("SELECT cycle_id FROM factor_pools WHERE active = 1 AND cycle_id > 0 ORDER BY cycle_id DESC LIMIT 1").fetchone()
                        if last_p_cycle and last_p_cycle["cycle_id"] != cycle_id:
                            source_cid = last_p_cycle["cycle_id"]
                            log_sys_event("TASK_ASSIGN_RESCUE", user_id, f"偵測到週期 {cycle_id} 缺乏 Pool，緊急從週期 {source_cid} 繼承", {"source_cid": source_cid})
                            
                            conn.execute("""
                                INSERT INTO factor_pools (cycle_id, name, external_key, symbol, direction, timeframe_min, years, family, grid_spec_json, risk_spec_json, num_partitions, seed, active, created_at)
                                SELECT ?, name, COALESCE(external_key, ''), symbol, COALESCE(direction, 'long'), timeframe_min, years, family, grid_spec_json, risk_spec_json, num_partitions, seed, active, ?
                                FROM factor_pools WHERE cycle_id = ? AND active = 1
                            """, (cycle_id, _now_iso(), source_cid))
                            conn.commit()
                            pools = conn.execute("SELECT id, num_partitions FROM factor_pools WHERE cycle_id = ? AND active = 1", (cycle_id,)).fetchall()
                    except Exception as rescue_e:
                        conn.rollback()
                        import traceback
                        log_sys_event("TASK_ASSIGN_RESCUE_FAIL", user_id, f"緊急繼承 Pool 失敗: {rescue_e}", {"trace": traceback.format_exc()})

                if not pools:
                    log_sys_event("TASK_ASSIGN_FAIL", user_id, f"目前週期 {cycle_id} 無活躍策略池，停止派發", {"cycle_id": cycle_id})
                    break
                
                # [改進] 使用集合快速查詢已佔用分區
                assigned_count = 0
                pool_list = [dict(p) for p in pools]
                random.shuffle(pool_list)
                sample_size = min(len(pool_list), max(32, needed * 8))
                if sample_size > 0:
                    pool_list = pool_list[:sample_size]
                now_str = _now_iso()
                
                for p in pool_list:
                    if assigned_count >= needed:
                        break
                        
                    pid = int(p["id"])
                    num_parts = int(p["num_partitions"])
                    if num_parts <= 0:
                        continue

                    taken_rows = conn.execute(
                        "SELECT DISTINCT partition_idx FROM mining_tasks WHERE cycle_id = ? AND pool_id = ?",
                        (cycle_id, pid),
                    ).fetchall()
                    taken_parts = {
                        int(t["partition_idx"])
                        for t in taken_rows
                        if t.get("partition_idx") is not None
                    }
                    available_parts = [part_idx for part_idx in range(num_parts) if part_idx not in taken_parts]
                    
                    if not available_parts:
                        continue
                    
                    random.shuffle(available_parts)
                    
                    for chosen_part in available_parts[:needed - assigned_count]:
                        try:
                            # [原子性插入] 使用 INSERT ... WHERE NOT EXISTS 防止重複分配
                            if getattr(conn, "kind", "sqlite") == "postgres":
                                cur_insert = conn.execute("""
                                    INSERT INTO mining_tasks (user_id, pool_id, cycle_id, partition_idx, num_partitions, status, created_at, updated_at)
                                    SELECT ?, ?, ?, ?, ?, 'assigned', ?, ?
                                    WHERE NOT EXISTS (
                                        SELECT 1 FROM mining_tasks 
                                        WHERE pool_id = ? AND partition_idx = ? AND cycle_id = ? 
                                        AND status IN ('assigned', 'running', 'queued', 'completed')
                                    )
                                """, (user_id, pid, cycle_id, chosen_part, num_parts, now_str, now_str,
                                      pid, chosen_part, cycle_id))
                            else:
                                # SQLite 版本
                                cur_insert = conn.execute("""
                                    INSERT INTO mining_tasks (user_id, pool_id, cycle_id, partition_idx, num_partitions, status, created_at, updated_at)
                                    SELECT ?, ?, ?, ?, ?, 'assigned', ?, ?
                                    WHERE NOT EXISTS (
                                        SELECT 1 FROM mining_tasks 
                                        WHERE pool_id = ? AND partition_idx = ? AND cycle_id = ?
                                    )
                                """, (user_id, pid, cycle_id, chosen_part, num_parts, now_str, now_str,
                                      pid, chosen_part, cycle_id))
                            try:
                                inserted = int(getattr(cur_insert, "rowcount", 0) or 0)
                            finally:
                                try:
                                    cur_insert.close()
                                except Exception:
                                    pass
                            if inserted <= 0:
                                continue
                            assigned_count += 1
                            taken_parts.add(chosen_part)
                        except Exception as insert_e:
                            # 忽略唯一性衝突（另一個請求搶先了）
                            if "UNIQUE" not in str(insert_e).upper():
                                raise
                                
                if assigned_count > 0:
                    conn.commit()
                    log_sys_event("TASK_ASSIGN_SUCCESS", user_id, f"成功派發了 {assigned_count} 個新任務", {"needed": needed, "assigned": assigned_count})
                break
            finally:
                conn.close()
        except Exception as e:
            if attempt == 4:
                import traceback
                err_str = traceback.format_exc()
                log_sys_event("TASK_ASSIGN_CRASH", user_id, f"派發任務時發生嚴重例外: {e}", {"trace": err_str})
            time.sleep(0.05 * (2 ** attempt))
def _safe_listdir(dir_path: str) -> List[str]:
    try:
        return [os.path.join(dir_path, x) for x in os.listdir(dir_path)]
    except Exception:
        return []

def _sqlite_has_table(conn: sqlite3.Connection, table: str) -> bool:
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table,),
        ).fetchone()
        return bool(row)
    except Exception:
        return False

def _read_factor_pools_from_sqlite(db_path: str) -> List[Dict[str, Any]]:
    try:
        # UI 不准因 DB 鎖等到天荒地老：預設 2 秒就放棄，避免你看到「動畫跑完→空白卡死」
        try:
            timeout_s = float(os.environ.get("SHEEP_SQLITE_TIMEOUT_S", "2.0") or "2.0")
        except Exception:
            timeout_s = 2.0
        timeout_s = max(0.2, min(30.0, float(timeout_s)))

        conn = sqlite3.connect(path, timeout=timeout_s, check_same_thread=False)
        conn.row_factory = sqlite3.Row
    except Exception:
        return []
    try:
        if not _sqlite_has_table(conn, "factor_pools"):
            return []
        rows = conn.execute("SELECT * FROM factor_pools").fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass

def recover_factor_pools_from_local(cycle_id: int, search_roots: Optional[List[str]] = None, max_files: int = 80) -> Dict[str, Any]:
    """
    從本機/容器常見位置掃描舊的 sqlite db，將 factor_pools 匯入目前 cycle。
    目的：救回「原本設定過但現在看不到」的 Pool。
    """
    report: Dict[str, Any] = {
        "cycle_id": int(cycle_id),
        "current_db_path": _db_path(),
        "scanned_files": 0,
        "candidates": [],
        "imported": 0,
        "skipped_duplicates": 0,
        "errors": [],
    }

    cur_db = _db_path()
    roots = search_roots[:] if search_roots else []
    if not roots:
        try:
            here = os.path.dirname(os.path.abspath(__file__))
        except Exception:
            here = os.getcwd()

        roots = [
            os.getcwd(),
            here,
            os.path.join(here, "data"),
            "/app",
            "/app/data",
            "/data",
            "/mnt",
            "/mnt/data",
        ]

    # 收集 db 檔候選
    seen = set()
    db_files: List[str] = []
    for r in roots:
        if not r or not isinstance(r, str):
            continue
        r = r.strip()
        if not r:
            continue

        # 若給的是檔案就直接加入
        if os.path.isfile(r) and r.lower().endswith(".db"):
            if r not in seen:
                seen.add(r)
                db_files.append(r)
            continue

        # 若是資料夾就掃描一層
        if os.path.isdir(r):
            for p in _safe_listdir(r):
                pl = str(p).lower()
                if not pl.endswith(".db"):
                    continue
                name = os.path.basename(pl)
                if ("sheep" in name) or (name in ("db.sqlite", "sqlite.db", "app.db")):
                    if p not in seen:
                        seen.add(p)
                        db_files.append(p)

    # 再保守一點：把 roots 底下一層所有 .db 都掃進來（上限保護）
    if len(db_files) < 3:
        for r in roots:
            if os.path.isdir(r):
                for p in _safe_listdir(r):
                    if str(p).lower().endswith(".db") and p not in seen:
                        seen.add(p)
                        db_files.append(p)

    # 排除目前正在用的 db
    db_files = [p for p in db_files if os.path.abspath(p) != os.path.abspath(cur_db)]
    db_files = db_files[: max_files]

    report["scanned_files"] = len(db_files)

    # 讀取目前 cycle 已有 pools，做去重 key
    existing_keys = set()
    try:
        cur_conn = _conn()
        try:
            if _sqlite_has_table(cur_conn, "factor_pools"):
                for r in cur_conn.execute("SELECT name, symbol, timeframe_min, years, family, grid_spec_json, risk_spec_json, num_partitions, seed FROM factor_pools WHERE cycle_id = ?", (int(cycle_id),)).fetchall():
                    key = (
                        str(r["name"] or ""),
                        str(r["symbol"] or ""),
                        int(r["timeframe_min"] or 0),
                        int(r["years"] or 0),
                        str(r["family"] or ""),
                        str(r["grid_spec_json"] or ""),
                        str(r["risk_spec_json"] or ""),
                        int(r["num_partitions"] or 0),
                        int(r["seed"] or 0),
                    )
                    existing_keys.add(key)
        finally:
            cur_conn.close()
    except Exception as e:
        report["errors"].append(f"讀取既有 pools 失敗: {e}")

    # 掃描舊 db 並匯入
    imported = 0
    skipped = 0
    for p in db_files:
        rows = _read_factor_pools_from_sqlite(p)
        if not rows:
            continue

        # 只挑 active=1 的優先（若沒有就全部）
        active_rows = [r for r in rows if int(r.get("active") or 0) == 1]
        use_rows = active_rows if active_rows else rows

        report["candidates"].append({"path": p, "rows": len(rows), "use_rows": len(use_rows)})

        for r in use_rows:
            try:
                name = str(r.get("name") or "")
                symbol = str(r.get("symbol") or "")
                timeframe_min = int(r.get("timeframe_min") or 0)
                years = int(r.get("years") or 0)
                family = str(r.get("family") or "")
                grid_spec_json = str(r.get("grid_spec_json") or r.get("grid_spec") or "{}")
                risk_spec_json = str(r.get("risk_spec_json") or r.get("risk_spec") or "{}")
                num_partitions = int(r.get("num_partitions") or 0)
                seed = int(r.get("seed") or 0)
                active = int(r.get("active") or 0)

                key = (
                    name, symbol, timeframe_min, years, family,
                    grid_spec_json, risk_spec_json, num_partitions, seed
                )
                if key in existing_keys:
                    skipped += 1
                    continue

                conn = _conn()
                try:
                    conn.execute(
                        """
                        INSERT INTO factor_pools (cycle_id, name, symbol, timeframe_min, years, family, grid_spec_json, risk_spec_json, num_partitions, seed, active, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (int(cycle_id), name, symbol, timeframe_min, years, family, grid_spec_json, risk_spec_json, num_partitions, seed, 1 if active else 0, _now_iso())
                    )
                    conn.commit()
                finally:
                    conn.close()

                existing_keys.add(key)
                imported += 1
            except Exception as e:
                report["errors"].append(f"匯入失敗 {p}: {e}")

    report["imported"] = int(imported)
    report["skipped_duplicates"] = int(skipped)
    return report
def list_tasks_for_user(user_id: int, cycle_id: int = 0, limit: int = 500) -> list:
    import time, random
    last_err = None
    for attempt in range(15):
        try:
            conn = _conn()
            try:
                params: List[Any] = [int(user_id)]
                query = """
                    SELECT t.*, p.name as pool_name, p.symbol, p.timeframe_min, p.family
                    FROM mining_tasks t
                    LEFT JOIN factor_pools p ON t.pool_id = p.id
                    WHERE t.user_id = ?
                """
                if cycle_id > 0:
                    query += " AND t.cycle_id = ?"
                    params.append(int(cycle_id))
                query += " ORDER BY t.id DESC"
                if int(limit or 0) > 0:
                    query += " LIMIT ?"
                    params.append(int(limit))
                cur = conn.execute(query, params)
                return [dict(row) for row in cur.fetchall()]
            finally:
                conn.close()
        except Exception as e:
            last_err = e
            time.sleep(random.uniform(0.1, 0.5) * (1.2 ** attempt))
            
    import traceback
    import sys
    print(f"[CRITICAL DB ERROR] list_tasks_for_user 遭遇嚴重鎖死或異常，無法讀取資料: {last_err}\n{traceback.format_exc()}", file=sys.stderr)
    # [專家級防護] 絕對禁止回傳空陣列，否則 UI 會誤判任務歸零。拋出明確異常讓前端捕捉並顯示忙碌。
    raise RuntimeError(f"資料庫高併發鎖定，無法讀取任務列表，請稍後再試。 ({last_err})")

def count_tasks_for_user(user_id: int, cycle_id: int = 0, statuses: Optional[List[str]] = None) -> int:
    conn = _conn()
    try:
        query = "SELECT COUNT(*) AS c FROM mining_tasks WHERE user_id = ?"
        params: List[Any] = [int(user_id)]
        if int(cycle_id or 0) > 0:
            query += " AND cycle_id = ?"
            params.append(int(cycle_id))
        norm_statuses = [str(s).strip() for s in (statuses or []) if str(s).strip()]
        if norm_statuses:
            placeholders = ",".join("?" for _ in norm_statuses)
            query += f" AND status IN ({placeholders})"
            params.extend(norm_statuses)
        row = conn.execute(query, params).fetchone()
        if row is None:
            return 0
        if isinstance(row, dict):
            return int(row.get("c") or 0)
        try:
            return int(row["c"] or 0)
        except Exception:
            return int(row[0] or 0)
    finally:
        conn.close()


def count_review_pipeline_tasks_for_user(user_id: int, cycle_id: int) -> int:
    conn = _conn()
    try:
        pipeline_statuses = ["auto_managed", "queued", "running"]
        placeholders = ",".join("?" for _ in pipeline_statuses)
        if getattr(conn, "kind", "sqlite") == "postgres":
            query = f"""
                SELECT COUNT(*) AS c
                FROM mining_tasks t
                WHERE t.user_id = ?
                  AND t.cycle_id = ?
                  AND COALESCE(t.progress_json::jsonb->>'best_any_passed', 'false') IN ('true', '1', 'True')
                  AND (
                    CASE
                      WHEN lower(COALESCE(t.progress_json::jsonb->>'review_status', '')) = 'passed' THEN 'auto_managed'
                      WHEN lower(COALESCE(t.progress_json::jsonb->>'review_status', '')) IN ('auto_managed', 'queued', 'running', 'rejected', 'error', 'not_eligible')
                        THEN lower(COALESCE(t.progress_json::jsonb->>'review_status', ''))
                      WHEN lower(COALESCE(t.progress_json::jsonb->>'oos_status', '')) = 'passed' THEN 'auto_managed'
                      WHEN lower(COALESCE(t.progress_json::jsonb->>'oos_status', '')) IN ('auto_managed', 'queued', 'running', 'rejected', 'error', 'not_eligible')
                        THEN lower(COALESCE(t.progress_json::jsonb->>'oos_status', ''))
                      WHEN lower(COALESCE(t.status, '')) IN ('running', 'syncing') THEN 'running'
                      WHEN lower(COALESCE(t.status, '')) IN ('assigned', 'queued') THEN 'queued'
                      ELSE 'auto_managed'
                    END
                  ) IN ({placeholders})
            """
        else:
            query = f"""
                SELECT COUNT(*) AS c
                FROM mining_tasks t
                WHERE t.user_id = ?
                  AND t.cycle_id = ?
                  AND COALESCE(CAST(json_extract(t.progress_json, '$.best_any_passed') AS TEXT), 'false') IN ('1', 'true', 'True')
                  AND (
                    CASE
                      WHEN lower(COALESCE(CAST(json_extract(t.progress_json, '$.review_status') AS TEXT), '')) = 'passed' THEN 'auto_managed'
                      WHEN lower(COALESCE(CAST(json_extract(t.progress_json, '$.review_status') AS TEXT), '')) IN ('auto_managed', 'queued', 'running', 'rejected', 'error', 'not_eligible')
                        THEN lower(COALESCE(CAST(json_extract(t.progress_json, '$.review_status') AS TEXT), ''))
                      WHEN lower(COALESCE(CAST(json_extract(t.progress_json, '$.oos_status') AS TEXT), '')) = 'passed' THEN 'auto_managed'
                      WHEN lower(COALESCE(CAST(json_extract(t.progress_json, '$.oos_status') AS TEXT), '')) IN ('auto_managed', 'queued', 'running', 'rejected', 'error', 'not_eligible')
                        THEN lower(COALESCE(CAST(json_extract(t.progress_json, '$.oos_status') AS TEXT), ''))
                      WHEN lower(COALESCE(t.status, '')) IN ('running', 'syncing') THEN 'running'
                      WHEN lower(COALESCE(t.status, '')) IN ('assigned', 'queued') THEN 'queued'
                      ELSE 'auto_managed'
                    END
                  ) IN ({placeholders})
            """
        params: List[Any] = [int(user_id), int(cycle_id), *pipeline_statuses]
        row = conn.execute(query, params).fetchone()
        if row is None:
            return 0
        if isinstance(row, dict):
            return int(row.get("c") or 0)
        try:
            return int(row["c"] or 0)
        except Exception:
            return int(row[0] or 0)
    finally:
        conn.close()

def list_submissions(user_id: int = 0, status: str = "", limit: int = 300) -> list:
    import time
    for attempt in range(5):
        try:
            conn = _conn()
            try:
                query = "SELECT s.*, u.username, p.name as pool_name, p.symbol, p.timeframe_min, p.family FROM submissions s LEFT JOIN users u ON s.user_id = u.id LEFT JOIN factor_pools p ON s.pool_id = p.id WHERE 1=1"
                params = []
                if user_id > 0:
                    query += " AND s.user_id = ?"
                    params.append(user_id)
                if status:
                    query += " AND s.status = ?"
                    params.append(status)
                query += " ORDER BY s.id DESC LIMIT ?"
                params.append(limit)
                cur = conn.execute(query, params)
                return [dict(row) for row in cur.fetchall()]
            finally:
                conn.close()
        except Exception as e:
            if attempt == 4:
                print(f"[DB ERROR] list_submissions: {e}")
                return []
            time.sleep(0.1 * (2 ** attempt))
    return []

def list_task_overview(limit: int = 500) -> list:
    conn = _conn()
    try:
        cur = conn.execute("SELECT t.*, u.username, p.name as pool_name, p.symbol, p.timeframe_min, p.family FROM mining_tasks t LEFT JOIN users u ON t.user_id = u.id LEFT JOIN factor_pools p ON t.pool_id = p.id ORDER BY t.id DESC LIMIT ?", (limit,))
        return [dict(row) for row in cur.fetchall()]
    except Exception as e:
        print(f"[DB ERROR] list_task_overview: {e}")
        return []
    finally:
        conn.close()

def list_strategies(user_id: int = 0, status: str = "", limit: int = 200) -> list:
    import time
    for attempt in range(5):
        try:
            conn = _conn()
            try:
                query = "SELECT s.*, u.username, u.nickname, u.avatar_url, p.name as pool_name, p.symbol, p.timeframe_min, p.family FROM strategies s LEFT JOIN users u ON s.user_id = u.id LEFT JOIN factor_pools p ON s.pool_id = p.id WHERE 1=1"
                params = []
                if user_id > 0:
                    query += " AND s.user_id = ?"
                    params.append(user_id)
                if status:
                    query += " AND s.status = ?"
                    params.append(status)
                query += " ORDER BY s.id DESC LIMIT ?"
                params.append(limit)
                cur = conn.execute(query, params)
                return [_normalize_strategy_row(row) for row in cur.fetchall()]
            finally:
                conn.close()
        except Exception as e:
            if attempt == 4:
                print(f"[DB ERROR] list_strategies: {e}")
                return []
            time.sleep(0.1 * (2 ** attempt))
    return []

def list_review_ready_items_for_user(user_id: int, limit: int = 200) -> list:
    uid = int(user_id or 0)
    if uid <= 0:
        return []
    conn = _conn()
    try:
        rows = conn.execute(
            """
            SELECT su.id as submission_id, su.status as submission_status, su.audit_json, su.submitted_at,
                   st.id as strategy_id, st.status as strategy_status, st.note as strategy_note, st.external_key, st.created_at as strategy_created_at,
                   c.id as candidate_id, c.task_id, c.score, c.metrics_json, c.params_json, c.direction,
                   p.id as pool_id, p.name as pool_name, p.symbol, p.timeframe_min, p.family
            FROM submissions su
            LEFT JOIN strategies st ON st.submission_id = su.id AND COALESCE(st.status, '') = 'active'
            LEFT JOIN candidates c ON su.candidate_id = c.id
            LEFT JOIN factor_pools p ON su.pool_id = p.id
            WHERE su.user_id = ? AND COALESCE(su.status, '') = 'approved'
            ORDER BY COALESCE(st.created_at, su.submitted_at) DESC, su.id DESC
            LIMIT ?
            """,
            (uid, max(1, min(500, int(limit or 200)))),
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(row or {})
            metrics = parse_json_object(item.get("metrics_json"))
            params = parse_json_object(item.get("params_json"))
            audit = parse_json_object(item.get("audit_json"))
            strategy_id = int(item.get("strategy_id") or 0)
            submission_id = int(item.get("submission_id") or 0)
            task_id = int(item.get("task_id") or 0)
            score = float(item.get("score") or metrics.get("sharpe") or 0.0)
            family = str(item.get("family") or params.get("family") or "").strip()
            note = str(item.get("strategy_note") or "").strip()
            reason = str(audit.get("reason") or note or ("系統持續管理中" if strategy_id > 0 else "已達標，等待系統持續追蹤")).strip()
            out.append(
                {
                    "id": task_id if task_id > 0 else submission_id,
                    "submission_id": submission_id,
                    "strategy_id": strategy_id,
                    "candidate_id": int(item.get("candidate_id") or 0),
                    "task_id": task_id,
                    "pool_id": int(item.get("pool_id") or 0),
                    "pool_name": str(item.get("pool_name") or family or "-"),
                    "family": family,
                    "symbol": str(item.get("symbol") or "").strip().upper(),
                    "timeframe_min": int(item.get("timeframe_min") or 0),
                    "interval": str(item.get("timeframe_min") or ""),
                    "direction": normalize_direction(item.get("direction"), default="long"),
                    "status": "completed",
                    "review_status": "auto_managed",
                    "review_reason": reason,
                    "best_any_passed": True,
                    "best_any_score": score,
                    "score": score,
                    "external_key": str(item.get("external_key") or "").strip(),
                    "submitted_at": str(item.get("submitted_at") or ""),
                    "created_at": str(item.get("strategy_created_at") or item.get("submitted_at") or ""),
                }
            )
        out.sort(
            key=lambda entry: (
                -float(entry.get("best_any_score") or 0.0),
                -int(entry.get("strategy_id") or 0),
                -int(entry.get("submission_id") or 0),
            )
        )
        return out
    finally:
        conn.close()


def count_strategies(user_id: int = 0, status: str = "") -> int:
    conn = _conn()
    try:
        query = "SELECT COUNT(*) AS c FROM strategies WHERE 1=1"
        params: List[Any] = []
        if int(user_id or 0) > 0:
            query += " AND user_id = ?"
            params.append(int(user_id))
        if str(status or "").strip():
            query += " AND status = ?"
            params.append(str(status))
        row = conn.execute(query, params).fetchone()
        if row is None:
            return 0
        if isinstance(row, dict):
            return int(row.get("c") or 0)
        try:
            return int(row["c"] or 0)
        except Exception:
            return int(row[0] or 0)
    finally:
        conn.close()


def count_submissions(user_id: int = 0, status: str = "") -> int:
    conn = _conn()
    try:
        query = "SELECT COUNT(*) AS c FROM submissions WHERE 1=1"
        params: List[Any] = []
        if int(user_id or 0) > 0:
            query += " AND user_id = ?"
            params.append(int(user_id))
        if str(status or "").strip():
            query += " AND status = ?"
            params.append(str(status))
        row = conn.execute(query, params).fetchone()
        if row is None:
            return 0
        try:
            return int((dict(row) if not isinstance(row, dict) else row).get("c") or 0)
        except Exception:
            try:
                return int(row["c"] or 0)
            except Exception:
                return int(row[0] or 0)
    finally:
        conn.close()


def count_review_ready_tasks(user_id: int = 0) -> int:
    cache_key = f"user:{int(user_id or 0)}"
    now_ts = time.time()
    cached_values = dict(_REVIEW_READY_CACHE.get("values") or {})
    retry_after = dict(_REVIEW_READY_CACHE.get("retry_after") or {})
    db_kind = _db_kind()

    if db_kind == "postgres":
        if now_ts < float(retry_after.get(cache_key) or 0.0) and cache_key in cached_values:
            return int(cached_values.get(cache_key) or 0)
        try:
            total = int(count_submissions(user_id=int(user_id or 0), status="approved"))
            _REVIEW_READY_CACHE["ts"] = now_ts
            _REVIEW_READY_CACHE.setdefault("values", {})[cache_key] = int(total)
            _REVIEW_READY_CACHE.setdefault("retry_after", {}).pop(cache_key, None)
            return int(total)
        except Exception:
            if cache_key in cached_values:
                return int(cached_values.get(cache_key) or 0)
            _REVIEW_READY_CACHE.setdefault("retry_after", {})[cache_key] = now_ts + 30.0
            return 0

    conn = _conn()
    try:
        query = "SELECT COUNT(*) AS c FROM mining_tasks WHERE status = 'completed'"
        params: List[Any] = []
        if int(user_id or 0) > 0:
            query += " AND user_id = ?"
            params.append(int(user_id))
        query += " AND LOWER(COALESCE(json_extract(progress_json, '$.review_status'), json_extract(progress_json, '$.oos_status'), '')) IN ('auto_managed', 'passed')"
        row = conn.execute(query, params).fetchone()
        if row is None:
            total = 0
        else:
            try:
                total = int((dict(row) if not isinstance(row, dict) else row).get("c") or 0)
            except Exception:
                try:
                    total = int(row["c"] or 0)
                except Exception:
                    total = int(row[0] or 0)
        _REVIEW_READY_CACHE["ts"] = now_ts
        _REVIEW_READY_CACHE.setdefault("values", {})[cache_key] = int(total)
        _REVIEW_READY_CACHE.setdefault("retry_after", {}).pop(cache_key, None)
        return int(total)
    except Exception:
        try:
            if cache_key in cached_values:
                return int(cached_values.get(cache_key) or 0)
        except Exception:
            pass
        from sheep_review import normalize_review_fields

        try:
            query = "SELECT progress_json, status FROM mining_tasks WHERE status = 'completed'"
            params = []
            if int(user_id or 0) > 0:
                query += " AND user_id = ?"
                params.append(int(user_id))
            rows = conn.execute(query, params).fetchall()
            total = 0
            for row in rows:
                entry = dict(row or {})
                review = normalize_review_fields(entry.get("progress_json"), str(entry.get("status") or ""))
                if str(review.get("review_status") or "").strip().lower() == "auto_managed":
                    total += 1
            return int(total)
        except Exception:
            return 0
    finally:
        conn.close()

def list_payouts(user_id: int = 0, status: str = "", limit: int = 200) -> list:
    conn = _conn()
    try:
        query = "SELECT p.*, u.username FROM payouts p LEFT JOIN users u ON p.user_id = u.id WHERE 1=1"
        params = []
        if user_id > 0:
            query += " AND p.user_id = ?"
            params.append(user_id)
        if status:
            query += " AND p.status = ?"
            params.append(status)
        query += " ORDER BY p.id DESC LIMIT ?"
        params.append(limit)
        cur = conn.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception as e:
        print(f"[DB ERROR] list_payouts: {e}")
        return []
    finally:
        conn.close()

def list_candidates(task_id: int, limit: int = 50) -> list:
    conn = _conn()
    try:
        cur = conn.execute("SELECT * FROM candidates WHERE task_id = ? ORDER BY score DESC LIMIT ?", (task_id, limit))
        return [_normalize_candidate_row(row) for row in cur.fetchall()]
    except Exception as e:
        print(f"[DB ERROR] list_candidates: {e}")
        return []
    finally:
        conn.close()

def get_pool(pool_id: int) -> Optional[dict]:
    conn = _conn()
    try:
        row = conn.execute("SELECT * FROM factor_pools WHERE id = ?", (pool_id,)).fetchone()
        if not row: return None
        return _normalize_pool_row(row)
    except Exception as e:
        print(f"[DB ERROR] get_pool: {e}")
        return None
    finally:
        conn.close()

def get_db_info() -> dict:
    return {"kind": "sqlite3"}

def release_assigned_tasks_for_user_not_in_pools(user_id: int, cycle_id: int, allowed_pool_ids: list) -> None:
    conn = _conn()
    try:
        # [專家級修復] 直接 DELETE 尚未執行的任務，將配額瞬間釋放給全網排他機制
        if not allowed_pool_ids:
            conn.execute("DELETE FROM mining_tasks WHERE user_id = ? AND cycle_id = ? AND status = 'assigned'", (user_id, cycle_id))
        else:
            placeholders = ",".join("?" for _ in allowed_pool_ids)
            query = f"DELETE FROM mining_tasks WHERE user_id = ? AND cycle_id = ? AND status = 'assigned' AND pool_id NOT IN ({placeholders})"
            params = [user_id, cycle_id] + allowed_pool_ids
            conn.execute(query, params)
        conn.commit()
    except Exception as e:
        import sys
        print(f"[DB ERROR] release_assigned_tasks_for_user: {e}", file=sys.stderr)
    finally:
        conn.close()

def delete_tasks_for_pool(cycle_id: int, pool_id: int) -> int:
    conn = _conn()
    try:
        cur = conn.execute("DELETE FROM mining_tasks WHERE cycle_id = ? AND pool_id = ?", (cycle_id, pool_id))
        conn.commit()
        return cur.rowcount
    except Exception:
        return 0
    finally:
        conn.close()
        
def get_global_progress_snapshot(cycle_id: int) -> dict:
    conn = _conn()
    t0 = time.time()
    try:
        pools = list_factor_pools(int(cycle_id))
        pools_by_id: Dict[int, Dict[str, Any]] = {}
        for p in pools:
            try:
                pid = int(p.get("id") or 0)
            except Exception:
                pid = 0
            p["tasks"] = []
            pools_by_id[pid] = p

        # 快路徑：用 window function 在 DB 端直接「每個 (pool_id, partition_idx) 只取最佳那筆」
        # 這會把原本 Python 逐筆掃描 + 去重，變成 DB 一次做完，效能差距是量級級別
        try:
            cur = conn.execute(
                """
                WITH ranked AS (
                    SELECT
                        t.id,
                        t.user_id,
                        t.pool_id,
                        t.cycle_id,
                        t.partition_idx,
                        t.num_partitions,
                        t.status,
                        t.progress_json,
                        t.last_heartbeat,
                        t.created_at,
                        t.updated_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY t.pool_id, t.partition_idx
                            ORDER BY
                                CASE t.status
                                    WHEN 'completed' THEN 3
                                    WHEN 'running' THEN 2
                                    WHEN 'assigned' THEN 1
                                    ELSE 0
                                END DESC,
                                COALESCE(t.updated_at, t.created_at) DESC,
                                t.id DESC
                        ) AS rn
                    FROM mining_tasks t
                    WHERE t.cycle_id = ?
                )
                SELECT r.*, u.username AS username
                FROM ranked r
                LEFT JOIN users u ON u.id = r.user_id
                WHERE r.rn = 1
                ORDER BY r.pool_id ASC, r.partition_idx ASC
                """,
                (int(cycle_id),),
            )

            for row in cur.fetchall():
                t = dict(row)
                try:
                    t["progress"] = json.loads(t.get("progress_json") or "{}")
                except Exception:
                    t["progress"] = {}

                try:
                    pid = int(t.get("pool_id") or 0)
                except Exception:
                    pid = 0

                p = pools_by_id.get(pid)
                if p is not None:
                    p["tasks"].append(t)

        except Exception as fast_e:
            # 安全 fallback：如果 SQLite 版本/環境不支援 window function，就退回舊邏輯
            # 但仍然縮小欄位，避免 SELECT t.* 造成不必要的資料搬運
            def _status_rank(s: str) -> int:
                if s == "completed":
                    return 3
                if s == "running":
                    return 2
                if s == "assigned":
                    return 1
                return 0

            def _pick_better(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
                sa = str(a.get("status") or "")
                sb = str(b.get("status") or "")
                ra = _status_rank(sa)
                rb = _status_rank(sb)
                if rb != ra:
                    return b if rb > ra else a
                ta = str(a.get("updated_at") or a.get("created_at") or "")
                tb = str(b.get("updated_at") or b.get("created_at") or "")
                if tb != ta:
                    return b if tb > ta else a
                ia = int(a.get("id") or 0)
                ib = int(b.get("id") or 0)
                return b if ib > ia else a

            for p in pools:
                pool_id = int(p.get("id") or 0)
                cur2 = conn.execute(
                    """
                    SELECT
                        t.id, t.user_id, t.pool_id, t.cycle_id,
                        t.partition_idx, t.num_partitions,
                        t.status, t.progress_json,
                        t.last_heartbeat, t.created_at, t.updated_at,
                        u.username AS username
                    FROM mining_tasks t
                    LEFT JOIN users u ON u.id = t.user_id
                    WHERE t.pool_id = ? AND t.cycle_id = ?
                    """,
                    (pool_id, int(cycle_id)),
                )

                best: Dict[int, Dict[str, Any]] = {}
                for row in cur2.fetchall():
                    t = dict(row)
                    try:
                        t["progress"] = json.loads(t.get("progress_json") or "{}")
                    except Exception:
                        t["progress"] = {}

                    try:
                        idx = int(t.get("partition_idx") or 0)
                    except Exception:
                        idx = 0

                    if idx not in best:
                        best[idx] = t
                    else:
                        best[idx] = _pick_better(best[idx], t)

                p["tasks"] = [best[k] for k in sorted(best.keys())]

            print(f"[DB WARN] get_global_progress_snapshot fallback used: {fast_e}")

        dt = time.time() - t0
        if dt > 0.8:
            print(f"[DB SLOW] get_global_progress_snapshot cycle_id={cycle_id} took {dt:.3f}s pools={len(pools)}")

        return {"system_user_id": 0, "pools": pools}

    except Exception as e:
        import traceback
        print(f"[DB ERROR] get_global_progress_snapshot: {e}\n{traceback.format_exc()}")
        return {"system_user_id": 0, "pools": []}
    finally:
        conn.close()
def get_global_paid_payout_sum_usdt(cycle_id: int) -> float:
    conn = _conn()
    try:
        # 優先以 cycle_id 精準計算（payouts -> strategies -> factor_pools）
        try:
            cur = conn.execute(
                """
                SELECT SUM(p.amount_usdt) as s
                FROM payouts p
                JOIN strategies s ON s.id = p.strategy_id
                JOIN factor_pools fp ON fp.id = s.pool_id
                WHERE p.status = 'paid' AND fp.cycle_id = ?
                """,
                (int(cycle_id),),
            )
            row = cur.fetchone()
            return float(row["s"] or 0.0) if row else 0.0
        except Exception:
            # 後備：若舊資料結構無法 join，退回全站累計（至少不炸）
            cur = conn.execute("SELECT SUM(amount_usdt) as s FROM payouts WHERE status = 'paid'")
            row = cur.fetchone()
            return float(row["s"] or 0.0) if row else 0.0
    except Exception:
        return 0.0
    finally:
        conn.close()

def create_factor_pool(
    cycle_id: int,
    name: str,
    symbol: str,
    timeframe_min: int,
    years: int,
    family: str,
    grid_spec: dict,
    risk_spec: dict,
    num_partitions: int,
    seed: int,
    active: bool,
    auto_expand: bool = False,
    direction: str = "long",
    external_key: str = "",
) -> list:
    """專家級 Pool 建立器：支援 14 種組合自動擴展功能"""
    ids = []
    targets = [(symbol, timeframe_min)]
    if auto_expand:
        # 管理員勾選最大化範圍：自動生成 BTC/ETH 與 7 種 Timeframe
        symbols = ["BTC_USDT", "ETH_USDT"]
        tfs = [1, 5, 15, 30, 60, 240, 1440]
        targets = [(s, t) for s in symbols for t in tfs]

    normalized_direction = normalize_direction(direction, reverse=parse_json_object(risk_spec).get("reverse_mode"), default="long")
    normalized_risk_spec = _normalize_risk_spec(normalized_direction, risk_spec)
    conn = _conn()
    try:
        for s, t in targets:
            # 修正：精準檢查是否已存在於該週期，避免重複建立導致任務派發混亂
            exist = conn.execute("SELECT id FROM factor_pools WHERE cycle_id=? AND symbol=? AND timeframe_min=? AND family=?", (cycle_id, s, t, family)).fetchone()
            if exist:
                ids.append(exist["id"])
                continue

            expanded_name = f"{name} [{s}_{t}m]" if auto_expand else name
            if getattr(conn, "kind", "sqlite") == "postgres":
                row_pool = conn.execute(
                    """
                    INSERT INTO factor_pools (cycle_id, name, external_key, symbol, direction, timeframe_min, years, family, grid_spec_json, risk_spec_json, num_partitions, seed, active, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id
                    """,
                    (
                        cycle_id,
                        expanded_name,
                        str(external_key or ""),
                        s,
                        normalized_direction,
                        t,
                        int(years),
                        family,
                        json.dumps(grid_spec, ensure_ascii=False),
                        json.dumps(normalized_risk_spec, ensure_ascii=False),
                        int(num_partitions),
                        int(seed),
                        1 if active else 0,
                        _now_iso(),
                    )
                ).fetchone()
                ids.append(int((row_pool or {}).get("id") or 0))
            else:
                cur = conn.execute(
                    """
                    INSERT INTO factor_pools (cycle_id, name, external_key, symbol, direction, timeframe_min, years, family, grid_spec_json, risk_spec_json, num_partitions, seed, active, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        cycle_id,
                        expanded_name,
                        str(external_key or ""),
                        s,
                        normalized_direction,
                        t,
                        int(years),
                        family,
                        json.dumps(grid_spec, ensure_ascii=False),
                        json.dumps(normalized_risk_spec, ensure_ascii=False),
                        int(num_partitions),
                        int(seed),
                        1 if active else 0,
                        _now_iso(),
                    )
                )
                ids.append(int(cur.lastrowid))
        conn.commit()
        return ids
    except Exception as e:
        print(f"[DB ERROR] create_factor_pool fatal: {e}")
        raise
    finally:
        conn.close()

def save_candidate_to_disk(task_id: int, user_id: int, pool_id: int, data: dict):
    """將跑過的組合數據存入檔案系統而非資料庫，提升管理效率與安全性"""
    base_dir = os.path.join(os.getcwd(), "data", "storage", f"pool_{pool_id}", f"task_{task_id}")
    os.makedirs(base_dir, exist_ok=True)
    file_path = os.path.join(base_dir, f"user_{user_id}_{int(time.time()*1000)}.json")
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return file_path
    except Exception as e:
        print(f"[DISK STORAGE ERROR] {e}")
        return None

def update_factor_pool(
    pool_id: int,
    name: str,
    symbol: str,
    timeframe_min: int,
    years: int,
    family: str,
    grid_spec: dict,
    risk_spec: dict,
    num_partitions: int,
    seed: int,
    active: bool,
    direction: str = "long",
    external_key: str = "",
) -> None:
    conn = _conn()
    try:
        normalized_direction = normalize_direction(direction, reverse=parse_json_object(risk_spec).get("reverse_mode"), default="long")
        normalized_risk_spec = _normalize_risk_spec(normalized_direction, risk_spec)
        conn.execute(
            """
            UPDATE factor_pools
            SET name=?, external_key=?, symbol=?, direction=?, timeframe_min=?, years=?, family=?, grid_spec_json=?, risk_spec_json=?, num_partitions=?, seed=?, active=?
            WHERE id=?
            """,
            (
                name,
                str(external_key or ""),
                symbol,
                normalized_direction,
                timeframe_min,
                years,
                family,
                json.dumps(grid_spec, ensure_ascii=False),
                json.dumps(normalized_risk_spec, ensure_ascii=False),
                num_partitions,
                seed,
                1 if active else 0,
                pool_id,
            )
        )
        conn.commit()
    finally:
        conn.close()


def _interval_to_minutes(interval: Any) -> int:
    text = str(interval or "").strip().lower()
    if not text:
        return 0
    if text.endswith("m"):
        try:
            return int(float(text[:-1] or 0))
        except Exception:
            return 0
    if text.endswith("h"):
        try:
            return int(float(text[:-1] or 0) * 60)
        except Exception:
            return 0
    if text.endswith("d"):
        try:
            return int(float(text[:-1] or 0) * 1440)
        except Exception:
            return 0
    try:
        return int(float(text))
    except Exception:
        return 0


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def save_runtime_portfolio_snapshot(
    *,
    scope: str,
    user_id: int = 0,
    published_by: int = 0,
    source: str = "holy_grail_runtime",
    items: List[Dict[str, Any]],
    summary: Optional[Dict[str, Any]] = None,
    updated_at: str = "",
    checksum: str = "",
) -> Dict[str, Any]:
    scope_text = "global" if str(scope or "").strip().lower() == "global" else "personal"
    owner_user_id = int(user_id or 0) if scope_text == "personal" else 0
    published_by_id = int(published_by or 0)
    ts = str(updated_at or "").strip() or _now_iso()
    normalized_items: List[Dict[str, Any]] = []
    for idx, raw_item in enumerate(list(items or []), start=1):
        entry = normalize_runtime_strategy_entry(
            dict(raw_item or {}),
            default_symbol=str((raw_item or {}).get("symbol") or ""),
            default_interval=str((raw_item or {}).get("interval") or ""),
        )
        try:
            strategy_id = int(raw_item.get("strategy_id") or entry.get("strategy_id") or 0)
        except Exception:
            strategy_id = 0
        params = dict(entry.get("family_params") or {})
        params["direction"] = entry["direction"]
        corr_stats = {
            "avg_pairwise_corr_to_selected": raw_item.get("avg_pairwise_corr_to_selected"),
            "max_pairwise_corr_to_selected": raw_item.get("max_pairwise_corr_to_selected"),
            "duplicate_group_id": raw_item.get("duplicate_group_id"),
        }
        normalized_items.append(
            {
                "rank": int(raw_item.get("rank") or raw_item.get("selected_rank") or idx),
                "strategy_key": str(raw_item.get("strategy_key") or raw_item.get("name") or f"{entry.get('family')}_{idx}"),
                "strategy_id": strategy_id,
                "family": str(entry.get("family") or "").strip(),
                "symbol": str(entry.get("symbol") or "").strip().upper(),
                "direction": str(entry.get("direction") or "long"),
                "interval": str(entry.get("interval") or "").strip(),
                "stake_pct": float(raw_item.get("stake_pct") or entry.get("stake_pct") or 0.0),
                "sharpe": float(raw_item.get("sharpe") or 0.0),
                "total_return_pct": float(raw_item.get("total_return_pct") or 0.0),
                "max_drawdown_pct": float(raw_item.get("max_drawdown_pct") or 0.0),
                "avg_corr": float(raw_item.get("avg_pairwise_corr_to_selected") or 0.0),
                "max_corr": float(raw_item.get("max_pairwise_corr_to_selected") or 0.0),
                "params_json": params,
                "corr_stats_json": corr_stats,
            }
        )
    summary_payload = dict(summary or {})
    summary_payload["scope"] = scope_text
    summary_payload["strategy_count"] = len(normalized_items)
    checksum_value = str(checksum or "").strip() or _stable_json_checksum(
        {
            "scope": scope_text,
            "user_id": owner_user_id,
            "source": source,
            "updated_at": ts,
            "summary": summary_payload,
            "items": normalized_items,
        }
    )

    conn = _conn()
    try:
        if getattr(conn, "kind", "sqlite") == "postgres":
            row = conn.execute(
                """
                INSERT INTO runtime_portfolio_snapshots (scope, user_id, published_by, updated_at, source, strategy_count, summary_json, checksum)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
                """,
                (
                    scope_text,
                    owner_user_id if owner_user_id > 0 else None,
                    published_by_id if published_by_id > 0 else None,
                    ts,
                    str(source or "holy_grail_runtime"),
                    len(normalized_items),
                    json.dumps(summary_payload, ensure_ascii=False),
                    checksum_value,
                ),
            ).fetchone()
            snapshot_id = int((row or {}).get("id") or 0)
        else:
            cur = conn.execute(
                """
                INSERT INTO runtime_portfolio_snapshots (scope, user_id, published_by, updated_at, source, strategy_count, summary_json, checksum)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    scope_text,
                    owner_user_id if owner_user_id > 0 else None,
                    published_by_id if published_by_id > 0 else None,
                    ts,
                    str(source or "holy_grail_runtime"),
                    len(normalized_items),
                    json.dumps(summary_payload, ensure_ascii=False),
                    checksum_value,
                ),
            )
            snapshot_id = int(cur.lastrowid)

        for item in normalized_items:
            conn.execute(
                """
                INSERT INTO runtime_portfolio_items (snapshot_id, rank, strategy_key, strategy_id, family, symbol, direction, interval, stake_pct, sharpe, total_return_pct, max_drawdown_pct, avg_corr, max_corr, params_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    int(item["rank"]),
                    item["strategy_key"],
                    int(item.get("strategy_id") or 0),
                    item["family"],
                    item["symbol"],
                    item["direction"],
                    item["interval"],
                    float(item["stake_pct"]),
                    float(item["sharpe"]),
                    float(item.get("total_return_pct") or 0.0),
                    float(item.get("max_drawdown_pct") or 0.0),
                    float(item["avg_corr"]),
                    float(item["max_corr"]),
                    json.dumps(item["params_json"], ensure_ascii=False),
                ),
            )
        conn.commit()
        return {
            "id": snapshot_id,
            "scope": scope_text,
            "user_id": owner_user_id,
            "published_by": published_by_id,
            "updated_at": ts,
            "source": str(source or "holy_grail_runtime"),
            "strategy_count": len(normalized_items),
            "checksum": checksum_value,
            "summary": summary_payload,
            "items": normalized_items,
        }
    finally:
        conn.close()


def get_runtime_portfolio_snapshot(scope: str, user_id: int = 0) -> Optional[dict]:
    scope_text = "global" if str(scope or "").strip().lower() == "global" else "personal"
    owner_user_id = int(user_id or 0) if scope_text == "personal" else 0
    conn = _conn()
    try:
        if scope_text == "global":
            row = conn.execute(
                "SELECT * FROM runtime_portfolio_snapshots WHERE scope = 'global' ORDER BY updated_at DESC, id DESC LIMIT 1"
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM runtime_portfolio_snapshots WHERE scope = 'personal' AND user_id = ? ORDER BY updated_at DESC, id DESC LIMIT 1",
                (owner_user_id,),
            ).fetchone()
        if not row:
            return None
        snapshot = _runtime_snapshot_row(row)
        items = conn.execute(
            "SELECT * FROM runtime_portfolio_items WHERE snapshot_id = ? ORDER BY rank ASC, id ASC",
            (int(snapshot["id"]),),
        ).fetchall()
        snapshot["items"] = [_runtime_item_row(item) for item in items]
        return snapshot
    finally:
        conn.close()


def import_admin_catalog(
    *,
    cycle_id: int,
    owner_user_id: int,
    payload: Dict[str, Any],
    dry_run: bool = True,
) -> Dict[str, Any]:
    raw = dict(payload or {})
    schema_version = int(raw.get("schema_version") or 1)
    factor_pools = list(raw.get("factor_pools") or [])
    strategies = list(raw.get("strategies") or [])
    report: Dict[str, Any] = {
        "ok": True,
        "dry_run": bool(dry_run),
        "schema_version": schema_version,
        "factor_pools": {"create": 0, "update": 0, "skip": 0, "errors": []},
        "strategies": {"create": 0, "update": 0, "skip": 0, "errors": []},
    }

    if not factor_pools and not strategies:
        report["ok"] = False
        report["error"] = "catalog_payload_empty"
        return report

    cycle_id = int(cycle_id or 0)
    owner_user_id = int(owner_user_id or 0)
    conn = _conn()
    try:
        pending_pool_lookup: Dict[Tuple[str, str, int, str], int] = {}

        for idx, raw_pool in enumerate(factor_pools, start=1):
            item = dict(raw_pool or {})
            key = str(item.get("key") or "").strip()
            name = str(item.get("name") or key).strip()
            symbol = str(item.get("symbol") or "").strip().upper()
            family = str(item.get("family") or "").strip()
            direction = normalize_direction(item.get("direction"), default="")
            timeframe_min = int(item.get("timeframe_min") or 0)
            if not key or not name or not symbol or not family or direction not in {"long", "short"} or timeframe_min <= 0:
                report["factor_pools"]["errors"].append({"index": idx, "key": key, "error": "missing_required_fields"})
                continue
            grid_spec = parse_json_object(item.get("grid_spec"))
            risk_spec = _normalize_risk_spec(direction, item.get("risk_spec"))
            years = int(item.get("years") or 3)
            num_partitions = int(item.get("num_partitions") or 1)
            seed = int(item.get("seed") or 42)
            active = _as_bool(item.get("active"), True)
            auto_expand = _as_bool(item.get("auto_expand"), False)
            rows = conn.execute(
                "SELECT id FROM factor_pools WHERE cycle_id = ? AND external_key = ? ORDER BY id ASC",
                (cycle_id, key),
            ).fetchall()
            if rows:
                if not dry_run:
                    for row in rows:
                        update_factor_pool(
                            pool_id=int(row["id"]),
                            name=name,
                            symbol=symbol,
                            timeframe_min=timeframe_min,
                            years=years,
                            family=family,
                            grid_spec=grid_spec,
                            risk_spec=risk_spec,
                            num_partitions=num_partitions,
                            seed=seed,
                            active=active,
                            direction=direction,
                            external_key=key,
                        )
                report["factor_pools"]["update"] += len(rows)
                pending_pool_lookup[(family, symbol, timeframe_min, direction)] = int(rows[0]["id"])
            else:
                if not dry_run:
                    ids = create_factor_pool(
                        cycle_id=cycle_id,
                        name=name,
                        symbol=symbol,
                        timeframe_min=timeframe_min,
                        years=years,
                        family=family,
                        grid_spec=grid_spec,
                        risk_spec=risk_spec,
                        num_partitions=num_partitions,
                        seed=seed,
                        active=active,
                        auto_expand=auto_expand,
                        direction=direction,
                        external_key=key,
                    )
                    if ids:
                        pending_pool_lookup[(family, symbol, timeframe_min, direction)] = int(ids[0])
                else:
                    pending_pool_lookup[(family, symbol, timeframe_min, direction)] = -1
                report["factor_pools"]["create"] += 1

        if not dry_run:
            conn.close()
            conn = _conn()

        for idx, raw_strategy in enumerate(strategies, start=1):
            item = dict(raw_strategy or {})
            key = str(item.get("key") or "").strip()
            name = str(item.get("name") or key).strip()
            family = str(item.get("family") or "").strip()
            symbol = str(item.get("symbol") or "").strip().upper()
            direction = normalize_direction(item.get("direction"), default="")
            interval = str(item.get("interval") or "").strip()
            timeframe_min = _interval_to_minutes(interval)
            if not key or not name or not family or not symbol or not interval or timeframe_min <= 0 or direction not in {"long", "short"}:
                report["strategies"]["errors"].append({"index": idx, "key": key, "error": "missing_required_fields"})
                continue
            params_payload = _normalize_strategy_params_payload(
                {
                    "family": family,
                    "family_params": parse_json_object(item.get("family_params")),
                    "tp": float(item.get("tp_pct") or 0.0) / 100.0,
                    "sl": float(item.get("sl_pct") or 0.0) / 100.0,
                    "max_hold": int(item.get("max_hold_bars") or 0),
                    "symbol": symbol,
                    "interval": interval,
                    "direction": direction,
                },
                direction=direction,
                family=family,
                symbol=symbol,
                interval=interval,
            )
            params_payload["_catalog_name"] = name
            params_payload["_catalog_enabled"] = _as_bool(item.get("enabled"), True)
            params_payload["stake_pct"] = float(item.get("stake_pct") or 0.0)
            strategy_status = str(item.get("status") or "active").strip().lower() or "active"
            if not params_payload["_catalog_enabled"] and strategy_status == "active":
                strategy_status = "disabled"

            pool_row = conn.execute(
                """
                SELECT id FROM factor_pools
                WHERE cycle_id = ? AND family = ? AND symbol = ? AND timeframe_min = ? AND COALESCE(direction, 'long') = ?
                ORDER BY id DESC LIMIT 1
                """,
                (cycle_id, family, symbol, timeframe_min, direction),
            ).fetchone()
            pool_row = dict(pool_row) if pool_row else None
            pool_id = int((pool_row or {}).get("id") or 0) if pool_row else int(pending_pool_lookup.get((family, symbol, timeframe_min, direction)) or 0)
            if pool_id <= 0 and not (dry_run and pool_id == -1):
                report["strategies"]["errors"].append({"index": idx, "key": key, "error": "matching_factor_pool_not_found"})
                continue

            existing = conn.execute(
                "SELECT id FROM strategies WHERE external_key = ? ORDER BY id DESC LIMIT 1",
                (key,),
            ).fetchone()
            existing = dict(existing) if existing else None
            if existing:
                if not dry_run:
                    conn.execute(
                        """
                        UPDATE strategies
                        SET user_id=?, pool_id=?, direction=?, params_json=?, status=?, allocation_pct=?, note=?, expires_at=?
                        WHERE id=?
                        """,
                        (
                            owner_user_id,
                            pool_id,
                            direction,
                            json.dumps(params_payload, ensure_ascii=False),
                            strategy_status,
                            float(item.get("stake_pct") or 0.0) / 100.0 if float(item.get("stake_pct") or 0.0) > 1 else float(item.get("stake_pct") or 0.0),
                            f"Catalog Import: {name}",
                            "2099-12-31T23:59:59Z",
                            int(existing["id"]),
                        ),
                    )
                report["strategies"]["update"] += 1
            else:
                if not dry_run:
                    conn.execute(
                        """
                        INSERT INTO strategies (submission_id, user_id, pool_id, external_key, direction, params_json, status, allocation_pct, note, created_at, expires_at)
                        VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            owner_user_id,
                            pool_id,
                            key,
                            direction,
                            json.dumps(params_payload, ensure_ascii=False),
                            strategy_status,
                            float(item.get("stake_pct") or 0.0) / 100.0 if float(item.get("stake_pct") or 0.0) > 1 else float(item.get("stake_pct") or 0.0),
                            f"Catalog Import: {name}",
                            _now_iso(),
                            "2099-12-31T23:59:59Z",
                        ),
                    )
                report["strategies"]["create"] += 1

        if dry_run:
            conn.rollback()
        else:
            conn.commit()
        report["ok"] = not bool(report["factor_pools"]["errors"] or report["strategies"]["errors"])
        return report
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        report["ok"] = False
        report["error"] = str(e)
        return report
    finally:
        conn.close()


def get_submission(sub_id: int) -> Optional[dict]:
    conn = _conn()
    try:
        row = conn.execute("SELECT s.*, c.params_json FROM submissions s LEFT JOIN candidates c ON s.candidate_id = c.id WHERE s.id = ?", (sub_id,)).fetchone()
        if not row: return None
        d = dict(row)
        d["audit"] = json.loads(d.get("audit_json") or "{}")
        d["params_json"] = json.loads(d.get("params_json") or "{}")
        return d
    finally:
        conn.close()

def set_submission_status(sub_id: int, status: str, approved_by: int = 0) -> None:
    conn = _conn()
    try:
        conn.execute("UPDATE submissions SET status = ? WHERE id = ?", (status, sub_id))
        conn.commit()
    finally:
        conn.close()

def create_strategy_from_submission(sub_id: int, allocation_pct: float, note: str) -> int:
    conn = _conn()
    try:
        sub = conn.execute("SELECT * FROM submissions WHERE id = ?", (sub_id,)).fetchone()
        if not sub: return 0
        cand = conn.execute("SELECT params_json, direction FROM candidates WHERE id = ?", (sub["candidate_id"],)).fetchone()
        params = cand["params_json"] if cand else "{}"
        direction = _infer_direction(direction=(cand or {}).get("direction"), params_json=params)
        normalized_params = _normalize_strategy_params_payload(params, direction=direction)
        
        if getattr(conn, "kind", "sqlite") == "postgres":
            row = conn.execute(
                "INSERT INTO strategies (submission_id, user_id, pool_id, direction, params_json, status, allocation_pct, note, created_at, expires_at, external_key) VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?) RETURNING id",
                (
                    sub_id,
                    sub["user_id"],
                    sub["pool_id"],
                    direction,
                    json.dumps(normalized_params, ensure_ascii=False),
                    allocation_pct,
                    note,
                    _now_iso(),
                    "2099-12-31T23:59:59Z",
                    "",
                )
            ).fetchone()
            new_id = int((row or {}).get("id") or 0)
        else:
            cur = conn.execute(
                "INSERT INTO strategies (submission_id, user_id, pool_id, direction, params_json, status, allocation_pct, note, created_at, expires_at, external_key) VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?)",
                (
                    sub_id,
                    sub["user_id"],
                    sub["pool_id"],
                    direction,
                    json.dumps(normalized_params, ensure_ascii=False),
                    allocation_pct,
                    note,
                    _now_iso(),
                    "2099-12-31T23:59:59Z",
                    "",
                )
            )
            new_id = int(cur.lastrowid)
            
        conn.commit()
        return new_id
    finally:
        conn.close()

def set_strategy_status(strategy_id: int, status: str) -> None:
    conn = _conn()
    try:
        conn.execute("UPDATE strategies SET status = ? WHERE id = ?", (status, strategy_id))
        conn.commit()
    finally:
        conn.close()

def get_strategy_with_params(strategy_id: int) -> Optional[dict]:
    conn = _conn()
    try:
        row = conn.execute("SELECT * FROM strategies WHERE id = ?", (strategy_id,)).fetchone()
        if not row: return None
        return _normalize_strategy_row(row)
    finally:
        conn.close()

def payout_exists(strategy_id: int, week_start_ts: str) -> bool:
    conn = _conn()
    try:
        row = conn.execute("SELECT 1 FROM payouts WHERE strategy_id = ? AND week_start_ts = ?", (strategy_id, week_start_ts)).fetchone()
        return bool(row)
    finally:
        conn.close()

def create_payout(strategy_id: int, user_id: int, week_start_ts: str, amount_usdt: float) -> int:
    conn = _conn()
    try:
        cur = conn.execute("INSERT INTO payouts (strategy_id, user_id, week_start_ts, amount_usdt, status, created_at) VALUES (?, ?, ?, ?, 'unpaid', ?)",
                           (strategy_id, user_id, week_start_ts, amount_usdt, _now_iso()))
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()

def create_weekly_check(strategy_id: int, week_start_ts: str, week_end_ts: str, return_pct: float, max_drawdown_pct: float, trades: int, eligible: bool) -> None:
    conn = _conn()
    try:
        conn.execute("INSERT INTO weekly_checks (strategy_id, week_start_ts, week_end_ts, return_pct, max_drawdown_pct, trades, eligible, checked_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                     (strategy_id, week_start_ts, week_end_ts, return_pct, max_drawdown_pct, trades, 1 if eligible else 0, _now_iso()))
        conn.commit()
    finally:
        conn.close()

def set_payout_paid(payout_id: int, txid: str) -> None:
    conn = _conn()
    try:
        conn.execute("UPDATE payouts SET status = 'paid', txid = ? WHERE id = ?", (txid, payout_id))
        conn.commit()
    finally:
        conn.close()

def get_task(task_id: int) -> Optional[dict]:
    conn = _conn()
    try:
        row = conn.execute("SELECT t.*, p.family, p.symbol, p.timeframe_min, p.years, p.grid_spec_json, p.risk_spec_json, p.seed, p.name as pool_name FROM mining_tasks t LEFT JOIN factor_pools p ON t.pool_id = p.id WHERE t.id = ?", (task_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()

def update_task_progress(task_id: int, progress: dict) -> None:
    import random, time
    for attempt in range(15):
        try:
            conn = _conn()
            try:
                conn.execute(
                    "UPDATE mining_tasks SET progress_json = ?, updated_at = ?, last_heartbeat = ? WHERE id = ?", 
                    (json.dumps(progress, ensure_ascii=False), _now_iso(), _now_iso(), task_id)
                )
                conn.commit()
                break
            finally:
                conn.close()
        except Exception as e:
            if attempt == 14:
                print(f"[CRITICAL DB ERROR] update_task_progress 放棄重試: {e}")
                # 吞下錯誤，絕對不拋出異常，防止執行緒崩潰導致任務被標記為 error
                break
            time.sleep(random.uniform(0.1, 0.5) * (1.2 ** attempt))

def update_task_status(task_id: int, status: str, finished: bool = False) -> None:
    import random, time
    for attempt in range(15):
        try:
            conn = _conn()
            try:
                conn.execute(
                    "UPDATE mining_tasks SET status = ?, updated_at = ?, last_heartbeat = ? WHERE id = ?", 
                    (status, _now_iso(), _now_iso(), task_id)
                )
                conn.commit()
                break
            finally:
                conn.close()
        except Exception as e:
            if attempt == 14:
                print(f"[CRITICAL DB ERROR] update_task_status 放棄重試: {e}")
                # 吞下錯誤，絕對不拋出異常
                break
            time.sleep(random.uniform(0.1, 0.5) * (1.2 ** attempt))

def clear_candidates_for_task(task_id: int) -> None:
    conn = _conn()
    try:
        conn.execute("DELETE FROM candidates WHERE task_id = ?", (task_id,))
        conn.commit()
    finally:
        conn.close()

def insert_candidate(task_id: int, user_id: int, pool_id: int, params: dict, metrics: dict, score: float) -> int:
    import time
    for attempt in range(8):
        try:
            conn = _conn()
            try:
                pool = conn.execute("SELECT direction, risk_spec_json FROM factor_pools WHERE id = ?", (int(pool_id),)).fetchone()
                direction = _infer_direction(
                    direction=(pool or {}).get("direction") if pool else None,
                    params_json=params,
                    risk_spec_json=(pool or {}).get("risk_spec_json") if pool else None,
                )
                if getattr(conn, "kind", "sqlite") == "postgres":
                    row = conn.execute(
                        "INSERT INTO candidates (task_id, user_id, pool_id, direction, params_json, metrics_json, score, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
                        (
                            task_id,
                            user_id,
                            pool_id,
                            direction,
                            json.dumps(params, ensure_ascii=False),
                            json.dumps(metrics, ensure_ascii=False),
                            score,
                            _now_iso(),
                        )
                    ).fetchone()
                    conn.commit()
                    return int((row or {}).get("id") or 0)

                cur = conn.execute(
                    "INSERT INTO candidates (task_id, user_id, pool_id, direction, params_json, metrics_json, score, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        task_id,
                        user_id,
                        pool_id,
                        direction,
                        json.dumps(params, ensure_ascii=False),
                        json.dumps(metrics, ensure_ascii=False),
                        score,
                        _now_iso(),
                    )
                )
                conn.commit()
                return cur.lastrowid
            finally:
                conn.close()
        except Exception as e:
            if attempt == 7:
                print(f"[DB ERROR] insert_candidate 失敗: {e}")
                raise e
            time.sleep(0.1 * (1.5 ** attempt))
    return 0

def claim_task_for_run(task_id: int) -> bool:
    conn = _conn()
    try:
        cur = conn.execute("UPDATE mining_tasks SET status = 'running', updated_at = ? WHERE id = ? AND status IN ('assigned', 'queued')", (_now_iso(), task_id))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()

def create_submission(candidate_id: int, user_id: int, pool_id: int, audit: dict) -> int:
    conn = _conn()
    try:
        if getattr(conn, "kind", "sqlite") == "postgres":
            row = conn.execute("INSERT INTO submissions (candidate_id, user_id, pool_id, audit_json, submitted_at) VALUES (?, ?, ?, ?, ?) RETURNING id",
                               (candidate_id, user_id, pool_id, json.dumps(audit, ensure_ascii=False), _now_iso())).fetchone()
            sub_id = int((row or {}).get("id") or 0)
        else:
            cur = conn.execute("INSERT INTO submissions (candidate_id, user_id, pool_id, audit_json, submitted_at) VALUES (?, ?, ?, ?, ?)",
                               (candidate_id, user_id, pool_id, json.dumps(audit, ensure_ascii=False), _now_iso()))
            sub_id = int(cur.lastrowid)
            
        conn.execute("UPDATE candidates SET is_submitted = 1 WHERE id = ?", (candidate_id,))
        conn.commit()
        return sub_id
    finally:
        conn.close()

def data_hash_setting_key(symbol: str, tf_min: int, years: int) -> str:
    return f"dh_{symbol}_{tf_min}m_{years}y"

def data_hash_ts_setting_key(symbol: str, tf_min: int, years: int) -> str:
    return f"dh_ts_{symbol}_{tf_min}m_{years}y"

def get_data_hash(symbol: str, tf_min: int, years: int) -> dict:
    conn = _conn()
    try:
        h = get_setting(conn, data_hash_setting_key(symbol, tf_min, years), "")
        ts = get_setting(conn, data_hash_ts_setting_key(symbol, tf_min, years), "")
        return {"data_hash": str(h), "data_hash_ts": str(ts)}
    finally:
        conn.close()

def set_data_hash(symbol: str, tf_min: int, years: int, data_hash: str, ts: str) -> None:
    conn = _conn()
    try:
        set_setting(conn, data_hash_setting_key(symbol, tf_min, years), data_hash)
        set_setting(conn, data_hash_ts_setting_key(symbol, tf_min, years), ts)
        # [史詩級修復] 補上遺失的 commit！確保資料指紋真實落地，
        # 徹底打破「 Worker 領取 -> 查無指紋 -> 觸發同步 -> 沒存檔 -> Worker 再次領取」的無限死迴圈，
        # 同時消滅伺服器重複建置 K 線的恐怖效能黑洞！
        conn.commit()
    finally:
        conn.close()

def worker_heartbeat(worker_id: str, user_id: int, task_id: int = None) -> None:
    conn = _conn()
    try:
        if task_id is not None:
            conn.execute(
                "UPDATE mining_tasks SET last_heartbeat = ? WHERE id = ? AND user_id = ?",
                (_now_iso(), task_id, user_id)
            )
            conn.commit()
    except Exception as e:
        print(f"[DB ERROR] worker_heartbeat 失敗: {e}")
    finally:
        conn.close()

def insert_worker_event(user_id: Optional[int], worker_id: Optional[str], event: str, detail: Any) -> None:
    conn = _conn()
    try:
        conn.execute(
            "INSERT INTO worker_events (ts, user_id, worker_id, event, detail_json) VALUES (?, ?, ?, ?, ?)",
            (_now_iso(), int(user_id) if user_id is not None else None, str(worker_id or ""), str(event or ""), json.dumps(detail or {}, ensure_ascii=False)),
        )
        conn.commit()
    finally:
        conn.close()

def upsert_worker(worker_id: str, user_id: int, version: str, protocol: int, meta: dict) -> None:
    wid = str(worker_id or "").strip()
    if not wid:
        return

    uid = int(user_id or 0)
    now = _now_iso()

    kind = "worker"
    try:
        if isinstance(meta, dict) and str(meta.get("kind") or "").strip():
            kind = str(meta.get("kind")).strip()
    except Exception:
        kind = "worker"

    conn = _conn()
    try:
        # [專家級修復] 徹底移除這裡的 CREATE TABLE 腳本！
        # 建表任務已由 init_db 統一處理，這裡只做純粹的資料寫入，消滅所有 PostgreSQL 語法衝突與連線懸空
        conn.execute(
            """
            INSERT INTO workers (worker_id, user_id, kind, version, protocol, created_at, last_seen_at, meta_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(worker_id) DO UPDATE SET
                user_id=excluded.user_id,
                kind=excluded.kind,
                version=excluded.version,
                protocol=excluded.protocol,
                last_seen_at=excluded.last_seen_at,
                meta_json=excluded.meta_json
            """,
            (wid, uid if uid > 0 else None, str(kind), str(version or ""), int(protocol or 0), now, now, json.dumps(meta or {}, ensure_ascii=False)),
        )
        conn.commit()
    finally:
        conn.close()

def _worker_mark_finish(worker_id: str, ok: bool, cps: float = 0.0, err: str = "", task_id: int = 0, owner_user_id: int = 0) -> None:
    wid = str(worker_id or "").strip()
    if not wid:
        return
    conn = _conn()
    try:
        now = _now_iso()
        # 簡單 EMA 平滑 avg_cps（避免亂跳）
        row = conn.execute("SELECT avg_cps, tasks_done, tasks_fail FROM workers WHERE worker_id=? LIMIT 1", (wid,)).fetchone()
        prev = float(row["avg_cps"]) if row and row.get("avg_cps") is not None else 0.0
        alpha = 0.25
        new_avg = (alpha * float(cps)) + ((1 - alpha) * float(prev))

        if bool(ok):
            conn.execute(
                "UPDATE workers SET last_seen_at=?, last_task_id=?, tasks_done=tasks_done+1, avg_cps=? , last_error='' WHERE worker_id=?",
                (now, int(task_id) if task_id else None, float(new_avg), wid),
            )
            insert_worker_event(owner_user_id if owner_user_id > 0 else None, wid, "task_finish_ok", {"task_id": int(task_id), "cps": float(cps)})
        else:
            conn.execute(
                "UPDATE workers SET last_seen_at=?, last_task_id=?, tasks_fail=tasks_fail+1, avg_cps=? , last_error=? WHERE worker_id=?",
                (now, int(task_id) if task_id else None, float(new_avg), str(err or "")[:600], wid),
            )
            insert_worker_event(owner_user_id if owner_user_id > 0 else None, wid, "task_finish_fail", {"task_id": int(task_id), "error": str(err or "")[:600]})

        conn.commit()
    finally:
        conn.close()

def worker_touch_progress(worker_id: str, cps: float = 0.0, task_id: int = 0) -> None:
    wid = str(worker_id or "").strip()
    if not wid:
        return
    conn = _conn()
    try:
        now = _now_iso()
        row = conn.execute("SELECT avg_cps FROM workers WHERE worker_id=? LIMIT 1", (wid,)).fetchone()
        prev = float(row["avg_cps"]) if row and row.get("avg_cps") is not None else 0.0
        alpha = 0.15
        new_avg = (alpha * float(cps)) + ((1 - alpha) * float(prev))
        conn.execute(
            "UPDATE workers SET last_seen_at=?, last_task_id=?, avg_cps=? WHERE worker_id=?",
            (now, int(task_id) if task_id else None, float(new_avg), wid),
        )
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()

def get_worker_stats_snapshot(window_seconds: int = 60) -> Dict[str, Any]:
    win_s = int(max(10, min(3600, int(window_seconds or 60))))
    now_dt = datetime.now(timezone.utc)
    cutoff = (now_dt - timedelta(seconds=win_s)).isoformat()
    active_cutoff = (now_dt - timedelta(seconds=30)).isoformat()

    conn = _conn()
    try:
        total = conn.execute("SELECT COUNT(*) as c FROM workers").fetchone()
        total_workers = int(total["c"] or 0) if total else 0

        act = conn.execute("SELECT COUNT(*) as c FROM workers WHERE last_seen_at >= ?", (active_cutoff,)).fetchone()
        active_workers = int(act["c"] or 0) if act else 0

        okr = conn.execute("SELECT COUNT(*) as c FROM worker_events WHERE ts >= ? AND event = 'task_finish_ok'", (cutoff,)).fetchone()
        far = conn.execute("SELECT COUNT(*) as c FROM worker_events WHERE ts >= ? AND event = 'task_finish_fail'", (cutoff,)).fetchone()
        ok_n = int(okr["c"] or 0) if okr else 0
        fail_n = int(far["c"] or 0) if far else 0

        denom = float(max(1, ok_n + fail_n))
        fail_rate = float(fail_n) / denom

        tasks_per_min = float(ok_n) / (float(win_s) / 60.0)

        rows = conn.execute(
            "SELECT worker_id, kind, version, protocol, last_seen_at, last_task_id, tasks_done, tasks_fail, avg_cps, last_error FROM workers ORDER BY last_seen_at DESC LIMIT 200"
        ).fetchall()
        workers = [dict(r) for r in rows] if rows else []

        return {
            "window_seconds": win_s,
            "active_workers": int(active_workers),
            "total_workers": int(total_workers),
            "tasks_ok": int(ok_n),
            "tasks_fail": int(fail_n),
            "tasks_per_min": float(tasks_per_min),
            "fail_rate": float(fail_rate),
            "workers": workers,
        }
    finally:
        conn.close()

def claim_next_task(user_id: int, worker_id: str) -> Optional[dict]:
    import uuid
    conn = _conn()
    try:
        # 尋找 assigned 且屬於該用戶的任務
        row = conn.execute(
            "SELECT t.*, p.family, p.symbol, p.timeframe_min, p.years, p.grid_spec_json, p.risk_spec_json, p.seed, p.name as pool_name "
            "FROM mining_tasks t LEFT JOIN factor_pools p ON t.pool_id = p.id "
            "WHERE t.user_id = ? AND t.status IN ('assigned', 'queued') ORDER BY t.id ASC LIMIT 1",
            (user_id,)
        ).fetchone()
        
        if not row:
            return None
            
        task_id = row["id"]
        lease_id = str(uuid.uuid4())
        
        cur = conn.execute(
            "UPDATE mining_tasks SET status = 'running', updated_at = ? WHERE id = ? AND status IN ('assigned', 'queued')",
            (_now_iso(), task_id)
        )
        if cur.rowcount == 0:
            return None # 被搶走
            
        conn.commit()
        d = dict(row)
        d["lease_id"] = lease_id
        d["lease_worker_id"] = worker_id
        return d
    except Exception as e:
        import traceback
        print(f"[DB ERROR] claim_next_task 嚴重錯誤: {e}\n{traceback.format_exc()}")
        return None
    finally:
        conn.close()

def update_task_progress_with_lease(task_id: int, user_id: int, worker_id: str, lease_id: str, progress: dict) -> bool:
    conn = _conn()
    try:
        # lease 機制雖然在單機版弱化，但保留狀態檢查確保安全
        cur = conn.execute(
            "UPDATE mining_tasks SET progress_json = ?, updated_at = ?, last_heartbeat = ? WHERE id = ? AND user_id = ? AND status = 'running'",
            (json.dumps(progress, ensure_ascii=False), _now_iso(), _now_iso(), task_id, user_id)
        )
        ok = (cur.rowcount > 0)
        conn.commit()

        # 統計：吞吐 cps / last_seen
        try:
            sp = 0.0
            try:
                sp = float((progress or {}).get("speed_cps") or 0.0)
            except Exception:
                sp = 0.0
            if ok:
                worker_touch_progress(worker_id=str(worker_id), cps=float(sp), task_id=int(task_id))
        except Exception:
            pass

        return bool(ok)
    except Exception as e:
        print(f"[DB ERROR] update_task_progress_with_lease: {e}")
        return False
    finally:
        conn.close()

def release_task_with_lease(task_id: int, user_id: int, worker_id: str, lease_id: str, progress: dict) -> bool:
    conn = _conn()
    try:
        cur = conn.execute(
            "UPDATE mining_tasks SET status = 'assigned', progress_json = ?, updated_at = ? WHERE id = ? AND user_id = ?",
            (json.dumps(progress, ensure_ascii=False), _now_iso(), task_id, user_id)
        )
        conn.commit()
        return cur.rowcount > 0
    except Exception as e:
        print(f"[DB ERROR] release_task_with_lease: {e}")
        return False
    finally:
        conn.close()

def finish_task_with_lease(task_id: int, user_id: int, worker_id: str, lease_id: str, candidates: list, final_progress: dict) -> Optional[int]:
    conn = _conn()
    try:
        cur = conn.execute(
            "UPDATE mining_tasks SET status = 'completed', progress_json = ?, updated_at = ? WHERE id = ? AND user_id = ? AND status = 'running'",
            (json.dumps(final_progress, ensure_ascii=False), _now_iso(), task_id, user_id)
        )
        if cur.rowcount == 0:
            return None
            
        task_info = get_task(task_id)
        pool_id = task_info["pool_id"] if task_info else 0
        best_candidate_id = None
        
        for cand in candidates:
            sc = cand.get("score", 0.0)
            prms = cand.get("params", {})
            mets = cand.get("metrics", {})
            cid = insert_candidate(task_id, user_id, pool_id, prms, mets, sc)
            if best_candidate_id is None:
                best_candidate_id = cid
                
        conn.commit()

        # 統計：完成任務
        try:
            cps = 0.0
            try:
                cps = float((final_progress or {}).get("speed_cps") or 0.0)
            except Exception:
                cps = 0.0
            _worker_mark_finish(worker_id=str(worker_id), ok=True, cps=float(cps), err="", task_id=int(task_id), owner_user_id=int(user_id))
        except Exception:
            pass

        return best_candidate_id
    except Exception as e:
        import traceback
        print(f"[DB ERROR] finish_task_with_lease: {e}\n{traceback.format_exc()}")
        return None
    finally:
        conn.close()

def utc_now_iso() -> str:
    return _now_iso()

def clean_zombie_tasks(timeout_minutes: int = 15) -> int:
    """自動清理異常斷線導致卡在 running 狀態的任務，並重置狀態與介面進度"""
    conn = _conn()
    count = 0
    try:
        from datetime import datetime, timedelta, timezone
        import json
        cutoff_dt = datetime.now(timezone.utc) - timedelta(minutes=timeout_minutes)
        cutoff_iso = cutoff_dt.isoformat()
        
        # [極致修復] 同時抓出僵屍任務與崩潰卡死的 error 任務，並納入 syncing 狀態防止無限卡死
        rows = conn.execute(
            "SELECT id, progress_json, status, attempt FROM mining_tasks WHERE (status IN ('running', 'syncing') AND last_heartbeat < ?) OR status = 'error'", 
            (cutoff_iso,)
        ).fetchall()
        
        now = _now_iso()
        updates_completed = []
        updates_assigned = []
        for row in rows:
            tid = row["id"]
            attempt = int(row["attempt"] or 0) + 1
            try:
                prog = json.loads(row["progress_json"] or "{}")
            except Exception:
                prog = {}
            
            if attempt >= 4:
                prog["phase"] = "error"
                prog["phase_msg"] = "終止"
                prog["last_error"] = "執行異常"
                prog["updated_at"] = now
                updates_completed.append((attempt, now, json.dumps(prog, ensure_ascii=False), tid))
            else:
                prog["phase"] = "queued"
                prog["phase_msg"] = f"任務因異常中斷 (第 {attempt} 次)"
                prog["last_error"] = "重試"
                prog["updated_at"] = now
                updates_assigned.append((attempt, now, json.dumps(prog, ensure_ascii=False), tid))
            count += 1
            
        # [專家級修復] 改用迴圈 execute，因為自訂 _DBConn 尚未實作 executemany，在 PostgreSQL 下效能依舊極佳
        if updates_completed:
            for p in updates_completed:
                conn.execute("UPDATE mining_tasks SET status = 'completed', attempt = ?, updated_at = ?, progress_json = ? WHERE id = ?", p)
                log_sys_event("ZOMBIE_TASK_KILLED", None, f"系統強制終止崩潰死鎖任務 ID: {p[3]} (重試次數達標)", {"task_id": p[3], "attempt": p[0]})
        if updates_assigned:
            for p in updates_assigned:
                conn.execute("UPDATE mining_tasks SET status = 'assigned', attempt = ?, updated_at = ?, progress_json = ? WHERE id = ?", p)
                log_sys_event("ZOMBIE_TASK_RECYCLED", None, f"系統回收逾時未心跳任務 ID: {p[3]} (釋放回佇列)", {"task_id": p[3], "attempt": p[0]})
            
        if count > 0:
            conn.commit()
        return count
    except Exception as e:
        print(f"[DB ERROR] clean_zombie_tasks: {e}")
        return 0
    finally:
        conn.close()

def update_user_nickname(user_id: int, nickname: str) -> None:
    """
    更新用戶暱稱。
    """
    conn = _conn()
    try:
        row = conn.execute("SELECT username FROM users WHERE id = ? LIMIT 1", (int(user_id),)).fetchone()
        fallback_username = str((dict(row) if row else {}).get("username") or "")
        safe_nick = _sanitize_nickname_text(nickname, fallback_username)
        conn.execute(
            "UPDATE users SET nickname = ?, profile_updated_at = ? WHERE id = ?",
            (safe_nick, _now_iso(), int(user_id)),
        )
        conn.commit()
    except Exception as e:
        print(f"[DB ERROR] update_user_nickname: {e}")
        raise
    finally:
        conn.close()


def update_user_profile(user_id: int, *, nickname: Optional[str] = None, avatar_url: Optional[str] = None, clear_avatar: bool = False) -> Dict[str, Any]:
    conn = _conn()
    try:
        row = conn.execute("SELECT * FROM users WHERE id = ? LIMIT 1", (int(user_id),)).fetchone()
        if not row:
            raise ValueError("user_not_found")
        current = dict(row)
        next_nickname = _sanitize_nickname_text(
            current.get("nickname") if nickname is None else nickname,
            current.get("username"),
        )
        next_avatar_url = "" if clear_avatar else _sanitize_avatar_url(current.get("avatar_url") if avatar_url is None else avatar_url)
        conn.execute(
            "UPDATE users SET nickname = ?, avatar_url = ?, profile_updated_at = ? WHERE id = ?",
            (next_nickname, next_avatar_url, _now_iso(), int(user_id)),
        )
        conn.commit()
        fresh = conn.execute("SELECT * FROM users WHERE id = ? LIMIT 1", (int(user_id),)).fetchone()
        return _decorate_user_row(fresh, default_avatar_url=_default_avatar_url_from_conn(conn)) if fresh else {}
    finally:
        conn.close()


def set_default_avatar_url(avatar_url: str) -> str:
    safe_avatar = _sanitize_avatar_url(avatar_url)
    set_setting("default_avatar_data_url", safe_avatar)
    return safe_avatar


def get_default_avatar_url() -> str:
    return _default_avatar_url_from_conn()


def _leaderboard_task_scan_limit() -> int:
    try:
        value = int(os.environ.get("SHEEP_LEADERBOARD_TASK_SCAN_LIMIT", "4000") or "4000")
    except Exception:
        value = 4000
    return max(500, min(20000, value))


def _leaderboard_python_fallback(conn: Any, cutoff_iso: str, window_end_iso: str) -> Dict[str, List[Dict[str, Any]]]:
    def _parse_iso(value: Any) -> Optional[datetime]:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None

    default_avatar_url = _default_avatar_url_from_conn(conn)
    query = """
        SELECT recent.id, recent.progress_json, recent.created_at, recent.activity_at,
               u.id as user_id, u.username, u.nickname, u.avatar_url
        FROM (
            SELECT t.id, t.user_id, t.progress_json, t.created_at,
                   COALESCE(t.last_heartbeat, t.updated_at, t.created_at) as activity_at
            FROM mining_tasks t
            WHERE COALESCE(t.last_heartbeat, t.updated_at, t.created_at) >= ?
              AND COALESCE(t.last_heartbeat, t.updated_at, t.created_at) <= ?
              AND t.status IN ('running', 'completed')
            ORDER BY COALESCE(t.last_heartbeat, t.updated_at, t.created_at) DESC, t.id DESC
            LIMIT ?
        ) recent
        JOIN users u ON recent.user_id = u.id
        ORDER BY recent.activity_at DESC, recent.id DESC
    """
    rows = conn.execute(query, (cutoff_iso, window_end_iso, int(_leaderboard_task_scan_limit()))).fetchall()
    combos_map: Dict[int, Dict[str, Any]] = {}
    time_map: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        entry = dict(row or {})
        user_bits = _decorate_user_row(entry, default_avatar_url=default_avatar_url)
        progress = parse_json_object(entry.get("progress_json"))
        try:
            combos_done = max(0.0, float(progress.get("combos_done") or progress.get("done") or progress.get("combos_total") or progress.get("total") or 0.0))
        except Exception:
            combos_done = 0.0
        try:
            elapsed_s = max(0.0, float(progress.get("elapsed_s") or progress.get("elapsed") or 0.0))
        except Exception:
            elapsed_s = 0.0
        if elapsed_s <= 0:
            activity_dt = _parse_iso(entry.get("activity_at"))
            created_dt = _parse_iso(entry.get("created_at"))
            if activity_dt is not None and created_dt is not None:
                try:
                    elapsed_s = max(
                        0.0,
                        float((activity_dt.astimezone(timezone.utc) - created_dt.astimezone(timezone.utc)).total_seconds()),
                    )
                except Exception:
                    elapsed_s = 0.0
        uid = int(entry.get("user_id") or 0)
        if uid <= 0:
            continue
        if combos_done > 0:
            rec = combos_map.setdefault(
                uid,
                {
                    "username": user_bits.get("username"),
                    "nickname": user_bits.get("nickname"),
                    "avatar_url": user_bits.get("avatar_url"),
                    "task_count": 0,
                    "total_done": 0.0,
                },
            )
            rec["task_count"] = int(rec.get("task_count") or 0) + 1
            rec["total_done"] = float(rec.get("total_done") or 0.0) + float(combos_done)
        if elapsed_s > 0:
            rec = time_map.setdefault(
                uid,
                {
                    "username": user_bits.get("username"),
                    "nickname": user_bits.get("nickname"),
                    "avatar_url": user_bits.get("avatar_url"),
                    "total_seconds": 0.0,
                },
            )
            rec["total_seconds"] = float(rec.get("total_seconds") or 0.0) + float(elapsed_s)
    combos_rows = sorted(combos_map.values(), key=lambda item: (-float(item.get("total_done") or 0.0), str(item.get("username") or "")))[:300]
    time_rows = sorted(time_map.values(), key=lambda item: (-float(item.get("total_seconds") or 0.0), str(item.get("username") or "")))[:300]
    return {"combos": combos_rows, "time": time_rows}


def get_leaderboard_stats(period_hours: int = 720) -> dict:
    """
    專家級聚合查詢：一次性撈取排行榜所需的所有維度數據。
    period_hours: 1 (1h), 24 (24h), 720 (30d)
    """
    conn = _conn()
    try:
        hours = max(1, min(720, int(period_hours or 720)))
        now_dt = datetime.now(timezone.utc)
        cutoff_dt = now_dt - timedelta(hours=hours)
        cutoff_iso = cutoff_dt.isoformat()
        window_end_iso = _now_iso()
        default_avatar_url = _default_avatar_url_from_conn(conn)

        results = {
            "combos": [],
            "score": [],
            "time": [],
            "points": [],
            "qualified_strategies": [],
        }

        db_kind = _db_kind()
        if db_kind == "postgres":
            try:
                recent_rows = _leaderboard_python_fallback(conn, cutoff_iso, window_end_iso)
                results["combos"] = recent_rows.get("combos") or []
                results["time"] = recent_rows.get("time") or []
            except Exception as e:
                print(f"[DB WARN] Leaderboard recent-task query failed: {e}")
                results["combos"] = []
                results["time"] = []
        else:
            sql_combos = """
                SELECT u.username, u.nickname, u.avatar_url, COUNT(t.id) as task_count,
                       SUM(COALESCE(CAST(json_extract(t.progress_json, '$.combos_done') AS INTEGER), 0)) as total_done
                FROM mining_tasks t
                JOIN users u ON t.user_id = u.id
                WHERE COALESCE(t.last_heartbeat, t.updated_at, t.created_at) >= ?
                  AND COALESCE(t.last_heartbeat, t.updated_at, t.created_at) <= ?
                  AND t.status IN ('running', 'completed')
                GROUP BY u.id, u.username, u.nickname, u.avatar_url
                ORDER BY total_done DESC
                LIMIT 300
            """
            sql_time = """
                WITH task_elapsed AS (
                    SELECT
                        t.id,
                        t.user_id,
                        MAX(COALESCE(CAST(json_extract(t.progress_json, '$.elapsed_s') AS REAL), 0.0)) as elapsed_s
                    FROM mining_tasks t
                    WHERE COALESCE(t.last_heartbeat, t.updated_at, t.created_at) >= ?
                      AND COALESCE(t.last_heartbeat, t.updated_at, t.created_at) <= ?
                      AND t.status IN ('running', 'completed')
                    GROUP BY t.id, t.user_id
                )
                SELECT u.username, u.nickname, u.avatar_url,
                       SUM(te.elapsed_s) as total_seconds
                FROM task_elapsed te
                JOIN users u ON te.user_id = u.id
                GROUP BY u.id, u.username, u.nickname, u.avatar_url
                ORDER BY total_seconds DESC
                LIMIT 300
            """
            try:
                rows = conn.execute(sql_combos, (cutoff_iso, window_end_iso)).fetchall()
                results["combos"] = [
                    _decorate_user_row(dict(r), default_avatar_url=default_avatar_url)
                    for r in rows
                    if r["total_done"] is not None and float(r["total_done"]) > 0
                ]
            except Exception as e:
                print(f"[DB WARN] Leaderboard combos query failed: {e}")
                results["combos"] = []

        sql_score = """
            SELECT u.username, u.nickname, u.avatar_url, MAX(c.score) as max_score
            FROM candidates c
            JOIN users u ON c.user_id = u.id
            WHERE c.created_at >= ?
            GROUP BY u.id, u.username, u.nickname, u.avatar_url
            ORDER BY max_score DESC
            LIMIT 300
        """
        try:
            rows = conn.execute(sql_score, (cutoff_iso,)).fetchall()
            results["score"] = [
                _decorate_user_row(dict(r), default_avatar_url=default_avatar_url)
                for r in rows
                if r["max_score"] is not None
            ]
        except Exception:
            results["score"] = []

        if db_kind != "postgres":
            try:
                rows = conn.execute(sql_time, (cutoff_iso, window_end_iso)).fetchall()
                results["time"] = [
                    _decorate_user_row(dict(r), default_avatar_url=default_avatar_url)
                    for r in rows
                    if r["total_seconds"] is not None and float(r["total_seconds"]) > 0
                ]
            except Exception as e:
                print(f"[DB WARN] Leaderboard time query failed: {e}")
                results["time"] = []

            if not results["combos"] or not results["time"]:
                try:
                    fallback = _leaderboard_python_fallback(conn, cutoff_iso, window_end_iso)
                    if not results["combos"]:
                        results["combos"] = fallback.get("combos") or []
                    if not results["time"]:
                        results["time"] = fallback.get("time") or []
                except Exception as e:
                    print(f"[DB WARN] Leaderboard python fallback failed: {e}")

        sql_points = """
            SELECT u.username, u.nickname, u.avatar_url, SUM(p.amount_usdt) as total_usdt
            FROM payouts p
            JOIN users u ON p.user_id = u.id
            WHERE p.created_at >= ?
            GROUP BY u.id, u.username, u.nickname, u.avatar_url
            ORDER BY total_usdt DESC
            LIMIT 300
        """
        try:
            rows = conn.execute(sql_points, (cutoff_iso,)).fetchall()
            results["points"] = [
                _decorate_user_row(dict(r), default_avatar_url=default_avatar_url)
                for r in rows
                if r["total_usdt"] is not None and float(r["total_usdt"]) > 0
            ]
        except Exception as e:
            print(f"[DB WARN] Leaderboard points query failed: {e}")
            results["points"] = []

        if not results["points"]:
            sql_points_all_time = """
                SELECT u.username, u.nickname, u.avatar_url, SUM(p.amount_usdt) as total_usdt
                FROM payouts p
                JOIN users u ON p.user_id = u.id
                GROUP BY u.id, u.username, u.nickname, u.avatar_url
                ORDER BY total_usdt DESC
                LIMIT 300
            """
            try:
                rows = conn.execute(sql_points_all_time).fetchall()
                results["points"] = [
                    _decorate_user_row(dict(r), default_avatar_url=default_avatar_url)
                    for r in rows
                    if r["total_usdt"] is not None and float(r["total_usdt"]) > 0
                ]
            except Exception as e:
                print(f"[DB WARN] Leaderboard all-time points query failed: {e}")

        if not results["points"]:
            capital_usdt = float(get_setting(conn, "capital_usdt", 0.0) or 0.0)
            payout_rate = float(get_setting(conn, "payout_rate", 0.0) or 0.0)
            if capital_usdt > 0.0 and payout_rate > 0.0:
                sql_points_fallback = """
                    SELECT u.username, u.nickname, u.avatar_url,
                           SUM((COALESCE(wc.return_pct, 0.0) / 100.0) * (COALESCE(st.allocation_pct, 0.0) / 100.0) * ? * ?) as total_usdt
                    FROM weekly_checks wc
                    JOIN strategies st ON wc.strategy_id = st.id
                    JOIN users u ON st.user_id = u.id
                    WHERE wc.checked_at >= ? AND COALESCE(wc.eligible, 0) = 1
                    GROUP BY u.id, u.username, u.nickname, u.avatar_url
                    ORDER BY total_usdt DESC
                    LIMIT 300
                """
                try:
                    rows = conn.execute(sql_points_fallback, (capital_usdt, payout_rate, cutoff_iso)).fetchall()
                    results["points"] = [
                        _decorate_user_row(dict(r), default_avatar_url=default_avatar_url)
                        for r in rows
                        if r["total_usdt"] is not None and float(r["total_usdt"]) > 0
                    ]
                except Exception as e:
                    print(f"[DB WARN] Leaderboard points fallback query failed: {e}")

        sql_qualified_strategies = """
            SELECT u.username, u.nickname, u.avatar_url, COUNT(st.id) as active_strategy_count
            FROM strategies st
            JOIN users u ON st.user_id = u.id
            WHERE COALESCE(st.status, '') = 'active'
            GROUP BY u.id, u.username, u.nickname, u.avatar_url
            ORDER BY active_strategy_count DESC, u.id ASC
            LIMIT 300
        """
        try:
            rows = conn.execute(sql_qualified_strategies).fetchall()
            results["qualified_strategies"] = [
                _decorate_user_row(dict(r), default_avatar_url=default_avatar_url)
                for r in rows
                if r["active_strategy_count"] is not None and int(r["active_strategy_count"]) > 0
            ]
        except Exception as e:
            print(f"[DB WARN] Leaderboard qualified_strategies query failed: {e}")
            results["qualified_strategies"] = []

        return results
    except Exception as e:
        print(f"[DB ERROR] get_leaderboard_stats: {e}")
        return {"combos": [], "score": [], "time": [], "points": [], "qualified_strategies": []}
    finally:
        conn.close()
import uuid as _uuid

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _iso_add_seconds(sec: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=int(sec))).isoformat()

def _lease_seconds_default() -> int:
    try:
        return int(os.environ.get("SHEEP_TASK_LEASE_S", "120") or "120")
    except Exception:
        return 120

def _lease_extend_seconds() -> int:
    try:
        return int(os.environ.get("SHEEP_TASK_LEASE_EXTEND_S", "120") or "120")
    except Exception:
        return 120

_LAST_REAP_TS = 0.0
_REAP_LOCK = __import__("threading").Lock()

def _reap_expired_running(conn: _DBConn) -> int:
    global _LAST_REAP_TS
    import time
    now_ts = time.time()
    # [專家級優化] 節流：每 5 秒最多執行一次過期任務清理，避免大量 Worker 併發請求時引發嚴重的資料庫寫入鎖定 (Write Lock Contention)
    if now_ts - _LAST_REAP_TS < 5.0:
        return 0
    if not _REAP_LOCK.acquire(blocking=False):
        return 0
    try:
        _LAST_REAP_TS = time.time()
        now = _utc_now_iso()
        cur = conn.execute(
            """
            UPDATE mining_tasks
            SET status='assigned', lease_id=NULL, lease_worker_id=NULL, lease_expires_at=NULL, updated_at=?
            WHERE status='running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?
            """,
            (now, now),
        )
        return int(cur.rowcount or 0)
    except Exception:
        return 0
    finally:
        _REAP_LOCK.release()

def claim_next_task_any(worker_id: str) -> Optional[dict]:
    # compute token 專用：跨用戶派工（只派給 run_enabled=1）
    wid = str(worker_id or "").strip()
    if not wid:
        return None

    lease_s = _lease_seconds_default()
    lease_id = _uuid.uuid4().hex
    now = _utc_now_iso()
    exp = _iso_add_seconds(lease_s)

    conn = _conn()
    try:
        _reap_expired_running(conn)

        if getattr(conn, "kind", "sqlite") == "postgres":
            row = conn.execute(
                """
                SELECT t.id
                FROM mining_tasks t
                JOIN users u ON u.id = t.user_id
                JOIN factor_pools p ON p.id = t.pool_id
                WHERE t.status IN ('assigned','queued')
                  AND COALESCE(u.disabled,0)=0
                  AND COALESCE(u.run_enabled,1)=1
                  AND COALESCE(p.active,1)=1
                ORDER BY t.id ASC
                FOR UPDATE OF t SKIP LOCKED
                LIMIT 1
                """
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT t.id
                FROM mining_tasks t
                JOIN users u ON u.id = t.user_id
                JOIN factor_pools p ON p.id = t.pool_id
                WHERE t.status IN ('assigned','queued')
                  AND COALESCE(u.disabled,0)=0
                  AND COALESCE(u.run_enabled,1)=1
                  AND COALESCE(p.active,1)=1
                ORDER BY t.id ASC
                LIMIT 1
                """
            ).fetchone()

        if not row:
            conn.commit()
            return None

        tid = int(row.get("id") or 0)
        if tid <= 0:
            conn.commit()
            return None

        cur = conn.execute(
            """
            UPDATE mining_tasks
            SET status='running',
                lease_id=?,
                lease_worker_id=?,
                lease_expires_at=?,
                last_heartbeat=?,
                updated_at=?,
                attempt=COALESCE(attempt,0)+1
            WHERE id=? AND status IN ('assigned','queued')
            """,
            (lease_id, wid, exp, now, now, tid),
        )
        if int(cur.rowcount or 0) <= 0:
            conn.commit()
            return None

        conn.commit()

    finally:
        conn.close()

    t = get_task(int(tid))
    if not t:
        return None
    t["lease_id"] = str(lease_id)
    t["lease_worker_id"] = str(wid)
    t["lease_expires_at"] = str(exp)
    return t

def claim_next_task(user_id: int, worker_id: str) -> Optional[dict]:
    # 原本 worker token：只領自己的任務（保留）
    uid = int(user_id or 0)
    wid = str(worker_id or "").strip()
    if uid <= 0 or not wid:
        return None

    lease_s = _lease_seconds_default()
    lease_id = _uuid.uuid4().hex
    now = _utc_now_iso()
    exp = _iso_add_seconds(lease_s)

    conn = _conn()
    try:
        _reap_expired_running(conn)

        # [專家級優化] 同樣注入 cycle_id，利用複合索引秒殺查詢
        c_row = conn.execute("SELECT id FROM mining_cycles WHERE status = 'active' ORDER BY id DESC LIMIT 1").fetchone()
        c_id = int(c_row["id"]) if c_row else 0

        if getattr(conn, "kind", "sqlite") == "postgres":
            row = conn.execute(
                """
                SELECT t.id
                FROM mining_tasks t
                JOIN users u ON u.id = t.user_id
                JOIN factor_pools p ON p.id = t.pool_id
                WHERE t.user_id=?
                AND t.cycle_id=?
                AND t.status IN ('assigned','queued')
                AND COALESCE(u.disabled,0)=0
                AND COALESCE(u.run_enabled,1)=1
                AND COALESCE(p.active,1)=1
                ORDER BY t.id ASC
                FOR UPDATE OF t SKIP LOCKED
                LIMIT 1
                """,
                (uid, c_id),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT t.id
                FROM mining_tasks t
                JOIN users u ON u.id = t.user_id
                JOIN factor_pools p ON p.id = t.pool_id
                WHERE t.user_id=?
                AND t.cycle_id=?
                AND t.status IN ('assigned','queued')
                AND COALESCE(u.disabled,0)=0
                AND COALESCE(u.run_enabled,1)=1
                AND COALESCE(p.active,1)=1
                ORDER BY t.id ASC
                LIMIT 1
                """,
                (uid, c_id),
            ).fetchone()

        if not row:
            conn.commit()
            return None

        tid = int(row.get("id") or 0)
        if tid <= 0:
            conn.commit()
            return None

        cur = conn.execute(
            """
            UPDATE mining_tasks
            SET status='running',
                lease_id=?,
                lease_worker_id=?,
                lease_expires_at=?,
                last_heartbeat=?,
                updated_at=?,
                attempt=COALESCE(attempt,0)+1
            WHERE id=? AND user_id=? AND status IN ('assigned','queued')
            """,
            (lease_id, wid, exp, now, now, tid, uid),
        )
        if int(cur.rowcount or 0) <= 0:
            conn.commit()
            return None

        conn.commit()

    finally:
        conn.close()

    t = get_task(int(tid))
    if not t:
        return None
    t["lease_id"] = str(lease_id)
    t["lease_worker_id"] = str(wid)
    t["lease_expires_at"] = str(exp)
    return t

def update_task_progress_with_lease(task_id: int, user_id: int, worker_id: str, lease_id: str, progress: dict, allow_cross_user: bool = False) -> bool:
    tid = int(task_id or 0)
    wid = str(worker_id or "").strip()
    lid = str(lease_id or "").strip()
    if tid <= 0 or not wid or not lid:
        return False

    now = _utc_now_iso()
    new_exp = _iso_add_seconds(_lease_extend_seconds())

    conn = _conn()
    try:
        if bool(allow_cross_user):
            cur = conn.execute(
                """
                UPDATE mining_tasks
                SET progress_json=?, updated_at=?, last_heartbeat=?, lease_expires_at=?
                WHERE id=? AND status='running' AND lease_id=? AND lease_worker_id=?
                """,
                (json.dumps(progress or {}, ensure_ascii=False), now, now, new_exp, tid, lid, wid),
            )
        else:
            cur = conn.execute(
                """
                UPDATE mining_tasks
                SET progress_json=?, updated_at=?, last_heartbeat=?, lease_expires_at=?
                WHERE id=? AND user_id=? AND status='running' AND lease_id=? AND lease_worker_id=?
                """,
                (json.dumps(progress or {}, ensure_ascii=False), now, now, new_exp, tid, int(user_id or 0), lid, wid),
            )
        conn.commit()
        return int(cur.rowcount or 0) > 0
    except Exception:
        return False
    finally:
        conn.close()

def release_task_with_lease(task_id: int, user_id: int, worker_id: str, lease_id: str, progress: dict, allow_cross_user: bool = False) -> bool:
    tid = int(task_id or 0)
    wid = str(worker_id or "").strip()
    lid = str(lease_id or "").strip()
    if tid <= 0 or not wid or not lid:
        return False

    now = _utc_now_iso()

    conn = _conn()
    try:
        if bool(allow_cross_user):
            cur = conn.execute(
                """
                UPDATE mining_tasks
                SET status='assigned', progress_json=?, updated_at=?, lease_id=NULL, lease_worker_id=NULL, lease_expires_at=NULL
                WHERE id=? AND status='running' AND lease_id=? AND lease_worker_id=?
                """,
                (json.dumps(progress or {}, ensure_ascii=False), now, tid, lid, wid),
            )
        else:
            cur = conn.execute(
                """
                UPDATE mining_tasks
                SET status='assigned', progress_json=?, updated_at=?, lease_id=NULL, lease_worker_id=NULL, lease_expires_at=NULL
                WHERE id=? AND user_id=? AND status='running' AND lease_id=? AND lease_worker_id=?
                """,
                (json.dumps(progress or {}, ensure_ascii=False), now, tid, int(user_id or 0), lid, wid),
            )
        conn.commit()
        return int(cur.rowcount or 0) > 0
    except Exception:
        return False
    finally:
        conn.close()

def finish_task_with_lease(task_id: int, user_id: int, worker_id: str, lease_id: str, candidates: list, final_progress: dict, allow_cross_user: bool = False) -> Optional[int]:
    tid = int(task_id or 0)
    wid = str(worker_id or "").strip()
    lid = str(lease_id or "").strip()
    if tid <= 0 or not wid or not lid:
        return None

    now = _utc_now_iso()

    conn = _conn()
    try:
        if bool(allow_cross_user):
            trow = conn.execute(
                "SELECT id, user_id, pool_id FROM mining_tasks WHERE id=? AND status='running' AND lease_id=? AND lease_worker_id=? LIMIT 1",
                (tid, lid, wid),
            ).fetchone()
        else:
            trow = conn.execute(
                "SELECT id, user_id, pool_id FROM mining_tasks WHERE id=? AND user_id=? AND status='running' AND lease_id=? AND lease_worker_id=? LIMIT 1",
                (tid, int(user_id or 0), lid, wid),
            ).fetchone()

        if not trow:
            conn.commit()
            return None

        owner_id = int(trow.get("user_id") or 0)
        pool_id = int(trow.get("pool_id") or 0)

        cur = conn.execute(
            """
            UPDATE mining_tasks
            SET status='completed', progress_json=?, updated_at=?, last_heartbeat=?,
                lease_id=NULL, lease_worker_id=NULL, lease_expires_at=NULL
            WHERE id=? AND status='running' AND lease_id=? AND lease_worker_id=?
            """,
            (json.dumps(final_progress or {}, ensure_ascii=False), now, now, tid, lid, wid),
        )
        if int(cur.rowcount or 0) <= 0:
            conn.commit()
            return None

        best_candidate_id = None
        for c in list(candidates or []):
            if not isinstance(c, dict):
                continue
            sc = float(c.get("score") or 0.0)
            pr = c.get("params") or c.get("params_json") or {}
            me = c.get("metrics") or {}
            direction = _infer_direction(params_json=pr)
            normalized_params = _normalize_strategy_params_payload(params_json=pr, direction=direction)
            
            if getattr(conn, "kind", "sqlite") == "postgres":
                row = conn.execute(
                    "INSERT INTO candidates (task_id, user_id, pool_id, direction, params_json, metrics_json, score, created_at, is_submitted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1) RETURNING id",
                    (tid, owner_id, pool_id, direction, json.dumps(normalized_params, ensure_ascii=False), json.dumps(me, ensure_ascii=False), sc, now),
                )
                rid = (row.fetchone() or {}).get("id")
                cid = int(rid or 0) if rid else None
            else:
                row = conn.execute(
                    "INSERT INTO candidates (task_id, user_id, pool_id, direction, params_json, metrics_json, score, created_at, is_submitted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)",
                    (tid, owner_id, pool_id, direction, json.dumps(normalized_params, ensure_ascii=False), json.dumps(me, ensure_ascii=False), sc, now),
                )
                cid = int(getattr(row, "lastrowid", 0) or 0)

            if cid:
                if best_candidate_id is None:
                    best_candidate_id = cid
                
                # 全自動佈署：建立 Submission 與 Strategy 上線
                try:
                    if getattr(conn, "kind", "sqlite") == "postgres":
                        row_sub = conn.execute(
                            "INSERT INTO submissions (candidate_id, user_id, pool_id, status, audit_json, submitted_at) VALUES (?, ?, ?, 'approved', '{}', ?) RETURNING id", 
                            (cid, owner_id, pool_id, now)
                        ).fetchone()
                        sub_id = int((row_sub or {}).get("id") or 0)
                        if sub_id > 0:
                            conn.execute(
                                "INSERT INTO strategies (submission_id, user_id, pool_id, direction, params_json, status, allocation_pct, note, created_at, expires_at, external_key) VALUES (?, ?, ?, ?, ?, 'active', 1.0, 'Auto-Deploy', ?, ?, ?)", 
                                (sub_id, owner_id, pool_id, direction, json.dumps(normalized_params, ensure_ascii=False), now, "2099-12-31T23:59:59Z", "")
                            )
                    else:
                        cur_sub = conn.execute(
                            "INSERT INTO submissions (candidate_id, user_id, pool_id, status, audit_json, submitted_at) VALUES (?, ?, ?, 'approved', '{}', ?)", 
                            (cid, owner_id, pool_id, now)
                        )
                        sub_id = int(cur_sub.lastrowid)
                        if sub_id > 0:
                            conn.execute(
                                "INSERT INTO strategies (submission_id, user_id, pool_id, direction, params_json, status, allocation_pct, note, created_at, expires_at, external_key) VALUES (?, ?, ?, ?, ?, 'active', 1.0, 'Auto-Deploy', ?, ?, ?)", 
                                (sub_id, owner_id, pool_id, direction, json.dumps(normalized_params, ensure_ascii=False), now, "2099-12-31T23:59:59Z", "")
                            )
                    log_sys_event("AUTO_DEPLOY_SUCCESS", owner_id, f"任務 {tid} 達標，已自動佈署策略上因子池", {"candidate_id": cid, "score": sc})
                except Exception as auto_deploy_err:
                    log_sys_event("AUTO_DEPLOY_FAIL", owner_id, f"任務 {tid} 自動佈署失敗: {auto_deploy_err}", {"candidate_id": cid})

        conn.commit()
        return best_candidate_id
    except Exception:
        try:
            conn.commit()
        except Exception:
            pass
        return None
    finally:
        conn.close()

# (重複的 set_user_run_enabled 已被移除，統一使用上方具備重試機制的版本)


def get_all_candidates_detailed(limit: int = 1000) -> list:
    """專家級：撈取全域策略與關聯的 OOS 績效，供 Web Excel 總表使用"""
    conn = _conn()
    try:
        query = """
        SELECT c.id as candidate_id, c.score, c.params_json, c.metrics_json, c.created_at, c.is_submitted,
               u.username, u.nickname, u.role,
               p.name as pool_name, p.symbol, p.timeframe_min,
               t.progress_json as task_progress
        FROM candidates c
        LEFT JOIN users u ON c.user_id = u.id
        LEFT JOIN factor_pools p ON c.pool_id = p.id
        LEFT JOIN mining_tasks t ON c.task_id = t.id
        ORDER BY c.score DESC
        LIMIT ?
        """
        rows = conn.execute(query, (limit,)).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            try: d["params"] = json.loads(d.get("params_json") or "{}")
            except Exception: d["params"] = {}
            try: d["metrics"] = json.loads(d.get("metrics_json") or "{}")
            except Exception: d["metrics"] = {}
            try:
                tprog = json.loads(d.get("task_progress") or "{}")
                # 強制顯示已上因子池，因未達標的不會被記錄在 candidates 中
                d["oos_status"] = "已上因子池"
                d["oos_metrics"] = tprog.get("oos_metrics", {})
            except Exception:
                d["oos_status"] = "已上因子池"
                d["oos_metrics"] = {}
            out.append(d)
        return out
    except Exception as e:
        import traceback
        print(f"[DB ERROR] get_all_candidates_detailed failed: {e}\n{traceback.format_exc()}")
        return []
    finally:
        conn.close()

# 【新增】管理員 SSH 直連 exec（完全複製 fetch_pg_dump.py 基礎設施：paramiko + docker exec psql）
# 確保管理員控制面板 API 的資料 100% 與 dump 腳本讀取到的資料一致，並擁有完整修改權限
# 最大化錯誤顯示：任何失敗都會印出完整 traceback + SSH 詳細錯誤
def admin_ssh_direct_exec(sql: str, params: tuple = None) -> int:
    """僅限 admin 使用。透過 SSH 隧道直接執行 psql（與 fetch_pg_dump.py 完全相同基礎設施），支援 INSERT/UPDATE/DELETE"""
    if not sql or not sql.strip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
        raise ValueError("僅允許修改類 SQL，禁止 SELECT 以防資料外洩")
    SSH_HOST = str(os.environ.get("SHEEP_ADMIN_SSH_HOST", "")).strip()
    SSH_USER = str(os.environ.get("SHEEP_ADMIN_SSH_USER", "")).strip()
    SSH_KEY_PATH = str(os.environ.get("SHEEP_ADMIN_SSH_KEY_PATH", "")).strip()
    REMOTE_WORK_DIR = str(os.environ.get("SHEEP_ADMIN_SSH_REMOTE_WORK_DIR", "/home/wm105020/repo/deploy")).strip()
    if not SSH_HOST or not SSH_USER or not SSH_KEY_PATH:
        raise RuntimeError(
            "Missing admin SSH settings. Set SHEEP_ADMIN_SSH_HOST, SHEEP_ADMIN_SSH_USER, and SHEEP_ADMIN_SSH_KEY_PATH."
        )
    try:
        import paramiko
        import traceback
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        key = paramiko.Ed25519Key.from_private_key_file(SSH_KEY_PATH)
        client.connect(hostname=SSH_HOST, username=SSH_USER, pkey=key, timeout=15)
        cmd = f'cd {REMOTE_WORK_DIR} && docker compose exec -T db psql -U sheep -d sheep -c "{sql}"'
        stdin, stdout, stderr = client.exec_command(cmd)
        exit_status = stdout.channel.recv_exit_status()
        out = stdout.read().decode("utf-8", errors="ignore").strip()
        err = stderr.read().decode("utf-8", errors="ignore").strip()
        client.close()
        if exit_status != 0:
            print(f"[ADMIN SSH EXEC ERROR] exit={exit_status} stdout={out} stderr={err}")
            raise RuntimeError(f"SSH exec 失敗: {err}")
        # 估計影響行數（psql \copy 回傳格式）
        affected = int(out.splitlines()[-1].strip().split()[-1]) if out else 0
        print(f"[ADMIN SSH EXEC SUCCESS] 影響 {affected} 行，SQL: {sql[:200]}...")
        return affected
    except Exception as e:
        print(f"[ADMIN SSH EXEC FATAL] {traceback.format_exc()}")
        raise

def get_admin_active_strategies() -> list:
    """專家級：供管理員面板讀取所有已過審且活躍的策略總覽"""
    conn = _conn()
    try:
        query = """
        SELECT st.id as strategy_id, st.status, st.allocation_pct, st.created_at, st.direction, st.params_json, st.external_key,
               u.username, u.nickname, u.avatar_url,
               p.name as pool_name, p.symbol, p.timeframe_min,
               c.metrics_json, c.score,
                t.progress_json
        FROM strategies st
        LEFT JOIN users u ON st.user_id = u.id
        LEFT JOIN factor_pools p ON st.pool_id = p.id
        LEFT JOIN submissions su ON st.submission_id = su.id
        LEFT JOIN candidates c ON su.candidate_id = c.id
        LEFT JOIN mining_tasks t ON c.task_id = t.id
        WHERE st.status = 'active'
        ORDER BY st.id DESC
        """
        rows = conn.execute(query).fetchall()
        out = []
        for r in rows:
            d = _normalize_strategy_row(r)
            try: d["metrics"] = json.loads(d.get("metrics_json") or "{}")
            except Exception: d["metrics"] = {}
            try: d["progress"] = json.loads(d.get("progress_json") or "{}")
            except Exception: d["progress"] = {}
            out.append(d)
        return out
    except Exception as e:
        import traceback
        print(f"[DB ERROR] get_admin_active_strategies failed: {e}\n{traceback.format_exc()}")
        return []
    finally:
        conn.close()


def get_admin_active_strategies_page(
    *,
    page: int = 1,
    page_size: int = 50,
    q: str = "",
    username: str = "",
    symbol: str = "",
    direction: str = "",
) -> dict:
    current_page = max(1, int(page or 1))
    size = max(1, min(200, int(page_size or 50)))
    offset = (current_page - 1) * size
    direction_value = normalize_direction(direction, default="")
    if direction_value not in {"long", "short"}:
        direction_value = ""

    where = ["st.status = 'active'"]
    params: List[Any] = []

    def _append_like(columns: List[str], value: str) -> None:
        text = str(value or "").strip().lower()
        if not text:
            return
        like_term = f"%{text}%"
        where.append("(" + " OR ".join([f"LOWER(COALESCE({col}, '')) LIKE ?" for col in columns]) + ")")
        params.extend([like_term] * len(columns))

    _append_like(
        ["u.username", "u.nickname", "p.name", "p.symbol", "st.external_key"],
        q,
    )
    _append_like(["u.username", "u.nickname"], username)
    _append_like(["p.symbol"], symbol)
    if direction_value:
        where.append("COALESCE(st.direction, 'long') = ?")
        params.append(direction_value)

    where_sql = " AND ".join(where)
    conn = _conn()
    try:
        total_row = conn.execute(
            f"""
            SELECT COUNT(*) as c
            FROM strategies st
            LEFT JOIN users u ON st.user_id = u.id
            LEFT JOIN factor_pools p ON st.pool_id = p.id
            WHERE {where_sql}
            """,
            params,
        ).fetchone()
        total = int((dict(total_row) if total_row is not None else {}).get("c") or 0)
        rows = conn.execute(
            f"""
            SELECT st.id as strategy_id, st.status, st.allocation_pct, st.created_at, st.direction, st.params_json, st.external_key,
                   u.id as owner_user_id, u.username, u.nickname, u.avatar_url,
                   p.name as pool_name, p.symbol, p.timeframe_min,
                   c.metrics_json, c.score,
                   t.progress_json
            FROM strategies st
            LEFT JOIN users u ON st.user_id = u.id
            LEFT JOIN factor_pools p ON st.pool_id = p.id
            LEFT JOIN submissions su ON st.submission_id = su.id
            LEFT JOIN candidates c ON su.candidate_id = c.id
            LEFT JOIN mining_tasks t ON c.task_id = t.id
            WHERE {where_sql}
            ORDER BY st.id DESC
            LIMIT ? OFFSET ?
            """,
            [*params, size, offset],
        ).fetchall()
        items = []
        for row in rows:
            item = _normalize_strategy_row(row)
            try:
                item["metrics"] = json.loads(item.get("metrics_json") or "{}")
            except Exception:
                item["metrics"] = {}
            try:
                item["progress"] = json.loads(item.get("progress_json") or "{}")
            except Exception:
                item["progress"] = {}
            items.append(item)
        return {
            "items": items,
            "total": total,
            "page": current_page,
            "page_size": size,
            "has_next": offset + len(items) < total,
        }
    finally:
        conn.close()


def list_active_strategy_runtime_rows(limit: int = 20000, runtime_items: Optional[List[Dict[str, Any]]] = None) -> list:
    conn = _conn()
    try:
        query = """
            SELECT st.id as strategy_id, st.status, st.allocation_pct, st.created_at, st.direction, st.params_json, st.external_key,
                   u.id as owner_user_id, u.username, u.nickname, u.avatar_url,
                   p.id as pool_id, p.name as pool_name, p.symbol, p.timeframe_min, p.family,
                   su.id as submission_id,
                   c.id as candidate_id, c.task_id, c.metrics_json, c.score,
                   (
                       SELECT wc.return_pct
                       FROM weekly_checks wc
                       WHERE wc.strategy_id = st.id
                       ORDER BY wc.checked_at DESC, wc.id DESC
                       LIMIT 1
                   ) as latest_return_pct,
                   (
                       SELECT wc.max_drawdown_pct
                       FROM weekly_checks wc
                       WHERE wc.strategy_id = st.id
                       ORDER BY wc.checked_at DESC, wc.id DESC
                       LIMIT 1
                   ) as latest_max_drawdown_pct
            FROM strategies st
            LEFT JOIN users u ON st.user_id = u.id
            LEFT JOIN factor_pools p ON st.pool_id = p.id
            LEFT JOIN submissions su ON st.submission_id = su.id
            LEFT JOIN candidates c ON su.candidate_id = c.id
            WHERE st.status = 'active'
        """
        params: List[Any] = []

        runtime_items = list(runtime_items or [])
        if runtime_items:
            external_keys: List[str] = []
            signature_filters: List[Tuple[str, str, int, str]] = []
            seen_keys: set[str] = set()
            seen_signatures: set[Tuple[str, str, int, str]] = set()
            for raw in runtime_items:
                item = dict(raw or {})
                entry = normalize_runtime_strategy_entry(
                    item.get("params_json") if isinstance(item.get("params_json"), dict) else item,
                    default_symbol=str(item.get("symbol") or ""),
                    default_interval=str(item.get("interval") or item.get("timeframe_min") or ""),
                )
                external_key = str(
                    item.get("external_key") or item.get("strategy_key") or entry.get("strategy_key") or ""
                ).strip()
                if external_key and external_key not in seen_keys:
                    seen_keys.add(external_key)
                    external_keys.append(external_key)
                family = str(entry.get("family") or item.get("family") or "").strip()
                symbol = str(entry.get("symbol") or item.get("symbol") or "").strip().upper()
                timeframe_min = int(_interval_to_minutes(entry.get("interval") or item.get("interval") or item.get("timeframe_min")))
                direction = normalize_direction(entry.get("direction") or item.get("direction"), default="long")
                signature = (family, symbol, timeframe_min, direction)
                if family and symbol and timeframe_min > 0 and signature not in seen_signatures:
                    seen_signatures.add(signature)
                    signature_filters.append(signature)

            where_parts: List[str] = []
            if external_keys:
                where_parts.append("st.external_key IN (" + ", ".join(["?"] * len(external_keys)) + ")")
                params.extend(external_keys)
            for family, symbol, timeframe_min, direction in signature_filters:
                where_parts.append(
                    "(COALESCE(p.family, '') = ? AND COALESCE(p.symbol, '') = ? AND COALESCE(p.timeframe_min, 0) = ? AND COALESCE(st.direction, 'long') = ?)"
                )
                params.extend([family, symbol, int(timeframe_min), direction])
            if not where_parts:
                return []
            query += " AND (" + " OR ".join(where_parts) + ")"
            limit = max(20, min(500, int(limit or 200)))

        query += """
            ORDER BY COALESCE(c.score, 0) DESC, st.created_at DESC, st.id DESC
            LIMIT ?
        """
        params.append(int(limit or 20000))
        rows = conn.execute(query, params).fetchall()
        default_avatar_url = _default_avatar_url_from_conn(conn)
        out = []
        for row in rows:
            item = _normalize_strategy_row(row)
            try:
                item["metrics"] = json.loads(item.get("metrics_json") or "{}")
            except Exception:
                item["metrics"] = {}
            if item.get("latest_return_pct") is not None and item["metrics"].get("total_return_pct") is None:
                item["metrics"]["total_return_pct"] = float(item.get("latest_return_pct") or 0.0)
            if item.get("latest_max_drawdown_pct") is not None and item["metrics"].get("max_drawdown_pct") is None:
                item["metrics"]["max_drawdown_pct"] = float(item.get("latest_max_drawdown_pct") or 0.0)
            user_bits = _decorate_user_row(
                {
                    "username": item.get("username"),
                    "nickname": item.get("nickname"),
                    "avatar_url": item.get("avatar_url"),
                },
                default_avatar_url=default_avatar_url,
            )
            item["nickname"] = user_bits.get("nickname")
            item["avatar_url"] = user_bits.get("avatar_url")
            item["display_name"] = user_bits.get("display_name")
            out.append(item)
        return out
    finally:
        conn.close()


def list_actionable_error_rows(limit: int = 2000) -> list:
    actionable_tokens = ("ERROR", "FAIL", "CRASH", "WARN", "REJECT", "ALARM", "DENIED", "MISMATCH")
    actionable_exact = {
        "CAPTCHA_GEN_ERROR",
        "LOGIN_CAPTCHA_FAIL",
        "REGISTER_CAPTCHA_FAIL",
        "UNKNOWN_ROUTE",
        "ADMIN_QUERY_EMPTY",
        "RUNTIME_SYNC_DEPRECATED_AUTH",
        "RUNTIME_SYNC_FAIL",
    }
    max_rows = max(1, min(10000, int(limit or 2000)))
    conn = _conn()
    try:
        report_rows: List[Dict[str, Any]] = []
        sys_rows = conn.execute(
            """
            SELECT created_at, user_id, event_type, message, detail_json
            FROM sys_monitor_events
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (max_rows,),
        ).fetchall()
        for row in sys_rows:
            event_type = str(row["event_type"] or "").strip()
            if event_type not in actionable_exact and not any(token in event_type for token in actionable_tokens):
                continue
            report_rows.append(
                {
                    "timestamp": str(row["created_at"] or ""),
                    "source": "sys_monitor",
                    "event_type": event_type,
                    "user_id": row["user_id"],
                    "worker_id": "",
                    "message": str(row["message"] or ""),
                    "detail_json": str(row["detail_json"] or "{}"),
                }
            )

        worker_rows = conn.execute(
            """
            SELECT ts, user_id, worker_id, event, detail_json
            FROM worker_events
            WHERE event IN ('task_finish_fail')
            ORDER BY ts DESC, id DESC
            LIMIT ?
            """,
            (max_rows,),
        ).fetchall()
        for row in worker_rows:
            report_rows.append(
                {
                    "timestamp": str(row["ts"] or ""),
                    "source": "worker",
                    "event_type": str(row["event"] or ""),
                    "user_id": row["user_id"],
                    "worker_id": str(row["worker_id"] or ""),
                    "message": str(row["event"] or ""),
                    "detail_json": str(row["detail_json"] or "{}"),
                }
            )
        report_rows.sort(key=lambda item: str(item.get("timestamp") or ""), reverse=True)
        return report_rows[:max_rows]
    finally:
        conn.close()
