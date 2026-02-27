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
from urllib.parse import urlparse, urlunparse
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

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


def _in_docker() -> bool:
    # 目的：判斷目前程式是否在 docker/container 裡，避免把 compose 專用的 host 規則套到本機直跑
    try:
        if os.path.exists("/.dockerenv"):
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

        host = hostport
        port = ""

        if hostport.startswith("["):
            m = re.match(r"^\[(?P<h>.+)\](?::(?P<p>\d+))?$", hostport)
            if m:
                host = str(m.group("h") or "")
                port = str(m.group("p") or "")
        else:
            if ":" in hostport:
                host, port = hostport.split(":", 1)

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
    # 主來源：SHEEP_DB_URL（你原本就用這個）:contentReference[oaicite:3]{index=3}
    u = str(os.environ.get("SHEEP_DB_URL", "") or "").strip()

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
    _is_db_conn = True

    def __init__(self, kind: str, raw):
        self.kind = str(kind)
        self._raw = raw
        self._closed = False

    def executescript(self, script: str) -> None:
        s = str(script or "")
        stmts = [x.strip() for x in s.split(";") if x.strip()]
        for st in stmts:
            self.execute(st)

    def execute(self, sql: str, params=None) -> _DBResult:
        q = str(sql or "")
        p = params

        if self.kind == "postgres":
            if psycopg2 is None:
                raise RuntimeError("psycopg2 not available but postgres URL is set")
            cur = self._raw.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                if p is None:
                    cur.execute(q)
                else:
                    # sqlite style '?' -> psycopg2 '%s'
                    q2 = q.replace("?", "%s")
                    cur.execute(q2, tuple(p))
                if cur.description is None:
                    rc = int(cur.rowcount or 0)
                    try:
                        cur.close()
                    except Exception:
                        pass
                    return _DBResult(None, rowcount=rc, lastrowid=0)
                return _DBResult(cur, rowcount=int(cur.rowcount or 0), lastrowid=0)
            except Exception:
                try:
                    cur.close()
                except Exception:
                    pass
                raise

        # sqlite
        cur = self._raw.cursor()
        try:
            if p is None:
                cur.execute(q)
            else:
                cur.execute(q, tuple(p))
            if cur.description is None:
                rc = int(cur.rowcount or 0)
                lr = int(getattr(cur, "lastrowid", 0) or 0)
                try:
                    cur.close()
                except Exception:
                    pass
                return _DBResult(None, rowcount=rc, lastrowid=lr)
            return _DBResult(cur, rowcount=int(cur.rowcount or 0), lastrowid=int(getattr(cur, "lastrowid", 0) or 0))
        except Exception:
            try:
                cur.close()
            except Exception:
                pass
            raise

    def commit(self) -> None:
        self._raw.commit()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        _release_conn(self.kind, self._raw)


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

        maxconn = 30
        try:
            maxconn = int(os.environ.get("SHEEP_PG_MAXCONN", "30") or "30")
        except Exception:
            maxconn = 30
        maxconn = max(5, min(200, int(maxconn)))

        # 建立候選 DSN（同一個密碼、同一個 DB，只改 host）
        candidates = []

        # 先把「override 後的 DSN」放最前面（_db_url 已處理 SHEEP_PG_HOST / 非 docker 時 host=db -> 127.0.0.1）
        candidates.append(url0)

        # 若仍然是 host=db，這代表：
        # - 你真的在 docker/compose 裡（通常 OK）
        # - 或你單獨跑 container / 本機跑但 override 沒設（通常會炸）
        try:
            p0 = urlparse(url0)
            host0 = str(p0.hostname or "")
        except Exception:
            host0 = ""

        # 額外 fallback：在 docker 內但解析不到 db，最常見就是你根本沒進 compose network
        if host0 == "db":
            if _in_docker():
                for h in ("host.docker.internal", "localhost"):
                    u1 = _rewrite_pg_host(url0, h)
                    if u1 not in candidates:
                        candidates.append(u1)
            else:
                # 本機直跑：127.0.0.1/localhost 都試一下（前面可能已經是 127.0.0.1，這裡再補齊）
                for h in ("127.0.0.1", "localhost"):
                    u1 = _rewrite_pg_host(url0, h)
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
                _PG_POOL = None
                continue

        # 全部候選都失敗：把「你需要的根因」完整印出來（但不洩漏密碼）
        try:
            import traceback as _tb
            tried = [_mask_dsn(x) for x in candidates]
            print("[DB FATAL] Postgres connection pool init failed.", file=_sys.stderr, flush=True)
            print(f"[DB FATAL] in_docker={_in_docker()} candidates={tried}", file=_sys.stderr, flush=True)
            print(f"[DB FATAL] hint: if you are running outside docker-compose, do NOT use host=db; set SHEEP_PG_HOST=localhost and publish 5432 if needed.", file=_sys.stderr, flush=True)
            print(_tb.format_exc(), file=_sys.stderr, flush=True)
        except Exception:
            pass

        raise RuntimeError(
            "Postgres 連線失敗：目前 DSN host 無法解析或無法連線。"
            "若你用 docker compose，請確認服務都在同一個 compose network（service 名稱 db 才會解析）。"
            "若你本機直跑 streamlit，請把 SHEEP_DB_URL 的 host 改成 localhost/127.0.0.1，或設定 SHEEP_PG_HOST=localhost。"
        ) from last_err


def _release_conn(kind: str, raw) -> None:
    if kind == "postgres":
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
        c = _pg_pool().getconn()
        try:
            c.autocommit = False
        except Exception:
            pass
        return _DBConn("postgres", c)

    # sqlite
    path = _db_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    except Exception as e:
        import traceback
        print(f"[DB ERROR] 無法建立資料庫目錄 {path}, 錯誤詳情: {e}\n{traceback.format_exc()}")

    raw = sqlite3.connect(path, timeout=30.0, check_same_thread=False)
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
        busy_ms = int(float(os.environ.get("SHEEP_SQLITE_BUSY_TIMEOUT_MS", "2000") or "2000"))
    except Exception:
        busy_ms = 2000
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

    return _DBConn("sqlite", raw)


def init_db() -> None:
    conn = _conn()
    if getattr(conn, "kind", "sqlite") == "postgres":
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id BIGSERIAL PRIMARY KEY,
                    username TEXT NOT NULL,
                    username_norm TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL DEFAULT 'user',
                    nickname TEXT DEFAULT '',
                    disabled INTEGER NOT NULL DEFAULT 0,
                    run_enabled INTEGER NOT NULL DEFAULT 1,
                    wallet_address TEXT NOT NULL DEFAULT '',
                    wallet_chain TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    last_login_at TEXT
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
                    symbol TEXT,
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

                CREATE TABLE IF NOT EXISTS candidates (
                    id BIGSERIAL PRIMARY KEY,
                    task_id BIGINT,
                    user_id BIGINT,
                    pool_id BIGINT,
                    params_json TEXT,
                    metrics_json TEXT,
                    score DOUBLE PRECISION,
                    is_submitted INTEGER DEFAULT 0,
                    created_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_candidates_created_at ON candidates(created_at);

                CREATE TABLE IF NOT EXISTS strategies (
                    id BIGSERIAL PRIMARY KEY,
                    submission_id BIGINT,
                    user_id BIGINT,
                    pool_id BIGINT,
                    params_json TEXT,
                    status TEXT DEFAULT 'active',
                    allocation_pct DOUBLE PRECISION,
                    note TEXT,
                    created_at TEXT,
                    expires_at TEXT
                );

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
                """
            )

            # ── 相容舊版 alembic schema：users 可能沒有 username_norm（你現在爆炸的根因）
            try:
                conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username_norm TEXT")
            except Exception:
                pass

            # 補齊既有資料（先用 lower(username) 當作 norm；可用就好，先救命）
            try:
                conn.execute("UPDATE users SET username_norm = lower(username) WHERE username_norm IS NULL OR username_norm = ''")
            except Exception:
                pass

            # 盡量加唯一索引；如果遇到大小寫重複導致建不成，也不能讓系統起不來
            try:
                conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS users_username_norm_uq ON users(username_norm)")
            except Exception:
                try:
                    conn.execute("CREATE INDEX IF NOT EXISTS users_username_norm_idx ON users(username_norm)")
                except Exception:
                    pass

            # ── 你 UI/結算/錢包流程會用到的欄位（舊 schema 可能也沒有）
            try:
                conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS nickname TEXT DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS disabled INTEGER NOT NULL DEFAULT 0")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS run_enabled INTEGER NOT NULL DEFAULT 1")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS wallet_address TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS wallet_chain TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass

            conn.commit()
            return
        finally:
            conn.close()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                username_norm TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                nickname TEXT DEFAULT '',
                disabled INTEGER NOT NULL DEFAULT 0,
                run_enabled INTEGER NOT NULL DEFAULT 1,
                wallet_address TEXT NOT NULL DEFAULT '',
                wallet_chain TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                last_login_at TEXT
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
                symbol TEXT,
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
            
            /* Indexes: 讓全域進度與派發查詢在資料量大時仍維持可用速度 */
            CREATE INDEX IF NOT EXISTS idx_mining_tasks_pool_cycle_part ON mining_tasks (pool_id, cycle_id, partition_idx);
            CREATE INDEX IF NOT EXISTS idx_mining_tasks_cycle_status ON mining_tasks (cycle_id, status);
            
            CREATE TABLE IF NOT EXISTS submissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidate_id INTEGER,
                user_id INTEGER,
                pool_id INTEGER,
                status TEXT DEFAULT 'pending',
                audit_json TEXT DEFAULT '{}',
                submitted_at TEXT
            );

            CREATE TABLE IF NOT EXISTS candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER,
                user_id INTEGER,
                pool_id INTEGER,
                params_json TEXT,
                metrics_json TEXT,
                score REAL,
                is_submitted INTEGER DEFAULT 0,
                created_at TEXT
            );
            
            CREATE TABLE IF NOT EXISTS strategies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                submission_id INTEGER,
                user_id INTEGER,
                pool_id INTEGER,
                params_json TEXT,
                status TEXT DEFAULT 'active',
                allocation_pct REAL,
                note TEXT,
                created_at TEXT,
                expires_at TEXT
            );

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
            """
        )
        try:
            conn.execute("ALTER TABLE users ADD COLUMN run_enabled INTEGER NOT NULL DEFAULT 1")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE users ADD COLUMN wallet_address TEXT NOT NULL DEFAULT ''")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE users ADD COLUMN wallet_chain TEXT NOT NULL DEFAULT ''")
        except Exception:
            pass
        # ── mining_tasks lease 欄位：你現在崩潰的根因就是它不存在
        try:
            conn.execute("ALTER TABLE mining_tasks ADD COLUMN lease_id TEXT")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE mining_tasks ADD COLUMN lease_worker_id TEXT")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE mining_tasks ADD COLUMN lease_expires_at TEXT")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE mining_tasks ADD COLUMN attempt INTEGER DEFAULT 0")
        except Exception:
            pass

        # ── compute worker 狀態面板必備表
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id TEXT PRIMARY KEY,
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
                );

                CREATE TABLE IF NOT EXISTS worker_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    worker_id TEXT,
                    event TEXT NOT NULL,
                    detail_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_workers_last_seen ON workers(last_seen_at);
                CREATE INDEX IF NOT EXISTS idx_worker_events_ts ON worker_events(ts);
                CREATE INDEX IF NOT EXISTS idx_mining_tasks_user_cycle_status_upd ON mining_tasks(user_id, cycle_id, status, updated_at);
                """
            )
        except Exception:
            pass

        # ── 任務 lease 欄位（舊 sqlite DB 也能自動補齊）
        try:
            conn.execute("ALTER TABLE mining_tasks ADD COLUMN lease_id TEXT")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE mining_tasks ADD COLUMN lease_worker_id TEXT")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE mining_tasks ADD COLUMN lease_expires_at TEXT")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE mining_tasks ADD COLUMN attempt INTEGER DEFAULT 0")
        except Exception:
            pass

        # ── workers / worker_events（compute worker 狀態面板必備）
        try:
            conn.executescript(
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
                );

                CREATE TABLE IF NOT EXISTS worker_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    user_id INTEGER,
                    worker_id TEXT,
                    event TEXT NOT NULL,
                    detail_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_workers_last_seen ON workers(last_seen_at);
                CREATE INDEX IF NOT EXISTS idx_worker_events_ts ON worker_events(ts);
                CREATE INDEX IF NOT EXISTS idx_worker_events_event_ts ON worker_events(event, ts);
                """
            )
        except Exception:
            pass

        # ── 任務查詢加速（對 tasks_live_query_ms 直接有效）
        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_mining_tasks_user_cycle_status_upd ON mining_tasks(user_id, cycle_id, status, updated_at)")
        except Exception:
            pass
        # mining_tasks lease columns (compute worker 必備)
        try:
            if getattr(conn, "kind", "sqlite") == "postgres":
                conn.execute("ALTER TABLE mining_tasks ADD COLUMN IF NOT EXISTS lease_id TEXT")
                conn.execute("ALTER TABLE mining_tasks ADD COLUMN IF NOT EXISTS lease_worker_id TEXT")
                conn.execute("ALTER TABLE mining_tasks ADD COLUMN IF NOT EXISTS lease_expires_at TEXT")
                conn.execute("ALTER TABLE mining_tasks ADD COLUMN IF NOT EXISTS attempt INTEGER DEFAULT 0")
            else:
                conn.execute("ALTER TABLE mining_tasks ADD COLUMN lease_id TEXT")
        except Exception:
            pass
        try:
            if getattr(conn, "kind", "sqlite") != "postgres":
                conn.execute("ALTER TABLE mining_tasks ADD COLUMN lease_worker_id TEXT")
        except Exception:
            pass
        try:
            if getattr(conn, "kind", "sqlite") != "postgres":
                conn.execute("ALTER TABLE mining_tasks ADD COLUMN lease_expires_at TEXT")
        except Exception:
            pass
        try:
            if getattr(conn, "kind", "sqlite") != "postgres":
                conn.execute("ALTER TABLE mining_tasks ADD COLUMN attempt INTEGER DEFAULT 0")
        except Exception:
            pass

        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_mining_tasks_status_lease ON mining_tasks(status, lease_expires_at)")
        except Exception:
            pass        
        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_mining_tasks_updated_status ON mining_tasks(updated_at, status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_candidates_created_at ON candidates(created_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_payouts_created_at ON payouts(created_at)")
        except Exception:
            pass

        conn.commit()
    finally:
        conn.close()


def get_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    raw = str(username or "")
    uname_norm = normalize_username(raw)
    if not uname_norm:
        return None

    conn = _conn()
    try:
        # 優先走 username_norm（新 schema / 已補欄位的 DB）
        try:
            row = conn.execute(
                "SELECT * FROM users WHERE username_norm = ? LIMIT 1",
                (uname_norm,),
            ).fetchone()
            if row:
                return dict(row)
        except Exception as e:
            # Postgres 舊 schema 沒 username_norm 會噴 UndefinedColumn (pgcode=42703)
            msg = str(e)
            pgcode = str(getattr(e, "pgcode", "") or "")
            if ("username_norm" not in msg) and (pgcode != "42703"):
                raise

        # 後備：直接用 username（大小寫不敏感）查，至少讓登入不炸
        row2 = conn.execute(
            "SELECT * FROM users WHERE lower(username) = ? OR username = ? LIMIT 1",
            (uname_norm, raw.strip()),
        ).fetchone()
        return dict(row2) if row2 else None
    finally:
        conn.close()


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    try:
        uid = int(user_id)
    except Exception:
        return None
    conn = _conn()
    try:
        row = conn.execute("SELECT * FROM users WHERE id = ? LIMIT 1", (uid,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def create_user(
    username: str,
    password_hash: str,
    role: str = "user",
    wallet_address: str = "",
    wallet_chain: str = "",
) -> int:
    uname = str(username or "").strip()
    uname_norm = normalize_username(uname)
    if not uname_norm:
        raise ValueError("invalid username")
        
    if isinstance(password_hash, bytes):
        pw_str = password_hash.decode("utf-8")
    else:
        pw_str = str(password_hash or "")
        
    conn = _conn()
    try:
        if getattr(conn, "kind", "sqlite") == "postgres":
            row = conn.execute(
                """
                INSERT INTO users (username, username_norm, password_hash, role, disabled, run_enabled, wallet_address, wallet_chain, created_at)
                VALUES (?, ?, ?, ?, 0, 1, ?, ?, ?)
                RETURNING id
                """,
                (uname, uname_norm, pw_str, str(role or "user"), str(wallet_address or ""), str(wallet_chain or ""), _now_iso()),
            ).fetchone()
            conn.commit()
            return int((row or {}).get("id") or 0)

        cur = conn.execute(
            """
            INSERT INTO users (username, username_norm, password_hash, role, disabled, run_enabled, wallet_address, wallet_chain, created_at)
            VALUES (?, ?, ?, ?, 0, 1, ?, ?, ?)
            """,
            (uname, uname_norm, pw_str, str(role or "user"), str(wallet_address or ""), str(wallet_chain or ""), _now_iso()),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def list_users(limit: int = 500) -> List[Dict[str, Any]]:
    conn = _conn()
    try:
        rows = conn.execute("SELECT * FROM users ORDER BY id DESC LIMIT ?", (int(limit),)).fetchall()
        return [dict(r) for r in rows]
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
    conn = _conn()
    try:
        if success:
            conn.execute("UPDATE users SET last_login_at = ? WHERE id = ?", (_now_iso(), int(user_id)))
            conn.commit()
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

    conn = _conn()
    try:
        conn.execute("UPDATE users SET run_enabled = ? WHERE id = ?", (1 if enabled else 0, uid))

        # 關閉時：回收該 user 所有 running 任務，避免浪費算力
        if not bool(enabled):
            now = _now_iso()

            # 舊 sqlite DB 可能沒有 lease 欄位：先嘗試補齊（不成功也不能炸）
            try:
                conn.execute("ALTER TABLE mining_tasks ADD COLUMN lease_id TEXT")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE mining_tasks ADD COLUMN lease_worker_id TEXT")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE mining_tasks ADD COLUMN lease_expires_at TEXT")
            except Exception:
                pass

            # 優先用「帶 lease 清除」的回收；若欄位仍不存在則 fallback
            try:
                conn.execute(
                    """
                    UPDATE mining_tasks
                    SET status='assigned',
                        lease_id=NULL,
                        lease_worker_id=NULL,
                        lease_expires_at=NULL,
                        updated_at=?
                    WHERE user_id=? AND status='running'
                    """,
                    (now, uid),
                )
            except Exception:
                conn.execute(
                    "UPDATE mining_tasks SET status='assigned', updated_at=? WHERE user_id=? AND status='running'",
                    (now, uid),
                )

        conn.commit()
    finally:
        conn.close()


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
        try:
            return json.loads(row.get("value"))
        except Exception:
            return row.get("value")
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
        try:
            return json.loads(row.get("value"))
        except Exception:
            return row.get("value")
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
    import secrets
    token = secrets.token_urlsafe(32)
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
    conn = _conn()
    try:
        if getattr(conn, "kind", "sqlite") == "postgres":
            row = conn.execute(
                "INSERT INTO api_tokens (user_id, token, name, expires_at, created_at) VALUES (?, ?, ?, ?, ?) RETURNING id",
                (user_id, token, name, expires_at, _now_iso())
            ).fetchone()
            conn.commit()
            return {"token_id": int((row or {}).get("id") or 0), "token": token, "expires_at": expires_at, "issued_at": _now_iso()}

        cur = conn.execute(
            "INSERT INTO api_tokens (user_id, token, name, expires_at, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, token, name, expires_at, _now_iso())
        )
        conn.commit()
        return {"token_id": cur.lastrowid, "token": token, "expires_at": expires_at, "issued_at": _now_iso()}
    except Exception as e:
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
                cur2 = conn.execute("INSERT INTO mining_cycles (name, status, start_ts, end_ts) VALUES (?, ?, ?, ?)",
                                (f"Cycle {active['id'] + 1}", "active", now_str, new_end))
                new_cycle_id = cur2.lastrowid
                
                try:
                    conn.execute("""
                        INSERT INTO factor_pools (
                            cycle_id, name, symbol, timeframe_min, years, family, 
                            grid_spec_json, risk_spec_json, num_partitions, seed, 
                            active, created_at
                        )
                        SELECT ?, name, symbol, timeframe_min, years, family, 
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
                except Exception as cycle_fatal:
                    print(f"[CRITICAL DB ERROR] 週期 Pool 繼承失敗: {cycle_fatal}")
                    
                conn.commit()
    except Exception:
        pass
    finally:
        conn.close()

def get_active_cycle() -> dict:
    conn = _conn()
    try:
        cur = conn.execute("SELECT id, name, status, start_ts, end_ts FROM mining_cycles WHERE status = 'active' ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        if row:
            return dict(row)
        return {}
    except Exception:
        return {}
    finally:
        conn.close()

def list_factor_pools(cycle_id: int) -> list:
    """專家級 Pool 檢索：具備自動修復與跨週期一致性檢查機制"""
    conn = _conn()
    try:
        cur = conn.execute("SELECT * FROM factor_pools WHERE cycle_id = ?", (int(cycle_id),))
        rows = [dict(row) for row in cur.fetchall()]
        
        # [主動除錯機制] 若偵測到新週期 Pool 遺失，執行深度聯集救援
        if not rows:
            # 尋找最近一個擁有 Pool 的週期
            last_p_cycle = conn.execute("SELECT cycle_id FROM factor_pools ORDER BY cycle_id DESC LIMIT 1").fetchone()
            if last_p_cycle and last_p_cycle["cycle_id"] != cycle_id:
                source_cid = last_p_cycle["cycle_id"]
                print(f"[DB MAINTENANCE] 偵測到週期 {cycle_id} 缺乏 Pool 資料，啟動從週期 {source_cid} 繼承程序...")
                try:
                    conn.execute("""
                        INSERT INTO factor_pools (cycle_id, name, symbol, timeframe_min, years, family, grid_spec_json, risk_spec_json, num_partitions, seed, active, created_at)
                        SELECT ?, name, symbol, timeframe_min, years, family, grid_spec_json, risk_spec_json, num_partitions, seed, active, ?
                        FROM factor_pools WHERE cycle_id = ? AND active = 1
                    """, (cycle_id, _now_iso(), source_cid))
                    conn.commit()
                    cur = conn.execute("SELECT * FROM factor_pools WHERE cycle_id = ?", (cycle_id,))
                    rows = [dict(row) for row in cur.fetchall()]
                except Exception as rescue_e:
                    import traceback
                    print(f"[FATAL DB ERROR] Pool 跨週期繼承失敗: {rescue_e}\n{traceback.format_exc()}")
        
        return rows
    except Exception as e:
        import traceback
        print(f"[DB ERROR] list_factor_pools 執行異常: {e}\n{traceback.format_exc()}")
        return []
    finally:
        conn.close()

def assign_tasks_for_user(user_id: int, cycle_id: int = 0, min_tasks: int = 2, max_tasks: int = 6, preferred_family: str = "") -> None:
    import time
    for attempt in range(5):
        try:
            conn = _conn()
            try:
                if cycle_id <= 0:
                    cycle_row = conn.execute("SELECT id FROM mining_cycles WHERE status = 'active' ORDER BY id DESC LIMIT 1").fetchone()
                    if not cycle_row: return
                    cycle_id = cycle_row["id"]
                
                cur = conn.execute("SELECT COUNT(*) as c FROM mining_tasks WHERE user_id = ? AND status IN ('assigned', 'running', 'queued') AND cycle_id = ?", (user_id, cycle_id))
                current_tasks = cur.fetchone()["c"]
                
                if current_tasks < min_tasks:
                    needed = min_tasks - current_tasks
                    pools_query = "SELECT id, num_partitions FROM factor_pools WHERE cycle_id = ? AND active = 1"
                    params = [cycle_id]
                    if preferred_family:
                        pools_query += " AND family = ?"
                        params.append(preferred_family)
                    
                    pools = conn.execute(pools_query, params).fetchall()
                    
                    if not pools and preferred_family:
                        pools = conn.execute("SELECT id, num_partitions FROM factor_pools WHERE cycle_id = ? AND active = 1", (cycle_id,)).fetchall()
                        
                    for _ in range(needed):
                        if pools:
                            import random
                            p = random.choice(pools)
                            part_idx = random.randint(0, max(0, int(p["num_partitions"]) - 1))
                            # 避免同一個 user 在同一個 pool/cycle 反覆拿到同分割，減少重複與資料膨脹
                            conn.execute(
                                """
                                INSERT INTO mining_tasks (user_id, pool_id, cycle_id, partition_idx, num_partitions, status, created_at, updated_at)
                                SELECT ?, ?, ?, ?, ?, 'assigned', ?, ?
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM mining_tasks
                                    WHERE user_id = ? AND pool_id = ? AND cycle_id = ? AND partition_idx = ?
                                    AND status IN ('assigned','queued','running')
                                )
                                """,
                                (user_id, int(p["id"]), cycle_id, int(part_idx), int(p["num_partitions"]), _now_iso(), _now_iso(),
                                 user_id, int(p["id"]), cycle_id, int(part_idx)),
                            )
                    conn.commit()
                break
            finally:
                conn.close()
        except Exception as e:
            if attempt == 4:
                print(f"[DB ERROR] assign_tasks_for_user: {e}")
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
def list_tasks_for_user(user_id: int, cycle_id: int = 0) -> list:
    conn = _conn()
    try:
        if cycle_id > 0:
            cur = conn.execute("SELECT t.*, p.name as pool_name, p.symbol, p.timeframe_min, p.family FROM mining_tasks t LEFT JOIN factor_pools p ON t.pool_id = p.id WHERE t.user_id = ? AND t.cycle_id = ?", (user_id, cycle_id))
        else:
            cur = conn.execute("SELECT t.*, p.name as pool_name, p.symbol, p.timeframe_min, p.family FROM mining_tasks t LEFT JOIN factor_pools p ON t.pool_id = p.id WHERE t.user_id = ?", (user_id,))
        return [dict(row) for row in cur.fetchall()]
    except Exception as e:
        print(f"[DB ERROR] list_tasks_for_user: {e}")
        return []
    finally:
        conn.close()

def list_submissions(user_id: int = 0, status: str = "", limit: int = 300) -> list:
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
    except Exception as e:
        print(f"[DB ERROR] list_submissions: {e}")
        return []
    finally:
        conn.close()

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
    conn = _conn()
    try:
        query = "SELECT s.*, u.username, p.name as pool_name, p.symbol, p.timeframe_min, p.family FROM strategies s LEFT JOIN users u ON s.user_id = u.id LEFT JOIN factor_pools p ON s.pool_id = p.id WHERE 1=1"
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
    except Exception as e:
        print(f"[DB ERROR] list_strategies: {e}")
        return []
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
        rows = [dict(row) for row in cur.fetchall()]
        for r in rows:
            r["params_json"] = json.loads(r.get("params_json") or "{}")
            r["metrics"] = json.loads(r.get("metrics_json") or "{}")
        return rows
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
        d = dict(row)
        d["grid_spec"] = json.loads(d.get("grid_spec_json") or "{}")
        d["risk_spec"] = json.loads(d.get("risk_spec_json") or "{}")
        return d
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
        if not allowed_pool_ids:
            conn.execute("UPDATE mining_tasks SET status = 'queued' WHERE user_id = ? AND cycle_id = ? AND status = 'assigned'", (user_id, cycle_id))
        else:
            placeholders = ",".join("?" for _ in allowed_pool_ids)
            query = f"UPDATE mining_tasks SET status = 'queued' WHERE user_id = ? AND cycle_id = ? AND status = 'assigned' AND pool_id NOT IN ({placeholders})"
            params = [user_id, cycle_id] + allowed_pool_ids
            conn.execute(query, params)
        conn.commit()
    except Exception as e:
        print(f"[DB ERROR] release_assigned_tasks_for_user: {e}")
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

def create_factor_pool(cycle_id: int, name: str, symbol: str, timeframe_min: int, years: int, family: str, grid_spec: dict, risk_spec: dict, num_partitions: int, seed: int, active: bool, auto_expand: bool = False) -> list:
    """專家級 Pool 建立器：支援 14 種組合自動擴展功能"""
    ids = []
    targets = [(symbol, timeframe_min)]
    if auto_expand:
        # 管理員勾選最大化範圍：自動生成 BTC/ETH 與 7 種 Timeframe
        symbols = ["BTC_USDT", "ETH_USDT"]
        tfs = [1, 5, 15, 30, 60, 240, 1440]
        targets = [(s, t) for s in symbols for t in tfs]

    conn = _conn()
    try:
        for s, t in targets:
            # 修正：精準檢查是否已存在於該週期，避免重複建立導致任務派發混亂
            exist = conn.execute("SELECT id FROM factor_pools WHERE cycle_id=? AND symbol=? AND timeframe_min=? AND family=?", (cycle_id, s, t, family)).fetchone()
            if exist:
                ids.append(exist["id"])
                continue

            expanded_name = f"{name} [{s}_{t}m]" if auto_expand else name
            cur = conn.execute(
                """
                INSERT INTO factor_pools (cycle_id, name, symbol, timeframe_min, years, family, grid_spec_json, risk_spec_json, num_partitions, seed, active, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (cycle_id, expanded_name, s, t, int(years), family, json.dumps(grid_spec, ensure_ascii=False), json.dumps(risk_spec, ensure_ascii=False), int(num_partitions), int(seed), 1 if active else 0, _now_iso())
            )
            ids.append(cur.lastrowid)
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

def update_factor_pool(pool_id: int, name: str, symbol: str, timeframe_min: int, years: int, family: str, grid_spec: dict, risk_spec: dict, num_partitions: int, seed: int, active: bool) -> None:
    conn = _conn()
    try:
        conn.execute(
            """
            UPDATE factor_pools
            SET name=?, symbol=?, timeframe_min=?, years=?, family=?, grid_spec_json=?, risk_spec_json=?, num_partitions=?, seed=?, active=?
            WHERE id=?
            """,
            (name, symbol, timeframe_min, years, family, json.dumps(grid_spec, ensure_ascii=False), json.dumps(risk_spec, ensure_ascii=False), num_partitions, seed, 1 if active else 0, pool_id)
        )
        conn.commit()
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
        cand = conn.execute("SELECT params_json FROM candidates WHERE id = ?", (sub["candidate_id"],)).fetchone()
        params = cand["params_json"] if cand else "{}"
        
        cur = conn.execute(
            "INSERT INTO strategies (submission_id, user_id, pool_id, params_json, status, allocation_pct, note, created_at, expires_at) VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?)",
            (sub_id, sub["user_id"], sub["pool_id"], params, allocation_pct, note, _now_iso(), "2099-12-31T23:59:59Z")
        )
        conn.commit()
        return cur.lastrowid
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
        d = dict(row)
        d["params_json"] = json.loads(d.get("params_json") or "{}")
        return d
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
    for attempt in range(5):
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
            if attempt == 4:
                print(f"[DB ERROR] update_task_progress 放棄重試: {e}")
                raise e
            time.sleep(0.05 * (2 ** attempt))

def update_task_status(task_id: int, status: str, finished: bool = False) -> None:
    for attempt in range(5):
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
            if attempt == 4:
                print(f"[DB ERROR] update_task_status 放棄重試: {e}")
                raise e
            time.sleep(0.05 * (2 ** attempt))

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
                if getattr(conn, "kind", "sqlite") == "postgres":
                    row = conn.execute(
                        "INSERT INTO candidates (task_id, user_id, pool_id, params_json, metrics_json, score, created_at) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id",
                        (task_id, user_id, pool_id, json.dumps(params, ensure_ascii=False), json.dumps(metrics, ensure_ascii=False), score, _now_iso())
                    ).fetchone()
                    conn.commit()
                    return int((row or {}).get("id") or 0)

                cur = conn.execute(
                    "INSERT INTO candidates (task_id, user_id, pool_id, params_json, metrics_json, score, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (task_id, user_id, pool_id, json.dumps(params, ensure_ascii=False), json.dumps(metrics, ensure_ascii=False), score, _now_iso())
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
        cur = conn.execute("INSERT INTO submissions (candidate_id, user_id, pool_id, audit_json, submitted_at) VALUES (?, ?, ?, ?, ?)",
                           (candidate_id, user_id, pool_id, json.dumps(audit, ensure_ascii=False), _now_iso()))
        conn.execute("UPDATE candidates SET is_submitted = 1 WHERE id = ?", (candidate_id,))
        conn.commit()
        return cur.lastrowid
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
        # 確保表存在（就算 init_db 沒跑到也不會炸）
        try:
            conn.executescript(
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
                );
                CREATE TABLE IF NOT EXISTS worker_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    user_id INTEGER,
                    worker_id TEXT,
                    event TEXT NOT NULL,
                    detail_json TEXT NOT NULL DEFAULT '{}'
                );
                """
            )
        except Exception:
            pass

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
        
        rows = conn.execute(
            "SELECT id, progress_json FROM mining_tasks WHERE status = 'running' AND last_heartbeat < ?", 
            (cutoff_iso,)
        ).fetchall()
        
        for row in rows:
            tid = row["id"]
            try:
                prog = json.loads(row["progress_json"] or "{}")
            except Exception:
                prog = {}
            prog["phase"] = "queued"
            prog["phase_msg"] = "任務因超時或節點斷線，已由系統自動回收並等待重新分配。"
            prog["last_error"] = "執行超時系統強制回收"
            prog["updated_at"] = _now_iso()
            
            conn.execute(
                "UPDATE mining_tasks SET status = 'assigned', updated_at = ?, progress_json = ? WHERE id = ?", 
                (_now_iso(), json.dumps(prog, ensure_ascii=False), tid)
            )
            count += 1
            
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
        raw = str(nickname or "").strip()
        safe_nick = html.escape(raw[:10])
        
        conn.execute("UPDATE users SET nickname = ? WHERE id = ?", (safe_nick, int(user_id)))
        conn.commit()
    except Exception as e:
        print(f"[DB ERROR] update_user_nickname: {e}")
        raise
    finally:
        conn.close()

def get_leaderboard_stats(period_hours: int = 720) -> dict:
    """
    專家級聚合查詢：一次性撈取 排行榜所需的所有維度數據。
    period_hours: 1 (1h), 24 (24h), 720 (30d)
    """
    conn = _conn()
    try:
        # 計算時間視窗
        now_dt = datetime.now(timezone.utc)
        cutoff_dt = now_dt - timedelta(hours=period_hours)
        cutoff_iso = cutoff_dt.isoformat()

        results = {
            "combos": [],   # 貢獻組合數 (勤勞度)
            "score": [],    # 最高分 (運氣/實力)
            "time": [],     # 貢獻時長 (掛機時間)
            "points": []    # 積分 (已獲利)
        }

        # 1. 總已跑組合數 (Total Combos Done)
        # 解析 progress_json 消耗較大，改用 SQL 內的簡單字串擷取或假設應用層已寫入 (這裡使用近似統計以保效能)
        # 正規做法應在 mining_tasks 增加 done 欄位，這裡使用應用層相容做法：
        # 統計該時段內 updated_at 的任務，並累加 combos_done
        # 為了效能，我們只統計 status='completed' 或 'running'
        # 注意：SQLite JSON 函數需 json1 extension，大部分環境有。若無則退回計數。
        
        if _db_kind() == "postgres":
            sql_combos = """
                SELECT u.username, u.nickname, COUNT(t.id) as task_count,
                       SUM(COALESCE((t.progress_json::jsonb->>'combos_done')::bigint, 0)) as total_done
                FROM mining_tasks t
                JOIN users u ON t.user_id = u.id
                WHERE t.updated_at >= ? AND t.updated_at <= ?
                GROUP BY u.id, u.username, u.nickname
                ORDER BY total_done DESC
                LIMIT 50
            """
        else:
            sql_combos = """
                SELECT u.username, u.nickname, COUNT(t.id) as task_count, 
                       SUM(CAST(json_extract(t.progress_json, '$.combos_done') AS INTEGER)) as total_done
                FROM mining_tasks t
                JOIN users u ON t.user_id = u.id
                WHERE t.updated_at >= ? AND t.updated_at <= ?
                GROUP BY u.id
                ORDER BY total_done DESC
                LIMIT 50
            """
        
        try:
            rows = conn.execute(sql_combos, (cutoff_iso, _now_iso())).fetchall()
            # [專家修復] 強制轉型，避免 None 導致比較錯誤
            results["combos"] = [dict(r) for r in rows if r["total_done"] is not None and int(r["total_done"]) > 0]
        except Exception as e:
            print(f"[DB WARN] Leaderboard combos query failed: {e}")
            results["combos"] = []

        # 2. 最高分 (Highest Score) - 從 candidates 表
        sql_score = """
            SELECT u.username, u.nickname, MAX(c.score) as max_score
            FROM candidates c
            JOIN users u ON c.user_id = u.id
            WHERE c.created_at >= ?
            GROUP BY u.id
            ORDER BY max_score DESC
            LIMIT 50
        """
        try:
            rows = conn.execute(sql_score, (cutoff_iso,)).fetchall()
            results["score"] = [dict(r) for r in rows if r["max_score"] is not None]
        except Exception:
            results["score"] = []

        # 3. 總挖礦時長 (Mining Time) - 近似值：SUM(elapsed_s)
        sql_time = """
            SELECT u.username, u.nickname, 
                   SUM(CAST(json_extract(t.progress_json, '$.elapsed_s') AS REAL)) as total_seconds
            FROM mining_tasks t
            JOIN users u ON t.user_id = u.id
            WHERE t.updated_at >= ? AND t.status IN ('completed', 'running')
            GROUP BY u.id
            ORDER BY total_seconds DESC
            LIMIT 50
        """
        try:
            rows = conn.execute(sql_time, (cutoff_iso,)).fetchall()
            # [專家修復] 強制轉型 check
            results["time"] = [dict(r) for r in rows if r["total_seconds"] is not None and float(r["total_seconds"]) > 0]
        except Exception:
            results["time"] = []

        # 4. 積分 (Points/USDT) - 從 payouts 表
        # 注意：payouts 通常是週結，短週期可能無數據
        sql_points = """
            SELECT u.username, u.nickname, SUM(p.amount_usdt) as total_usdt
            FROM payouts p
            JOIN users u ON p.user_id = u.id
            WHERE p.created_at >= ?
            GROUP BY u.id
            ORDER BY total_usdt DESC
            LIMIT 50
        """
        rows = conn.execute(sql_points, (cutoff_iso,)).fetchall()
        results["points"] = [dict(r) for r in rows if r["total_usdt"] and r["total_usdt"] > 0]

        return results
    except Exception as e:
        print(f"[DB ERROR] get_leaderboard_stats: {e}")
        return {"combos": [], "score": [], "time": [], "points": []}
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

def _reap_expired_running(conn: _DBConn) -> int:
    now = _utc_now_iso()
    try:
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
                ORDER BY COALESCE(t.updated_at, t.created_at) ASC, t.id ASC
                FOR UPDATE SKIP LOCKED
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
                ORDER BY COALESCE(t.updated_at, t.created_at) ASC, t.id ASC
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

        if getattr(conn, "kind", "sqlite") == "postgres":
            row = conn.execute(
                """
                SELECT t.id
                FROM mining_tasks t
                JOIN users u ON u.id = t.user_id
                JOIN factor_pools p ON p.id = t.pool_id
                WHERE t.user_id=?
                  AND t.status IN ('assigned','queued')
                  AND COALESCE(u.disabled,0)=0
                  AND COALESCE(u.run_enabled,1)=1
                  AND COALESCE(p.active,1)=1
                ORDER BY COALESCE(t.updated_at, t.created_at) ASC, t.id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """,
                (uid,),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT t.id
                FROM mining_tasks t
                JOIN users u ON u.id = t.user_id
                JOIN factor_pools p ON p.id = t.pool_id
                WHERE t.user_id=?
                  AND t.status IN ('assigned','queued')
                  AND COALESCE(u.disabled,0)=0
                  AND COALESCE(u.run_enabled,1)=1
                  AND COALESCE(p.active,1)=1
                ORDER BY COALESCE(t.updated_at, t.created_at) ASC, t.id ASC
                LIMIT 1
                """,
                (uid,),
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
            row = conn.execute(
                "INSERT INTO candidates (task_id, user_id, pool_id, params_json, metrics_json, score, created_at) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id"
                if getattr(conn, "kind", "sqlite") == "postgres"
                else "INSERT INTO candidates (task_id, user_id, pool_id, params_json, metrics_json, score, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (tid, owner_id, pool_id, json.dumps(pr, ensure_ascii=False), json.dumps(me, ensure_ascii=False), sc, now),
            )
            if best_candidate_id is None:
                if getattr(conn, "kind", "sqlite") == "postgres":
                    rid = (row.fetchone() or {}).get("id")
                    best_candidate_id = int(rid or 0) if rid else None
                else:
                    best_candidate_id = int(getattr(row, "lastrowid", 0) or 0)

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

# 停用 run_enabled 時，直接回收該 user 的 running 任務（避免浪費算力）
def set_user_run_enabled(user_id: int, enabled: bool) -> None:
    conn = _conn()
    try:
        conn.execute("UPDATE users SET run_enabled = ? WHERE id = ?", (1 if enabled else 0, int(user_id)))
        if not bool(enabled):
            now = _utc_now_iso()
            # 回收 lease，讓 compute worker 立刻停手（progress 會 409 然後 release）
            conn.execute(
                """
                UPDATE mining_tasks
                SET status='assigned',
                    lease_id=NULL, lease_worker_id=NULL, lease_expires_at=NULL,
                    updated_at=?
                WHERE user_id=? AND status='running'
                """,
                (now, int(user_id)),
            )
        conn.commit()
    finally:
        conn.close()