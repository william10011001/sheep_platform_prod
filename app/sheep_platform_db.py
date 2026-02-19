import os
import json
import sqlite3
import threading
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from filelock import FileLock

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:
    psycopg = None
    dict_row = None

from sheep_platform_security import utc_now_iso, encrypt_text, decrypt_text, json_dumps, get_hmac_key, random_token, stable_hmac_sha256, hash_password


DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "sheep_platform.db"
DB_LOCK = DATA_DIR / "sheep_platform.db.lock"

DB_URL = (os.environ.get("SHEEP_DB_URL", "") or os.environ.get("DATABASE_URL", "") or "").strip()
_DB_KIND = "postgres" if DB_URL.lower().startswith(("postgresql://", "postgres://")) else "sqlite"

DB_MIGRATION = (os.environ.get("SHEEP_DB_MIGRATION", "") or "").strip().lower()
if not DB_MIGRATION:
    DB_MIGRATION = "alembic" if _DB_KIND == "postgres" else "legacy"



def get_db_info() -> Dict[str, Any]:
    if _DB_KIND == "postgres":
        return {"kind": "postgres", "url": DB_URL}
    return {"kind": "sqlite", "path": str(DB_PATH)}


def _qmark_to_ps(sql: str) -> str:
    return sql.replace("?", "%s")


class _DBConnWrapper:
    def __init__(self, raw_conn: Any, kind: str):
        self._c = raw_conn
        self._kind = kind

    def execute(self, sql: str, params: Tuple[Any, ...] = ()) -> Any:
        if self._kind == "postgres":
            sql = _qmark_to_ps(sql)
        return self._c.execute(sql, params)

    def executescript(self, script: str) -> None:
        if self._kind == "sqlite":
            self._c.executescript(script)
            return
        parts = [p.strip() for p in script.split(";") if p.strip()]
        for stmt in parts:
            self._c.execute(stmt)

    def commit(self) -> None:
        self._c.commit()

    def close(self) -> None:
        self._c.close()

    def __enter__(self) -> "_DBConnWrapper":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is None:
                self._c.commit()
        finally:
            self._c.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._c, name)


def _insert_id(conn: Any, sql: str, params: Tuple[Any, ...]) -> int:
    if _DB_KIND == "postgres":
        sql2 = sql.strip().rstrip(";")
        if "returning" not in sql2.lower():
            sql2 = sql2 + " RETURNING id"
        cur = conn.execute(sql2, params)
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("insert_failed")
        if isinstance(row, dict):
            return int(row.get("id"))
        return int(row[0])
    cur = conn.execute(sql, params)
    return int(cur.lastrowid)

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _cleanup_stale_running_tasks(conn: Any, cycle_id: int, lease_seconds: int) -> int:
    lease_seconds = max(30, int(lease_seconds))
    cutoff = _utc_now() - timedelta(seconds=lease_seconds)
    cutoff_iso = _iso(cutoff)
    now_iso = utc_now_iso()

    system_uid = _get_or_create_system_user_id(conn)

    cur = conn.execute(
        "UPDATE tasks SET status = 'assigned', user_id = ?, started_at = NULL, finished_at = NULL, lease_id = NULL, lease_worker_id = NULL, lease_expires_at = NULL, last_heartbeat = ? "
        "WHERE cycle_id = ? AND status = 'running' AND (last_heartbeat IS NULL OR last_heartbeat < ?)",
        (int(system_uid), str(now_iso), int(cycle_id), str(cutoff_iso)),
    )
    return int(cur.rowcount or 0)


def _cleanup_stale_assigned_tasks(conn: Any, cycle_id: int, reserve_seconds: int) -> int:
    """Release long-idle reserved tasks back to system.

    A reserved task is: status='assigned' and user_id != system.
    If it stays reserved for too long without moving to running, it blocks the pool.
    """
    reserve_seconds = max(30, int(reserve_seconds))
    cutoff = _utc_now() - timedelta(seconds=reserve_seconds)
    cutoff_iso = _iso(cutoff)
    now_iso = utc_now_iso()

    system_uid = _get_or_create_system_user_id(conn)

    cur = conn.execute(
        "UPDATE tasks SET user_id = ?, assigned_at = ?, last_heartbeat = ? "
        "WHERE cycle_id = ? AND status = 'assigned' AND user_id <> ? AND (assigned_at IS NULL OR assigned_at < ?)",
        (int(system_uid), str(now_iso), str(now_iso), int(cycle_id), int(system_uid), str(cutoff_iso)),
    )
    return int(cur.rowcount or 0)


def _conn() -> Any:
    if _DB_KIND == "postgres":
        if psycopg is None:
            raise RuntimeError("psycopg_not_installed")
        raw = psycopg.connect(DB_URL, row_factory=dict_row)
        return _DBConnWrapper(raw, "postgres")

    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return _DBConnWrapper(conn, "sqlite")


def init_db() -> None:
    if _DB_KIND == "postgres":
        if DB_MIGRATION == "legacy":
            _init_db_postgres()
            return

        # Alembic-managed schema: we do NOT run any CREATE TABLE / ALTER TABLE here.
        # If schema isn't present, fail fast with a clear error.
        conn = _conn()
        try:
            try:
                conn.execute("SELECT 1 FROM users LIMIT 1").fetchone()
                conn.execute("SELECT 1 FROM settings LIMIT 1").fetchone()
            except Exception as e:
                raise RuntimeError("db_schema_missing: run `alembic upgrade head` before starting the app") from e

            _ensure_user_run_enabled_column(conn)
            _ensure_wallet_chain_column(conn)
            _init_defaults(conn)
            _get_or_create_system_user_id(conn)
            conn.commit()
        finally:
            conn.close()
        return
    with FileLock(str(DB_LOCK)):
        conn = _conn()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL CHECK(role IN ('user','admin')),
                    wallet_chain TEXT NOT NULL DEFAULT 'TRC20',
                    wallet_address_enc BLOB,
                    created_at TEXT NOT NULL,
                    last_login_at TEXT,
                    disabled INTEGER NOT NULL DEFAULT 0,
                    login_fail_count INTEGER NOT NULL DEFAULT 0,
                    lock_until TEXT
                );

                CREATE TABLE IF NOT EXISTS cycles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    start_ts TEXT NOT NULL,
                    end_ts TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    seed INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS factor_pools (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cycle_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timeframe_min INTEGER NOT NULL,
                    years INTEGER NOT NULL,
                    family TEXT NOT NULL,
                    grid_spec_json TEXT NOT NULL,
                    risk_spec_json TEXT NOT NULL,
                    num_partitions INTEGER NOT NULL,
                    seed INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1,
                    FOREIGN KEY(cycle_id) REFERENCES cycles(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cycle_id INTEGER NOT NULL,
                    pool_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    partition_idx INTEGER NOT NULL,
                    partition_total INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('assigned','running','completed','expired','revoked')),
                    assigned_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    last_heartbeat TEXT,
                    lease_id TEXT,
                    lease_worker_id TEXT,
                    lease_expires_at TEXT,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    estimated_combos INTEGER NOT NULL DEFAULT 0,
                    priority INTEGER NOT NULL DEFAULT 0,
                    progress_json TEXT NOT NULL,
                    UNIQUE(cycle_id, pool_id, partition_idx),
                    FOREIGN KEY(cycle_id) REFERENCES cycles(id) ON DELETE CASCADE,
                    FOREIGN KEY(pool_id) REFERENCES factor_pools(id) ON DELETE CASCADE,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS candidates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    pool_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    params_enc BLOB NOT NULL,
                    metrics_json TEXT NOT NULL,
                    score REAL NOT NULL,
                    is_submitted INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY(pool_id) REFERENCES factor_pools(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS submissions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    candidate_id INTEGER NOT NULL UNIQUE,
                    user_id INTEGER NOT NULL,
                    pool_id INTEGER NOT NULL,
                    submitted_at TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('pending','approved','rejected')),
                    audit_json TEXT,
                    approved_at TEXT,
                    approved_by INTEGER,
                    FOREIGN KEY(candidate_id) REFERENCES candidates(id) ON DELETE CASCADE,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY(pool_id) REFERENCES factor_pools(id) ON DELETE CASCADE,
                    FOREIGN KEY(approved_by) REFERENCES users(id)
                );

                CREATE TABLE IF NOT EXISTS strategies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    submission_id INTEGER NOT NULL UNIQUE,
                    user_id INTEGER NOT NULL,
                    pool_id INTEGER NOT NULL,
                    cycle_id INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('active','disqualified','expired','paused')),
                    activated_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    allocation_pct REAL NOT NULL,
                    note TEXT,
                    FOREIGN KEY(submission_id) REFERENCES submissions(id) ON DELETE CASCADE,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY(pool_id) REFERENCES factor_pools(id) ON DELETE CASCADE,
                    FOREIGN KEY(cycle_id) REFERENCES cycles(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS weekly_checks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id INTEGER NOT NULL,
                    week_start_ts TEXT NOT NULL,
                    week_end_ts TEXT NOT NULL,
                    computed_at TEXT NOT NULL,
                    return_pct REAL NOT NULL,
                    max_drawdown_pct REAL NOT NULL,
                    trades INTEGER NOT NULL,
                    eligible INTEGER NOT NULL,
                    UNIQUE(strategy_id, week_start_ts),
                    FOREIGN KEY(strategy_id) REFERENCES strategies(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS payouts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    week_start_ts TEXT NOT NULL,
                    amount_usdt REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('unpaid','paid','void')),
                    paid_at TEXT,
                    txid TEXT,
                    FOREIGN KEY(strategy_id) REFERENCES strategies(id) ON DELETE CASCADE,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    actor_user_id INTEGER,
                    action TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    FOREIGN KEY(actor_user_id) REFERENCES users(id)
                );

                
                CREATE TABLE IF NOT EXISTS api_tokens (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    token_hash TEXT NOT NULL UNIQUE,
                    issued_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    revoked_at TEXT,
                    last_seen_at TEXT,
                    last_ip TEXT,
                    last_user_agent TEXT,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS workers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    worker_id TEXT NOT NULL UNIQUE,
                    user_id INTEGER NOT NULL,
                    name TEXT,
                    version TEXT NOT NULL,
                    protocol INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    last_heartbeat_at TEXT NOT NULL,
                    last_task_id INTEGER,
                    avg_cps REAL NOT NULL DEFAULT 0.0,
                    meta_json TEXT NOT NULL,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS worker_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    user_id INTEGER,
                    worker_id TEXT,
                    event TEXT NOT NULL,
                    detail_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS api_request_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    user_id INTEGER,
                    worker_id TEXT,
                    token_id INTEGER,
                    method TEXT NOT NULL,
                    path TEXT NOT NULL,
                    status_code INTEGER NOT NULL,
                    duration_ms REAL NOT NULL,
                    detail_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_api_tokens_user ON api_tokens(user_id, revoked_at, expires_at);
                CREATE INDEX IF NOT EXISTS idx_workers_user ON workers(user_id, last_seen_at);
                CREATE INDEX IF NOT EXISTS idx_worker_events_ts ON worker_events(ts);
                CREATE INDEX IF NOT EXISTS idx_api_request_log_ts ON api_request_log(ts);
                CREATE INDEX IF NOT EXISTS idx_api_request_log_path_ts ON api_request_log(path, ts);
CREATE INDEX IF NOT EXISTS idx_tasks_user_cycle_status ON tasks(user_id, cycle_id, status);
                CREATE INDEX IF NOT EXISTS idx_tasks_cycle_pool_status ON tasks(cycle_id, pool_id, status);
            CREATE INDEX IF NOT EXISTS idx_tasks_cycle_status_lease ON tasks(cycle_id, status, lease_expires_at);
                CREATE INDEX IF NOT EXISTS idx_tasks_cycle_status_lease ON tasks(cycle_id, status, lease_expires_at);
                CREATE INDEX IF NOT EXISTS idx_candidates_task_score ON candidates(task_id, score);
                CREATE INDEX IF NOT EXISTS idx_submissions_status ON submissions(status, submitted_at);
                CREATE INDEX IF NOT EXISTS idx_strategies_status ON strategies(status, expires_at);
                CREATE INDEX IF NOT EXISTS idx_payouts_status ON payouts(status, created_at);
                CREATE INDEX IF NOT EXISTS idx_audit_log_ts ON audit_log(ts);
                """
            )

            _ensure_user_run_enabled_column(conn)
            _ensure_wallet_chain_column(conn)
            _init_defaults(conn)
            _ensure_tasks_columns(conn)
            _get_or_create_system_user_id(conn)
            conn.commit()
        finally:
            conn.close()

def _ensure_user_run_enabled_column(conn: Any) -> None:
    try:
        if _DB_KIND == "sqlite":
            cols = [r["name"] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
            if "run_enabled" not in cols:
                conn.execute("ALTER TABLE users ADD COLUMN run_enabled INTEGER NOT NULL DEFAULT 0")
                conn.commit()
            return

        row = conn.execute(
            "SELECT 1 FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'run_enabled' LIMIT 1"
        ).fetchone()
        if not row:
            conn.execute("ALTER TABLE users ADD COLUMN run_enabled INTEGER NOT NULL DEFAULT 0")
            conn.commit()
    except Exception:
        return


def _ensure_wallet_chain_column(conn: Any) -> None:
    try:
        if _DB_KIND == "sqlite":
            cols = [r["name"] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
            if "wallet_chain" not in cols:
                conn.execute("ALTER TABLE users ADD COLUMN wallet_chain TEXT NOT NULL DEFAULT 'TRC20'")
                conn.commit()
            return

        row = conn.execute(
            "SELECT 1 FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'wallet_chain' LIMIT 1"
        ).fetchone()
        if not row:
            conn.execute("ALTER TABLE users ADD COLUMN wallet_chain TEXT NOT NULL DEFAULT 'TRC20'")
            conn.commit()
    except Exception:
        return


def _init_db_postgres() -> None:
    conn = _conn()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS users (
                id BIGSERIAL PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('user','admin')),
                wallet_chain TEXT NOT NULL DEFAULT 'TRC20',
                wallet_address_enc BYTEA,
                created_at TEXT NOT NULL,
                last_login_at TEXT,
                disabled INTEGER NOT NULL DEFAULT 0,
                login_fail_count INTEGER NOT NULL DEFAULT 0,
                lock_until TEXT,
                run_enabled INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS cycles (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                start_ts TEXT NOT NULL,
                end_ts TEXT NOT NULL,
                created_at TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                seed INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS factor_pools (
                id BIGSERIAL PRIMARY KEY,
                cycle_id BIGINT NOT NULL REFERENCES cycles(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timeframe_min INTEGER NOT NULL,
                years INTEGER NOT NULL,
                family TEXT NOT NULL,
                grid_spec_json TEXT NOT NULL,
                risk_spec_json TEXT NOT NULL,
                num_partitions INTEGER NOT NULL,
                seed INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id BIGSERIAL PRIMARY KEY,
                cycle_id BIGINT NOT NULL REFERENCES cycles(id) ON DELETE CASCADE,
                pool_id BIGINT NOT NULL REFERENCES factor_pools(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                partition_idx INTEGER NOT NULL,
                partition_total INTEGER NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('assigned','running','completed','expired','revoked')),
                assigned_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                last_heartbeat TEXT,
                lease_id TEXT,
                lease_worker_id TEXT,
                lease_expires_at TEXT,
                attempt INTEGER NOT NULL DEFAULT 0,
                estimated_combos BIGINT NOT NULL DEFAULT 0,
                priority INTEGER NOT NULL DEFAULT 0,
                progress_json TEXT NOT NULL,
                UNIQUE(cycle_id, pool_id, partition_idx)
            );

            CREATE TABLE IF NOT EXISTS candidates (
                id BIGSERIAL PRIMARY KEY,
                task_id BIGINT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                pool_id BIGINT NOT NULL REFERENCES factor_pools(id) ON DELETE CASCADE,
                created_at TEXT NOT NULL,
                params_enc BYTEA NOT NULL,
                metrics_json TEXT NOT NULL,
                score DOUBLE PRECISION NOT NULL,
                is_submitted INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS submissions (
                id BIGSERIAL PRIMARY KEY,
                candidate_id BIGINT NOT NULL UNIQUE REFERENCES candidates(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                pool_id BIGINT NOT NULL REFERENCES factor_pools(id) ON DELETE CASCADE,
                submitted_at TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('pending','approved','rejected')),
                audit_json TEXT,
                approved_at TEXT,
                approved_by BIGINT REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS strategies (
                id BIGSERIAL PRIMARY KEY,
                submission_id BIGINT NOT NULL UNIQUE REFERENCES submissions(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                pool_id BIGINT NOT NULL REFERENCES factor_pools(id) ON DELETE CASCADE,
                cycle_id BIGINT NOT NULL REFERENCES cycles(id) ON DELETE CASCADE,
                status TEXT NOT NULL CHECK(status IN ('active','disqualified','expired','paused')),
                activated_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                allocation_pct DOUBLE PRECISION NOT NULL,
                note TEXT
            );

            CREATE TABLE IF NOT EXISTS weekly_checks (
                id BIGSERIAL PRIMARY KEY,
                strategy_id BIGINT NOT NULL REFERENCES strategies(id) ON DELETE CASCADE,
                week_start_ts TEXT NOT NULL,
                week_end_ts TEXT NOT NULL,
                computed_at TEXT NOT NULL,
                return_pct DOUBLE PRECISION NOT NULL,
                max_drawdown_pct DOUBLE PRECISION NOT NULL,
                trades INTEGER NOT NULL,
                eligible INTEGER NOT NULL,
                UNIQUE(strategy_id, week_start_ts)
            );

            CREATE TABLE IF NOT EXISTS payouts (
                id BIGSERIAL PRIMARY KEY,
                strategy_id BIGINT NOT NULL REFERENCES strategies(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                week_start_ts TEXT NOT NULL,
                amount_usdt DOUBLE PRECISION NOT NULL,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('unpaid','paid','void')),
                paid_at TEXT,
                txid TEXT
            );

            CREATE TABLE IF NOT EXISTS audit_log (
                id BIGSERIAL PRIMARY KEY,
                ts TEXT NOT NULL,
                actor_user_id BIGINT REFERENCES users(id),
                action TEXT NOT NULL,
                detail_json TEXT NOT NULL
            );

            
            CREATE TABLE IF NOT EXISTS api_tokens (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                issued_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                revoked_at TEXT,
                last_seen_at TEXT,
                last_ip TEXT,
                last_user_agent TEXT
            );

            CREATE TABLE IF NOT EXISTS workers (
                id BIGSERIAL PRIMARY KEY,
                worker_id TEXT NOT NULL UNIQUE,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                name TEXT,
                version TEXT NOT NULL,
                protocol INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                last_heartbeat_at TEXT NOT NULL,
                last_task_id BIGINT,
                avg_cps DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                meta_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS worker_events (
                id BIGSERIAL PRIMARY KEY,
                ts TEXT NOT NULL,
                user_id BIGINT,
                worker_id TEXT,
                event TEXT NOT NULL,
                detail_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS api_request_log (
                id BIGSERIAL PRIMARY KEY,
                ts TEXT NOT NULL,
                user_id BIGINT,
                worker_id TEXT,
                token_id BIGINT,
                method TEXT NOT NULL,
                path TEXT NOT NULL,
                status_code INTEGER NOT NULL,
                duration_ms DOUBLE PRECISION NOT NULL,
                detail_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_api_tokens_user ON api_tokens(user_id, revoked_at, expires_at);
            CREATE INDEX IF NOT EXISTS idx_workers_user ON workers(user_id, last_seen_at);
            CREATE INDEX IF NOT EXISTS idx_worker_events_ts ON worker_events(ts);
            CREATE INDEX IF NOT EXISTS idx_api_request_log_ts ON api_request_log(ts);
            CREATE INDEX IF NOT EXISTS idx_api_request_log_path_ts ON api_request_log(path, ts);
CREATE INDEX IF NOT EXISTS idx_tasks_user_cycle_status ON tasks(user_id, cycle_id, status);
            CREATE INDEX IF NOT EXISTS idx_tasks_cycle_pool_status ON tasks(cycle_id, pool_id, status);
            CREATE INDEX IF NOT EXISTS idx_tasks_cycle_status_lease ON tasks(cycle_id, status, lease_expires_at);
                CREATE INDEX IF NOT EXISTS idx_tasks_cycle_status_lease ON tasks(cycle_id, status, lease_expires_at);
            CREATE INDEX IF NOT EXISTS idx_candidates_task_score ON candidates(task_id, score);
            CREATE INDEX IF NOT EXISTS idx_submissions_status ON submissions(status, submitted_at);
            CREATE INDEX IF NOT EXISTS idx_strategies_status ON strategies(status, expires_at);
            CREATE INDEX IF NOT EXISTS idx_payouts_status ON payouts(status, created_at);
            CREATE INDEX IF NOT EXISTS idx_audit_log_ts ON audit_log(ts);
            """
        )
        _ensure_user_run_enabled_column(conn)
        _ensure_wallet_chain_column(conn)
        _init_defaults(conn)
        _ensure_tasks_columns(conn)
        _get_or_create_system_user_id(conn)
        conn.commit()
    finally:
        conn.close()


def _init_defaults(conn: sqlite3.Connection) -> None:
    now = utc_now_iso()

    def set_default(key: str, value: Any) -> None:
        row = conn.execute("SELECT key FROM settings WHERE key = ?", (key,)).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO settings(key, value_json, updated_at) VALUES(?,?,?)",
                (key, json_dumps(value), now),
            )

    set_default("min_tasks_per_user", 2)
    set_default("max_tasks_per_user", 6)
    set_default("max_concurrent_jobs", 2)
    set_default("task_lease_minutes", 180)
    set_default("task_lease_seconds", 600)
    set_default("task_reserve_minutes", 60)
    set_default("task_reserve_seconds", 3600)
    set_default("task_target_seconds", 900)
    set_default("default_worker_cps", 50.0)
    set_default("max_active_leases_per_user", 4)
    set_default("api_ratelimit_rpm", 600.0)
    set_default("api_ratelimit_burst", 120.0)
    set_default("api_slow_ms", 800.0)
    set_default("api_log_sample_rate", 0.05)
    set_default("monitor_retention_days", 14)
    set_default("worker_min_version", "2.1.0")
    set_default("worker_latest_version", "2.1.0")
    set_default("worker_min_protocol", 2)
    set_default("worker_download_url", "")
    set_default("candidate_keep_top_n", 30)

    # Worker result verification (anti-cheat)
    set_default("verify_max_candidates", 10)
    set_default("verify_tolerance_return_pct", 0.1)
    set_default("verify_tolerance_drawdown_pct", 0.1)
    set_default("verify_tolerance_sharpe", 0.05)
    set_default("verify_tolerance_trades", 1)

    # Tutorial assets
    set_default("tutorial_video_path", "")

    # Share preview (Open Graph)
    set_default("og_title", "羊肉爐挖礦分潤平台")
    set_default("og_description", "分散算力進行參數搜尋，提交達標策略並參與週期結算。")
    set_default("og_image_url", "")
    set_default("og_site_name", "羊肉爐")
    set_default("og_redirect_url", "")

    # Withdrawal display rules
    set_default("withdraw_min_usdt", 20.0)
    set_default("withdraw_fee_usdt", 1.0)
    set_default("withdraw_fee_mode", "deduct")

    # ToS
    set_default("tos_version", "2026-02-19")
    set_default(
        "tos_text",
        "量化交易與加密貨幣具有高風險，過去績效不代表未來結果。\n\n"
        "本平台提供參數搜尋、策略審核與結算記錄，不保證任何策略永遠獲利。\n\n"
        "分潤以實盤結果與平台規則為準，可能出現當週未發放或發放為 0 的情況。\n\n"
        "平台保留因風控、策略失效、系統維護等因素暫停或終止策略之權利。\n\n"
        "使用者需自行承擔設備耗電、硬體磨損、網路中斷等成本與風險。\n\n"
        "分潤地址如填寫錯誤造成資產損失，平台不負任何責任。",
    )

    # Chat moderation
    set_default("chat_blocked_words", "")
    set_default("chat_max_len", 120)
    set_default("capital_usdt", 0.0)
    set_default("payout_rate", 0.0)
    set_default("payout_currency", "USDT")
    set_default("default_allocation_pct", 10.0)
    set_default("min_trades", 40)
    set_default("min_total_return_pct", 15.0)
    set_default("max_drawdown_pct", 25.0)
    set_default("min_sharpe", 0.6)
    set_default("execution_mode", "server")
    set_default("worker_api_url", "http://127.0.0.1:8001")

    if conn.execute("SELECT COUNT(1) AS c FROM cycles").fetchone()["c"] == 0:
        cycle = compute_current_cycle(_utc_now())
        conn.execute(
            "INSERT INTO cycles(name, start_ts, end_ts, created_at, is_active, seed) VALUES(?,?,?,?,?,?)",
            (cycle["name"], cycle["start_ts"], cycle["end_ts"], now, 1, cycle["seed"]),
        )

    if conn.execute("SELECT COUNT(1) AS c FROM factor_pools").fetchone()["c"] == 0:
        cycle_id = conn.execute("SELECT id FROM cycles ORDER BY id DESC LIMIT 1").fetchone()["id"]
        default_pool = default_factor_pool_spec()
        conn.execute(
            """
            INSERT INTO factor_pools(
                cycle_id, name, symbol, timeframe_min, years, family,
                grid_spec_json, risk_spec_json,
                num_partitions, seed, created_at, active
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                cycle_id,
                default_pool["name"],
                default_pool["symbol"],
                default_pool["timeframe_min"],
                default_pool["years"],
                default_pool["family"],
                json_dumps(default_pool["grid_spec"]),
                json_dumps(default_pool["risk_spec"]),
                default_pool["num_partitions"],
                default_pool["seed"],
                now,
                1,
            ),
        )


def default_factor_pool_spec() -> Dict[str, Any]:
    # Conservative default. Admin UI can add more pools.
    return {
        "name": "Default Pool",
        "symbol": "BTC_USDT",
        "timeframe_min": 30,
        "years": 3,
        "family": "RSI",
        "grid_spec": {
            "rsi_p_min": 6, "rsi_p_max": 21, "rsi_p_step": 1,
            "rsi_lv_min": 10, "rsi_lv_max": 35, "rsi_lv_step": 1,
        },
        "risk_spec": {
            "tp_min": 0.30, "tp_max": 1.20, "tp_step": 0.10,
            "sl_min": 0.30, "sl_max": 1.20, "sl_step": 0.10,
            "max_hold_min": 4, "max_hold_max": 80, "max_hold_step": 4,
            "fee_side": 0.0002,
            "slippage": 0.0,
            "worst_case": True,
            "reverse_mode": False,
        },
        "num_partitions": 128,
        "seed": 20240201,
    }

def compute_current_cycle(now_utc: datetime) -> Dict[str, Any]:
    # Quarterly cycles aligned to UTC calendar quarters
    y = now_utc.year
    m = now_utc.month
    q = (m - 1) // 3 + 1
    start_month = 3 * (q - 1) + 1
    start = datetime(y, start_month, 1, tzinfo=timezone.utc)
    if start_month == 10:
        end = datetime(y + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(y, start_month + 3, 1, tzinfo=timezone.utc)
    seed = int(start.timestamp()) & 0x7FFFFFFF
    return {
        "name": f"{y}Q{q}",
        "start_ts": _iso(start),
        "end_ts": _iso(end),
        "seed": seed,
    }


def get_setting(conn: sqlite3.Connection, key: str, default: Any = None) -> Any:
    row = conn.execute("SELECT value_json FROM settings WHERE key = ?", (key,)).fetchone()
    if row is None:
        return default
    try:
        return json.loads(row["value_json"])
    except Exception:
        return default


def get_settings_rev(conn: Any) -> str:
    try:
        row = conn.execute("SELECT MAX(updated_at) AS m FROM settings").fetchone()
    except Exception:
        return ""
    if not row:
        return ""
    if isinstance(row, dict):
        return str(row.get("m") or "")
    try:
        return str(row["m"] or "")
    except Exception:
        return ""


def set_setting(conn: sqlite3.Connection, key: str, value: Any) -> None:
    now = utc_now_iso()
    conn.execute(
        "INSERT INTO settings(key, value_json, updated_at) VALUES(?,?,?) "
        "ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at",
        (key, json_dumps(value), now),
    )


def data_hash_setting_key(symbol: str, timeframe_min: int, years: int) -> str:
    sym = str(symbol or "").strip().upper()
    tf = int(timeframe_min or 0)
    yrs = int(years or 0)
    return f"data_hash:{sym}:{tf}:{yrs}"


def data_hash_ts_setting_key(symbol: str, timeframe_min: int, years: int) -> str:
    sym = str(symbol or "").strip().upper()
    tf = int(timeframe_min or 0)
    yrs = int(years or 0)
    return f"data_hash_ts:{sym}:{tf}:{yrs}"


def get_data_hash(symbol: str, timeframe_min: int, years: int) -> Dict[str, str]:
    """Return data hash metadata stored in settings.

    The actual hash is maintained by a cron job. If not present, returns empty strings.
    """
    conn = _conn()
    try:
        key = data_hash_setting_key(symbol, timeframe_min, years)
        ts_key = data_hash_ts_setting_key(symbol, timeframe_min, years)
        h = str(get_setting(conn, key, "") or "").strip()
        ts = str(get_setting(conn, ts_key, "") or "").strip()
        return {"data_hash": h, "data_hash_ts": ts}
    finally:
        conn.close()


def _user_pref_key_pool_ids(user_id: int) -> str:
    return f"user_pref_pool_ids:{int(user_id)}"


def get_user_pref_pool_ids(user_id: int) -> List[int]:
    conn = _conn()
    try:
        raw = get_setting(conn, _user_pref_key_pool_ids(int(user_id)), [])
    finally:
        conn.close()

    out: List[int] = []
    if isinstance(raw, list):
        for x in raw:
            try:
                out.append(int(x))
            except Exception:
                pass
    seen = set()
    uniq: List[int] = []
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        uniq.append(x)
    return uniq


def set_user_pref_pool_ids(user_id: int, pool_ids: List[int]) -> None:
    ids: List[int] = []
    for x in pool_ids or []:
        try:
            ids.append(int(x))
        except Exception:
            pass
    seen = set()
    uniq: List[int] = []
    for x in ids:
        if x in seen:
            continue
        seen.add(x)
        uniq.append(x)

    conn = _conn()
    try:
        set_setting(conn, _user_pref_key_pool_ids(int(user_id)), uniq)
        conn.commit()
    finally:
        conn.close()


def delete_assigned_tasks_for_user_not_in_pools(user_id: int, cycle_id: int, keep_pool_ids: List[int]) -> int:
    user_id = int(user_id)
    cycle_id = int(cycle_id)
    keep_ids: List[int] = []
    for x in keep_pool_ids or []:
        try:
            keep_ids.append(int(x))
        except Exception:
            pass
    keep_ids = [x for x in keep_ids if x > 0]

    conn = _conn()
    try:
        if keep_ids:
            placeholders = ",".join(["?"] * len(keep_ids))
            sql = f"DELETE FROM tasks WHERE user_id = ? AND cycle_id = ? AND status = 'assigned' AND pool_id NOT IN ({placeholders})"
            params = (int(user_id), int(cycle_id), *[int(x) for x in keep_ids])
        else:
            sql = "DELETE FROM tasks WHERE user_id = ? AND cycle_id = ? AND status = 'assigned'"
            params = (int(user_id), int(cycle_id))

        cur = conn.execute(sql, params)
        conn.commit()
        return int(cur.rowcount or 0)
    finally:
        conn.close()


def create_user(username: str, password_hash: str, role: str, wallet_address: str, wallet_chain: str = "TRC20") -> int:
    now = utc_now_iso()
    conn = _conn()
    try:
        chain = str(wallet_chain or "TRC20").strip().upper()[:16] or "TRC20"
        try:
            sql = "INSERT INTO users(username, password_hash, role, wallet_chain, wallet_address_enc, created_at) VALUES(?,?,?,?,?,?)"
            uid = _insert_id(conn, sql, (username, password_hash, role, chain, encrypt_text(wallet_address), now))
        except Exception:
            sql = "INSERT INTO users(username, password_hash, role, wallet_address_enc, created_at) VALUES(?,?,?,?,?)"
            uid = _insert_id(conn, sql, (username, password_hash, role, encrypt_text(wallet_address), now))
        conn.commit()
        return int(uid)
    finally:
        conn.close()


def get_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    conn = _conn()
    try:
        row = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    conn = _conn()
    try:
        row = conn.execute("SELECT * FROM users WHERE id = ?", (int(user_id),)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def update_user_login_state(user_id: int, success: bool, lock_seconds: int = 600, max_fail: int = 6) -> None:
    conn = _conn()
    try:
        if success:
            conn.execute(
                "UPDATE users SET last_login_at = ?, login_fail_count = 0, lock_until = NULL WHERE id = ?",
                (utc_now_iso(), int(user_id)),
            )
        else:
            row = conn.execute("SELECT login_fail_count FROM users WHERE id = ?", (int(user_id),)).fetchone()
            fail = int(row["login_fail_count"]) if row else 0
            fail += 1
            lock_until = None
            if fail >= max_fail:
                lock_until = _iso(_utc_now() + timedelta(seconds=lock_seconds))
                fail = 0
            conn.execute(
                "UPDATE users SET login_fail_count = ?, lock_until = ? WHERE id = ?",
                (fail, lock_until, int(user_id)),
            )
        conn.commit()
    finally:
        conn.close()


def is_user_locked(user_row: Dict[str, Any]) -> bool:
    until = _parse_iso(user_row.get("lock_until"))
    if until is None:
        return False
    return _utc_now() < until


def list_users(limit: int = 200) -> List[Dict[str, Any]]:
    conn = _conn()
    try:
        rows = conn.execute("SELECT * FROM users ORDER BY created_at DESC LIMIT ?", (int(limit),)).fetchall()
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


def get_active_cycle() -> Dict[str, Any]:
    conn = _conn()
    try:
        row = conn.execute("SELECT * FROM cycles WHERE is_active = 1 ORDER BY id DESC LIMIT 1").fetchone()
        if row:
            return dict(row)
        return {}
    finally:
        conn.close()


def ensure_cycle_rollover() -> Dict[str, Any]:
    conn = _conn()
    try:
        now = _utc_now()
        active = conn.execute("SELECT * FROM cycles WHERE is_active = 1 ORDER BY id DESC LIMIT 1").fetchone()
        if active:
            end_ts = _parse_iso(active["end_ts"])
            if end_ts and now < end_ts:
                return dict(active)

            old_cycle_id = int(active["id"])
            conn.execute("UPDATE cycles SET is_active = 0 WHERE id = ?", (old_cycle_id,))
            conn.execute("UPDATE strategies SET status = 'expired' WHERE cycle_id = ? AND status != 'expired'", (old_cycle_id,))
            conn.execute("UPDATE tasks SET status = 'expired' WHERE cycle_id = ? AND status != 'expired'", (old_cycle_id,))

        cycle = compute_current_cycle(now)
        conn.execute(
            "INSERT INTO cycles(name, start_ts, end_ts, created_at, is_active, seed) VALUES(?,?,?,?,?,?)",
            (cycle["name"], cycle["start_ts"], cycle["end_ts"], utc_now_iso(), 1, cycle["seed"]),
        )
        new_id = int(conn.execute("SELECT id FROM cycles ORDER BY id DESC LIMIT 1").fetchone()["id"])

        # Pools need to be created per new cycle. Default pool is copied.
        pools = conn.execute("SELECT * FROM factor_pools ORDER BY id DESC LIMIT 50").fetchall()
        if pools:
            last_pool = dict(pools[0])
            conn.execute(
                """
                INSERT INTO factor_pools(
                    cycle_id, name, symbol, timeframe_min, years, family,
                    grid_spec_json, risk_spec_json,
                    num_partitions, seed, created_at, active
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    new_id,
                    last_pool["name"],
                    last_pool["symbol"],
                    last_pool["timeframe_min"],
                    last_pool["years"],
                    last_pool["family"],
                    last_pool["grid_spec_json"],
                    last_pool["risk_spec_json"],
                    last_pool["num_partitions"],
                    last_pool["seed"],
                    utc_now_iso(),
                    1,
                ),
            )
        else:
            default_pool = default_factor_pool_spec()
            conn.execute(
                """
                INSERT INTO factor_pools(
                    cycle_id, name, symbol, timeframe_min, years, family,
                    grid_spec_json, risk_spec_json,
                    num_partitions, seed, created_at, active
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    new_id,
                    default_pool["name"],
                    default_pool["symbol"],
                    default_pool["timeframe_min"],
                    default_pool["years"],
                    default_pool["family"],
                    json_dumps(default_pool["grid_spec"]),
                    json_dumps(default_pool["risk_spec"]),
                    default_pool["num_partitions"],
                    default_pool["seed"],
                    utc_now_iso(),
                    1,
                ),
            )

        conn.commit()
        new_cycle_row = conn.execute("SELECT * FROM cycles WHERE id = ?", (new_id,)).fetchone()
        new_cycle = dict(new_cycle_row) if new_cycle_row else {}
        new_cycle_id = int(new_id)
    finally:
        conn.close()

    try:
        ensure_cycle_tasks(new_cycle_id)
    except Exception:
        pass
    return new_cycle


def list_factor_pools(cycle_id: Optional[int] = None) -> List[Dict[str, Any]]:
    conn = _conn()
    try:
        if cycle_id is None:
            row = conn.execute("SELECT id FROM cycles WHERE is_active = 1 ORDER BY id DESC LIMIT 1").fetchone()
            cycle_id = int(row["id"]) if row else None
        if cycle_id is None:
            return []
        rows = conn.execute(
            "SELECT * FROM factor_pools WHERE cycle_id = ? AND active = 1 ORDER BY id ASC",
            (int(cycle_id),),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def create_factor_pool(
    cycle_id: int,
    name: str,
    symbol: str,
    timeframe_min: int,
    years: int,
    family: str,
    grid_spec: Dict[str, Any],
    risk_spec: Dict[str, Any],
    num_partitions: int,
    seed: int,
    active: bool = True,
) -> int:
    conn = _conn()
    try:
        sql = """
            INSERT INTO factor_pools(
                cycle_id, name, symbol, timeframe_min, years, family,
                grid_spec_json, risk_spec_json,
                num_partitions, seed, created_at, active
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """
        pid = _insert_id(
            conn,
            sql,
            (
                int(cycle_id),
                str(name),
                str(symbol),
                int(timeframe_min),
                int(years),
                str(family),
                json_dumps(grid_spec),
                json_dumps(risk_spec),
                int(num_partitions),
                int(seed),
                utc_now_iso(),
                1 if active else 0,
            ),
        )
        conn.commit()
        pid_int = int(pid)
    finally:
        conn.close()

    try:
        ensure_tasks_for_pool(pid_int)
    except Exception:
        pass
    return pid_int


def set_pool_active(pool_id: int, active: bool) -> None:
    conn = _conn()
    try:
        conn.execute("UPDATE factor_pools SET active = ? WHERE id = ?", (1 if active else 0, int(pool_id)))
        conn.commit()
    finally:
        conn.close()


def assign_tasks_for_user(
    user_id: int,
    min_needed: int = 2,
    cycle_id: Optional[int] = None,
    min_tasks: Optional[int] = None,
    max_tasks: Optional[int] = None,
) -> List[int]:
    if min_tasks is not None:
        min_needed = int(min_tasks)
    min_needed = max(0, int(min_needed))

    conn = _conn()
    try:
        if cycle_id is None:
            cycle = conn.execute("SELECT * FROM cycles WHERE is_active = 1 ORDER BY id DESC LIMIT 1").fetchone()
            if not cycle:
                return []
            cycle_id = int(cycle["id"])
        else:
            cycle_id = int(cycle_id)

        # Make sure pool tasks exist (seeded as system-owned assigned tasks).
        try:
            ensure_cycle_tasks(int(cycle_id))
        except Exception:
            pass

        lease_seconds = int(get_setting(conn, "task_lease_seconds", int(get_setting(conn, "task_lease_minutes", 180)) * 60))
        _cleanup_stale_running_tasks(conn, int(cycle_id), int(lease_seconds))

        # Release long-idle reserved tasks back to system so one user can't hoard all partitions.
        try:
            reserve_seconds = int(get_setting(conn, "task_reserve_seconds", int(get_setting(conn, "task_reserve_minutes", 60)) * 60))
            reserve_seconds = max(30, int(reserve_seconds))
        except Exception:
            reserve_seconds = 3600
        try:
            _cleanup_stale_assigned_tasks(conn, int(cycle_id), int(reserve_seconds))
        except Exception:
            pass

        pools = conn.execute(
            "SELECT id FROM factor_pools WHERE cycle_id = ? AND active = 1 ORDER BY id ASC",
            (int(cycle_id),),
        ).fetchall()
        if not pools:
            return []

        if max_tasks is None:
            max_tasks = int(get_setting(conn, "max_tasks_per_user", 6))
        max_tasks = max(min_needed, int(max_tasks))

        active_count = int(
            conn.execute(
                "SELECT COUNT(1) AS c FROM tasks WHERE user_id = ? AND cycle_id = ? AND status IN ('assigned','running')",
                (int(user_id), int(cycle_id)),
            ).fetchone()["c"]
            or 0
        )

        need = int(min_needed) - int(active_count)
        if need <= 0:
            return []
        need = min(int(need), max(0, int(max_tasks) - int(active_count)))
        if need <= 0:
            return []

        system_uid = _get_or_create_system_user_id(conn)
        now_iso = utc_now_iso()

        # Claim system-owned assigned tasks.
        rows = conn.execute(
            """
            SELECT id
            FROM tasks
            WHERE cycle_id = ? AND status = 'assigned' AND user_id = ?
            ORDER BY priority DESC, assigned_at ASC, id ASC
            LIMIT ?
            """,
            (int(cycle_id), int(system_uid), int(need)),
        ).fetchall()

        claimed_ids: List[int] = []
        for r in rows:
            tid = int(r["id"] if isinstance(r, dict) else r[0])
            cur = conn.execute(
                """
                UPDATE tasks
                SET user_id = ?, assigned_at = ?
                WHERE id = ? AND cycle_id = ? AND status = 'assigned' AND user_id = ?
                """,
                (int(user_id), str(now_iso), int(tid), int(cycle_id), int(system_uid)),
            )
            if int(cur.rowcount or 0) == 1:
                claimed_ids.append(int(tid))

        conn.commit()
        return claimed_ids
    finally:
        conn.close()



def list_tasks_for_user(user_id: int, cycle_id: Optional[int] = None) -> List[Dict[str, Any]]:
    conn = _conn()
    try:
        if cycle_id is None:
            row = conn.execute("SELECT id FROM cycles WHERE is_active = 1 ORDER BY id DESC LIMIT 1").fetchone()
            cycle_id = int(row["id"]) if row else None
        if cycle_id is None:
            return []
        rows = conn.execute(
            """
            SELECT t.*, p.name AS pool_name, p.symbol, p.timeframe_min, p.family, t.partition_total AS num_partitions
            FROM tasks t
            JOIN factor_pools p ON p.id = t.pool_id
            WHERE t.user_id = ? AND t.cycle_id = ?
            ORDER BY t.assigned_at DESC
            """,
            (int(user_id), int(cycle_id)),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_task(task_id: int) -> Optional[Dict[str, Any]]:
    conn = _conn()
    try:
        row = conn.execute(
            """
            SELECT t.*, p.name AS pool_name, p.symbol, p.timeframe_min, p.years, p.family, p.grid_spec_json, p.risk_spec_json, t.partition_total AS num_partitions, p.seed
            FROM tasks t
            JOIN factor_pools p ON p.id = t.pool_id
            WHERE t.id = ?
            """,
            (int(task_id),),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()

def update_task_status(task_id: int, status: str, started: bool = False, finished: bool = False) -> None:
    conn = _conn()
    try:
        now = utc_now_iso()
        if started:
            conn.execute(
                "UPDATE tasks SET status = ?, started_at = ?, last_heartbeat = ? WHERE id = ?",
                (status, now, now, int(task_id)),
            )
        elif finished:
            conn.execute(
                "UPDATE tasks SET status = ?, finished_at = ?, last_heartbeat = ? WHERE id = ?",
                (status, now, now, int(task_id)),
            )
        else:
            conn.execute(
                "UPDATE tasks SET status = ?, last_heartbeat = ? WHERE id = ?",
                (status, now, int(task_id)),
            )
        conn.commit()
    finally:
        conn.close()

def claim_task_for_run(task_id: int) -> bool:
    conn = _conn()
    try:
        now = utc_now_iso()
        cur = conn.execute(
            "UPDATE tasks SET status = 'running', started_at = COALESCE(started_at, ?), last_heartbeat = ? WHERE id = ? AND status = 'assigned'",
            (now, now, int(task_id)),
        )
        conn.commit()
        return int(cur.rowcount or 0) == 1
    finally:
        conn.close()


def get_user_run_enabled(user_id: int) -> int:
    conn = _conn()
    try:
        row = conn.execute("SELECT run_enabled FROM users WHERE id = ?", (int(user_id),)).fetchone()
        if not row:
            return 0
        return int(row["run_enabled"] or 0)
    finally:
        conn.close()


def set_user_run_enabled(user_id: int, enabled: bool) -> None:
    conn = _conn()
    try:
        conn.execute("UPDATE users SET run_enabled = ? WHERE id = ?", (1 if enabled else 0, int(user_id)))
        conn.commit()
    finally:
        conn.close()


def update_factor_pool(
    pool_id: int,
    name: str,
    symbol: str,
    timeframe_min: int,
    years: int,
    family: str,
    grid_spec: Dict[str, Any],
    risk_spec: Dict[str, Any],
    num_partitions: int,
    seed: int,
    active: bool,
) -> None:
    conn = _conn()
    try:
        conn.execute(
            """
            UPDATE factor_pools
            SET name = ?, symbol = ?, timeframe_min = ?, years = ?, family = ?,
                grid_spec_json = ?, risk_spec_json = ?,
                num_partitions = ?, seed = ?, active = ?
            WHERE id = ?
            """,
            (
                str(name),
                str(symbol),
                int(timeframe_min),
                int(years),
                str(family),
                json_dumps(grid_spec),
                json_dumps(risk_spec),
                int(num_partitions),
                int(seed),
                1 if active else 0,
                int(pool_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def delete_tasks_for_pool(cycle_id: int, pool_id: int) -> int:
    conn = _conn()
    try:
        cur = conn.execute("DELETE FROM tasks WHERE cycle_id = ? AND pool_id = ?", (int(cycle_id), int(pool_id)))
        conn.commit()
        return int(cur.rowcount or 0)
    finally:
        conn.close()

def update_task_progress(task_id: int, progress: Dict[str, Any]) -> None:
    conn = _conn()
    try:
        conn.execute(
            "UPDATE tasks SET progress_json = ?, last_heartbeat = ? WHERE id = ?",
            (json_dumps(progress), utc_now_iso(), int(task_id)),
        )
        conn.commit()
    finally:
        conn.close()


def clear_candidates_for_task(task_id: int) -> None:
    conn = _conn()
    try:
        conn.execute("DELETE FROM candidates WHERE task_id = ?", (int(task_id),))
        conn.commit()
    finally:
        conn.close()


def insert_candidate(
    task_id: int,
    user_id: int,
    pool_id: int,
    params_json: Dict[str, Any],
    metrics_json: Dict[str, Any],
    score: float,
) -> int:
    conn = _conn()
    try:
        sql = """
            INSERT INTO candidates(task_id, user_id, pool_id, created_at, params_enc, metrics_json, score, is_submitted)
            VALUES(?,?,?,?,?,?,?,0)
        """
        cid = _insert_id(
            conn,
            sql,
            (
                int(task_id),
                int(user_id),
                int(pool_id),
                utc_now_iso(),
                encrypt_text(json_dumps(params_json)),
                json_dumps(metrics_json),
                float(score),
            ),
        )
        conn.commit()
        return int(cid)
    finally:
        conn.close()


def list_candidates(task_id: int, limit: int = 50) -> List[Dict[str, Any]]:
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT * FROM candidates WHERE task_id = ? ORDER BY score DESC LIMIT ?",
            (int(task_id), int(limit)),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["params_json"] = None
            params = decrypt_text(d.get("params_enc"))
            if params:
                try:
                    d["params_json"] = json.loads(params)
                except Exception:
                    d["params_json"] = None
            try:
                d["metrics"] = json.loads(d.get("metrics_json") or "{}")
            except Exception:
                d["metrics"] = {}
            out.append(d)
        return out
    finally:
        conn.close()


def mark_candidate_submitted(candidate_id: int) -> None:
    conn = _conn()
    try:
        conn.execute("UPDATE candidates SET is_submitted = 1 WHERE id = ?", (int(candidate_id),))
        conn.commit()
    finally:
        conn.close()


def create_submission(candidate_id: int, user_id: int, pool_id: int, audit: Dict[str, Any]) -> int:
    conn = _conn()
    try:
        sql = """
            INSERT INTO submissions(candidate_id, user_id, pool_id, submitted_at, status, audit_json)
            VALUES(?,?,?,?,?,?)
        """
        sid = _insert_id(
            conn,
            sql,
            (int(candidate_id), int(user_id), int(pool_id), utc_now_iso(), "pending", json_dumps(audit)),
        )
        conn.execute("UPDATE candidates SET is_submitted = 1 WHERE id = ?", (int(candidate_id),))
        conn.commit()
        return int(sid)
    finally:
        conn.close()


def list_submissions(user_id: Optional[int] = None, status: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
    conn = _conn()
    try:
        q = """
            SELECT s.*, u.username, p.name AS pool_name, p.symbol, p.timeframe_min, p.family
            FROM submissions s
            JOIN users u ON u.id = s.user_id
            JOIN factor_pools p ON p.id = s.pool_id
        """
        where = []
        params: List[Any] = []
        if user_id is not None:
            where.append("s.user_id = ?")
            params.append(int(user_id))
        if status is not None:
            where.append("s.status = ?")
            params.append(str(status))
        if where:
            q += " WHERE " + " AND ".join(where)
        q += " ORDER BY s.submitted_at DESC LIMIT ?"
        params.append(int(limit))
        rows = conn.execute(q, tuple(params)).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            try:
                d["audit"] = json.loads(d.get("audit_json") or "{}")
            except Exception:
                d["audit"] = {}
            out.append(d)
        return out
    finally:
        conn.close()


def get_submission(submission_id: int) -> Optional[Dict[str, Any]]:
    conn = _conn()
    try:
        row = conn.execute(
            """
            SELECT s.*, c.params_enc, c.metrics_json, u.username, p.name AS pool_name, p.symbol, p.timeframe_min, p.family
            FROM submissions s
            JOIN candidates c ON c.id = s.candidate_id
            JOIN users u ON u.id = s.user_id
            JOIN factor_pools p ON p.id = s.pool_id
            WHERE s.id = ?
            """,
            (int(submission_id),),
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        raw = decrypt_text(d.get("params_enc"))
        d["params_json"] = json.loads(raw) if raw else None
        try:
            d["metrics"] = json.loads(d.get("metrics_json") or "{}")
        except Exception:
            d["metrics"] = {}
        try:
            d["audit"] = json.loads(d.get("audit_json") or "{}")
        except Exception:
            d["audit"] = {}
        return d
    finally:
        conn.close()


def set_submission_status(submission_id: int, status: str, approved_by: Optional[int] = None) -> None:
    conn = _conn()
    try:
        now = utc_now_iso()
        if status == "approved":
            conn.execute(
                "UPDATE submissions SET status = ?, approved_at = ?, approved_by = ? WHERE id = ?",
                (status, now, int(approved_by) if approved_by is not None else None, int(submission_id)),
            )
        else:
            conn.execute(
                "UPDATE submissions SET status = ?, approved_at = NULL, approved_by = NULL WHERE id = ?",
                (status, int(submission_id)),
            )
        conn.commit()
    finally:
        conn.close()


def create_strategy_from_submission(submission_id: int, allocation_pct: float, note: str = "") -> int:
    conn = _conn()
    try:
        sub = conn.execute("SELECT * FROM submissions WHERE id = ?", (int(submission_id),)).fetchone()
        if not sub:
            raise ValueError("submission_not_found")

        pool = conn.execute("SELECT * FROM factor_pools WHERE id = ?", (int(sub["pool_id"]),)).fetchone()
        cycle = conn.execute("SELECT * FROM cycles WHERE id = ?", (int(pool["cycle_id"]),)).fetchone()

        activated_at = utc_now_iso()
        expires_at = str(cycle["end_ts"])

        sql = """
            INSERT INTO strategies(submission_id, user_id, pool_id, cycle_id, status, activated_at, expires_at, allocation_pct, note)
            VALUES(?,?,?,?,?,?,?,?,?)
        """
        stid = _insert_id(
            conn,
            sql,
            (
                int(submission_id),
                int(sub["user_id"]),
                int(sub["pool_id"]),
                int(pool["cycle_id"]),
                "active",
                activated_at,
                expires_at,
                float(allocation_pct),
                str(note or ""),
            ),
        )
        conn.commit()
        return int(stid)
    finally:
        conn.close()


def list_strategies(user_id: Optional[int] = None, status: Optional[str] = None, limit: int = 300) -> List[Dict[str, Any]]:
    conn = _conn()
    try:
        q = """
            SELECT st.*, u.username, p.name AS pool_name, p.symbol, p.timeframe_min, p.family
            FROM strategies st
            JOIN users u ON u.id = st.user_id
            JOIN factor_pools p ON p.id = st.pool_id
        """
        where = []
        params: List[Any] = []
        if user_id is not None:
            where.append("st.user_id = ?")
            params.append(int(user_id))
        if status is not None:
            where.append("st.status = ?")
            params.append(str(status))
        if where:
            q += " WHERE " + " AND ".join(where)
        q += " ORDER BY st.activated_at DESC LIMIT ?"
        params.append(int(limit))
        rows = conn.execute(q, tuple(params)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def set_strategy_status(strategy_id: int, status: str) -> None:
    conn = _conn()
    try:
        conn.execute("UPDATE strategies SET status = ? WHERE id = ?", (str(status), int(strategy_id)))
        conn.commit()
    finally:
        conn.close()


def create_weekly_check(
    strategy_id: int,
    week_start_ts: str,
    week_end_ts: str,
    return_pct: float,
    max_drawdown_pct: float,
    trades: int,
    eligible: bool,
) -> None:
    conn = _conn()
    try:
        conn.execute(
            """
            INSERT INTO weekly_checks(strategy_id, week_start_ts, week_end_ts, computed_at, return_pct, max_drawdown_pct, trades, eligible)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(strategy_id, week_start_ts) DO NOTHING
            """,
            (
                int(strategy_id),
                str(week_start_ts),
                str(week_end_ts),
                utc_now_iso(),
                float(return_pct),
                float(max_drawdown_pct),
                int(trades),
                1 if eligible else 0,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def list_weekly_checks(strategy_id: Optional[int] = None, limit: int = 500) -> List[Dict[str, Any]]:
    conn = _conn()
    try:
        if strategy_id is None:
            rows = conn.execute(
                """
                SELECT wc.*, st.user_id, u.username
                FROM weekly_checks wc
                JOIN strategies st ON st.id = wc.strategy_id
                JOIN users u ON u.id = st.user_id
                ORDER BY wc.week_start_ts DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM weekly_checks
                WHERE strategy_id = ?
                ORDER BY week_start_ts DESC
                LIMIT ?
                """,
                (int(strategy_id), int(limit)),
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def payout_exists(strategy_id: int, week_start_ts: str) -> bool:
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT 1 FROM payouts WHERE strategy_id = ? AND week_start_ts = ? LIMIT 1",
            (int(strategy_id), str(week_start_ts)),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def create_payout(strategy_id: int, user_id: int, week_start_ts: str, amount_usdt: float) -> int:
    conn = _conn()
    try:
        sql = """
            INSERT INTO payouts(strategy_id, user_id, week_start_ts, amount_usdt, created_at, status)
            VALUES(?,?,?,?,?,?)
        """
        pid = _insert_id(
            conn,
            sql,
            (int(strategy_id), int(user_id), str(week_start_ts), float(amount_usdt), utc_now_iso(), "unpaid"),
        )
        conn.commit()
        return int(pid)
    finally:
        conn.close()


def list_payouts(user_id: Optional[int] = None, status: Optional[str] = None, limit: int = 500) -> List[Dict[str, Any]]:
    conn = _conn()
    try:
        q = """
            SELECT p.*, u.username, st.status AS strategy_status
            FROM payouts p
            JOIN users u ON u.id = p.user_id
            JOIN strategies st ON st.id = p.strategy_id
        """
        where = []
        params: List[Any] = []
        if user_id is not None:
            where.append("p.user_id = ?")
            params.append(int(user_id))
        if status is not None:
            where.append("p.status = ?")
            params.append(str(status))
        if where:
            q += " WHERE " + " AND ".join(where)
        q += " ORDER BY p.week_start_ts DESC, p.created_at DESC LIMIT ?"
        params.append(int(limit))
        rows = conn.execute(q, tuple(params)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def set_payout_paid(payout_id: int, txid: str) -> None:
    conn = _conn()
    try:
        conn.execute(
            "UPDATE payouts SET status = 'paid', paid_at = ?, txid = ? WHERE id = ?",
            (utc_now_iso(), str(txid or ""), int(payout_id)),
        )
        conn.commit()
    finally:
        conn.close()


def write_audit_log(actor_user_id: Optional[int], action: str, detail: Dict[str, Any]) -> None:
    conn = _conn()
    try:
        conn.execute(
            "INSERT INTO audit_log(ts, actor_user_id, action, detail_json) VALUES(?,?,?,?)",
            (utc_now_iso(), int(actor_user_id) if actor_user_id is not None else None, str(action), json_dumps(detail)),
        )
        conn.commit()
    finally:
        conn.close()


def get_wallet_info(user_id: int) -> Dict[str, str]:
    conn = _conn()
    try:
        try:
            row = conn.execute("SELECT wallet_chain, wallet_address_enc FROM users WHERE id = ?", (int(user_id),)).fetchone()
            if not row:
                return {"chain": "TRC20", "address": ""}
            chain = str(row.get("wallet_chain") or "TRC20").strip().upper() if isinstance(row, dict) else "TRC20"
            addr = decrypt_text(row["wallet_address_enc"]) if isinstance(row, dict) else decrypt_text(row[1])
            return {"chain": chain or "TRC20", "address": addr or ""}
        except Exception:
            row = conn.execute("SELECT wallet_address_enc FROM users WHERE id = ?", (int(user_id),)).fetchone()
            if not row:
                return {"chain": "TRC20", "address": ""}
            addr = decrypt_text(row["wallet_address_enc"]) if isinstance(row, dict) else decrypt_text(row[0])
            return {"chain": "TRC20", "address": addr or ""}
    finally:
        conn.close()


def get_wallet_address(user_id: int) -> Optional[str]:
    info = get_wallet_info(int(user_id))
    return info.get("address") or None


def set_wallet_address(user_id: int, wallet_address: str, wallet_chain: str = "") -> None:
    chain = str(wallet_chain or "").strip().upper()[:16]
    conn = _conn()
    try:
        if chain:
            try:
                conn.execute(
                    "UPDATE users SET wallet_chain = ?, wallet_address_enc = ? WHERE id = ?",
                    (chain, encrypt_text(wallet_address), int(user_id)),
                )
            except Exception:
                conn.execute(
                    "UPDATE users SET wallet_address_enc = ? WHERE id = ?",
                    (encrypt_text(wallet_address), int(user_id)),
                )
        else:
            conn.execute(
                "UPDATE users SET wallet_address_enc = ? WHERE id = ?",
                (encrypt_text(wallet_address), int(user_id)),
            )
        conn.commit()
    finally:
        conn.close()


def get_candidate_params(candidate_id: int) -> Optional[Dict[str, Any]]:
    conn = _conn()
    try:
        row = conn.execute("SELECT params_enc FROM candidates WHERE id = ?", (int(candidate_id),)).fetchone()
        if not row:
            return None
        raw = decrypt_text(row["params_enc"])
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None
    finally:
        conn.close()


def get_pool(pool_id: int) -> Optional[Dict[str, Any]]:
    conn = _conn()
    try:
        row = conn.execute("SELECT * FROM factor_pools WHERE id = ?", (int(pool_id),)).fetchone()
        if not row:
            return None
        d = dict(row)
        try:
            d["grid_spec"] = json.loads(d.get("grid_spec_json") or "{}")
        except Exception:
            d["grid_spec"] = {}
        try:
            d["risk_spec"] = json.loads(d.get("risk_spec_json") or "{}")
        except Exception:
            d["risk_spec"] = {}
        return d
    finally:
        conn.close()


def get_strategy_with_params(strategy_id: int) -> Optional[Dict[str, Any]]:
    conn = _conn()
    try:
        row = conn.execute(
            """
            SELECT st.*, s.audit_json, c.params_enc, c.metrics_json
            FROM strategies st
            JOIN submissions s ON s.id = st.submission_id
            JOIN candidates c ON c.id = s.candidate_id
            WHERE st.id = ?
            """,
            (int(strategy_id),),
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        raw = decrypt_text(d.get("params_enc"))
        d["params_json"] = json.loads(raw) if raw else None
        try:
            d["metrics"] = json.loads(d.get("metrics_json") or "{}")
        except Exception:
            d["metrics"] = {}
        try:
            d["audit"] = json.loads(d.get("audit_json") or "{}")
        except Exception:
            d["audit"] = {}
        return d
    finally:
        conn.close()


def list_task_overview(limit: int = 500) -> List[Dict[str, Any]]:
    conn = _conn()
    try:
        rows = conn.execute(
            """
            SELECT t.*, u.username, p.name AS pool_name, p.symbol, p.timeframe_min, p.family
            FROM tasks t
            JOIN users u ON u.id = t.user_id
            JOIN factor_pools p ON p.id = t.pool_id
            ORDER BY t.assigned_at DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            try:
                d["progress"] = json.loads(d.get("progress_json") or "{}")
            except Exception:
                d["progress"] = {}
            out.append(d)
        return out
    finally:
        conn.close()


def get_global_progress_snapshot(cycle_id: int) -> Dict[str, Any]:
    """Return a global snapshot for progress visualization.

    This is intentionally read-only and optimized for UI. It aggregates tasks by pool and
    includes per-partition status so the UI can render a partition map.
    """
    conn = _conn()
    try:
        system_uid = _get_or_create_system_user_id(conn)
        rows = conn.execute(
            """
            SELECT
                t.id,
                t.cycle_id,
                t.pool_id,
                p.name AS pool_name,
                p.symbol,
                p.timeframe_min,
                p.years,
                p.family,
                t.partition_idx,
                t.partition_total AS num_partitions,
                t.status,
                t.user_id,
                u.username,
                t.assigned_at,
                t.started_at,
                t.finished_at,
                t.last_heartbeat,
                t.estimated_combos,
                t.progress_json
            FROM tasks t
            JOIN factor_pools p ON p.id = t.pool_id
            LEFT JOIN users u ON u.id = t.user_id
            WHERE t.cycle_id = ? AND p.active = 1
            ORDER BY p.id ASC, t.partition_idx ASC
            """,
            (int(cycle_id),),
        ).fetchall()

        by_pool: Dict[int, Dict[str, Any]] = {}
        for r in rows:
            d = dict(r)
            pid = int(d.get("pool_id") or 0)
            if pid not in by_pool:
                by_pool[pid] = {
                    "pool_id": pid,
                    "pool_name": str(d.get("pool_name") or ""),
                    "symbol": str(d.get("symbol") or ""),
                    "timeframe_min": int(d.get("timeframe_min") or 0),
                    "years": int(d.get("years") or 0),
                    "family": str(d.get("family") or ""),
                    "num_partitions": int(d.get("num_partitions") or 1),
                    "tasks": [],
                }
            try:
                prog = json.loads(d.get("progress_json") or "{}")
            except Exception:
                prog = {}
            by_pool[pid]["tasks"].append(
                {
                    "task_id": int(d.get("id") or 0),
                    "partition_idx": int(d.get("partition_idx") or 0),
                    "num_partitions": int(d.get("num_partitions") or 1),
                    "status": str(d.get("status") or ""),
                    "user_id": int(d.get("user_id") or 0),
                    "username": str(d.get("username") or ""),
                    "assigned_at": str(d.get("assigned_at") or ""),
                    "started_at": str(d.get("started_at") or ""),
                    "finished_at": str(d.get("finished_at") or ""),
                    "last_heartbeat": str(d.get("last_heartbeat") or ""),
                    "estimated_combos": int(d.get("estimated_combos") or 0),
                    "progress": prog,
                }
            )

        pools = list(by_pool.values())
        return {
            "cycle_id": int(cycle_id),
            "system_user_id": int(system_uid),
            "pools": pools,
            "ts": utc_now_iso(),
        }
    finally:
        conn.close()



# ---- Worker orchestration, API tokens, and monitoring ----

_SYSTEM_USERNAME = "__system__"


def _row_id_value(row: Any) -> int:
    if row is None:
        return 0
    if isinstance(row, dict):
        return int(row.get("id") or 0)
    try:
        return int(row[0])
    except Exception:
        return 0


def _get_or_create_system_user_id(conn: Any) -> int:
    row = conn.execute("SELECT id FROM users WHERE username = ? LIMIT 1", (_SYSTEM_USERNAME,)).fetchone()
    if row:
        return _row_id_value(row)

    pw = hash_password(random_token(16))
    sql = "INSERT INTO users(username, password_hash, role, wallet_address_enc, created_at, disabled, login_fail_count, run_enabled) VALUES(?,?,?,?,?,?,?,?)"
    _insert_id(conn, sql, (_SYSTEM_USERNAME, pw, "admin", None, utc_now_iso(), 1, 0, 0))

    row2 = conn.execute("SELECT id FROM users WHERE username = ? LIMIT 1", (_SYSTEM_USERNAME,)).fetchone()
    if not row2:
        raise RuntimeError("system_user_create_failed")
    return _row_id_value(row2)


def get_system_user_id() -> int:
    conn = _conn()
    try:
        return int(_get_or_create_system_user_id(conn))
    finally:
        conn.close()


def _ensure_tasks_columns(conn: Any) -> None:
    try:
        if _DB_KIND == "sqlite":
            cols = [r["name"] for r in conn.execute("PRAGMA table_info(tasks)").fetchall()]

            def add_col(defn: str) -> None:
                conn.execute(f"ALTER TABLE tasks ADD COLUMN {defn}")

            if "partition_total" not in cols:
                add_col("partition_total INTEGER NOT NULL DEFAULT 0")
            if "lease_id" not in cols:
                add_col("lease_id TEXT")
            if "lease_worker_id" not in cols:
                add_col("lease_worker_id TEXT")
            if "lease_expires_at" not in cols:
                add_col("lease_expires_at TEXT")
            if "attempt" not in cols:
                add_col("attempt INTEGER NOT NULL DEFAULT 0")
            if "estimated_combos" not in cols:
                add_col("estimated_combos INTEGER NOT NULL DEFAULT 0")
            if "priority" not in cols:
                add_col("priority INTEGER NOT NULL DEFAULT 0")

            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_cycle_status_lease ON tasks(cycle_id, status, lease_expires_at)")
            conn.execute("UPDATE tasks SET partition_total = (SELECT num_partitions FROM factor_pools p WHERE p.id = tasks.pool_id) WHERE (partition_total IS NULL OR partition_total <= 0)")
        else:
            rows = conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'tasks'").fetchall()
            cols = set()
            for r in rows:
                if isinstance(r, dict):
                    cols.add(str(r.get("column_name") or ""))
                else:
                    cols.add(str(r[0] if r else ""))

            def add_col_pg(defn: str) -> None:
                conn.execute(f"ALTER TABLE tasks ADD COLUMN {defn}")

            if "partition_total" not in cols:
                add_col_pg("partition_total INTEGER NOT NULL DEFAULT 0")
            if "lease_id" not in cols:
                add_col_pg("lease_id TEXT")
            if "lease_worker_id" not in cols:
                add_col_pg("lease_worker_id TEXT")
            if "lease_expires_at" not in cols:
                add_col_pg("lease_expires_at TEXT")
            if "attempt" not in cols:
                add_col_pg("attempt INTEGER NOT NULL DEFAULT 0")
            if "estimated_combos" not in cols:
                add_col_pg("estimated_combos BIGINT NOT NULL DEFAULT 0")
            if "priority" not in cols:
                add_col_pg("priority INTEGER NOT NULL DEFAULT 0")

            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_cycle_status_lease ON tasks(cycle_id, status, lease_expires_at)")
            conn.execute(
                "UPDATE tasks t SET partition_total = p.num_partitions FROM factor_pools p WHERE p.id = t.pool_id AND (t.partition_total IS NULL OR t.partition_total <= 0)"
            )

        # Backfill estimated_combos for recent tasks only to keep startup cheap.
        rows = conn.execute(
            """
            SELECT t.id, t.partition_idx, t.partition_total, p.family, p.grid_spec_json, p.risk_spec_json
            FROM tasks t
            JOIN factor_pools p ON p.id = t.pool_id
            WHERE (t.estimated_combos IS NULL OR t.estimated_combos <= 0)
            ORDER BY t.id DESC
            LIMIT 500
            """
        ).fetchall()
        for r in rows:
            if isinstance(r, dict):
                d = dict(r)
            else:
                try:
                    d = {
                        "id": r[0],
                        "partition_idx": r[1],
                        "partition_total": r[2],
                        "family": r[3],
                        "grid_spec_json": r[4],
                        "risk_spec_json": r[5],
                    }
                except Exception:
                    continue

            pool = {"family": d.get("family"), "grid_spec_json": d.get("grid_spec_json"), "risk_spec_json": d.get("risk_spec_json")}
            est = _estimate_task_work(pool, int(d.get("partition_total") or 1), int(d.get("partition_idx") or 0))
            try:
                conn.execute("UPDATE tasks SET estimated_combos = ? WHERE id = ?", (int(est), int(d["id"])))
            except Exception:
                pass

    except Exception:
        return


def _range_count(a: Any, b: Any, step: Any) -> int:
    try:
        a2 = float(a)
        b2 = float(b)
        s2 = float(step)
    except Exception:
        return 1
    if s2 <= 0 or b2 < a2:
        return 1
    n = int((b2 - a2) / s2) + 1
    return max(1, n)


def _estimate_grid_count(grid_spec: Dict[str, Any]) -> int:
    if not isinstance(grid_spec, dict):
        return 1
    groups: Dict[str, Dict[str, Any]] = {}
    for k, v in grid_spec.items():
        if not isinstance(k, str):
            continue
        if k.endswith("_min"):
            p = k[:-4]
            groups.setdefault(p, {})["min"] = v
        elif k.endswith("_max"):
            p = k[:-4]
            groups.setdefault(p, {})["max"] = v
        elif k.endswith("_step"):
            p = k[:-5]
            groups.setdefault(p, {})["step"] = v

    total = 1
    for _, g in groups.items():
        total *= _range_count(g.get("min"), g.get("max"), g.get("step"))
        if total > 10_000_000:
            return int(total)
    return int(max(1, total))


def _estimate_risk_count(family: str, risk_spec: Dict[str, Any]) -> int:
    if not isinstance(risk_spec, dict):
        return 1
    tp_n = _range_count(risk_spec.get("tp_min", 0.3), risk_spec.get("tp_max", 1.2), risk_spec.get("tp_step", 0.1))
    sl_n = _range_count(risk_spec.get("sl_min", 0.3), risk_spec.get("sl_max", 1.2), risk_spec.get("sl_step", 0.1))
    mh_n = _range_count(risk_spec.get("max_hold_min", 4), risk_spec.get("max_hold_max", 80), risk_spec.get("max_hold_step", 4))

    fam = str(family or "")
    if fam in ("TEMA_RSI", "LaguerreRSI_TEMA"):
        return int(max(1, mh_n))
    return int(max(1, tp_n * sl_n * mh_n))


def _estimate_task_work(pool_row: Dict[str, Any], partition_total: int, partition_idx: int) -> int:
    partition_total = max(1, int(partition_total))
    partition_idx = max(0, int(partition_idx))
    family = str(pool_row.get("family") or "")
    try:
        grid_spec = json.loads(pool_row.get("grid_spec_json") or "{}")
    except Exception:
        grid_spec = {}
    try:
        risk_spec = json.loads(pool_row.get("risk_spec_json") or "{}")
    except Exception:
        risk_spec = {}

    grid_n = _estimate_grid_count(grid_spec)
    risk_n = _estimate_risk_count(family, risk_spec)
    per_part = int(math.ceil(grid_n / float(partition_total)))
    return int(max(0, per_part * risk_n))


def _prune_monitoring_tables(conn: Any) -> None:
    try:
        days = int(get_setting(conn, "monitor_retention_days", 14))
        days = max(1, min(365, days))
        cutoff = _iso(_utc_now() - timedelta(days=days))
        conn.execute("DELETE FROM api_request_log WHERE ts < ?", (str(cutoff),))
        conn.execute("DELETE FROM worker_events WHERE ts < ?", (str(cutoff),))
    except Exception:
        return


def ensure_cycle_tasks(cycle_id: Optional[int] = None) -> int:
    conn = _conn()
    try:
        if cycle_id is None:
            row = conn.execute("SELECT id FROM cycles WHERE is_active = 1 ORDER BY id DESC LIMIT 1").fetchone()
            if not row:
                return 0
            cycle_id = int(row["id"] if isinstance(row, dict) else row[0])

        cycle_id = int(cycle_id)
        system_uid = _get_or_create_system_user_id(conn)

        lease_seconds = int(get_setting(conn, "task_lease_seconds", int(get_setting(conn, "task_lease_minutes", 180)) * 60))
        _cleanup_stale_running_tasks(conn, cycle_id, lease_seconds)

        pools = conn.execute(
            "SELECT * FROM factor_pools WHERE cycle_id = ? AND active = 1 ORDER BY id ASC",
            (int(cycle_id),),
        ).fetchall()

        target_seconds = float(get_setting(conn, "task_target_seconds", 900))
        default_cps = float(get_setting(conn, "default_worker_cps", 50.0))
        target_combos = int(max(200, target_seconds * max(1e-6, default_cps)))

        created = 0
        now_iso = utc_now_iso()

        for p in pools:
            pool = dict(p)
            pool_id = int(pool["id"])

            existing = conn.execute(
                "SELECT partition_total FROM tasks WHERE cycle_id = ? AND pool_id = ? ORDER BY id ASC LIMIT 1",
                (int(cycle_id), int(pool_id)),
            ).fetchone()
            if existing:
                part_total = int((existing["partition_total"] if isinstance(existing, dict) else existing[0]) or 1)
            else:
                part_total = int(pool.get("num_partitions") or 0)
                if part_total <= 0:
                    try:
                        grid_spec = json.loads(pool.get("grid_spec_json") or "{}")
                    except Exception:
                        grid_spec = {}
                    try:
                        risk_spec = json.loads(pool.get("risk_spec_json") or "{}")
                    except Exception:
                        risk_spec = {}
                    grid_n = _estimate_grid_count(grid_spec)
                    risk_n = _estimate_risk_count(str(pool.get("family") or ""), risk_spec)
                    total = int(max(1, grid_n * risk_n))
                    part_total = int(max(1, math.ceil(total / float(target_combos))))
                    part_total = int(min(part_total, max(1, grid_n)))

            base_progress = {
                "combos_total": 0,
                "combos_done": 0,
                "best_score": None,
                "best_candidate_id": None,
                "best_any_score": None,
                "best_any_metrics": None,
                "best_any_params": None,
                "best_any_passed": False,
                "phase": "queued",
                "phase_progress": 0.0,
                "phase_msg": "",
                "last_error": None,
                "elapsed_s": 0.0,
                "speed_cps": 0.0,
                "eta_s": None,
                "checkpoint_candidates": [],
                "updated_at": now_iso,
            }

            for part_idx in range(int(part_total)):
                est = _estimate_task_work(pool, int(part_total), int(part_idx))
                if _DB_KIND == "sqlite":
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO tasks(
                            cycle_id, pool_id, user_id, partition_idx, partition_total,
                            status, assigned_at, estimated_combos, priority, progress_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?)
                        """,
                        (int(cycle_id), int(pool_id), int(system_uid), int(part_idx), int(part_total), "assigned", str(now_iso), int(est), 0, json_dumps(base_progress)),
                    )
                    if int(conn.execute("SELECT changes() AS c").fetchone()["c"]) > 0:
                        created += 1
                else:
                    conn.execute(
                        """
                        INSERT INTO tasks(
                            cycle_id, pool_id, user_id, partition_idx, partition_total,
                            status, assigned_at, estimated_combos, priority, progress_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT (cycle_id, pool_id, partition_idx) DO NOTHING
                        """,
                        (int(cycle_id), int(pool_id), int(system_uid), int(part_idx), int(part_total), "assigned", str(now_iso), int(est), 0, json_dumps(base_progress)),
                    )

        _prune_monitoring_tables(conn)
        conn.commit()
        return int(created)
    finally:
        conn.close()


def ensure_tasks_for_pool(pool_id: int) -> None:
    conn = _conn()
    try:
        row = conn.execute("SELECT cycle_id FROM factor_pools WHERE id = ? LIMIT 1", (int(pool_id),)).fetchone()
        if not row:
            return
        cycle_id = int(row["cycle_id"] if isinstance(row, dict) else row[0])

        # If tasks already exist for this pool in this cycle, do nothing.
        existing = conn.execute("SELECT 1 FROM tasks WHERE cycle_id = ? AND pool_id = ? LIMIT 1", (int(cycle_id), int(pool_id))).fetchone()
        if existing:
            return
    finally:
        conn.close()

    ensure_cycle_tasks(cycle_id)


def _active_pool_lease_counts(conn: Any, cycle_id: int, now_iso: str) -> Dict[int, int]:
    rows = conn.execute(
        """
        SELECT pool_id, COUNT(1) AS c
        FROM tasks
        WHERE cycle_id = ? AND status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at >= ?
        GROUP BY pool_id
        """,
        (int(cycle_id), str(now_iso)),
    ).fetchall()
    out: Dict[int, int] = {}
    for r in rows:
        d = dict(r)
        out[int(d.get("pool_id"))] = int(d.get("c") or 0)
    return out


def _select_claim_candidates(conn: Any, cycle_id: int, now_iso: str, limit: int) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            t.id, t.pool_id, t.partition_idx, t.partition_total, t.status, t.assigned_at,
            t.estimated_combos, t.priority, t.lease_expires_at
        FROM tasks t
        JOIN factor_pools p ON p.id = t.pool_id
        WHERE
            t.cycle_id = ?
            AND p.active = 1
            AND t.status IN ('assigned','running')
            AND (
                t.status = 'assigned'
                OR (t.status = 'running' AND t.lease_expires_at IS NOT NULL AND t.lease_expires_at < ?)
            )
        ORDER BY t.priority DESC, t.assigned_at ASC
        LIMIT ?
        """,
        (int(cycle_id), str(now_iso), int(limit)),
    ).fetchall()
    return [dict(r) for r in rows]


def upsert_worker(worker_id: str, user_id: int, version: str, protocol: int, name: Optional[str] = None, meta: Optional[Dict[str, Any]] = None) -> None:
    worker_id = str(worker_id or "").strip()
    if not worker_id:
        return
    conn = _conn()
    try:
        now_iso = utc_now_iso()
        user_id = int(user_id)
        version = str(version or "")
        protocol = int(protocol)

        meta_json = json_dumps(meta or {})
        row = conn.execute("SELECT id FROM workers WHERE worker_id = ? LIMIT 1", (str(worker_id),)).fetchone()
        if row:
            conn.execute(
                "UPDATE workers SET user_id = ?, version = ?, protocol = ?, last_seen_at = ?, meta_json = ?, name = COALESCE(?, name) WHERE worker_id = ?",
                (int(user_id), str(version), int(protocol), str(now_iso), str(meta_json), str(name) if name else None, str(worker_id)),
            )
        else:
            conn.execute(
                """
                INSERT INTO workers(
                    worker_id, user_id, name, version, protocol,
                    created_at, last_seen_at, last_heartbeat_at, last_task_id, avg_cps, meta_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (str(worker_id), int(user_id), str(name) if name else None, str(version), int(protocol), str(now_iso), str(now_iso), str(now_iso), None, 0.0, str(meta_json)),
            )
        conn.commit()
    finally:
        conn.close()


def worker_heartbeat(worker_id: str, user_id: int, task_id: Optional[int] = None) -> None:
    worker_id = str(worker_id or "").strip()
    if not worker_id:
        return
    conn = _conn()
    try:
        now_iso = utc_now_iso()
        if task_id is None:
            conn.execute("UPDATE workers SET last_seen_at = ?, last_heartbeat_at = ? WHERE worker_id = ? AND user_id = ?", (str(now_iso), str(now_iso), str(worker_id), int(user_id)))
        else:
            conn.execute("UPDATE workers SET last_seen_at = ?, last_heartbeat_at = ?, last_task_id = ? WHERE worker_id = ? AND user_id = ?", (str(now_iso), str(now_iso), int(task_id), str(worker_id), int(user_id)))
        conn.commit()
    finally:
        conn.close()


def list_workers(limit: int = 200) -> List[Dict[str, Any]]:
    conn = _conn()
    try:
        limit = int(max(1, min(2000, int(limit))))
        rows = conn.execute(
            """
            SELECT w.*, u.username
            FROM workers w
            JOIN users u ON u.id = w.user_id
            ORDER BY w.last_seen_at DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        now_dt = _utc_now()
        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            seen = _parse_iso(d.get("last_seen_at")) or now_dt
            hb = _parse_iso(d.get("last_heartbeat_at")) or now_dt
            d["seen_age_s"] = float((now_dt - seen).total_seconds())
            d["heartbeat_age_s"] = float((now_dt - hb).total_seconds())
            out.append(d)
        return out
    finally:
        conn.close()


def insert_worker_event(user_id: Optional[int], worker_id: Optional[str], event: str, detail: Optional[Dict[str, Any]] = None) -> None:
    conn = _conn()
    try:
        now_iso = utc_now_iso()
        conn.execute(
            "INSERT INTO worker_events(ts, user_id, worker_id, event, detail_json) VALUES(?,?,?,?,?)",
            (str(now_iso), int(user_id) if user_id is not None else None, str(worker_id) if worker_id else None, str(event or ""), json_dumps(detail or {})),
        )
        _prune_monitoring_tables(conn)
        conn.commit()
    finally:
        conn.close()


def list_worker_events(limit: int = 200) -> List[Dict[str, Any]]:
    conn = _conn()
    try:
        limit = int(max(1, min(2000, int(limit))))
        rows = conn.execute("SELECT * FROM worker_events ORDER BY ts DESC LIMIT ?", (int(limit),)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def list_chat_messages(limit: int = 50) -> List[Dict[str, Any]]:
    conn = _conn()
    try:
        limit = int(max(1, min(200, int(limit))))
        rows = conn.execute(
            """
            SELECT we.ts, we.user_id, u.username, we.detail_json
            FROM worker_events we
            LEFT JOIN users u ON u.id = we.user_id
            WHERE we.event = 'chat_message'
            ORDER BY we.ts DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            detail = {}
            try:
                detail = json.loads(d.get("detail_json") or "{}")
            except Exception:
                detail = {}
            out.append(
                {
                    "ts": str(d.get("ts") or ""),
                    "user_id": int(d.get("user_id") or 0),
                    "username": str(d.get("username") or ""),
                    "text": str(detail.get("text") or ""),
                }
            )
        out.reverse()
        return out
    finally:
        conn.close()


def log_api_request(
    ts_iso: str,
    user_id: Optional[int],
    worker_id: Optional[str],
    token_id: Optional[int],
    method: str,
    path: str,
    status_code: int,
    duration_ms: float,
    detail: Optional[Dict[str, Any]] = None,
) -> None:
    conn = _conn()
    try:
        conn.execute(
            """
            INSERT INTO api_request_log(
                ts, user_id, worker_id, token_id, method, path, status_code, duration_ms, detail_json
            ) VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                str(ts_iso),
                int(user_id) if user_id is not None else None,
                str(worker_id) if worker_id else None,
                int(token_id) if token_id is not None else None,
                str(method or ""),
                str(path or ""),
                int(status_code),
                float(duration_ms),
                json_dumps(detail or {}),
            ),
        )
        _prune_monitoring_tables(conn)
        conn.commit()
    finally:
        conn.close()


def list_api_requests(limit: int = 200) -> List[Dict[str, Any]]:
    conn = _conn()
    try:
        limit = int(max(1, min(2000, int(limit))))
        rows = conn.execute("SELECT * FROM api_request_log ORDER BY ts DESC LIMIT ?", (int(limit),)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def create_api_token(user_id: int, ttl_seconds: int = 86400, name: str = "worker") -> Dict[str, Any]:
    conn = _conn()
    try:
        user_id = int(user_id)
        ttl_seconds = int(max(300, int(ttl_seconds)))
        name = str(name or "worker").strip()[:64]

        raw = random_token(32)
        token_hash = stable_hmac_sha256(get_hmac_key(), raw)

        now = _utc_now()
        issued_at = _iso(now)
        expires_at = _iso(now + timedelta(seconds=ttl_seconds))

        token_id = _insert_id(
            conn,
            "INSERT INTO api_tokens(user_id, name, token_hash, issued_at, expires_at, revoked_at, last_seen_at) VALUES(?,?,?,?,?,?,?)",
            (int(user_id), str(name), str(token_hash), str(issued_at), str(expires_at), None, None),
        )
        conn.commit()
        return {"token": str(raw), "token_id": int(token_id), "issued_at": str(issued_at), "expires_at": str(expires_at), "name": str(name)}
    finally:
        conn.close()


def list_api_tokens(user_id: int, include_revoked: bool = False, limit: int = 200) -> List[Dict[str, Any]]:
    conn = _conn()
    try:
        user_id = int(user_id)
        limit = int(max(1, min(1000, int(limit))))
        if include_revoked:
            rows = conn.execute("SELECT * FROM api_tokens WHERE user_id = ? ORDER BY issued_at DESC LIMIT ?", (int(user_id), int(limit))).fetchall()
        else:
            now_iso = utc_now_iso()
            rows = conn.execute(
                """
                SELECT * FROM api_tokens
                WHERE user_id = ? AND revoked_at IS NULL AND expires_at >= ?
                ORDER BY issued_at DESC
                LIMIT ?
                """,
                (int(user_id), str(now_iso), int(limit)),
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            h = str(d.get("token_hash") or "")
            d["display"] = (h[:6] + "..." + h[-6:]) if len(h) >= 12 else h
            out.append(d)
        return out
    finally:
        conn.close()


def revoke_api_token(token_id: int, user_id: Optional[int] = None) -> bool:
    conn = _conn()
    try:
        now_iso = utc_now_iso()
        if user_id is None:
            cur = conn.execute("UPDATE api_tokens SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL", (str(now_iso), int(token_id)))
        else:
            cur = conn.execute(
                "UPDATE api_tokens SET revoked_at = ? WHERE id = ? AND user_id = ? AND revoked_at IS NULL",
                (str(now_iso), int(token_id), int(user_id)),
            )
        conn.commit()
        return bool(int(cur.rowcount or 0) > 0)
    finally:
        conn.close()


def verify_api_token(raw_token: str) -> Optional[Dict[str, Any]]:
    raw_token = str(raw_token or "").strip()
    if not raw_token:
        return None
    conn = _conn()
    try:
        token_hash = stable_hmac_sha256(get_hmac_key(), raw_token)
        now_iso = utc_now_iso()
        t = conn.execute("SELECT * FROM api_tokens WHERE token_hash = ? LIMIT 1", (str(token_hash),)).fetchone()
        if not t:
            return None
        td = dict(t)
        if td.get("revoked_at") is not None:
            return None
        if str(td.get("expires_at") or "") < str(now_iso):
            return None
        u = conn.execute("SELECT * FROM users WHERE id = ? LIMIT 1", (int(td["user_id"]),)).fetchone()
        if not u:
            return None
        ud = dict(u)
        if int(ud.get("disabled") or 0) != 0:
            return None
        return {"user": ud, "token": td}
    finally:
        conn.close()


def touch_api_token(token_id: int, ip: Optional[str] = None, user_agent: Optional[str] = None) -> None:
    conn = _conn()
    try:
        now_iso = utc_now_iso()
        conn.execute(
            "UPDATE api_tokens SET last_seen_at = ?, last_ip = COALESCE(?, last_ip), last_user_agent = COALESCE(?, last_user_agent) WHERE id = ?",
            (str(now_iso), str(ip) if ip else None, str(user_agent) if user_agent else None, int(token_id)),
        )
        conn.commit()
    finally:
        conn.close()


def claim_next_task(user_id: int, worker_id: str) -> Optional[Dict[str, Any]]:
    worker_id = str(worker_id or "").strip()
    if not worker_id:
        return None

    conn = _conn()
    try:
        user_id = int(user_id)

        cycle = conn.execute("SELECT id FROM cycles WHERE is_active = 1 ORDER BY id DESC LIMIT 1").fetchone()
        if not cycle:
            return None
        cycle_id = int(cycle["id"] if isinstance(cycle, dict) else cycle[0])

        # Fast-path seeding: only seed if a pool has no tasks yet.
        pools = conn.execute("SELECT id FROM factor_pools WHERE cycle_id = ? AND active = 1", (int(cycle_id),)).fetchall()
        for pr in pools:
            pid = int(pr["id"] if isinstance(pr, dict) else pr[0])
            exists = conn.execute("SELECT 1 FROM tasks WHERE cycle_id = ? AND pool_id = ? LIMIT 1", (int(cycle_id), int(pid))).fetchone()
            if not exists:
                conn.close()
                ensure_cycle_tasks(cycle_id)
                conn = _conn()
                break

        now_iso = utc_now_iso()

        max_leases = int(get_setting(conn, "max_active_leases_per_user", 4))
        if max_leases > 0:
            row = conn.execute(
                """
                SELECT COUNT(1) AS c
                FROM tasks
                WHERE user_id = ? AND status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at >= ?
                """,
                (int(user_id), str(now_iso)),
            ).fetchone()
            c = int((row["c"] if isinstance(row, dict) else row[0]) or 0)
            if c >= max_leases:
                return None

        lease_seconds = int(get_setting(conn, "task_lease_seconds", int(get_setting(conn, "task_lease_minutes", 180)) * 60))
        lease_expires_at = _iso(_utc_now() + timedelta(seconds=max(30, lease_seconds)))

        wrow = conn.execute("SELECT avg_cps FROM workers WHERE worker_id = ? LIMIT 1", (str(worker_id),)).fetchone()
        avg_cps = float((wrow["avg_cps"] if (wrow and isinstance(wrow, dict)) else (wrow[0] if wrow else 0.0)) or 0.0)
        if avg_cps <= 0:
            avg_cps = float(get_setting(conn, "default_worker_cps", 50.0))

        target_seconds = float(get_setting(conn, "task_target_seconds", 900))
        target_combos = float(max(200.0, avg_cps * target_seconds))

        candidates = _select_claim_candidates(conn, cycle_id, now_iso, limit=250)
        if not candidates:
            return None

        pool_counts = _active_pool_lease_counts(conn, cycle_id, now_iso)
        now_dt = _utc_now()

        scored: List[Tuple[float, Dict[str, Any]]] = []
        for c in candidates:
            est = float(c.get("estimated_combos") or 0.0)
            if est <= 0:
                est = float(target_combos)

            mismatch = abs(est - target_combos) / max(1.0, target_combos)
            prio = float(c.get("priority") or 0.0)

            assigned_at = _parse_iso(c.get("assigned_at"))
            age_s = float((now_dt - assigned_at).total_seconds()) if assigned_at else 0.0
            age_bonus = min(1.5, age_s / 3600.0)

            status = str(c.get("status") or "")
            reclaim_bonus = -3.0 if status == "running" else 0.0

            pool_id = int(c.get("pool_id") or 0)
            pool_penalty = 0.15 * float(pool_counts.get(pool_id, 0))

            score = 5.0 * mismatch + pool_penalty - 0.25 * age_bonus - 0.1 * prio + reclaim_bonus + random.random() * 0.05
            scored.append((float(score), c))

        scored.sort(key=lambda x: x[0])

        for _, c in scored[:60]:
            task_id = int(c["id"])
            lease_id = random_token(16)

            cur = conn.execute(
                """
                UPDATE tasks
                SET
                    status = 'running',
                    user_id = ?,
                    lease_id = ?,
                    lease_worker_id = ?,
                    lease_expires_at = ?,
                    last_heartbeat = ?,
                    started_at = COALESCE(started_at, ?),
                    attempt = attempt + 1
                WHERE
                    id = ?
                    AND (
                        status = 'assigned'
                        OR (status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?)
                    )
                """,
                (int(user_id), str(lease_id), str(worker_id), str(lease_expires_at), str(now_iso), str(now_iso), int(task_id), str(now_iso)),
            )
            if int(cur.rowcount or 0) == 1:
                conn.execute("UPDATE workers SET last_seen_at = ?, last_heartbeat_at = ?, last_task_id = ? WHERE worker_id = ?", (str(now_iso), str(now_iso), int(task_id), str(worker_id)))
                conn.commit()
                task = get_task(int(task_id))
                if task is None:
                    return None
                td = dict(task)
                td["lease_id"] = str(lease_id)
                return td

        return None
    finally:
        conn.close()


def update_task_progress_with_lease(task_id: int, user_id: int, worker_id: str, lease_id: str, progress: Dict[str, Any]) -> bool:
    worker_id = str(worker_id or "").strip()
    lease_id = str(lease_id or "").strip()
    if not worker_id or not lease_id:
        return False

    conn = _conn()
    try:
        row = conn.execute(
            "SELECT status, user_id, lease_id, lease_worker_id FROM tasks WHERE id = ?",
            (int(task_id),),
        ).fetchone()
        if not row:
            return False
        d = dict(row)
        if str(d.get("status") or "") != "running":
            return False
        if int(d.get("user_id") or 0) != int(user_id):
            return False
        if str(d.get("lease_id") or "") != lease_id:
            return False
        if str(d.get("lease_worker_id") or "") != worker_id:
            return False

        lease_seconds = int(get_setting(conn, "task_lease_seconds", int(get_setting(conn, "task_lease_minutes", 180)) * 60))
        lease_expires_at = _iso(_utc_now() + timedelta(seconds=max(30, lease_seconds)))
        now_iso = utc_now_iso()

        prog = dict(progress or {})
        prog["updated_at"] = now_iso

        conn.execute(
            """
            UPDATE tasks
            SET progress_json = ?, last_heartbeat = ?, lease_expires_at = ?
            WHERE id = ? AND lease_id = ? AND lease_worker_id = ? AND user_id = ? AND status = 'running'
            """,
            (json_dumps(prog), str(now_iso), str(lease_expires_at), int(task_id), str(lease_id), str(worker_id), int(user_id)),
        )

        speed = float(prog.get("speed_cps") or 0.0)
        if speed > 0:
            w = conn.execute("SELECT avg_cps FROM workers WHERE worker_id = ? LIMIT 1", (str(worker_id),)).fetchone()
            old = float((w["avg_cps"] if (w and isinstance(w, dict)) else (w[0] if w else 0.0)) or 0.0)
            new_avg = 0.8 * old + 0.2 * float(speed)
            conn.execute(
                "UPDATE workers SET avg_cps = ?, last_seen_at = ?, last_heartbeat_at = ?, last_task_id = ? WHERE worker_id = ?",
                (float(new_avg), str(now_iso), str(now_iso), int(task_id), str(worker_id)),
            )
        else:
            conn.execute("UPDATE workers SET last_seen_at = ?, last_heartbeat_at = ?, last_task_id = ? WHERE worker_id = ?", (str(now_iso), str(now_iso), int(task_id), str(worker_id)))

        conn.commit()
        return True
    finally:
        conn.close()


def release_task_with_lease(task_id: int, user_id: int, worker_id: str, lease_id: str, progress: Dict[str, Any]) -> bool:
    worker_id = str(worker_id or "").strip()
    lease_id = str(lease_id or "").strip()
    if not worker_id or not lease_id:
        return False

    conn = _conn()
    try:
        row = conn.execute("SELECT status, user_id, lease_id, lease_worker_id FROM tasks WHERE id = ?", (int(task_id),)).fetchone()
        if not row:
            return False
        d = dict(row)
        if str(d.get("status") or "") != "running":
            return False
        if int(d.get("user_id") or 0) != int(user_id):
            return False
        if str(d.get("lease_id") or "") != lease_id:
            return False
        if str(d.get("lease_worker_id") or "") != worker_id:
            return False

        system_uid = _get_or_create_system_user_id(conn)
        now_iso = utc_now_iso()
        prog = dict(progress or {})
        prog["updated_at"] = now_iso

        conn.execute(
            """
            UPDATE tasks
            SET
                status = 'assigned',
                user_id = ?,
                started_at = NULL,
                finished_at = NULL,
                last_heartbeat = ?,
                lease_id = NULL,
                lease_worker_id = NULL,
                lease_expires_at = NULL,
                progress_json = ?
            WHERE id = ? AND lease_id = ? AND lease_worker_id = ? AND status = 'running' AND user_id = ?
            """,
            (int(system_uid), str(now_iso), json_dumps(prog), int(task_id), str(lease_id), str(worker_id), int(user_id)),
        )
        conn.execute("UPDATE workers SET last_seen_at = ?, last_heartbeat_at = ?, last_task_id = ? WHERE worker_id = ?", (str(now_iso), str(now_iso), int(task_id), str(worker_id)))
        conn.commit()
        return True
    finally:
        conn.close()


def _clear_candidates_conn(conn: Any, task_id: int) -> None:
    conn.execute("DELETE FROM candidates WHERE task_id = ?", (int(task_id),))


def _insert_candidate_conn(conn: Any, task_id: int, user_id: int, pool_id: int, params: Dict[str, Any], metrics: Dict[str, Any], score: float) -> int:
    sql = """
        INSERT INTO candidates(task_id, user_id, pool_id, params_json, metrics_json, score, created_at)
        VALUES(?,?,?,?,?,?,?)
    """
    return _insert_id(conn, sql, (int(task_id), int(user_id), int(pool_id), json_dumps(params), json_dumps(metrics), float(score), utc_now_iso()))


def finish_task_with_lease(task_id: int, user_id: int, worker_id: str, lease_id: str, candidates: List[Dict[str, Any]], final_progress: Dict[str, Any]) -> Optional[int]:
    worker_id = str(worker_id or "").strip()
    lease_id = str(lease_id or "").strip()
    if not worker_id or not lease_id:
        return None

    conn = _conn()
    try:
        row = conn.execute("SELECT id, status, user_id, lease_id, lease_worker_id, pool_id FROM tasks WHERE id = ?", (int(task_id),)).fetchone()
        if not row:
            return None
        d = dict(row)
        if str(d.get("status") or "") != "running":
            return None
        if int(d.get("user_id") or 0) != int(user_id):
            return None
        if str(d.get("lease_id") or "") != lease_id:
            return None
        if str(d.get("lease_worker_id") or "") != worker_id:
            return None

        pool_id = int(d.get("pool_id") or 0)

        _clear_candidates_conn(conn, int(task_id))
        best_candidate_id: Optional[int] = None

        for c in candidates or []:
            try:
                params = dict(c.get("params") or {})
                metrics = dict(c.get("metrics") or {})
                score = float(c.get("score") or 0.0)
            except Exception:
                continue
            cid = _insert_candidate_conn(conn, int(task_id), int(user_id), int(pool_id), params, metrics, float(score))
            if best_candidate_id is None:
                best_candidate_id = int(cid)

        prog = dict(final_progress or {})
        prog["best_candidate_id"] = int(best_candidate_id) if best_candidate_id is not None else None
        prog["updated_at"] = utc_now_iso()

        now_iso = utc_now_iso()
        conn.execute(
            """
            UPDATE tasks
            SET
                status = 'completed',
                finished_at = ?,
                last_heartbeat = ?,
                lease_id = NULL,
                lease_worker_id = NULL,
                lease_expires_at = NULL,
                progress_json = ?
            WHERE id = ? AND lease_id = ? AND lease_worker_id = ? AND status = 'running' AND user_id = ?
            """,
            (str(now_iso), str(now_iso), json_dumps(prog), int(task_id), str(lease_id), str(worker_id), int(user_id)),
        )
        conn.execute("UPDATE workers SET last_seen_at = ?, last_heartbeat_at = ?, last_task_id = ? WHERE worker_id = ?", (str(now_iso), str(now_iso), int(task_id), str(worker_id)))

        conn.commit()
        return int(best_candidate_id) if best_candidate_id is not None else None
    finally:
        conn.close()
