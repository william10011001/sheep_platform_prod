import json
import os
import random
import re
import time
import math
import html
import base64
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
# DB API (sqlite) — minimal set required for Auth + Admin bootstrap
# ─────────────────────────────────────────────────────────────────────────────
import sqlite3


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _conn() -> sqlite3.Connection:
    path = _db_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    except Exception as e:
        import traceback
        print(f"[DB ERROR] 無法建立資料庫目錄 {path}, 錯誤詳情: {e}\n{traceback.format_exc()}")

    conn = sqlite3.connect(path, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    try:
        conn.execute("PRAGMA foreign_keys = ON;")
    except Exception:
        pass
    try:
        conn.execute("PRAGMA journal_mode = WAL;")
    except Exception:
        pass
    try:
        conn.execute("PRAGMA synchronous = NORMAL;")
    except Exception:
        pass
    try:
        # 設定 60 秒等待時間，大幅降低 database is locked 機率
        conn.execute("PRAGMA busy_timeout = 60000;")
    except Exception:
        pass

    return conn

# [專家級修正] 新增重試裝飾器，用於關鍵寫入操作
def _retry_on_lock(func):
    import time
    def wrapper(*args, **kwargs):
        attempts = 0
        while attempts < 5:
            try:
                return func(*args, **kwargs)
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower():
                    attempts += 1
                    time.sleep(0.2 * attempts)
                else:
                    raise
        raise sqlite3.OperationalError("Database locked after 5 retries")
    return wrapper


def init_db() -> None:
    conn = _conn()
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
        try:
            conn.execute("ALTER TABLE users ADD COLUMN nickname TEXT DEFAULT ''")
        except Exception:
            pass
        
        # [專家級效能優化] 針對排行榜聚合查詢建立必要索引，避免全表掃描導致系統卡死
        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at)")
            # 複合索引優化 Leaderboard 統計 (updated_at + status)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_mining_tasks_updated_status ON mining_tasks(updated_at, status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_candidates_created_at ON candidates(created_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_payouts_created_at ON payouts(created_at)")
        except Exception:
            pass

        conn.commit()
    finally:
        conn.close()


def get_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    uname_norm = normalize_username(str(username or ""))
    if not uname_norm:
        return None
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT * FROM users WHERE username_norm = ? LIMIT 1",
            (uname_norm,),
        ).fetchone()
        return dict(row) if row else None
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
    conn = _conn()
    try:
        conn.execute("UPDATE users SET run_enabled = ? WHERE id = ?", (1 if enabled else 0, int(user_id)))
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
    import sqlite3
    if isinstance(arg1, sqlite3.Connection):
        k = str(arg2 or "").strip()
        default = arg3
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
            return json.loads(row["value"])
        except Exception:
            return row["value"]
    finally:
        conn.close()


def set_setting(arg1: Any, arg2: Any, arg3: Any = None) -> None:
    import sqlite3
    if isinstance(arg1, sqlite3.Connection):
        k = str(arg2 or "").strip()
        value = arg3
    else:
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
                # [週期切換專家邏輯]
                # 1. 關閉舊週期
                conn.execute("UPDATE mining_cycles SET status = 'completed' WHERE id = ?", (active["id"],))
                
                # 2. 建立新週期
                new_end = (now_dt + _safe_td(days=7)).isoformat()
                cur2 = conn.execute("INSERT INTO mining_cycles (name, status, start_ts, end_ts) VALUES (?, ?, ?, ?)",
                                (f"Cycle {active['id'] + 1}", "active", now_str, new_end))
                new_cycle_id = cur2.lastrowid
                
                # 3. 深度繼承：使用 NOT EXISTS 確保幂等性（重複執行也不會爆炸）
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
            
            # 若偏好的策略無效，退回尋找全部
            if not pools and preferred_family:
                pools = conn.execute("SELECT id, num_partitions FROM factor_pools WHERE cycle_id = ? AND active = 1", (cycle_id,)).fetchall()
                
            for _ in range(needed):
                if pools:
                    p = random.choice(pools)
                    conn.execute("INSERT INTO mining_tasks (user_id, pool_id, cycle_id, partition_idx, num_partitions, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                 (user_id, p["id"], cycle_id, random.randint(0, max(0, p["num_partitions"]-1)), p["num_partitions"], 'assigned', _now_iso()))
            conn.commit()
    except Exception as e:
        print(f"[DB ERROR] assign_tasks_for_user: {e}")
    finally:
        conn.close()
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
        conn = sqlite3.connect(db_path, timeout=5.0, check_same_thread=False)
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

@_retry_on_lock
def update_task_progress(task_id: int, progress: dict) -> None:
    conn = _conn()
    try:
        # [專家級修復] 更新進度時必須同時更新 last_heartbeat
        # 否則伺服器端執行的任務會因為 last_heartbeat 停滯，而被 clean_zombie_tasks 誤殺！
        conn.execute(
            "UPDATE mining_tasks SET progress_json = ?, updated_at = ?, last_heartbeat = ? WHERE id = ?", 
            (json.dumps(progress, ensure_ascii=False), _now_iso(), _now_iso(), task_id)
        )
        conn.commit()
    finally:
        conn.close()

def update_task_status(task_id: int, status: str, finished: bool = False) -> None:
    conn = _conn()
    try:
        # [專家級修復] 狀態變更時亦同步更新 heartbeat，防範殭屍誤判
        conn.execute(
            "UPDATE mining_tasks SET status = ?, updated_at = ?, last_heartbeat = ? WHERE id = ?", 
            (status, _now_iso(), _now_iso(), task_id)
        )
        conn.commit()
    finally:
        conn.close()

def clear_candidates_for_task(task_id: int) -> None:
    conn = _conn()
    try:
        conn.execute("DELETE FROM candidates WHERE task_id = ?", (task_id,))
        conn.commit()
    finally:
        conn.close()

def insert_candidate(task_id: int, user_id: int, pool_id: int, params: dict, metrics: dict, score: float) -> int:
    conn = _conn()
    try:
        cur = conn.execute("INSERT INTO candidates (task_id, user_id, pool_id, params_json, metrics_json, score, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                           (task_id, user_id, pool_id, json.dumps(params, ensure_ascii=False), json.dumps(metrics, ensure_ascii=False), score, _now_iso()))
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()

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

def upsert_worker(worker_id: str, user_id: int, version: str, protocol: int, meta: dict) -> None:
    # 由於沒有獨立的 workers 表，這裡將其記錄在 audit_logs 作為心跳註冊，保證最大化顯示節點狀態
    write_audit_log(user_id, "worker_register", {
        "worker_id": worker_id, "version": version, "protocol": protocol, "meta": meta
    })

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
        conn.commit()
        return cur.rowcount > 0
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
    """專家級防護：自動清理異常斷線導致卡在 running 狀態的殭屍任務"""
    conn = _conn()
    try:
        cur = conn.execute(
            "UPDATE mining_tasks SET status = 'assigned', updated_at = ? "
            "WHERE status = 'running' AND last_heartbeat < datetime(?, ?)",
            (_now_iso(), _now_iso(), f"-{timeout_minutes} minutes")
        )
        conn.commit()
        return cur.rowcount
    except Exception as e:
        print(f"[DB ERROR] clean_zombie_tasks: {e}")
        return 0
    finally:
        conn.close()

def update_user_nickname(user_id: int, nickname: str) -> None:
    """
    更新用戶暱稱 (需先在應用層檢查權限)。
    [安全強化] 強制 HTML 轉義、去除首尾空格、限制長度 10 字元。
    """
    conn = _conn()
    try:
        # [資安防護] 嚴格限制：必須先截斷長度再進行 HTML Escape，否則切斷實體字元(如 &amp;)會破壞前端版面
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