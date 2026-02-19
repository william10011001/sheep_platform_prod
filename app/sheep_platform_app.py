import json
import os
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st
_st_dataframe = st.dataframe

def _dataframe_compat(data=None, **kwargs):
    if "use_container_width" in kwargs:
        u = kwargs.pop("use_container_width")
        kwargs.setdefault("width", "stretch" if bool(u) else "content")
    try:
        return _st_dataframe(data, **kwargs)
    except TypeError:
        if "width" in kwargs:
            w = kwargs.pop("width")
            kwargs["use_container_width"] = (str(w) == "stretch")
        return _st_dataframe(data, **kwargs)

st.dataframe = _dataframe_compat

import backtest_panel2 as bt

import sheep_platform_db as db
from sheep_platform_security import (
    hash_password,
    verify_password,
    validate_username,
    validate_password_strength,
    validate_wallet_address,
    normalize_username,
    get_fernet,
)
from sheep_platform_jobs import JOB_MANAGER, JobManager
from sheep_platform_audit import audit_candidate


APP_TITLE = "羊肉爐挖礦平台"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

def _issue_api_token(user: Dict[str, Any], ttl_seconds: int = 86400, name: str = "worker") -> Dict[str, Any]:
    """Issue an API token stored in DB (compatible with FastAPI Bearer auth).

    NOTE: Raw token is only shown once; store it securely on the worker side.
    """
    return db.create_api_token(int(user["id"]), ttl_seconds=int(ttl_seconds), name=str(name or "worker"))



def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def _week_bounds_last_completed(now_utc: datetime) -> Dict[str, str]:
    monday = (now_utc - timedelta(days=now_utc.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    week_end = monday
    week_start = monday - timedelta(days=7)
    return {"week_start_ts": _iso(week_start), "week_end_ts": _iso(week_end)}


def _style() -> None:
    st.markdown(
        """
        <style>
        :root {
          --bg: #0b0f19;
          --card: rgba(255,255,255,0.04);
          --border: rgba(255,255,255,0.12);
          --text: rgba(255,255,255,0.92);
          --muted: rgba(255,255,255,0.66);
          --accent: rgba(120, 180, 255, 0.95);
        }

        .stApp {
          background: radial-gradient(1200px 600px at 20% -10%, rgba(120,180,255,0.25), transparent 60%),
                      radial-gradient(900px 500px at 110% 20%, rgba(255,120,180,0.14), transparent 55%),
                      var(--bg);
          color: var(--text);
        }

        div[data-testid="stSidebar"] {
          background: rgba(255,255,255,0.02);
          border-right: 1px solid var(--border);
        }

        .block-container {
          padding-top: 1.0rem;
          padding-bottom: 3.0rem;
          max-width: 1200px;
        }

        .card {
          background: var(--card);
          border: 1px solid var(--border);
          border-radius: 14px;
          padding: 16px 16px 10px 16px;
        }

        .metric-row {
          display: flex;
          gap: 10px;
          flex-wrap: wrap;
        }
        .metric {
          padding: 10px 12px;
          border-radius: 12px;
          border: 1px solid var(--border);
          background: rgba(255,255,255,0.03);
          min-width: 160px;
        }
        .metric .k { color: var(--muted); font-size: 12px; }
        .metric .v { color: var(--text); font-size: 20px; font-weight: 600; }

        .small-muted { color: var(--muted); font-size: 12px; }

        header[data-testid="stHeader"] { background: rgba(0,0,0,0); }
        footer { visibility: hidden; }
        #MainMenu { visibility: hidden; }
        </style>
        """,
        unsafe_allow_html=True,
    )



_LAST_ROLLOVER_CHECK = 0.0


@st.cache_resource
def _init_once() -> None:
    """Initialize DB schema/defaults once per Streamlit process.

    Streamlit 會一直 rerun 腳本；把 init 放在 cache_resource 可以讓每次 rerun 直接略過，
    讓頁面刷新快很多、DB 壓力也小很多。
    """
    db.init_db()

    # Bootstrap admin user if none exists (first boot only).
    users = db.list_users(limit=1000)
    has_admin = any(u.get("role") == "admin" for u in users)
    if not has_admin:
        from sheep_platform_security import random_token

        username = os.environ.get("SHEEP_BOOTSTRAP_ADMIN_USER", "admin").strip()
        password = os.environ.get("SHEEP_BOOTSTRAP_ADMIN_PASS", "").strip()
        if not password:
            password = random_token(18)
            try:
                (db.DATA_DIR / "bootstrap_admin.txt").write_text(f"{username}\n{password}\n", encoding="utf-8")
            except Exception:
                pass

        try:
            db.create_user(username=username, password_hash=hash_password(password), role="admin", wallet_address="N/A")
            db.write_audit_log(None, "bootstrap_admin", {"username": username})
        except Exception:
            pass


def _bootstrap() -> None:
    """Lightweight bootstrap called every rerun."""
    _init_once()

    # Cycle rollover isn't required every rerun; throttle it to reduce DB chatter.
    global _LAST_ROLLOVER_CHECK
    try:
        interval_s = float(os.environ.get("SHEEP_ROLLOVER_CHECK_S", "30") or "30")
    except Exception:
        interval_s = 30.0
    interval_s = float(max(5.0, min(300.0, interval_s)))

    now = time.time()
    if now - float(_LAST_ROLLOVER_CHECK or 0.0) >= interval_s:
        _LAST_ROLLOVER_CHECK = now
        db.ensure_cycle_rollover()


def _session_user() -> Optional[Dict[str, Any]]:
    uid = st.session_state.get("auth_user_id")
    if uid is None:
        return None
    row = db.get_user_by_id(int(uid))
    if not row:
        return None
    return row


def _set_session_user(user: Dict[str, Any]) -> None:
    """Persist logged-in user into Streamlit session_state.

    This app runs on Streamlit, so we don't have traditional server-side sessions.
    We store the minimal authenticated identity in st.session_state and always
    re-fetch the latest user row from DB via _session_user() to avoid stale role changes.
    """
    # Prevent cross-user leakage when the same browser session logs in/out repeatedly.
    for k in list(st.session_state.keys()):
        if (
            k.startswith("auth_")
            or k.startswith("worker_")
            or k.startswith("run_")
            or k.startswith("audit_result_")
            or k.startswith("tasks_")
            or k.startswith("captcha_slider_")
        ):
            try:
                del st.session_state[k]
            except Exception:
                pass

    # Store only what we need. _session_user() uses auth_user_id as source of truth.
    st.session_state["auth_user_id"] = int(user["id"])
    st.session_state["auth_username"] = str(user.get("username") or "")
    st.session_state["auth_role"] = str(user.get("role") or "user")
    st.session_state["auth_login_at"] = _iso(_utc_now())

    # Reset captcha so the next login (after logout) cannot reuse an already-100 slider.
    st.session_state["captcha_nonce"] = random.randint(1000, 9999)
    st.session_state["captcha_t0"] = time.time()


def _logout() -> None:
    # Clear auth + per-user runtime state
    for k in list(st.session_state.keys()):
        if (
            k.startswith("auth_")
            or k.startswith("worker_")
            or k.startswith("run_")
            or k.startswith("audit_result_")
            or k.startswith("tasks_")
            or k.startswith("captcha_slider_")
        ):
            try:
                del st.session_state[k]
            except Exception:
                pass

    # Force captcha refresh for the next login screen
    st.session_state["captcha_nonce"] = random.randint(1000, 9999)
    st.session_state["captcha_t0"] = time.time()



def _login_form() -> None:
    st.markdown(f"### {APP_TITLE}")
    st.markdown('<div class="small-muted">登入</div>', unsafe_allow_html=True)

    # Slider Captcha (no external dependency, works even without Cloudflare).
    # This is NOT as strong as Turnstile, but it blocks a lot of low-effort credential stuffing bots.
    captcha_enabled = (os.environ.get("SHEEP_CAPTCHA", "1").strip() != "0")
    try:
        captcha_min_s = float(os.environ.get("SHEEP_CAPTCHA_MIN_S", "0.8") or 0.8)
    except Exception:
        captcha_min_s = 0.8
    captcha_min_s = float(max(0.2, min(10.0, captcha_min_s)))

    if "captcha_nonce" not in st.session_state:
        st.session_state["captcha_nonce"] = random.randint(1000, 9999)
        st.session_state["captcha_t0"] = time.time()

    nonce = int(st.session_state.get("captcha_nonce") or 0)
    captcha_key = f"captcha_slider_{nonce}"

    with st.form("login_form", clear_on_submit=False):
        username = st.text_input("帳號", value="", autocomplete="username")
        password = st.text_input("密碼", value="", type="password", autocomplete="current-password")

        if captcha_enabled:
            st.markdown('<div class="small-muted">滑動驗證碼：把滑桿拖到最右邊（100）</div>', unsafe_allow_html=True)
            st.slider(" ", min_value=0, max_value=100, value=0, step=1, key=captcha_key)

        submitted = st.form_submit_button("登入")

    if not submitted:
        return

    if captcha_enabled:
        dt = float(time.time() - float(st.session_state.get("captcha_t0") or time.time()))
        if int(st.session_state.get(captcha_key) or 0) != 100:
            st.error("滑動驗證碼未通過（請拖到 100）。")
            st.session_state["captcha_nonce"] = random.randint(1000, 9999)
            st.session_state["captcha_t0"] = time.time()
            return
        if dt < captcha_min_s:
            st.error("滑動太快了 🤨 重新拖一次。")
            st.session_state["captcha_nonce"] = random.randint(1000, 9999)
            st.session_state["captcha_t0"] = time.time()
            return

    uname = normalize_username(username)
    user = db.get_user_by_username(uname)
    if not user:
        st.error("帳號或密碼錯誤。")
        return

    if int(user.get("disabled") or 0) == 1:
        st.error("帳號已停用。")
        return

    if db.is_user_locked(user):
        st.error("登入已鎖定。")
        return

    if not verify_password(password, user["password_hash"]):
        db.update_user_login_state(int(user["id"]), success=False)
        st.error("帳號或密碼錯誤。")
        return

    db.update_user_login_state(int(user["id"]), success=True)
    _set_session_user(user)
    st.success("登入成功。")
    st.rerun()

def _register_form() -> None:
    st.markdown("### 註冊")
    with st.form("register_form", clear_on_submit=False):
        username = st.text_input("帳號", value="")
        password = st.text_input("密碼", value="", type="password")
        password2 = st.text_input("確認密碼", value="", type="password")
        wallet = st.text_input("分潤地址", value="")
        submitted = st.form_submit_button("建立帳號")

    if not submitted:
        return

    ok, msg = validate_username(username)
    if not ok:
        st.error(msg)
        return

    ok, msg = validate_password_strength(password)
    if not ok:
        st.error(msg)
        return

    if password != password2:
        st.error("密碼不一致。")
        return

    ok, msg = validate_wallet_address(wallet)
    if not ok:
        st.error(msg)
        return

    uname = normalize_username(username)
    if db.get_user_by_username(uname):
        st.error("帳號已存在。")
        return

    try:
        uid = db.create_user(username=uname, password_hash=hash_password(password), role="user", wallet_address=wallet)
        db.write_audit_log(uid, "register", {"username": uname})
        st.success("帳號已建立。")
    except Exception as e:
        st.error("建立失敗。")


def _page_auth() -> None:
    col1, col2 = st.columns([1, 1])
    with col1:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        _login_form()
        st.markdown("</div>", unsafe_allow_html=True)
    with col2:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        _register_form()
        st.markdown("</div>", unsafe_allow_html=True)


def _render_kpi(title: str, value: Any, sub: str = "") -> str:
    v = value if value is not None else "-"
    return f'<div class="metric"><div class="k">{title}</div><div class="v">{v}</div><div class="small-muted">{sub}</div></div>'




def _page_tutorial(user: Optional[Dict[str, Any]] = None) -> None:
    st.markdown(f"### {APP_TITLE} · 新手教學")
    st.markdown('<div class="small-muted">三分鐘上手：少走彎路，少踩坑。</div>', unsafe_allow_html=True)

    st.markdown("")

    # A tiny animated walkthrough using pure HTML/CSS (no extra dependencies).
    st.components.v1.html(
        """
        <div style="padding:16px;border-radius:14px;background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);">
          <div style="font-size:14px;opacity:.9;margin-bottom:8px;">流程總覽</div>
          <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
            <div class="sp-step">登入</div>
            <div class="sp-arrow">➜</div>
            <div class="sp-step sp-pulse">開始全部任務</div>
            <div class="sp-arrow">➜</div>
            <div class="sp-step">等它跑完</div>
            <div class="sp-arrow">➜</div>
            <div class="sp-step">看候選結果</div>
            <div class="sp-arrow">➜</div>
            <div class="sp-step">提交策略</div>
            <div class="sp-arrow">➜</div>
            <div class="sp-step">結算領獎</div>
          </div>
        </div>
        <style>
          .sp-step{
            padding:10px 12px;
            border-radius:999px;
            background:rgba(255,255,255,0.06);
            border:1px solid rgba(255,255,255,0.10);
            font-size:13px;
          }
          .sp-arrow{opacity:.35}
          .sp-pulse{
            animation: spPulse 1.3s ease-in-out infinite;
            border-color: rgba(255,255,255,0.22);
          }
          @keyframes spPulse {
            0% { transform: scale(1); filter: brightness(1); }
            50% { transform: scale(1.03); filter: brightness(1.15); }
            100% { transform: scale(1); filter: brightness(1); }
          }
        </style>
        """,
        height=140,
    )

    st.markdown("")
    st.markdown("#### 1) 先登入 / 註冊")
    st.write("註冊時，帳號只能英數底線，密碼要夠長（至少 10 碼、大小寫 + 數字）。這不是在刁你，是在擋低成本撞庫。")

    st.markdown("#### 2) 到「任務」頁，按一次「開始全部任務」就夠了")
    st.write(
        "你只要點一次。系統會："
        "\n- 自動把目前分配到的任務塞進隊列"
        "\n- 跑完後自動抓下一批任務"
        "\n- 不用你再回去點第二次、更不用你雙擊自動刷新"
    )

    st.markdown("#### 3) 觀察進度與最佳參數")
    st.write("每個任務會顯示：參數進度、最佳分數、是否達標、速度與 ETA。你只要看「達標」跟「候選結果」就好，其他都是工程師用的 debug 資訊。")

    st.markdown("#### 4) 任務完成後，去「候選結果」挑你要提交的策略")
    st.write("任務完成後會出現候選列表。提交後會進入策略池，後續結算會用它算獎勵。")

    st.markdown("#### 5) 到「結算」頁填錢包地址")
    st.write("地址會做基本格式檢查。不要亂填，不然獎勵發不出去只能怪你自己。")

    st.markdown("")
    st.info("小提醒：如果你只是想『讓它一直跑』，保持「自動刷新」勾著就對了。")

def _page_dashboard(user: Dict[str, Any]) -> None:
    cycle = db.get_active_cycle()
    pools = db.list_factor_pools(cycle_id=int(cycle["id"])) if cycle else []

    st.markdown("### 控制台")
    st.markdown('<div class="small-muted">週期：' + str(cycle.get("name") or "-") + '</div>', unsafe_allow_html=True)

    # Ensure tasks quota
    conn = db._conn()
    try:
        min_tasks = int(db.get_setting(conn, "min_tasks_per_user", 2))
    finally:
        conn.close()
    db.assign_tasks_for_user(int(user["id"]), min_tasks)

    tasks = db.list_tasks_for_user(int(user["id"]), cycle_id=int(cycle["id"]))
    strategies = db.list_strategies(user_id=int(user["id"]), limit=200)
    payouts = db.list_payouts(user_id=int(user["id"]), limit=200)

    active_tasks = [t for t in tasks if t["status"] in ("assigned", "running")]
    completed_tasks = [t for t in tasks if t["status"] == "completed"]

    active_strategies = [s for s in strategies if s["status"] == "active"]
    unpaid = [p for p in payouts if p["status"] == "unpaid"]

    st.markdown('<div class="metric-row">', unsafe_allow_html=True)
    st.markdown(_render_kpi("任務", len(active_tasks), "進行中"), unsafe_allow_html=True)
    st.markdown(_render_kpi("任務", len(completed_tasks), "已完成"), unsafe_allow_html=True)
    st.markdown(_render_kpi("策略", len(active_strategies), "有效中"), unsafe_allow_html=True)
    st.markdown(_render_kpi("結算", len(unpaid), "未發放"), unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("### 任務摘要")
    if not tasks:
        st.info("無任務。")
        return

    df = pd.DataFrame([
        {
            "task_id": t["id"],
            "pool": t["pool_name"],
            "symbol": t["symbol"],
            "tf_min": t["timeframe_min"],
            "family": t["family"],
            "partition": f'{int(t["partition_idx"])+1}/{int(t.get("num_partitions") or 1)}',
            "status": t["status"],
            "progress": t.get("progress_json", ""),
        }
        for t in tasks
    ])
    st.dataframe(df, use_container_width=True, hide_index=True)


def _page_tasks(user: Dict[str, Any], job_mgr: JobManager) -> None:
    cycle = db.get_active_cycle()
    if not cycle:
        st.error("週期未初始化。")
        return

    conn = db._conn()
    try:
        min_tasks = int(db.get_setting(conn, "min_tasks_per_user", 2))
        max_tasks = int(db.get_setting(conn, "max_tasks_per_user", 6))
        max_concurrent_jobs = int(db.get_setting(conn, "max_concurrent_jobs", 2))
        min_trades = int(db.get_setting(conn, "min_trades", 40))
        min_total_return_pct = float(db.get_setting(conn, "min_total_return_pct", 15.0))
        max_drawdown_pct = float(db.get_setting(conn, "max_drawdown_pct", 25.0))
        min_sharpe = float(db.get_setting(conn, "min_sharpe", 0.6))
        exec_mode = str(db.get_setting(conn, "execution_mode", "server") or "server").strip().lower()
        api_url = str(db.get_setting(conn, "worker_api_url", "http://127.0.0.1:8001") or "http://127.0.0.1:8001").strip()
    finally:
        conn.close()

    if exec_mode not in ("server", "worker"):
        exec_mode = "server"

    db.assign_tasks_for_user(int(user["id"]), cycle_id=int(cycle["id"]), min_tasks=int(min_tasks), max_tasks=int(max_tasks))

    st.markdown("### 任務")

    tasks = db.list_tasks_for_user(int(user["id"]), cycle_id=int(cycle["id"]))
    if not tasks:
        st.info("無任務。")
        return

    assigned_cnt = 0
    running_cnt = 0
    completed_cnt = 0
    combos_done_sum = 0
    combos_total_sum = 0

    for t in tasks:
        status = str(t.get("status") or "")
        if status == "assigned":
            assigned_cnt += 1
        if status == "completed":
            completed_cnt += 1

        tid = int(t["id"])
        if job_mgr.is_running(tid) or status == "running":
            running_cnt += 1

        try:
            prog = json.loads(t.get("progress_json") or "{}")
        except Exception:
            prog = {}

        combos_done_sum += int(prog.get("combos_done") or 0)
        combos_total_sum += int(prog.get("combos_total") or 0)

    queued_cnt = job_mgr.queue_len(int(user["id"])) if exec_mode == "server" else 0

    col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1.2])
    with col1:
        st.metric("已分配", int(len(tasks)))
    with col2:
        st.metric("待執行", int(assigned_cnt))
    with col3:
        st.metric("執行中", int(running_cnt))
    with col4:
        st.metric("隊列", int(queued_cnt))
    with col5:
        pct = 0.0
        if combos_total_sum > 0:
            pct = 100.0 * float(combos_done_sum) / float(combos_total_sum)
        st.metric("參數進度", f"{int(combos_done_sum)}/{int(combos_total_sum)} ({pct:.2f}%)")

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("#### 控制")
    st.markdown(f'<div class="small-muted">執行模式：{exec_mode}</div>', unsafe_allow_html=True)

    col_a, col_b, col_c, col_d = st.columns([1.1, 1.1, 1.0, 1.2])

    if exec_mode == "server":
        run_key = f"server_run_all_{int(user['id'])}"
        if run_key not in st.session_state:
            st.session_state[run_key] = False
        run_all = bool(st.session_state.get(run_key, False))

        with col_a:
            if st.button("開始全部任務", key="start_all"):
                st.session_state[run_key] = True
                run_all = True
                to_queue: List[int] = []
                for t in tasks:
                    tid = int(t["id"])
                    if str(t.get("status") or "") != "assigned":
                        continue
                    if job_mgr.is_running(tid):
                        continue
                    if job_mgr.is_queued(int(user["id"]), tid):
                        continue
                    to_queue.append(tid)
                result = job_mgr.enqueue_many(int(user["id"]), to_queue, bt)
                db.write_audit_log(
                    int(user["id"]),
                    "task_queue_all",
                    {"queued": int(result.get("queued") or 0), "skipped": int(result.get("skipped") or 0)},
                )
                st.rerun()

        with col_b:
            if st.button("停止全部任務", key="stop_all"):
                st.session_state[run_key] = False
                run_all = False
                job_mgr.stop_all_for_user(int(user["id"]))
                db.write_audit_log(int(user["id"]), "task_stop_all", {})
                st.rerun()

        with col_c:
            if st.button("刷新", key="tasks_refresh"):
                st.rerun()

        with col_d:
            auto_refresh = st.checkbox("自動刷新", value=True, key="tasks_auto_refresh")
            refresh_s = st.number_input(
                "刷新間隔秒數", min_value=0.5, max_value=10.0, value=1.0, step=0.5, key="tasks_refresh_s"
            )

        st.markdown(
            f'<div class="small-muted">狀態：{"running" if run_all else "idle"} · 並行上限 {int(max_concurrent_jobs)}</div>',
            unsafe_allow_html=True,
        )


    else:
        run_enabled = db.get_user_run_enabled(int(user["id"]))

        with col_a:
            if st.button("開始全部任務", key="worker_enable"):
                db.set_user_run_enabled(int(user["id"]), True)
                st.rerun()

        with col_b:
            if st.button("停止全部任務", key="worker_disable"):
                db.set_user_run_enabled(int(user["id"]), False)
                st.rerun()

        with col_c:
            if st.button("刷新", key="tasks_refresh"):
                st.rerun()

        with col_d:
            auto_refresh = st.checkbox("自動刷新", value=True, key="tasks_auto_refresh")
            refresh_s = st.number_input("刷新間隔秒數", min_value=0.5, max_value=10.0, value=1.0, step=0.5, key="tasks_refresh_s")

        st.markdown(f'<div class="small-muted">狀態：{"running" if run_enabled else "idle"}</div>', unsafe_allow_html=True)
        st.markdown('<div class="small-muted">Worker API：' + api_url + '</div>', unsafe_allow_html=True)

        last_hb = None
        for _t in tasks:
            hb = _t.get("last_heartbeat")
            if not hb:
                continue
            try:
                ts = _parse_iso(str(hb))
            except Exception:
                continue
            if last_hb is None or ts > last_hb:
                last_hb = ts
        if last_hb is not None:
            age_s = max(0.0, (_utc_now() - last_hb).total_seconds())
            st.markdown(f'<div class="small-muted">最後回報 {age_s:.0f}s</div>', unsafe_allow_html=True)

        token = st.session_state.get("worker_token")
        ttl_days = st.number_input("Token 有效天數", min_value=1, max_value=180, value=30, step=1)
        if st.button("產生 Token", key="issue_worker_token"):
            tok = _issue_api_token(user, ttl_seconds=int(ttl_days) * 86400)
            st.session_state["worker_token"] = str(tok.get("token") or "")
            st.session_state["worker_token_meta"] = tok
            st.rerun()

        token = st.session_state.get("worker_token")
        if token:
            meta = st.session_state.get("worker_token_meta") or {}
            if isinstance(meta, dict) and meta.get("expires_at"):
                st.caption(f"Token expires at: {meta.get('expires_at')}")
            st.code(token, language="text")
            cfg = {
                "server": api_url,
                "token": str(token),
                "poll_s": 2.0,
                "idle_s": 3.0,
                "flag_poll_s": 2.0,
                "commit_every": 50,
                "timeout_s": 20.0,
            }
            cfg_json = json.dumps(cfg, ensure_ascii=False, indent=2)
            st.download_button("下載 worker_config.json", data=cfg_json, file_name="worker_config.json", mime="application/json")

            launcher_py = (
                "import json\n"
                "import os\n"
                "import sys\n\n"
                "from sheep_worker_client import main as worker_main\n\n"
                f"CFG = {json.dumps(cfg, ensure_ascii=False, indent=2)}\n\n"
                "def main() -> None:\n"
                "    here = os.path.dirname(os.path.abspath(__file__))\n"
                "    cfg_path = os.path.join(here, 'worker_config.json')\n"
                "    with open(cfg_path, 'w', encoding='utf-8') as f:\n"
                "        f.write(json.dumps(CFG, ensure_ascii=False, indent=2))\n"
                "    sys.argv = ['sheep_worker_client.py', '--config', cfg_path]\n"
                "    worker_main()\n\n"
                "if __name__ == '__main__':\n"
                "    main()\n"
            )
            st.download_button("下載 start_worker.py", data=launcher_py, file_name="start_worker.py", mime="text/x-python")
            st.code("python start_worker.py", language="bash")
        else:
            st.code(f"python sheep_worker_client.py --server {api_url} --username {user['username']} --password <PASSWORD>", language="bash")

    st.markdown("</div>", unsafe_allow_html=True)

    # 無縫銜接模式：使用者只要點一次「開始全部任務」，之後每次刷新都會自動把新的 assigned 任務塞進隊列。
    # 這樣就不會出現「跑完後還要再手動點一次」的尷尬 UX。
    if exec_mode == "server":
        run_key = f"server_run_all_{int(user['id'])}"
        run_all = bool(st.session_state.get(run_key, False))
        if run_all:
            to_queue2: List[int] = []
            for _t in tasks:
                _tid = int(_t["id"])
                if str(_t.get("status") or "") != "assigned":
                    continue
                if job_mgr.is_running(_tid):
                    continue
                if job_mgr.is_queued(int(user["id"]), _tid):
                    continue
                to_queue2.append(_tid)

            if to_queue2:
                result2 = job_mgr.enqueue_many(int(user["id"]), to_queue2, bt)
                if int(result2.get("queued") or 0) > 0:
                    db.write_audit_log(
                        int(user["id"]),
                        "task_auto_queue",
                        {"queued": int(result2.get("queued") or 0), "skipped": int(result2.get("skipped") or 0)},
                    )
                st.rerun()

    def _fmt_gap_min(cur: Optional[float], thr: float) -> str:
        if cur is None:
            return "-"
        gap = float(thr) - float(cur)
        return f"{gap:.4f}" if gap > 0 else "0"

    def _fmt_gap_max(cur: Optional[float], thr: float) -> str:
        if cur is None:
            return "-"
        gap = float(cur) - float(thr)
        return f"{gap:.4f}" if gap > 0 else "0"

    any_active = False

    for t in tasks:
        try:
            prog = json.loads(t.get("progress_json") or "{}")
        except Exception:
            prog = {}

        tid = int(t["id"])
        status = str(t.get("status") or "")
        running = bool(job_mgr.is_running(tid)) if exec_mode == "server" else (status == "running")
        queued = bool(job_mgr.is_queued(int(user["id"]), tid)) if exec_mode == "server" else False

        view_status = status
        if exec_mode == "server":
            if running:
                view_status = "running"
            elif queued and status == "assigned":
                view_status = "queued"

        combos_total = int(prog.get("combos_total") or 0)
        combos_done = int(prog.get("combos_done") or 0)

        best_any_score = prog.get("best_any_score")
        best_any_metrics = prog.get("best_any_metrics") or {}
        best_any_params = prog.get("best_any_params") or {}
        best_any_passed = bool(prog.get("best_any_passed") or False)

        phase = str(prog.get("phase") or "")
        phase_progress = prog.get("phase_progress")
        phase_msg = str(prog.get("phase_msg") or "")
        last_error = str(prog.get("last_error") or "").strip()

        if view_status in ("running", "queued"):
            any_active = True

        ret_pct = None
        dd_pct = None
        sharpe = None
        trades = None
        try:
            if best_any_metrics:
                ret_pct = float(best_any_metrics.get("total_return_pct"))
                dd_pct = float(best_any_metrics.get("max_drawdown_pct"))
                sharpe = float(best_any_metrics.get("sharpe"))
                trades = int(best_any_metrics.get("trades"))
        except Exception:
            ret_pct = None
            dd_pct = None
            sharpe = None
            trades = None

        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown(f"#### 任務 {t['id']}")
        st.markdown(
            f'<div class="small-muted">{t["pool_name"]} · {t["symbol"]} · {t["timeframe_min"]}m · {t["family"]} · 分割 {int(t["partition_idx"])+1}/{int(t.get("num_partitions") or 1)}</div>',
            unsafe_allow_html=True,
        )

        hb = t.get("last_heartbeat")
        if hb:
            try:
                age_s = max(0.0, (_utc_now() - _parse_iso(str(hb))).total_seconds())
                st.markdown(f'<div class="small-muted">最後回報 {age_s:.0f}s</div>', unsafe_allow_html=True)
            except Exception:
                pass

        top_a, top_b, top_c, top_d = st.columns([1.1, 1.3, 1.3, 1.3])
        with top_a:
            st.write("狀態", view_status)
        with top_b:
            st.write("參數進度", f"{combos_done}/{combos_total}")
        with top_c:
            st.write("最佳分數", "-" if best_any_score is None else round(float(best_any_score), 6))
        with top_d:
            st.write("達標", bool(best_any_passed))

        elapsed_s = prog.get("elapsed_s")
        speed_cps = prog.get("speed_cps")
        eta_s = prog.get("eta_s")
        if isinstance(elapsed_s, (int, float)) or isinstance(speed_cps, (int, float)) or isinstance(eta_s, (int, float)):
            es = "-" if elapsed_s is None else f"{float(elapsed_s):.2f}s"
            sp = "-" if speed_cps is None else f"{float(speed_cps):.3f} cps"
            et = "-" if eta_s is None else f"{float(eta_s):.1f}s"
            st.markdown(f'<div class="small-muted">耗時 {es} · 速度 {sp} · ETA {et}</div>', unsafe_allow_html=True)

        if last_error:
            st.caption(last_error[:160])

        if combos_total > 0:
            st.progress(min(1.0, float(combos_done) / float(combos_total)))
        elif phase == "sync_data" and isinstance(phase_progress, (int, float)):
            st.progress(float(phase_progress))

        if phase_msg:
            st.caption(phase_msg)

        grid_a, grid_b = st.columns([1.2, 1.0])
        with grid_a:
            rows = []
            rows.append({"metric": "trades", "value": "-" if trades is None else int(trades), "threshold": int(min_trades), "gap": _fmt_gap_min(float(trades) if trades is not None else None, float(min_trades))})
            rows.append({"metric": "return_pct", "value": "-" if ret_pct is None else round(float(ret_pct), 4), "threshold": float(min_total_return_pct), "gap": _fmt_gap_min(ret_pct, float(min_total_return_pct))})
            rows.append({"metric": "max_drawdown_pct", "value": "-" if dd_pct is None else round(float(dd_pct), 4), "threshold": float(max_drawdown_pct), "gap": _fmt_gap_max(dd_pct, float(max_drawdown_pct))})
            rows.append({"metric": "sharpe", "value": "-" if sharpe is None else round(float(sharpe), 4), "threshold": float(min_sharpe), "gap": _fmt_gap_min(sharpe, float(min_sharpe))})
            st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

        with grid_b:
            if exec_mode == "server":
                col_btn1, col_btn2 = st.columns([1, 1])
                with col_btn1:
                    if status == "assigned" and (not running) and (not queued):
                        if st.button("立即開始", key=f"start_now_{tid}"):
                            ok = job_mgr.start(tid, bt)
                            if not ok:
                                job_mgr.enqueue_many(int(user["id"]), [tid], bt)
                            st.rerun()
                    elif queued and status == "assigned":
                        st.write("隊列中")
                    elif running:
                        st.write("執行中")
                with col_btn2:
                    if status == "assigned" and (not running) and (not queued):
                        if st.button("加入隊列", key=f"queue_{tid}"):
                            job_mgr.enqueue_many(int(user["id"]), [tid], bt)
                            st.rerun()
                    if running:
                        if st.button("停止", key=f"stop_{tid}"):
                            job_mgr.stop(tid)
                            st.rerun()
                st.caption(f"並行上限 {int(max_concurrent_jobs)}")
            else:
                st.caption("此模式由 worker 執行。")

        if best_any_params:
            with st.expander("最佳參數", expanded=False):
                st.json(best_any_params)

        if status == "completed":
            _render_candidates_and_submit(user, t)

        st.markdown("</div>", unsafe_allow_html=True)

    keep_polling = False
    if exec_mode == "server":
        run_key = f"server_run_all_{int(user['id'])}"
        keep_polling = bool(st.session_state.get(run_key, False))
    else:
        keep_polling = bool(run_enabled)

    # 自動刷新：預設永遠勾選。
    # - any_active=True：有人在跑 / 在隊列 -> 正常刷新
    # - keep_polling=True：使用者點過「開始全部任務」(server) 或 run_enabled=True(worker)
    #   即使暫時沒任務，也會持續刷新，才能無縫接新任務。
    if auto_refresh and (any_active or keep_polling):
        try:
            time.sleep(float(refresh_s))
        except Exception:
            time.sleep(1.0)
        st.rerun()


def _render_candidates_and_submit(user: Dict[str, Any], task_row: Dict[str, Any]) -> None:
    task_id = int(task_row["id"])
    cands = db.list_candidates(task_id, limit=50)
    if not cands:
        st.warning("無候選結果。")
        return

    st.markdown("候選結果")
    rows = []
    for c in cands:
        m = c.get("metrics") or {}
        rows.append({
            "candidate_id": c["id"],
            "score": round(float(c.get("score") or 0.0), 6),
            "return_pct": round(float(m.get("total_return_pct") or 0.0), 4),
            "dd_pct": round(float(m.get("max_drawdown_pct") or 0.0), 4),
            "sharpe": round(float(m.get("sharpe") or 0.0), 4),
            "trades": int(m.get("trades") or 0),
            "submitted": int(c.get("is_submitted") or 0),
        })
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)

    sel = st.number_input("候選編號", min_value=int(df["candidate_id"].min()), max_value=int(df["candidate_id"].max()), value=int(df["candidate_id"].min()), step=1)

    cand = next((c for c in cands if int(c["id"]) == int(sel)), None)
    if not cand:
        return

    if int(cand.get("is_submitted") or 0) == 1:
        st.info("已提交。")
        return

    params = cand.get("params_json") or {}
    if not params:
        st.error("候選資料損壞。")
        return

    pool = db.get_pool(int(task_row["pool_id"]))
    if not pool:
        st.error("Pool 資料不存在。")
        return

    conn = db._conn()
    try:
        min_trades = int(db.get_setting(conn, "min_trades", 40))
        min_sharpe = float(db.get_setting(conn, "min_sharpe", 0.6))
        max_drawdown = float(db.get_setting(conn, "max_drawdown_pct", 25.0))
        min_oos_return = 0.0
        min_fw_return = 0.0
        min_sharpe_oos = max(0.0, min_sharpe * 0.5)
        max_dd_oos = max_drawdown
    finally:
        conn.close()
    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("執行過擬合審核", key=f"audit_{task_id}_{sel}"):
            with st.spinner("執行中"):
                audit = _run_audit_for_candidate(pool, params, min_trades=min_trades, min_oos_return=min_oos_return, min_fw_return=min_fw_return, min_sharpe_oos=min_sharpe_oos, max_dd_oos=max_dd_oos)
            st.session_state[f"audit_result_{cand['id']}"] = audit
            st.rerun()
    with col2:
        if st.button("提交", key=f"submit_{task_id}_{sel}"):
            with st.spinner("執行中"):
                audit = _run_audit_for_candidate(pool, params, min_trades=min_trades, min_oos_return=min_oos_return, min_fw_return=min_fw_return, min_sharpe_oos=min_sharpe_oos, max_dd_oos=max_dd_oos)
            if not audit.get("passed"):
                st.error("審核未通過。")
                st.session_state[f"audit_result_{cand['id']}"] = audit
                st.stop()

            sid = db.create_submission(candidate_id=int(cand["id"]), user_id=int(user["id"]), pool_id=int(pool["id"]), audit=audit)
            db.write_audit_log(int(user["id"]), "submit", {"candidate_id": int(cand["id"]), "submission_id": int(sid)})
            st.success("已提交。")
            st.rerun()

    audit_key = f"audit_result_{cand['id']}"
    if audit_key in st.session_state:
        audit = st.session_state[audit_key]
        _render_audit(audit)


def _run_audit_for_candidate(
    pool: Dict[str, Any],
    params: Dict[str, Any],
    min_trades: int,
    min_oos_return: float,
    min_fw_return: float,
    min_sharpe_oos: float,
    max_dd_oos: float,
) -> Dict[str, Any]:
    symbol = str(pool["symbol"])
    tf_min = int(pool["timeframe_min"])
    years = int(pool.get("years") or 3)

    csv_main, _csv_1m = bt.ensure_bitmart_data(symbol=symbol, main_step_min=tf_min, years=years, auto_sync=True, force_full=False)
    df = bt.load_and_validate_csv(csv_main)

    family = str(params.get("family") or pool["family"])
    family_params = dict(params.get("family_params") or {})
    tp = float(params.get("tp"))
    sl = float(params.get("sl"))
    mh = int(params.get("max_hold"))

    risk_spec = dict(pool.get("risk_spec") or {})

    audit = audit_candidate(
        df=df,
        run_backtest_fn=bt.run_backtest,
        family=family,
        family_params=family_params,
        tp=tp,
        sl=sl,
        max_hold=mh,
        risk_overrides=risk_spec,
        min_trades=min_trades,
        min_oos_return_pct=min_oos_return,
        min_forward_return_pct=min_fw_return,
        min_sharpe_oos=min_sharpe_oos,
        max_drawdown_oos=max_dd_oos,
    )
    return audit


def _render_audit(audit: Dict[str, Any]) -> None:
    st.markdown("審核結果")
    passed = bool(audit.get("passed"))
    st.write("通過", passed)
    st.write("分數", round(float(audit.get("score") or 0.0), 6))
    if audit.get("reasons"):
        st.write("原因", audit.get("reasons"))

    splits = audit.get("splits") or {}
    rows = []
    for k in ["in_sample", "out_of_sample", "forward"]:
        seg = splits.get(k) or {}
        m = (seg.get("metrics") or {})
        rows.append({
            "segment": k,
            "return_pct": m.get("total_return_pct"),
            "dd_pct": m.get("max_drawdown_pct"),
            "sharpe": m.get("sharpe"),
            "trades": m.get("trades"),
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _page_submissions(user: Dict[str, Any]) -> None:
    st.markdown("### 提交紀錄")
    subs = db.list_submissions(user_id=int(user["id"]), limit=300)
    if not subs:
        st.info("無提交紀錄。")
        return

    rows = []
    for s in subs:
        audit = s.get("audit") or {}
        rows.append({
            "submission_id": s["id"],
            "status": s["status"],
            "pool": s["pool_name"],
            "symbol": s["symbol"],
            "tf_min": s["timeframe_min"],
            "family": s["family"],
            "score": round(float(audit.get("score") or 0.0), 6),
            "passed": bool(audit.get("passed")),
            "submitted_at": s["submitted_at"],
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _page_rewards(user: Dict[str, Any]) -> None:
    st.markdown("### 結算")
    wallet = db.get_wallet_address(int(user["id"])) or ""
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.write("分潤地址", wallet)
    new_wallet = st.text_input("更新分潤地址", value=wallet, key="wallet_update")
    if st.button("保存"):
        ok, msg = validate_wallet_address(new_wallet)
        if not ok:
            st.error(msg)
        else:
            db.set_wallet_address(int(user["id"]), new_wallet)
            db.write_audit_log(int(user["id"]), "wallet_update", {"wallet": "updated"})
            st.success("已保存。")
            st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

    conn = db._conn()
    try:
        payout_currency = str(db.get_setting(conn, "payout_currency", "USDT") or "USDT").strip()
    finally:
        conn.close()

    payouts = db.list_payouts(user_id=int(user["id"]), limit=500)
    if not payouts:
        st.info("無結算紀錄。")
        return

    rows = []
    for p in payouts:
        rows.append({
            "week_start": p["week_start_ts"],
            "amount": round(float(p["amount_usdt"] or 0.0), 6),
            "currency": payout_currency,
            "status": p["status"],
            "txid": p.get("txid") or "",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _page_admin(user: Dict[str, Any], job_mgr: JobManager) -> None:
    st.markdown("### 管理")
    tabs = st.tabs(["總覽", "用戶", "提交審核", "策略", "結算", "設定", "Pool"])

    with tabs[0]:
        cycle = db.get_active_cycle()
        st.write("週期", cycle.get("name"), cycle.get("start_ts"), cycle.get("end_ts"))
        ov = db.list_task_overview(limit=500)
        if ov:
            rows = []
            for t in ov:
                pr = {}
                try:
                    pr = json.loads(t.get("progress_json") or "{}")
                except Exception:
                    pr = {}
                rows.append({
                    "task_id": t["id"],
                    "user": t["username"],
                    "pool": t["pool_name"],
                    "symbol": t["symbol"],
                    "tf_min": t["timeframe_min"],
                    "family": t["family"],
                    "partition": t["partition_idx"],
                    "status": t["status"],
                    "done": pr.get("combos_done"),
                    "total": pr.get("combos_total"),
                    "best": pr.get("best_score"),
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        else:
            st.info("無任務。")

    with tabs[1]:
        users = db.list_users(limit=500)
        rows = []
        for u in users:
            rows.append({
                "id": u["id"],
                "username": u["username"],
                "role": u["role"],
                "disabled": int(u.get("disabled") or 0),
                "created_at": u["created_at"],
                "last_login_at": u.get("last_login_at") or "",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        uid = st.number_input("用戶編號", min_value=1, value=1, step=1)
        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button("停用"):
                db.set_user_disabled(int(uid), True)
                db.write_audit_log(int(user["id"]), "user_disable", {"user_id": int(uid)})
                st.rerun()
        with col2:
            if st.button("啟用"):
                db.set_user_disabled(int(uid), False)
                db.write_audit_log(int(user["id"]), "user_enable", {"user_id": int(uid)})
                st.rerun()

    with tabs[2]:
        subs = db.list_submissions(status="pending", limit=300)
        if not subs:
            st.info("無待審核。")
        else:
            rows = []
            for s in subs:
                audit = s.get("audit") or {}
                rows.append({
                    "submission_id": s["id"],
                    "user": s["username"],
                    "pool": s["pool_name"],
                    "symbol": s["symbol"],
                    "tf_min": s["timeframe_min"],
                    "family": s["family"],
                    "score": round(float(audit.get("score") or 0.0), 6),
                    "passed": bool(audit.get("passed")),
                    "submitted_at": s["submitted_at"],
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

            sid = st.number_input("提交編號", min_value=int(min(r["submission_id"] for r in rows)), max_value=int(max(r["submission_id"] for r in rows)), value=int(rows[0]["submission_id"]), step=1)
            alloc = st.number_input("資金配置百分比", min_value=0.0, max_value=100.0, value=10.0, step=1.0)
            note = st.text_input("備註", value="")
            sub_detail = db.get_submission(int(sid))
            if sub_detail and sub_detail.get("params_json"):
                with st.expander("參數", expanded=False):
                    st.json(sub_detail["params_json"])
                with st.expander("審核", expanded=False):
                    st.json(sub_detail.get("audit") or {})

            col1, col2 = st.columns([1, 1])
            with col1:
                if st.button("通過"):
                    db.set_submission_status(int(sid), "approved", approved_by=int(user["id"]))
                    st_id = db.create_strategy_from_submission(int(sid), allocation_pct=float(alloc), note=note)
                    db.write_audit_log(int(user["id"]), "approve", {"submission_id": int(sid), "strategy_id": int(st_id)})
                    st.rerun()
            with col2:
                if st.button("拒絕"):
                    db.set_submission_status(int(sid), "rejected", approved_by=int(user["id"]))
                    db.write_audit_log(int(user["id"]), "reject", {"submission_id": int(sid)})
                    st.rerun()

    with tabs[3]:
        strategies = db.list_strategies(limit=500)
        if not strategies:
            st.info("無策略。")
        else:
            rows = []
            for s in strategies:
                rows.append({
                    "id": s["id"],
                    "user": s["username"],
                    "pool": s["pool_name"],
                    "symbol": s["symbol"],
                    "tf_min": s["timeframe_min"],
                    "family": s["family"],
                    "status": s["status"],
                    "allocation_pct": s["allocation_pct"],
                    "expires_at": s["expires_at"],
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            stid = st.number_input("策略編號", min_value=1, value=1, step=1)
            col1, col2, col3 = st.columns([1, 1, 1])
            with col1:
                if st.button("停用策略"):
                    db.set_strategy_status(int(stid), "paused")
                    db.write_audit_log(int(user["id"]), "strategy_pause", {"strategy_id": int(stid)})
                    st.rerun()
            with col2:
                if st.button("啟用策略"):
                    db.set_strategy_status(int(stid), "active")
                    db.write_audit_log(int(user["id"]), "strategy_activate", {"strategy_id": int(stid)})
                    st.rerun()
            with col3:
                if st.button("失效"):
                    db.set_strategy_status(int(stid), "disqualified")
                    db.write_audit_log(int(user["id"]), "strategy_disqualify", {"strategy_id": int(stid)})
                    st.rerun()

    with tabs[4]:
        st.markdown("週度檢查")
        now = _utc_now()
        bounds = _week_bounds_last_completed(now)
        st.write("week_start_ts", bounds["week_start_ts"])
        st.write("week_end_ts", bounds["week_end_ts"])

        st.markdown("匯入週報表")
        report_file = st.file_uploader("週報表 CSV", type=["csv"], key="weekly_report_upload")
        if report_file is not None:
            if st.button("匯入"):
                with st.spinner("匯入中"):
                    result = _import_weekly_report_csv(report_file)
                if not result.get("ok"):
                    st.error("匯入失敗。")
                    st.write(result)
                else:
                    st.success(f'已匯入 {int(result.get("applied") or 0)} 筆。')
                    st.rerun()


        if st.button("執行本週期最近一週"):
            with st.spinner("執行中"):
                _run_weekly_check(bounds["week_start_ts"], bounds["week_end_ts"])
            st.success("完成。")
            st.rerun()

        st.markdown("未發放清單")
        conn = db._conn()
        try:
            payout_currency = str(db.get_setting(conn, "payout_currency", "USDT") or "USDT").strip()
        finally:
            conn.close()
        payouts = db.list_payouts(status="unpaid", limit=500)
        if payouts:
            rows = []
            for p in payouts:
                wallet = db.get_wallet_address(int(p["user_id"])) or ""
                rows.append({
                    "payout_id": p["id"],
                    "user": p["username"],
                    "week_start": p["week_start_ts"],
                    "amount": round(float(p["amount_usdt"] or 0.0), 6),
                    "currency": payout_currency,
                    "wallet": wallet,
                    "status": p["status"],
                })
            pdf = pd.DataFrame(rows)
            st.dataframe(pdf, use_container_width=True, hide_index=True)
            csv_bytes = pdf.to_csv(index=False).encode("utf-8-sig")
            st.download_button("下載 CSV", data=csv_bytes, file_name="payouts_unpaid.csv", mime="text/csv")

            pid = st.number_input("結算編號", min_value=int(pdf["payout_id"].min()), max_value=int(pdf["payout_id"].max()), value=int(pdf["payout_id"].min()), step=1)
            txid = st.text_input("交易編號", value="")
            if st.button("標記已發放"):
                db.set_payout_paid(int(pid), txid=txid)
                db.write_audit_log(int(user["id"]), "payout_paid", {"payout_id": int(pid)})
                st.rerun()
        else:
            st.info("無未發放。")

    with tabs[5]:
        st.markdown("設定")

        conn = db._conn()
        try:
            numeric_keys = [
                "min_tasks_per_user",
                "max_tasks_per_user",
                "max_concurrent_jobs",
                "task_lease_minutes",
                "candidate_keep_top_n",
                "capital_usdt",
                "payout_rate",
                "default_allocation_pct",
                "min_trades",
                "min_total_return_pct",
                "max_drawdown_pct",
                "min_sharpe",
            ]
            current_numeric = {k: db.get_setting(conn, k) for k in numeric_keys}

            exec_mode = str(db.get_setting(conn, "execution_mode", "server") or "server").strip().lower()
            worker_api_url = str(db.get_setting(conn, "worker_api_url", "http://127.0.0.1:8001") or "http://127.0.0.1:8001").strip()
            payout_currency = str(db.get_setting(conn, "payout_currency", "USDT") or "USDT").strip()
            db_info = db.get_db_info()
        finally:
            conn.close()

        col_a, col_b = st.columns([1.2, 1.0])
        with col_a:
            mode = st.selectbox("execution_mode", options=["server", "worker"], index=0 if exec_mode != "worker" else 1)
            worker_api_url_new = st.text_input("worker_api_url", value=worker_api_url)
            payout_currency_new = st.text_input("payout_currency", value=payout_currency)

        with col_b:
            st.markdown("資料庫")
            st.code(json.dumps({"kind": db_info.get("kind")}, ensure_ascii=False), language="json")

        edited = {}
        for k in numeric_keys:
            v = current_numeric.get(k)
            if isinstance(v, (int, float)):
                edited[k] = st.number_input(k, value=float(v), step=1.0)
            else:
                edited[k] = st.text_input(k, value=str(v))

        if st.button("保存設定"):
            conn = db._conn()
            try:
                for k, v in edited.items():
                    if k in ("min_tasks_per_user", "max_tasks_per_user", "max_concurrent_jobs", "candidate_keep_top_n", "min_trades", "task_lease_minutes"):
                        db.set_setting(conn, k, int(float(v)))
                    else:
                        db.set_setting(conn, k, float(v))
                db.set_setting(conn, "execution_mode", str(mode))
                db.set_setting(conn, "worker_api_url", str(worker_api_url_new))
                db.set_setting(conn, "payout_currency", str(payout_currency_new))
                conn.commit()
            finally:
                conn.close()
            db.write_audit_log(int(user["id"]), "settings_update", {"keys": list(edited.keys()) + ["execution_mode", "worker_api_url"]})
            st.rerun()

    with tabs[6]:
        st.markdown("Pool")

        cycle = db.get_active_cycle()
        if not cycle:
            st.error("週期未初始化。")
            st.stop()

        cycle_id = int(cycle["id"])
        pools = db.list_factor_pools(cycle_id=cycle_id)
        pool_map = {int(p["id"]): p for p in pools}

        if pools:
            rows = []
            for p in pools:
                rows.append(
                    {
                        "id": int(p["id"]),
                        "name": str(p.get("name") or ""),
                        "symbol": str(p.get("symbol") or ""),
                        "timeframe_min": int(p.get("timeframe_min") or 0),
                        "years": int(p.get("years") or 0),
                        "family": str(p.get("family") or ""),
                        "num_partitions": int(p.get("num_partitions") or 0),
                        "active": int(p.get("active") or 0),
                        "created_at": str(p.get("created_at") or ""),
                    }
                )
            st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
        else:
            st.info("無 Pool。")

        st.markdown("Pool 編輯")
        if not pools:
            st.stop()

        pool_ids = [int(p["id"]) for p in pools]

        def _fmt_pool(pid: int) -> str:
            p = pool_map.get(int(pid)) or {}
            return f"{int(pid)} · {p.get('name','')} · {p.get('symbol','')} · {p.get('timeframe_min','')}m · {p.get('family','')}"

        sel_id = st.selectbox("Pool", options=pool_ids, format_func=_fmt_pool, key="pool_sel")
        sel = db.get_pool(int(sel_id))

        if sel:
            with st.form("pool_edit_form", clear_on_submit=False):
                name = st.text_input("name", value=str(sel.get("name") or ""))
                symbol = st.text_input("symbol", value=str(sel.get("symbol") or ""))
                tf_min = st.number_input("timeframe_min", min_value=1, max_value=1440, value=int(sel.get("timeframe_min") or 30), step=1)
                years = st.number_input("years", min_value=1, max_value=10, value=int(sel.get("years") or 3), step=1)
                family = st.text_input("family", value=str(sel.get("family") or ""))
                num_partitions = st.number_input("num_partitions", min_value=8, max_value=2048, value=int(sel.get("num_partitions") or 128), step=8)
                seed = st.number_input("seed", min_value=0, value=int(sel.get("seed") or 0), step=1)

                grid_spec_json = st.text_area("grid_spec_json", value=json.dumps(sel.get("grid_spec") or {}, ensure_ascii=False), height=140)
                risk_spec_json = st.text_area("risk_spec_json", value=json.dumps(sel.get("risk_spec") or {}, ensure_ascii=False), height=140)

                active = st.checkbox("active", value=bool(int(sel.get("active") or 0) == 1))

                save = st.form_submit_button("保存")

            if save:
                try:
                    grid_spec = json.loads(grid_spec_json)
                    risk_spec = json.loads(risk_spec_json)
                except Exception:
                    st.error("JSON 格式錯誤。")
                    st.stop()

                db.update_factor_pool(
                    pool_id=int(sel_id),
                    name=str(name),
                    symbol=str(symbol),
                    timeframe_min=int(tf_min),
                    years=int(years),
                    family=str(family),
                    grid_spec=dict(grid_spec),
                    risk_spec=dict(risk_spec),
                    num_partitions=int(num_partitions),
                    seed=int(seed),
                    active=bool(active),
                )
                db.write_audit_log(int(user["id"]), "pool_update", {"pool_id": int(sel_id)})
                st.rerun()

            col_r1, col_r2 = st.columns([1, 1])
            with col_r1:
                if st.button("重置任務", key="pool_reset_tasks"):
                    n = db.delete_tasks_for_pool(cycle_id=cycle_id, pool_id=int(sel_id))
                    db.write_audit_log(int(user["id"]), "pool_reset_tasks", {"pool_id": int(sel_id), "deleted": int(n)})
                    st.rerun()
            with col_r2:
                if st.button("刷新", key="pool_refresh"):
                    st.rerun()

        st.markdown("複製 Pool")
        src_id = st.selectbox("來源 Pool", options=pool_ids, format_func=_fmt_pool, key="pool_clone_src")
        src = db.get_pool(int(src_id)) if src_id else None
        if src:
            with st.form("pool_clone", clear_on_submit=False):
                name = st.text_input("new_name", value=f"{src.get('name','')} Copy")
                symbol = st.text_input("new_symbol", value=str(src.get("symbol") or "BTC_USDT"))
                tf_min = st.number_input("new_timeframe_min", min_value=1, max_value=1440, value=int(src.get("timeframe_min") or 30), step=1)
                years = st.number_input("new_years", min_value=1, max_value=10, value=int(src.get("years") or 3), step=1)
                family = st.text_input("new_family", value=str(src.get("family") or "RSI"))
                num_partitions = st.number_input("new_num_partitions", min_value=8, max_value=2048, value=int(src.get("num_partitions") or 128), step=8)
                seed = st.number_input("new_seed", min_value=0, value=int(time.time()) & 0x7FFFFFFF, step=1)

                grid_spec_json = st.text_area("new_grid_spec_json", value=json.dumps(src.get("grid_spec") or {}, ensure_ascii=False), height=120)
                risk_spec_json = st.text_area("new_risk_spec_json", value=json.dumps(src.get("risk_spec") or {}, ensure_ascii=False), height=120)

                active = st.checkbox("new_active", value=True)
                submitted = st.form_submit_button("建立")

            if submitted:
                try:
                    grid_spec = json.loads(grid_spec_json)
                    risk_spec = json.loads(risk_spec_json)
                except Exception:
                    st.error("JSON 格式錯誤。")
                    st.stop()

                pid_new = db.create_factor_pool(
                    cycle_id=cycle_id,
                    name=str(name),
                    symbol=str(symbol),
                    timeframe_min=int(tf_min),
                    years=int(years),
                    family=str(family),
                    grid_spec=dict(grid_spec),
                    risk_spec=dict(risk_spec),
                    num_partitions=int(num_partitions),
                    seed=int(seed),
                    active=bool(active),
                )
                db.write_audit_log(int(user["id"]), "pool_clone", {"src_pool_id": int(src_id), "pool_id": int(pid_new)})
                st.rerun()

        st.markdown("新增 Pool")
        with st.form("pool_create", clear_on_submit=False):
            name = st.text_input("create_name", value="New Pool")
            symbol = st.text_input("create_symbol", value="BTC_USDT")
            tf_min = st.number_input("create_timeframe_min", min_value=1, max_value=1440, value=30, step=1)
            years = st.number_input("create_years", min_value=1, max_value=10, value=3, step=1)
            family = st.text_input("create_family", value="RSI")
            num_partitions = st.number_input("create_num_partitions", min_value=8, max_value=2048, value=128, step=8)
            seed = st.number_input("create_seed", min_value=0, value=int(time.time()) & 0x7FFFFFFF, step=1)
            grid_spec_json = st.text_area("create_grid_spec_json", value='{"rsi_p_min":6,"rsi_p_max":21,"rsi_p_step":1,"rsi_lv_min":10,"rsi_lv_max":35,"rsi_lv_step":1}', height=120)
            risk_spec_json = st.text_area("create_risk_spec_json", value='{"tp_min":0.30,"tp_max":1.20,"tp_step":0.10,"sl_min":0.30,"sl_max":1.20,"sl_step":0.10,"max_hold_min":4,"max_hold_max":80,"max_hold_step":4,"fee_side":0.0002,"slippage":0.0,"worst_case":true,"reverse_mode":false}', height=120)
            active = st.checkbox("create_active", value=True)
            submitted = st.form_submit_button("建立")

        if submitted:
            try:
                grid_spec = json.loads(grid_spec_json)
                risk_spec = json.loads(risk_spec_json)
            except Exception:
                st.error("JSON 格式錯誤。")
                st.stop()

            pid = db.create_factor_pool(
                cycle_id=cycle_id,
                name=str(name),
                symbol=str(symbol),
                timeframe_min=int(tf_min),
                years=int(years),
                family=str(family),
                grid_spec=dict(grid_spec),
                risk_spec=dict(risk_spec),
                num_partitions=int(num_partitions),
                seed=int(seed),
                active=bool(active),
            )
            db.write_audit_log(int(user["id"]), "pool_create", {"pool_id": int(pid)})
            st.rerun()

        st.markdown("同步任務")
        st.caption("依目前設定，為所有用戶分配缺少的任務。")

        if st.button("執行同步", key="sync_tasks_all"):
            sconn = db._conn()
            try:
                min_tasks = int(db.get_setting(sconn, "min_tasks_per_user", 2))
                max_tasks = int(db.get_setting(sconn, "max_tasks_per_user", 6))
            finally:
                sconn.close()

            users = db.list_users(limit=10000)
            applied = 0
            for u in users:
                if int(u.get("disabled") or 0) == 1:
                    continue
                before = len(db.list_tasks_for_user(int(u["id"]), cycle_id=cycle_id))
                db.assign_tasks_for_user(int(u["id"]), cycle_id=cycle_id, min_tasks=min_tasks, max_tasks=max_tasks)
                after = len(db.list_tasks_for_user(int(u["id"]), cycle_id=cycle_id))
                if after > before:
                    applied += int(after - before)

            db.write_audit_log(int(user["id"]), "sync_tasks_all_users", {"applied": int(applied)})
            st.success(f"已分配 {int(applied)} 個任務。")
            st.rerun()

def _import_weekly_report_csv(uploaded_file) -> Dict[str, Any]:
    conn = db._conn()
    try:
        capital_usdt = float(db.get_setting(conn, "capital_usdt", 0.0))
        payout_rate = float(db.get_setting(conn, "payout_rate", 0.0))
    finally:
        conn.close()

    report = pd.read_csv(uploaded_file)
    required = {"strategy_id", "week_start_ts", "return_pct"}
    missing = [c for c in required if c not in report.columns]
    if missing:
        return {"ok": False, "error": "missing_columns", "missing": missing}

    applied = 0
    for _, row in report.iterrows():
        try:
            strategy_id = int(row["strategy_id"])
            week_start_ts = str(row["week_start_ts"])
            week_end_ts = str(row["week_end_ts"]) if "week_end_ts" in report.columns and pd.notna(row.get("week_end_ts")) else ""
            return_pct = float(row["return_pct"])
            trades = int(row["trades"]) if "trades" in report.columns and pd.notna(row.get("trades")) else 0
            max_dd = float(row["max_drawdown_pct"]) if "max_drawdown_pct" in report.columns and pd.notna(row.get("max_drawdown_pct")) else 0.0
        except Exception:
            continue

        if not week_end_ts:
            try:
                ws = _parse_iso(week_start_ts)
                week_end_ts = _iso(ws + timedelta(days=7))
            except Exception:
                week_end_ts = week_start_ts

        eligible = return_pct > 0.0
        db.create_weekly_check(
            strategy_id=strategy_id,
            week_start_ts=week_start_ts,
            week_end_ts=week_end_ts,
            return_pct=return_pct,
            max_drawdown_pct=max_dd,
            trades=trades,
            eligible=eligible,
        )

        if not eligible:
            db.set_strategy_status(strategy_id, "disqualified")

        if eligible and capital_usdt > 0.0 and payout_rate > 0.0:
            st_row = db.get_strategy_with_params(strategy_id)
            if not st_row:
                continue
            alloc = float(st_row.get("allocation_pct") or 0.0) / 100.0
            amount = capital_usdt * (return_pct / 100.0) * alloc * payout_rate
            if amount > 0.0 and not db.payout_exists(strategy_id, week_start_ts):
                db.create_payout(strategy_id=strategy_id, user_id=int(st_row["user_id"]), week_start_ts=week_start_ts, amount_usdt=float(amount))

        applied += 1

    return {"ok": True, "applied": applied}

def _run_weekly_check(week_start_ts: str, week_end_ts: str) -> None:
    week_start = _parse_iso(week_start_ts)
    week_end = _parse_iso(week_end_ts)

    conn = db._conn()
    try:
        capital_usdt = float(db.get_setting(conn, "capital_usdt", 0.0))
        payout_rate = float(db.get_setting(conn, "payout_rate", 0.0))
    finally:
        conn.close()

    strategies = db.list_strategies(status="active", limit=1000)
    for s in strategies:
        st_row = db.get_strategy_with_params(int(s["id"]))
        if not st_row:
            continue
        pool = db.get_pool(int(st_row["pool_id"]))
        params = st_row.get("params_json") or {}
        family = str(params.get("family") or pool["family"])
        family_params = dict(params.get("family_params") or {})
        tp = float(params.get("tp"))
        sl = float(params.get("sl"))
        mh = int(params.get("max_hold"))

        csv_main, _csv_1m = bt.ensure_bitmart_data(
            symbol=str(pool["symbol"]),
            main_step_min=int(pool["timeframe_min"]),
            years=int(pool.get("years") or 3),
            auto_sync=True,
            force_full=False,
        )
        df = bt.load_and_validate_csv(csv_main)

        dff = df[(df["ts"] >= week_start) & (df["ts"] < week_end)].copy()
        if len(dff) < 100:
            continue

        res = bt.run_backtest(
            dff,
            family,
            family_params,
            tp,
            sl,
            mh,
            fee_side=float((pool.get("risk_spec") or {}).get("fee_side", 0.0002)),
            slippage=float((pool.get("risk_spec") or {}).get("slippage", 0.0)),
            worst_case=bool((pool.get("risk_spec") or {}).get("worst_case", True)),
            reverse_mode=bool((pool.get("risk_spec") or {}).get("reverse_mode", False)),
        )
        ret = float(res.get("total_return_pct") or 0.0)
        dd = float(res.get("max_drawdown_pct") or 0.0)
        trades = int(res.get("trades") or 0)

        eligible = ret > 0.0
        db.create_weekly_check(
            strategy_id=int(s["id"]),
            week_start_ts=week_start_ts,
            week_end_ts=week_end_ts,
            return_pct=ret,
            max_drawdown_pct=dd,
            trades=trades,
            eligible=eligible,
        )

        if not eligible:
            db.set_strategy_status(int(s["id"]), "disqualified")

        amount = 0.0
        if eligible and capital_usdt > 0.0 and payout_rate > 0.0:
            alloc = float(s.get("allocation_pct") or 0.0) / 100.0
            amount = capital_usdt * (ret / 100.0) * alloc * payout_rate
            if amount > 0.0 and not db.payout_exists(int(s["id"]), week_start_ts):
                db.create_payout(strategy_id=int(s["id"]), user_id=int(s["user_id"]), week_start_ts=week_start_ts, amount_usdt=float(amount))


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, page_icon="🐑", layout="wide", initial_sidebar_state="expanded")
    _style()
    _bootstrap()

    user = _session_user()
    job_mgr = JOB_MANAGER

    if not user:
        _page_auth()
        return

    role = str(user.get("role") or "user")

    with st.sidebar:
        st.markdown(f"### {APP_TITLE}")
        st.markdown(f'<div class="small-muted">{user["username"]} · {role}</div>', unsafe_allow_html=True)
        st.divider()
        page = st.radio("導航", options=["新手教學", "控制台", "任務", "提交", "結算"] + (["管理"] if role == "admin" else []), index=0)
        st.divider()
        if st.button("登出"):
            _logout()
            st.rerun()

    if page == "新手教學":
        _page_tutorial(user)
        return
    if page == "控制台":
        _page_dashboard(user)
        return
    if page == "任務":
        _page_tasks(user, job_mgr)
        return
    if page == "提交":
        _page_submissions(user)
        return
    if page == "結算":
        _page_rewards(user)
        return
    if page == "管理" and role == "admin":
        _page_admin(user, job_mgr)
        return


if __name__ == "__main__":
    main()
