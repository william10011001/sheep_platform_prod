import json
import os
import random
import re
import time
import math
import html
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import plotly.express as px
import streamlit as st

def _get_orig_dataframe():
    orig = getattr(st, "_sheep_orig_dataframe", None)
    if orig is not None and getattr(orig, "__name__", "") != "_dataframe_compat":
        return orig

    cur = getattr(st, "dataframe", None)
    if cur is None:
        return None

    inner = getattr(cur, "_sheep_orig", None)
    if inner is not None and getattr(inner, "__name__", "") != "_dataframe_compat":
        try:
            st._sheep_orig_dataframe = inner
        except Exception:
            pass
        return inner

    return cur


def _dataframe_compat(data=None, **kwargs):
    if "use_container_width" in kwargs:
        u = kwargs.pop("use_container_width")
        kwargs.setdefault("width", "stretch" if bool(u) else "content")

    orig = _get_orig_dataframe()
    if orig is None:
        orig = st.dataframe

    try:
        return orig(data, **kwargs)
    except TypeError:
        if "width" in kwargs:
            w = kwargs.pop("width")
            kwargs["use_container_width"] = (str(w) == "stretch")
        try:
            return orig(data, **kwargs)
        except TypeError:
            kwargs.pop("hide_index", None)
            return orig(data, **kwargs)


if getattr(st.dataframe, "__name__", "") != "_dataframe_compat":
    try:
        st._sheep_orig_dataframe = _get_orig_dataframe()
    except Exception:
        pass
    try:
        _dataframe_compat._sheep_orig = _get_orig_dataframe()
    except Exception:
        pass
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


APP_TITLE = "羊肉爐挖礦分潤任務平台"

_EXEC_MODE_LABEL = {
    "server": "伺服器",
    "worker": "工作端",
}

_TASK_STATUS_LABEL = {
    "assigned": "待執行",
    "queued": "排隊中",
    "running": "執行中",
    "completed": "已完成",
}

_PHASE_LABEL = {
    "idle": "尚未開始",
    "sync_data": "同步資料",
    "sync_data_hash": "同步資料",
    "grid_search": "搜尋中",
    "finished": "已完成",
    "stopped": "已停止",
    "error": "錯誤",
}


def _label_exec_mode(mode: str) -> str:
    return _EXEC_MODE_LABEL.get(str(mode or "").lower(), str(mode or ""))


def _label_task_status(status: str) -> str:
    return _TASK_STATUS_LABEL.get(str(status or "").lower(), str(status or ""))


def _label_phase(phase: str) -> str:
    return _PHASE_LABEL.get(str(phase or "").lower(), str(phase or ""))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

def _issue_api_token(user: Dict[str, Any], ttl_seconds: int = 86400, name: str = "worker") -> Dict[str, Any]:
    """Issue an API token stored in DB (compatible with FastAPI Bearer auth).

    NOTE: Raw token is only shown once; store it securely on the worker side.
    """
    return db.create_api_token(int(user["id"]), ttl_seconds=int(ttl_seconds), name=str(name or "worker"))


_REMEMBER_COOKIE_NAME = "sheep_remember"
_REMEMBER_TOKEN_NAME = "remember"
_REMEMBER_TTL_DAYS = 90


def _get_ws_headers() -> Dict[str, str]:
    # Prefer the non-deprecated API.
    try:
        h = getattr(st, "context", None)
        if h is not None and getattr(st.context, "headers", None) is not None:
            hdrs = st.context.headers
            try:
                return {str(k): str(v) for k, v in dict(hdrs).items()}
            except Exception:
                return {str(k): str(v) for k, v in hdrs.items()}
    except Exception:
        pass

    # Backward compatibility (older Streamlit only).
    try:
        from streamlit.web.server.websocket_headers import _get_websocket_headers  # type: ignore

        h2 = _get_websocket_headers()
        if not h2:
            return {}
        return {str(k): str(v) for k, v in dict(h2).items()}
    except Exception:
        return {}


def _parse_cookie_header(cookie_header: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    raw = str(cookie_header or "").strip()
    if not raw:
        return out
    parts = raw.split(";")
    for p in parts:
        s = p.strip()
        if not s or "=" not in s:
            continue
        k, v = s.split("=", 1)
        k = k.strip()
        v = v.strip()
        if not k:
            continue
        out[k] = v
    return out


def _get_cookie(name: str) -> str:
    headers = _get_ws_headers()
    ck = headers.get("Cookie") or headers.get("cookie") or ""
    cookies = _parse_cookie_header(ck)
    return str(cookies.get(str(name)) or "")


def _queue_set_cookie(name: str, value: str, max_age_s: int) -> None:
    st.session_state["_cookie_op"] = {"op": "set", "name": str(name), "value": str(value), "max_age_s": int(max_age_s)}


def _queue_clear_cookie(name: str) -> None:
    st.session_state["_cookie_op"] = {"op": "clear", "name": str(name)}


def _apply_cookie_ops() -> None:
    op = st.session_state.get("_cookie_op")
    if not isinstance(op, dict):
        return
    try:
        kind = str(op.get("op") or "")
        name = str(op.get("name") or "")
        if not name:
            return

        if kind == "set":
            val = str(op.get("value") or "")
            max_age_s = int(op.get("max_age_s") or 0)
            js = f"""
<script>
(function() {{
  try {{
    var name = {json.dumps(name)};
    var value = {json.dumps(val)};
    var maxAge = {int(max_age_s)};
    var cookie = name + "=" + value + "; Path=/; Max-Age=" + maxAge + "; SameSite=Lax";
    if (window.location && window.location.protocol === "https:") {{
      cookie += "; Secure";
    }}
    document.cookie = cookie;
  }} catch (e) {{}}
}})();
</script>
"""
            st.components.v1.html(js, height=0)
        elif kind == "clear":
            js = f"""
<script>
(function() {{
  try {{
    var name = {json.dumps(name)};
    var cookie = name + "=; Path=/; Max-Age=0; SameSite=Lax";
    if (window.location && window.location.protocol === "https:") {{
      cookie += "; Secure";
    }}
    document.cookie = cookie;
  }} catch (e) {{}}
}})();
</script>
"""
            st.components.v1.html(js, height=0)
    finally:
        try:
            del st.session_state["_cookie_op"]
        except Exception:
            pass


def _try_auto_login_from_cookie() -> bool:
    if st.session_state.get("auth_user_id") is not None:
        return True

    raw = _get_cookie(_REMEMBER_COOKIE_NAME)
    raw = str(raw or "").strip()
    if not raw:
        return False

    try:
        res = db.verify_api_token(raw)
    except Exception:
        res = None

    if not res or not isinstance(res, dict):
        return False

    u = res.get("user")
    t = res.get("token")
    if not isinstance(u, dict) or not isinstance(t, dict):
        return False

    try:
        db.touch_api_token(int(t.get("id") or 0))
    except Exception:
        pass

    _set_session_user(u)
    st.session_state["auth_remember_token_id"] = int(t.get("id") or 0)
    return True


def _ua_is_mobile(user_agent: str) -> bool:
    ua = str(user_agent or "").lower()
    if not ua:
        return False
    keys = ["iphone", "ipad", "android", "mobile", "ipod", "windows phone"]
    return any(k in ua for k in keys)


def _ua_is_inapp_browser(user_agent: str) -> bool:
    ua = str(user_agent or "").lower()
    if not ua:
        return False
    keys = ["line", "instagram", "fbav", "fb_iab", "fban", "micromessenger"]
    return any(k in ua for k in keys)


def _inject_meta(title: str, description: str) -> None:
    t = str(title or "").strip()
    d = str(description or "").strip()
    if not t:
        return
    js = f"""
<script>
(function() {{
  try {{
    document.title = {json.dumps(t)};
    var head = document.getElementsByTagName('head')[0];
    function upsert(name, attr, value) {{
      var sel = attr + "='" + name + "'";
      var el = head.querySelector("meta[" + sel + "]");
      if (!el) {{
        el = document.createElement('meta');
        el.setAttribute(attr, name);
        head.appendChild(el);
      }}
      el.setAttribute('content', value);
    }}
    if ({json.dumps(d)}.length > 0) {{
      upsert('description', 'name', {json.dumps(d)});
      upsert('og:description', 'property', {json.dumps(d)});
      upsert('twitter:description', 'name', {json.dumps(d)});
    }}
    upsert('og:title', 'property', {json.dumps(t)});
    upsert('twitter:title', 'name', {json.dumps(t)});
  }} catch (e) {{}}
}})();
</script>
"""
    st.components.v1.html(js, height=0)



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
          --card: rgba(255,255,255,0.045);
          --card2: rgba(255,255,255,0.06);
          --border: rgba(255,255,255,0.14);
          --text: rgba(255,255,255,0.92);
          --muted: rgba(255,255,255,0.66);
          --accent: rgba(120, 180, 255, 0.95);
          --accent2: rgba(255, 120, 180, 0.65);
          --shadow: 0 12px 30px rgba(0,0,0,0.35);
        }

        .stApp {
          background: radial-gradient(1200px 600px at 20% -10%, rgba(120,180,255,0.25), transparent 60%),
                      radial-gradient(900px 500px at 110% 20%, rgba(255,120,180,0.14), transparent 55%),
                      var(--bg);
          color: var(--text);
        }

        html, body, [class*="css"]  {
          font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Noto Sans", "Helvetica Neue", Arial;
        }

        html { color-scheme: dark; }

        /* In-app browsers (LINE/IG) sometimes force white input backgrounds; enforce readable contrast. */
        div[data-testid="stTextInput"] input,
        div[data-testid="stTextInput"] textarea,
        div[data-testid="stNumberInput"] input,
        div[data-testid="stTextArea"] textarea,
        div[data-testid="stPassword"] input {
          background: rgba(255,255,255,0.04) !important;
          border: 1px solid rgba(255,255,255,0.14) !important;
          border-radius: 12px !important;
          color: var(--text) !important;
          -webkit-text-fill-color: var(--text) !important;
          caret-color: var(--text) !important;
        }
        div[data-testid="stTextInput"] input::placeholder,
        div[data-testid="stTextArea"] textarea::placeholder,
        div[data-testid="stPassword"] input::placeholder {
          color: rgba(255,255,255,0.55) !important;
          -webkit-text-fill-color: rgba(255,255,255,0.55) !important;
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
          background: linear-gradient(180deg, rgba(255,255,255,0.055), rgba(255,255,255,0.03));
          border: 1px solid var(--border);
          border-radius: 16px;
          padding: 16px 16px 12px 16px;
          box-shadow: var(--shadow);
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
          background: rgba(255,255,255,0.035);
          min-width: 160px;
        }
        .metric .k { color: var(--muted); font-size: 12px; }
        .metric .v { color: var(--text); font-size: 20px; font-weight: 600; }

        .small-muted { color: var(--muted); font-size: 12px; }

        header[data-testid="stHeader"] { background: rgba(0,0,0,0); }
        footer { visibility: hidden; }
        #MainMenu { visibility: hidden; }

        .stButton > button, .stDownloadButton > button {
          width: 100%;
          border-radius: 12px;
          border: 1px solid rgba(255,255,255,0.14);
          background: rgba(255,255,255,0.06);
          color: var(--text);
          box-shadow: 0 8px 22px rgba(0,0,0,0.25);
          transition: transform 120ms ease, border-color 120ms ease, filter 120ms ease;
        }

        .stButton > button:hover, .stDownloadButton > button:hover {
          transform: translateY(-1px);
          border-color: rgba(120,180,255,0.55);
          filter: brightness(1.05);
        }

        .stButton > button:focus, .stDownloadButton > button:focus {
          outline: none;
          box-shadow: 0 0 0 2px rgba(120,180,255,0.35);
        }

        div[data-baseweb="input"] input,
        div[data-baseweb="textarea"] textarea,
        div[data-baseweb="select"] > div {
          background: rgba(255,255,255,0.04) !important;
          border: 1px solid rgba(255,255,255,0.14) !important;
          border-radius: 12px !important;
          color: var(--text) !important;
        }

        div[data-baseweb="input"] input:focus,
        div[data-baseweb="textarea"] textarea:focus {
          border-color: rgba(120,180,255,0.55) !important;
          box-shadow: 0 0 0 2px rgba(120,180,255,0.25) !important;
        }

        div[data-baseweb="input"] input,
        div[data-baseweb="textarea"] textarea {
          -webkit-text-fill-color: var(--text) !important;
          caret-color: var(--text) !important;
        }

        div[data-baseweb="input"] input::placeholder,
        div[data-baseweb="textarea"] textarea::placeholder {
          color: rgba(255,255,255,0.40) !important;
          -webkit-text-fill-color: rgba(255,255,255,0.40) !important;
          opacity: 1 !important;
        }

        input:-webkit-autofill,
        textarea:-webkit-autofill,
        select:-webkit-autofill {
          -webkit-text-fill-color: var(--text) !important;
          transition: background-color 99999s ease-in-out 0s;
          box-shadow: 0 0 0px 1000px rgba(255,255,255,0.04) inset !important;
          border: 1px solid rgba(255,255,255,0.14) !important;
        }

        /* Sidebar navigation as full-width product-style buttons (st.radio). */
        div[data-testid="stSidebar"] label[data-baseweb="radio"] {
          width: 100%;
          border: 1px solid rgba(255,255,255,0.14);
          border-radius: 12px;
          padding: 0.55rem 0.7rem;
          margin: 0.35rem 0;
          background: rgba(255,255,255,0.03);
        }
        div[data-testid="stSidebar"] label[data-baseweb="radio"]:hover {
          border-color: rgba(120,180,255,0.45);
        }
        div[data-testid="stSidebar"] label[data-baseweb="radio"]:has(input:checked) {
          background: rgba(120,180,255,0.12);
          border-color: rgba(120,180,255,0.55);
        }
        div[data-testid="stSidebar"] label[data-baseweb="radio"] > div {
          width: 100%;
        }


        /* Global progress partition map */
        .pm_legend {
          display: flex;
          gap: 0.8rem;
          flex-wrap: wrap;
          margin: 0.25rem 0 0.6rem 0;
        }
        .pm_item { display: inline-flex; align-items: center; gap: 0.35rem; }
        .pm_swatch { width: 10px; height: 10px; border-radius: 3px; border: 1px solid rgba(255,255,255,0.18); }
        .pm_swatch.done { background: rgba(120,180,255,0.55); }
        .pm_swatch.running { background: rgba(120,255,180,0.45); }
        .pm_swatch.reserved { background: rgba(255,200,120,0.45); }
        .pm_swatch.available { background: rgba(255,255,255,0.12); }

        .pm_grid {
          display: grid;
          gap: 3px;
          padding: 0.4rem 0.2rem;
        }
        .pm_cell {
          width: 10px;
          height: 10px;
          border-radius: 3px;
          border: 1px solid rgba(255,255,255,0.12);
          background: rgba(255,255,255,0.10);
        }
        .pm_cell.done { background: rgba(120,180,255,0.60); }
        .pm_cell.running { background: rgba(120,255,180,0.50); }
        .pm_cell.reserved { background: rgba(255,200,120,0.50); }
        .pm_cell.available { background: rgba(255,255,255,0.12); }
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

    # Ensure admin account is present and credentials are deterministic.
    admin_username = str(os.environ.get("SHEEP_BOOTSTRAP_ADMIN_USER", "sheep") or "sheep").strip() or "sheep"
    admin_password = str(os.environ.get("SHEEP_BOOTSTRAP_ADMIN_PASS", "@@Wm105020") or "@@Wm105020").strip() or "@@Wm105020"

    try:
        uname_norm = normalize_username(admin_username)
        row = db.get_user_by_username(uname_norm)
    except Exception:
        row = None

    try:
        if row:
            conn = db._conn()
            try:
                conn.execute(
                    "UPDATE users SET password_hash = ?, role = 'admin', disabled = 0 WHERE id = ?",
                    (hash_password(admin_password), int(row["id"])),
                )
                conn.commit()
            finally:
                conn.close()
        else:
            db.create_user(username=admin_username, password_hash=hash_password(admin_password), role="admin", wallet_address="", wallet_chain="TRC20")

        try:
            db.write_audit_log(None, "ensure_admin", {"username": admin_username})
        except Exception:
            pass
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
    # Revoke remember token if present
    try:
        tid = int(st.session_state.get("auth_remember_token_id") or 0)
    except Exception:
        tid = 0
    if tid > 0:
        try:
            db.revoke_api_token(int(tid))
        except Exception:
            pass

    _queue_clear_cookie(_REMEMBER_COOKIE_NAME)

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
        remember = st.checkbox("在本裝置記住我", value=False, key="login_remember_me")

        if captcha_enabled:
            st.markdown('<div class="small-muted">滑動驗證碼：把滑桿拖到最右邊（100）</div>', unsafe_allow_html=True)
            st.slider(" ", min_value=0, max_value=100, value=0, step=1, key=captcha_key)

        submitted = st.form_submit_button("登入")

    if not submitted:
        return

    if captcha_enabled:
        dt = float(time.time() - float(st.session_state.get("captcha_t0") or time.time()))
        if int(st.session_state.get(captcha_key) or 0) != 100:
            st.error("滑動驗證碼未通過。")
            st.session_state["captcha_nonce"] = random.randint(1000, 9999)
            st.session_state["captcha_t0"] = time.time()
            return
        if dt < captcha_min_s:
            st.error("滑動時間過短。")
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

    if bool(remember):
        ttl_days = int(_REMEMBER_TTL_DAYS)
        tok = _issue_api_token(user, ttl_seconds=int(ttl_days) * 86400, name=_REMEMBER_TOKEN_NAME)
        raw = str(tok.get("token") or "")
        max_age_s = int(ttl_days) * 86400
        _queue_set_cookie(_REMEMBER_COOKIE_NAME, raw, max_age_s)
        st.session_state["auth_remember_token_id"] = int(tok.get("token_id") or 0)
    else:
        _queue_clear_cookie(_REMEMBER_COOKIE_NAME)

    st.success("登入成功。")
    st.session_state["nav_page_pending"] = "控制台"
    st.rerun()

def _register_form() -> None:
    st.markdown("### 註冊")

    tos_text = ""
    tos_version = ""
    try:
        conn = db._conn()
        try:
            tos_text = str(db.get_setting(conn, "tos_text", "") or "")
            tos_version = str(db.get_setting(conn, "tos_version", "") or "")
        finally:
            conn.close()
    except Exception:
        tos_text = ""
        tos_version = ""

    if st.button("查看服務條款", key="open_tos_dialog"):
        st.session_state["tos_dialog_open"] = True

    if bool(st.session_state.get("tos_dialog_open")):
        @st.dialog("服務條款與分潤風險協議")
        def _tos_dialog() -> None:
            if tos_text.strip():
                st.markdown(tos_text)
            else:
                st.markdown("服務條款暫未設定。")
            if st.button("關閉", key="close_tos_dialog"):
                st.session_state["tos_dialog_open"] = False
                st.rerun()

        _tos_dialog()

    with st.form("register_form", clear_on_submit=False):
        username = st.text_input("帳號", value="")
        password = st.text_input("密碼", value="", type="password", placeholder="至少 6 碼，需包含英文字母與數字")
        password2 = st.text_input("確認密碼", value="", type="password")

        tos_ok = st.checkbox("我已閱讀並同意平台服務條款與分潤風險協議", value=False, key="register_tos_ok")
        remember = st.checkbox("在本裝置記住我", value=False, key="register_remember_me")
        submitted = st.form_submit_button("建立帳號並登入")

    if not submitted:
        return

    uname = normalize_username(username)
    if not uname:
        st.error("帳號不可為空。")
        return
    if len(uname) > 64:
        st.error("帳號長度上限為 64 字元。")
        return
    if any(ch in uname for ch in ["\r", "\n"]):
        st.error("帳號不可包含換行字元。")
        return

    pw = str(password or "")
    if len(pw) < 6:
        st.error("密碼長度至少 6 字元。")
        return
    has_alpha = any(ch.isalpha() for ch in pw)
    has_digit = any(ch.isdigit() for ch in pw)
    if not (has_alpha and has_digit):
        st.error("密碼需同時包含英文字母與數字。")
        return
    if pw != str(password2 or ""):
        st.error("密碼不一致。")
        return

    if not bool(tos_ok):
        st.error("必須同意服務條款與風險協議才可註冊。")
        return

    if db.get_user_by_username(uname):
        st.error("帳號已存在。")
        return

    try:
        uid = db.create_user(username=uname, password_hash=hash_password(pw), role="user", wallet_address="", wallet_chain="TRC20")
        db.write_audit_log(uid, "register", {"username": uname})
        if tos_version.strip():
            db.write_audit_log(uid, "tos_accept", {"version": tos_version})
        else:
            db.write_audit_log(uid, "tos_accept", {})
    except Exception:
        st.error("建立失敗。")
        return

    user = db.get_user_by_id(int(uid))
    if not user:
        st.success("帳號已建立。")
        return

    _set_session_user(user)

    if bool(remember):
        ttl_days = int(_REMEMBER_TTL_DAYS)
        tok = _issue_api_token(user, ttl_seconds=int(ttl_days) * 86400, name=_REMEMBER_TOKEN_NAME)
        raw = str(tok.get("token") or "")
        max_age_s = int(ttl_days) * 86400
        _queue_set_cookie(_REMEMBER_COOKIE_NAME, raw, max_age_s)
        st.session_state["auth_remember_token_id"] = int(tok.get("token_id") or 0)
    else:
        _queue_clear_cookie(_REMEMBER_COOKIE_NAME)

    st.success("帳號已建立並完成登入。")
    st.session_state["nav_page_pending"] = "控制台"
    st.rerun()


def _render_auth_onboarding_dialog() -> None:
    video_path = ""
    try:
        conn = db._conn()
        try:
            video_path = str(db.get_setting(conn, "tutorial_video_path", "") or "").strip()
        finally:
            conn.close()
    except Exception:
        video_path = ""

    has_video = bool(video_path and os.path.exists(video_path))

    def _dialog_body() -> None:
        tab_names = ["總覽", "合作模式", "分潤", "成本", "風險"]
        if has_video:
            tab_names.append("影片")
        tabs = st.tabs(tab_names)

        with tabs[0]:
            st.markdown("#### 這不是傳統挖礦，是參數搜尋")
            st.write("平台把一個策略的參數空間切成很多份，每個人拿一份去跑回測。你提供算力，平台提供任務與審核。")
            st.markdown("#### 流程")
            st.components.v1.html(
                """
            <div class="sp-flow-wrap">
            <div class="sp-flow-head">
                <div class="sp-flow-title">步驟總覽</div>
                <div class="sp-flow-hint">滑鼠移動或點擊步驟可查看詳細說明</div>
            </div>

            <div class="sp-flow-track" id="spFlowTrack">
                <button class="sp-step" data-step="login" type="button">註冊 / 登入</button>
                <span class="sp-sep">></span>
                <button class="sp-step" data-step="start" type="button">開始任務</button>
                <span class="sp-sep">></span>
                <button class="sp-step" data-step="cand" type="button">候選結果</button>
                <span class="sp-sep">></span>
                <button class="sp-step" data-step="verify" type="button">伺服器複驗</button>
                <span class="sp-sep">></span>
                <button class="sp-step" data-step="submit" type="button">提交策略池</button>
                <span class="sp-sep">></span>
                <button class="sp-step" data-step="settle" type="button">週期結算</button>
            </div>

            <div class="sp-flow-detail" id="spFlowDetail">
                <div class="sp-flow-detail-title" id="spFlowDetailTitle">註冊 / 登入</div>
                <div class="sp-flow-detail-body" id="spFlowDetailBody">
                建立帳號或登入後，即可領取任務並開始提供算力。註冊時需同意服務條款與分潤風險協議。
                </div>

                <div class="sp-flow-detail-meta">
                <div class="sp-meta-pill" id="spFlowMeta1">建議：使用電腦</div>
                <div class="sp-meta-pill" id="spFlowMeta2">重點：保持網路穩定</div>
                </div>
            </div>
            </div>

            <style>
            .sp-flow-wrap{
                border-radius:16px;
                border:1px solid rgba(255,255,255,0.10);
                background:rgba(255,255,255,0.04);
                padding:14px 14px 12px 14px;
                color:rgba(255,255,255,0.92);
            }

            .sp-flow-head{
                display:flex;
                align-items:flex-end;
                justify-content:space-between;
                gap:10px;
                margin-bottom:10px;
            }

            .sp-flow-title{
                font-size:14px;
                font-weight:800;
                letter-spacing:.2px;
                opacity:1.0;
            }

            .sp-flow-hint{
                font-size:12.5px;
                opacity:.88;
                text-align:right;
                color:rgba(255,255,255,0.88);
            }

            .sp-flow-track{
                display:flex;
                align-items:center;
                gap:10px;
                flex-wrap:nowrap;
                overflow-x:auto;
                overflow-y:hidden;
                padding:8px 6px 10px 6px;
                scrollbar-width:thin;
                scroll-behavior:smooth;
            }

            .sp-flow-track::-webkit-scrollbar{height:10px}
            .sp-flow-track::-webkit-scrollbar-thumb{background:rgba(255,255,255,0.12);border-radius:999px}
            .sp-flow-track::-webkit-scrollbar-track{background:rgba(255,255,255,0.04);border-radius:999px}

            .sp-sep{
                opacity:.35;
                flex:0 0 auto;
                user-select:none;
            }

            .sp-step{
                flex:0 0 auto;
                border-radius:999px;
                padding:10px 12px;
                border:1px solid rgba(255,255,255,0.10);
                background:rgba(255,255,255,0.06);
                color:rgba(255,255,255,0.92);
                font-size:13px;
                cursor:pointer;
                transition:transform .06s ease, border-color .15s ease, background .15s ease;
                user-select:none;
                outline:none;
            }

            .sp-step:hover{
                border-color: rgba(120,180,255,0.55);
                background: rgba(120,180,255,0.10);
            }

            .sp-step:active{
                transform: translateY(1px);
            }

            .sp-step.is-active{
                border-color: rgba(120,180,255,0.75);
                background: rgba(120,180,255,0.14);
            }

            .sp-flow-detail{
                margin-top:10px;
                border-radius:14px;
                border:1px solid rgba(255,255,255,0.10);
                background:rgba(0,0,0,0.22);
                padding:12px 12px 10px 12px;
            }

            .sp-flow-detail-title{
                font-size:13px;
                font-weight:800;
                margin-bottom:6px;
                opacity:.95;
            }

            .sp-flow-detail-body{
                font-size:13px;
                line-height:1.6;
                opacity:.92;
                color:rgba(255,255,255,0.90);
            }

            .sp-flow-detail-meta{
                display:flex;
                gap:8px;
                flex-wrap:wrap;
                margin-top:10px;
            }

            .sp-meta-pill{
                font-size:12px;
                padding:6px 10px;
                border-radius:999px;
                border:1px solid rgba(255,255,255,0.10);
                background:rgba(255,255,255,0.06);
                opacity:.88;
            }

            @media (max-width: 520px){
                .sp-flow-head{
                    flex-direction:column;
                    align-items:flex-start;
                }
                .sp-flow-hint{
                    text-align:left;
                }
                .sp-flow-track{
                    flex-wrap:wrap;
                    overflow-x:hidden;
                    padding:8px 0 10px 0;
                }
                .sp-sep{
                    display:none;
                }
                .sp-step{
                    width:100%;
                    border-radius:14px;
                    text-align:left;
                    padding:12px 14px;
                    font-size:13.5px;
                }
            }
            </style>

            <script>
            (function(){
            const steps = {
                login: {
                title: "註冊 / 登入",
                body: "建立帳號或登入後，即可領取任務並開始提供算力。註冊時需同意服務條款與分潤風險協議。",
                meta1: "建議：使用電腦",
                meta2: "重點：同意條款"
                },
                start: {
                title: "開始任務",
                body: "選擇策略池與運行模式後開始領取分割任務。任務在本機運算，產出候選參數與績效指標。",
                meta1: "建議：保持前景運行",
                meta2: "重點：算力越穩越好"
                },
                cand: {
                title: "候選結果",
                body: "本機運算會產生候選參數組合。候選只代表本機結果，尚未被平台視為有效成果。",
                meta1: "建議：只提交高品質候選",
                meta2: "重點：避免使用過期資料"
                },
                verify: {
                title: "伺服器複驗",
                body: "伺服器會在受控環境中重跑回測，核對結果一致性。誤差超過門檻的回報會被拒絕並記錄。",
                meta1: "建議：資料版本需一致",
                meta2: "重點：反作弊必經"
                },
                submit: {
                title: "提交策略池",
                body: "複驗通過的候選會進入策略池候選名單，等待策略池規則篩選、排程與淘汰機制運作。",
                meta1: "建議：持續貢獻提升採用率",
                meta2: "重點：策略池會重置"
                },
                settle: {
                title: "週期結算",
                body: "依平台結算週期統計可分配收益並產出明細。提現需滿足最低門檻與手續費規則。",
                meta1: "建議：先在結算頁設定地址",
                meta2: "重點：提現門檻與費用"
                }
            };

            const track = document.getElementById("spFlowTrack");
            const titleEl = document.getElementById("spFlowDetailTitle");
            const bodyEl = document.getElementById("spFlowDetailBody");
            const meta1El = document.getElementById("spFlowMeta1");
            const meta2El = document.getElementById("spFlowMeta2");

            function setActive(stepKey){
                const cfg = steps[stepKey];
                if(!cfg) return;

                titleEl.textContent = cfg.title;
                bodyEl.textContent = cfg.body;
                meta1El.textContent = cfg.meta1 || "";
                meta2El.textContent = cfg.meta2 || "";

                const btns = track.querySelectorAll(".sp-step");
                btns.forEach(b => b.classList.remove("is-active"));
                const activeBtn = track.querySelector('.sp-step[data-step="'+stepKey+'"]');
                if(activeBtn){
                activeBtn.classList.add("is-active");
                try{
                    activeBtn.scrollIntoView({block:"nearest", inline:"nearest"});
                }catch(e){
                    try{ activeBtn.scrollIntoView(); }catch(e2){}
                }
                }
            }

            const pick = (e) => {
                const target = e && e.target ? e.target : null;
                const btn = target && target.closest ? target.closest(".sp-step") : null;
                if(!btn) return;
                setActive(btn.getAttribute("data-step"));
            };

            track.addEventListener("mouseover", pick);
            track.addEventListener("click", pick);
            track.addEventListener("pointerdown", pick);
            track.addEventListener("touchstart", pick, {passive:true});

            setActive("login");
            })();
            </script>
                """,
                height=320,
                scrolling=False,
            )


        with tabs[1]:
            st.markdown("#### 你提供什麼")
            st.write("本機算力（CPU/GPU）、穩定網路、運行時間。")
            st.markdown("#### 平台提供什麼")
            st.write("策略框架、任務分割、反作弊複驗、策略池管理與結算。")
            st.markdown("#### 反作弊與公平")
            st.write("Worker 回報的候選參數會在伺服器端重跑回測。結果不一致將被視為異常處理。")

        with tabs[2]:
            st.markdown("#### 分潤怎麼來")
            st.write("策略池中的策略可能被拿去做模擬或實盤；若有可分配收益，依平台規則分配。")
            st.markdown("#### 你會拿到什麼")
            st.write("不是保底收入。你貢獻的策略被採用、且結算週期有可分配金額，才會有發放。")
            st.markdown("#### 結算方式")
            st.write("以 USDT 計算，明細會在結算頁看到。")

        with tabs[3]:
            st.markdown("#### 成本")
            st.write("CPU/GPU 使用率上升、耗電、風扇噪音、網路流量。長時間建議用電腦。")
            st.markdown("#### 建議")
            st.write("不要用行動裝置長時間跑任務。若在應用程式內建瀏覽器遇到輸入框問題，改用系統瀏覽器。")

        with tabs[4]:
            st.markdown("#### 風險")
            st.write("實盤有風險，不保證收益。策略可能因市場變化失效而停用或淘汰。")
            st.markdown("#### 重要提醒")
            st.write("分潤地址填錯，資產通常無法追回。請在結算頁設定並再次確認。")

        if has_video:
            with tabs[-1]:
                try:
                    data = open(video_path, "rb").read()
                    if video_path and os.path.exists(video_path):
                        try:
                            st.markdown("#### 教學影片")
                            st.video(video_path)
                        except Exception:
                            st.markdown('<div class="small-muted">教學影片載入失敗。</div>', unsafe_allow_html=True)

                except Exception:
                    st.warning("教學影片載入失敗。")

        if st.button("我已了解", key="auth_onboarding_close"):
            st.session_state["auth_onboarding_open"] = False
            st.rerun()

    dlg = getattr(st, "dialog", None)
    if callable(dlg):
        try:
            decorator = dlg("流程與操作要點")

            if hasattr(decorator, "__enter__"):
                with decorator:
                    _dialog_body()
            else:
                @decorator
                def _run_dialog() -> None:
                    _dialog_body()

                _run_dialog()
        except Exception:
            st.session_state["auth_onboarding_open"] = False
            st.rerun()
    else:
        st.session_state["auth_onboarding_open"] = False
        st.rerun()


def _page_auth() -> None:
    headers = _get_ws_headers()
    ua = str(headers.get("User-Agent") or headers.get("user-agent") or "")

    if _ua_is_mobile(ua):
        st.warning("偵測到行動裝置。挖礦計算量大且背景執行不穩定，建議改用電腦。")

    if _ua_is_inapp_browser(ua):
        st.info("偵測到應用程式內建瀏覽器。若遇到登入框顯示異常，請改用系統瀏覽器開啟。")

    if "auth_onboarding_seen" not in st.session_state:
        st.session_state["auth_onboarding_seen"] = False
    if "auth_onboarding_open" not in st.session_state:
        st.session_state["auth_onboarding_open"] = False

    if not bool(st.session_state.get("auth_onboarding_seen")):
        st.session_state["auth_onboarding_seen"] = True
        st.session_state["auth_onboarding_open"] = True

    top_l, top_r = st.columns([1.0, 0.28])
    with top_l:
        st.markdown(f"## {APP_TITLE}")
        st.markdown('<div class="small-muted">登入或註冊後即可開始參與運算任務。</div>', unsafe_allow_html=True)
    with top_r:
        if st.button("流程與操作要點", key="auth_open_onboarding"):
            st.session_state["auth_onboarding_open"] = True
            st.rerun()

    if bool(st.session_state.get("auth_onboarding_open")):
        _render_auth_onboarding_dialog()

    col1, col2 = st.columns([1, 1])
    with col1:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        _register_form()
        st.markdown("</div>", unsafe_allow_html=True)
    with col2:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        _login_form()
        st.markdown("</div>", unsafe_allow_html=True)


def _render_kpi(title: str, value: Any, sub: str = "") -> str:
    v = value if value is not None else "-"
    return f'<div class="metric"><div class="k">{title}</div><div class="v">{v}</div><div class="small-muted">{sub}</div></div>'


@st.cache_data(ttl=10)
def _cached_global_progress_snapshot(cycle_id: int) -> Dict[str, Any]:
    return db.get_global_progress_snapshot(int(cycle_id))


def _partition_bucket(task: Dict[str, Any], system_user_id: int) -> str:
    status = str(task.get("status") or "")
    uid = int(task.get("user_id") or 0)
    if status == "completed":
        return "已完成"
    if status == "running":
        return "執行中"
    if status == "assigned":
        if uid and uid != int(system_user_id):
            return "已預訂"
        return "待挖掘"
    return "其他"


def _partition_map_html(tasks: List[Dict[str, Any]], system_user_id: int) -> str:
    n = len(tasks)
    if n <= 0:
        return '<div class="small-muted">無分割資料</div>'

    cols = int(min(48, max(12, int(math.sqrt(n)) + 1)))
    cells: List[str] = []
    for t in tasks:
        idx = int(t.get("partition_idx") or 0)
        bucket = _partition_bucket(t, system_user_id)
        cls = {
            "已完成": "pm_done",
            "執行中": "pm_running",
            "已預訂": "pm_reserved",
            "待挖掘": "pm_available",
        }.get(bucket, "pm_other")
        user_name = str(t.get("username") or "")
        title = f"分割 {idx+1}/{n} · {bucket}" + (f" · {user_name}" if user_name else "")
        cells.append(f'<div class="pm_cell {cls}" title="{html.escape(title, quote=True)}"></div>')
    return f'<div class="pm_grid" style="grid-template-columns: repeat({cols}, 10px);">{"".join(cells)}</div>'


def _render_global_progress(cycle_id: int) -> None:
    try:
        snap = _cached_global_progress_snapshot(int(cycle_id))
    except Exception:
        st.warning("全域進度暫時無法讀取。")
        return

    system_uid = int(snap.get("system_user_id") or 0)
    pools = list(snap.get("pools") or [])
    if not pools:
        st.markdown('<div class="small-muted">目前沒有可用的進度資料。</div>', unsafe_allow_html=True)
        return

    total_est = 0.0
    total_done = 0.0
    total_speed = 0.0
    total_running = 0
    for p in pools:
        for t in p.get("tasks") or []:
            est = float(t.get("estimated_combos") or 0.0)
            prog = t.get("progress_json") or {}
            done = float(prog.get("combos_done") or 0.0)
            speed = float(prog.get("speed_cps") or 0.0)
            total_est += est
            total_done += done
            if str(t.get("status") or "") == "running":
                total_speed += speed
                total_running += 1

    st.markdown("#### 全域挖掘進度")
    ratio = (total_done / total_est) if total_est > 0 else 0.0
    st.progress(min(1.0, max(0.0, float(ratio))))
    cols = st.columns(3)
    with cols[0]:
        st.markdown(_render_kpi("已跑組合", f"{int(total_done):,}", f"預估總量 {int(total_est):,}"), unsafe_allow_html=True)
    with cols[1]:
        st.markdown(_render_kpi("全域進度", f"{ratio*100:.1f}%", "以預估工作量計算"), unsafe_allow_html=True)
    with cols[2]:
        st.markdown(_render_kpi("全域速度", f"{total_speed:.1f} 組合/秒", f"執行中任務 {total_running}"), unsafe_allow_html=True)

    recs: List[Dict[str, Any]] = []
    for p in pools:
        pool_name = str(p.get("pool_name") or "") or f"Pool {p.get('pool_id')}"
        tasks = list(p.get("tasks") or [])
        by_bucket: Dict[str, float] = {}
        for t in tasks:
            b = _partition_bucket(t, system_uid)
            by_bucket[b] = by_bucket.get(b, 0.0) + float(t.get("estimated_combos") or 0.0)
        for b, v in by_bucket.items():
            recs.append({"策略池": pool_name, "狀態": b, "預估組合": v})

    if recs:
        df = pd.DataFrame(recs)
        fig = px.bar(df, x="策略池", y="預估組合", color="狀態", barmode="stack", height=320)
        st.plotly_chart(fig, use_container_width=True)

    for p in pools:
        pool_name = str(p.get("pool_name") or "") or f"Pool {p.get('pool_id')}"
        meta = f"{p.get('symbol')} · {p.get('timeframe_min')}m · {p.get('family')}"
        with st.expander(f"{pool_name}（{meta}）", expanded=False):
            tasks = list(p.get("tasks") or [])
            st.markdown(_partition_map_html(tasks, system_uid), unsafe_allow_html=True)
            # Lightweight legend
            st.markdown(
                '<div class="pm_legend">'
                '<span class="pm_key"><span class="pm_cell pm_available"></span>待挖掘</span>'
                '<span class="pm_key"><span class="pm_cell pm_reserved"></span>已預訂</span>'
                '<span class="pm_key"><span class="pm_cell pm_running"></span>執行中</span>'
                '<span class="pm_key"><span class="pm_cell pm_done"></span>已完成</span>'
                '</div>',
                unsafe_allow_html=True,
            )




def _page_tutorial(user: Optional[Dict[str, Any]] = None) -> None:
    st.markdown(f"### {APP_TITLE} · 使用指引")
    st.markdown('<div class="small-muted">流程與操作要點</div>', unsafe_allow_html=True)

    st.markdown("")

    st.components.v1.html(
        """
        <div style="padding:16px;border-radius:14px;background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);">
          <div style="font-size:14px;opacity:.9;margin-bottom:10px;">流程</div>
          <div class="sp-flow">
            <span class="sp-step">登入</span>
            <span class="sp-sep">></span>
            <span class="sp-step sp-focus">開始全部任務</span>
            <span class="sp-sep">></span>
            <span class="sp-step">候選結果</span>
            <span class="sp-sep">></span>
            <span class="sp-step">提交策略</span>
            <span class="sp-sep">></span>
            <span class="sp-step">結算</span>
          </div>
        </div>
        <style>
          .sp-flow{
            display:flex;gap:10px;align-items:center;flex-wrap:wrap;
          }
          .sp-step{
            padding:10px 12px;
            border-radius:999px;
            background:rgba(255,255,255,0.06);
            border:1px solid rgba(255,255,255,0.10);
            font-size:13px;
          }
          .sp-sep{opacity:.35}
          .sp-focus{
            border-color: rgba(120,180,255,0.55);
            background: rgba(120,180,255,0.10);
          }
        </style>
        """,
        height=140,
    )

    st.markdown("")
    st.markdown("#### 1) 登入或建立帳號")
    st.write("帳號可使用一般文字（不建議空白與換行）。")

    st.markdown("#### 2) 任務執行")
    st.write(
        "在任務頁點擊開始全部任務後，系統會自動排隊並依序執行。\n"
        "若採用自動刷新，任務完成後會自動接續下一批。"
    )

    st.markdown("#### 3) 進度與最佳參數")
    st.write("任務卡片會顯示參數進度、最佳分數、達標狀態、速度與預估剩餘時間。")

    st.markdown("#### 4) 候選結果與提交")
    st.write("任務完成後會產生候選列表。提交後會進入策略池並參與後續結算。")

    st.markdown("#### 5) 結算資料")
    st.write("結算頁可更新分潤地址。地址會做基本格式檢查。")

    st.markdown("")
    st.info("提示：若需持續自動接續任務，啟用自動刷新。")

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

    rows = []
    for t in tasks:
        try:
            prog = json.loads(t.get("progress_json") or "{}")
        except Exception:
            prog = {}

        combos_done = int(prog.get("combos_done") or 0)
        combos_total = int(prog.get("combos_total") or 0)
        pct = (100.0 * float(combos_done) / float(combos_total)) if combos_total > 0 else 0.0

        best_score = prog.get("best_any_score")
        passed = bool(prog.get("best_any_passed") or False)

        eta_s = prog.get("eta_s")
        speed_cps = prog.get("speed_cps")
        phase = str(prog.get("phase") or "")
        updated_at = str(prog.get("updated_at") or "")

        status_raw = str(t.get("status") or "")
        status_cn = _TASK_STATUS_LABEL.get(status_raw, status_raw)
        phase_cn = _PHASE_LABEL.get(phase, phase)

        rows.append(
            {
                "任務ID": int(t["id"]),
                "策略池": str(t.get("pool_name") or ""),
                "交易對": str(t.get("symbol") or ""),
                "週期": f"{int(t.get('timeframe_min') or 0)}m",
                "策略族": str(t.get("family") or ""),
                "分割": f'{int(t.get("partition_idx") or 0) + 1}/{int(t.get("num_partitions") or 1)}',
                "狀態": status_cn,
                "階段": phase_cn,
                "進度(%)": round(float(pct), 2),
                "已跑組合": int(combos_done),
                "組合總量": int(combos_total),
                "最佳分數": None if best_score is None else round(float(best_score), 6),
                "達標": bool(passed),
                "速度(組合/秒)": None if speed_cps is None else round(float(speed_cps), 3),
                "預估剩餘(秒)": None if eta_s is None else round(float(eta_s), 1),
                "更新時間": updated_at,
                "__status_raw": status_raw,
            }
        )

    df = pd.DataFrame(rows)

    order = {"running": 0, "assigned": 1, "queued": 2, "completed": 3, "expired": 4, "revoked": 5}
    try:
        df["_ord"] = df["__status_raw"].map(order).fillna(9)
        df = df.sort_values(["_ord", "任務ID"], ascending=[True, False]).drop(columns=["_ord", "__status_raw"])
    except Exception:
        pass

    st.dataframe(df, use_container_width=True, hide_index=True)

    st.markdown("### 全域進度")
    _render_global_progress(int(cycle.get("id") or 0))


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
    exec_mode_label = _EXEC_MODE_LABEL.get(exec_mode, exec_mode)
    st.markdown(f'<div class="small-muted">執行模式：{exec_mode_label}</div>', unsafe_allow_html=True)

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

        status_map = {
            "assigned": "待執行",
            "queued": "隊列中",
            "running": "執行中",
            "completed": "已完成",
            "expired": "已過期",
            "revoked": "已撤銷",
        }
        phase_map = {
            "idle": "待命",
            "sync_data": "資料同步",
            "build_grid": "建立參數",
            "grid_search": "參數搜尋",
            "stopped": "已停止",
            "error": "錯誤",
        }

        status_label = status_map.get(str(view_status), str(view_status) or "-")
        phase_label = phase_map.get(str(phase), str(phase) or "-")
        passed_label = "是" if bool(best_any_passed) else "否"

        def _pill_class(kind: str) -> str:
            k = str(kind or "")
            if k in ("completed",):
                return "ok"
            if k in ("running",):
                return "info"
            if k in ("queued", "assigned"):
                return "warn"
            if k in ("expired", "revoked", "error"):
                return "bad"
            return "neutral"

        top_a, top_b, top_c, top_d, top_e = st.columns([1.0, 1.1, 1.5, 1.3, 1.0])
        with top_a:
            st.markdown(
                f'<span class="pill pill-{_pill_class(view_status)}">{status_label}</span>',
                unsafe_allow_html=True,
            )
        with top_b:
            st.markdown(f'<div class="small-muted">階段</div><div class="kpi">{phase_label}</div>', unsafe_allow_html=True)
        with top_c:
            prog_text = "-"
            sync = prog.get("sync")
            if int(combos_total) > 0:
                prog_text = f"{int(combos_done)}/{int(combos_total)}"
            elif str(phase) == "sync_data" and isinstance(sync, dict):
                items = sync.get("items")
                cur = str(sync.get("current") or "")
                if isinstance(items, dict) and cur in items:
                    try:
                        done_i = int(items[cur].get("done") or 0)
                        total_i = int(items[cur].get("total") or 0)
                        if total_i > 0:
                            prog_text = f"{cur} {done_i}/{total_i}"
                    except Exception:
                        prog_text = "-"
            st.markdown(f'<div class="small-muted">進度</div><div class="kpi">{prog_text}</div>', unsafe_allow_html=True)
        with top_d:
            sc_txt = "-" if best_any_score is None else str(round(float(best_any_score), 6))
            st.markdown(f'<div class="small-muted">最佳分數</div><div class="kpi">{sc_txt}</div>', unsafe_allow_html=True)
        with top_e:
            st.markdown(
                f'<span class="pill pill-{"ok" if bool(best_any_passed) else "neutral"}">達標 {passed_label}</span>',
                unsafe_allow_html=True,
            )

        # Only show speed indicators when in grid_search.
        if str(phase) == "grid_search":
            elapsed_s = prog.get("elapsed_s")
            speed_cps = prog.get("speed_cps")
            eta_s = prog.get("eta_s")
            es = "-" if elapsed_s is None else f"{float(elapsed_s):.2f}s"
            sp = "-" if speed_cps is None else f"{float(speed_cps):.3f} cps"
            et = "-" if eta_s is None else f"{float(eta_s):.1f}s"
            st.markdown(f'<div class="small-muted">耗時 {es} · 速度 {sp} · ETA {et}</div>', unsafe_allow_html=True)

        if last_error:
            st.error(last_error[:220])

        # Progress visualization
        sync = prog.get("sync")
        if combos_total > 0:
            st.progress(min(1.0, float(combos_done) / float(combos_total)))
        elif str(phase) == "sync_data":
            items = sync.get("items") if isinstance(sync, dict) else None
            if isinstance(items, dict) and items:
                order = []
                if "1m" in items:
                    order.append("1m")
                cur = str(sync.get("current") or "") if isinstance(sync, dict) else ""
                if cur and cur in items and cur not in order:
                    order.append(cur)
                for k in sorted(items.keys()):
                    if k not in order:
                        order.append(k)
                for k in order:
                    try:
                        d = int(items[k].get("done") or 0)
                        tot = int(items[k].get("total") or 0)
                    except Exception:
                        d, tot = 0, 0
                    if tot > 0:
                        st.markdown(f'<div class="small-muted">資料同步 {k}：{d}/{tot}</div>', unsafe_allow_html=True)
                        st.progress(min(1.0, float(d) / float(tot)))
            elif isinstance(phase_progress, (int, float)):
                st.progress(float(phase_progress))

        if phase_msg and (str(phase) != "sync_data" or not (isinstance(sync, dict) and isinstance(sync.get("items"), dict) and sync.get("items"))):
            st.caption(phase_msg)

        grid_a, grid_b = st.columns([1.2, 1.0])
        with grid_a:
            rows = []
            rows.append({"指標": "交易筆數", "目前": "-" if trades is None else int(trades), "門檻": int(min_trades), "差距": _fmt_gap_min(float(trades) if trades is not None else None, float(min_trades))})
            rows.append({"指標": "總報酬", "目前": "-" if ret_pct is None else round(float(ret_pct), 4), "門檻": float(min_total_return_pct), "差距": _fmt_gap_min(ret_pct, float(min_total_return_pct))})
            rows.append({"指標": "最大回撤", "目前": "-" if dd_pct is None else round(float(dd_pct), 4), "門檻": float(max_drawdown_pct), "差距": _fmt_gap_max(dd_pct, float(max_drawdown_pct))})
            rows.append({"指標": "Sharpe", "目前": "-" if sharpe is None else round(float(sharpe), 4), "門檻": float(min_sharpe), "差距": _fmt_gap_min(sharpe, float(min_sharpe))})
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

    conn = db._conn()
    try:
        payout_currency = str(db.get_setting(conn, "payout_currency", "USDT") or "USDT").strip()
        withdraw_min = float(db.get_setting(conn, "withdraw_min_usdt", 20.0) or 20.0)
        withdraw_fee_usdt = float(db.get_setting(conn, "withdraw_fee_usdt", 1.0) or 1.0)
        withdraw_fee_mode = str(db.get_setting(conn, "withdraw_fee_mode", "platform_absorb") or "platform_absorb").strip()
    finally:
        conn.close()

    fee_mode_label = "平台吸收" if withdraw_fee_mode == "platform_absorb" else "用戶內扣"

    chain, wallet = db.get_wallet_info(int(user["id"]))
    chain = (chain or "TRC20").strip().upper()

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.write("提現規則")
    st.write(
        "最低提現門檻", f"{withdraw_min:g} {payout_currency}"
    )
    st.write(
        "鏈上手續費", f"約 {withdraw_fee_usdt:g} {payout_currency}（{fee_mode_label}）"
    )
    st.write("錢包設定")
    chain_opts = ["TRC20", "BEP20"]
    chain_index = chain_opts.index(chain) if chain in chain_opts else 0
    new_chain = st.selectbox("提現鏈", options=chain_opts, index=chain_index, key="wallet_chain_update")
    new_wallet = st.text_input("錢包地址", value=str(wallet or ""), key="wallet_update")
    if st.button("保存", key="wallet_save"):
        ok, msg = validate_wallet_address(new_wallet, chain=new_chain)
        if not ok:
            st.error(msg)
        else:
            db.set_wallet_address(int(user["id"]), new_wallet.strip(), wallet_chain=new_chain)
            db.write_audit_log(int(user["id"]), "wallet_update", {"wallet": "updated", "chain": new_chain})
            st.success("已保存。")
            st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

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

        st.divider()

        # 教學影片：上傳 MP4 後會顯示在登入頁的「流程與操作要點」中。
        st.markdown("#### 教學影片")

        conn_v = db._conn()
        try:
            tutorial_path = str(db.get_setting(conn_v, "tutorial_video_path", "") or "").strip()
        finally:
            conn_v.close()

        uploaded_video = st.file_uploader("上傳 MP4", type=["mp4"], accept_multiple_files=False, key="admin_tutorial_mp4")
        if uploaded_video is not None:
            base_dir = os.path.join(os.path.dirname(__file__), "data")
            os.makedirs(base_dir, exist_ok=True)
            save_path = os.path.join(base_dir, "tutorial.mp4")
            with open(save_path, "wb") as f:
                f.write(uploaded_video.getbuffer())

            conn_w = db._conn()
            try:
                db.set_setting(conn_w, "tutorial_video_path", save_path)
                conn_w.commit()
            finally:
                conn_w.close()

            db.write_audit_log(int(user["id"]), "tutorial_video_update", {"path": save_path})
            st.success("已更新教學影片")
            st.rerun()

        if tutorial_path and os.path.exists(tutorial_path):
            try:
                st.video(tutorial_path)
            except Exception:
                st.markdown('<div class="small-muted">教學影片載入失敗。</div>', unsafe_allow_html=True)

            if st.button("移除教學影片", key="remove_tutorial_video"):
                try:
                    os.remove(tutorial_path)
                except Exception:
                    pass

                conn_w = db._conn()
                try:
                    db.set_setting(conn_w, "tutorial_video_path", "")
                    conn_w.commit()
                finally:
                    conn_w.close()

                db.write_audit_log(int(user["id"]), "tutorial_video_remove", {})
                st.rerun()
        else:
            st.markdown('<div class="small-muted">目前未上傳教學影片。</div>', unsafe_allow_html=True)

        st.divider()

        conn = db._conn()
        try:
            st.markdown("#### 分享預覽")
            og_title = st.text_input("OG 標題", value=str(db.get_setting(conn, "og_title", "") or ""), key="og_title")
            og_desc = st.text_area("OG 描述", value=str(db.get_setting(conn, "og_description", "") or ""), height=80, key="og_desc")
            og_image = st.text_input("OG 圖片 URL", value=str(db.get_setting(conn, "og_image_url", "") or ""), key="og_img")
            og_url = st.text_input("OG URL", value=str(db.get_setting(conn, "og_url", "") or ""), key="og_url")
            og_redirect = st.text_input("分享後導向 URL", value=str(db.get_setting(conn, "og_redirect_url", "") or ""), key="og_redirect")
            if st.button("保存分享預覽", key="save_og"):
                db.set_setting(conn, "og_title", og_title)
                db.set_setting(conn, "og_description", og_desc)
                db.set_setting(conn, "og_image_url", og_image)
                db.set_setting(conn, "og_url", og_url)
                db.set_setting(conn, "og_redirect_url", og_redirect)
                conn.commit()
                db.write_audit_log(int(user["id"]), "og_settings_update", {})
                st.success("已保存分享預覽設定")
                st.rerun()

            st.divider()

            st.markdown("#### 提現規則顯示")
            w_min = st.number_input("最低提現金額（USDT）", min_value=0.0, value=float(db.get_setting(conn, "withdraw_min_usdt", 20.0) or 20.0), step=1.0, key="withdraw_min")
            w_fee = st.number_input("預估鏈上手續費（USDT）", min_value=0.0, value=float(db.get_setting(conn, "withdraw_fee_usdt", 1.0) or 1.0), step=0.5, key="withdraw_fee")
            w_mode = st.selectbox("手續費承擔方式", options=["deduct", "platform_absorb"], index=0 if str(db.get_setting(conn, "withdraw_fee_mode", "deduct") or "deduct") == "deduct" else 1, key="withdraw_mode")
            if st.button("保存提現規則", key="save_withdraw"):
                db.set_setting(conn, "withdraw_min_usdt", float(w_min))
                db.set_setting(conn, "withdraw_fee_usdt", float(w_fee))
                db.set_setting(conn, "withdraw_fee_mode", str(w_mode))
                conn.commit()
                db.write_audit_log(int(user["id"]), "withdraw_rule_update", {})
                st.success("已保存提現規則")
                st.rerun()

            st.divider()

            st.markdown("#### 服務條款與風險協議")
            tos_version = st.text_input("條款版本", value=str(db.get_setting(conn, "tos_version", "") or ""), key="tos_version")
            tos_text = st.text_area("條款內容", value=str(db.get_setting(conn, "tos_text", "") or ""), height=240, key="tos_text")
            if st.button("保存條款", key="save_tos"):
                db.set_setting(conn, "tos_version", tos_version)
                db.set_setting(conn, "tos_text", tos_text)
                conn.commit()
                db.write_audit_log(int(user["id"]), "tos_update", {"version": tos_version})
                st.success("已保存條款")
                st.rerun()
        finally:
            conn.close()

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
    st.set_page_config(page_title=APP_TITLE, layout="wide", initial_sidebar_state="expanded")
    _style()
    _bootstrap()

    _apply_cookie_ops()
    _inject_meta(APP_TITLE, "分散算力挖礦與週結算分潤任務平台")

    _try_auto_login_from_cookie()

    user = _session_user()
    job_mgr = JOB_MANAGER

    if not user:
        _page_auth()
        return

    headers = _get_ws_headers()
    ua = str(headers.get("User-Agent") or headers.get("user-agent") or "")
    if _ua_is_mobile(ua):
        st.warning("偵測到行動裝置。挖礦計算量大且背景執行不穩定，建議改用電腦。")

    role = str(user.get("role") or "user")

    pages = ["新手教學", "控制台", "任務", "提交", "結算"] + (["管理"] if role == "admin" else [])

    if "nav_page_pending" in st.session_state:
        try:
            _pending = str(st.session_state.pop("nav_page_pending") or "").strip()
        except Exception:
            _pending = ""
        if _pending and _pending in pages:
            st.session_state["nav_page"] = _pending

    if "nav_page" not in st.session_state:
        st.session_state["nav_page"] = pages[0]

    with st.sidebar:
        st.markdown(f"### {APP_TITLE}")
        st.markdown(f'<div class="small-muted">{user["username"]} · {role}</div>', unsafe_allow_html=True)
        st.divider()

        st.markdown('<div class="small-muted">導航</div>', unsafe_allow_html=True)
        page = st.radio(
            "導航",
            options=pages,
            key="nav_page",
            label_visibility="collapsed",
        )

        st.divider()
        if st.button("登出"):
            _logout()
            st.rerun()

    page = str(page or pages[0])

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
