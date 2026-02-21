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
    orig = getattr(st, "_sheep_orig_dataframe", None)
    if orig is None or getattr(orig, "__name__", "") == "_dataframe_compat":
        # 如果無法取得原方法，強制退回 Streamlit 原始方法
        import streamlit.elements.dataframe_selector as ds
        orig = ds.DataFrameMixin.dataframe if hasattr(ds, "DataFrameMixin") else getattr(st, "dataframe")
        
    if "width" in kwargs and str(kwargs["width"]) == "stretch":
        kwargs.pop("width")
        kwargs["use_container_width"] = True
        
    try:
        return orig(data, **kwargs)
    except TypeError as e:
        if "hide_index" in kwargs:
            kwargs.pop("hide_index")
        try:
            return orig(data, **kwargs)
        except Exception as e2:
            import streamlit as _internal_st
            # 如果 dataframe 完全渲染失敗，退回最原始的靜態 table 保證資料可見
            return _internal_st.table(data)

if getattr(st.dataframe, "__name__", "") != "_dataframe_compat":
    try:
        st._sheep_orig_dataframe = st.dataframe
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

_BRAND_WEBM_1 = os.environ.get("SHEEP_BRAND_WEBM_1", "static/羊LOGO影片(去背).webm")
# 注意：舊版 Streamlit 的 st.components.v1.html 不支援 key=
# 因此不使用 key，並改用更穩的 CSS selector 來固定 iframe


def _abs_asset_path(p: str) -> str:
    p = (p or "").strip()
    if not p:
        return ""
    if os.path.isabs(p):
        return p
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    except Exception:
        base_dir = os.getcwd()
    return os.path.join(base_dir, p)


@st.cache_data(show_spinner=False)
def _read_file_b64(path_str: str) -> str:
    try:
        ap = _abs_asset_path(path_str)
        if not ap:
            return ""
        with open(ap, "rb") as f:
            raw = f.read()
        if not raw:
            return ""
        return base64.b64encode(raw).decode("ascii")
    except Exception:
        return ""


def _render_brand_header(animate: bool, dim: bool = False) -> None:
    v1 = _read_file_b64(_BRAND_WEBM_1)
    dim_css = ""

    st.markdown(
        f"""
<style>
iframe[data-sheep-brand="1"],
iframe[srcdoc*="SHEEP_BRAND_HDR_V3"] {{
  position: fixed !important;
  top: 0 !important;
  left: 0 !important;
  width: 360px !important;
  height: 84px !important;
  border: 0 !important;
  z-index: 2147483000 !important;
  background: transparent !important;
}}

@media (max-width: 720px) {{
  iframe[data-sheep-brand="1"],
  iframe[srcdoc*="SHEEP_BRAND_HDR_V3"] {{
    width: 270px !important;
    height: 78px !important;
  }}
}}

div[data-testid="stAppViewContainer"] > .main {{
  padding-top: 0 !important;
}}

{dim_css}
</style>
""",
        unsafe_allow_html=True,
    )

    data_v1 = f"data:video/webm;base64,{v1}" if v1 else ""

    html_block = f"""
    <!doctype html>
    <html>
    <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <style>
    html, body {{
        height: 100%;
        margin: 0;
        background: transparent;
        overflow: hidden;
        font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Noto Sans", Arial;
    }}

    .brandWrap {{
        position: absolute;
        top: 6px;
        left: 6px;
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 6px 16px 6px 6px;
        border-radius: 9999px;
        background: rgba(10, 14, 23, 0.85);
        border: 1px solid rgba(255, 255, 255, 0.08);
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.6);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        transition: background 0.3s ease, border-color 0.3s ease;
    }}

    .logoContainer {{
        width: 72px;
        height: 72px;
        border-radius: 9999px;
        background: transparent;
        overflow: hidden;
        position: relative;
        flex: 0 0 auto;
        display: flex;
        align-items: center;
        justify-content: center;
    }}

    video {{
        position: absolute;
        width: 100%;
        height: 100%;
        object-fit: cover;
        transform: scale(1.4);
        display: block;
        background: transparent;
    }}

    .fallback {{
        position: absolute;
        inset: 0;
        display: none;
        align-items: center;
        justify-content: center;
        color: rgba(255,255,255,0.62);
        font-weight: 800;
        letter-spacing: 0.6px;
        font-size: 14px;
    }}

    .name {{
        font-size: 21px;
        font-weight: 900;
        letter-spacing: 0.5px;
        line-height: 1;
        color: #f8fbff;
        text-shadow: 0 4px 12px rgba(0,0,0,0.8);
        user-select: none;
        white-space: nowrap;
        transition: filter 0.3s ease;
    }}
    
    .name .souper {{
        font-weight: 850;
        background: linear-gradient(135deg, #ffffff 0%, #a0b4ce 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }}
    
    .name .sheep {{
        font-weight: 950;
        background: linear-gradient(135deg, #78b4ff 0%, #50f0dc 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }}

    .brandWrap:hover {{
        background: rgba(15, 21, 32, 0.95);
        border-color: rgba(120, 180, 255, 0.3);
    }}
    
    .brandWrap:hover .name {{
        filter: brightness(1.15);
    }}

    .brandWrap.pulse {{
        animation: ringPulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite;
    }}
    
    @keyframes ringPulse {{
        0%   {{ box-shadow: 0 8px 32px rgba(0,0,0,0.6); border-color: rgba(255,255,255,0.08); }}
        50%  {{ box-shadow: 0 12px 48px rgba(0,0,0,0.8), 0 0 20px rgba(120,180,255,0.15); border-color: rgba(120,180,255,0.4); }}
        100% {{ box-shadow: 0 8px 32px rgba(0,0,0,0.6); border-color: rgba(255,255,255,0.08); }}
    }}

    @media (max-width: 720px) {{
        .brandWrap {{ top: 6px; left: 6px; gap: 8px; padding: 4px 12px 4px 4px; }}
        .logoContainer {{ width: 62px; height: 62px; }}
        .name {{ font-size: 18px; }}
    }}

    @media (prefers-reduced-motion: reduce) {{
        .brandWrap.pulse {{ animation: none; }}
    }}
    </style>
    </head>
    <body>
    <div class="brandWrap" id="brand">
        <div class="logoContainer" aria-hidden="true">
        <video id="v1" muted playsinline loop preload="auto"></video>
        <div id="fb" class="fallback">SS</div>
        </div>
        <div class="name" aria-label="SouperSheep">
        <span class="souper">Souper</span><span class="sheep">Sheep</span>
        </div>
    </div>

    <script>
    (function() {{
    try {{
        if (window.frameElement) {{
        window.frameElement.setAttribute("data-sheep-brand", "1");
        }}
    }} catch (e) {{}}

    const brand = document.getElementById("brand");
    const v1 = document.getElementById("v1");
    const fb = document.getElementById("fb");

    const hasV1 = { "true" if v1 else "false" };

    function showFallback() {{
        fb.style.display = "flex";
        try {{ v1.style.display = "none"; }} catch(e) {{}}
    }}

    function playSafe(v) {{
        try {{
        const p = v.play();
        if (p && typeof p.catch === "function") p.catch(() => {{}});
        }} catch(e) {{}}
    }}

    function setRate(r) {{
        try {{ v1.playbackRate = r; }} catch(e) {{}}
    }}

    brand.addEventListener("mouseenter", () => setRate(1.15));
    brand.addEventListener("mouseleave", () => setRate(1.00));

    window.addEventListener("message", (ev) => {{
        try {{
        const d = ev.data || {{}};
        if (!d || d.type !== "SHEEP_HDR_PULSE") return;
        if (d.on) brand.classList.add("pulse");
        else brand.classList.remove("pulse");
        }} catch(e) {{}}
    }});

    if (!hasV1) {{
        showFallback();
        return;
    }}

    v1.src = "{data_v1}";
    setRate(1.00);
    playSafe(v1);
    }})();
    </script>
    </body>
    </html>
    """
    st.components.v1.html(html_block, height=90, scrolling=False)
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
          --bg: #05070a;
          --card: rgba(20, 24, 32, 0.6);
          --card-hover: rgba(28, 34, 46, 0.8);
          --border: rgba(255, 255, 255, 0.08);
          --border-hover: rgba(120, 180, 255, 0.3);
          --text: #e2e8f0;
          --muted: #8492a6;
          --accent: #3b82f6;
          --accent-glow: rgba(59, 130, 246, 0.4);
          --shadow: 0 8px 32px rgba(0, 0, 0, 0.5);
        }

        .stApp {
          background: radial-gradient(circle at 15% 0%, rgba(30, 58, 138, 0.15) 0%, transparent 40%),
                      radial-gradient(circle at 85% 100%, rgba(15, 118, 110, 0.1) 0%, transparent 40%),
                      var(--bg);
          color: var(--text);
        }

        html, body, [class*="css"]  {
          font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Noto Sans", "Helvetica Neue", Arial, sans-serif;
        }

        html { color-scheme: dark; }

        div[data-testid="stTextInput"] input,
        div[data-testid="stTextInput"] textarea,
        div[data-testid="stNumberInput"] input,
        div[data-testid="stTextArea"] textarea,
        div[data-testid="stPassword"] input {
          background: rgba(0, 0, 0, 0.2) !important;
          border: 1px solid var(--border) !important;
          border-radius: 8px !important;
          color: var(--text) !important;
          transition: all 0.2s ease;
        }
        div[data-testid="stTextInput"] input:focus,
        div[data-testid="stTextArea"] textarea:focus,
        div[data-testid="stPassword"] input:focus {
          border-color: var(--accent) !important;
          box-shadow: 0 0 0 1px var(--accent-glow) !important;
        }
        div[data-testid="stTextInput"] input::placeholder,
        div[data-testid="stTextArea"] textarea::placeholder,
        div[data-testid="stPassword"] input::placeholder {
          color: var(--muted) !important;
        }

        div[data-testid="stSidebar"] {
          background: rgba(10, 14, 20, 0.95);
          border-right: 1px solid var(--border);
          backdrop-filter: blur(10px);
          -webkit-backdrop-filter: blur(10px);
        }

        .block-container {
          padding-top: 2rem;
          padding-bottom: 4rem;
          max-width: 1280px;
        }

        .card {
          background: var(--card);
          border: 1px solid var(--border);
          border-radius: 12px;
          padding: 20px;
          box-shadow: var(--shadow);
          backdrop-filter: blur(8px);
          transition: all 0.3s ease;
        }
        .card:hover {
          background: var(--card-hover);
          border-color: var(--border-hover);
          transform: translateY(-2px);
        }

        .metric-row {
          display: flex;
          gap: 16px;
          flex-wrap: wrap;
          margin-bottom: 24px;
        }
        .metric {
          padding: 16px;
          border-radius: 12px;
          border: 1px solid var(--border);
          background: var(--card);
          min-width: 180px;
          flex: 1;
          display: flex;
          flex-direction: column;
          gap: 4px;
          box-shadow: 0 4px 12px rgba(0,0,0,0.2);
        }
        .metric .k { color: var(--muted); font-size: 13px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }
        .metric .v { color: #ffffff; font-size: 28px; font-weight: 700; line-height: 1.2; }

        .small-muted { color: var(--muted); font-size: 13px; }

        .auth_title {
          font-size: 24px;
          font-weight: 800;
          letter-spacing: 0.5px;
          margin: 0 0 16px 0;
          color: #ffffff;
        }

        header[data-testid="stHeader"] { background: transparent; }
        footer { visibility: hidden; }
        #MainMenu { visibility: hidden; }

        div[data-testid="stToolbar"] { opacity: 0.1; transition: opacity 0.3s ease; }
        div[data-testid="stToolbar"]:hover { opacity: 1; }
        div[data-testid="stStatusWidget"] { display: none !important; }
        div[data-testid="stDecoration"] { display: none !important; }

        /* [頂級專家防擋修復] 側邊欄收起/展開按鈕 UI/UX 徹底重構 */
        /* 解除側邊欄的溢出隱藏限制，讓按鈕可以凸出去而不被切斷 */
        section[data-testid="stSidebar"] {
            overflow: visible !important;
        }
        section[data-testid="stSidebar"] > div:first-child {
            overflow: visible !important;
        }
        
        /* 1. 展開按鈕 (側邊欄收起時)：固定在畫面最左側中央，明顯的藍色懸浮拉環。升級支援新版 Streamlit 選擇器 */
        [data-testid="collapsedControl"], [data-testid="stSidebarCollapsedControl"] {
            display: flex !important;
            opacity: 1 !important;
            visibility: visible !important;
            position: fixed !important;
            top: 50% !important;
            left: 0 !important;
            transform: translateY(-50%) !important;
            z-index: 2147483647 !important;
            background: rgba(37, 99, 235, 0.95) !important;
            border: 2px solid rgba(96, 165, 250, 0.8) !important;
            border-left: none !important;
            border-radius: 0 12px 12px 0 !important;
            width: 40px !important;
            height: 70px !important;
            align-items: center !important;
            justify-content: center !important;
            box-shadow: 4px 0 20px rgba(0,0,0,0.8), 0 0 15px rgba(59,130,246,0.6) !important;
            transition: all 0.2s ease !important;
        }
        [data-testid="collapsedControl"]:hover, [data-testid="stSidebarCollapsedControl"]:hover {
            background: rgba(29, 78, 216, 1) !important;
            width: 48px !important;
            border-color: #ffffff !important;
            box-shadow: 4px 0 25px rgba(0,0,0,0.9), 0 0 20px rgba(96,165,250,0.8) !important;
            cursor: pointer !important;
        }
        [data-testid="collapsedControl"] svg, [data-testid="stSidebarCollapsedControl"] svg {
            fill: #ffffff !important;
            width: 24px !important;
            height: 24px !important;
        }
        /* 確保側邊欄按鈕不會被頂部導航組件遮擋而無法點擊 */
        [data-testid="stHeader"] {
            z-index: 100 !important;
            pointer-events: none !important;
        }

        /* 2. 收起按鈕 (側邊欄打開時)：釘在側邊欄的最右側邊緣中間（一半在內一半在外） */
        [data-testid="stSidebarCollapseButton"] {
            display: flex !important;
            opacity: 1 !important;
            visibility: visible !important;
            position: absolute !important;
            top: 50% !important;
            right: -20px !important; 
            transform: translateY(-50%) !important;
            z-index: 2147483647 !important;
            background: rgba(15, 23, 42, 1) !important;
            border: 2px solid rgba(120, 180, 255, 0.8) !important;
            border-radius: 50% !important;
            width: 40px !important;
            height: 40px !important;
            align-items: center !important;
            justify-content: center !important;
            box-shadow: 0 0 16px rgba(0,0,0,0.8), 0 0 12px rgba(120,180,255,0.4) !important;
            transition: all 0.2s ease !important;
        }
        [data-testid="stSidebarCollapseButton"]:hover {
            background: rgba(30, 58, 138, 1) !important;
            border-color: #ffffff !important;
            transform: translateY(-50%) scale(1.15) !important;
            box-shadow: 0 0 20px rgba(59,130,246,0.8) !important;
            cursor: pointer !important;
        }
        [data-testid="stSidebarCollapseButton"] svg {
            fill: #ffffff !important;
            width: 20px !important;
            height: 20px !important;
        }

        .stButton > button[kind="primary"], .stDownloadButton > button[kind="primary"] {
          background: linear-gradient(to right, #2563eb, #3b82f6) !important;
          border: none !important;
          color: #ffffff !important;
          font-weight: 600 !important;
          letter-spacing: 0.5px !important;
        }
        .stButton > button[kind="primary"]:hover, .stDownloadButton > button[kind="primary"]:hover {
          background: linear-gradient(to right, #1d4ed8, #2563eb) !important;
          box-shadow: 0 4px 12px var(--accent-glow) !important;
        }

        .stButton > button[kind="secondary"], .stDownloadButton > button[kind="secondary"] {
          background: rgba(255, 255, 255, 0.05) !important;
          border: 1px solid var(--border) !important;
          color: #e2e8f0 !important;
        }
        .stButton > button[kind="secondary"]:hover, .stDownloadButton > button[kind="secondary"]:hover {
          background: rgba(255, 255, 255, 0.1) !important;
          border-color: rgba(255, 255, 255, 0.2) !important;
        }

        .stButton > button, .stDownloadButton > button {
          width: 100%;
          border-radius: 8px;
          padding: 8px 16px;
          transition: all 0.2s ease;
        }

        div[data-baseweb="input"] input,
        div[data-baseweb="textarea"] textarea,
        div[data-baseweb="select"] > div {
          background: rgba(0, 0, 0, 0.2) !important;
          border: 1px solid var(--border) !important;
          border-radius: 8px !important;
          color: var(--text) !important;
        }

        div[data-testid="stSidebar"] label[data-baseweb="radio"] > div > div:first-child { display: none !important; }
        div[data-testid="stSidebar"] input[type="radio"] { opacity: 0 !important; width: 0 !important; height: 0 !important; }
        div[data-testid="stSidebar"] label[data-baseweb="radio"] svg { display: none !important; }
        
        div[data-testid="stSidebar"] label[data-baseweb="radio"] {
          width: 100%;
          border: 1px solid transparent;
          border-radius: 8px;
          padding: 10px 16px;
          margin: 4px 0;
          background: transparent;
          color: var(--muted);
          transition: all 0.2s ease;
        }
        div[data-testid="stSidebar"] label[data-baseweb="radio"]:hover {
          background: rgba(255,255,255,0.03);
          color: var(--text);
        }
        div[data-testid="stSidebar"] label[data-baseweb="radio"]:has(input:checked) {
          background: rgba(59, 130, 246, 0.1);
          border-color: rgba(59, 130, 246, 0.3);
          color: #ffffff;
          font-weight: 600;
        }

        .pm_legend { display: flex; gap: 12px; flex-wrap: wrap; margin: 8px 0 16px 0; }
        .pm_key { display: inline-flex; align-items: center; gap: 6px; font-size: 13px; color: var(--muted); }
        
        .pm_grid { display: grid; gap: 4px; padding: 8px 0; }
        .pm_cell {
          width: 12px; height: 12px; border-radius: 2px;
          border: 1px solid rgba(255,255,255,0.05);
          background: rgba(255,255,255,0.05);
          transition: transform 0.2s ease;
        }
        .pm_cell:hover { transform: scale(1.2); z-index: 2; border-color: rgba(255,255,255,0.5); }
        .pm_cell.pm_done { background: #3b82f6; border-color: #2563eb; }
        .pm_cell.pm_running { background: #10b981; border-color: #059669; }
        .pm_cell.pm_reserved { background: #f59e0b; border-color: #d97706; }
        .pm_cell.pm_available { background: rgba(255,255,255,0.1); }

        div[data-testid="stSidebarContent"] { padding-bottom: 220px !important; }

        .help_wrap { display: inline-flex; position: relative; align-items: center; margin-left: 8px; z-index: 50; }
        .help_icon {
          width: 20px; height: 20px; border-radius: 50%;
          display: inline-flex; align-items: center; justify-content: center;
          font-size: 12px; font-weight: 700;
          border: 1px solid var(--border);
          background: rgba(255,255,255,0.05); color: var(--muted);
          cursor: help; transition: all 0.2s ease;
        }
        .help_wrap:hover .help_icon { border-color: var(--accent); background: var(--accent-glow); color: #ffffff; }
        .help_tip {
          position: absolute; bottom: calc(100% + 8px); left: 50%; transform: translateX(-50%) translateY(4px);
          width: max-content; max-width: 320px; padding: 12px 16px;
          border-radius: 8px; background: #1e293b;
          border: 1px solid #334155; box-shadow: 0 16px 40px rgba(0, 0, 0, 0.6);
          color: #f8fafc; font-size: 13px; line-height: 1.5;
          opacity: 0; pointer-events: none; transition: all 0.2s ease; backdrop-filter: blur(8px);
          z-index: 999999;
          white-space: normal;
        }
        .help_wrap:hover .help_tip { opacity: 1; transform: translateX(-50%) translateY(0); }
        
        /* [專家修復] 針對左下角 User HUD 內的提示框，強制向右展開，避免被螢幕左側切斷 */
        .user_hud .help_tip {
          left: 0;
          transform: translateY(4px);
        }
        .user_hud .help_wrap:hover .help_tip {
          transform: translateY(0);
        }

        .sec_h3 { font-size: 24px; font-weight: 800; color: #ffffff; margin: 32px 0 16px 0; display: flex; align-items: center; }
        .sec_h4 { font-size: 18px; font-weight: 700; color: #e2e8f0; margin: 24px 0 12px 0; display: flex; align-items: center; }

        .panel {
          border-radius: 12px; border: 1px solid var(--border);
          background: var(--card); padding: 24px;
          margin: 16px 0; box-shadow: var(--shadow);
        }

        .user_hud {
          position: fixed; left: 16px; bottom: 16px; width: 268px;
          padding: 16px; border-radius: 12px;
          border: 1px solid var(--border);
          background: rgba(15, 23, 42, 0.85);
          box-shadow: 0 8px 32px rgba(0,0,0,0.5);
          backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);
          z-index: 100000;
        }
        .user_hud .hud_name { font-size: 15px; font-weight: 700; color: #ffffff; margin-bottom: 12px; }
        .user_hud .hud_row { display: flex; justify-content: space-between; align-items: center; margin-top: 8px; }
        .user_hud .hud_k { font-size: 13px; color: var(--muted); }
        .user_hud .hud_v { font-size: 15px; font-weight: 700; color: #ffffff; }
        .user_hud .hud_div { height: 1px; background: var(--border); margin: 12px 0; }
        
        .pill {
            display: inline-block; padding: 4px 10px; border-radius: 6px;
            font-size: 12px; font-weight: 600; text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        .pill-ok { background: rgba(16, 185, 129, 0.15); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.3); }
        .pill-info { background: rgba(59, 130, 246, 0.15); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.3); }
        .pill-warn { background: rgba(245, 158, 11, 0.15); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.3); }
        .pill-bad { background: rgba(239, 68, 68, 0.15); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.3); }
        .pill-neutral { background: rgba(255, 255, 255, 0.1); color: #94a3b8; border: 1px solid rgba(255, 255, 255, 0.2); }
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
    if hasattr(db, "init_db"):
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
                pw_hash = hash_password(admin_password)
                pw_hash_str = pw_hash.decode('utf-8') if isinstance(pw_hash, bytes) else str(pw_hash)
                conn.execute(
                    "UPDATE users SET password_hash = ?, role = 'admin', disabled = 0 WHERE id = ?",
                    (pw_hash_str, int(row["id"])),
                )
                conn.commit()
            finally:
                conn.close()
        else:
            pw_hash = hash_password(admin_password)
            pw_hash_str = pw_hash.decode('utf-8') if isinstance(pw_hash, bytes) else str(pw_hash)
            db.create_user(username=admin_username, password_hash=pw_hash_str, role="admin", wallet_address="", wallet_chain="TRC20")
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
    st.markdown('<div class="auth_title">登入</div>', unsafe_allow_html=True)

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

    if db.is_user_locked(int(user["id"])):
        st.error("登入已鎖定。")
        return

    # [專家除錯] 強化密碼驗證邏輯，完美處理資料庫儲存為 bytes/str 以及字串化 bytes (如 "b'...'") 的潛在錯誤
    is_valid = False
    hash_stored = user.get("password_hash", "")
    
    if isinstance(hash_stored, str):
        if hash_stored.startswith("b'") and hash_stored.endswith("'"):
            hash_stored = hash_stored[2:-1]
        elif hash_stored.startswith('b"') and hash_stored.endswith('"'):
            hash_stored = hash_stored[2:-1]
            
    try:
        is_valid = verify_password(password, hash_stored)
    except TypeError:
        pw_bytes = password.encode("utf-8") if isinstance(password, str) else password
        hash_bytes = hash_stored.encode("utf-8") if isinstance(hash_stored, str) else hash_stored
        try:
            is_valid = verify_password(pw_bytes, hash_bytes)
        except Exception:
            is_valid = False
    except Exception:
        is_valid = False

    if not is_valid:
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
    st.markdown('<div class="auth_title">註冊</div>', unsafe_allow_html=True)

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

    with st.form("register_form", clear_on_submit=False):
        username = st.text_input("帳號", value="")
        password = st.text_input("密碼", value="", type="password", placeholder="至少 6 碼，需包含英文字母與數字")
        password2 = st.text_input("確認密碼", value="", type="password")

        tos_ok = st.checkbox("我已閱讀並同意平台服務條款與分潤規則", value=False, key="register_tos_ok")
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
        st.error("必須同意服務條款與分潤規則才可註冊。")
        return

    if db.get_user_by_username(uname):
        st.error("帳號已存在。")
        return

    try:
        try:
            pw_hashed = hash_password(pw)
        except TypeError:
            pw_hashed = hash_password(pw.encode("utf-8"))

        uid = db.create_user(username=uname, password_hash=pw_hashed, role="user", wallet_address="", wallet_chain="TRC20")
        db.write_audit_log(uid, "register", {"username": uname})
        if tos_version.strip():
            db.write_audit_log(uid, "tos_accept", {"version": tos_version})
        else:
            db.write_audit_log(uid, "tos_accept", {})
    except Exception as e:
        st.error(f"建立失敗：{e}")
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


def _render_tos_dialog() -> None:
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

    @st.dialog("服務條款與分潤規則")
    def _run() -> None:
        if tos_version.strip():
            st.markdown(f"**條款版本：{tos_version.strip()}**")
        if tos_text.strip():
            st.markdown(tos_text)
        else:
            st.markdown("服務條款暫未設定。")

        if st.button("關閉", key="close_tos_dialog"):
            st.session_state["auth_dialog"] = ""
            st.rerun()

    _run()


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
        tab_names = ["總覽", "合作模式", "分潤"]
        if has_video:
            tab_names.append("影片")
        tabs = st.tabs(tab_names)

        with tabs[0]:
            st.markdown("#### 用你們的裝置算出最佳交易策略 賺取獎勵分潤")
            st.write("平台提供所有策略與參數，讓用戶提供算力自行組合並挖出最佳結果。若該策略獲利達標將獲得平台分潤獎勵。")
            st.markdown("#### 流程")
            st.components.v1.html(
                """
            <div class="sp-flow-wrap">
            <div class="sp-flow-head">
                <div class="sp-flow-title">步驟總覽</div>
                <div class="sp-flow-hint">滑鼠移動或點擊步驟可查看詳細說明</div>
            </div>

            <div class="sp-flow-track" id="spFlowTrack">
                <button class="sp-step" data-step="login" type="button" onclick="spFlowPick('login')" onmouseover="spFlowPick('login')" ontouchstart="spFlowPick('login')">註冊 / 登入</button>
                <button class="sp-step" data-step="start" type="button" onclick="spFlowPick('start')" onmouseover="spFlowPick('start')" ontouchstart="spFlowPick('start')">開始任務</button>
                <button class="sp-step" data-step="cand" type="button" onclick="spFlowPick('cand')" onmouseover="spFlowPick('cand')" ontouchstart="spFlowPick('cand')">候選結果</button>
                <button class="sp-step" data-step="verify" type="button" onclick="spFlowPick('verify')" onmouseover="spFlowPick('verify')" ontouchstart="spFlowPick('verify')">伺服器複驗</button>
                <button class="sp-step" data-step="submit" type="button" onclick="spFlowPick('submit')" onmouseover="spFlowPick('submit')" ontouchstart="spFlowPick('submit')">提交策略池</button>
                <button class="sp-step" data-step="settle" type="button" onclick="spFlowPick('settle')" onmouseover="spFlowPick('settle')" ontouchstart="spFlowPick('settle')">週期結算</button>

            </div>

            <div class="sp-flow-detail" id="spFlowDetail">
                <div class="sp-flow-detail-title" id="spFlowDetailTitle">註冊 / 登入</div>
                <div class="sp-flow-detail-body" id="spFlowDetailBody">
                建立帳號或登入後，即可領取任務並開始提供算力。註冊時需同意服務條款與分潤規則。
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
                max-height:180px;
                overflow-y:auto;
                padding-right:4px;
            }

            .sp-flow-detail-body::-webkit-scrollbar{width:10px}
            .sp-flow-detail-body::-webkit-scrollbar-thumb{background:rgba(255,255,255,0.12);border-radius:999px}
            .sp-flow-detail-body::-webkit-scrollbar-track{background:rgba(255,255,255,0.04);border-radius:999px}

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
                    flex-wrap:nowrap;
                    overflow-x:auto;
                    padding:8px 6px 10px 6px;
                }
                .sp-step{
                    width:auto;
                    border-radius:999px;
                    text-align:center;
                    padding:10px 12px;
                    font-size:12.5px;
                }
            }
            </style>

            <script>
            (function(){
            const steps = {
                login: {
                title: "註冊 / 登入",
                body: "建立帳號或登入後，即可領取任務並開始提供算力。註冊時需同意服務條款與分潤規則。",
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

            window.spFlowPick = function(stepKey){
                try{ setActive(stepKey); }catch(e){}
            };

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
                height=560,
                scrolling=True,
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
            st.session_state["auth_dialog"] = ""
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
    if "auth_dialog" not in st.session_state:
        st.session_state["auth_dialog"] = ""
    if "brand_enter_played" not in st.session_state:
        st.session_state["brand_enter_played"] = False

    if not bool(st.session_state.get("auth_onboarding_seen")):
        st.session_state["auth_onboarding_seen"] = True
        st.session_state["auth_dialog"] = "onboarding"

    dlg_name = str(st.session_state.get("auth_dialog") or "").strip()
    is_dialog_open = bool(dlg_name)

    animate_brand = (not is_dialog_open) and (not bool(st.session_state.get("brand_enter_played")))
    if animate_brand:
        st.session_state["brand_enter_played"] = True

    st.markdown(
        """
<style>
.auth_scope div[data-testid="stForm"]{
  border-radius: 18px;
  border: 1px solid rgba(255,255,255,0.14);
  background: rgba(255,255,255,0.05);
  padding: 14px 14px 10px 14px;
  transition: transform 160ms ease, box-shadow 220ms ease, border-color 220ms ease, background 220ms ease;
}
.auth_scope div[data-testid="stForm"]:hover{
  transform: translateY(-2px);
  border-color: rgba(120,180,255,0.22);
  box-shadow: 0 18px 46px rgba(0,0,0,0.35);
}
.auth_scope div[data-baseweb="input"] input:focus,
.auth_scope div[data-baseweb="textarea"] textarea:focus{
  border-color: rgba(120,180,255,0.62) !important;
  box-shadow: 0 0 0 2px rgba(120,180,255,0.22) !important;
}
.auth_scope .stButton > button:hover{
  transform: translateY(-1px);
}
</style>
""",
        unsafe_allow_html=True,
    )

    st.markdown('<div class="auth_scope">', unsafe_allow_html=True)

    top_l, top_r = st.columns([1.0, 0.55])
    with top_l:
        st.markdown('<div class="small-muted" style="margin-top: 10px;">登入或註冊後即可開始參與運算任務。</div>', unsafe_allow_html=True)
    with top_r:
        b1, b2 = st.columns([1.1, 1])
        with b1:
            if st.button("流程與操作要點", key="auth_open_onboarding", type="primary", use_container_width=True):
                st.session_state["auth_dialog"] = "onboarding"
                st.rerun()
        with b2:
            if st.button("服務條款", key="auth_open_tos", type="secondary", use_container_width=True):
                st.session_state["auth_dialog"] = "tos"
                st.rerun()

    col1, col2 = st.columns([1, 1])
    with col1:
        _register_form()
    with col2:
        _login_form()

    st.components.v1.html(
        """
<!doctype html>
<html>
<head><meta charset="utf-8" /></head>
<body>
<script>
(function() {
  function findBrandIframe() {
    const d = window.parent.document;
    return d.querySelector('iframe[data-sheep-brand="1"]') || d.querySelector('iframe[data-sheep-brand="1"]') || d.querySelector('iframe[srcdoc*="SHEEP_BRAND_HDR_V3"]');
  }

  function postPulse(on) {
    const f = findBrandIframe();
    if (!f) return;
    try { f.contentWindow.postMessage({ type: "SHEEP_HDR_PULSE", on: !!on }, "*"); } catch (e) {}
  }

  function bind() {
    const d = window.parent.document;
    const cards = Array.from(d.querySelectorAll(".auth_scope .card"));
    if (!cards.length) return;

    let hoverCount = 0;
    function onEnter(){ hoverCount += 1; postPulse(true); }
    function onLeave(){ hoverCount = Math.max(0, hoverCount - 1); if (hoverCount === 0) postPulse(false); }

    for (const c of cards) {
      c.addEventListener("mouseenter", onEnter, { passive: true });
      c.addEventListener("mouseleave", onLeave, { passive: true });
    }
  }

  setTimeout(bind, 420);
})();
</script>
</body>
</html>
""",
        height=0,
        scrolling=False,
    )

    st.markdown("</div>", unsafe_allow_html=True)

    if dlg_name == "onboarding":
        _render_auth_onboarding_dialog()
    elif dlg_name == "tos":
        _render_tos_dialog()

def _help_icon_html(text: str) -> str:
    t = str(text or "").strip()
    if not t:
        return ""
    tip = html.escape(t, quote=True).replace("\n", "<br>")
    return (
        '<span class="help_wrap">'
        '<span class="help_icon" aria-hidden="true">?</span>'
        f'<span class="help_tip">{tip}</span>'
        "</span>"
    )


def _section_title_html(title: str, help_text: str = "", level: int = 3) -> str:
    cls = "sec_h3" if int(level) == 3 else "sec_h4"
    return f'<div class="{cls}">{html.escape(str(title))}{_help_icon_html(help_text)}</div>'


def _render_kpi(title: str, value: Any, sub: str = "", help_text: str = "") -> str:
    v = value if value is not None else "-"
    k = f"{html.escape(str(title))}{_help_icon_html(help_text)}"
    sub_html = html.escape(str(sub)) if sub else ""
    return f'<div class="metric"><div class="k">{k}</div><div class="v">{v}</div><div class="small-muted">{sub_html}</div></div>'


@st.cache_data(ttl=10)
def _cached_global_progress_snapshot(cycle_id: int) -> Dict[str, Any]:
    return db.get_global_progress_snapshot(int(cycle_id))


@st.cache_data(ttl=20)
def _cached_global_paid_payout_sum(cycle_id: int) -> float:
    return float(db.get_global_paid_payout_sum_usdt(int(cycle_id)))


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
    except Exception as e:
        st.warning(f"全域進度暫時無法讀取：{e}")
        return

    system_uid = int(snap.get("system_user_id") or 0)
    pools_all = list(snap.get("pools") or [])
    if not pools_all:
        st.markdown('<div class="small-muted">目前沒有可用的進度資料。</div>', unsafe_allow_html=True)
        return

    # [專家級修復] 精準動態計算策略池的真實組合總數，支援使用者自訂的極端 JSON 列表 (徹底解決 5120 問題)
    def _calc_pool_est(p_data: dict) -> float:
        try:
            fam = str(p_data.get("family", ""))
            g_str = p_data.get("grid_spec_json") or "{}"
            r_str = p_data.get("risk_spec_json") or "{}"
            grid_s = json.loads(g_str) if isinstance(g_str, str) else g_str
            risk_s = json.loads(r_str) if isinstance(r_str, str) else r_str
            
            # 若使用者傳入的是明確的 JSON 列表 (List)，直接取長度；否則走 UI 範圍演算
            if isinstance(grid_s, list):
                g_size = max(1, len(grid_s))
            else:
                gc = bt.grid_combinations_from_ui(fam, grid_s)
                g_size = max(1, len(gc))
            
            # 計算 Risk 組合大小 (同步支援使用者傳入自訂風控列表)
            if fam in ["TEMA_RSI", "LaguerreRSI_TEMA"]:
                mh_min = int(risk_s.get("max_hold_min", 4))
                mh_max = int(risk_s.get("max_hold_max", 80))
                mh_step = max(1, int(risk_s.get("max_hold_step", 4)))
                r_size = max(1, len(list(range(mh_min, mh_max + 1, mh_step))))
            else:
                if isinstance(risk_s, dict) and "tp_list" in risk_s and "sl_list" in risk_s and "max_hold_list" in risk_s:
                    r_size = max(1, len(risk_s["tp_list"])) * max(1, len(risk_s["sl_list"])) * max(1, len(risk_s["max_hold_list"]))
                else:
                    def fr(a, b, step): return max(1, int(round((float(b) - float(a)) / float(step))) + 1) if float(step) > 0 else 1
                    t_s = fr(risk_s.get("tp_min",0.3), risk_s.get("tp_max",1.2), risk_s.get("tp_step",0.1))
                    s_s = fr(risk_s.get("sl_min",0.3), risk_s.get("sl_max",1.2), risk_s.get("sl_step",0.1))
                    mh_min = int(risk_s.get("max_hold_min", 4))
                    mh_max = int(risk_s.get("max_hold_max", 80))
                    mh_step = max(1, int(risk_s.get("max_hold_step", 4)))
                    m_s = max(1, len(list(range(mh_min, mh_max + 1, mh_step))))
                    r_size = t_s * s_s * m_s
            return float(g_size * r_size)
        except Exception:
            return 0.0

    families = sorted({str(p.get("family") or "").strip() for p in pools_all if str(p.get("family") or "").strip()})
    family_opts = ["全部策略"] + families
    sel_family = st.selectbox("查看策略全域進度", options=family_opts, index=0, key=f"gp_family_{int(cycle_id)}")

    pools = pools_all
    if sel_family != "全部策略":
        pools = [p for p in pools_all if str(p.get("family") or "").strip() == sel_family]
        
    total_est = 0.0
    total_done = 0.0
    total_running = 0
    total_assigned = 0
    total_completed = 0
    total_reserved = 0

    for p in pools:
        pool_true_total = _calc_pool_est(p)
        num_parts = max(1, int(p.get("num_partitions") or 1))
        est_per_task = pool_true_total / num_parts
        
        for t in p.get("tasks") or []:
            est = est_per_task
            prog_raw = t.get("progress") or t.get("progress_json") or "{}"
            if isinstance(prog_raw, str):
                try:
                    prog = json.loads(prog_raw)
                except Exception:
                    prog = {}
            else:
                prog = prog_raw if isinstance(prog_raw, dict) else {}
                
            # 若任務有回報真實 combos_total，則以回報的為主更精準
            reported_total = float(prog.get("combos_total") or 0.0)
            if reported_total > 0:
                est = reported_total
                
            done = float(prog.get("combos_done") or 0.0)
            status = str(t.get("status") or "")

            total_est += est
            total_done += done

            if status == "running":
                total_running += 1
            elif status == "completed":
                total_completed += 1
            elif status == "assigned":
                total_assigned += 1
                if int(t.get("user_id") or 0) != int(system_uid):
                    total_reserved += 1

    ratio = (total_done / total_est) if total_est > 0 else 0.0
    paid_sum = 0.0
    try:
        paid_sum = float(_cached_global_paid_payout_sum(int(cycle_id)) or 0.0)
    except Exception:
        paid_sum = 0.0

    st.markdown(_section_title_html("全域挖掘進度", "統計全部用戶已跑組合數、任務完成數與全域進度。下方圖表顯示各策略池分割狀態。", level=4), unsafe_allow_html=True)
    st.progress(min(1.0, max(0.0, float(ratio))))

    kcols = st.columns(4)
    with kcols[0]:
        st.markdown(_render_kpi("全球已跑組合數", f"{int(total_done):,}", f"預估總量 {int(total_est):,}", help_text="累計所有用戶已測試的參數組合數。預估總量來自策略池的工作量精準估計。"), unsafe_allow_html=True)
    with kcols[1]:
        st.markdown(_render_kpi("全球任務完成總數", f"{int(total_completed):,}", f"執行中 {int(total_running):,}", help_text="完成代表分割任務已跑完並上傳結果。執行中表示目前有工作端或伺服器正在運算。"), unsafe_allow_html=True)
    with kcols[2]:
        st.markdown(_render_kpi("全球挖礦進度", f"{ratio*100:.1f}%", f"策略範圍 {sel_family}", help_text="已跑組合數 / 預估總量。可用上方選單切換策略範圍。"), unsafe_allow_html=True)
    with kcols[3]:
        st.markdown(_render_kpi("全用戶已實現分潤", f"{paid_sum:,.6f}", "USDT", help_text="本週期已完成發放並標記為已支付的分潤總額。"), unsafe_allow_html=True)

    recs: List[Dict[str, Any]] = []
    pool_rows: List[Dict[str, Any]] = []
    for p in pools:
        pool_name = str(p.get("pool_name") or "") or f"Pool {p.get('pool_id')}"
        tasks = list(p.get("tasks") or [])
        
        pool_true_total = _calc_pool_est(p)
        num_parts = max(1, int(p.get("num_partitions") or 1))
        est_per_task = pool_true_total / num_parts

        est_sum = 0.0
        done_sum = 0.0
        running = 0
        
        by_bucket: Dict[str, float] = {}
        for t in tasks:
            est = est_per_task
            prog_raw = t.get("progress") or t.get("progress_json") or "{}"
            if isinstance(prog_raw, str):
                try:
                    prog = json.loads(prog_raw)
                except Exception:
                    prog = {}
            else:
                prog = prog_raw if isinstance(prog_raw, dict) else {}
                
            reported_total = float(prog.get("combos_total") or 0.0)
            if reported_total > 0:
                est = reported_total
                
            done = float(prog.get("combos_done") or 0.0)
            status = str(t.get("status") or "")

            est_sum += est
            done_sum += done
            if status == "running":
                running += 1

            b = _partition_bucket(t, system_uid)
            by_bucket[b] = by_bucket.get(b, 0.0) + est

        r = (done_sum / est_sum) if est_sum > 0 else 0.0
        pool_rows.append(
            {
                "策略池": pool_name,
                "標的": str(p.get("symbol") or ""),
                "週期": f"{int(p.get('timeframe_min') or 0)}m",
                "策略": str(p.get("family") or ""),
                "進度": f"{r*100:.1f}%",
                "執行中": running,
            }
        )

        for b, v in by_bucket.items():
            recs.append({"策略池": f"{pool_name}", "狀態": b, "預估組合": v})

    if recs:
        df = pd.DataFrame(recs)
        fig = px.bar(df, x="策略池", y="預估組合", color="狀態", barmode="stack", height=340)
        st.plotly_chart(fig, use_container_width=True)

    if pool_rows:
        st.markdown(_section_title_html("策略池概覽", "列出每個策略池的目標、週期、策略族與進度。進度是該池已跑組合數占預估總量的比例。", level=4), unsafe_allow_html=True)
        st.dataframe(pd.DataFrame(pool_rows), use_container_width=True, hide_index=True)

    st.markdown(_section_title_html("分割分佈", "每個策略池會被切成多個分割。方格顯示各分割目前狀態：待挖掘、已預訂、執行中、已完成。", level=4), unsafe_allow_html=True)
    for p in pools:
        pool_name = str(p.get("pool_name") or "") or f"Pool {p.get('pool_id')}"
        meta = f"{p.get('symbol')} · {p.get('timeframe_min')}m · {p.get('family')}"
        with st.expander(f"{pool_name}（{meta}）", expanded=False):
            tasks = list(p.get("tasks") or [])
            st.markdown(_partition_map_html(tasks, system_uid), unsafe_allow_html=True)
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

    st.markdown(
        """
<style>
.tut_card{
  border-radius:16px;
  border:1px solid rgba(255,255,255,0.10);
  background:rgba(255,255,255,0.04);
  padding:14px 14px 12px 14px;
  margin-bottom:12px;
}
.tut_kicker{font-size:12px;opacity:.70;margin-bottom:4px;}
.tut_title{font-size:16px;font-weight:800;opacity:.96;margin-bottom:6px;}
.tut_body{font-size:13px;line-height:1.65;opacity:.82;}
.tut_anim{animation: tutFade .25s ease-out 1;}
@keyframes tutFade{from{transform:translateY(6px);opacity:.0}to{transform:translateY(0);opacity:1}}
.flow_panel{border-radius:16px;border:1px solid rgba(255,255,255,0.12);background:linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.03));padding:14px 14px 10px 14px;margin-top:10px;}
.flow_header{display:flex;align-items:flex-end;justify-content:space-between;gap:10px;margin-bottom:10px;}
.flow_title{font-size:14px;font-weight:800;opacity:.96;}
.flow_hint{font-size:12px;opacity:.70;}
.flow_items{display:flex;flex-direction:column;gap:8px;}
.flow_item{display:flex;gap:10px;align-items:flex-start;padding:10px 10px;border-radius:12px;border:1px solid rgba(255,255,255,0.10);background:rgba(255,255,255,0.03);}
.flow_badge{width:28px;height:28px;border-radius:10px;display:flex;align-items:center;justify-content:center;border:1px solid rgba(120,180,255,0.45);background:rgba(120,180,255,0.12);font-weight:800;font-size:12px;}
.flow_text{font-size:13px;line-height:1.55;opacity:.88;}
.flow_sub{font-size:12px;opacity:.70;margin-top:2px;}
</style>
        """,
        unsafe_allow_html=True,
    )

    tabs = st.tabs(["總覽", "任務執行", "策略選擇", "候選與提交", "結算"])

    with tabs[0]:
        st.markdown('<div class="tut_card tut_anim"><div class="tut_kicker">快速理解</div><div class="tut_title">用你們的裝置算出最佳交易策略 賺取獎勵分潤</div><div class="tut_body">平台提供所有策略與參數，讓用戶提供算力自行組合並挖出最佳結果。若該策略獲利達標將獲得平台分潤獎勵。</div></div>', unsafe_allow_html=True)
        st.markdown(
            """
<div class="flow_panel tut_anim">
  <div class="flow_header">
    <div class="flow_title">使用流程</div>
    <div class="flow_hint">共 5 步</div>
  </div>
  <div class="flow_items">
    <div class="flow_item"><div class="flow_badge">1</div><div><div class="flow_text">選擇策略並開始任務</div><div class="flow_sub">任務依策略池切分並自動排程</div></div></div>
    <div class="flow_item"><div class="flow_badge">2</div><div><div class="flow_text">產生候選結果</div><div class="flow_sub">本機運算產出候選參數與績效指標</div></div></div>
    <div class="flow_item"><div class="flow_badge">3</div><div><div class="flow_text">提交候選並等待複驗</div><div class="flow_sub">提交後進入伺服器複驗流程</div></div></div>
    <div class="flow_item"><div class="flow_badge">4</div><div><div class="flow_text">複驗通過後進入策略池</div><div class="flow_sub">候選通過驗證後納入策略池</div></div></div>
    <div class="flow_item"><div class="flow_badge">5</div><div><div class="flow_text">依結算週期產出明細</div><div class="flow_sub">依週期統計並產出分潤明細</div></div></div>
  </div>
</div>
""",
            unsafe_allow_html=True,
        )
    with tabs[1]:
        st.markdown('<div class="tut_card tut_anim"><div class="tut_kicker">任務頁</div><div class="tut_title">如何穩定跑任務</div><div class="tut_body">建議使用電腦長時間運行並保持網路穩定。任務頁可一鍵開始全部任務，系統會自動排程並依序執行。</div></div>', unsafe_allow_html=True)
        st.markdown(
            """
<div class="flow_panel tut_anim">
  <div class="flow_header">
    <div class="flow_title">建議操作</div>
    <div class="flow_hint">4 項</div>
  </div>
  <div class="flow_items">
    <div class="flow_item"><div class="flow_badge">1</div><div><div class="flow_text">先選擇策略</div><div class="flow_sub">建議先固定一個策略池，避免反覆切換造成任務釋回。</div></div></div>
    <div class="flow_item"><div class="flow_badge">2</div><div><div class="flow_text">開始全部任務</div><div class="flow_sub">點擊開始後系統會自動排程，並依並行上限逐步執行。</div></div></div>
    <div class="flow_item"><div class="flow_badge">3</div><div><div class="flow_text">觀察執行狀態</div><div class="flow_sub">以執行中數量與任務階段確認是否正常運行。</div></div></div>
    <div class="flow_item"><div class="flow_badge">4</div><div><div class="flow_text">完成後檢視候選</div><div class="flow_sub">任務完成會產生候選結果，可再視品質提交複驗。</div></div></div>
  </div>
</div>
""",
            unsafe_allow_html=True,
        )

    with tabs[2]:
        st.markdown('<div class="tut_card tut_anim"><div class="tut_kicker">策略選擇</div><div class="tut_title">用戶可自行選擇要跑的策略</div><div class="tut_body">策略選擇會影響你領取的分割任務來源。切換策略時，系統會釋回未開始的已分配任務，避免卡住新的任務分配。</div></div>', unsafe_allow_html=True)
        st.markdown(
            """
<div class="flow_panel tut_anim">
  <div class="flow_header">
    <div class="flow_title">選擇原則</div>
    <div class="flow_hint">3 項</div>
  </div>
  <div class="flow_items">
    <div class="flow_item"><div class="flow_badge">1</div><div><div class="flow_text">依共識或偏好選擇</div><div class="flow_sub">可依社群共識或你熟悉的策略型態先投入。</div></div></div>
    <div class="flow_item"><div class="flow_badge">2</div><div><div class="flow_text">固定策略累積貢獻</div><div class="flow_sub">固定策略較容易累積可複用成果，避免分散。</div></div></div>
    <div class="flow_item"><div class="flow_badge">3</div><div><div class="flow_text">用控制台看全域進度</div><div class="flow_sub">控制台可查看策略池進度與分割分佈，選擇更需要算力的池。</div></div></div>
  </div>
</div>
""",
            unsafe_allow_html=True,
        )

    with tabs[3]:
        st.markdown('<div class="tut_card tut_anim"><div class="tut_kicker">候選與提交</div><div class="tut_title">候選不是最終成果</div><div class="tut_body">候選需要通過伺服器複驗。複驗通過後才會進入策略池並參與後續規則與結算。</div></div>', unsafe_allow_html=True)
        st.markdown(
            """
<div class="flow_panel tut_anim">
  <div class="flow_header">
    <div class="flow_title">提交建議</div>
    <div class="flow_hint">3 項</div>
  </div>
  <div class="flow_items">
    <div class="flow_item"><div class="flow_badge">1</div><div><div class="flow_text">優先提交高品質候選</div><div class="flow_sub">以交易數達標且分數較高者優先，提升複驗通過率。</div></div></div>
    <div class="flow_item"><div class="flow_badge">2</div><div><div class="flow_text">避免大量低品質提交</div><div class="flow_sub">低品質候選會增加伺服器負擔，也降低整體效率。</div></div></div>
    <div class="flow_item"><div class="flow_badge">3</div><div><div class="flow_text">看複驗結果再調整投入</div><div class="flow_sub">複驗回饋能反映策略與數據差異，依結果再調整方向。</div></div></div>
  </div>
</div>
""",
            unsafe_allow_html=True,
        )

    with tabs[4]:
        st.markdown('<div class="tut_card tut_anim"><div class="tut_kicker">結算</div><div class="tut_title">提現門檻與手續費規則</div><div class="tut_body">結算頁可設定分潤地址與鏈別。提現需滿足最低門檻，並依平台規則處理手續費。</div></div>', unsafe_allow_html=True)
        st.markdown(
            """
<div class="flow_panel tut_anim">
  <div class="flow_header">
    <div class="flow_title">注意事項</div>
    <div class="flow_hint">3 項</div>
  </div>
  <div class="flow_items">
    <div class="flow_item"><div class="flow_badge">1</div><div><div class="flow_text">分潤地址務必確認</div><div class="flow_sub">地址錯誤可能導致資產無法追回，提交前請自行核對。</div></div></div>
    <div class="flow_item"><div class="flow_badge">2</div><div><div class="flow_text">鏈別需與地址相符</div><div class="flow_sub">鏈別不符可能造成轉帳失敗或資產損失。</div></div></div>
    <div class="flow_item"><div class="flow_badge">3</div><div><div class="flow_text">提現規則以平台設定為準</div><div class="flow_sub">門檻、手續費與結算以平台當期設定與公告為準。</div></div></div>
  </div>
</div>
""",
            unsafe_allow_html=True,
        )

def _page_dashboard(user: Dict[str, Any]) -> None:
    try:
        cycle = db.get_active_cycle()
        if not cycle or "id" not in cycle:
            st.warning("⚠️ 週期尚未初始化，系統正在嘗試建立新週期...")
            db.ensure_cycle_rollover()
            cycle = db.get_active_cycle()
            if not cycle:
                st.error(" 週期建立失敗，請通知系統管理員檢查資料庫權限。")
                return

        pools = db.list_factor_pools(cycle_id=int(cycle["id"])) if cycle else []

        st.markdown(_section_title_html("控制台", "查看你的任務、策略與結算概況。此頁也提供全域挖礦進度與策略池狀態。", level=3), unsafe_allow_html=True)

        # Ensure tasks quota
        conn = db._conn()
        try:
            min_tasks = int(db.get_setting(conn, "min_tasks_per_user", 2))
        finally:
            conn.close()
            
        try:
            db.assign_tasks_for_user(int(user["id"]), min_tasks)
        except AttributeError as ae:
            st.error(f" 系統錯誤：核心函數遺失。\n\n詳細錯誤：{ae}")
            st.info(" 提示：您的 `sheep_platform_db.py` 檔案內容疑似被意外覆蓋，請復原正確的資料庫邏輯。")
            import traceback
            st.code(traceback.format_exc(), language="python")
            return

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

        st.markdown(_section_title_html("任務摘要", "列出目前分配給你的任務狀態、階段、進度與最佳分數。達標表示候選已符合門檻。", level=3), unsafe_allow_html=True)
        if not tasks:
            st.info("無任務。")
        else:
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
                status_cn = _label_task_status(status_raw)
                phase_cn = _label_phase(phase)

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

        st.markdown(_section_title_html("全域進度", "顯示全站所有用戶的整體挖礦進度與分潤統計。可依策略篩選觀察。", level=3), unsafe_allow_html=True)
        # 呼叫此函式也被包裝在最外層的 try-except 中，確保不再出現裸奔錯誤
        cycle_id = int(cycle.get("id") or 0) if cycle else 0
        if cycle_id > 0:
            _render_global_progress(cycle_id)
        else:
            st.info("尚未建立有效的結算週期，目前無全域進度可顯示。")

    except Exception as dashboard_e:
        # [極端專家修復] 最強保護網：不論是上述哪一行程式碼出錯（包含_render_global_progress），都會被攔截並印出精準 Traceback
        st.error(f" 控制台頁面發生嚴重錯誤，已啟動防護隔離：{str(dashboard_e)}")
        st.info("請將下方完整錯誤訊息截圖提供給開發人員進行除錯：")
        import traceback
        st.code(traceback.format_exc(), language="python")
        return

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

    st.markdown("### 任務")

    pools_meta = db.list_factor_pools(int(cycle["id"]))
    fams = sorted({str(p.get("family") or "").strip() for p in pools_meta if str(p.get("family") or "").strip()})
    fam_opts = ["全部策略"] + fams
    sel_family = st.selectbox("策略", options=fam_opts, index=0, key=f"task_family_{int(cycle['id'])}")

    allowed_pool_ids: List[int] = []
    if sel_family != "全部策略":
        allowed_pool_ids = [int(p.get("id") or 0) for p in pools_meta if str(p.get("family") or "").strip() == sel_family and int(p.get("id") or 0) > 0]
        # 切換策略時釋回未開始的已分配任務，避免卡住新策略分配
        try:
            db.release_assigned_tasks_for_user_not_in_pools(int(user["id"]), int(cycle["id"]), allowed_pool_ids)
        except Exception:
            pass
        db.assign_tasks_for_user(
            int(user["id"]),
            cycle_id=int(cycle["id"]),
            min_tasks=int(min_tasks),
            max_tasks=int(max_tasks),
            preferred_family=str(sel_family),
        )
    else:
        db.assign_tasks_for_user(int(user["id"]), cycle_id=int(cycle["id"]), min_tasks=int(min_tasks), max_tasks=int(max_tasks))

    tasks = db.list_tasks_for_user(int(user["id"]), cycle_id=int(cycle["id"]))
    if sel_family != "全部策略" and allowed_pool_ids:
        tasks = [t for t in tasks if int(t.get("pool_id") or 0) in set(allowed_pool_ids)]

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

    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown("#### 控制")
    exec_mode_label = _EXEC_MODE_LABEL.get(exec_mode, exec_mode)
    st.markdown(f'<div class="small-muted">執行模式：{exec_mode_label}</div>', unsafe_allow_html=True)

    auto_refresh = True
    try:
        refresh_s = float(os.environ.get("SHEEP_TASKS_REFRESH_S", "1.0") or 1.0)
    except Exception:
        refresh_s = 1.0
    refresh_s = float(max(0.5, min(10.0, refresh_s)))

    col_a, col_b = st.columns([1.2, 2.0])

    if exec_mode == "server":
        run_key = f"server_run_all_{int(user['id'])}"
        if run_key not in st.session_state:
            st.session_state[run_key] = False
        run_all = bool(st.session_state.get(run_key, False))

        with col_a:
            if not run_all:
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
            else:
                if st.button("停止全部任務", key="stop_all"):
                    st.session_state[run_key] = False
                    run_all = False
                    job_mgr.stop_all_for_user(int(user["id"]))
                    db.write_audit_log(int(user["id"]), "task_stop_all", {})
                    st.rerun()

        with col_b:
            st.markdown(
                f'<div class="small-muted">狀態：{"執行中" if run_all else "待命"} · 並行上限 {int(max_concurrent_jobs)}</div>',
                unsafe_allow_html=True,
            )


    else:
        run_enabled = db.get_user_run_enabled(int(user["id"]))

        with col_a:
            if not bool(run_enabled):
                if st.button("開始全部任務", key="worker_enable"):
                    db.set_user_run_enabled(int(user["id"]), True)
                    st.rerun()
            else:
                if st.button("停止全部任務", key="worker_disable"):
                    db.set_user_run_enabled(int(user["id"]), False)
                    st.rerun()

        with col_b:
            st.markdown(
                f'<div class="small-muted">狀態：{"執行中" if run_enabled else "待命"}</div>',
                unsafe_allow_html=True,
            )
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

        st.markdown('<div class="panel">', unsafe_allow_html=True)
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
    try:
        subs = db.list_submissions(user_id=int(user["id"]), limit=300)
    except AttributeError as ae:
        st.error(f" 系統錯誤：`list_submissions` 函數遺失。\n\n詳細錯誤：{ae}")
        return
    except Exception as e:
        st.error(f" 載入提交紀錄時發生錯誤：{str(e)}")
        import traceback
        st.code(traceback.format_exc(), language="text")
        return
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

    st.markdown('<div class="panel">', unsafe_allow_html=True)
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
        try:
            cycle = db.get_active_cycle()
            if not cycle:
                st.warning("⚠️ 週期尚未初始化")
                st.write("週期", "None", "None", "None")
            else:
                st.write("週期", cycle.get("name"), cycle.get("start_ts"), cycle.get("end_ts"))
                
            ov = db.list_task_overview(limit=500)
        except AttributeError as ae:
            st.error(f" 系統錯誤：管理核心函數遺失。\n\n詳細錯誤：{ae}")
            ov = None
        except Exception as e:
            st.error(f" 載入管理總覽時發生錯誤：{str(e)}")
            import traceback
            st.code(traceback.format_exc(), language="text")
            ov = None
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

            st.markdown("#### 服務條款與分潤規則")
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
        with st.expander("Pool 救援 / 匯入", expanded=(not bool(pools))):
            try:
                st.caption(f"目前資料庫：{db._db_path()} · active_cycle_id={int(cycle_id)}")
            except Exception:
                pass

            if st.button("從本地掃描找回 Pool", key="pool_recover"):
                try:
                    rep = db.recover_factor_pools_from_local(cycle_id=int(cycle_id))
                    if int(rep.get("imported") or 0) > 0:
                        st.success(f"已匯入 {int(rep.get('imported') or 0)} 個 Pool（重複略過 {int(rep.get('skipped_duplicates') or 0)}）")
                    else:
                        st.warning("沒有找到可匯入的 Pool（或全部都已存在）。")
                    st.json(rep)
                    st.rerun()
                except Exception as e:
                    import traceback
                    st.error(f"Pool 救援失敗：{e}")
                    st.code(traceback.format_exc(), language="text")
                    st.stop()
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

        st.markdown("Pool 編輯 / 複製 Pool")

        if not pools:
            st.caption("目前沒有任何 Pool。你可以直接在下方「新增 Pool」建立，或使用「Pool 救援 / 匯入」找回舊資料。")
        else:
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
                    except Exception as e:
                        import traceback
                        st.error(f"保存失敗：{e}")
                        st.code(traceback.format_exc(), language="text")
                        st.stop()

                col_r1, col_r2 = st.columns([1, 1])
                with col_r1:
                    if st.button("重置任務", key="pool_reset_tasks"):
                        try:
                            n = db.delete_tasks_for_pool(cycle_id=cycle_id, pool_id=int(sel_id))
                            db.write_audit_log(int(user["id"]), "pool_reset_tasks", {"pool_id": int(sel_id), "deleted": int(n)})
                            st.rerun()
                        except Exception as e:
                            import traceback
                            st.error(f"重置任務失敗：{e}")
                            st.code(traceback.format_exc(), language="text")
                            st.stop()

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
                    except Exception as e:
                        import traceback
                        st.error(f"複製失敗：{e}")
                        st.code(traceback.format_exc(), language="text")
                        st.stop()

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



def _render_user_hud(user: Dict[str, Any]) -> None:
    """Fixed bottom-left user panel."""
    try:
        cycle = db.get_active_cycle()
    except Exception:
        cycle = None

    cycle_id = int(cycle.get("id") or 0) if cycle else 0

    combos_done_sum = 0
    try:
        if cycle_id > 0:
            tasks = db.list_tasks_for_user(int(user["id"]), cycle_id=cycle_id)
        else:
            tasks = db.list_tasks_for_user(int(user["id"]))
        for t in tasks or []:
            try:
                prog = json.loads(t.get("progress_json") or "{}")
            except Exception:
                prog = {}
            combos_done_sum += int(prog.get("combos_done") or 0)
    except Exception:
        combos_done_sum = 0

    points_sum = 0.0
    try:
        payouts = db.list_payouts(user_id=int(user["id"]), limit=500)
        for p in payouts or []:
            if str(p.get("status") or "") == "void":
                continue
            points_sum += float(p.get("amount_usdt") or 0.0)
    except Exception:
        points_sum = 0.0

    points_help = (
        "積分的規則：每跑過一個達標組合，且因子池系統在一週實盤結算後的 USDT 利潤，其中一半會換算成你的積分。積分可兌換等值 USDT，兌換與發放以結算規則為準。"
    )

    hud_html = (
        '<div class="user_hud">'
        f'<div class="hud_name">{html.escape(str(user.get("username") or ""))}</div>'
        '<div class="hud_div"></div>'
        '<div class="hud_row">'
        '<div class="hud_k">已跑組合</div>'
        f'<div class="hud_v">{int(combos_done_sum):,}</div>'
        "</div>"
        '<div class="hud_row">'
        f'<div class="hud_k">積分{_help_icon_html(points_help)}</div>'
        f'<div class="hud_v">{float(points_sum):,.6f}</div>'
        "</div>"
        "</div>"
    )
    st.markdown(hud_html, unsafe_allow_html=True)



def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide", initial_sidebar_state="expanded")
    _style()
    _bootstrap()

    _apply_cookie_ops()
    _inject_meta(APP_TITLE, "分散算力挖礦與週結算分潤任務平台")

    _render_brand_header(animate=False, dim=False)

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

        # Navigation (custom buttons instead of st.radio) to avoid default red dot indicator
        current_page = str(st.session_state.get("nav_page") or pages[0])

        for p in pages:
            is_active = (p == current_page)
            btn_type = "primary" if is_active else "secondary"
            if st.button(p, key=f"nav_btn_{p}", type=btn_type, use_container_width=True):
                st.session_state["nav_page"] = p
                st.rerun()

        st.markdown('<div style="height: 10px"></div>', unsafe_allow_html=True)

        if st.button("登出", key="logout_btn", type="secondary", use_container_width=True):
            _logout()
            st.rerun()

        _render_user_hud(user)

    page = str(st.session_state.get("nav_page") or pages[0])

    # [專家修復] 全域頁面路由錯誤攔截，最大化顯示錯誤細節與 Traceback，便於光速除錯
    import traceback
    try:
        if page == "新手教學":
            _page_tutorial(user)
        elif page == "控制台":
            _page_dashboard(user)
        elif page == "任務":
            _page_tasks(user, job_mgr)
        elif page == "提交":
            _page_submissions(user)
        elif page == "結算":
            _page_rewards(user)
        elif page == "管理" and role == "admin":
            _page_admin(user, job_mgr)
    except Exception as route_err:
        st.error(f"頁面「{page}」發生錯誤，已觸發全域保護機制。")
        st.info("請將下方錯誤訊息截圖提供給開發人員：")
        st.code(traceback.format_exc(), language="python")
    return


if __name__ == "__main__":
    main()
