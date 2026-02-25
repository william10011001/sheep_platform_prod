import json
import os
import random
import re
import time
import math
import html
import base64
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st
import traceback
import sys

# DataFrame 版本相容處理
def _get_orig_dataframe():
    # 嘗試取得 Streamlit 原始的 dataframe 渲染方法，避開遞迴
    if hasattr(st, "_sheep_orig_dataframe"):
        return st._sheep_orig_dataframe
    
    # 這裡直接從 st 取得原始方法，但要確保我們沒有取得已經被覆蓋過的自己
    orig = getattr(st, "dataframe")
    if getattr(orig, "__name__", "") == "_dataframe_compat":
        # 如果已經被覆蓋，嘗試從類別定義中找回原始方法
        try:
            from streamlit.delta_generator import DeltaGenerator
            return DeltaGenerator.dataframe
        except Exception:
            return orig
    return orig

def _dataframe_compat(data=None, **kwargs):
    # 取得原始 dataframe 方法
    orig = _get_orig_dataframe()
    
    # 攔截並轉換即將廢棄的 use_container_width 參數為新版 width 參數
    if "use_container_width" in kwargs:
        val = kwargs.pop("use_container_width")
        if val is True:
            kwargs["width"] = "stretch"
        elif val is False:
            kwargs["width"] = "content"
            
    # 移除新舊版本衝突的參數
    pop_args = ["hide_index"] # 如果版本太舊不支援此參數，先移除
    
    try:
        # 第一次嘗試執行 (新版 Streamlit)
        return orig(data, **kwargs)
    except (TypeError, Exception):
        # 如果報錯（通常是舊版 Streamlit 不支援 width='stretch'），回退到舊版參數
        if "width" in kwargs:
            w_val = kwargs.pop("width")
            if w_val == "stretch":
                kwargs["use_container_width"] = True
        for arg in pop_args:
            kwargs.pop(arg, None)
        try:
            return orig(data, **kwargs)
        except Exception:
            # 最後防線：退回靜態表格顯示，確保資料不丟失
            return st.table(data)

# 執行覆蓋，僅在尚未覆蓋時進行
if getattr(st.dataframe, "__name__", "") != "_dataframe_compat":
    st._sheep_orig_dataframe = st.dataframe
    st.dataframe = _dataframe_compat
# --------------------------------------------------------
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

APP_TITLE = "羊肉爐團隊 挖礦系統"

# 品牌 WebM：優先吃環境變數，其次走固定且「容器最穩」的 data/ 路徑，最後才退回 static/ 的中文檔名。
# 現實：中文檔名在容器/部署/掛載時最容易失蹤或編碼出事，所以不要只靠它。
_BRAND_WEBM_1 = str(os.environ.get("SHEEP_BRAND_WEBM_1", "") or "").strip()
_BRAND_WEBM_FALLBACKS = [
    "data/brand.webm",
    "data/logo.webm",
    "static/brand.webm",
    "static/logo.webm",
    "static/羊LOGO影片(去背).webm",
]

# 入場 overlay（預設開啟；要關掉就設 SHEEP_ENTRY_OVERLAY=0）
_ENTRY_OVERLAY_ENABLED = (str(os.environ.get("SHEEP_ENTRY_OVERLAY", "1") or "1").strip() != "0")


def _mask_username(username: str, nickname: str = None) -> str:
    """
    Privacy mask logic (V2):
    1. If nickname is set, return nickname.
    2. Masking rules:
       - Length <= 2: First char + *
       - Length 3~4: First char + ** + Last char
       - Length >= 5: First char + *** + Last 2 chars
    """
    if nickname and str(nickname).strip():
        return str(nickname).strip()
    
    s = str(username or "")
    n = len(s)
    if n <= 0:
        return "???"
    if n <= 2:
        return s[0] + "*"
    if n <= 4:
        return f"{s[0]}**{s[-1]}"
    
    # 長度 >= 5: 首1 + *** + 尾2 (例如 s***pd)
    return f"{s[0]}***{s[-2:]}"

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
    ap = _abs_asset_path(path_str)
    try:
        if not ap:
            raise FileNotFoundError(f"empty path (input={path_str!r})")
        with open(ap, "rb") as f:
            raw = f.read()
        if not raw:
            raise IOError(f"file is empty: {ap}")
        return base64.b64encode(raw).decode("ascii")
    except Exception as e:
        # 最大化顯示根因：印到 server log（你看 docker logs 就能直接定位）
        try:
            print(f"[ASSET ERROR] _read_file_b64 failed: input={path_str!r} abs={ap!r} err={e}", file=sys.stderr, flush=True)
            print(traceback.format_exc(), file=sys.stderr, flush=True)
        except Exception:
            pass
        return ""


def _pick_brand_webm_path() -> str:
    # 1) env 指定
    if _BRAND_WEBM_1:
        ap = _abs_asset_path(_BRAND_WEBM_1)
        if ap and os.path.exists(ap):
            return _BRAND_WEBM_1
        try:
            print(f"[ASSET WARN] SHEEP_BRAND_WEBM_1 set but not found: {_BRAND_WEBM_1} (abs={ap})", file=sys.stderr, flush=True)
        except Exception:
            pass

    # 2) fallback 掃描
    for p in _BRAND_WEBM_FALLBACKS:
        ap = _abs_asset_path(p)
        if ap and os.path.exists(ap):
            return p

    # 3) 都沒有：回傳最後一個，讓 log 有明確路徑可看
    return _BRAND_WEBM_FALLBACKS[-1] if _BRAND_WEBM_FALLBACKS else ""


def _render_brand_header(animate: bool, dim: bool = False) -> None:
    # 現實防線：就算檔案不存在，也要留明確線索在 log
    webm_path = _pick_brand_webm_path()
    v1 = _read_file_b64(webm_path)
    data_v1 = f"data:video/webm;base64,{v1}" if v1 else ""

    # 可選： dim 模式（目前保留參數，不強制啟用）
    dim_css = ""
    if dim:
        dim_css = """
#sheepBrandHdr { filter: brightness(0.85) saturate(0.9); }
"""

    # 直接注入到主 DOM：避免 iframe sandbox/allow/autoplay 的不確定性
    # 同時把定位固定，確保各裝置一致
    st.markdown(
        f"""
<style>
#sheepBrandHdr {{
  position: fixed !important;
  top: 0 !important;
  left: 60px !important;
  width: 300px !important;
  height: 84px !important;
  z-index: 500 !important;
  background: transparent !important;
  pointer-events: auto !important;
}}

@media (max-width: 720px) {{
  #sheepBrandHdr {{
    width: 270px !important;
    height: 78px !important;
  }}
}}

div[data-testid="stSidebarCollapsedControl"],
div[data-testid="collapsedControl"] {{
  opacity: 0 !important;
  position: absolute !important;
  width: 1px !important;
  height: 1px !important;
  overflow: hidden !important;
  z-index: -1 !important;
}}

section[data-testid="stSidebar"] button[kind="headerNoPadding"],
section[data-testid="stSidebar"] button[aria-label="Close sidebar"],
button[aria-label="Close sidebar"] {{
  opacity: 0 !important;
  position: absolute !important;
  width: 1px !important;
  height: 1px !important;
  overflow: hidden !important;
  z-index: -1 !important;
}}

header[data-testid="stHeader"] {{
  background: transparent !important;
  z-index: 99999 !important;
  pointer-events: none !important;
}}
header[data-testid="stHeader"] * {{
  pointer-events: auto !important;
}}

{dim_css}

/* header UI */
#sheepBrandHdr .brandWrap {{
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

#sheepBrandHdr .logoContainer {{
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

#sheepBrandHdr video {{
  position: absolute;
  width: 100%;
  height: 100%;
  object-fit: cover;
  transform: scale(1.4);
  display: block;
  background: transparent;
}}

#sheepBrandHdr .fallback {{
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

#sheepBrandHdr .name {{
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

#sheepBrandHdr .name .souper {{
  font-weight: 850;
  background: linear-gradient(135deg, #ffffff 0%, #a0b4ce 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}}

#sheepBrandHdr .name .sheep {{
  font-weight: 950;
  background: linear-gradient(135deg, #ff4b4b 0%, #ff003c 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}}

#sheepBrandHdr .brandWrap:hover {{
  background: rgba(20, 5, 10, 0.95);
  border-color: rgba(255, 0, 60, 0.3);
}}
#sheepBrandHdr .brandWrap:hover .name {{
  filter: brightness(1.15);
}}

#sheepBrandHdr.pulse .brandWrap {{
  animation: ringPulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite;
}}

@keyframes ringPulse {{
  0%   {{ box-shadow: 0 8px 32px rgba(0,0,0,0.6); border-color: rgba(255,255,255,0.08); }}
  50%  {{ box-shadow: 0 12px 48px rgba(0,0,0,0.8), 0 0 20px rgba(255,0,60,0.15); border-color: rgba(255,0,60,0.4); }}
  100% {{ box-shadow: 0 8px 32px rgba(0,0,0,0.6); border-color: rgba(255,255,255,0.08); }}
}}

@media (max-width: 720px) {{
  #sheepBrandHdr .brandWrap {{ top: 6px; left: 6px; gap: 8px; padding: 4px 12px 4px 4px; }}
  #sheepBrandHdr .logoContainer {{ width: 62px; height: 62px; }}
  #sheepBrandHdr .name {{ font-size: 18px; }}
}}
</style>

<div id="sheepBrandHdr" class="{ "pulse" if bool(animate) else "" }">
  <div class="brandWrap" aria-label="Brand">
    <div class="logoContainer" aria-hidden="true">
      <video autoplay muted playsinline loop preload="auto" src="{data_v1}" style="display:{ "block" if data_v1 else "none" };"></video>
      <div class="fallback" style="display:{ "none" if data_v1 else "flex" };">ON</div>
    </div>
    <div class="name" aria-label="OpenNode">
      <span class="souper">Open</span><span class="sheep">Node</span>
    </div>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )

    # 若讀不到就再補一次「很吵但有用」的 log（你要抓根因就靠這個）
    if not data_v1:
        ap = _abs_asset_path(webm_path)
        try:
            print(f"[ASSET WARN] brand webm missing => set SHEEP_BRAND_WEBM_1 or place file at one of: {_BRAND_WEBM_FALLBACKS}", file=sys.stderr, flush=True)
            print(f"[ASSET WARN] chosen={webm_path!r} abs={ap!r}", file=sys.stderr, flush=True)
        except Exception:
            pass


def _render_entry_overlay_once() -> None:
    # 目標：在 Streamlit 內做「跟 deploy/nginx/html/index.html 視覺一致」的入場畫面。
    # 做法：固定全螢幕 overlay + CSS 全部 scope 到 #sheepEntryOverlay，避免被全站 CSS 汙染而造成偏移。
    if not bool(_ENTRY_OVERLAY_ENABLED):
        return
    if bool(st.session_state.get("_sheep_entry_overlay_done")):
        return
    st.session_state["_sheep_entry_overlay_done"] = True

    st.markdown(
        """
<style>
#sheepEntryOverlay{
  position: fixed;
  inset: 0;
  z-index: 2147483646;
  display: flex;
  align-items: center;
  justify-content: center;
  background: #0B0E11;
  overflow: hidden;
  animation: sheepEntryHide 0.25s ease forwards;
  animation-delay: 0.85s;
}

@keyframes sheepEntryHide {
  to { opacity: 0; visibility: hidden; pointer-events: none; }
}

#sheepEntryOverlay, #sheepEntryOverlay * { box-sizing: border-box; }

#sheepEntryOverlay .container{
  width: 100%;
  max-width: 420px;
  padding: 40px;
  background: #181A20;
  border: 1px solid #2B3139;
  border-radius: 4px;
  text-align: center;
  font-family: "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  color: #EAECEF;
}

#sheepEntryOverlay .header { font-size: 20px; font-weight: 600; color: #EAECEF; margin-bottom: 8px; }
#sheepEntryOverlay .status-text { font-size: 14px; color: #848E9C; line-height: 1.5; margin-bottom: 32px; }

#sheepEntryOverlay .progress-bar-container{
  width: 100%;
  height: 4px;
  background: #2B3139;
  border-radius: 2px;
  overflow: hidden;
  margin-bottom: 32px;
}

#sheepEntryOverlay .progress-bar{
  height: 100%;
  width: 0%;
  background: #FCD535;
  animation: loadProgress 0.8s ease-in-out forwards;
}

#sheepEntryOverlay .action-btn{
  display: inline-block;
  width: 100%;
  padding: 12px 0;
  background: #2B3139;
  color: #EAECEF;
  font-size: 14px;
  font-weight: 500;
  text-decoration: none;
  border-radius: 4px;
  transition: background 0.2s;
}

#sheepEntryOverlay .action-btn:hover { background: #3B424C; }

@keyframes loadProgress { to { width: 100%; } }
</style>

<div id="sheepEntryOverlay" data-sheep-entry="SHEEP_ENTRY_V1">
  <div class="container">
    <div class="header">伺服器連線</div>
    <div class="status-text">正在驗證安全憑證並路由至交易終端...</div>
    <div class="progress-bar-container"><div class="progress-bar"></div></div>
    <a class="action-btn" href="/app/">強制進入</a>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )
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
    # 終極修復：_style 每次 rerun 都重塞超大 CSS/iframe，會讓第二次開始越來越慢
    if bool(st.session_state.get("_sheep_style_done")):
        return
    st.session_state["_sheep_style_done"] = True    
    st.markdown(
        """
        <style>
        :root {
          --bg: #0a0c10;
          --card: #161b22;
          --card-border: #30363d;
          --text-primary: #c9d1d9;
          --text-secondary: #8b949e;
          --accent: #1f6feb;
          --accent-hover: #388bfd;
          --success: #238636;
          --danger: #da3633;
          --warning: #d29922;
        }

        .stApp {
          background-color: var(--bg);
          color: var(--text-primary);
          font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
        }

        /* 隱藏預設元素 */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header[data-testid="stHeader"] {background: transparent !important;}

        /* 輸入框美化 */
        div[data-testid="stTextInput"] input,
        div[data-testid="stNumberInput"] input, 
        div[data-testid="stPassword"] input,
        div[data-testid="stTextArea"] textarea,
        div[data-baseweb="select"] > div {
            background-color: #0d1117 !important;
            border: 1px solid var(--card-border) !important;
            color: var(--text-primary) !important;
            border-radius: 6px !important;
        }
        
        div[data-testid="stTextInput"] input:focus,
        div[data-testid="stNumberInput"] input:focus,
        div[data-testid="stPassword"] input:focus, 
        div[data-testid="stTextArea"] textarea:focus {
            border-color: var(--accent) !important;
            box-shadow: 0 0 0 3px rgba(31, 111, 235, 0.3) !important;
        }

        /* 按鈕美化 (極簡黑灰風格) */
        button[kind="secondary"] {
            background-color: #000000 !important;
            border: 1px solid #333333 !important;
            color: #94a3b8 !important;
            font-weight: 600 !important;
            transition: all 0.2s ease !important;
            box-shadow: none !important;
        }
        button[kind="secondary"]:hover {
            background-color: rgba(128, 128, 128, 0.1) !important;
            border-color: #555555 !important;
            color: #e2e8f0 !important;
        }
        
        button[kind="primary"] {
            background-color: rgba(128, 128, 128, 0.2) !important;
            border: 1px solid #666666 !important;
            color: #ffffff !important;
            font-weight: 600 !important;
            transition: all 0.2s ease !important;
            box-shadow: none !important;
        }
        button[kind="primary"]:hover {
            background-color: rgba(128, 128, 128, 0.3) !important;
            border-color: #888888 !important;
        }

        /* 卡片容器 */
        .panel {
            background-color: var(--card);
            border: 1px solid var(--card-border);
            border-radius: 6px;
            padding: 24px;
            margin-bottom: 24px;
        }

        /* 度量指標 */
        .metric {
            background-color: var(--card);
            border: 1px solid var(--card-border);
            padding: 16px;
            border-radius: 6px;
            text-align: center;
        }
        .metric .k {
            font-size: 12px;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 4px;
        }
        .metric .v {
            font-size: 24px;
            font-weight: 600;
            color: var(--text-primary);
        }

        /* 狀態標籤 */
        .pill {
            display: inline-flex;
            align-items: center;
            padding: 2px 10px;
            border-radius: 2em;
            font-size: 12px;
            font-weight: 500;
            line-height: 18px;
            border: 1px solid transparent;
        }
        .pill-ok { background-color: rgba(35, 134, 54, 0.15); color: #3fb950; border-color: rgba(35, 134, 54, 0.4); }
        .pill-warn { background-color: rgba(187, 128, 9, 0.15); color: #d29922; border-color: rgba(187, 128, 9, 0.4); }
        .pill-bad { background-color: rgba(218, 54, 51, 0.15); color: #f85149; border-color: rgba(218, 54, 51, 0.4); }
        .pill-info { background-color: rgba(31, 111, 235, 0.15); color: #58a6ff; border-color: rgba(31, 111, 235, 0.4); }
        .pill-neutral { background-color: rgba(110, 118, 129, 0.15); color: #8b949e; border-color: rgba(110, 118, 129, 0.4); }

        /* 排行榜表格優化 */
        .lb-table { width: 100%; border-collapse: collapse; }
        .lb-row { border-bottom: 1px solid var(--card-border); transition: background-color 0.2s; }
        .lb-row:hover { background-color: #1c2128; }
        .lb-cell { padding: 12px 16px; color: var(--text-primary); font-size: 14px; }
        .rank-badge {
            display: inline-block; width: 24px; height: 24px;
            line-height: 24px; text-align: center;
            border-radius: 50%; font-size: 12px; font-weight: bold;
            background-color: #30363d; color: #8b949e;
        }
        .rank-1 { background-color: #d29922; color: #0d1117; }
        .rank-2 { background-color: #8b949e; color: #0d1117; }
        .rank-3 { background-color: #9e6a03; color: #ffffff; }

        /* 標題樣式 */
        h1, h2, h3 { color: var(--text-primary); font-weight: 600; letter-spacing: -0.5px; }
        .small-muted { font-size: 12px; color: var(--text-secondary); }

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

        header[data-testid="stHeader"] {
          background: transparent !important;
          z-index: 2147483640 !important;
          pointer-events: auto !important;
        }

        footer { visibility: hidden !important; }
        #MainMenu { visibility: hidden !important; }

        div[data-testid="stToolbar"] { display: none !important; }
        div[data-testid="stStatusWidget"] { display: none !important; }
        div[data-testid="stDecoration"] { display: none !important; }

        div[data-testid="stAppViewBlockContainer"] {
            opacity: 1 !important;
            transition: none !important;
            filter: none !important;
            animation: none !important;
        }
        
        .main .block-container {
            padding-top: 90px !important;
        }

        .stButton > button[kind="secondary"], .stDownloadButton > button[kind="secondary"] {
          background: #000000 !important;
          border: 1px solid #333333 !important;
          color: #94a3b8 !important;
          font-weight: 600 !important;
          letter-spacing: 0.5px !important;
          box-shadow: none !important;
        }
        .stButton > button[kind="secondary"]:hover, .stDownloadButton > button[kind="secondary"]:hover {
          background: rgba(128, 128, 128, 0.1) !important;
          border-color: #555555 !important;
          color: #e2e8f0 !important;
        }

        .stButton > button[kind="primary"], .stDownloadButton > button[kind="primary"] {
          background: rgba(128, 128, 128, 0.2) !important;
          border: 1px solid #666666 !important;
          color: #ffffff !important;
          font-weight: 600 !important;
          letter-spacing: 0.5px !important;
          box-shadow: none !important;
        }
        .stButton > button[kind="primary"]:hover, .stDownloadButton > button[kind="primary"]:hover {
          background: rgba(128, 128, 128, 0.3) !important;
          border-color: #888888 !important;
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

        /* [專家級修正] 徹底解決側邊欄位移、滾動條跳動、頂部留白與按鈕間距問題 */
        section[data-testid="stSidebar"] {
            min-width: 260px !important;
            max-width: 260px !important;
            background: rgba(10, 14, 20, 0.95) !important;
            border-right: 1px solid var(--border) !important;
            backdrop-filter: blur(10px);
            -webkit-backdrop-filter: blur(10px);
        }
        
        div[data-testid="stSidebarHeader"] {
            display: none !important; /* 隱藏原生頂部區塊，消滅巨大留白與關閉按鈕造成的排版跳動 */
        }

        div[data-testid="stSidebarUserContent"] {
            padding-top: 0 !important; /* 防止 Streamlit 原生插入的多餘頂部間距 */
        }

        div[data-testid="stSidebarContent"] {
            padding-top: 80px !important; /* 為左上角自訂懸浮選單按鈕留出絕對安全的空間 */
            padding-bottom: 40px !important;
            overflow-x: hidden !important; /* 絕對禁止水平卷軸導致的內容左右推擠位移 */
            overflow-y: overlay !important;
        }
        
        div[data-testid="stSidebarContent"]::-webkit-scrollbar {
            width: 4px; /* 縮小垂直卷軸體積，避免擠壓導覽按鈕 */
            background: transparent;
        }
        div[data-testid="stSidebarContent"]::-webkit-scrollbar-thumb {
            background: rgba(255,255,255,0.1);
            border-radius: 4px;
        }

        /* 消除選單按鈕之間的無規則外邊距，防止點擊或 Hover 時的上下跳動現象 */
        div[data-testid="stSidebar"] div[data-testid="stVerticalBlock"] > div.element-container {
            margin-bottom: 2px !important;
        }

        .help_wrap { display: inline-flex; position: relative; align-items: center; margin-left: 8px; z-index: 50; }        .help_icon {
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
          width: max-content; max-width: 260px; padding: 12px 16px;
          border-radius: 8px; background: rgba(30, 41, 59, 0.95);
          border: 1px solid rgba(255, 255, 255, 0.1); box-shadow: 0 16px 40px rgba(0, 0, 0, 0.8);
          color: #f8fafc; font-size: 13px; line-height: 1.5;
          opacity: 0; pointer-events: none; transition: all 0.2s ease; backdrop-filter: blur(12px);
          z-index: 2147483647;
          white-space: normal;
          word-break: break-word;
        }
        .help_wrap:hover .help_tip { opacity: 1; transform: translateX(-50%) translateY(0); }
        
        .user_hud .help_tip {
          left: 0;
          transform: translateX(0) translateY(4px);
        }
        .user_hud .help_wrap:hover .help_tip {
          transform: translateX(0) translateY(0);
        }

        .sec_h3 { font-size: 24px; font-weight: 800; color: #ffffff; margin: 32px 0 16px 0; display: flex; align-items: center; }
        .sec_h4 { font-size: 18px; font-weight: 700; color: #e2e8f0; margin: 24px 0 12px 0; display: flex; align-items: center; }

        .panel {
          border-radius: 12px; border: 1px solid var(--border);
          background: var(--card); padding: 24px;
          margin: 16px 0; box-shadow: var(--shadow);
        }

        .user_hud {
          margin-top: 24px;
          width: 100%;
          box-sizing: border-box;
          padding: 16px; border-radius: 12px;
          border: 1px solid var(--border);
          background: rgba(15, 23, 42, 0.85);
          box-shadow: 0 4px 12px rgba(0,0,0,0.3);
          backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);
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

        #custom-sys-menu-btn {
            position: fixed !important;
            top: 16px !important;
            left: 16px !important;
            width: 44px !important;
            height: 44px !important;
            background: linear-gradient(135deg, rgba(30,41,59,0.95) 0%, rgba(15,23,42,0.98) 100%) !important;
            border-radius: 12px !important;
            border: 1px solid rgba(255,255,255,0.15) !important;
            z-index: 2147483647 !important;
            cursor: pointer !important;
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
            box-shadow: 0 4px 20px rgba(0,0,0,0.6) !important;
            transition: all 0.2s ease !important;
            pointer-events: auto !important;
            backdrop-filter: blur(10px);
            -webkit-backdrop-filter: blur(10px);
        }
        #custom-sys-menu-btn:hover {
            background: linear-gradient(135deg, rgba(59,130,246,0.9) 0%, rgba(37,99,235,0.95) 100%) !important;
            border-color: rgba(96,165,250,0.5) !important;
            transform: translateY(-2px);
            box-shadow: 0 8px 24px rgba(37,99,235,0.4) !important;
        }
        #custom-sys-menu-btn svg {
            fill: #ffffff !important;
            width: 22px !important;
            height: 22px !important;
        }

        /* 排行榜樣式設定 */
        /* 隱藏原生 Radio 裝飾元素 */
        .lb-period-selector div[role="radiogroup"] > label > div:first-child {
            display: none !important;
        }
        .lb-period-selector div[role="radiogroup"] {
            display: flex;
            gap: 8px;
            background: rgba(255, 255, 255, 0.03);
            padding: 4px;
            border-radius: 12px;
            border: 1px solid rgba(255, 255, 255, 0.08);
        }
        .lb-period-selector label {
            flex: 1;
            text-align: center;
            padding: 8px 16px !important;
            border-radius: 8px !important;
            border: 1px solid transparent !important;
            transition: all 0.2s ease !important;
            margin: 0 !important;
            background: transparent;
            color: #94a3b8 !important;
            font-weight: 600 !important;
            display: flex;
            justify-content: center;
            align-items: center;
        }
        /* 選中狀態模擬 */
        .lb-period-selector label[data-baseweb="radio"]:has(input:checked) {
            background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%) !important;
            color: #ffffff !important;
            box-shadow: 0 4px 12px rgba(37, 99, 235, 0.4);
            border: 1px solid rgba(255, 255, 255, 0.1) !important;
        }
        .lb-period-selector label:hover {
            background: rgba(255, 255, 255, 0.05);
            color: #e2e8f0 !important;
        }

        /* 2. 排行榜表格樣式 (取代 DataFrame) */
        .lb-table {
            width: 100%;
            border-collapse: separate;
            border-spacing: 0 8px;
            margin-top: 10px;
        }
        .lb-row {
            background: rgba(30, 41, 59, 0.4);
            border-radius: 12px;
            transition: transform 0.2s ease, background 0.2s ease;
        }
        .lb-row:hover {
            transform: scale(1.01);
            background: rgba(30, 41, 59, 0.7);
            box-shadow: 0 4px 20px rgba(0,0,0,0.3);
        }
        .lb-cell {
            padding: 16px;
            color: #e2e8f0;
            vertical-align: middle;
            border-top: 1px solid rgba(255,255,255,0.05);
            border-bottom: 1px solid rgba(255,255,255,0.05);
        }
        .lb-row td:first-child {
            border-left: 1px solid rgba(255,255,255,0.05);
            border-top-left-radius: 12px;
            border-bottom-left-radius: 12px;
            width: 60px;
            text-align: center;
        }
        .lb-row td:last-child {
            border-right: 1px solid rgba(255,255,255,0.05);
            border-top-right-radius: 12px;
            border-bottom-right-radius: 12px;
            text-align: right;
            font-family: monospace;
            font-size: 1.1em;
            font-weight: 700;
            color: #60a5fa;
        }
        
        /* CSS 獎牌渲染 */
        .rank-badge {
            width: 32px;
            height: 32px;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            border-radius: 50%;
            font-weight: 800;
            font-size: 14px;
            color: #fff;
            text-shadow: 0 1px 2px rgba(0,0,0,0.5);
            box-shadow: 0 4px 10px rgba(0,0,0,0.3);
        }
        .rank-1 {
            background: linear-gradient(135deg, #FFD700 0%, #FDB931 100%);
            border: 2px solid #FFF8D6;
            box-shadow: 0 0 15px rgba(255, 215, 0, 0.4);
        }
        .rank-2 {
            background: linear-gradient(135deg, #E0E0E0 0%, #BDBDBD 100%);
            border: 2px solid #FFFFFF;
        }
        .rank-3 {
            background: linear-gradient(135deg, #CD7F32 0%, #A0522D 100%);
            border: 2px solid #FFDAB9;
        }
        .rank-other {
            background: rgba(255,255,255,0.1);
            color: #94a3b8;
            border: 1px solid rgba(255,255,255,0.1);
            width: 28px; height: 28px; font-size: 12px;
        }

        /* 3. 暱稱設定卡片樣式設定 */
        .nick-card {
            background: linear-gradient(135deg, rgba(255,215,0,0.08) 0%, rgba(0,0,0,0.2) 100%);
            border: 1px solid rgba(255,215,0,0.4);
            border-radius: 16px;
            padding: 24px;
            position: relative;
            overflow: hidden;
            margin-bottom: 28px;
            box-shadow: 0 4px 24px rgba(255, 215, 0, 0.05);
        }
        .nick-card::before {
            content: '';
            position: absolute;
            top: 0; left: 0; width: 6px; height: 100%;
            background: linear-gradient(to bottom, #FFD700, #FDB931);
            box-shadow: 2px 0 15px rgba(255, 215, 0, 0.6);
        }
        
        /* 純 CSS 皇冠渲染 (去除 Emoji) */
        .crown-icon {
            display: inline-block;
            width: 28px; height: 28px;
            background: linear-gradient(135deg, #FFD700 0%, #FDB931 100%);
            /* CSS Crown Polygon */
            clip-path: polygon(5% 100%, 100% 100%, 95% 0%, 75% 65%, 50% 10%, 25% 65%, 5% 0%);
            margin-right: 12px;
            vertical-align: text-bottom;
            box-shadow: 0 2px 10px rgba(255, 215, 0, 0.8);
        }

        /* 4. 排行榜週期選單美化 (Segmented Control 模擬) */
        /* 強制隱藏 Streamlit Radio 的圓點與預設樣式 */
        .lb-period-selector div.stRadio > label { display: none !important; } /* Hide label title */
        .lb-period-selector div[role="radiogroup"] {
            display: flex !important;
            flex-direction: row !important;
            background: transparent !important;
            padding: 0px !important;
            border-radius: 12px !important;
            border: none !important;
            gap: 8px !important;
            width: fit-content !important;
            margin-bottom: 20px !important;
        }
        .lb-period-selector div[role="radiogroup"] label {
            margin-right: 0px !important;
            padding: 8px 24px !important;
            border-radius: 8px !important;
            border: 1px solid #333333 !important;
            background: #000000 !important;
            transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1) !important;
            color: #94a3b8 !important;
            font-weight: 600 !important;
            font-size: 14px !important;
            cursor: pointer !important;
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
            min-width: 100px !important;
        }
        /* 選中狀態的高亮 */
        .lb-period-selector div[role="radiogroup"] label:has(input:checked) {
            background: rgba(128, 128, 128, 0.2) !important;
            color: #ffffff !important;
            box-shadow: none !important;
            border: 1px solid #666666 !important;
            transform: none !important;
        }
        /* Hover 效果 */
        .lb-period-selector div[role="radiogroup"] label:hover:not(:has(input:checked)) {
            background: rgba(128, 128, 128, 0.1) !important;
            border-color: #555555 !important;
            color: #e2e8f0 !important;
        }
        /* 隱藏原生 input */
        .lb-period-selector input[type="radio"] {
            display: none !important;
        }
        /* 隱藏 Streamlit 原生 Radio 裝飾 div */
        .lb-period-selector div[role="radiogroup"] label > div:first-child {
            display: none !important;
        }

        /* 主頁按鈕專屬樣式 (極度顯眼的紅色脈衝效果) */
        .home-core-btn {
            background: linear-gradient(135deg, #FF003C 0%, #8A0020 100%) !important;
            color: #ffffff !important;
            border: 2px solid #FF003C !important;
            box-shadow: 0 0 15px rgba(255, 0, 60, 0.6), inset 0 0 10px rgba(255, 255, 255, 0.2) !important;
            font-weight: 900 !important;
            letter-spacing: 3px !important;
            animation: pulseHomeBtn 2s cubic-bezier(0.4, 0, 0.6, 1) infinite !important;
            transition: all 0.3s ease !important;
        }
        .home-core-btn:hover {
            background: linear-gradient(135deg, #ff1a53 0%, #a30026 100%) !important;
            box-shadow: 0 0 25px rgba(255, 0, 60, 0.9), inset 0 0 15px rgba(255, 255, 255, 0.4) !important;
            transform: translateY(-2px) scale(1.02) !important;
            color: #ffffff !important;
        }
        @keyframes pulseHomeBtn {
            0%, 100% { box-shadow: 0 0 15px rgba(255, 0, 60, 0.6), inset 0 0 10px rgba(255, 255, 255, 0.2); }
            50% { box-shadow: 0 0 30px rgba(255, 0, 60, 0.95), inset 0 0 15px rgba(255, 255, 255, 0.4); }
        }

/* --- [純 CSS 綁定側邊欄按鈕特效與圖示] --- /
/ [終極除錯與修復] 徹底隱藏錨點，避免影響 DOM 流與觸發 :has() 失效 */
div[data-testid="stSidebar"] div.element-container:has(.sidebar-anchor) {
position: absolute !important;
width: 0 !important;
height: 0 !important;
opacity: 0 !important;
margin: 0 !important;
padding: 0 !important;
pointer-events: none !important;
visibility: hidden !important;
}

    /* [核心修復] 將按鈕內容容器轉為極度穩定的絕對定位基準點 */
    div[data-testid=&quot;stSidebar&quot;] .stButton button {
        position: relative !important;
        overflow: visible !important;
    }

    div[data-testid=&quot;stSidebar&quot;] .stButton button div[data-testid=&quot;stMarkdownContainer&quot;] {
        width: 100% !important;
        display: flex !important;
        align-items: center !important;
        justify-content: flex-start !important;
    }

    /* 統一按鈕文字與圖示間距，徹底拋棄會因視窗寬度跑版的 calc(50%) 置中對齊 */
    div[data-testid=&quot;stSidebar&quot;] .stButton button p {
        position: relative !important;
        display: flex !important;
        align-items: center !important;
        justify-content: flex-start !important;
        width: 100% !important;
        margin: 0 !important;
        padding-left: 36px !important; /* 強制預留左側 ICON 空間 */
        font-size: 15px !important;
        font-weight: 600 !important;
        letter-spacing: 0.5px !important;
    }
    
    /* 統一設定偽類 ICON 基礎屬性，絕對定位在文字左方，完全避免被擠壓消失 */
    div[data-testid=&quot;stSidebar&quot;] .stButton button p::before {
        content: &#39;&#39; !important;
        position: absolute !important;
        left: 4px !important; /* 絕對固定在文字左側 4px 處 */
        top: 50% !important;
        transform: translateY(-50%) !important;
        width: 18px !important; 
        height: 18px !important;
        display: block !important;
        flex-shrink: 0 !important;
        transition: all 0.2s ease !important;
        background-color: transparent !important;
        background-image: none !important;
        border: none !important;
        clip-path: none !important;
        box-sizing: border-box !important;
    }

    /* ------------------ 極簡高階純CSS幾何圖示定義 (完全 currentColor，無 Emoji) ------------------ */

    /* 1. 主頁按鈕 (Home - 現代幾何房屋) */
    div.element-container:has(.nav-anchor-主頁) + div.element-container .stButton button {
        background: linear-gradient(135deg, #FF003C 0%, #8A0020 100%) !important;
        border: 2px solid #FF003C !important;
        box-shadow: 0 0 15px rgba(255, 0, 60, 0.6), inset 0 0 10px rgba(255, 255, 255, 0.2) !important;
        animation: pulseHomeBtn 2s cubic-bezier(0.4, 0, 0.6, 1) infinite !important;
        transition: all 0.3s ease !important;
    }
    div.element-container:has(.nav-anchor-主頁) + div.element-container .stButton button p { color: #ffffff !important; }
    div.element-container:has(.nav-anchor-主頁) + div.element-container .stButton button:hover {
        background: linear-gradient(135deg, #ff1a53 0%, #a30026 100%) !important;
        box-shadow: 0 0 25px rgba(255, 0, 60, 0.9), inset 0 0 15px rgba(255, 255, 255, 0.4) !important;
        transform: translateY(-2px) scale(1.02) !important;
    }
    div.element-container:has(.nav-anchor-主頁) + div.element-container .stButton button p::before {
        background-color: currentColor !important;
        clip-path: polygon(50% 0%, 100% 45%, 85% 45%, 85% 100%, 15% 100%, 15% 45%, 0% 45%) !important;
    }
    @keyframes pulseHomeBtn {
        0%, 100% { box-shadow: 0 0 15px rgba(255, 0, 60, 0.6), inset 0 0 10px rgba(255, 255, 255, 0.2); }
        50% { box-shadow: 0 0 30px rgba(255, 0, 60, 0.95), inset 0 0 15px rgba(255, 255, 255, 0.4); }
    }

    /* 2. 控制台 (Dashboard - 科技感 2x2 方塊網格) */
    div.element-container:has(.nav-anchor-控制台) + div.element-container .stButton button p::before {
        background-image: 
            linear-gradient(currentColor, currentColor), linear-gradient(currentColor, currentColor),
            linear-gradient(currentColor, currentColor), linear-gradient(currentColor, currentColor) !important;
        background-position: 0 0, 100% 0, 0 100%, 100% 100% !important;
        background-size: 7px 7px !important;
        background-repeat: no-repeat !important;
    }

    /* 3. 排行榜 (Leaderboard - 數據長條圖) */
    div.element-container:has(.nav-anchor-排行榜) + div.element-container .stButton button p::before {
        background-image: 
            linear-gradient(currentColor, currentColor),
            linear-gradient(currentColor, currentColor),
            linear-gradient(currentColor, currentColor) !important;
        background-position: 0 100%, 50% 100%, 100% 100% !important;
        background-size: 4px 10px, 4px 18px, 4px 14px !important;
        background-repeat: no-repeat !important;
    }

    /* 4. 任務 (Tasks - 任務清單線條) */
    div.element-container:has(.nav-anchor-任務) + div.element-container .stButton button p::before {
        background-image: 
            linear-gradient(currentColor, currentColor), linear-gradient(currentColor, currentColor),
            linear-gradient(currentColor, currentColor), linear-gradient(currentColor, currentColor),
            linear-gradient(currentColor, currentColor), linear-gradient(currentColor, currentColor) !important;
        background-position: 
            0 2px, 0 8px, 0 14px,
            6px 2px, 6px 8px, 6px 14px !important;
        background-size: 
            4px 3px, 4px 3px, 4px 3px,
            12px 3px, 12px 3px, 12px 3px !important;
        background-repeat: no-repeat !important;
    }

    /* 5. 提交 (Submit - 上傳/提交箭頭) */
    div.element-container:has(.nav-anchor-提交) + div.element-container .stButton button p::before {
        background-color: currentColor !important;
        clip-path: polygon(50% 0%, 100% 45%, 70% 45%, 70% 100%, 30% 100%, 30% 45%, 0% 45%) !important;
    }

    /* 6. 結算 (Rewards - 錢幣/代幣) */
    div.element-container:has(.nav-anchor-結算) + div.element-container .stButton button p::before {
        border: 2px solid currentColor !important;
        border-radius: 50% !important;
        background-image: radial-gradient(circle at center, currentColor 35%, transparent 40%) !important;
    }

    /* 7. 管理 (Admin - 權限盾牌) */
    div.element-container:has(.nav-anchor-管理) + div.element-container .stButton button p::before {
        background-color: currentColor !important;
        clip-path: polygon(50% 0%, 100% 20%, 100% 60%, 50% 100%, 0% 60%, 0% 20%) !important;
    }

    /* 8. 新手教學 (Tutorial - 資訊 &#39;i&#39; 標誌) */
    div.element-container:has(.nav-anchor-新手教學) + div.element-container .stButton button {
        background: linear-gradient(135deg, rgba(251, 146, 60, 0.15) 0%, rgba(234, 88, 12, 0.05) 100%) !important;
        backdrop-filter: blur(12px) !important;
        border: 1px solid rgba(251, 146, 60, 0.3) !important;
        box-shadow: 0 4px 16px rgba(251, 146, 60, 0.05) !important;
        transition: all 0.3s ease !important;
    }
    div.element-container:has(.nav-anchor-新手教學) + div.element-container .stButton button p { color: #fed7aa !important; }
    div.element-container:has(.nav-anchor-新手教學) + div.element-container .stButton button:hover {
        background: linear-gradient(135deg, rgba(251, 146, 60, 0.25) 0%, rgba(234, 88, 12, 0.1) 100%) !important;
        border-color: rgba(251, 146, 60, 0.5) !important;
        box-shadow: 0 6px 20px rgba(251, 146, 60, 0.15) !important;
        transform: translateY(-1px);
    }
    div.element-container:has(.nav-anchor-新手教學) + div.element-container .stButton button:hover p { color: #ffffff !important; }
    div.element-container:has(.nav-anchor-新手教學) + div.element-container .stButton button p::before {
        border: 2px solid currentColor !important;
        border-radius: 50% !important;
        background-image: 
            linear-gradient(currentColor, currentColor),
            linear-gradient(currentColor, currentColor) !important;
        background-position: center 3px, center 8px !important;
        background-size: 3px 3px, 3px 6px !important;
        background-repeat: no-repeat !important;
    }

    /* 9. 登出 (Logout - 電源/退出標誌) */
    div.element-container:has(.nav-anchor-登出) + div.element-container .stButton button {
        background: linear-gradient(135deg, #dc2626 0%, #991b1b 100%) !important;
        border: 1px solid #ef4444 !important;
        box-shadow: 0 4px 15px rgba(220, 38, 38, 0.4) !important;
        transition: all 0.3s ease !important;
    }
    div.element-container:has(.nav-anchor-登出) + div.element-container .stButton button p { color: #ffffff !important; }
    div.element-container:has(.nav-anchor-登出) + div.element-container .stButton button:hover {
        background: linear-gradient(135deg, #ef4444 0%, #b91c1c 100%) !important;
        border-color: #f87171 !important;
        box-shadow: 0 6px 20px rgba(239, 68, 68, 0.6) !important;
        transform: translateY(-2px) !important;
    }
    div.element-container:has(.nav-anchor-登出) + div.element-container .stButton button p::before {
        border: 2px solid currentColor !important;
        border-top-color: transparent !important;
        border-radius: 50% !important;
        background-image: linear-gradient(currentColor, currentColor) !important;
        background-position: center top !important;
        background-size: 2px 8px !important;
        background-repeat: no-repeat !important;
    }

        /* 終極紅色量化主題覆蓋 */
        :root {
        --bg: #050007 !important;
        --card: rgba(18, 2, 5, 0.85) !important;
        --card-border: rgba(255, 0, 60, 0.5) !important;
        --text-primary: #f8fafc !important;
        --text-secondary: #ff8a9f !important;
        --accent: #FF003C !important;
        --accent-glow: rgba(255, 0, 60, 0.8) !important;
    }
    .stApp {
        background-color: #050007 !important;
        background-image: 
            radial-gradient(circle at 50% 20%, rgba(255,0,60,0.2) 0%, transparent 60%),
            linear-gradient(rgba(255,0,60,0.12) 1px, transparent 1px),
            linear-gradient(90deg, rgba(255,0,60,0.12) 1px, transparent 1px) !important;
        background-size: 100% 100%, 40px 40px, 40px 40px !important;
        background-attachment: fixed !important;
        animation: cyberpunkGrid 15s linear infinite !important;
    }
    .stApp::before {
        content: "";
        position: fixed;
        top: 0; left: 0; width: 100vw; height: 100vh;
        background: repeating-linear-gradient(to bottom, transparent 0px, rgba(255,0,60,0.03) 1px, transparent 3px);
        pointer-events: none;
        z-index: 2147483645;
    }
    @keyframes cyberpunkGrid {
        0% { background-position: 0 0, 0 0, 0 0; }
        100% { background-position: 0 0, 0 40px, 40px 0; }
    }
    .main { background: transparent !important; }
        section[data-testid="stSidebar"] {
            background: rgba(8, 0, 2, 0.95) !important;
            border-right: 1px solid rgba(255, 0, 60, 0.4) !important;
        }
        button[kind="primary"] {
            background: rgba(255, 0, 60, 0.15) !important;
            border: 1px solid #FF003C !important;
            color: #FF003C !important;
            text-shadow: 0 0 8px rgba(255, 0, 60, 0.5) !important;
            box-shadow: inset 0 0 10px rgba(255,0,60,0.2) !important;
        }
        button[kind="primary"]:hover {
            background: rgba(255, 0, 60, 0.3) !important;
            box-shadow: 0 0 20px rgba(255,0,60,0.5) !important;
            color: #fff !important;
            text-shadow: 0 0 10px #fff !important;
        }
        div[data-testid="stTextInput"] input, div[data-testid="stNumberInput"] input, div[data-testid="stPassword"] input {
            background: rgba(15, 0, 3, 0.8) !important;
            border-color: rgba(255, 0, 60, 0.4) !important;
        }
        div[data-testid="stTextInput"] input:focus, div[data-testid="stNumberInput"] input:focus, div[data-testid="stPassword"] input:focus {
            border-color: #FF003C !important;
            box-shadow: 0 0 0 3px rgba(255, 0, 60, 0.3) !important;
        }
        .pill-ok { background: rgba(0, 255, 204, 0.15) !important; color: #00FFCC !important; border-color: rgba(0, 255, 204, 0.4) !important; }
        .pill-warn { background: rgba(255, 153, 0, 0.15) !important; color: #ff9900 !important; border-color: rgba(255, 153, 0, 0.4) !important; }
        .pill-bad { background: rgba(255, 0, 60, 0.15) !important; color: #FF003C !important; border-color: rgba(255, 0, 60, 0.4) !important; }
        .pill-info { background: rgba(255, 51, 102, 0.15) !important; color: #ff3366 !important; border-color: rgba(255, 51, 102, 0.4) !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )

# [載入動畫] 完全使用 index.html，並加入 Streamlit iframe 全螢幕與隱藏修正
    st.components.v1.html(
        """
        <!doctype html>
        <html lang="zh-Hant">
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1" />

          <title>羊肉爐挖礦分潤任務平台</title>
          <style>
            :root{
              --bg0:#050007;
              --bg1:#12000a;
              --ink:#e9e9ee;
              --accent:#ff003c;
              --accent2:#f7931a;
              --gridA:0;
              --flash:0;
            }

            html,body{height:100%;}
            body{
              margin:0;
              background: radial-gradient(1200px 700px at 50% 45%, var(--bg1) 0%, var(--bg0) 60%, #020001 100%);
              color:var(--ink);
              font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
              overflow:hidden;
            }

            #loader{
              position:fixed;
              inset:0;
              display:block;
              isolation:isolate;
              background: radial-gradient(900px 600px at 50% 45%, rgba(255,0,60,0.10) 0%, rgba(5,0,7,0.95) 65%, rgba(0,0,0,0.98) 100%);
              --gridA: 0;
              --flash: 0;
            }

            #loader::before{
              content:"";
              position:absolute;
              inset:-2px;
              background:
                repeating-linear-gradient(
                  to bottom,
                  rgba(255,255,255,0.03) 0px,
                  rgba(255,255,255,0.03) 1px,
                  rgba(0,0,0,0) 3px,
                  rgba(0,0,0,0) 6px
                );
              opacity:0.35;
              mix-blend-mode: overlay;
              pointer-events:none;
              z-index:4;
            }
            #loader::after{
              content:"";
              position:absolute;
              inset:0;
              background: radial-gradient(closest-side at 50% 45%, rgba(255,255,255,0.06), rgba(0,0,0,0.55));
              opacity: calc(0.65 + var(--flash) * 0.55);
              pointer-events:none;
              z-index:5;
            }

            .grid-layer{
              position:absolute;
              left:50%;
              top:62%;
              width:1800px;
              height:1800px;
              transform: translate(-50%,-50%) perspective(900px) rotateX(68deg) translateY(120px);
              transform-origin:center;
              background-image:
                linear-gradient(rgba(255,0,60,0.30) 1px, transparent 1px),
                linear-gradient(90deg, rgba(255,0,60,0.30) 1px, transparent 1px);
              background-size: 72px 72px;
              opacity: var(--gridA);
              filter: drop-shadow(0 0 14px rgba(255,0,60,0.35));
              animation: gridMove 1.1s linear infinite;
              z-index:1;
              pointer-events:none;
            }
            @keyframes gridMove{to{background-position:0 72px;}}

            #fx{
              position:absolute;
              inset:0;
              width:100%;
              height:100%;
              z-index:3;
              pointer-events:none;
            }

            #scene{
              position:absolute;
              inset:0;
              width:100%;
              height:100%;
              z-index:2;
              overflow:visible;
              pointer-events:none;
              will-change: transform;
            }

            body.edit-mode #scene{ pointer-events:auto; }
            body.edit-mode #scene *{ pointer-events:auto; }
            body.edit-mode{ cursor: default; }

            #editorHud{
              position: absolute;
              right: 18px;
              top: 18px;
              z-index: 10;
              width: min(420px, calc(100vw - 36px));
              padding: 14px 14px 12px 14px;
              border: 1px solid rgba(255,0,60,0.55);
              border-radius: 14px;
              background: rgba(0,0,0,0.55);
              box-shadow: 0 0 24px rgba(255,0,60,0.18);
              backdrop-filter: blur(10px);
              display: none;
              user-select:none;
              pointer-events:none;
            }
            body.edit-mode #editorHud{ display:block; }

            #editorHud .t{
              font-weight: 900;
              letter-spacing: 0.08em;
              color: rgba(255,0,60,0.95);
              text-shadow: 0 0 10px rgba(255,0,60,0.35);
              margin-bottom: 10px;
              font-size: 12px;
            }
            #editorHud .b{
              font-size: 12px;
              line-height: 1.55;
              color: rgba(255,255,255,0.85);
            }
            #editorHud .k{
              color: rgba(247,147,26,0.95);
              font-weight: 800;
            }
            #editorHud .id{
              color: rgba(255,255,255,0.95);
              font-weight: 900;
            }

            .hud{
              position:absolute;
              left:26px;
              bottom:22px;
              z-index:6;
              display:flex;
              flex-direction:column;
              gap:10px;
              user-select:none;
              pointer-events:none;
            }
            .hud .row{display:flex; align-items:baseline; gap:10px; letter-spacing:0.06em;}
            .hud .label{color: rgba(255,0,60,0.95); text-shadow: 0 0 10px rgba(255,0,60,0.45); font-weight:700; font-size:12px;}
            .hud .pct{font-weight:800; font-size:14px; color: rgba(255,255,255,0.92); text-shadow: 0 0 12px rgba(255,255,255,0.22);}
            .hud .bar{
              width:min(360px, calc(100vw - 52px));
              height:10px;
              border:1px solid rgba(255,0,60,0.55);
              border-radius:999px;
              overflow:hidden;
              background: rgba(0,0,0,0.22);
              box-shadow: 0 0 18px rgba(255,0,60,0.18);
            }
            .hud .bar > i{
              display:block;
              height:100%;
              width:0%;
              background: linear-gradient(90deg, rgba(255,0,60,0.15), rgba(255,0,60,0.95), rgba(247,147,26,0.85));
              box-shadow: inset 0 0 10px rgba(255,255,255,0.15);
            }
            .hud .hint{font-size:11px; opacity:0.65; color: rgba(255,255,255,0.75);}

            #loader.is-fading{animation: fadeOut 420ms ease forwards;}
            @keyframes fadeOut{to{opacity:0; transform:scale(1.01);}}

            @media (prefers-reduced-motion: reduce){
              .grid-layer{animation:none;}
              #loader::before{opacity:0.10;}
            }
          </style>
        </head>
        <body>
        <div id="loader" data-auto-remove="true" aria-label="Loading" role="status">
          <div class="grid-layer" aria-hidden="true"></div>
          <canvas id="fx" aria-hidden="true"></canvas>

          <svg id="scene" viewBox="0 0 1000 600" preserveAspectRatio="xMidYMid meet" aria-hidden="true">
            <defs>
              <filter id="glowRed" x="-50%" y="-50%" width="200%" height="200%">
                <feGaussianBlur stdDeviation="2.8" result="b"/>
                <feMerge>
                  <feMergeNode in="b"/>
                  <feMergeNode in="SourceGraphic"/>
                </feMerge>
              </filter>
              <filter id="glowOrange" x="-60%" y="-60%" width="220%" height="220%">
                <feGaussianBlur stdDeviation="3.4" result="b"/>
                <feMerge>
                  <feMergeNode in="b"/>
                  <feMergeNode in="SourceGraphic"/>
                </feMerge>
              </filter>
              <radialGradient id="btcGrad" cx="35%" cy="30%" r="70%">
                <stop offset="0%" stop-color="#ffd08b"/>
                <stop offset="45%" stop-color="#f7a93b"/>
                <stop offset="100%" stop-color="#d67900"/>
              </radialGradient>
              <linearGradient id="metalGrad" x1="0" y1="0" x2="1" y2="0">
                <stop offset="0%" stop-color="#161616"/>
                <stop offset="50%" stop-color="#3a3a3a"/>
                <stop offset="100%" stop-color="#121212"/>
              </linearGradient>
              <radialGradient id="portalGrad" cx="50%" cy="50%" r="60%">
                <stop offset="0%" stop-color="#000000"/>
                <stop offset="65%" stop-color="#12000a"/>
                <stop offset="100%" stop-color="#ff003c" stop-opacity="0.25"/>
              </radialGradient>
            </defs>

            <g id="trash" transform="translate(500 470)" opacity="0">
              <g id="portal">
                <circle id="portalRing" r="52" fill="none" stroke="#ff003c" stroke-width="4" stroke-dasharray="10 8" filter="url(#glowRed)" opacity="0.9"/>
                <circle id="portalCore" r="38" fill="url(#portalGrad)" opacity="0.95"/>
                <text x="0" y="7" text-anchor="middle" font-size="16" font-weight="800" fill="#ff003c" style="letter-spacing:0.16em" filter="url(#glowRed)">DEL</text>
              </g>

              <g id="trashCan" transform="translate(0 6)" filter="url(#glowRed)">
                <path d="M-26 -8 H26" stroke="#ff003c" stroke-width="6" stroke-linecap="round"/>
                <path d="M-18 -8 V-18 H18 V-8" fill="none" stroke="#ff003c" stroke-width="6" stroke-linejoin="round" stroke-linecap="round"/>
                <path d="M-20 -6 L-16 34 H16 L20 -6" fill="rgba(0,0,0,0.65)" stroke="#ff003c" stroke-width="6" stroke-linejoin="round"/>
                <path d="M-8 2 V28" stroke="rgba(255,0,60,0.8)" stroke-width="3" stroke-linecap="round"/>
                <path d="M0 2 V28" stroke="rgba(255,0,60,0.8)" stroke-width="3" stroke-linecap="round"/>
                <path d="M8 2 V28" stroke="rgba(255,0,60,0.8)" stroke-width="3" stroke-linecap="round"/>
              </g>

              <circle id="trashMouth" cx="0" cy="-6" r="2" fill="rgba(255,255,255,0.0)"/>
            </g>

            <circle id="shockwave" cx="0" cy="0" r="16" fill="rgba(255,0,60,0.12)" stroke="#ff003c" stroke-width="3" opacity="0" filter="url(#glowRed)"/>

            <g id="miningGroup" transform="translate(500 280)">
              <g id="select" opacity="0">
                <rect id="selectBox" x="-165" y="-150" width="330" height="260" rx="14" fill="rgba(255,0,60,0.05)" stroke="rgba(255,0,60,0.9)" stroke-width="2" filter="url(#glowRed)"/>
                <path d="M-165 -120 v-18 h18 M165 -120 v-18 h-18 M-165 110 v18 h18 M165 110 v18 h-18" fill="none" stroke="rgba(247,147,26,0.9)" stroke-width="3" stroke-linecap="round" filter="url(#glowOrange)"/>
              </g>

              <g id="stickRoot" transform="translate(-60 25)">
                <g id="stickBase" stroke="#ff003c" stroke-width="6" stroke-linecap="round" stroke-linejoin="round" filter="url(#glowRed)">
                  <circle cx="0" cy="-78" r="18" fill="rgba(0,0,0,0.65)"/>
                  <line x1="0" y1="-60" x2="0" y2="-10" />
                  <line x1="0" y1="-38" x2="-40" y2="-10" opacity="0.75"/>
                  <line x1="0" y1="-10" x2="-28" y2="42" opacity="0.8"/>
                  <line x1="0" y1="-10" x2="28" y2="42" />
                </g>

                <g id="armRGroup" transform="translate(0 -38)">
                  <g id="armRRot" transform="rotate(-35)">

                    <g id="pickaxeHandle" transform="translate(0 0)"> 
                      <line x1="0" y1="0" x2="56" y2="0" stroke="url(#metalGrad)" stroke-width="10" stroke-linecap="round" />
                      <path d="M34 -5 L34 5 M39 -5 L39 5 M44 -5 L44 5 M49 -5 L49 5"
                            stroke="rgba(247,147,26,0.75)" stroke-width="2" stroke-linecap="round" filter="url(#glowOrange)"/>
                      <path d="M30 -10 C 26 -14, 24 -8, 28 -6" fill="none" stroke="rgba(255,255,255,0.35)" stroke-width="2" stroke-linecap="round"/>
                    </g>

                    <line x1="56" y1="0" x2="84" y2="0" stroke="#ff003c" stroke-width="8" stroke-linecap="round" filter="url(#glowRed)"/>
                    <line x1="84" y1="0" x2="112" y2="0" stroke="#ff003c" stroke-width="8" stroke-linecap="round" filter="url(#glowRed)"/>
                    <line x1="56" y1="0" x2="84" y2="0" stroke="rgba(255,255,255,0.28)" stroke-width="2.5" stroke-linecap="round"/>
                    <line x1="84" y1="0" x2="112" y2="0" stroke="rgba(255,255,255,0.28)" stroke-width="2.5" stroke-linecap="round"/>

                    <g id="handR" transform="translate(56 0)">
                      <g id="handRInner" transform="scale(1)">
                        <path d="M2 -6
                                 C 9 -10, 18 -9, 21 -2
                                 C 23 4, 20 11, 12 12
                                 C 6 13, 1 8, 0 2
                                 C -1 -3, -1 -4, 2 -6 Z"
                              fill="rgba(0,0,0,0.55)" stroke="#ff003c" stroke-width="2.5"
                              stroke-linejoin="round" filter="url(#glowRed)"/>
                        <path d="M10 -6 C 12 -12, 20 -11, 21 -5" fill="none" stroke="#ff003c" stroke-width="3" stroke-linecap="round" filter="url(#glowRed)"/>
                        <path d="M12 -2 C 14 -8, 22 -7, 23 -1" fill="none" stroke="#ff003c" stroke-width="3" stroke-linecap="round" filter="url(#glowRed)"/>
                        <path d="M12 2 C 14 -3, 22 -2, 23 4" fill="none" stroke="#ff003c" stroke-width="3" stroke-linecap="round" filter="url(#glowRed)"/>
                        <path d="M11 6 C 13 3, 20 4, 21 9" fill="none" stroke="#ff003c" stroke-width="3" stroke-linecap="round" filter="url(#glowRed)"/>
                        <path d="M3 2 C -2 3, -5 -1, -2 -5" fill="none" stroke="#ff003c" stroke-width="3" stroke-linecap="round" filter="url(#glowRed)"/>
                      </g>
                    </g>

                    <g id="pickaxe" transform="translate(56 0)">
                      <line x1="57" y1="-22" x2="57" y2="22" stroke="#111" stroke-width="12" stroke-linecap="round" />
                      <rect x="52" y="-10" width="10" height="20" rx="4"
                            fill="#111" filter="url(#glowRed)"/>
                      <path d="M57 -7 H82" fill="none" stroke="#ff003c" stroke-width="6" stroke-linecap="round" filter="url(#glowRed)"/>
                      <path d="M57 8 L80 16" fill="none" stroke="#ff003c" stroke-width="6" stroke-linecap="round" filter="url(#glowRed)"/>
                    </g>

                  </g>
                </g>
              </g>

              <g id="coin" transform="translate(50 -10)">
                <circle r="28" fill="url(#btcGrad)" stroke="#f7931a" stroke-width="4" filter="url(#glowOrange)" />
                <circle r="24" fill="none" stroke="rgba(255,255,255,0.25)" stroke-width="2" />
                <text x="0" y="12" text-anchor="middle" font-size="34" font-weight="900" fill="#fff" style="paint-order:stroke; stroke: rgba(0,0,0,0.35); stroke-width:3">₿</text>
              </g>
            </g>

            <g id="cursor" transform="translate(1040 80) scale(1.1)" opacity="0">
              <path d="M0 0 L54 34 L28 36 L36 66 L20 72 L12 40 L0 52 Z" fill="#ffffff" opacity="0.95" />
              <path d="M6 8 L44 32 L26 33 L32 56 L22 60 L16 36 L6 46 Z" fill="#121212" opacity="0.95"/>
            </g>

          </svg>

          <div class="hud">
            <div class="row"><span class="label" id="hudText">MINING CACHE…</span><span class="pct" id="hudPct">0%</span></div>
            <div class="bar" aria-hidden="true"><i id="progressBar"></i></div>
            <div class="hint"></div>
          </div>
        </div>

        <script>
        (() => {
          try {
            if (window.parent.__sheep_loader_played) {
              if (window.frameElement) window.frameElement.style.display = 'none';
              try { window.parent.__sheep_loader_finished = true; } catch(e) {}
              return;
            }
            window.parent.__sheep_loader_played = true;

            if (window.frameElement) {
              window.frameElement.style.position = 'fixed';
              window.frameElement.style.top = '0';
              window.frameElement.style.left = '0';
              window.frameElement.style.width = '100vw';
              window.frameElement.style.height = '100vh';
              window.frameElement.style.zIndex = '2147483647';
              window.frameElement.style.border = 'none';
              window.frameElement.style.background = 'transparent';
              setTimeout(() => window.dispatchEvent(new Event('resize')), 50);
            }
          } catch(err) {
            console.error('[Loader] Iframe breakout failed:', err);
          }

          const reduceMotion = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

          const loader = document.getElementById('loader');
          const svg = document.getElementById('scene');
          const canvas = document.getElementById('fx');
          const ctx = canvas.getContext('2d', { alpha: true });

          const must = (id) => {
            const n = document.getElementById(id);
            if (!n) {
              console.error('[LoaderAnim] Missing required element:', id);
              throw new Error('[LoaderAnim] Missing required element: ' + id);
            }
            return n;
          };

          const el = {
            group: must('miningGroup'),
            stickRoot: must('stickRoot'),
            armRot: must('armRRot'),
            coin: must('coin'),
            cursor: must('cursor'),
            trash: must('trash'),
            portalRing: must('portalRing'),
            shock: must('shockwave'),
            select: must('select'),
            handInner: must('handRInner'),
            hudText: must('hudText'),
            hudPct: must('hudPct'),
            progress: must('progressBar'),
          };

          const VB = { w: 1000, h: 600 };
          const CONFIG = {
            duration: 7200,
            autoRemove: loader.dataset.autoRemove === 'true',
            idleAfter: true,
          };

          const TL = Object.freeze({
            tApproach0: 900,  tApproach1: 2000,
            tGrab0: 2000,     tGrab1: 2600,
            tDrag0: 2600,     tDrag1: 3800,
            tRelease: 3800,
            tLand: 4500,
            tSwallow0: 4500,  tSwallow1: 5200,
            tShock0: 5000,    tShock1: 5600,
            tGrid0: 5400,     tGrid1: 6200,
          });

          const CALIB_STORAGE_KEY = 'loader_calib_preset_v1';

          const CALIB_DEFAULT_PRESET = {
            deleted_ids: [
              'core_path_15',
              'core_path_21','core_path_22','core_path_23','core_path_24','core_path_25','core_path_26',
            ],
            transforms: {
              portalRing: 'rotate(195.46)',
              shockwave: 'translate(500 470) scale(0.000)',
              selectBox: 'translate(0.95 14.99) rotate(0.84) scale(1.0000 1.0000)',

              core_line_14: 'translate(-2.94 2.56) rotate(0.00) scale(1.0000 1.0000)',
              core_path_16: 'translate(-143.07 88.77) rotate(0.00) scale(1.0000 1.0000)',
              core_line_17: 'translate(-9.20 1.09) rotate(0.00) scale(1.0000 1.0000)',
              core_line_18: 'translate(-25.34 2.33) rotate(0.00) scale(1.0000 1.0000)',
              core_line_19: 'translate(-12.59 40.59) rotate(321.35) scale(1.0000 1.0000)',
              core_line_20: 'translate(-12.53 36.95) rotate(323.05) scale(1.0000 1.0000)',
              core_line_27: 'translate(-17.44 6.57) rotate(329.68) scale(1.0000 1.0000)',
              core_rect_28: 'translate(-35.63 -25.73) rotate(330.39) scale(1.0000 1.0000)',
              core_path_29: 'translate(5.96 25.39) rotate(0.00) scale(1.0000 1.0000)',
              core_path_30: 'translate(-108.52 7.70) rotate(-13.24) scale(1.0000 1.0000)',
            },
            created_svg: ''
          };

          let BASE_CORE_IDS = null;
          let portalRingOffsetDeg = 0;

          const ensureDrawLayer = () => {
            const NS = 'http://www.w3.org/2000/svg';
            let dl = document.getElementById('drawLayer');
            if (!dl) {
              dl = document.createElementNS(NS, 'g');
              dl.setAttribute('id', 'drawLayer');
              svg.appendChild(dl);
            }
            return dl;
          };

          const assignDeterministicCoreIds = () => {
            let seq = 0;
            const ids = [];
            const nodes = svg.querySelectorAll('path,line,rect,circle,ellipse,polygon,polyline,text');
            for (const n of nodes) {
              if (n.closest('defs')) continue;
              if (n.closest('#editorOverlay')) continue;
              if (!n.id) {
                const tag = (n.tagName || 'el').toLowerCase();
                n.id = `core_${tag}_${(++seq)}`;
                n.setAttribute('data-core', '1');
              }
              ids.push(n.id);
            }
            return ids;
          };

          const loadCalibPreset = () => {
            try {
              const raw = localStorage.getItem(CALIB_STORAGE_KEY);
              if (!raw) return structuredClone(CALIB_DEFAULT_PRESET);

              const obj = JSON.parse(raw);
              if (!obj || typeof obj !== 'object') throw new Error('preset is not an object');

              const preset = {
                deleted_ids: Array.isArray(obj.deleted_ids) ? obj.deleted_ids : [],
                transforms: (obj.transforms && typeof obj.transforms === 'object') ? obj.transforms : {},
                created_svg: (typeof obj.created_svg === 'string') ? obj.created_svg : '',
              };
              return preset;
            } catch (e) {
              console.error('[Calib] load failed, fallback to default:', e);
              return structuredClone(CALIB_DEFAULT_PRESET);
            }
          };

          const saveCalibPreset = (preset) => {
            try {
              localStorage.setItem(CALIB_STORAGE_KEY, JSON.stringify(preset));
              console.log('[Calib] Saved preset to localStorage:', preset);
              return true;
            } catch (e) {
              console.error('[Calib] save failed:', e);
              return false;
            }
          };

          const parseRotateDeg = (s) => {
            if (!s) return null;
            const m = /rotate\(\s*([-\d.]+)/.exec(String(s));
            return m ? parseFloat(m[1]) : null;
          };

          const applyCalibPreset = (preset) => {
            try {
              const ids = assignDeterministicCoreIds();
              if (!BASE_CORE_IDS) BASE_CORE_IDS = ids.slice();

              const dl = ensureDrawLayer();

              const pr = parseRotateDeg(preset?.transforms?.portalRing);
              if (Number.isFinite(pr)) portalRingOffsetDeg = pr;

              for (const id of (preset.deleted_ids || [])) {
                const n = document.getElementById(id);
                if (n) n.remove();
                else console.error('[Calib] delete id not found:', id);
              }

              for (const [id, tr] of Object.entries(preset.transforms || {})) {
                const n = document.getElementById(id);
                if (!n) { console.error('[Calib] transform id not found:', id); continue; }
                if (!tr) n.removeAttribute('transform');
                else n.setAttribute('transform', tr);
              }

              if (typeof preset.created_svg === 'string' && preset.created_svg.trim()) {
                dl.innerHTML = preset.created_svg;
              }

              console.log('[Calib] Applied preset:', preset);
            } catch (e) {
              console.error('[Calib] apply failed:', e);
              throw e;
            }
          };

          const getDeletedTotalIds = () => {
            if (!Array.isArray(BASE_CORE_IDS)) return [];
            return BASE_CORE_IDS.filter(id => !document.getElementById(id));
          };

          const CALIB_ACTIVE = loadCalibPreset();
          applyCalibPreset(CALIB_ACTIVE);

          const clamp = (v, a, b) => Math.min(b, Math.max(a, v));
          const lerp = (a, b, t) => a + (b - a) * t;
          const mix2 = (p0, p1, t) => ({ x: lerp(p0.x, p1.x, t), y: lerp(p0.y, p1.y, t) });

          const easeOutCubic = t => 1 - Math.pow(1 - t, 3);
          const easeInCubic = t => t * t * t;
          const easeInOutCubic = t => t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
          const easeOutBack = t => {
            const c1 = 1.70158;
            const c3 = c1 + 1;
            return 1 + c3 * Math.pow(t - 1, 3) + c1 * Math.pow(t - 1, 2);
          };
          const easeInBack = t => {
            const c1 = 1.70158;
            const c3 = c1 + 1;
            return c3 * t * t * t - c1 * t * t;
          };

          const seg = (t, a, b) => clamp((t - a) / (b - a), 0, 1);
          const rad = d => d * Math.PI / 180;

          const bez3 = (p0, p1, p2, p3, t) => {
            const u = 1 - t;
            const tt = t * t;
            const uu = u * u;
            const uuu = uu * u;
            const ttt = tt * t;
            return {
              x: uuu * p0.x + 3 * uu * t * p1.x + 3 * u * tt * p2.x + ttt * p3.x,
              y: uuu * p0.y + 3 * uu * t * p1.y + 3 * u * tt * p2.y + ttt * p3.y,
            };
          };

          const geom = {
            base: { x: 500, y: 280 },
            stick: { x: -60, y: 25 },
            coinLocal: { x: 50, y: -10 },
            coinR: 28,
            grabOffset: { x: -42, y: 36 },
            trash: { x: 500, y: 470 },
            mouthLocal: { x: 0, y: -6 },
          };

          let map = { s: 1, ox: 0, oy: 0, dpr: 1 };
          function resize() {
            const dpr = Math.max(1, Math.min(2, window.devicePixelRatio || 1));
            map.dpr = dpr;
            canvas.width = Math.floor(window.innerWidth * dpr);
            canvas.height = Math.floor(window.innerHeight * dpr);
            canvas.style.width = window.innerWidth + 'px';
            canvas.style.height = window.innerHeight + 'px';
            ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

            const r = svg.getBoundingClientRect();
            const s = Math.min(r.width / VB.w, r.height / VB.h);
            const ox = r.left + (r.width - VB.w * s) / 2;
            const oy = r.top + (r.height - VB.h * s) / 2;
            map.s = s; map.ox = ox; map.oy = oy;
          }
          window.addEventListener('resize', resize, { passive: true });
          resize();

          const particles = [];
          function spawnSparks(worldX, worldY, power = 1) {
            const count = Math.floor(12 + 10 * power);
            for (let i = 0; i < count; i++) {
              const a = rad(-40 + Math.random() * 80);
              const sp = 380 + Math.random() * 520 * power;
              const life = 0.55 + Math.random() * 0.25;
              particles.push({
                x: worldX + (Math.random() * 2 - 1) * 4,
                y: worldY + (Math.random() * 2 - 1) * 4,
                vx: Math.cos(a) * sp,
                vy: Math.sin(a) * sp - 120,
                life,
                max: life,
                size: 1.2 + Math.random() * 1.8,
              });
            }
          }
          function drawParticles(dt) {
            ctx.clearRect(0, 0, window.innerWidth, window.innerHeight);
            if (!particles.length) return;

            const g = 1200;
            for (let i = particles.length - 1; i >= 0; i--) {
              const p = particles[i];
              p.vy += g * dt;
              p.x += p.vx * dt;
              p.y += p.vy * dt;
              p.life -= dt;
              if (p.life <= 0) { particles.splice(i, 1); continue; }

              const alpha = clamp(p.life / p.max, 0, 1);
              const px = p.x * map.s + map.ox;
              const py = p.y * map.s + map.oy;

              ctx.globalAlpha = alpha;
              ctx.beginPath();
              ctx.fillStyle = 'rgba(247,147,26,0.95)';
              ctx.arc(px, py, p.size, 0, Math.PI * 2);
              ctx.fill();

              ctx.globalAlpha = alpha * 0.6;
              ctx.fillStyle = 'rgba(255,255,255,0.9)';
              ctx.beginPath();
              ctx.arc(px, py, p.size * 0.45, 0, Math.PI * 2);
              ctx.fill();
            }
            ctx.globalAlpha = 1;
          }

          let running = true;
          let start = performance.now();
          let last = start;

          let groupPos = { ...geom.base };
          let groupVel = { x: 0, y: 0 };
          let groupRot = 0;
          let groupScale = 1;
          let groupOpacity = 1;

          let cursorPos = { x: 1040, y: 80 };
          let cursorOpacity = 0;
          let cursorScale = 1.1;

          let trashOpacity = 0;
          let trashScale = 0.1;

          let shockOpacity = 0;
          let shockScale = 0;

          let hitPulse = 0;
          let shake = 0;

          let impactedThisCycle = false;
          let lastCycle = -1;

          let releaseInit = false;
          let landed = false;

          function setTransform(elm, x, y, rDeg = 0, s = 1) {
            elm.setAttribute('transform', `translate(${x.toFixed(2)} ${y.toFixed(2)}) rotate(${rDeg.toFixed(2)}) scale(${s.toFixed(4)})`);
          }
          function setOpacity(elm, o) {
            elm.setAttribute('opacity', o.toFixed(4));
          }

          function miningArmAngle(phase) {
            const impactAt = 0.58;
            const up = -58;
            const down = 7;

            if (phase < impactAt) {
              const t = easeInCubic(phase / impactAt);
              return lerp(up, down, t);
            }
            const t = easeOutCubic((phase - impactAt) / (1 - impactAt));
            return lerp(down, up, t);
          }

          function worldImpactPoint(gx, gy) {
            return {
              x: gx + (geom.coinLocal.x - geom.coinR + 3),
              y: gy + geom.coinLocal.y,
            };
          }

          function updateHUD(t) {
            let pct;
            if (t < 2000) pct = lerp(8, 34, easeOutCubic(t / 2000));
            else if (t < 3800) pct = lerp(34, 72, easeInOutCubic((t - 2000) / 1800));
            else if (t < 5200) pct = lerp(72, 92, easeInOutCubic((t - 3800) / 1400));
            else if (t < CONFIG.duration) pct = lerp(92, 100, easeOutCubic((t - 5200) / (CONFIG.duration - 5200)));
            else pct = 100;

            const pctInt = Math.round(pct);
            el.hudPct.textContent = pctInt + '%';
            el.progress.style.width = pct + '%';

            if (t < 2000) el.hudText.textContent = '載入緩存...';
            else if (t < 3800) el.hudText.textContent = '鼠標覆蓋...';
            else if (t < 5200) el.hudText.textContent = '載入模型...';
            else el.hudText.textContent = '載入量化參數組合...';
          }

          function update(t, dt) {
            const {
              tApproach0, tApproach1,
              tGrab0, tGrab1,
              tDrag0, tDrag1,
              tRelease,
              tLand,
              tSwallow0, tSwallow1,
              tShock0, tShock1,
              tGrid0, tGrid1,
            } = TL;

            const pOff = { x: 1040, y: 80 };
            const pGrab = { x: 560, y: 220 };
            const pDrag = { x: 540, y: 418 };
            const pFlick = { x: 840, y: 250 };

            if (t < tApproach0) {
              cursorPos = { ...pOff };
              cursorOpacity = 0;
            } else if (t < tApproach1) {
              const u = easeOutCubic(seg(t, tApproach0, tApproach1));
              cursorPos = bez3(pOff, { x: 900, y: 120 }, { x: 700, y: 110 }, pGrab, u);
              cursorOpacity = u;
            } else if (t < tDrag1) {
              const dragU = easeInOutCubic(seg(t, tDrag0, tDrag1));
              const grabHold = easeInOutCubic(seg(t, tGrab0, tGrab1));
              const pHold = mix2(pGrab, { x: 548, y: 232 }, grabHold * 0.35);
              cursorPos = bez3(pHold, { x: 740, y: 280 }, { x: 620, y: 360 }, pDrag, dragU);
              cursorOpacity = 1;
            } else {
              const u = easeOutCubic(seg(t, tRelease, tRelease + 700));
              cursorPos = bez3(pDrag, { x: 620, y: 410 }, { x: 760, y: 300 }, pFlick, u);
              cursorOpacity = 1 - u;
            }

            const click = seg(t, tGrab0 + 80, tGrab1 - 120);
            const clickEase = easeInOutCubic(click);
            cursorScale = 1.1 - 0.22 * clickEase;

            const isMiningNow = t < tGrab0;
            const isGrabbedNow = (t >= tGrab0 && t < tRelease);

            const squeezeFromClick = isGrabbedNow ? (0.14 * clickEase) : 0;
            const squeezeFromHit = isMiningNow ? (0.10 * Math.max(0, hitPulse)) : 0;

            const handSX = 1 - clamp(squeezeFromClick + squeezeFromHit, 0, 0.18);
            const handSY = 1 + clamp((squeezeFromClick * 0.35) + (squeezeFromHit * 0.25), 0, 0.10);

            el.handInner.setAttribute('transform', `scale(${handSX.toFixed(3)} ${handSY.toFixed(3)})`);
            el.cursor.setAttribute('transform', `translate(${cursorPos.x.toFixed(2)} ${cursorPos.y.toFixed(2)}) scale(${cursorScale.toFixed(3)})`);
            setOpacity(el.cursor, cursorOpacity);

            if (t < 2400) {
              trashOpacity = 0;
              trashScale = 0.1;
            } else {
              const u = easeOutBack(seg(t, 2400, 3050));
              trashOpacity = clamp(u, 0, 1);
              trashScale = lerp(0.2, 1.0, u);
            }
            el.trash.setAttribute('transform', `translate(${geom.trash.x} ${geom.trash.y}) scale(${trashScale.toFixed(3)})`);
            setOpacity(el.trash, trashOpacity);

            const ringRot = (portalRingOffsetDeg + (t * 0.22)) % 360;
            el.portalRing.setAttribute('transform', `rotate(${ringRot.toFixed(2)})`);

            const isMining = t < tGrab0;
            const isGrabbed = t >= tGrab0 && t < tRelease;
            const isFlying = t >= tRelease && t < tLand;
            const isSwallow = t >= tSwallow0 && t < tSwallow1;

            const selIn = easeOutCubic(seg(t, tGrab0, tGrab1));
            const selOut = easeOutCubic(seg(t, tRelease, tRelease + 260));
            const selOpacity = clamp(selIn * (1 - selOut), 0, 1);
            setOpacity(el.select, selOpacity);

            let armAngle = -40;

            if (isMining) {
              const bob = Math.sin(t * 0.006) * 3;
              const sway = Math.sin(t * 0.003) * 2;
              groupPos = { x: geom.base.x + sway, y: geom.base.y + bob };
              groupVel = { x: 0, y: 0 };
              groupScale = 1;
              groupOpacity = 1;
              groupRot = Math.sin(t * 0.003) * 2;

              const cycle = 520;
              const phase = (t % cycle) / cycle;
              armAngle = miningArmAngle(phase);

              const ci = Math.floor(t / cycle);
              if (ci !== lastCycle) { lastCycle = ci; impactedThisCycle = false; }
              if (!impactedThisCycle && phase >= 0.58) {
                impactedThisCycle = true;
                hitPulse = 1;
                shake = Math.max(shake, 0.65);
                const ip = worldImpactPoint(groupPos.x, groupPos.y);
                spawnSparks(ip.x, ip.y, 0.9);
              }

            } else if (isGrabbed) {
              const grip = easeInOutCubic(seg(t, tGrab0, tGrab1));
              const target = { x: cursorPos.x + geom.grabOffset.x, y: cursorPos.y + geom.grabOffset.y };

              const k = lerp(0, 52, grip);
              const damp = lerp(18, 12, grip);

              groupVel.x += (target.x - groupPos.x) * k * dt;
              groupVel.y += (target.y - groupPos.y) * k * dt;
              const decay = Math.exp(-damp * dt);
              groupVel.x *= decay;
              groupVel.y *= decay;
              groupPos.x += groupVel.x * dt;
              groupPos.y += groupVel.y * dt;

              groupRot = clamp((-groupVel.x * 0.015) + (groupVel.y * 0.010), -18, 18);
              groupScale = lerp(1.0, 0.92, grip);
              groupOpacity = 1;

              armAngle = lerp(7, -18, grip) + clamp(-groupVel.x * 0.03, -14, 14);

            } else if (isFlying) {
              if (!releaseInit) {
                releaseInit = true;
                landed = false;

                const g = 2400;
                const dtFlight = (tLand - tRelease) / 1000;
                const target = { x: geom.trash.x, y: geom.trash.y + geom.mouthLocal.y };

                const vx = (target.x - groupPos.x) / dtFlight;
                const vy = (target.y - groupPos.y - 0.5 * g * dtFlight * dtFlight) / dtFlight;

                groupVel.x = vx;
                groupVel.y = vy;
              }

              const g = 2400;
              groupVel.y += g * dt;
              groupPos.x += groupVel.x * dt;
              groupPos.y += groupVel.y * dt;

              groupRot += clamp(groupVel.x * 0.0009, -1.2, 1.2) * 180 * dt;
              groupScale = 0.92;
              groupOpacity = 1;
              armAngle = -10 + clamp(groupVel.y * 0.01, -25, 25);

            } else if (isSwallow) {
              if (!landed) {
                landed = true;
                shake = Math.max(shake, 1.2);
                spawnSparks(geom.trash.x, geom.trash.y - 10, 1.6);
              }

              groupPos = { x: geom.trash.x, y: geom.trash.y + geom.mouthLocal.y };
              const u = easeInBack(seg(t, tSwallow0, tSwallow1));
              groupScale = lerp(0.92, 0.02, u);
              groupRot = lerp(groupRot, 540, easeOutCubic(u));
              groupOpacity = 1 - u;

              armAngle = lerp(-10, 25, easeInOutCubic(seg(t, tSwallow0, (tSwallow0 + tSwallow1) / 2)));

            } else {
              groupOpacity = 0;
              groupScale = 0.02;
            }

            hitPulse = Math.max(0, hitPulse - dt * 3.2);
            const coinSpin = (t * 0.35) % 360;
            const coinScale = 1 + hitPulse * 0.08;
            const coinTilt = Math.sin(t * 0.01) * 6 + hitPulse * 18;
            el.coin.setAttribute('transform', `translate(${geom.coinLocal.x} ${geom.coinLocal.y}) rotate(${(coinSpin + coinTilt).toFixed(2)}) scale(${coinScale.toFixed(3)})`);

            const micro = isMining ? Math.sin(t * 0.012) * 1.2 : clamp(groupVel.x * -0.04, -4, 4);
            el.stickRoot.setAttribute('transform', `translate(${geom.stick.x} ${geom.stick.y}) rotate(${micro.toFixed(2)})`);

            el.armRot.setAttribute('transform', `rotate(${armAngle.toFixed(2)})`);

            setTransform(el.group, groupPos.x, groupPos.y, groupRot, groupScale);
            setOpacity(el.group, groupOpacity);

            if (t < tShock0) {
              shockOpacity = 0;
              shockScale = 0;
            } else {
              const u = seg(t, tShock0, tShock1);
              shockOpacity = 1 - u;
              shockScale = lerp(0.2, 46, easeOutCubic(u));
            }

            el.shock.setAttribute('transform', `translate(500 470) scale(${shockScale.toFixed(3)})`);
            setOpacity(el.shock, shockOpacity);

            const gridA = easeOutCubic(seg(t, tGrid0, tGrid1));
            loader.style.setProperty('--gridA', gridA.toFixed(3));

            const flash = (t >= tShock0 && t <= tShock0 + 220) ? (1 - seg(t, tShock0, tShock0 + 220)) : 0;
            loader.style.setProperty('--flash', flash.toFixed(3));

            shake = Math.max(0, shake - dt * 2.6);
            const shakeX = (Math.random() * 2 - 1) * shake * 6;
            const shakeY = (Math.random() * 2 - 1) * shake * 5;
            svg.style.transform = `translate(${shakeX.toFixed(2)}px, ${shakeY.toFixed(2)}px)`;

            updateHUD(t);
            if (!reduceMotion) drawParticles(dt);

            if (CONFIG.autoRemove && t >= CONFIG.duration) {
              window.LoaderAnim.finish();
            }
          }

          function loop(now) {
            if (!running) return;
            const t = now - start;
            const dt = Math.min(0.033, Math.max(0.001, (now - last) / 1000));
            last = now;

            if (reduceMotion) {
              updateHUD(Math.min(t, CONFIG.duration));
              loader.style.setProperty('--gridA', '1');
              el.cursor.setAttribute('opacity', '0');
              el.trash.setAttribute('opacity', '1');
              el.trash.setAttribute('transform', `translate(${geom.trash.x} ${geom.trash.y}) scale(1)`);
              setTransform(el.group, geom.base.x, geom.base.y, 0, 1);
              setOpacity(el.group, 1);
              el.armRot.setAttribute('transform', `rotate(7)`);
              return;
            }

            update(t, dt);

            if (t >= CONFIG.duration) {
              loader.style.setProperty('--gridA', '1');
              if (!CONFIG.idleAfter) running = false;
            }

            requestAnimationFrame(loop);
          }

          window.LoaderAnim = {
            replay() {
              running = true;
              start = performance.now();
              last = start;
              releaseInit = false;
              landed = false;
              hitPulse = 0;
              shake = 0;
              impactedThisCycle = false;
              lastCycle = -1;
              particles.length = 0;
              groupPos = { ...geom.base };
              groupVel = { x: 0, y: 0 };
              groupRot = 0;
              groupScale = 1;
              groupOpacity = 1;
              cursorOpacity = 0;
              loader.classList.remove('is-fading');
              loader.style.opacity = '1';
              requestAnimationFrame(loop);
            },
            finish({ immediate = false } = {}) {
              if (immediate) {
                running = false;
                loader.remove();
                if (window.frameElement) window.frameElement.style.display = 'none';
                try { if(window.parent) window.parent.__sheep_loader_finished = true; } catch(e){}
                return;
              }
              loader.classList.add('is-fading');
              running = false;
              setTimeout(() => {
                  loader.remove();
                  if (window.frameElement) window.frameElement.style.display = 'none';
                  try { if(window.parent) window.parent.__sheep_loader_finished = true; } catch(e){}
              }, 430);
            }
          };

          let editMode = false;
          let pausedTms = 0;

          const Editor = (() => {
            const NS = 'http://www.w3.org/2000/svg';
            const body = document.body;

            let EDIT_NODES = [];
            let EDIT_NODE_BY_ID = new Map();

            const isCoreDrawable = (n) => {
              if (!n || !n.tagName) return false;
              const tag = n.tagName.toLowerCase();
              return (
                tag === 'path' || tag === 'line' || tag === 'rect' || tag === 'circle' ||
                tag === 'ellipse' || tag === 'polygon' || tag === 'polyline' || tag === 'text'
              );
            };

            const ensureIdsForCore = () => {
              let seq = 0;
              const nodes = svg.querySelectorAll('path,line,rect,circle,ellipse,polygon,polyline,text');
              for (const n of nodes) {
                if (n.closest('defs')) continue;
                if (n.closest('#editorOverlay')) continue;
                if (!n.id) {
                  const tag = (n.tagName || 'el').toLowerCase();
                  n.id = `core_${tag}_${(++seq)}`;
                  n.setAttribute('data-core', '1');
                }
              }
            };

            const indexEditableNodes = () => {
              ensureIdsForCore();
              EDIT_NODES = [];
              EDIT_NODE_BY_ID = new Map();

              const nodes = svg.querySelectorAll('path,line,rect,circle,ellipse,polygon,polyline,text');
              for (const n of nodes) {
                if (n.closest('defs')) continue;
                if (n.closest('#editorOverlay')) continue;
                if (!n.id) continue;
                EDIT_NODES.push(n);
                EDIT_NODE_BY_ID.set(n.id, n);
              }
            };

            let hud = null;
            let drawLayer = null;
            let overlay = null;
            let bboxRect = null;
            let pivotDot = null;

            let selected = null;
            let drag = null;
            let lastP = { x: 0, y: 0 };
            let lineMode = false;
            let lineStart = null;

            let uidSeq = 0;
            let deletedIds = new Set(); 

            const hardErr = (msg, extra) => {
              console.error('[LoaderEditor]', msg, extra || '');
            };

            const hardAssert = (cond, msg, extra) => {
              if (!cond) {
                hardErr(msg, extra);
                throw new Error('[LoaderEditor] ' + msg);
              }
            };

            const ensureHud = () => {
              if (hud) return hud;
              hud = document.createElement('div');
              hud.id = 'editorHud';
              hud.innerHTML = `
                <div class="t">編輯模式</div>
                <div class="b"></div>
              `;
              loader.appendChild(hud);
              return hud;
            };

            const ensureSvgLayers = () => {
              drawLayer = document.getElementById('drawLayer');
              if (!drawLayer) {
                drawLayer = document.createElementNS(NS, 'g');
                drawLayer.setAttribute('id', 'drawLayer');
                svg.appendChild(drawLayer);
              }

              overlay = document.getElementById('editorOverlay');
              if (!overlay) {
                overlay = document.createElementNS(NS, 'g');
                overlay.setAttribute('id', 'editorOverlay');
                overlay.setAttribute('opacity', '0');
                overlay.setAttribute('pointer-events', 'none'); 

                bboxRect = document.createElementNS(NS, 'rect');
                bboxRect.setAttribute('id', 'editorBBox');
                bboxRect.setAttribute('fill', 'rgba(255,0,60,0.06)');
                bboxRect.setAttribute('stroke', '#ff003c');
                bboxRect.setAttribute('stroke-width', '2');
                bboxRect.setAttribute('rx', '10');
                bboxRect.setAttribute('filter', 'url(#glowRed)');

                pivotDot = document.createElementNS(NS, 'circle');
                pivotDot.setAttribute('id', 'editorPivot');
                pivotDot.setAttribute('r', '4');
                pivotDot.setAttribute('fill', 'rgba(247,147,26,0.95)');
                pivotDot.setAttribute('filter', 'url(#glowOrange)');

                overlay.appendChild(bboxRect);
                overlay.appendChild(pivotDot);
                svg.appendChild(overlay);
              } else {
                bboxRect = document.getElementById('editorBBox');
                pivotDot = document.getElementById('editorPivot');
              }
            };

            const svgPoint = (evt) => {
              const pt = svg.createSVGPoint();
              pt.x = evt.clientX;
              pt.y = evt.clientY;
              const m = svg.getScreenCTM();
              hardAssert(m, 'getScreenCTM() returned null (SVG not in DOM?)');
              const p = pt.matrixTransform(m.inverse());
              return { x: p.x, y: p.y };
            };

            const parseTransform = (s) => {
              const out = { tx: 0, ty: 0, r: 0, sx: 1, sy: 1 };
              if (!s || typeof s !== 'string') return out;

              const t = /translate\(\s*([-\d.]+)(?:[ ,]+([-\d.]+))?\s*\)/.exec(s);
              if (t) { out.tx = parseFloat(t[1]); out.ty = parseFloat(t[2] ?? '0'); }

              const r = /rotate\(\s*([-\d.]+)\s*(?:[ ,]+([-\d.]+)[ ,]+([-\d.]+))?\s*\)/.exec(s);
              if (r) { out.r = parseFloat(r[1]); }

              const sc = /scale\(\s*([-\d.]+)(?:[ ,]+([-\d.]+))?\s*\)/.exec(s);
              if (sc) {
                out.sx = parseFloat(sc[1]);
                out.sy = parseFloat(sc[2] ?? sc[1]);
              }
              try {
                const presetForReload = {
                  deleted_ids: (typeof getDeletedTotalIds === 'function') ? getDeletedTotalIds() : [],
                  transforms: out,
                  created_svg: drawLayer ? drawLayer.innerHTML : '',
                };
                if (typeof saveCalibPreset === 'function') {
                  saveCalibPreset(presetForReload);
                }
              } catch (e) {
                hardErr('Persist preset failed', e);
              }

              return out;
            };

            const fmtTransform = (tr) => {
              const tx = Number.isFinite(tr.tx) ? tr.tx : 0;
              const ty = Number.isFinite(tr.ty) ? tr.ty : 0;
              const r  = Number.isFinite(tr.r) ? tr.r : 0;
              const sx = Number.isFinite(tr.sx) ? tr.sx : 1;
              const sy = Number.isFinite(tr.sy) ? tr.sy : 1;
              return `translate(${tx.toFixed(2)} ${ty.toFixed(2)}) rotate(${r.toFixed(2)}) scale(${sx.toFixed(4)} ${sy.toFixed(4)})`;
            };

            const isUserNode = (n) => !!(n && n.getAttribute && n.getAttribute('data-user') === '1');

            const pickEditable = (target) => {
              let n = target;
              while (n && n !== svg) {
                if (n.closest && n.closest('#editorOverlay')) return null;
                if (n.closest && n.closest('defs')) return null;

                const tag = (n.tagName || '').toLowerCase();
                if (tag === 'g') { n = n.parentNode; continue; }

                if (isUserNode(n)) return n;

                if (isCoreDrawable(n) && n.id && EDIT_NODE_BY_ID.has(n.id)) return n;

                n = n.parentNode;
              }
              return null;
            };

            const setSelected = (n) => {
              selected = n;
              const selEl = document.getElementById('edSel');
              if (selEl) selEl.textContent = selected ? (selected.id || '(no-id)') : 'none';
              updateOverlay();
            };

            const bboxInSvg = (node) => {
              const bb = node.getBBox();
              const m = node.getCTM();
              hardAssert(m, 'getCTM() returned null (node not renderable?)', node);

              const pts = [
                new DOMPoint(bb.x, bb.y),
                new DOMPoint(bb.x + bb.width, bb.y),
                new DOMPoint(bb.x, bb.y + bb.height),
                new DOMPoint(bb.x + bb.width, bb.y + bb.height),
              ].map(p => p.matrixTransform(m));

              let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
              for (const p of pts) {
                minX = Math.min(minX, p.x); minY = Math.min(minY, p.y);
                maxX = Math.max(maxX, p.x); maxY = Math.max(maxY, p.y);
              }
              return { x: minX, y: minY, w: maxX - minX, h: maxY - minY };
            };

            const updateOverlay = () => {
              if (!overlay || !bboxRect || !pivotDot) return;
              if (!selected) {
                overlay.setAttribute('opacity', '0');
                return;
              }
              try {
                const bb = bboxInSvg(selected);
                bboxRect.setAttribute('x', bb.x.toFixed(2));
                bboxRect.setAttribute('y', bb.y.toFixed(2));
                bboxRect.setAttribute('width', Math.max(0, bb.w).toFixed(2));
                bboxRect.setAttribute('height', Math.max(0, bb.h).toFixed(2));

                const tr = parseTransform(selected.getAttribute('transform'));
                const px = tr.tx || (bb.x + bb.w / 2);
                const py = tr.ty || (bb.y + bb.h / 2);
                pivotDot.setAttribute('cx', px.toFixed(2));
                pivotDot.setAttribute('cy', py.toFixed(2));

                overlay.setAttribute('opacity', '1');
              } catch (e) {
                hardErr('updateOverlay failed', e);
                overlay.setAttribute('opacity', '0');
              }
            };

            const uid = (prefix) => `${prefix}_${(++uidSeq)}_${Date.now().toString(36)}`;

            const makeEl = (tag, attrs) => {
              const n = document.createElementNS(NS, tag);
              for (const [k,v] of Object.entries(attrs || {})) n.setAttribute(k, String(v));
              n.setAttribute('data-user', '1');
              if (!n.id) n.id = uid(tag);
              return n;
            };

            const addStickman = (p) => {
              const g = makeEl('g', { transform: `translate(${p.x.toFixed(2)} ${p.y.toFixed(2)}) rotate(0) scale(1 1)` });
              g.id = uid('stick');
              g.appendChild(makeEl('circle', { cx: 0, cy: -32, r: 10, fill: 'rgba(0,0,0,0.55)', stroke: '#ff003c', 'stroke-width': 4, filter: 'url(#glowRed)' }));
              g.appendChild(makeEl('line', { x1: 0, y1: -22, x2: 0, y2: 16, stroke: '#ff003c', 'stroke-width': 4, 'stroke-linecap':'round', filter:'url(#glowRed)' }));
              g.appendChild(makeEl('line', { x1: 0, y1: -8, x2: -18, y2: 6, stroke: '#ff003c', 'stroke-width': 4, 'stroke-linecap':'round', filter:'url(#glowRed)' }));
              g.appendChild(makeEl('line', { x1: 0, y1: -8, x2: 18, y2: 6, stroke: '#ff003c', 'stroke-width': 4, 'stroke-linecap':'round', filter:'url(#glowRed)' }));
              g.appendChild(makeEl('line', { x1: 0, y1: 16, x2: -12, y2: 38, stroke: '#ff003c', 'stroke-width': 4, 'stroke-linecap':'round', filter:'url(#glowRed)' }));
              g.appendChild(makeEl('line', { x1: 0, y1: 16, x2: 12, y2: 38, stroke: '#ff003c', 'stroke-width': 4, 'stroke-linecap':'round', filter:'url(#glowRed)' }));
              drawLayer.appendChild(g);
              setSelected(g);
            };

            const addBitcoin = (p) => {
              const g = makeEl('g', { transform: `translate(${p.x.toFixed(2)} ${p.y.toFixed(2)}) rotate(0) scale(1 1)` });
              g.id = uid('btc');
              g.appendChild(makeEl('circle', { r: 22, fill: 'url(#btcGrad)', stroke: '#f7931a', 'stroke-width': 4, filter: 'url(#glowOrange)' }));
              g.appendChild(makeEl('text', { x: 0, y: 10, 'text-anchor':'middle', 'font-size': 26, 'font-weight': 900, fill:'#fff', style:'paint-order:stroke; stroke: rgba(0,0,0,0.35); stroke-width:3' })).textContent = '₿';
              drawLayer.appendChild(g);
              setSelected(g);
            };

            const addPickaxe = (p) => {
              const g = makeEl('g', { transform: `translate(${p.x.toFixed(2)} ${p.y.toFixed(2)}) rotate(0) scale(1 1)` });
              g.id = uid('pickaxe');
              g.appendChild(makeEl('line', { x1: 0, y1: 0, x2: 54, y2: 0, stroke: 'url(#metalGrad)', 'stroke-width': 10, 'stroke-linecap':'round' }));
              g.appendChild(makeEl('rect', { x: 52, y: -9, width: 10, height: 18, rx: 4, fill:'#111', filter:'url(#glowRed)' }));
              g.appendChild(makeEl('line', { x1: 57, y1: -20, x2: 57, y2: 20, stroke:'#111', 'stroke-width': 12, 'stroke-linecap':'round' }));
              g.appendChild(makeEl('path', { d:'M57 -6 H82', fill:'none', stroke:'#ff003c', 'stroke-width': 6, 'stroke-linecap':'round', filter:'url(#glowRed)' }));
              g.appendChild(makeEl('path', { d:'M57 7 L80 15', fill:'none', stroke:'#ff003c', 'stroke-width': 6, 'stroke-linecap':'round', filter:'url(#glowRed)' }));
              drawLayer.appendChild(g);
              setSelected(g);
            };

            const addLine = (a, b) => {
              const ln = makeEl('line', {
                x1: a.x.toFixed(2), y1: a.y.toFixed(2),
                x2: b.x.toFixed(2), y2: b.y.toFixed(2),
                stroke: '#ff003c', 'stroke-width': 6, 'stroke-linecap':'round', filter:'url(#glowRed)'
              });
              ln.id = uid('line');
              drawLayer.appendChild(ln);
              setSelected(ln);
            };

            const toParentLocalDelta = (node, dGlobal) => {
              try {
                const parent = node && node.parentNode;
                if (!parent || !parent.getCTM) return dGlobal;
                const pm = parent.getCTM();
                if (!pm) return dGlobal;

                const inv = pm.inverse();
                const p0 = new DOMPoint(0, 0).matrixTransform(inv);
                const p1 = new DOMPoint(dGlobal.x, dGlobal.y).matrixTransform(inv);
                return { x: p1.x - p0.x, y: p1.y - p0.y };
              } catch (e) {
                hardErr('toParentLocalDelta failed; fallback to global delta', e);
                return dGlobal;
              }
            };

            const beginDrag = (evt, node) => {
              const p = svgPoint(evt);
              lastP = p;

              const base = parseTransform(node.getAttribute('transform'));
              drag = {
                node,
                startP: p,
                start: { ...base },
                mode: evt.altKey ? 'scale' : (evt.shiftKey ? 'rotate' : 'move'),
              };

              if (drag.mode === 'rotate') {
                const bb = bboxInSvg(node);
                drag.px = bb.x + bb.w / 2;
                drag.py = bb.y + bb.h / 2;
                drag.a0 = Math.atan2(p.y - drag.py, p.x - drag.px);
              }

              evt.preventDefault();
              evt.stopPropagation();
            };

            const moveDrag = (evt) => {
              if (!drag || !drag.node) return;
              const p = svgPoint(evt);
              lastP = p;

              const dG = { x: p.x - drag.startP.x, y: p.y - drag.startP.y };
              const d = toParentLocalDelta(drag.node, dG);
              const tr = { ...drag.start };

              if (drag.mode === 'move') {
                tr.tx = drag.start.tx + d.x;
                tr.ty = drag.start.ty + d.y;
              } else if (drag.mode === 'rotate') {
                const a1 = Math.atan2(p.y - drag.py, p.x - drag.px);
                const da = (a1 - drag.a0) * 180 / Math.PI;
                tr.r = drag.start.r + da;
              } else if (drag.mode === 'scale') {
                const s = 1 + (dG.x - dG.y) / 260;
                tr.sx = Math.max(0.05, drag.start.sx * s);
                tr.sy = Math.max(0.05, drag.start.sy * s);
              }

              drag.node.setAttribute('transform', fmtTransform(tr));
              updateOverlay();
              evt.preventDefault();
              evt.stopPropagation();
            };

            const endDrag = () => {
              drag = null;
            };

            const onPointerDown = (evt) => {
              if (!editMode) return;
              try {
                const p = svgPoint(evt);
                lastP = p;

                if (lineMode) {
                  if (!lineStart) {
                    lineStart = p;
                  } else {
                    addLine(lineStart, p);
                    lineStart = null;
                    lineMode = false;
                  }
                  evt.preventDefault();
                  evt.stopPropagation();
                  return;
                }

                const n = pickEditable(evt.target);
                if (!n) return;
                setSelected(n);
                beginDrag(evt, n);
              } catch (e) {
                hardErr('pointerdown failed', e);
              }
            };

            const onPointerMove = (evt) => {
              if (!editMode) return;
              if (drag) moveDrag(evt);
              else {
                try { lastP = svgPoint(evt); } catch {}
              }
            };

            const onPointerUp = () => {
              if (!editMode) return;
              endDrag();
            };

            const deleteSelected = (forceCore = false) => {
              if (!selected) return;

              const id = selected.id || '(no-id)';
              const isCore = !isUserNode(selected);

              if (isCore && !forceCore) {
                hardErr('Refuse deleting core node (hold Shift+Delete if you insist):', id);
                return;
              }

              if (id && id !== '(no-id)') deletedIds.add(id);

              selected.remove();

              if (typeof EDIT_NODE_BY_ID?.delete === 'function') EDIT_NODE_BY_ID.delete(id);
              if (Array.isArray(EDIT_NODES)) EDIT_NODES = EDIT_NODES.filter(n => n && n.isConnected);

              setSelected(null);
            };

            const duplicateSelected = () => {
              if (!selected) return;
              if (!isUserNode(selected)) {
                return;
              }
              const clone = selected.cloneNode(true);
              clone.id = uid((selected.id || 'dup').replace(/\W+/g,'_'));
              clone.setAttribute('data-user', '1');

              const tr = parseTransform(clone.getAttribute('transform'));
              tr.tx += 14; tr.ty += 14;
              clone.setAttribute('transform', fmtTransform(tr));

              drawLayer.appendChild(clone);
              setSelected(clone);
            };

            const dumpParams = () => {
              const transforms = {};
              let moved = 0;

              const liveNodes = Array.isArray(EDIT_NODES) ? EDIT_NODES.filter(n => n && n.isConnected) : [];
              for (const n of liveNodes) {
                const tr = n.getAttribute('transform') || '';
                transforms[n.id] = tr;
                if (tr) moved++;
              }

              const created_svg = drawLayer ? drawLayer.innerHTML : '';

              const out = {
                ts: new Date().toISOString(),
                moved_count: moved,
                total_count: liveNodes.length,
                deleted_ids: Array.from(deletedIds),
                transforms,
                created_svg,
              };

              const raw = JSON.stringify(out, null, 2);

              const braceEscape = (s) => {
                if (typeof s.replaceAll === 'function') return s.replaceAll('{', '{{').replaceAll('}', '}}');
                return s.split('{').join('{{').split('}').join('}}');
              };
              const escaped = braceEscape(raw);

              window.__LOADER_EDITOR_RAW__ = raw;
              window.__LOADER_EDITOR_ESCAPED__ = escaped;

              if (navigator.clipboard && navigator.clipboard.writeText) {
                navigator.clipboard.writeText(escaped).catch(e => hardErr('Clipboard copy failed', e));
              }

              return out;
            };

            const validateBeforeExit = () => {
              const required = ['miningGroup','stickRoot','armRRot','coin','trash','cursor'];
              const missing = required.filter(id => !document.getElementById(id));
              if (missing.length) {
                hardErr('Missing required core elements:', missing);
                return false;
              }
              return true;
            };

            const enter = () => {
              ensureHud();
              ensureSvgLayers();
              indexEditableNodes(); 

              body.classList.add('edit-mode');
              overlay.setAttribute('opacity','0');
              setSelected(null);

              svg.addEventListener('pointerdown', onPointerDown, true);
              window.addEventListener('pointermove', onPointerMove, true);
              window.addEventListener('pointerup', onPointerUp, true);

              deletedIds = new Set(); 
            };

            const exit = () => {
              if (!validateBeforeExit()) return false;

              body.classList.remove('edit-mode');
              lineMode = false;
              lineStart = null;
              endDrag();

              svg.removeEventListener('pointerdown', onPointerDown, true);
              window.removeEventListener('pointermove', onPointerMove, true);
              window.removeEventListener('pointerup', onPointerUp, true);

              overlay.setAttribute('opacity','0');
              setSelected(null);

              dumpParams();
              return true;
            };

            const key = (evt) => {
              if (!editMode) return false;
              const k = evt.key.toLowerCase();

              if (k === 'l') { lineMode = !lineMode; lineStart = null; evt.preventDefault(); return true; }
              if (k === 's') { addStickman(lastP); evt.preventDefault(); return true; }
              if (k === 'p') { addPickaxe(lastP); evt.preventDefault(); return true; }
              if (k === 'b') { addBitcoin(lastP); evt.preventDefault(); return true; }

              if (k === 'delete' || k === 'backspace') {
                deleteSelected(evt.shiftKey); 
                evt.preventDefault();
                return true;
              }

              if ((evt.ctrlKey || evt.metaKey) && k === 'd') {
                duplicateSelected();
                evt.preventDefault();
                return true;
              }

              return false;
            };

            return { enter, exit, key, dumpParams };
          })();

          window.addEventListener('keydown', (e) => {
            if (e.repeat) return; 
            const k = e.key.toLowerCase();

            if (k === 'f') {
              e.preventDefault();
              e.stopPropagation();

              if (!editMode) {
                pausedTms = performance.now() - start;
                running = false;
                editMode = true;
                try { Editor.enter(); } catch (err) { console.error(err); }
              } else {
                let ok = false;
                try { ok = Editor.exit(); } catch (err) { console.error(err); }
                if (!ok) return; 

                editMode = false;
                start = performance.now() - pausedTms;
                last = performance.now();
                running = true;
                requestAnimationFrame(loop);
              }
              return;
            }

            if (editMode) {
              const consumed = Editor.key(e);
              if (consumed) return;
            }

            if (k === 'r') window.LoaderAnim.replay();
            if (k === 'x') window.LoaderAnim.finish();
          }, true);

          requestAnimationFrame(loop);
        })();
        </script>
        </body>
        </html>
        """,
        height=0,
    )
    # [專家級修正] 注入攔截器：強制將 Fivetran Webhook 等潛在阻塞的第三方請求設為非同步射後不理，瞬間釋放主渲染線程
    st.components.v1.html(
        """
        <script>
        (function() {
            try {
                const w = window.parent || window;
                if (!w._webhook_intercepted) {
                    w._webhook_intercepted = true;
                    const originalFetch = w.fetch;
                    w.fetch = async function() {
                        const url = arguments[0];
                        if (typeof url === 'string' && url.includes('webhooks.fivetran.com')) {
                            originalFetch.apply(this, arguments).catch(e => {});
                            return new Response(JSON.stringify({status: "ok"}), {status: 200, statusText: "OK"});
                        }
                        return originalFetch.apply(this, arguments);
                    };
                    const origOpen = w.XMLHttpRequest.prototype.open;
                    w.XMLHttpRequest.prototype.open = function(method, url, async) {
                        if (typeof url === 'string' && url.includes('webhooks.fivetran.com')) {
                            async = true;
                        }
                        origOpen.call(this, method, url, async);
                    };
                }
            } catch (err) {}
        })();
        </script>
        """,
        height=0,
    )

    st.components.v1.html(
        """
        <script>
        (function() {
            const w = window.parent || window;

            // 終極防線：Streamlit rerun 會重複注入這段 JS，沒有 guard 會造成 interval/observer 疊加，第二次開始直接爆慢
            if (w.__sheep_sys_menu_injected) {
                return;
            }
            w.__sheep_sys_menu_injected = true;

            const doc = w.document ? w.document : document;
            
            function isSidebarOpen() {
                try {
                    const sidebar = doc.querySelector('section[data-testid="stSidebar"]');
                    if (!sidebar) return false;
                    const left = sidebar.getBoundingClientRect().left;
                    return left >= 0;
                } catch (e) {
                    return false;
                }
            }

            function injectMenuButton() {
                try {
                    let btn = doc.getElementById('custom-sys-menu-btn');
                    if (!btn) {
                        btn = doc.createElement('div');
                        btn.id = 'custom-sys-menu-btn';
                        btn.innerHTML = '<svg viewBox="0 0 24 24"><path d="M3 18h18v-2H3v2zm0-5h18v-2H3v2zm0-7v2h18V6H3z"></path></svg>';
                        
                        btn.addEventListener('click', function(e) {
                            e.preventDefault();
                            e.stopPropagation();
                            
                            const stSidebar = doc.querySelector('section[data-testid="stSidebar"]');
                            if (!stSidebar) return;
                            
                            const isOpen = isSidebarOpen();
                            
                            stSidebar.style.removeProperty('transform');
                            stSidebar.style.removeProperty('min-width');
                            stSidebar.removeAttribute('aria-expanded');
                            
                            if (isOpen) {
                                const closeBtn = doc.querySelector('section[data-testid="stSidebar"] button[kind="headerNoPadding"]') 
                                              || doc.querySelector('section[data-testid="stSidebar"] button[aria-label="Close sidebar"]')
                                              || doc.querySelector('button[aria-label="Close sidebar"]')
                                              || doc.querySelector('section[data-testid="stSidebar"] [data-testid="baseButton-headerNoPadding"]');
                                if (closeBtn) {
                                    closeBtn.click();
                                } else {
                                    stSidebar.style.setProperty('transform', 'translateX(-100%)', 'important');
                                    stSidebar.style.setProperty('min-width', '0', 'important');
                                }
                            } else {
                                const openBtn = doc.querySelector('div[data-testid="collapsedControl"] button') 
                                             || doc.querySelector('div[data-testid="stSidebarCollapsedControl"] button') 
                                             || doc.querySelector('button[aria-label="Open sidebar"]') 
                                             || doc.querySelector('button[aria-label="View sidebar"]');
                                if (openBtn) {
                                    openBtn.click();
                                } else {
                                    stSidebar.style.setProperty('transform', 'translateX(0)', 'important');
                                    stSidebar.style.setProperty('min-width', '16rem', 'important');
                                }
                            }
                        });
                        doc.body.appendChild(btn);
                    }
                    btn.style.display = 'flex';
                } catch (err) {}
            }

            // [專家級修正] 徹底拔除消耗資源的 JS 樣式渲染與屬性監聽器，消除卡頓
            let layoutTimer = null;
            const observer = new MutationObserver(() => { 
                if (layoutTimer) return;
                layoutTimer = setTimeout(() => {
                    injectMenuButton(); 
                    layoutTimer = null;
                }, 150); // 節流
            });
            if (doc.body) {
                observer.observe(doc.body, { childList: true, subtree: true });
            }
            setInterval(injectMenuButton, 1000);
            
            if (doc.defaultView) {
                doc.defaultView.addEventListener('resize', injectMenuButton);
            }
        })();
        </script>
        """,
        height=0,
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
                # 確保管理員權限正常，避免覆蓋已修改之密碼
                conn.execute(
                    "UPDATE users SET role = 'admin', disabled = 0 WHERE id = ?",
                    (int(row["id"]),),
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
    except Exception as init_err:
        import traceback
        print(f"[CRITICAL] 資料庫初始化或管理員建立失敗: {init_err}")
        print(traceback.format_exc())


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

    # 強化密碼驗證邏輯，處理資料庫儲存為 bytes/str 以及字串化 bytes (如 "b'...'") 的潛在錯誤
    is_valid = False
    hash_stored = user.get("password_hash", "")
    
    if isinstance(hash_stored, str):
        import ast
        if hash_stored.startswith("b'") or hash_stored.startswith('b"'):
            try:
                hash_stored = ast.literal_eval(hash_stored).decode("utf-8")
            except Exception:
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
    st.session_state["nav_page_pending"] = "主頁"
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
    st.session_state["nav_page_pending"] = "主頁"
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
            st.write("以積分計算空投獎勵，明細會在結算頁看到。")

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
  const d = window.parent && window.parent.document ? window.parent.document : document;

  function findBrand() {
    return d.getElementById("sheepBrandHdr");
  }

  function bind() {
    const brand = findBrand();
    if (!brand) return;

    // Auth 頁面最穩的抓法：直接抓 stForm
    let targets = Array.from(d.querySelectorAll(".auth_scope div[data-testid='stForm']"));
    if (!targets.length) {
      targets = Array.from(d.querySelectorAll("div[data-testid='stForm']"));
    }
    if (!targets.length) return;

    let hoverCount = 0;
    function onEnter(){ hoverCount += 1; brand.classList.add("pulse"); }
    function onLeave(){ hoverCount = Math.max(0, hoverCount - 1); if (hoverCount === 0) brand.classList.remove("pulse"); }

    for (const t of targets) {
      t.addEventListener("mouseenter", onEnter, { passive: true });
      t.addEventListener("mouseleave", onLeave, { passive: true });
      t.addEventListener("focusin", onEnter, { passive: true });
      t.addEventListener("focusout", onLeave, { passive: true });
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
def _perf_init() -> None:
    if "_perf_ms" not in st.session_state or not isinstance(st.session_state.get("_perf_ms"), dict):
        st.session_state["_perf_ms"] = {}
    st.session_state["_perf_t0"] = time.perf_counter()

def _perf_mark(name: str) -> float:
    try:
        return float(time.perf_counter())
    except Exception:
        return 0.0

def _perf_add(name: str, t0: float) -> None:
    try:
        ms = (time.perf_counter() - float(t0)) * 1000.0
        st.session_state["_perf_ms"][str(name)] = round(float(ms), 3)
    except Exception:
        pass

def _perf_emit_payload() -> None:
    try:
        pm = st.session_state.get("_perf_ms") or {}
        pm["server_rerun_ms"] = round((time.perf_counter() - float(st.session_state.get("_perf_t0") or time.perf_counter())) * 1000.0, 3)
        payload = json.dumps(pm, ensure_ascii=False)
        st.components.v1.html(
            f"""
<script>
(function() {{
  const w = window.parent || window;
  try {{
    w.__sheep_perf_payload = {payload};
    w.__sheep_perf_payload_ts = Date.now();
  }} catch(e) {{}}
}})();
</script>
            """,
            height=0,
        )
    except Exception:
        pass

def _perf_hud_bootstrap_once() -> None:
    st.components.v1.html(
        """
<script>
(function() {
  const w = window.parent || window;
  const d = w.document || document;

  if (w.__sheep_perf_hud_inited) return;
  w.__sheep_perf_hud_inited = true;

  const style = d.createElement('style');
  style.textContent = `
#sheepPerfHud{
  position: fixed;
  right: 14px;
  bottom: 14px;
  z-index: 2147483647;
  background: rgba(0,0,0,0.56);
  border: 1px solid rgba(255,255,255,0.14);
  border-radius: 12px;
  padding: 10px 12px;
  color: rgba(255,255,255,0.86);
  font-size: 12px;
  line-height: 1.55;
  max-width: 360px;
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
  box-shadow: 0 10px 30px rgba(0,0,0,0.55);
  opacity: 0.30;
  transition: opacity 0.18s ease, transform 0.18s ease;
  user-select: none;
}
#sheepPerfHud:hover{
  opacity: 1.0;
  transform: translateY(-2px);
}
#sheepPerfHud .t{
  font-weight: 900;
  letter-spacing: 0.6px;
  margin-bottom: 8px;
  color: rgba(255,255,255,0.92);
}
#sheepPerfHud .r{
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
  color: rgba(255,255,255,0.88);
  white-space: pre-line;
}
#sheepPerfHud .sep{
  height: 1px;
  background: rgba(255,255,255,0.10);
  margin: 8px 0;
}
`;
  d.head.appendChild(style);

  const el = d.createElement('div');
  el.id = 'sheepPerfHud';
  el.innerHTML = `<div class="t">PERF</div><div class="r" id="sheepPerfHudBody">boot...</div>`;
  d.body.appendChild(el);

  // client-side nav timings
  function navTimings() {
    try {
      const nav = performance.getEntriesByType('navigation')[0];
      if (!nav) return null;
      return {
        ttfb: nav.responseStart,
        dom: nav.domContentLoadedEventEnd,
        load: nav.loadEventEnd
      };
    } catch(e) { return null; }
  }

  // longtask monitoring
  let longMax = 0;
  try {
    if (w.PerformanceObserver) {
      const obs = new PerformanceObserver((list) => {
        for (const e of list.getEntries()) {
          longMax = Math.max(longMax, e.duration || 0);
        }
      });
      obs.observe({ entryTypes: ['longtask'] });
    }
  } catch(e) {}

  setInterval(() => {
    try {
      const p = w.__sheep_perf_payload || {};
      const nav = navTimings();

      const lines = [];
      if (nav) {
        lines.push(`nav_ttfb_ms: ${nav.ttfb.toFixed(1)}`);
        lines.push(`nav_dom_ms:  ${nav.dom.toFixed(1)}`);
        lines.push(`nav_load_ms: ${nav.load.toFixed(1)}`);
      }

      if (longMax > 0) {
        lines.push(`longtask_max_ms: ${longMax.toFixed(1)}`);
      }

      // server metrics (sorted)
      const keys = Object.keys(p || {}).sort();
      if (keys.length) {
        lines.push('');
        for (const k of keys) {
          lines.push(`${k}: ${p[k]}`);
        }
      }

      const body = d.getElementById('sheepPerfHudBody');
      if (body) body.textContent = lines.join('\\n') || '...';
    } catch(e) {}
  }, 500);
})();
</script>
        """,
        height=0,
    )
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


@st.cache_data(ttl=30, show_spinner=False)
def _cached_active_cycle() -> Optional[Dict[str, Any]]:
    return db.get_active_cycle()

@st.cache_data(ttl=60, show_spinner=False)
def _cached_get_setting(key: str, default_val: Any = None) -> Any:
    conn = db._conn()
    try:
        return db.get_setting(conn, key, default_val)
    finally:
        conn.close()

@st.cache_data(ttl=20, show_spinner=False)
def _cached_global_progress_snapshot(cycle_id: int) -> Dict[str, Any]:
    return db.get_global_progress_snapshot(int(cycle_id))
@st.cache_data(ttl=2, show_spinner=False)
def _cached_worker_stats_snapshot() -> Dict[str, Any]:
    try:
        return db.get_worker_stats_snapshot(window_seconds=60)
    except Exception:
        return {"active_workers": 0, "total_workers": 0, "tasks_per_min": 0.0, "fail_rate": 0.0, "workers": []}

def _render_compute_workers_panel() -> None:
    snap = _cached_worker_stats_snapshot()
    active_w = int(snap.get("active_workers") or 0)
    total_w = int(snap.get("total_workers") or 0)
    tpm = float(snap.get("tasks_per_min") or 0.0)
    fr = float(snap.get("fail_rate") or 0.0) * 100.0
    workers = list(snap.get("workers") or [])

    st.markdown(_section_title_html("Compute Worker 狀態", "顯示目前 compute 節點數、吞吐、任務完成率與節點健康狀態。", level=4), unsafe_allow_html=True)

    cols = st.columns(4)
    with cols[0]:
        st.markdown(_render_kpi("活躍節點", f"{active_w}", f"總數 {total_w}", help_text="30 秒內有回報視為活躍。"), unsafe_allow_html=True)
    with cols[1]:
        st.markdown(_render_kpi("任務/分鐘", f"{tpm:.2f}", "近 60 秒", help_text="近 60 秒完成的任務吞吐。"), unsafe_allow_html=True)
    with cols[2]:
        st.markdown(_render_kpi("失敗率", f"{fr:.2f}%", "近 60 秒", help_text="近 60 秒 task_finish_fail / (ok+fail)。"), unsafe_allow_html=True)
    with cols[3]:
        avg_cps = 0.0
        try:
            if workers:
                avg_cps = sum(float(w.get("avg_cps") or 0.0) for w in workers[:min(20, len(workers))]) / float(max(1, min(20, len(workers))))
        except Exception:
            avg_cps = 0.0
        st.markdown(_render_kpi("平均 CPS", f"{avg_cps:.1f}", "Top 20 估計", help_text="以 workers.avg_cps 的簡易均值估計吞吐。"), unsafe_allow_html=True)

    with st.expander("展開節點清單", expanded=False):
        if not workers:
            st.markdown('<div class="small-muted">目前沒有任何 worker 註冊或回報。</div>', unsafe_allow_html=True)
        else:
            rows = []
            now = _utc_now()
            for w in workers:
                last_seen = str(w.get("last_seen_at") or "")
                age_s = None
                try:
                    age_s = max(0.0, (now - _parse_iso(last_seen)).total_seconds())
                except Exception:
                    age_s = None
                rows.append({
                    "worker_id": str(w.get("worker_id") or ""),
                    "kind": str(w.get("kind") or ""),
                    "avg_cps": float(w.get("avg_cps") or 0.0),
                    "tasks_done": int(w.get("tasks_done") or 0),
                    "tasks_fail": int(w.get("tasks_fail") or 0),
                    "last_seen_s": None if age_s is None else round(float(age_s), 1),
                    "last_task_id": w.get("last_task_id"),
                    "last_error": str(w.get("last_error") or "")[:120],
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

@st.cache_data(ttl=20)
def _cached_global_paid_payout_sum(cycle_id: int) -> float:
    return float(db.get_global_paid_payout_sum_usdt(int(cycle_id)))


@st.cache_data(ttl=300)
def _cached_pool_total_combos(family: str, grid_spec_json: Any, risk_spec_json: Any) -> int:
    """計算策略池的總組合數（精準、可快取、避免展開超大列表）。"""
    try:
        fam = str(family or "").strip()
        g_raw = grid_spec_json if grid_spec_json is not None else "{}"
        r_raw = risk_spec_json if risk_spec_json is not None else "{}"

        try:
            grid_s = json.loads(g_raw) if isinstance(g_raw, str) else g_raw
        except Exception:
            grid_s = {}

        try:
            risk_s = json.loads(r_raw) if isinstance(r_raw, str) else r_raw
        except Exception:
            risk_s = {}

        # Grid 組合數
        if isinstance(grid_s, list):
            g_size = int(len(grid_s))
        else:
            if hasattr(bt, "grid_combinations_count_from_ui"):
                g_size = int(bt.grid_combinations_count_from_ui(fam, grid_s or {}))
            else:
                g_size = int(len(bt.grid_combinations_from_ui(fam, grid_s or {})))
        g_size = max(0, int(g_size))

        # Risk 組合數
        def _fr_count(a: Any, b: Any, step: Any) -> int:
            try:
                a = float(a); b = float(b); step = float(step)
            except Exception:
                return 0
            if step <= 0:
                return 0
            if b < a:
                return 0
            return int(math.floor(((b - a) / step) + 1e-12)) + 1

        r_size = 1
        if fam in ["TEMA_RSI", "LaguerreRSI_TEMA"]:
            mh_min = int((risk_s or {}).get("max_hold_min", 4))
            mh_max = int((risk_s or {}).get("max_hold_max", 80))
            mh_step = max(1, int((risk_s or {}).get("max_hold_step", 4)))
            if mh_max < mh_min:
                r_size = 0
            else:
                r_size = int(((mh_max - mh_min) // mh_step) + 1)
        else:
            if isinstance(risk_s, dict) and "tp_list" in risk_s and "sl_list" in risk_s and "max_hold_list" in risk_s:
                r_size = max(0, len(risk_s.get("tp_list") or [])) * max(0, len(risk_s.get("sl_list") or [])) * max(0, len(risk_s.get("max_hold_list") or []))
            else:
                t_s = _fr_count((risk_s or {}).get("tp_min", 0.3), (risk_s or {}).get("tp_max", 1.2), (risk_s or {}).get("tp_step", 0.1))
                s_s = _fr_count((risk_s or {}).get("sl_min", 0.3), (risk_s or {}).get("sl_max", 1.2), (risk_s or {}).get("sl_step", 0.1))
                mh_min = int((risk_s or {}).get("max_hold_min", 4))
                mh_max = int((risk_s or {}).get("max_hold_max", 80))
                mh_step = max(1, int((risk_s or {}).get("max_hold_step", 4)))
                if mh_max < mh_min:
                    m_s = 0
                else:
                    m_s = int(((mh_max - mh_min) // mh_step) + 1)
                r_size = int(t_s) * int(s_s) * int(m_s)

        return int(g_size * max(0, int(r_size)))
    except Exception as combo_err:
        import traceback
        print(f"[ERROR] _cached_pool_total_combos 發生錯誤: {combo_err}")
        print(traceback.format_exc())
        return 0


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


def _partition_map_html(tasks: List[Dict[str, Any]], num_partitions: int, system_user_id: int) -> str:
    n = int(num_partitions or 0)
    if n <= 0:
        return '<div class="small-muted">無分割資料</div>'

    # 同一分割可能存在多筆任務紀錄（斷線接手、重派等），這裡做去重，避免顯示與統計被放大
    best: Dict[int, Dict[str, Any]] = {}

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

    for t in tasks or []:
        try:
            idx = int(t.get("partition_idx") or 0)
        except Exception:
            idx = 0
        if idx < 0 or idx >= n:
            continue
        if idx not in best:
            best[idx] = t
        else:
            best[idx] = _pick_better(best[idx], t)

    cols = int(min(48, max(12, int(math.sqrt(n)) + 1)))
    cells: List[str] = []
    for idx in range(n):
        t = best.get(idx)
        if t:
            bucket = _partition_bucket(t, system_user_id)
            user_name = str(t.get("username") or "")
        else:
            bucket = "待挖掘"
            user_name = ""

        cls = {
            "已完成": "pm_done",
            "執行中": "pm_running",
            "已預訂": "pm_reserved",
            "待挖掘": "pm_available",
        }.get(bucket, "pm_other")

        title = f"分割 {idx+1}/{n} · {bucket}" + (f" · {user_name}" if user_name else "")
        cells.append(f'<div class="pm_cell {cls}" title="{html.escape(title, quote=True)}"></div>')

    return f'<div class="pm_grid" style="grid-template-columns: repeat({cols}, 10px);">{"".join(cells)}</div>'


def _render_global_progress(cycle_id: int) -> None:
    # Perf HUD：記錄全域快照耗時（ms）
    if "_perf_ms" not in st.session_state:
        st.session_state["_perf_ms"] = {}

    _t0 = time.perf_counter()
    try:
        snap = _cached_global_progress_snapshot(int(cycle_id))
    finally:
        st.session_state["_perf_ms"]["global_snapshot_ms"] = round((time.perf_counter() - _t0) * 1000.0, 3)

    system_uid = int(snap.get("system_user_id") or 0)
    pools_all = list(snap.get("pools") or [])
    if not pools_all:
        st.markdown('<div class="small-muted">目前沒有可用的進度資料。</div>', unsafe_allow_html=True)
        return

    families = sorted({str(p.get("family") or "").strip() for p in pools_all if str(p.get("family") or "").strip()})
    family_opts = ["全部策略"] + families
    sel_family = st.selectbox("查看策略全域進度", options=family_opts, index=0, key=f"gp_family_{int(cycle_id)}")

    pools = pools_all
    if sel_family != "全部策略":
        pools = [p for p in pools_all if str(p.get("family") or "").strip() == sel_family]

    # ── 聚合（以「分割」為單位，而不是以「目前有幾筆 mining_tasks」為單位）
    # 重要：任務表可能只有被領取過的分割才會出現資料列，所以統計一定要補上「尚未出現的分割」。
    total_true = 0
    total_done = 0
    total_running = 0
    total_completed = 0
    total_reserved = 0

    pool_rows: List[Dict[str, Any]] = []
    recs: List[Dict[str, Any]] = []

    # 為了後面顯示分割分佈，只保留 pool_id -> (pool, num_parts, tasks)
    pools_for_map: List[Dict[str, Any]] = []

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
        pool_id = int(p.get("id") or p.get("pool_id") or 0)
        pool_name = str(p.get("name") or p.get("pool_name") or "") or f"Pool {pool_id}"
        fam = str(p.get("family") or "").strip()
        num_parts = max(1, int(p.get("num_partitions") or 1))

        # 精準總量：完全由策略/風控規格推導，不依賴 mining_tasks 是否存在
        pool_total = int(_cached_pool_total_combos(fam, p.get("grid_spec_json"), p.get("risk_spec_json")))
        pool_total = max(0, int(pool_total))

        # 去重後的任務：同一 partition_idx 可能有多筆紀錄（重派/接手），只取最合理的那筆
        tasks = list(p.get("tasks") or [])
        best: Dict[int, Dict[str, Any]] = {}
        for t in tasks:
            try:
                idx = int(t.get("partition_idx") or 0)
            except Exception:
                idx = 0
            if idx < 0 or idx >= num_parts:
                continue
            if idx not in best:
                best[idx] = t
            else:
                best[idx] = _pick_better(best[idx], t)

        # 進度 done（只算每個分割的最佳紀錄一次）
        done_sum = 0
        known_total_sum = 0  # 已回報 combos_total 的分割總量
        bucket_known: Dict[str, float] = {}
        bucket_unknown_parts: Dict[str, int] = {}

        bucket_counts: Dict[str, int] = {"已完成": 0, "執行中": 0, "已預訂": 0, "待挖掘": 0, "其他": 0}

        for idx, t in best.items():
            bucket = _partition_bucket(t, system_uid)
            bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1

            prog = t.get("progress") or t.get("progress_json") or {}
            if isinstance(prog, str):
                try:
                    prog = json.loads(prog)
                except Exception:
                    prog = {}

            try:
                done_i = int(float(prog.get("combos_done") or 0))
            except Exception:
                done_i = 0
            if done_i < 0:
                done_i = 0

            try:
                rep_total = float(prog.get("combos_total") or 0.0)
            except Exception:
                rep_total = 0.0

            if rep_total > 0:
                known_total_sum += float(rep_total)
                bucket_known[bucket] = bucket_known.get(bucket, 0.0) + float(rep_total)
                if done_i > int(rep_total):
                    done_i = int(rep_total)
            else:
                bucket_unknown_parts[bucket] = bucket_unknown_parts.get(bucket, 0) + 1

            done_sum += int(done_i)

        # 尚未被領取過的分割，一律視為「待挖掘」
        missing = max(0, int(num_parts - len(best)))
        if missing > 0:
            bucket_counts["待挖掘"] = bucket_counts.get("待挖掘", 0) + missing
            bucket_unknown_parts["待挖掘"] = bucket_unknown_parts.get("待挖掘", 0) + missing

        # 將未回報 combos_total 的分割，用「剩餘總量」按分割數量比例分配，確保總量精準等於 pool_total
        remaining = float(max(0, pool_total)) - float(max(0.0, known_total_sum))
        if remaining < 0:
            remaining = 0.0
        unk_parts_total = int(sum(bucket_unknown_parts.values()))
        bucket_est: Dict[str, float] = {}
        for b, v in bucket_known.items():
            bucket_est[b] = float(bucket_est.get(b, 0.0) + float(v))
        if unk_parts_total > 0 and remaining > 0:
            for b, n_unk in bucket_unknown_parts.items():
                bucket_est[b] = float(bucket_est.get(b, 0.0) + (remaining * (float(n_unk) / float(unk_parts_total))))

        # 若全部分割都回報 combos_total，就 bucket_est 可能為空：補上 bucket_known 以外的 0
        for b in ["已完成", "執行中", "已預訂", "待挖掘"]:
            bucket_est.setdefault(b, 0.0)

        # Pool KPI
        pool_ratio = (float(done_sum) / float(pool_total)) if pool_total > 0 else 0.0
        if pool_ratio < 0:
            pool_ratio = 0.0
        if pool_ratio > 1:
            pool_ratio = 1.0

        total_true += int(pool_total)
        total_done += int(done_sum)

        total_running += int(bucket_counts.get("執行中", 0))
        total_completed += int(bucket_counts.get("已完成", 0))
        total_reserved += int(bucket_counts.get("已預訂", 0))

        pool_rows.append(
            {
                "策略池": pool_name,
                "標的": str(p.get("symbol") or ""),
                "週期": f"{int(p.get('timeframe_min') or 0)}m",
                "策略": fam,
                "分割數": int(num_parts),
                "進度": f"{pool_ratio*100:.1f}%",
                "執行中": int(bucket_counts.get("執行中", 0)),
                "已完成": int(bucket_counts.get("已完成", 0)),
                "待挖掘": int(bucket_counts.get("待挖掘", 0)),
            }
        )

        pools_for_map.append({"pool_id": pool_id, "pool_name": pool_name, "meta": f"{p.get('symbol')} · {p.get('timeframe_min')}m · {fam}", "num_partitions": num_parts, "tasks": tasks})

    ratio = (float(total_done) / float(total_true)) if total_true > 0 else 0.0
    if ratio < 0: ratio = 0.0
    if ratio > 1: ratio = 1.0

    try:
        paid_sum = float(_cached_global_paid_payout_sum(int(cycle_id)) or 0.0)
    except Exception:
        paid_sum = 0.0

    st.markdown(_section_title_html("全域挖掘進度", "統計全部用戶已跑組合數、任務完成數與全域進度。", level=4), unsafe_allow_html=True)
    
    # 進度條與 KPI 卡片視覺設定
    st.progress(float(ratio))
    
    kcols = st.columns(4)
    with kcols[0]:
        st.markdown(_render_kpi("已計算參數組合", f"{int(total_done):,}", f"總量 {int(total_true):,}", help_text="累計所有用戶已測試的參數組合數。"), unsafe_allow_html=True)
    with kcols[1]:
        st.markdown(_render_kpi("任務完成進度", f"{ratio*100:.1f}%", f"範圍：{sel_family}", help_text="基於已跑組合數與總量的完成比例。"), unsafe_allow_html=True)
    with kcols[2]:
        st.markdown(_render_kpi("活躍任務數", f"{int(total_running):,}", f"已完成 {int(total_completed):,}", help_text="全球目前正在執行中的任務數量。"), unsafe_allow_html=True)
    with kcols[3]:
        st.markdown(_render_kpi("累計已發放獎勵", f"{paid_sum:,.2f}", "USDT", help_text="本週期已發放給用戶的獎勵總額。"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    
    # 將策略池概覽與分割分佈收納進 Expander，避免干擾主要視覺
    with st.expander("展開查看詳細策略池狀態與分割分佈", expanded=False):
        if pool_rows:
            st.dataframe(pd.DataFrame(pool_rows), use_container_width=True, hide_index=True)
            
        st.markdown("##### 分割熱力圖")
        if pools_for_map:
            labels = [f'{x["pool_name"]}（{x["meta"]}）' for x in pools_for_map]
            sel = st.selectbox("選擇策略池", options=list(range(len(labels))), format_func=lambda i: labels[i], index=0, key=f"pm_sel_{int(cycle_id)}_{sel_family}")
            chosen = pools_for_map[int(sel)]
            st.markdown(_partition_map_html(chosen["tasks"], int(chosen["num_partitions"]), system_uid), unsafe_allow_html=True)
            st.markdown(
                '<div class="pm_legend">'
                '<span class="pm_cell pm_available" style="display:inline-block; margin-right:4px;"></span>待挖掘&nbsp;&nbsp;'
                '<span class="pm_cell pm_reserved" style="display:inline-block; margin-right:4px;"></span>已預訂&nbsp;&nbsp;'
                '<span class="pm_cell pm_running" style="display:inline-block; margin-right:4px;"></span>執行中&nbsp;&nbsp;'
                '<span class="pm_cell pm_done" style="display:inline-block; margin-right:4px;"></span>已完成'
                '</div>',
                unsafe_allow_html=True,
            )
def _page_home(user: Optional[Dict[str, Any]] = None) -> None:
    st.components.v1.html(
        """
        <!DOCTYPE html>
        <html>
        <head>
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&display=swap');
        body { margin: 0; padding: 0; background-color: transparent; font-family: 'Space Mono', monospace; overflow: hidden; user-select: none; }
        
        .hero {
            position: relative; width: 100%; height: 260px;
            background: rgba(10, 0, 2, 0.6); border: 1px solid #33000b; border-left: 4px solid #FF003C;
            border-radius: 6px; box-sizing: border-box; display: flex; flex-direction: column;
            justify-content: center; padding-left: 40px; overflow: hidden; box-shadow: inset 0 0 50px rgba(255,0,60,0.05);
        }
        
        /* 漂浮代碼 */
        .code-float { position: absolute; font-size: 11px; color: rgba(255,0,60,0.15); font-weight: bold; white-space: nowrap; pointer-events: none; z-index: 1; text-shadow: 0 0 5px rgba(255,0,60,0.2); }
        .c1 { top: 15%; left: -10%; animation: floatR 20s linear infinite; }
        .c2 { top: 60%; left: 15%; animation: floatL 18s linear infinite reverse; }
        .c3 { top: 80%; left: 40%; animation: floatR 25s linear infinite; }
        @keyframes floatR { 100% { transform: translateX(250px); } }
        @keyframes floatL { 100% { transform: translateX(-250px); } }

        /* 動態滑鼠探照燈 */
        .spotlight {
            position: absolute; top: 0; left: 0; width: 100%; height: 100%;
            background: radial-gradient(circle 0px at 50% 50%, rgba(255,0,60,0.15) 0%, transparent 100%);
            pointer-events: none; z-index: 2; transition: background 0.15s ease-out;
        }

        .content { z-index: 10; position: relative; }

        /* RGB 色散標題 */
        .glitch-title {
            font-size: 54px; font-weight: 700; color: #ffffff; margin: 0 0 20px 0;
            letter-spacing: -2px; position: relative; display: inline-block;
            text-shadow: 0 0 15px rgba(255,255,255,0.1); transition: transform 0.1s;
        }
        .glitch-title:hover, .glitch-title.force-glitch {
            cursor: crosshair; transform: skewX(-6deg);
            text-shadow: 4px 0 #FF003C, -4px 0 #00FFCC;
            animation: shake 0.2s infinite;
        }
        .g-dot { color: #FF003C; transition: text-shadow 0.2s; }
        .glitch-title:hover .g-dot, .glitch-title.force-glitch .g-dot { text-shadow: 0 0 20px #FF003C, 0 0 40px #FF003C; }
        @keyframes shake {
            0%, 100% { transform: translate(0,0) skewX(-6deg); }
            25% { transform: translate(-2px,1px) skewX(-6deg); }
            50% { transform: translate(2px,-1px) skewX(-6deg); }
            75% { transform: translate(-1px,2px) skewX(-6deg); }
        }

        /* 3D 物理翻轉牌 */
        .row { display: flex; gap: 24px; flex-wrap: wrap; }
        .flip-box { width: 240px; height: 50px; perspective: 1000px; cursor: crosshair; }
        .flip-inner {
            position: relative; width: 100%; height: 100%;
            transition: transform 0.6s cubic-bezier(0.34, 1.56, 0.64, 1); /* 極端阻尼彈簧感 */
            transform-style: preserve-3d;
        }
        .flip-box:hover .flip-inner { transform: rotateX(180deg); }
        
        .face {
            position: absolute; width: 100%; height: 100%; backface-visibility: hidden;
            display: flex; align-items: center; justify-content: center; border-radius: 2px;
        }
        .front {
            background: rgba(10,2,4,0.9); border: 1px solid #3d0a16; color: #ff8a9f;
            font-size: 15px; font-weight: bold; letter-spacing: 1px;
            box-shadow: inset 0 0 15px rgba(0,0,0,0.9); transition: border-color 0.3s, color 0.3s;
        }
        .front::before {
            content: '>_'; color: #FF003C; margin-right: 10px; font-weight: 900;
            animation: blink 1s step-end infinite;
        }
        .flip-box:hover .front { border-color: #FF003C; color: #fff; }
        
        .back {
            background: #ffffff; color: #000000; transform: rotateX(180deg);
            font-family: "PingFang SC", "Microsoft YaHei", sans-serif;
            font-size: 17px; font-weight: 900; letter-spacing: 4px;
            border: 2px solid #FF003C; box-shadow: 0 0 25px rgba(255,0,60,0.35);
        }
        @keyframes blink { 50% { opacity: 0; } }
        </style>
        </head>
        <body>
            <div class="hero" id="cyber-hero">
                <div class="spotlight" id="spotlight"></div>
                
                <div class="code-float c1">@njit(fastmath=True, nogil=True)</div>
                <div class="code-float c2">await ws.send_json({"type": "EXEC", "payload": 0x9A})</div>
                <div class="code-float c3">df['RSI'] = ta.momentum.rsi(window=14)</div>
                
                <div class="content">
                    <div class="glitch-title">量化挖礦系統<span class="g-dot">.</span></div>
                    <div class="row">
                        <div class="flip-box">
                            <div class="flip-inner">
                                <div class="face front">OPEN_THE_MINE</div>
                                <div class="face back">解鎖無盡算力</div>
                            </div>
                        </div>
                        <div class="flip-box">
                            <div class="flip-inner">
                                <div class="face front">MINE_THE_NODE</div>
                                <div class="face back">共鑄量化節點</div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <script>
                const hero = document.getElementById('cyber-hero');
                const spot = document.getElementById('spotlight');
                let ticking = false;
                hero.addEventListener('mousemove', (e) => {
                    if (!ticking) {
                        window.requestAnimationFrame(() => {
                            const rect = hero.getBoundingClientRect();
                            const x = e.clientX - rect.left;
                            const y = e.clientY - rect.top;
                            spot.style.background = `radial-gradient(circle 200px at ${x}px ${y}px, rgba(255,0,60,0.18) 0%, transparent 100%)`;
                            ticking = false;
                        });
                        ticking = true;
                    }
                });
                hero.addEventListener('mouseleave', () => {
                    spot.style.background = `radial-gradient(circle 0px at 50% 50%, rgba(255,0,60,0.15) 0%, transparent 100%)`;
                });

                function startAutoAnimation() {
                    const title = document.querySelector('.glitch-title');
                    const inners = document.querySelectorAll('.flip-inner');
                    
                    if(title) title.classList.add('force-glitch');
                    
                    inners.forEach(el => {
                        el.style.transition = 'transform 1.2s cubic-bezier(0.2, 0.8, 0.2, 1)';
                        // 1260deg 代表旋轉三圈半，會剛好停在反面(中文)
                        el.style.transform = 'rotateX(1260deg)';
                    });

                    setTimeout(() => {
                        if(title) title.classList.remove('force-glitch');
                        inners.forEach(el => {
                            el.style.transition = 'transform 0.6s cubic-bezier(0.34, 1.56, 0.64, 1)';
                            // 1440deg 代表完整四圈，轉回正面(英文)
                            el.style.transform = 'rotateX(1440deg)';
                        });
                        
                        // 等待動畫結束後，清空行內屬性恢復原本的 hover CSS 互動
                        setTimeout(() => {
                            inners.forEach(el => {
                                el.style.transition = 'none';
                                el.style.transform = 'rotateX(0deg)';
                                void el.offsetHeight;
                                el.style.transition = '';
                                el.style.transform = '';
                            });
                        }, 600);
                    }, 2000);
                }

                const w = window.parent || window;
                if (w.__sheep_loader_finished) {
                    if (!w.__sheep_home_anim_played) {
                        w.__sheep_home_anim_played = true;
                        setTimeout(startAutoAnimation, 300);
                    }
                } else {
                    let pollTimer = setInterval(() => {
                        if (w.__sheep_loader_finished) {
                            clearInterval(pollTimer);
                            if (!w.__sheep_home_anim_played) {
                                w.__sheep_home_anim_played = true;
                                setTimeout(startAutoAnimation, 100);
                            }
                        }
                    }, 500);
                }
            </script>
        </body>
        </html>
        """,
        height=280,
        scrolling=False
    )

def _page_tutorial(user: Optional[Dict[str, Any]] = None) -> None:
    st.markdown('<div class="sec_h3">新手教學與操作手冊</div>', unsafe_allow_html=True)
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
.flow_badge{width:28px;height:28px;border-radius:10px;display:flex;align-items:center;justify-content:center;border:1px solid rgba(255,0,60,0.45);background:rgba(255,0,60,0.12);font-weight:800;font-size:12px;}
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
        cycle = _cached_active_cycle()
        if not cycle or "id" not in cycle:
            st.warning("週期尚未初始化，系統正在嘗試建立新週期。")
            db.ensure_cycle_rollover()
            _cached_active_cycle.clear()
            cycle = _cached_active_cycle()
            if not cycle:
                st.error("週期建立失敗，請通知系統管理員檢查資料庫權限。")
                return

        pools = db.list_factor_pools(cycle_id=int(cycle["id"])) if cycle else []

        st.markdown(_section_title_html("控制台", "查看你的任務、策略與結算概況。此頁也提供全域挖礦進度與策略池狀態。", level=3), unsafe_allow_html=True)
        # compute worker 面板（只有 admin 顯示）
        try:
            if str(user.get("role") or "") == "admin":
                _render_compute_workers_panel()
        except Exception:
            pass
        # Ensure tasks quota (使用快取)
        min_tasks = int(_cached_get_setting("min_tasks_per_user", 2))
            
        try:
            # 節流任務分配，避免每次畫面重新整理都狂刷資料庫 (15秒冷卻)
            _dash_assign_key = f"dash_assign_{user['id']}"
            _now = time.time()
            if _now - st.session_state.get(_dash_assign_key, 0) > 15:
                db.assign_tasks_for_user(int(user["id"]), cycle_id=int(cycle["id"]), min_tasks=min_tasks)
                st.session_state[_dash_assign_key] = _now
        except AttributeError as ae:
            st.error(f"系統核心函數遺失。")
            with st.expander("詳細錯誤資訊", expanded=True):
                import traceback
                st.code(traceback.format_exc(), language="python")
            return
        except Exception as general_e:
            st.error(f"分配任務時發生未預期錯誤。")
            with st.expander("詳細錯誤資訊", expanded=True):
                import traceback
                st.code(traceback.format_exc(), language="python")
            return

        _tq0 = _perf_mark("tasks_query_ms")
        tasks = db.list_tasks_for_user(int(user["id"]), cycle_id=int(cycle["id"]))
        _perf_add("tasks_query_ms", _tq0)
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
        st.error("控制台頁面發生異常。")
        with st.expander("錯誤追蹤紀錄 (Traceback)", expanded=True):
            import traceback
            st.code(traceback.format_exc(), language="python")
        return

def _page_tasks(user: Dict[str, Any], job_mgr: JobManager) -> None:
    cycle = _cached_active_cycle()
    if not cycle:
        st.error("週期未初始化。")
        return

    min_tasks = int(_cached_get_setting("min_tasks_per_user", 2))
    max_tasks = int(_cached_get_setting("max_tasks_per_user", 6))
    max_concurrent_jobs = int(_cached_get_setting("max_concurrent_jobs", 2))
    min_trades = int(_cached_get_setting("min_trades", 40))
    min_total_return_pct = float(_cached_get_setting("min_total_return_pct", 15.0))
    max_drawdown_pct = float(_cached_get_setting("max_drawdown_pct", 25.0))
    min_sharpe = float(_cached_get_setting("min_sharpe", 0.6))
    exec_mode = str(_cached_get_setting("execution_mode", "server") or "server").strip().lower()
    api_url = str(_cached_get_setting("worker_api_url", "http://127.0.0.1:8001") or "http://127.0.0.1:8001").strip()

    if exec_mode not in ("server", "worker"):
        exec_mode = "server"

    st.markdown("### 任務")

    pools_meta = db.list_factor_pools(int(cycle["id"]))
    fams = sorted({str(p.get("family") or "").strip() for p in pools_meta if str(p.get("family") or "").strip()})
    fam_opts = ["全部策略"] + fams
    sel_family = st.selectbox("策略", options=fam_opts, index=0, key=f"task_family_{int(cycle['id'])}")

    allowed_pool_ids: List[int] = []
    _task_assign_key = f"task_assign_{user['id']}_{sel_family}"
    _now = time.time()
    
    if sel_family != "全部策略":
        allowed_pool_ids = [int(p.get("id") or 0) for p in pools_meta if str(p.get("family") or "").strip() == sel_family and int(p.get("id") or 0) > 0]
        # 節流任務分配，避免每次畫面重新整理都狂刷資料庫 (15秒冷卻)
        if _now - st.session_state.get(_task_assign_key, 0) > 15:
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
            st.session_state[_task_assign_key] = _now
    else:
        if _now - st.session_state.get(_task_assign_key, 0) > 15:
            db.assign_tasks_for_user(int(user["id"]), cycle_id=int(cycle["id"]), min_tasks=int(min_tasks), max_tasks=int(max_tasks))
            st.session_state[_task_assign_key] = _now

    _tq0 = _perf_mark("tasks_live_query_ms")
    tasks = db.list_tasks_for_user(int(user["id"]), cycle_id=int(cycle["id"]))
    _perf_add("tasks_live_query_ms", _tq0)
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
        run_enabled = db.get_user_run_enabled(int(user["id"]))
        run_key = f"server_run_all_{int(user['id'])}"
        run_all = bool(run_enabled)
        st.session_state[run_key] = run_all

        with col_a:
            if not run_all:
                # [UI 強化] 使用 primary 顏色突顯開始按鈕
                if st.button("開始挖礦", key="start_all", type="primary"):
                    db.set_user_run_enabled(int(user["id"]), True)
                    st.session_state[run_key] = True
                    run_all = True
                    to_queue: List[int] = []
                    for t in tasks:
                        tid = int(t["id"])
                        st_raw = str(t.get("status") or "")
                        
                        # 擴大可排程狀態，包含 queued 與意外中止的 running
                        if st_raw not in ("assigned", "queued", "error", "running"):
                            continue
                        if job_mgr.is_running(tid):
                            continue
                        if job_mgr.is_queued(int(user["id"]), tid):
                            continue
                            
                        # 處理卡在 running 狀態但實際上未執行的任務，重置為 assigned
                        if st_raw == "running":
                            try:
                                db.update_task_status(tid, "assigned")
                            except Exception:
                                pass
                        elif st_raw == "error":
                            # 若之前發生錯誤，重新排程時初始化狀態
                            try:
                                db.update_task_status(tid, "assigned")
                            except Exception:
                                pass
                                
                        to_queue.append(tid)
                    
                    if to_queue:
                        # 將任務加入排程列隊
                        result = job_mgr.enqueue_many(int(user["id"]), to_queue, bt)
                        
                        # 同步更新任務狀態與進度，確保介面即時反映排隊狀態
                        for qid in to_queue:
                            try:
                                db.update_task_status(qid, "queued")
                                trow = db.get_task(int(qid)) or {}
                                try:
                                    prog0 = json.loads(trow.get("progress_json") or "{}")
                                except Exception:
                                    prog0 = {}
                                if not isinstance(prog0, dict):
                                    prog0 = {}

                                prog0["phase"] = "queued"
                                prog0["phase_msg"] = "調度器：已寫入佇列等待運算資源分配..."
                                prog0["updated_at"] = _iso(_utc_now())

                                db.update_task_progress(qid, prog0)
                            except Exception:
                                pass
                        
                        db.write_audit_log(
                            int(user["id"]),
                            "task_queue_all",
                            {"queued": int(result.get("queued") or 0), "skipped": int(result.get("skipped") or 0)},
                        )
                        st.toast(f"已成功排程 {len(to_queue)} 個任務")
                    else:
                        st.toast("目前無可執行的任務")
                    
                    time.sleep(0.5)
                    st.rerun()
            else:
                if st.button("中斷全域運算配置", key="stop_all"):
                    db.set_user_run_enabled(int(user["id"]), False)
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
                if st.button("啟動客戶端節點分配", key="worker_enable", type="primary"):
                    db.set_user_run_enabled(int(user["id"]), True)
                    st.rerun()
            else:
                if st.button("暫停客戶端節點分配", key="worker_disable"):
                    db.set_user_run_enabled(int(user["id"]), False)
                    st.rerun()

        with col_b:
            st.markdown(
                f'<div class="small-muted">節點狀態：{"存取授權中" if run_enabled else "連線靜置"}</div>',
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

    # 自動銜接模式：啟用後會自動將新的 assigned 任務排入隊列
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
                    for qid in to_queue2:
                        try:
                            db.update_task_status(qid, "queued")
                            trow = db.get_task(int(qid)) or {}
                            try:
                                prog0 = json.loads(trow.get("progress_json") or "{}")
                            except Exception:
                                prog0 = {}
                            if not isinstance(prog0, dict):
                                prog0 = {}

                            prog0["phase"] = "queued"
                            prog0["phase_msg"] = "持續性部署：程序已掛載至等待序列..."
                            prog0["updated_at"] = _iso(_utc_now())

                            db.update_task_progress(qid, prog0)
                        except Exception:
                            pass
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

    st.markdown('''
        <style>
        /* 任務檢視選單按鈕美化樣式 */
        div[data-testid="stRadio"] > label { display: none !important; }
        div[data-testid="stRadio"] div[role="radiogroup"] {
            display: flex !important;
            flex-direction: column !important;
            gap: 8px !important;
            background: transparent !important;
            margin-left: 50px !important;
            width: calc(100% - 50px) !important;
        }
        div[data-testid="stRadio"] div[role="radiogroup"] label {
            background: #000000 !important;
            border: 1px solid #333333 !important;
            border-radius: 10px !important;
            padding: 16px 20px !important;
            margin: 0 !important;
            cursor: pointer !important;
            transition: all 0.2s ease !important;
            width: 100% !important;
        }
        div[data-testid="stRadio"] div[role="radiogroup"] label:hover {
            background: rgba(128, 128, 128, 0.1) !important;
        }
        div[data-testid="stRadio"] div[role="radiogroup"] label:has(input:checked) {
            background: rgba(128, 128, 128, 0.2) !important;
            border-color: #666666 !important;
        }
        div[data-testid="stRadio"] div[role="radiogroup"] label div[data-testid="stMarkdownContainer"] p {
            color: #94a3b8 !important;
            font-weight: 600 !important;
            font-size: 16px !important;
            margin: 0 !important;
            text-align: center !important;
        }
        div[data-testid="stRadio"] div[role="radiogroup"] label:has(input:checked) div[data-testid="stMarkdownContainer"] p {
            color: #ffffff !important;
        }
        div[data-testid="stRadio"] div[role="radiogroup"] label > div:first-child {
            display: none !important;
        }
        </style>
    ''', unsafe_allow_html=True)

    content_col, space_col, menu_col = st.columns([3.5, 0.3, 1.2])
    with menu_col:
        st.markdown('<div style="font-size:18px; font-weight:700; color:#f8fafc; margin-bottom:16px; text-align:center;">任務檢視</div>', unsafe_allow_html=True)
        view_mode = st.radio("選擇檢視", ["執行中的任務", "歷史紀錄", "候選與審核"], label_visibility="collapsed", key="task_view_mode_radio")

    live_tasks = db.list_tasks_for_user(int(user["id"]), cycle_id=int(cycle["id"]))
    if sel_family != "全部策略" and allowed_pool_ids:
        live_tasks = [t for t in live_tasks if int(t.get("pool_id") or 0) in set(allowed_pool_ids)]

    def _sort_task_priority(task_item: Dict[str, Any]) -> tuple:
        st_val = str(task_item.get("status") or "")
        priority = {"running": 0, "queued": 1, "assigned": 2, "error": 3, "expired": 4, "revoked": 5, "completed": 6}
        return (priority.get(st_val, 9), -int(task_item.get("id") or 0))

    live_tasks = sorted(live_tasks, key=_sort_task_priority)
    active_tasks = []
    history_tasks = []

    for t in live_tasks:
        st_raw = str(t.get("status") or "")
        _tid = int(t["id"])
        view_status = st_raw
        if exec_mode == "server":
            if job_mgr.is_running(_tid):
                view_status = "running"
            elif job_mgr.is_queued(int(user["id"]), _tid) and st_raw == "assigned":
                view_status = "queued"

        if view_status in ("running", "queued", "assigned"):
            active_tasks.append((t, view_status))
        else:
            history_tasks.append((t, view_status))

    any_active_local = len(active_tasks) > 0
    completed_tasks = [t for t in history_tasks if t[1] == "completed"]

    def _render_single_task(task_obj: Dict[str, Any], v_status: str, is_active: bool):
        t_id = int(task_obj["id"])
        try:
            prog = json.loads(task_obj.get("progress_json") or "{}")
        except Exception:
            prog = {}

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

        ret_pct, dd_pct, sharpe, trades_count = None, None, None, None
        if best_any_metrics:
            try:
                ret_pct = float(best_any_metrics.get("total_return_pct", 0))
                dd_pct = float(best_any_metrics.get("max_drawdown_pct", 0))
                sharpe = float(best_any_metrics.get("sharpe", 0))
                trades_count = int(best_any_metrics.get("trades", 0))
            except Exception:
                pass

        st.markdown('<div class="panel" style="margin-bottom: 16px;">', unsafe_allow_html=True)
        
        status_map = {
            "assigned": "配置完畢",
            "queued": "排程等待",
            "running": "演算執行",
            "completed": "程序完成",
            "expired": "許可逾期",
            "revoked": "權限撤銷",
            "error": "程序異常"
        }
        phase_map = {
            "idle": "待命",
            "sync_data": "同步資料",
            "build_grid": "準備參數",
            "grid_search": "計算中",
            "stopped": "已停止",
            "error": "發生錯誤",
        }
        status_label = status_map.get(v_status, v_status.upper())
        phase_label = phase_map.get(phase, phase.upper())
        passed_label = "條件符合" if best_any_passed else "未達閾值"

        def _pill_cls(kind: str) -> str:
            if kind in ("completed",): return "neutral"
            if kind in ("running",): return "neutral"
            if kind in ("queued", "assigned"): return "warn"
            if kind in ("expired", "revoked", "error"): return "bad"
            return "neutral"

        st.markdown(f'<div style="display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:12px;">'
                    f'<div>'
                    f'<div style="font-size:18px; font-weight:800; color:#f8fafc; margin-bottom:4px;">編號{t_id}</div>'
                    f'</div>'
                    f'<div style="display:flex; gap:8px; flex-direction:column; align-items:flex-end;">'
                    f'<span class="pill pill-{_pill_cls(v_status)}">狀態: {status_label}</span>'
                    f'<span class="pill pill-{"ok" if best_any_passed else "neutral"}">檢驗: {passed_label}</span>'
                    f'</div>'
                    f'</div>', unsafe_allow_html=True)

        hb = task_obj.get("last_heartbeat")
        if hb:
            try:
                age_s = max(0.0, (_utc_now() - _parse_iso(str(hb))).total_seconds())
                st.markdown(f'<div class="small-muted" style="margin-top:-8px; margin-bottom:12px;">節點延遲: {age_s:.1f}s</div>', unsafe_allow_html=True)
            except Exception:
                pass

        phase_color = "#94a3b8"
        is_anim = False
        
        if phase == "queued":
            phase_color = "#f59e0b"
            is_anim = True
        elif phase == "sync_data":
            phase_color = "#94a3b8"
            is_anim = True
        elif phase == "build_grid":
            phase_color = "#94a3b8"
            is_anim = True
        elif phase == "grid_search":
            phase_color = "#10b981"
            is_anim = True
        elif phase == "error" or v_status == "error":
            phase_color = "#ef4444"

        if is_anim:
            icon_html = f'<div style="width: 14px; height: 14px; border: 2px solid {phase_color}30; border-top: 2px solid {phase_color}; border-radius: 50%; animation: custom-spin 0.8s linear infinite;"></div>'
        else:
            icon_html = f'<div style="width: 8px; height: 8px; border-radius: 50%; background-color: {phase_color};"></div>'
        
        st.markdown(
            f"""
            <style>
            @keyframes custom-spin {{
                0% {{ transform: rotate(0deg); }}
                100% {{ transform: rotate(360deg); }}
            }}
            </style>
            <div style="background: rgba(15,23,42,0.4); border: 1px solid {phase_color}40; border-left: 3px solid {phase_color}; border-radius: 8px; padding: 12px 16px; margin-bottom: 16px;">
                <div style="display: flex; align-items: center; gap: 8px;">
                    {icon_html}
                    <span style="color: {phase_color}; font-weight: 700; font-size: 14px; letter-spacing:0.5px;">程序階段：{phase_label}</span>
                </div>
                <div style="margin-top: 6px; font-size: 13px; color: #94a3b8;">
                    {phase_msg if phase_msg else '資源整備中...'}
                </div>
            </div>
            """, unsafe_allow_html=True
        )
        

        top_b, top_c, top_d = st.columns([1.5, 1.5, 1.5])
        with top_b:
            prog_text = "-"
            sync = prog.get("sync")
            if combos_total > 0:
                prog_text = f"{int(combos_done)} / {int(combos_total)}"
            elif phase == "sync_data" and isinstance(sync, dict):
                items = sync.get("items")
                cur = str(sync.get("current") or "")
                if isinstance(items, dict) and cur in items:
                    done_i = int(items[cur].get("done", 0))
                    total_i = int(items[cur].get("total", 0))
                    if total_i > 0:
                        prog_text = f"{cur} {done_i}/{total_i}"
            st.markdown(f'<div class="small-muted">運算進度</div><div style="font-size:20px; font-weight:700; color:#f8fafc; font-family:monospace;">{prog_text}</div>', unsafe_allow_html=True)
        with top_c:
            elapsed_s = prog.get("elapsed_s")
            es = "-" if elapsed_s is None else f"{float(elapsed_s):.1f}s"
            st.markdown(f'<div class="small-muted">花費時間</div><div style="font-size:20px; font-weight:700; color:#f8fafc; font-family:monospace;">{es}</div>', unsafe_allow_html=True)
        with top_d:
            sc_txt = "-" if best_any_score is None else f"{float(best_any_score):.6f}"
            st.markdown(f'<div class="small-muted">目前最高分</div><div style="font-size:20px; font-weight:700; color:#10b981; font-family:monospace;">{sc_txt}</div>', unsafe_allow_html=True)

        if phase == "grid_search":
            speed_cps = prog.get("speed_cps")
            eta_s = prog.get("eta_s")
            sp = "-" if speed_cps is None else f"{float(speed_cps):.0f} iter/s"
            et = "-" if eta_s is None else f"{float(eta_s):.1f}s"
            st.markdown(f'<div style="background:rgba(255,255,255,0.02); padding:6px 12px; border-radius:6px; margin-top:8px; font-size:12px; color:#94a3b8; display:flex; justify-content:space-between; border: 1px solid rgba(255,255,255,0.05);">'
                        f'<span>運算速度: <span style="color:#60a5fa; font-family:monospace;">{sp}</span></span>'
                        f'<span>預估剩餘時間: <span style="color:#fbbf24; font-family:monospace;">{et}</span></span>'
                        f'</div>', unsafe_allow_html=True)

        if last_error:
            st.error(f"核心防護觸發：\n\n{last_error}")
            if prog.get("debug_traceback"):
                with st.expander("展開記憶體傾印 (Traceback)"):
                    st.code(prog.get("debug_traceback"), language="python") 

        sync = prog.get("sync")
        if combos_total > 0:
            st.progress(min(1.0, float(combos_done) / float(combos_total)))
        elif phase == "sync_data":
            items = sync.get("items") if isinstance(sync, dict) else None
            if isinstance(items, dict) and items:
                order = []
                if "1m" in items: order.append("1m")
                cur = str(sync.get("current") or "") if isinstance(sync, dict) else ""
                if cur and cur in items and cur not in order: order.append(cur)
                for k in sorted(items.keys()):
                    if k not in order: order.append(k)
                for k in order:
                    try:
                        d = int(items[k].get("done") or 0)
                        tot = int(items[k].get("total") or 0)
                    except Exception:
                        d, tot = 0, 0
                    if tot > 0:
                        st.markdown(f'<div class="small-muted">資料處理 {k}：{d}/{tot}</div>', unsafe_allow_html=True)
                        st.progress(min(1.0, float(d) / float(tot)))
        elif isinstance(phase_progress, (int, float)):
            st.progress(float(phase_progress))

        grid_a, grid_b = st.columns([1.3, 1.0])
        with grid_a:
            rows = [
                {"項目": "交易次數", "當前數值": "-" if trades_count is None else int(trades_count), "下限要求": int(min_trades), "偏差值": _fmt_gap_min(float(trades_count) if trades_count is not None else None, float(min_trades))},
                {"項目": "預期報酬", "當前數值": "-" if ret_pct is None else round(float(ret_pct), 4), "下限要求": float(min_total_return_pct), "偏差值": _fmt_gap_min(ret_pct, float(min_total_return_pct))},
                {"項目": "最大回撤", "當前數值": "-" if dd_pct is None else round(float(dd_pct), 4), "上限要求": float(max_drawdown_pct), "偏差值": _fmt_gap_max(dd_pct, float(max_drawdown_pct))},
                {"項目": "夏普值", "當前數值": "-" if sharpe is None else round(float(sharpe), 4), "下限要求": float(min_sharpe), "偏差值": _fmt_gap_min(sharpe, float(min_sharpe))}
            ]
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        with grid_b:
            if exec_mode == "server":
                col_btn1, col_btn2 = st.columns([1, 1])
                with col_btn1:
                    if v_status == "assigned":
                        if st.button("配置資源", key=f"start_now_{t_id}", use_container_width=True):
                            ok = job_mgr.start(t_id, bt)
                            if not ok:
                                job_mgr.enqueue_many(int(user["id"]), [t_id], bt)
                            st.rerun()
                    elif v_status == "queued":
                        st.markdown('<div style="text-align:center; padding:8px; border:1px solid #334155; border-radius:6px; color:#cbd5e1; font-size:14px;">序列佇列中</div>', unsafe_allow_html=True)
                    elif v_status == "running":
                        st.markdown('<div style="text-align:center; padding:8px; border:1px solid #10b981; border-radius:6px; color:#34d399; font-size:14px;">模組執行中</div>', unsafe_allow_html=True)
                with col_btn2:
                    if v_status == "assigned":
                        if st.button("加入叢集", key=f"queue_{t_id}", use_container_width=True):
                            job_mgr.enqueue_many(int(user["id"]), [t_id], bt)
                            st.rerun()
                    if v_status == "running":
                        if st.button("釋放資源", key=f"stop_{t_id}", use_container_width=True):
                            job_mgr.stop(t_id)
                            st.rerun()
                st.markdown(f'<div style="text-align:right; font-size:12px; color:#64748b; margin-top:8px;">節點並行上限: {int(max_concurrent_jobs)}</div>', unsafe_allow_html=True)
            else:
                st.caption("客戶端運算節點模式，接受排程分配。")

        if best_any_params:
            with st.expander("最佳參數", expanded=False):
                st.json(best_any_params)

        st.markdown('</div>', unsafe_allow_html=True)

    # 準備定時更新的 Fragment 裝飾器
    fragment_decorator = getattr(st, "fragment", None)
    if fragment_decorator:
        try:
            refresh_interval = float(os.environ.get("SHEEP_TASKS_REFRESH_S", "2.0"))
            refresh_interval = max(1.0, min(10.0, refresh_interval))
        except Exception:
            refresh_interval = 2.0
        fragment_decorator = fragment_decorator(run_every=timedelta(seconds=refresh_interval))
    else:
        def fragment_decorator(func): return func

    @fragment_decorator
    def _render_active_tasks_live():
        # 關鍵修復：fragment 不能用外層算好的 active_tasks（那是第一次的快照）
        # 必須每次 run 都重抓 DB，否則你看到的 combos_total/combos_done 永遠不會變，逼你整頁 reload
        live_tasks2 = db.list_tasks_for_user(int(user["id"]), cycle_id=int(cycle["id"]))
        if sel_family != "全部策略" and allowed_pool_ids:
            live_tasks2 = [t for t in live_tasks2 if int(t.get("pool_id") or 0) in set(allowed_pool_ids)]

        live_tasks2 = sorted(live_tasks2, key=_sort_task_priority)

        active2 = []
        for t in live_tasks2:
            st_raw = str(t.get("status") or "")
            _tid = int(t["id"])
            view_status = st_raw
            if exec_mode == "server":
                if job_mgr.is_running(_tid):
                    view_status = "running"
                elif job_mgr.is_queued(int(user["id"]), _tid) and st_raw == "assigned":
                    view_status = "queued"

            if view_status in ("running", "queued", "assigned"):
                active2.append((t, view_status))

        if active2:
            for t_obj, v_stat in active2:
                _render_single_task(t_obj, v_stat, True)
        else:
            st.markdown("""
            <div style="text-align:center; padding: 40px 20px; background: rgba(255,255,255,0.02); border-radius: 12px; border: 1px dashed rgba(255,255,255,0.05);">
                <div style="color: #94a3b8; font-size: 14px; font-weight: 600;">系統目前未偵測到執行中的任務</div>
                <div style="color: #64748b; font-size: 12px; margin-top: 4px;">請於上方控制面板分配運算資源或啟動全域佇列</div>
            </div>
            """, unsafe_allow_html=True)

    if view_mode == "執行中的任務":
        with content_col:
            _render_active_tasks_live()
        any_active = any_active_local
    else:
        with content_col:
            if view_mode == "歷史紀錄":
                if history_tasks:
                    for t_obj, v_stat in history_tasks:
                        _render_single_task(t_obj, v_stat, False)
                else:
                    st.markdown("""
                    <div style="text-align:center; padding: 40px 20px; background: rgba(255,255,255,0.02); border-radius: 12px; border: 1px dashed rgba(255,255,255,0.05);">
                        <div style="color: #64748b; font-size: 14px;">暫無終端執行紀錄</div>
                    </div>
                    """, unsafe_allow_html=True)
            elif view_mode == "候選與審核":
                _render_all_candidates_and_audit(user, completed_tasks)
        
        # 不在執行中頁面時，關閉外部 JS 的 AutoRefresh，確保不再閃爍
        any_active = False

    if not getattr(st, "fragment", None):
        keep_polling = False
        if exec_mode == "server":
            run_key = f"server_run_all_{int(user['id'])}"
            keep_polling = bool(st.session_state.get(run_key, False))
        else:
            keep_polling = bool(run_enabled)

        # 修正縮排確保排程觸發，並於缺乏 fragment 支援時使用 JS 刷新
        if auto_refresh and (any_active or keep_polling):
            try:
                base_s = float(refresh_s)
            except Exception:
                base_s = 1.0

            if not any_active:
                base_s = max(base_s, 3.0)

            base_s = float(min(30.0, max(0.8, base_s)))
            interval_ms = int(base_s * 1000)

            try:
                interval_ms = int(interval_ms + random.randint(0, 250))
            except Exception:
                pass

            # 放置隱藏按鈕作為刷新觸發點
            if st.button("AutoRefreshHiddenBtn", key="hidden_refresh_btn", use_container_width=False):
                pass

            st.components.v1.html(
                f"""
<script>
(function() {{
  try {{
    const w = window.parent || window;
    const ms = Math.max(1000, Math.min(60000, {interval_ms}));

    const ps = w.document.querySelectorAll('button p, button div');
    let targetBtn = null;
    ps.forEach(p => {{
        if (p.textContent && p.textContent.trim() === 'AutoRefreshHiddenBtn') {{
            targetBtn = p.closest('button');
            if (targetBtn) {{
                targetBtn.style.opacity = '0';
                targetBtn.style.position = 'absolute';
                targetBtn.style.width = '1px';
                targetBtn.style.height = '1px';
                targetBtn.style.pointerEvents = 'none';
                targetBtn.style.overflow = 'hidden';
            }}
        }}
    }});

    if (w.__sheep_autorefresh_timer) {{
      clearTimeout(w.__sheep_autorefresh_timer);
    }}

    w.__sheep_autorefresh_timer = setTimeout(function() {{
      try {{
        if (document.hidden) return;
        
        const activeEl = w.document.activeElement;
        if (activeEl && (activeEl.tagName === 'INPUT' || activeEl.tagName === 'TEXTAREA' || activeEl.tagName === 'SELECT')) {{
            return;
        }}
        
        if (targetBtn && typeof targetBtn.click === 'function') {{
            targetBtn.click();
        }}
      }} catch (e) {{
        console.warn('AutoRefresh error', e);
      }}
    }}, ms);
  }} catch (e) {{
    console.warn('AutoRefresh init error', e);
  }}
}})();
</script>
                """,
                height=0,
            )


@st.cache_data(ttl=120, show_spinner=False)
def _cached_list_candidates(task_id: int) -> List[Dict[str, Any]]:
    return db.list_candidates(task_id, limit=50)

@st.cache_data(ttl=300, show_spinner=False)
def _cached_get_pool(pool_id: int) -> Dict[str, Any]:
    return db.get_pool(pool_id)

def _render_all_candidates_and_audit(user: Dict[str, Any], completed_tasks: List[Tuple[Dict[str, Any], str]]) -> None:
    st.markdown('<div class="sec_h4">候選結果與自動過擬合審核</div>', unsafe_allow_html=True)
    
    all_cands = []
    for t_obj, _ in completed_tasks:
        try:
            task_id = int(t_obj["id"])
            cands = _cached_list_candidates(task_id)
            if cands:
                for c in cands:
                    all_cands.append((t_obj, c))
        except Exception as e:
            st.error(f"讀取任務 {t_obj.get('id')} 候選時發生錯誤: {e}")
            
    if not all_cands:
        st.info("目前無任何候選結果。")
        return
        
    min_trades = int(_cached_get_setting("min_trades", 40))
    min_sharpe = float(_cached_get_setting("min_sharpe", 0.6))
    max_drawdown = float(_cached_get_setting("max_drawdown_pct", 25.0))
    min_oos_return = 0.0
    min_fw_return = 0.0
    min_sharpe_oos = max(0.0, min_sharpe * 0.5)
    max_dd_oos = max_drawdown

    un_audited = [ (t, c) for t, c in all_cands if f"audit_result_{c['id']}" not in st.session_state ]
    
    if un_audited:
        progress_text = "自動執行過擬合審核中..."
        my_bar = st.progress(0, text=progress_text)
        total = len(un_audited)
        for idx, (t_obj, c) in enumerate(un_audited):
            try:
                pool_id = int(t_obj["pool_id"])
                pool = _cached_get_pool(pool_id)
                params = c.get("params_json") or {}
                if pool and params:
                    audit = _run_audit_for_candidate(pool, params, min_trades=min_trades, min_oos_return=min_oos_return, min_fw_return=min_fw_return, min_sharpe_oos=min_sharpe_oos, max_dd_oos=max_dd_oos)
                    st.session_state[f"audit_result_{c['id']}"] = audit
                else:
                    st.session_state[f"audit_result_{c['id']}"] = {"passed": False, "error": "參數或策略池無效"}
            except Exception as e:
                st.session_state[f"audit_result_{c['id']}"] = {"passed": False, "error": str(e)}
            my_bar.progress((idx + 1) / total, text=f"審核進度: {idx + 1}/{total}")
            time.sleep(0.005) # 釋放 GIL，防止審核迴圈霸佔導致其他用戶 WebSocket 逾時
        my_bar.empty()
        
    passed_cands = []
    failed_cands = []
    
    for t_obj, c in all_cands:
        audit = st.session_state.get(f"audit_result_{c['id']}")
        if audit and audit.get("passed"):
            passed_cands.append((t_obj, c, audit))
        else:
            failed_cands.append((t_obj, c, audit))
            
    tab1, tab2 = st.tabs(["通過審核 (可提交)", "未通過審核"])
    
    with tab1:
        if not passed_cands:
            st.info("無通過審核的候選結果。")
        else:
            rows = []
            for t_obj, c, audit in passed_cands:
                m = c.get("metrics") or {}
                rows.append({
                    "候選編號": c["id"],
                    "任務ID": t_obj["id"],
                    "策略池": t_obj.get("pool_name", ""),
                    "分數": round(float(c.get("score") or 0.0), 6),
                    "總報酬(%)": round(float(m.get("total_return_pct") or 0.0), 4),
                    "最大回撤(%)": round(float(m.get("max_drawdown_pct") or 0.0), 4),
                    "夏普": round(float(m.get("sharpe") or 0.0), 4),
                    "交易次數": int(m.get("trades") or 0),
                    "提交狀態": "已提交" if int(c.get("is_submitted") or 0) == 1 else "未提交"
                })
            df_passed = pd.DataFrame(rows)
            st.dataframe(df_passed, use_container_width=True, hide_index=True)
            
            unsubmitted = [r for r in rows if r["提交狀態"] == "未提交"]
            if unsubmitted:
                st.markdown("#### 提交候選結果")
                c_id_to_submit = st.selectbox("選擇要提交的候選編號", [r["候選編號"] for r in unsubmitted])
                if st.button("確認提交", key="btn_submit_passed"):
                    c_info = next((item for item in passed_cands if item[1]["id"] == c_id_to_submit), None)
                    if c_info:
                        t_obj, c, audit = c_info
                        pool = _cached_get_pool(int(t_obj["pool_id"]))
                        try:
                            sid = db.create_submission(candidate_id=int(c["id"]), user_id=int(user["id"]), pool_id=int(pool["id"]), audit=audit)
                            db.write_audit_log(int(user["id"]), "submit", {"candidate_id": int(c["id"]), "submission_id": int(sid)})
                            st.success(f"候選編號 {c['id']} 已成功提交！")
                            st.rerun()
                        except Exception as e:
                            st.error(f"提交失敗: {e}")
                            
    with tab2:
        if not failed_cands:
            st.info("無未通過審核的候選結果。")
        else:
            rows = []
            for t_obj, c, audit in failed_cands:
                m = c.get("metrics") or {}
                if audit:
                    reason = ", ".join(audit.get("reasons", [])) if audit.get("reasons") else audit.get("error", "條件未達標")
                else:
                    reason = "未知錯誤"
                rows.append({
                    "候選編號": c["id"],
                    "任務ID": t_obj["id"],
                    "策略池": t_obj.get("pool_name", ""),
                    "分數": round(float(c.get("score") or 0.0), 6),
                    "總報酬(%)": round(float(m.get("total_return_pct") or 0.0), 4),
                    "最大回撤(%)": round(float(m.get("max_drawdown_pct") or 0.0), 4),
                    "交易次數": int(m.get("trades") or 0),
                    "未通過原因": reason
                })
            df_failed = pd.DataFrame(rows)
            st.dataframe(df_failed, use_container_width=True, hide_index=True)


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
@st.cache_data(ttl=60, show_spinner=False)
def _cached_leaderboard_stats(period_hours: int) -> Dict[str, Any]:
    return db.get_leaderboard_stats(period_hours=period_hours)

def _page_leaderboard(user: Dict[str, Any]) -> None:
    st.markdown(
        """
        <div style="background: linear-gradient(135deg, rgba(255,215,0,0.1) 0%, rgba(255,140,0,0.05) 100%); 
                    border: 1px solid rgba(255, 215, 0, 0.3); 
                    border-radius: 12px; 
                    padding: 20px 24px; 
                    margin-bottom: 24px;
                    box-shadow: 0 8px 32px rgba(255, 215, 0, 0.05);
                    display: flex; justify-content: space-between; align-items: center;">
            <div style="display: flex; align-items: center; gap: 16px;">
                <div style="width: 48px; height: 48px; background: linear-gradient(135deg, #FFD700 0%, #FF8C00 100%); border-radius: 12px; display: flex; align-items: center; justify-content: center; box-shadow: 0 4px 16px rgba(255, 215, 0, 0.4);">
                    <span style="font-size: 24px; font-weight: bold; color: #fff;">1</span>
                </div>
                <div>
                    <div style="display: flex; align-items: center;">
                        <h2 style="margin: 0; padding: 0; font-size: 28px; font-weight: 900; background: linear-gradient(135deg, #FFD700 0%, #FFFFFF 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; letter-spacing: 1px;">排行榜</h2>
                        """ + _help_icon_html("此區塊展示全平台數據統計與排名。分為算力貢獻、積分收益、單次最高分與累積掛機時長。排名前列者將獲得專屬自訂稱號與相關權限。") + """
                    </div>
                    <div style="font-size: 13px; color: #94a3b8; margin-top: 4px;">展示頂尖貢獻者與數據紀錄。數據每分鐘更新一次。</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True
    )

    # 1. 徹底拋棄原生 Radio 紅點：注入頂級 Segmented Control CSS 模擬器
    st.markdown('''
        <style>
        /* 隱藏原生 Radio 按鈕及其圓點 */
        div[data-testid="stRadio"] > label { display: none !important; }
        div[data-testid="stRadio"] div[role="radiogroup"] {
            display: flex !important;
            flex-direction: row !important;
            gap: 12px !important;
            background: rgba(15, 23, 42, 0.5) !important;
            padding: 8px !important;
            border-radius: 16px !important;
            border: 1px solid rgba(255, 255, 255, 0.05) !important;
            width: fit-content !important;
            margin-bottom: 15px !important;
        }
        div[data-testid="stRadio"] div[role="radiogroup"] label {
            background: #000000 !important;
            border: 1px solid #333333 !important;
            border-radius: 10px !important;
            padding: 10px 24px !important;
            cursor: pointer !important;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
        }
        div[data-testid="stRadio"] div[role="radiogroup"] label:hover {
            background: rgba(128, 128, 128, 0.1) !important;
            border-color: #555555 !important;
            transform: translateY(-2px);
        }
        div[data-testid="stRadio"] div[role="radiogroup"] label:has(input:checked) {
            background: rgba(128, 128, 128, 0.2) !important;
            border-color: #666666 !important;
            box-shadow: none !important;
            transform: translateY(-2px);
        }
        div[data-testid="stRadio"] div[role="radiogroup"] label div[data-testid="stMarkdownContainer"] p {
            color: #94a3b8 !important;
            font-weight: 600 !important;
            font-size: 15px !important;
            margin: 0 !important;
        }
        div[data-testid="stRadio"] div[role="radiogroup"] label:has(input:checked) div[data-testid="stMarkdownContainer"] p {
            color: #ffffff !important;
            text-shadow: none !important;
        }
        div[data-testid="stRadio"] div[role="radiogroup"] label > div:first-child {
            display: none !important; /* 強制消滅原生選取圓圈 */
        }
        </style>
    ''', unsafe_allow_html=True)
    
    period_map = {"小時榜": 1, "日榜": 24, "月榜": 720}
    period_label = st.radio("統計週期", list(period_map.keys()), index=1, horizontal=True, key="lb_period", label_visibility="collapsed")
    
    period_hours = period_map[period_label]

    try:
        data = _cached_leaderboard_stats(period_hours=period_hours)
    except Exception as e:
        st.error(f"排行榜資料讀取錯誤：{e}")
        import traceback
        st.code(traceback.format_exc(), language="python")
        return

    # 2. 檢查當前用戶是否在 "30天 - 組合數" 前 5 名
    can_set_nickname = False
    my_rank_info = ""
    
    if period_hours == 720:
        combos_list = data.get("combos", [])
        for idx, row in enumerate(combos_list):
            if row.get("username") == user["username"]:
                rank = idx + 1
                my_rank_info = f"（目前排名：第 {rank} 名）"
                if rank <= 5:
                    can_set_nickname = True
                break
    
    # 3. 暱稱設定區塊
    if can_set_nickname:
        st.markdown(
            """
            <div class="nick-card">
                <div style="font-size:20px; font-weight:800; color:#FFD700; margin-bottom:12px; display:flex; align-items:center;">
                    <span class="crown-icon"></span>權限已解鎖
                </div>
                <div style="font-size:15px; color:#cbd5e1; line-height:1.6;">
                    您是本月算力貢獻前 5 名。您現在可以設定專屬暱稱。
                </div>
            </div>
            """, unsafe_allow_html=True
        )
        col_n1, col_n2 = st.columns([3, 1])
        with col_n1:
            # 增加一些 padding 和 placeholder
            new_nick = st.text_input("設定新暱稱", value=user.get("nickname", ""), max_chars=10, label_visibility="collapsed", placeholder="在此輸入您的稱號...")
        with col_n2:
            if st.button("更新稱號", type="primary", use_container_width=True):
                safe_nick = html.escape(new_nick.strip())
                if safe_nick:
                    db.update_user_nickname(int(user["id"]), safe_nick)
                    user["nickname"] = safe_nick # Update session cache
                    db.write_audit_log(int(user["id"]), "update_nickname", {"nickname": safe_nick})
                    st.toast("稱號已更新。")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.warning("稱號不可為空")
    elif period_hours == 720:
        st.info(f"提示：月度算力榜前 5 名即可解鎖自訂暱稱功能。{my_rank_info}")

    # 4. 排行榜 HTML 渲染器 (修復 HTML 外洩問題)
    def _render_html_table(rows: list, val_col: str, val_fmt: str, unit: str):
        if not rows:
            st.markdown('<div class="panel" style="text-align:center; color:#64748b; padding:40px; font-size:14px;">此區間尚無數據，快來搶頭香！</div>', unsafe_allow_html=True)
            return

        html_rows = []
        for i, r in enumerate(rows):
            rank = i + 1
            rank_class = f"rank-{rank}" if rank <= 3 else "rank-other"
            
            # 數值格式化
            val = r.get(val_col, 0)
            if val_fmt == "int":
                val_str = f"{int(val):,}"
            elif val_fmt == "float":
                val_str = f"{float(val):.2f}"
            elif val_fmt == "time":
                val_str = f"{float(val)/3600:.1f}h"
            else:
                val_str = str(val)
            
            # 處理暱稱顯示 (CSS 皇冠)
            raw_nick = r.get("nickname")
            is_vip = bool(raw_nick and raw_nick.strip())
            display_name = _mask_username(r.get("username"), raw_nick)
            
            # 構建名稱 HTML
            if is_vip:
                # 注入 Crown Icon span
                name_html = f'<span class="crown-icon" style="width:16px; height:16px; margin-right:6px; vertical-align:middle;"></span><span style="color:#FFD700; text-shadow:0 0 10px rgba(255,215,0,0.3);">{html.escape(display_name)}</span>'
            else:
                name_html = html.escape(display_name)
            
            # 使用者高亮
            is_me = (r.get("username") == user["username"])
            bg_style = 'style="background: rgba(59, 130, 246, 0.15); border: 1px solid rgba(59, 130, 246, 0.4); box-shadow: 0 4px 12px rgba(0,0,0,0.2);"' if is_me else ""
            
            # 去除行內 HTML 縮排，確保 Markdown 不會介入干擾
            row_html = (
                f'<tr class="lb-row" {bg_style}>\n'
                f'<td><div class="rank-badge {rank_class}">{rank}</div></td>\n'
                f'<td class="lb-cell"><div style="font-weight:600; font-size:15px; color:#f8fafc; display:flex; align-items:center;">{name_html}</div></td>\n'
                f'<td class="lb-cell" style="text-align:right;">{val_str} <span style="font-size:12px; color:#64748b; font-weight:400; margin-left:4px;">{unit}</span></td>\n'
                f'</tr>'
            )
            html_rows.append(row_html)

        # 組合 Table，注意：必須使用 unsafe_allow_html=True
        # 移除縮排避免 Markdown 誤判
        full_table = (
            '<div class="leaderboard-wrapper">\n'
            '<table class="lb-table" style="width:100%; border-spacing:0 8px; border-collapse:separate;">\n'
            '<tbody>\n'
            f'{ "".join(html_rows) }\n'
            '</tbody>\n'
            '</table>\n'
            '</div>'
        )
        st.markdown(full_table, unsafe_allow_html=True)

    t1, t2, t3, t4 = st.tabs(["算力貢獻", "積分收益", "最高分", "挖礦總時長"])

    with t1:
        st.caption("依據「已運算並回報的策略組合總數」排名。")
        _render_html_table(data["combos"], "total_done", "int", "組")
    
    with t2:
        st.caption("依據「獲得的 USDT 積分獎勵」排名。")
        _render_html_table(data["points"], "total_usdt", "float", "USDT")

    with t3:
        st.caption("依據「單一策略回測分數」排名。")
        _render_html_table(data["score"], "max_score", "float", "分")

    with t4:
        st.caption("依據「累積運算時間」排名。")
        _render_html_table(data["time"], "total_seconds", "time", "")
def _page_submissions(user: Dict[str, Any]) -> None:
    st.markdown("### 提交紀錄")
    try:
        subs = db.list_submissions(user_id=int(user["id"]), limit=300)
    except AttributeError as ae:
        st.error("系統錯誤：`list_submissions` 函數遺失。")
        with st.expander("詳細錯誤資訊", expanded=True):
            import traceback
            st.code(traceback.format_exc(), language="python")
        return
    except Exception as e:
        st.error("載入提交紀錄時發生錯誤。")
        with st.expander("詳細錯誤資訊", expanded=True):
            import traceback
            st.code(traceback.format_exc(), language="python")
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

    payout_currency = str(_cached_get_setting("payout_currency", "USDT") or "USDT").strip()
    withdraw_min = float(_cached_get_setting("withdraw_min_usdt", 20.0) or 20.0)
    withdraw_fee_usdt = float(_cached_get_setting("withdraw_fee_usdt", 1.0) or 1.0)
    withdraw_fee_mode = str(_cached_get_setting("withdraw_fee_mode", "platform_absorb") or "platform_absorb").strip()

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
            _render_compute_workers_panel()
        except Exception:
            pass
        try:
            cycle = _cached_active_cycle()
            if not cycle:
                st.warning("週期尚未初始化")
                st.write("週期", "None", "None", "None")
            else:
                st.write("週期", cycle.get("name"), cycle.get("start_ts"), cycle.get("end_ts"))
                
            ov = db.list_task_overview(limit=500)
        except AttributeError as ae:
            st.error("系統錯誤：管理核心函數遺失。")
            with st.expander("詳細錯誤資訊", expanded=True):
                import traceback
                st.code(traceback.format_exc(), language="python")
            ov = None
        except Exception as e:
            st.error("載入管理總覽時發生錯誤。")
            with st.expander("詳細錯誤資訊", expanded=True):
                import traceback
                st.code(traceback.format_exc(), language="python")
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
                    try:
                        result = _import_weekly_report_csv(report_file)
                        if not result.get("ok"):
                            st.error("匯入失敗。")
                            st.write(result)
                        else:
                            st.success(f'已匯入 {int(result.get("applied") or 0)} 筆。')
                            st.rerun()
                    except Exception as imp_err:
                        st.error("檔案解析或匯入過程發生致命錯誤。")
                        with st.expander("詳細錯誤資訊", expanded=True):
                            import traceback
                            st.code(traceback.format_exc(), language="python")


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

        cycle = _cached_active_cycle()
        if not cycle:
            st.error("週期未初始化。")
            st.stop()

        cycle_id = int(cycle["id"])
        pools = db.list_factor_pools(cycle_id=cycle_id)
        pool_map = {int(p["id"]): p for p in pools}
        with st.expander("策略池復原與匯入", expanded=(not bool(pools))):
            try:
                st.caption(f"資料庫路徑：{db._db_path()} | 當前週期 ID：{int(cycle_id)}")
            except Exception:
                pass

            if st.button("掃描本地資料庫並復原策略池", key="pool_recover"):
                try:
                    rep = db.recover_factor_pools_from_local(cycle_id=int(cycle_id))
                    imported_count = int(rep.get("imported") or 0)
                    skipped_count = int(rep.get("skipped_duplicates") or 0)
                    
                    if imported_count > 0:
                        st.success(f"成功復原 {imported_count} 個策略池 (已略過重複項目: {skipped_count})")
                    else:
                        st.info("未掃描到可復原的策略池，或所有項目已存在。")
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

        st.markdown("新增 Pool (單筆或批量 JSON)")
        with st.expander("展開批量匯入或手動建立", expanded=False):
            batch_json = st.text_area("貼上 Pool JSON 陣列 (選填)", value="", height=200, help='格式需為 [{"name": "...", "symbol": "...", ...}, ...]')
            
            st.markdown("---")
            col1, col2 = st.columns(2)
            with col1:
                name = st.text_input("單筆: create_name", value="New Pool")
                symbol = st.text_input("單筆: create_symbol", value="BTC_USDT")
                tf_min = st.number_input("單筆: timeframe_min", min_value=1, max_value=1440, value=30)
                years = st.number_input("單筆: create_years", min_value=1, max_value=10, value=3)
            with col2:
                family = st.text_input("單筆: create_family", value="TEMA_RSI")
                num_partitions = st.number_input("單筆: num_partitions", min_value=8, max_value=4096, value=128)
                seed = st.number_input("單筆: create_seed", value=int(time.time()) & 0x7FFFFFFF)
                active = st.checkbox("單筆: create_active", value=True)

            grid_spec_json = st.text_area("單筆: grid_spec_json", value='{"fast_min":3,"fast_max":3,"slow_min":100,"slow_max":100,"rsi_thr_min":20,"rsi_thr_max":20}', height=100)
            risk_spec_json = st.text_area("單筆: risk_spec_json", value='{"tp_min":2.2,"sl_min":6.0,"max_hold_min":10,"max_hold_max":60,"max_hold_step":10}', height=100)
            
            auto_expand_all = st.checkbox("自動套用 14 種熱門組合 (BTC/ETH × 7個週期)", value=True, help="勾選後，系統會將此策略自動複製到 BTC_USDT 與 ETH_USDT，並涵蓋 1m, 5m, 15m, 30m, 1h, 4h, 1d 所有級別。")
            
            if st.button("確認執行新增並派發任務", type="primary", use_container_width=True):
                try:
                    if batch_json.strip():
                        clean_json = batch_json.strip()
                        if clean_json.endswith('}') and clean_json.count('[') > clean_json.count(']'):
                            clean_json += ']'
                        
                        try:
                            raw_data = json.loads(clean_json)
                        except json.JSONDecodeError as je:
                            st.error(f"JSON 語法錯誤：{je.msg} (行 {je.lineno}, 列 {je.colno})")
                            st.info("提示：請檢查 JSON 格式是否正確，括號是否對齊。")
                            with st.expander("查看錯誤位置上下文"):
                                lines = clean_json.split('\n')
                                start_err = max(0, je.lineno - 3)
                                end_err = min(len(lines), je.lineno + 3)
                                for i in range(start_err, end_err):
                                    pointer = " <--- 錯誤位置附近" if (i+1) == je.lineno else ""
                                    st.code(f"{i+1}: {lines[i]}{pointer}")
                            st.stop()

                        pool_list = raw_data if isinstance(raw_data, list) else [raw_data]
                        success_count = 0
                        for p_idx, p_item in enumerate(pool_list):
                            try:
                                pids = db.create_factor_pool(
                                    cycle_id=cycle_id,
                                    name=str(p_item.get("name", f"Imported Pool {p_idx+1}")),
                                    symbol=str(p_item.get("symbol", "BTC_USDT")),
                                    timeframe_min=int(p_item.get("timeframe_min", 30)),
                                    years=int(p_item.get("years", 3)),
                                    family=str(p_item.get("family", "TEMA_RSI")),
                                    grid_spec=p_item.get("grid_spec", {}),
                                    risk_spec=p_item.get("risk_spec", {}),
                                    num_partitions=int(p_item.get("num_partitions", 128)),
                                    seed=int(p_item.get("seed", 0)),
                                    active=bool(p_item.get("active", True)),
                                    auto_expand=auto_expand_all
                                )
                                success_count += len(pids)
                            except Exception as item_e:
                                st.error(f"第 {p_idx+1} 個物件匯入失敗：{item_e}")
                        
                        st.success(f"成功處理 {success_count} 個策略分片。")
                    else:
                        grid_spec = json.loads(grid_spec_json)
                        risk_spec = json.loads(risk_spec_json)
                        pids = db.create_factor_pool(
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
                            auto_expand=auto_expand_all
                        )
                        st.success(f"成功建立 {len(pids)} 個策略池（含自動擴展分片）")
                    
                    db.write_audit_log(int(user["id"]), "pool_batch_create", {"count": len(batch_json.strip()) if batch_json.strip() else 1})
                    time.sleep(1)
                    st.rerun()
                except Exception as fatal_e:
                    st.error(f"建立失敗：{str(fatal_e)}")
                    st.code(traceback.format_exc(), language="python")

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
                time.sleep(0.005) # 釋放 GIL，防止大規模派發任務時鎖死全站

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
        time.sleep(0.005) # 釋放 GIL，防止管理員背景結算癱瘓主執行緒



@st.cache_data(ttl=15, show_spinner=False)
def _cached_user_hud_stats(user_id: int, cycle_id: int) -> Tuple[int, float]:
    combos = 0
    points = 0.0
    try:
        if cycle_id > 0:
            tasks = db.list_tasks_for_user(user_id, cycle_id=cycle_id)
        else:
            tasks = db.list_tasks_for_user(user_id)
        for t in tasks or []:
            try:
                prog = json.loads(t.get("progress_json") or "{}")
            except Exception:
                prog = {}
            combos += int(prog.get("combos_done") or 0)
    except Exception:
        pass

    try:
        payouts = db.list_payouts(user_id=user_id, limit=500)
        for p in payouts or []:
            if str(p.get("status") or "") == "void":
                continue
            points += float(p.get("amount_usdt") or 0.0)
    except Exception:
        pass
    return combos, points

def _render_user_hud(user: Dict[str, Any]) -> None:
    """Fixed bottom-left user panel."""
    try:
        cycle = _cached_active_cycle()
    except Exception:
        cycle = None

    cycle_id = int(cycle.get("id") or 0) if cycle else 0

    combos_done_sum, points_sum = _cached_user_hud_stats(int(user["id"]), cycle_id)

    points_help = (
        "積分的規則：每跑過一個達標組合，且因子池系統在一週實盤結算後的 利潤，其中一半會換算成你的積分。積分可根據排名等等兌換空投獎勵，兌換與發放以結算規則為準。"
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
    _perf_init()
    _perf_hud_bootstrap_once()
    st.set_page_config(page_title=APP_TITLE, layout="wide", initial_sidebar_state="expanded")
    _render_entry_overlay_once()
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

    # [新增] 主頁分離與排行榜頁面入口
    pages = ["主頁", "控制台", "排行榜", "任務", "提交", "結算", "新手教學"] + (["管理"] if role == "admin" else [])

    # [新增] 強制重新載入(動畫播放前)回到主頁，避免停留在其他頁面
    if "_sheep_fresh_load" not in st.session_state:
        st.session_state["_sheep_fresh_load"] = True
        st.session_state["nav_page"] = "主頁"
        try:
            st.query_params["page"] = "主頁"
        except Exception as query_err:
            import traceback
            print(f"[WARN] 無法寫入 query params: {query_err}\n{traceback.format_exc()}")
    else:
        # 利用 URL 查詢參數持久化當前頁面狀態
        try:
            q_page = st.query_params.get("page", "")
            if q_page in pages:
                st.session_state["nav_page"] = q_page
        except Exception as query_err:
            import traceback
            print(f"[WARN] 無法讀取 query params: {query_err}\n{traceback.format_exc()}")

    if "nav_page_pending" in st.session_state:
        try:
            _pending = str(st.session_state.pop("nav_page_pending") or "").strip()
        except Exception:
            _pending = ""
        if _pending and _pending in pages:
            st.session_state["nav_page"] = _pending
            try:
                st.query_params["page"] = _pending
            except Exception:
                pass

    if "nav_page" not in st.session_state or st.session_state["nav_page"] not in pages:
        st.session_state["nav_page"] = pages[0]
        try:
            st.query_params["page"] = pages[0]
        except Exception:
            pass

    with st.sidebar:
        st.markdown(f"### {APP_TITLE}")
        st.markdown(f'<div class="small-muted">{user["username"]} · {role}</div>', unsafe_allow_html=True)

        # Navigation (custom buttons instead of st.radio) to avoid default red dot indicator
        current_page = str(st.session_state.get("nav_page") or pages[0])

        for p in pages:
            is_active = (p == current_page)
            btn_type = "primary" if is_active else "secondary"
            # 插入隱藏的定位錨點，讓 CSS 透過 :has() 找到對應的按鈕
            st.markdown(f'<div class="sidebar-anchor nav-anchor-{p}"></div>', unsafe_allow_html=True)
            if st.button(p, key=f"nav_btn_{p}", type=btn_type, use_container_width=True):
                st.session_state["nav_page"] = p
                try:
                    st.query_params["page"] = p
                except Exception:
                    pass
                st.rerun()

        st.markdown('<div style="height: 10px"></div>', unsafe_allow_html=True)

        # 插入隱藏的定位錨點，讓 CSS 透過 :has() 找到對應的按鈕
        st.markdown('<div class="sidebar-anchor nav-anchor-登出"></div>', unsafe_allow_html=True)
        if st.button("登出", key="logout_btn", type="secondary", use_container_width=True):
            _logout()
            st.rerun()

        _render_user_hud(user)

    page = str(st.session_state.get("nav_page") or pages[0])

    import traceback
    try:
        if page == "主頁":
            _page_home(user)
        elif page == "新手教學":
            _page_tutorial(user)
        elif page == "控制台":
            _page_dashboard(user)
        elif page == "排行榜":
            _page_leaderboard(user)
        elif page == "任務":
            _page_tasks(user, job_mgr)
        elif page == "提交":
            _page_submissions(user)
        elif page == "結算":
            _page_rewards(user)
        elif page == "管理" and role == "admin":
            _page_admin(user, job_mgr)
    except Exception as route_err:
        import uuid
        from datetime import datetime, timezone

        err_id = str(uuid.uuid4())
        ts_utc = datetime.now(timezone.utc).isoformat()
        tb = traceback.format_exc()

        try:
            print(f"[CRITICAL UI ERROR] id={err_id} ts_utc={ts_utc} page={page} user={(user.get('username') if isinstance(user, dict) else '')}", flush=True)
            print(tb, flush=True)
        except Exception:
            pass

        st.error("系統頁面渲染發生異常，已記錄錯誤日誌。")
        st.info(f"錯誤追蹤代碼：{err_id}")
        with st.expander("展開查看詳細錯誤堆疊資訊 (Traceback)", expanded=True):
            st.code(tb, language="python")
    # ---- Perf HUD (fixed, tiny, no layout impact) ----
    try:
        pm = st.session_state.get("_perf_ms") or {}
        hud_items = []
        for k in ["global_snapshot_ms"]:
            if k in pm:
                hud_items.append(f"{k}: {pm[k]} ms")

        if hud_items:
            st.markdown(
                """
<style>
#sheepPerfHud{
  position: fixed;
  right: 14px;
  bottom: 14px;
  z-index: 2147483647;
  background: rgba(0,0,0,0.55);
  border: 1px solid rgba(255,255,255,0.14);
  border-radius: 12px;
  padding: 8px 10px;
  color: rgba(255,255,255,0.85);
  font-size: 12px;
  line-height: 1.5;
  max-width: 320px;
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
  box-shadow: 0 10px 30px rgba(0,0,0,0.55);
  opacity: 0.35;
  transition: opacity 0.2s ease, transform 0.2s ease;
}
#sheepPerfHud:hover{
  opacity: 1.0;
  transform: translateY(-2px);
}
#sheepPerfHud .t{
  font-weight: 800;
  letter-spacing: 0.4px;
  color: rgba(255,255,255,0.92);
  margin-bottom: 6px;
}
#sheepPerfHud .r{
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
  color: rgba(255,255,255,0.85);
}
</style>
                """,
                unsafe_allow_html=True,
            )
            st.markdown(
                "<div id='sheepPerfHud'><div class='t'>PERF</div><div class='r'>"
                + "<br>".join([html.escape(x) for x in hud_items])
                + "</div></div>",
                unsafe_allow_html=True,
            )
    except Exception:
        pass    
    _perf_emit_payload()    
    return


if __name__ == "__main__":
    try:
        main()
    except Exception as fatal_e:
        st.error("系統運行階段發生未預期的致命錯誤，應用程式已中止執行。")
        st.info("請將以下錯誤堆疊資訊提供給系統管理員進行修復：")
        st.code(traceback.format_exc(), language="python")
