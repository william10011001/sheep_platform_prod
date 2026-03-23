#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ethusdt_rsi_l05.py

— TEMA_RSI 策略自動交易機器人 v2.0 —

1) 策略核心：TEMA (三重指數移動平均) + RSI 複合策略。
   • 邏輯：結合 TEMA 快慢線交叉、形態判斷與 RSI 過濾。
   • 進場模式：包含回調 (Pullback)、動能 (Momentum)、交叉 (Cross) 與 RSI 反轉。

2) 風險控制：
   • 移動止損 (Trailing Stop)：價格獲利達標後啟動，隨行情移動止損線。
   • 動態倉位：依據帳戶淨值百分比 (Stake %) 計算下單量。
   • 時間止損 (Time Stop)：持倉過久強制平倉。
   • 雙重防護：包含本地端監控與交易所限價單。

3) 系統功能：
   • 支援 BitMart 合約 API (V2)。
   • 圖形化介面 (GUI) 設定參數。
   • 自動錯誤處理與斷線重連。

依賴：Python 3.9+；pip install requests pandas numpy pillow
"""
from decimal import Decimal, ROUND_FLOOR
import sys, os, io, json, math, time, hmac, hashlib, threading, queue, traceback, urllib.parse, random, uuid, argparse, logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List

# ============ 可選相依（Pillow：背景圖載入/縮放/模糊） ============
try:
    from PIL import Image, ImageTk, ImageFilter
    PIL_OK = True
except Exception:
    PIL_OK = False

# ============ 必要相依 ============
import csv
import requests
import pandas as pd
import numpy as np
import subprocess
import warnings
import urllib3

# [專家級防護] 關閉 SSL 警告與 Pandas 煩人的未來版本通知
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

# [專家級潔癖] 在匯入任何可能間接觸發 Streamlit 的模組前，先封鎖快取警告
import logging
logging.getLogger("streamlit.runtime.caching.cache_data_api").setLevel(logging.CRITICAL)

# [shared runtime] resolve imports from the project root instead of the realtime cwd.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_DIR = PROJECT_ROOT / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from sheep_holy_grail_runtime import HolyGrailRuntime, run_holy_grail_build
from sheep_runtime_paths import (
    ensure_parent,
    import_backtest_runtime,
    realtime_local_config_path,
    realtime_public_config_path,
    realtime_config_path,
    realtime_config_template_path,
    realtime_exec_log_dir,
    realtime_log_path,
    realtime_state_path,
)
from sheep_strategy_schema import normalize_direction, normalize_runtime_strategy_entry, normalize_strategy_batch

bt, HOLY_GRAIL_IMPORT_ERROR = import_backtest_runtime(PROJECT_ROOT)

# [專家級潔癖] import 完成後，進行全域掃蕩，確保沒有任何遺漏的 Streamlit 日誌會弄髒您的實盤介面
for name in list(logging.root.manager.loggerDict.keys()):
    if "streamlit" in name.lower():
        _logger = logging.getLogger(name)
        _logger.setLevel(logging.CRITICAL)
        _logger.propagate = False

try:
    import websocket
    WS_OK = True
except ImportError:
    WS_OK = False

try:
    import paramiko
    PARAMIKO_OK = True
except ImportError:
    PARAMIKO_OK = False

# [專家新增] CSV 延遲與滑點紀錄檔案設定
LOG_DIR = str(realtime_exec_log_dir())
EXEC_CSV_FILE = os.path.join(LOG_DIR, f"execution_log_{datetime.now(timezone(timedelta(hours=8))).strftime('%Y%m%d_%H%M%S')}.csv")

def init_csv_log():
    try:
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR, exist_ok=True)
        with open(EXEC_CSV_FILE, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # 欄位：時間, 模式(DRY_RUN/LIVE/TEST), 方向, 訊號延遲(秒), 下單延遲(秒), 觸發時價格(Before), 實際成交或回報價格(After)
            writer.writerow(["Time", "Mode", "Side", "Signal_Delay_sec", "Exec_Delay_sec", "Price_Before", "Price_After"])
    except Exception as e:
        print(f"CSV初始化失敗: {e}")

def log_execution_csv(mode, side, sig_delay, exec_delay, px_before, px_after):
    try:
        with open(EXEC_CSV_FILE, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([now_ts(), mode, side, f"{sig_delay:.4f}", f"{exec_delay:.4f}", f"{px_before}", f"{px_after}"])
    except Exception as e:
        log(f"CSV寫入失敗: {e}")
# [專家新增] 全域環境配置
pd.set_option('display.max_rows', 10)
np.seterr(divide='ignore', invalid='ignore')
# ============ GUI ============
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
APP_NAME = "TEMA_RSI 自動交易系統"
CFG_FILE = str(realtime_local_config_path())
CFG_FALLBACK_FILE = str(realtime_config_path())
CFG_PUBLIC_FILE = str(realtime_public_config_path())
CFG_TEMPLATE_FILE = str(realtime_config_template_path())
LOG_FILE = str(realtime_log_path())
STATE_FILE = str(realtime_state_path())
TZ8 = timezone(timedelta(hours=8))

# ============ 全域共享 ============
log_q: "queue.Queue[str]" = queue.Queue()
stop_event = threading.Event()
ui_running_lock = threading.Lock()

# ============ 預設 (Institutional Defaults) ============
DRY_RUN_DEFAULT = False
# [專家修正] 機構級別 30m 專用預設參數，嚴格對齊 Backtest Panel 最佳參數
DEFAULT_INTERVAL = "30m" 
DEFAULT_FAST_LEN = 12
DEFAULT_SLOW_LEN = 50
DEFAULT_RSI_LEN = 14
DEFAULT_RSI_THR = 5
DEFAULT_MINTICK = 0.01
DEFAULT_STAKE_PCT = 95.0
DEFAULT_ACT_PCT = 0.1
DEFAULT_TRAIL_TICKS = 500
DEFAULT_TP_PCT = 0.1
DEFAULT_SL_PCT = 0.1
DEFAULT_MAX_HOLD = 300
DEFAULT_BG_URL = ""

# ============ 通用工具與日誌 ============
def now_ts() -> str:
    return datetime.now(TZ8).strftime("%Y-%m-%d %H:%M:%S")

# [Institutional] 旋轉日誌：避免長期運行把硬碟寫爆
_logger = logging.getLogger("tema_rsi")
_logger.setLevel(logging.INFO)
if not _logger.handlers:
    try:
        _h = RotatingFileHandler(str(ensure_parent(LOG_FILE)), maxBytes=8*1024*1024, backupCount=5, encoding="utf-8")
        _h.setFormatter(logging.Formatter("%(message)s"))
        _logger.addHandler(_h)
    except Exception:
        pass

def log(msg: str):
    line = f"[{now_ts()}] {msg}"
    try:
        log_q.put_nowait(line)
    except Exception:
        pass
    try:
        _logger.info(line)
    except Exception:
        pass


def clamp(v, lo, hi):
    try:
        v = float(v)
        return max(lo, min(hi, v))
    except Exception:
        return lo

def fmt_json(d) -> str:
    try:
        return json.dumps(d, ensure_ascii=False, indent=2)
    except Exception:
        return str(d)

def safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)

def debounce(ms: int):
    """UI 事件防抖：將多次觸發合併成最後一次。"""
    def deco(fn):
        def wrapper(self, *args, **kwargs):
            key = f"_deb_{fn.__name__}"
            try:
                if not hasattr(self, "_debounce_handles"):
                    self._debounce_handles = {}
                h = self._debounce_handles.get(key)
                if h is not None:
                    self.after_cancel(h)
                self._debounce_handles[key] = self.after(ms, lambda: fn(self, *args, **kwargs))
            except Exception:
                pass
        return wrapper
    return deco
# ============ 機構級：速率限制 / 重試底座 ============
class _TokenBucket:
    def __init__(self, capacity: int, window_sec: float):
        self.capacity = float(max(1, int(capacity)))
        self.window_sec = float(max(0.2, window_sec))
        self.tokens = self.capacity
        self.updated = time.time()
        self.lock = threading.Lock()

    def acquire(self, n: float = 1.0):
        n = float(n)
        while True:
            with self.lock:
                now = time.time()
                dt = now - self.updated
                if dt > 0:
                    refill = (dt / self.window_sec) * self.capacity
                    self.tokens = min(self.capacity, self.tokens + refill)
                    self.updated = now
                if self.tokens >= n:
                    self.tokens -= n
                    return
                missing = n - self.tokens
                sleep_s = (missing / self.capacity) * self.window_sec
            time.sleep(max(0.01, min(2.0, sleep_s)))

class RateLimiter:
    def __init__(self):
        self.buckets: Dict[str, _TokenBucket] = {}

    def _bucket(self, key: str, cap: int, window_sec: float) -> _TokenBucket:
        b = self.buckets.get(key)
        if b is None:
            b = _TokenBucket(cap, window_sec)
            self.buckets[key] = b
        return b

    def acquire(self, key: str, cap: int, window_sec: float, n: float = 1.0):
        self._bucket(key, cap, window_sec).acquire(n)

def _bm_client_order_id(prefix: str = "BM") -> str:
    return f"{prefix}{uuid.uuid4().hex[:24]}"

# ============ BitMart API Client（相容 V2 Futures 完美修正版） ============
class BitmartClient:
    def __init__(self, api_key: str, secret: str, memo: str, trade_base: str, quote_base: str, timeout=15, retries=3, retry_sleep=0.8, dry_run=False):
        self.api_key = (api_key or "").strip()
        self.secret = (secret or "").strip()
        self.memo = (memo or "").strip()
        # BitMart V2 API Base URL
        default_url = "https://api-cloud-v2.bitmart.com/"
        self.trade_base = (trade_base or "").strip().rstrip("/") or default_url
        self.timeout = max(5, int(timeout or 15))
        self.retries = max(1, int(retries or 3))
        self.retry_sleep = float(retry_sleep or 0.8)
        self.dry_run = bool(dry_run)
        self.contract_map = {}
        # [Institutional] 連線重用，降低延遲 + 降低被 WAF 誤判機率
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "tema-rsi-bot/2.1 (+https://api-cloud-v2.bitmart.com)"
        })
        # [Institutional] 速率限制器（依 BitMart 官方 rate limit 做保守保護）
        self.rl = RateLimiter()
        # 記住每個 symbol 的 leverage/open_type，避免 40012「不同步」
        self._leverage_by_symbol: Dict[str, str] = {}
        self._open_type_by_symbol: Dict[str, str] = {}


    def _get_headers(self, query_string: str, body_str: str = "") -> Dict[str, str]:
        ts = str(int(time.time() * 1000))
        # [專家修正] 嚴格執行 V2 簽名規範：所有非 ASCII 字元必須在簽名前進行 URL 編碼
        # 這是為了防止與 BitMart 負載平衡器發生 HTTP 400 衝突
        safe_memo = urllib.parse.quote(self.memo) if any(ord(c) > 127 for c in self.memo) else self.memo
            
        text = f"{ts}#{safe_memo}#{body_str or query_string}"
        sign = hmac.new(self.secret.encode(), text.encode(), hashlib.sha256).hexdigest()
        
        headers = {
            "Content-Type": "application/json",
            "X-BM-KEY": self.api_key,
            "X-BM-TIMESTAMP": ts,
            "X-BM-SIGN": sign,
            "X-BM-MEMO": safe_memo, 
        }
        return headers

    def _request(self, method: str, path: str, params: Dict[str, Any] = None, signed: bool = True) -> Dict[str, Any]:
        last_exc = None
        url = f"{self.trade_base}{path}"
        
        if self.dry_run:
            # [專家修正] 原本用 submit_order/cancel_orders（底線）判斷，
            # 但本程式呼叫的是 submit-order/cancel-orders（連字號），導致乾跑模式形同虛設。
            if ("assets" in path) or ("wallet" in path):
                return {"code": 1000, "data": [{"currency":"USDT", "available_balance":"999999", "equity":"1000000"}]}
            if "position" in path:
                return {"code": 1000, "data": []}
            if ("submit-order" in path) or ("submit_order" in path):
                return {"code": 1000, "data": {"order_id": int(time.time()*1000)}}
            if "submit-tp-sl-order" in path:
                return {"code": 1000, "data": {"order_id": int(time.time()*1000)}}
            if ("cancel-orders" in path) or ("cancel_orders" in path):
                return {"code": 1000, "data": {}}
            if "contracts" in path:
                return {"code": 1000, "data": {"symbols": [{"symbol":"ETHUSDT", "contract_size": 0.001}]}}

        for attempt in range(1, self.retries + 1):
            try:
                p_clean = {k: v for k, v in (params or {}).items() if v is not None}
                query_str = ""
                body_str = ""
                headers = {}

                if method.upper() == "GET":
                    query_str = urllib.parse.urlencode(p_clean)
                    full_url = f"{url}?{query_str}" if query_str else url
                    if signed: headers = self._get_headers(query_str, "")
                    resp = requests.get(full_url, headers=headers, timeout=self.timeout)
                else:
                    body_str = json.dumps(p_clean)
                    if signed: headers = self._get_headers("", body_str)
                    resp = requests.post(url, data=body_str, headers=headers, timeout=self.timeout)

                try:
                    data = resp.json()
                except:
                    resp.raise_for_status()
                    data = {}

                code = int(data.get("code", 0))
                if code != 1000:
                    # [Expert Fix] 捕捉完整錯誤訊息以便除錯 (含 raw data)
                    msg = data.get("message") or data.get("msg") or "Unknown"
                    raw_dump = json.dumps(data, ensure_ascii=False)
                    raise RuntimeError(f"BitMart API Error {code}: {msg} | Raw: {raw_dump}")
                return data

            except Exception as e:
                last_exc = e
                if "30030" in str(e):
                    # V2 deprecated error or Auth error
                    raise RuntimeError(f"BitMart 錯誤 30030 (版本或授權異常): {e}")
                if attempt < self.retries:
                    time.sleep(self.retry_sleep * (1.6 ** (attempt-1)))
                else:
                    break
        raise RuntimeError(f"HTTP Failed: {last_exc}")

    def get_balance(self):
            # V2 (Path works with V2 Headers): /contract/private/assets-detail
            # 這是 V2 簽名模式下獲取餘額的常用路徑
            res = self._request("GET", "/contract/private/assets-detail", {})
            usdt_bal = {}
            # Data is a list
            for x in (res.get("data") or []):
                if x.get("currency") == "USDT":
                    usdt_bal = x
                    break
            
            equity = safe_float(usdt_bal.get("equity"), 0.0)
            avail = safe_float(usdt_bal.get("available_balance"), 0.0)
            return {
                "code": "0",
                "data": {
                    "balance": {
                        "availableBalance": avail,
                        "walletBalance": equity,
                        "equity": equity
                    }
                }
            }

    def get_positions(self):
        res = self._request("GET", "/contract/private/position", {})
        out = []
        for p in (res.get("data") or []):
            amt = safe_float(p.get("current_amount"), 0)
            if amt == 0:
                continue

            sym_raw = p.get("symbol", "")
            sym = (sym_raw or "").replace("-", "").replace("_", "").upper()

            pos_type = int(p.get("position_type", 1))
            side = "LONG" if pos_type == 1 else "SHORT"

            out.append({
                "symbol": sym,
                "positionSide": side,
                "positionId": f"{sym}:{side}",
                "positionAmt": amt if side == "LONG" else -amt,
                "entryPrice": safe_float(p.get("open_avg_price"), 0),
                "markPrice": safe_float(p.get("mark_price") or p.get("fair_price") or p.get("last_price"), 0),
                "unrealizedPnl": safe_float(p.get("unrealized_profit") or p.get("unrealized_pnl") or p.get("unrealised_pnl"), 0),
                "positionValue": safe_float(p.get("position_value") or p.get("hold_value") or p.get("position_margin_value"), 0),
                "margin": safe_float(p.get("position_margin") or p.get("margin") or p.get("hold_margin"), 0),
                "marginRatePct": safe_float(p.get("margin_rate") or p.get("margin_ratio") or p.get("risk_rate"), 0) * 100.0,
                "liquidationPrice": safe_float(p.get("liquidation_price") or p.get("liq_price") or p.get("force_close_price"), 0),
                "raw": dict(p or {}),
            })
        return {"code": "0", "data": out}


    def get_open_orders(self, symbol=None):
        return {"code":"0", "data": []}

    def cancel_all_open_orders(self, symbol):
        # [專家修正] BitMart V2 符號規範：使用 ETHUSDT（無底線/無連字號）
        s = symbol.replace("-", "").replace("_", "")
        # V2 (Path works with V2 Headers): /contract/private/cancel-orders
        self._request("POST", "/contract/private/cancel-orders", {"symbol": s})
        return {}

    def submit_tp_sl_order(
        self,
        symbol: str,
        position_side: str,
        tp_sl_type: str,
        trigger_price: float,
        executive_price: float = None,
        price_type: int = 2,
        plan_category: int = 2,
        category: str = "market",
        size: int = None,
        client_order_id: str = None,
    ):
        """
        BitMart Futures V2:
        POST /contract/private/submit-tp-sl-order

        官方欄位對照：
        - type: take_profit / stop_loss
        - side（oneway/hedge 關閉方向一致）: 3=sell(reduce only)=平多, 2=buy(reduce only)=平空
        - price_type: 1=last_price, 2=fair_price
        - plan_category: 2=Position TP/SL（預設）
        - category: market / limit
        """
        sym = symbol.replace("-", "").replace("_", "")
        ps = (position_side or "").upper()
        if ps not in ("LONG", "SHORT"):
            raise ValueError(f"position_side must be LONG/SHORT, got: {position_side}")

        order_type = (tp_sl_type or "").lower()
        if order_type not in ("take_profit", "stop_loss"):
            raise ValueError(f"tp_sl_type must be take_profit/stop_loss, got: {tp_sl_type}")

        side = 3 if ps == "LONG" else 2
        trig = str(trigger_price)
        exec_px = str(executive_price if executive_price is not None else trigger_price)

        params = {
            "symbol": sym,
            "side": int(side),
            "type": order_type,
            "trigger_price": trig,
            "executive_price": exec_px,
            "price_type": int(price_type),
            "plan_category": int(plan_category),
            "category": str(category),
        }
        if size is not None:
            params["size"] = int(size)
        if client_order_id:
            params["client_order_id"] = str(client_order_id)

        return self._request("POST", "/contract/private/submit-tp-sl-order", params)

    def set_position_mode(self, dual=True): return {}
    def get_position_mode(self): return {"code":"0", "data":{"dualSidePosition": True}}
    def get_margin_type(self, symbol): return {"code":"0", "data":{"marginType":"CROSSED"}}
    def set_margin_type(self, symbol, mtype): return {}
    def set_leverage(self, symbol, lev): 
        # [專家修正] BitMart V2 API 符號規範：使用 ETHUSDT (無底線)
        s = symbol.replace("-", "").replace("_", "")
        
        # V2 (Path works with V2 Headers): /contract/private/submit-leverage
        try:
            # [專家修正] BitMart V2 規範: open_type 為字串 "cross"=全倉, "isolated"=逐倉
            # 我們強制傳送整數 2 (全倉)，確保與下單邏輯一致
            self._request("POST", "/contract/private/submit-leverage", {
                "symbol": s, "leverage": str(int(lev)), "open_type": "cross" 
            })
        except Exception as e:
            # 40012 代表槓桿未變更或不允許修改(或是已設定為該值)，通常可忽略
            if "40012" not in str(e):
                log(f"槓桿設定警示 (若為 40012 可忽略): {e}")
        return {}

    def get_contract_size(self, symbol_bitmart_fmt):
            """
            [強制修正] 獲取合約面額。
            強制鎖定 ETHUSDT = 0.001（BitMart ETH 永續常見最小數量/面額即為 0.001）
            """
            s = symbol_bitmart_fmt.upper()
            if "ETHUSDT" in s:
                return 0.001
            if "BTCUSDT" in s:
                return 0.001

            # 其他幣種才真的查 API（或 fallback）
            data = self._request("GET", "/contract/public/details", signed=False)
            for item in data.get("data", []):
                if item.get("symbol") == symbol_bitmart_fmt:
                    return float(item.get("contract_size", 1))
            return 1


    def place_order(self, symbol, side, position_side, otype, qty=None, price=None, stop_price=None, working_type=None, position_id=None, close_position=None, tif=None):
        # [專家修正] BitMart V2 下單必須使用標準格式 ETHUSDT (移除底線強制轉換)
        sym = symbol.replace("-", "").replace("_", "")

        bm_side = 0
        s_upper = (side or "").upper()
        ps_upper = (position_side or "").upper()
        
        # 1. Side 映射
        if s_upper == "BUY":
            if ps_upper == "LONG": bm_side = 1     # 開多
            elif ps_upper == "SHORT": bm_side = 2  # 平空
        elif s_upper == "SELL":
            if ps_upper == "LONG": bm_side = 3     # 平多
            elif ps_upper == "SHORT": bm_side = 4  # 開空

        # [專家修正] 嚴格張數計算邏輯：BitMart 必須無條件捨去小數位以符合張數整數要求
        c_size = self.get_contract_size(sym)
        raw_qty = safe_float(qty, 0)
        
        if raw_qty > 0 and c_size > 0:
            # 確保 vol 為整數張，且必須大於 0
            vol = int(Decimal(str(raw_qty)) / Decimal(str(c_size)))
        else:
            vol = 0

        # Sanity check
        if vol < 1 and not close_position:
            log(f"ERR: Order vol is 0 (Qty:{raw_qty} / Size:{c_size})")
            return {"code":"-1", "message":"Zero Volume"}
            
        # Fallback for close order
        if vol < 1 and not close_position:
            vol = 1

        bm_type = "limit" if otype == "LIMIT" else "market"
        price_str = str(price) if (bm_type=="limit" and price) else None

        # ================== 下單記錄 ==================
        log(f"[下單] {symbol} | {otype} | 數量:{raw_qty} -> 張數:{vol} | 價格:{price_str}")

        # [專家修正] BitMart V2 參數規範嚴格檢查
        # 1. open_type 必須為整數 2 (全倉)，不能是字串 "cross"
        # 2. leverage 建議與 set_leverage 保持一致
        # 3. 市價單絕對不能傳送 "price" 欄位，即便是 null 也會導致 40011
        # [Institutional] leverage/open_type 必須與 submit-leverage 保持一致，否則會出 40012
        lev_str = self._leverage_by_symbol.get(sym) or "5"
        ot = (self._open_type_by_symbol.get(sym) or "cross").lower()

        params = {
            "symbol": sym,
            "client_order_id": _bm_client_order_id(),
            "side": int(bm_side),
            "mode": 1,          # 1=GTC
            "type": bm_type,
            "leverage": str(lev_str),
            "open_type": ot,
            "size": int(vol),
        }

        if price_str is not None:
            params["price"] = price_str
        
        # BitMart V2 主要靠 side 判斷平倉
        # 此處維持 V2 標準
        
        # log(f"傳送數據: {json.dumps(params)}") # 減少日誌干擾，暫時註解

        # V2 (Path works with V2 Headers): /contract/private/submit-order
        try:
            res = self._request("POST", "/contract/private/submit-order", params)
        except Exception as e:
            # [Expert Debug] 若下單失敗，強制印出當下參數供查核 (解決 40011 盲點)
            log(f"【下單參數診斷】Params: {json.dumps(params)}")
            raise e

        oid = (res.get("data") or {}).get("order_id")
        return {"code":"0", "data":{"orderId": str(oid)}}

    def close_position_by_id(self, position_id: str, close_qty: float = None):
        try:
            parts = position_id.split(":")
            if len(parts) < 2:
                # 若 ID 格式不正確 (如舊的時間戳 ID)，嘗試透過持有倉位反查
                # [專家修正] 移除對 self.symbol 的依賴，這在多家族 client 層級可能不安全
                log(f"倉位 ID '{position_id}' 格式不符 (無冒號)，無法安全平倉")
                return {"code":"-1", "data":{"status":"invalid_id"}}
            else:
                sym = parts[0]
                pos_side = parts[1]
            
            curr_pos = self.get_positions().get("data", [])
            target_amt = 0
            for p in curr_pos:
                if p.get("positionId") == position_id:
                    target_amt = abs(float(p.get("positionAmt")))
                    break
            
            if target_amt > 0:
                c_size = self.get_contract_size(sym)
                # [專家核心升級] 支援多策略部分平倉，精準隔離並保護其他策略的倉位
                max_qty_token = target_amt * c_size 
                qty_token = min(close_qty, max_qty_token) if close_qty and close_qty > 0 else max_qty_token
                
                if qty_token <= 0:
                    return {"code":"0", "data":{"status":"ignored_zero_qty"}}
                    
                side_to_order = "SELL" if pos_side == "LONG" else "BUY"
                self.place_order(sym.replace("_","-"), side_to_order, pos_side, "MARKET", qty=qty_token, close_position=True)

        except Exception as e:
            log(f"BitMart Close Pos Error: {e}")
        return {"code":"0", "data":{"status":"closed"}}

    def get_contracts(self):
        # V2 Public (Path works with V2 Headers): /contract/public/details
        res = self._request("GET", "/contract/public/details", {}, signed=False)
        out = []
        for x in (res.get("data") or {}).get("symbols", []):
            s = x.get("symbol", "")

            # --- 1) 嚴格解析 BitMart 官方欄位 ---
            # price_precision 可能是：
            #   - "0.1" / "0.01" 這種 tick size
            #   - 或 "1" / "2" 這種「小數位數」
            price_prec_raw = x.get("price_precision") or x.get("min_price_precision")

            contract_size = safe_float(x.get("contract_size"), 1.0)
            # min_volume 為「最小張數」，我們要換算成幣本位數量
            min_vol = safe_float(x.get("min_volume") or x.get("min_vol") or 1, 1)

            # --- 2) 轉成真正的 priceStep（tick size） ---
            if price_prec_raw is None:
                price_step_val = 0.1  # 給一個保守預設，不會超出絕大多數合約限制
            else:
                try:
                    f = float(price_prec_raw)
                    if 0 < f < 1:
                        # 像 "0.1" / "0.01" 這種，本來就是 tick size
                        price_step_val = f
                    else:
                        # 像 "1" / "2" 表示小數位數 → 轉成 10^-d
                        d = int(f)
                        price_step_val = 10 ** (-d) if d >= 0 else 0.1
                except Exception:
                    price_step_val = 0.1

            # --- 3) quantityStep / minQty 一律用「幣本位」 ---
            # BitMart 合約是以「張數」計量，contract_size 是 1 張 = 幾幣
            # 我們內部用 token 數量，所以：
            #   一張的幣數 = contract_size → 這是最小步進
            #   最小下單幣數 = contract_size * min_vol
            if contract_size <= 0:
                contract_size = 1.0
            qty_step_val = contract_size
            min_qty_val = contract_size * min_vol

            out.append({
                "symbol": s.replace("_", "-"),
                "priceStep": str(price_step_val),
                "quantityStep": str(qty_step_val),
                "minQty": str(min_qty_val),
                "maxLeverage": x.get("max_leverage")
            })
        return {"code": "0", "data": out}



    def _get_ticker(self, symbol) -> Dict[str, Any]:
        s = (symbol or "").replace("-", "").replace("_", "")
        try:
            res = self._request("GET", "/contract/public/ticker", {"symbol": s}, signed=False)
            d = res.get("data")
            # 常見格式: {data: {...}} 或 {data: [{...}]} 或 {data: {"tickers":[...]}}
            if isinstance(d, dict) and "tickers" in d:
                d = d.get("tickers")
            if isinstance(d, list) and d:
                # 優先找 symbol 相符，找不到就用第一筆
                for it in d:
                    ss = str(it.get("symbol","")).replace("-", "").replace("_", "")
                    if ss == s:
                        return it
                return d[0]
            if isinstance(d, dict):
                return d
        except Exception:
            pass
        return {}

    def get_mark_price(self, symbol) -> float:
        t = self._get_ticker(symbol)
        for k in ("fair_price", "fairPrice", "mark_price", "markPrice", "index_price", "indexPrice"):
            v = safe_float(t.get(k), 0.0)
            if v > 0:
                return v
        return self.get_last_price(symbol)

    def get_last_price(self, symbol) -> float:
        # 1) 先走 ticker（速度較快、欄位完整）
        t = self._get_ticker(symbol)
        for k in ("last_price", "lastPrice", "price", "close_price", "closePrice"):
            v = safe_float(t.get(k), 0.0)
            if v > 0:
                return v

        # 2) fallback: market-trade
        try:
            s = (symbol or "").replace("-", "").replace("_", "")
            res = self._request("GET", "/contract/public/market-trade", {"symbol": s}, signed=False)
            d = res.get("data")
            if isinstance(d, list) and d:
                # 取「最新」那筆：若 API 回傳排序不保證，保守地挑 timestamp 最大的
                best = None
                best_ts = -1
                for it in d:
                    ts = int(it.get("timestamp") or it.get("ts") or 0)
                    if ts > best_ts:
                        best_ts = ts
                        best = it
                d = best or d[0]
            return safe_float((d or {}).get("price"), 0.0)
        except Exception:
            return 0.0


# ============ 市場與技術計算 ============
INTERVAL_MS = {"1m":60000, "3m":180000, "5m":300000, "15m":900000, "30m":1800000, "1h":3600000, "2h":7200000, "4h":14400000}

def _last_closed_kline_index(df: pd.DataFrame, interval_ms: int, now_ms: Optional[int]=None) -> int:
    """
    回傳「最後一根已收盤 K 棒」的 index。

    TradingView / Pine Script 的策略預設只在 bar close 進行一次計算（除非 calc_on_every_tick=true）。
    交易所 K 線 API 通常會把「正在形成的最新一根」也回傳，該根的 close_price 其實是「當下最後成交價」，
    會導致訊號看起來像在吃即時價格（repaint / 與 TV 不一致）。

    規則：若該 K 棒的 (open_time + interval_ms) <= now_ms，視為已收盤。
    """
    try:
        if df is None or df.empty:
            return -1
        if now_ms is None:
            now_ms = int(time.time() * 1000)

        # BitMart Kline 的 time 欄位是該根 K 棒「開盤時間」(ms)；收盤時間 = open + interval
        t_open = df["time"].values.astype(np.int64)
        close_ms = t_open + int(interval_ms)

        idxs = np.where(close_ms <= int(now_ms))[0]
        return int(idxs[-1]) if idxs.size > 0 else -1
    except Exception:
        # 若遇到資料欄位異常，退化成「最後一筆」避免整個策略崩潰
        return (len(df) - 1) if df is not None else -1

# ============ 策略核心邏輯 (Institutional Port) ============
def _np_ema(arr: np.ndarray, period: int) -> np.ndarray:
    alpha = 2.0 / (period + 1.0)
    out = np.empty_like(arr, dtype=np.float64)
    out[:] = np.nan
    s = 0.0
    n = 0
    for i, v in enumerate(arr):
        if np.isnan(v):
            out[i] = np.nan
            continue
        if n == 0:
            s = v
        else:
            s = alpha * v + (1 - alpha) * s
        out[i] = s
        n += 1
    return out

def _rolling_mean(arr: np.ndarray, period: int) -> np.ndarray:
    out = np.full_like(arr, np.nan, dtype=np.float64)
    if period <= 0 or period > len(arr):
        return out
    csum = np.cumsum(np.insert(arr, 0, 0.0))
    vals = (csum[period:] - csum[:-period]) / float(period)
    out[period-1:] = vals
    return out

def _rolling_std(arr: np.ndarray, period: int) -> np.ndarray:
    out = np.full_like(arr, np.nan, dtype=np.float64)
    if period <= 1 or period > len(arr):
        return out
    csum = np.cumsum(np.insert(arr, 0, 0.0))
    csum2 = np.cumsum(np.insert(arr*arr, 0, 0.0))
    n = period
    mean = (csum[n:] - csum[:-n]) / n
    mean2 = (csum2[n:] - csum2[:-n]) / n
    var = np.maximum(mean2 - mean*mean, 0.0)
    out[n-1:] = np.sqrt(var)
    return out

def SMA(close: np.ndarray, period: int) -> np.ndarray:
    return _rolling_mean(close, period)

def EMA(close: np.ndarray, period: int) -> np.ndarray:
    return _np_ema(close, period)

def WMA(close: np.ndarray, period: int) -> np.ndarray:
    out = np.full_like(close, np.nan, dtype=np.float64)
    if period <= 0 or period > len(close): return out
    weights = np.arange(1, period+1)
    wsum = weights.sum()
    for i in range(period-1, len(close)):
        window = close[i-period+1:i+1]
        if np.any(np.isnan(window)): out[i] = np.nan
        else: out[i] = np.dot(window, weights) / wsum
    return out

def HMA(close: np.ndarray, period: int) -> np.ndarray:
    p2 = max(2, period // 2)
    wma1 = WMA(close, p2)
    wma2 = WMA(close, period)
    diff = 2*wma1 - wma2
    sqrtp = max(2, int(math.sqrt(period)))
    return WMA(diff, sqrtp)

def DEMA(close: np.ndarray, period: int) -> np.ndarray:
    e = EMA(close, period)
    return 2*e - EMA(e, period)

def TEMA(close: np.ndarray, period: int) -> np.ndarray:
    e1 = EMA(close, period)
    e2 = EMA(e1, period)
    e3 = EMA(e2, period)
    return 3*(e1 - e2) + e3

def ROC(close: np.ndarray, period: int) -> np.ndarray:
    out = np.full_like(close, np.nan, dtype=np.float64)
    if period <= 0: return out
    out[period:] = (close[period:] / close[:-period]) - 1.0
    return out

def RSI(close: np.ndarray, period: int) -> np.ndarray:
    close = np.asarray(close, dtype=np.float64)
    p = int(period)
    diff = np.diff(close, prepend=np.nan)
    up = np.where(diff > 0, diff, 0.0)
    dn = np.where(diff < 0, -diff, 0.0)

    def wilder(arr):
        out = np.full_like(arr, np.nan, dtype=np.float64)
        alpha = 1.0 / p
        s = np.nan
        for i, v in enumerate(arr):
            if np.isnan(v): continue
            if np.isnan(s): s = v
            else: s = (1 - alpha) * s + alpha * v
            out[i] = s
        return out

    au = wilder(up)
    ad = wilder(dn)
    rs = au / np.where(ad == 0, np.nan, ad)
    return 100.0 - (100.0 / (1.0 + rs))

def TrueRange(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    prev_close = np.roll(close, 1)
    prev_close[0] = np.nan
    tr1 = high - low
    tr2 = np.abs(high - prev_close)
    tr3 = np.abs(low - prev_close)
    return np.nanmax(np.vstack([tr1, tr2, tr3]), axis=0)

def ATR(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> np.ndarray:
    tr = TrueRange(high, low, close)
    return _np_ema(tr, period)

def Stoch_K(high: np.ndarray, low: np.ndarray, close: np.ndarray, k: int) -> np.ndarray:
    hh = pd.Series(high).rolling(k, min_periods=k).max().values
    ll = pd.Series(low).rolling(k, min_periods=k).min().values
    return (close - ll) / np.where((hh - ll) == 0, np.nan, (hh - ll)) * 100.0

def Stoch_D(high: np.ndarray, low: np.ndarray, close: np.ndarray, k: int, d: int) -> np.ndarray:
    kline = Stoch_K(high, low, close, k)
    return _rolling_mean(kline, d)

def WillR(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> np.ndarray:
    hh = pd.Series(high).rolling(period, min_periods=period).max().values
    ll = pd.Series(low).rolling(period, min_periods=period).min().values
    return (hh - close) / np.where((hh - ll) == 0, np.nan, (hh - ll)) * -100.0

def CCI(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> np.ndarray:
    tp = (high + low + close) / 3.0
    sma = _rolling_mean(tp, period)
    mad = pd.Series(tp).rolling(period, min_periods=period).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True).values
    return (tp - sma) / (0.015 * mad)

def MFI(high: np.ndarray, low: np.ndarray, close: np.ndarray, volume: np.ndarray, period: int) -> np.ndarray:
    tp = (high + low + close) / 3.0
    rmf = tp * volume
    pos = np.where(np.diff(tp, prepend=np.nan) >= 0, rmf, 0.0)
    neg = np.where(np.diff(tp, prepend=np.nan) < 0, rmf, 0.0)
    pos_sum = pd.Series(pos).rolling(period, min_periods=period).sum().values
    neg_sum = pd.Series(neg).rolling(period, min_periods=period).sum().values
    mr = pos_sum / np.where(neg_sum == 0, np.nan, neg_sum)
    return 100.0 - 100.0 / (1.0 + mr)

def OBV(close: np.ndarray, volume: np.ndarray) -> np.ndarray:
    sign = np.sign(np.diff(close, prepend=close[0]))
    return np.cumsum(sign * volume)

def BBANDS(close: np.ndarray, period: int, nstd: float) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    mid = _rolling_mean(close, period)
    sd = _rolling_std(close, period)
    upper = mid + nstd * sd
    lower = mid - nstd * sd
    percent_b = (close - lower) / np.where((upper - lower) == 0, np.nan, (upper - lower))
    return mid, upper, lower, percent_b

def MACD(close: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    macd_line = EMA(close, fast) - EMA(close, slow)
    signal_line = EMA(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

def PPO(close: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    ema_fast = EMA(close, fast)
    ema_slow = EMA(close, slow)
    ppo_line = (ema_fast - ema_slow) / np.where(ema_slow == 0, np.nan, ema_slow) * 100.0
    signal_line = EMA(ppo_line, signal)
    hist = ppo_line - signal_line
    return ppo_line, signal_line, hist

def ADL(high: np.ndarray, low: np.ndarray, close: np.ndarray, volume: np.ndarray) -> np.ndarray:
    mfm = ((close - low) - (high - close)) / np.where((high - low) == 0, np.nan, (high - low))
    mfv = mfm * volume
    return np.nancumsum(mfv)

def CMF(high: np.ndarray, low: np.ndarray, close: np.ndarray, volume: np.ndarray, period: int) -> np.ndarray:
    mfm = ((close - low) - (high - close)) / np.where((high - low) == 0, np.nan, (high - low))
    mfv = mfm * volume
    mfv_sum = pd.Series(mfv).rolling(period, min_periods=period).sum().values
    vol_sum = pd.Series(volume).rolling(period, min_periods=period).sum().values
    return mfv_sum / np.where(vol_sum == 0, np.nan, vol_sum)

def Aroon(high: np.ndarray, low: np.ndarray, period: int) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    up = np.full_like(high, np.nan, dtype=np.float64)
    dn = np.full_like(high, np.nan, dtype=np.float64)
    for i in range(period, len(high)):
        hh_idx = np.argmax(high[i-period+1:i+1])
        ll_idx = np.argmin(low[i-period+1:i+1])
        up[i] = (period - (period-1-hh_idx)) / period * 100.0
        dn[i] = (period - (period-1-ll_idx)) / period * 100.0
    osc = up - dn
    return up, dn, osc

def Vortex(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> Tuple[np.ndarray, np.ndarray]:
    vm_plus = np.abs(high - np.roll(low, 1)); vm_plus[0] = np.nan
    vm_minus = np.abs(low - np.roll(high, 1)); vm_minus[0] = np.nan
    tr = TrueRange(high, low, close)
    tr_sum = pd.Series(tr).rolling(period, min_periods=period).sum().values
    vmp = pd.Series(vm_plus).rolling(period, min_periods=period).sum().values / tr_sum
    vmm = pd.Series(vm_minus).rolling(period, min_periods=period).sum().values / tr_sum
    return vmp, vmm

def ADX(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    up_move = high - np.roll(high, 1); up_move[0] = np.nan
    dn_move = np.roll(low, 1) - low;   dn_move[0] = np.nan
    plus_dm = np.where((up_move > dn_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((dn_move > up_move) & (dn_move > 0), dn_move, 0.0)
    tr = TrueRange(high, low, close)
    atr_ = _np_ema(tr, period)
    plus_di = 100.0 * _np_ema(plus_dm, period) / np.where(atr_ == 0, np.nan, atr_)
    minus_di = 100.0 * _np_ema(minus_dm, period) / np.where(atr_ == 0, np.nan, atr_)
    dx = 100.0 * np.abs(plus_di - minus_di) / np.where((plus_di + minus_di) == 0, np.nan, (plus_di + minus_di))
    adx = _np_ema(dx, period)
    return plus_di, minus_di, adx

def Donchian(high: np.ndarray, low: np.ndarray, period: int) -> Tuple[np.ndarray, np.ndarray]:
    upper = pd.Series(high).rolling(period, min_periods=period).max().values
    lower = pd.Series(low).rolling(period, min_periods=period).min().values
    return upper, lower

def EFI(close: np.ndarray, volume: np.ndarray, period: int) -> np.ndarray:
    fi = (close - np.roll(close, 1)); fi[0] = np.nan
    raw = fi * volume
    return _np_ema(raw, period)

def KAMA(close: np.ndarray, period: int, fast: int = 2, slow: int = 30) -> np.ndarray:
    change = np.abs(close - np.roll(close, period))
    volatility = pd.Series(np.abs(np.diff(close, prepend=np.nan))).rolling(period, min_periods=period).sum().values
    er = change / np.where(volatility == 0, np.nan, volatility)
    sc = (er * (2.0/(fast+1) - 2.0/(slow+1)) + 2.0/(slow+1)) ** 2
    out = np.full_like(close, np.nan, dtype=np.float64)
    for i in range(len(close)):
        if i == 0 or np.isnan(sc[i]) or np.isnan(close[i]):
            out[i] = np.nan
        elif np.isnan(out[i-1]):
            out[i] = close[i]
        else:
            out[i] = out[i-1] + sc[i] * (close[i] - out[i-1])
    return out

def TRIX(close: np.ndarray, period: int) -> np.ndarray:
    e1 = EMA(close, period)
    e2 = EMA(e1, period)
    e3 = EMA(e2, period)
    trix = ROC(e3, 1) * 100.0
    return trix

def DPO(close: np.ndarray, period: int) -> np.ndarray:
    shift = int((period/2)+1)
    sma = SMA(close, period)
    return close - np.roll(sma, shift)

def PVO(volume: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    ema_fast = EMA(volume, fast)
    ema_slow = EMA(volume, slow)
    pvo_line = (ema_fast - ema_slow) / np.where(ema_slow == 0, np.nan, ema_slow) * 100.0
    signal_line = EMA(pvo_line, signal)
    hist = pvo_line - signal_line
    return pvo_line, signal_line, hist

def _calc_laguerre_rsi(src: np.ndarray, gamma: float) -> np.ndarray:
    n = len(src)
    out = np.full(n, np.nan, dtype=np.float64)
    l0 = l1 = l2 = l3 = src[0] if n > 0 else 0.0
    for i in range(n):
        prev_l0, prev_l1, prev_l2, prev_l3 = l0, l1, l2, l3
        l0 = (1.0 - gamma) * src[i] + gamma * prev_l0
        l1 = -gamma * l0 + prev_l0 + gamma * prev_l1
        l2 = -gamma * l1 + prev_l1 + gamma * prev_l2
        l3 = -gamma * l2 + prev_l2 + gamma * prev_l3
        cu = cd = 0.0
        if l0 >= l1: cu += (l0 - l1)
        else: cd += (l1 - l0)
        if l1 >= l2: cu += (l1 - l2)
        else: cd += (l2 - l1)
        if l2 >= l3: cu += (l2 - l3)
        else: cd += (l3 - l2)
        if (cu + cd) != 0.0: out[i] = cu / (cu + cd)
        else: out[i] = 0.0
    return out

def signal_from_family(family: str, o: np.ndarray, h: np.ndarray, l: np.ndarray, c: np.ndarray, v: np.ndarray, params: Dict) -> np.ndarray:
    """Sheep Platform Prod 核心訊號引擎 (30+ 家族大全配整合版)"""
    N = len(c)
    sig = np.zeros(N, dtype=bool)

    try:
        if family == "RSI":
            period = int(params.get("period", 14))
            thr = float(params.get("enter_level", 30.0))
            r = RSI(c, period)
            sig = r <= thr

        elif family == "SMA_Cross":
            f = int(params.get("fast", 10))
            s = int(params.get("slow", 30))
            ma_f = SMA(c, f)
            ma_s = SMA(c, s)
            sig = (ma_f > ma_s) & (np.roll(ma_f, 1) <= np.roll(ma_s, 1))

        elif family == "EMA_Cross":
            f = int(params.get("fast", 12))
            s = int(params.get("slow", 26))
            e1 = EMA(c, f)
            e2 = EMA(c, s)
            sig = (e1 > e2) & (np.roll(e1, 1) <= np.roll(e2, 1))

        elif family == "HMA_Cross":
            f = int(params.get("fast", 12))
            s = int(params.get("slow", 26))
            h1 = HMA(c, f)
            h2 = HMA(c, s)
            sig = (h1 > h2) & (np.roll(h1, 1) <= np.roll(h2, 1))

        elif family == "MACD_Cross":
            f = int(params.get("fast", 12)); s = int(params.get("slow", 26)); sg = int(params.get("signal", 9))
            macd, sigline, _ = MACD(c, f, s, sg)
            sig = (macd > sigline) & (np.roll(macd, 1) <= np.roll(sigline, 1))

        elif family == "PPO_Cross":
            f = int(params.get("fast", 12)); s = int(params.get("slow", 26)); sg = int(params.get("signal", 9))
            ppo, sigline, _ = PPO(c, f, s, sg)
            sig = (ppo > sigline) & (np.roll(ppo, 1) <= np.roll(sigline, 1))

        elif family == "Bollinger_Touch":
            p = int(params.get("period", 20)); nstd = float(params.get("nstd", 2.0))
            _, _, lower, _ = BBANDS(c, p, nstd)
            sig = c <= lower

        elif family == "Stoch_Oversold":
            k = int(params.get("k", 14)); d = int(params.get("d", 3)); lv = float(params.get("enter_level", 20.0))
            kline = Stoch_K(h, l, c, k)
            dline = _rolling_mean(kline, d)
            sig = (kline <= lv) & (np.roll(kline, 1) > lv)

        elif family == "CCI_Oversold":
            p = int(params.get("period", 20)); lv = float(params.get("enter_level", -100.0))
            cci = CCI(h, l, c, p)
            sig = cci <= lv

        elif family == "WillR_Oversold":
            p = int(params.get("period", 14)); lv = float(params.get("enter_level", -80.0))
            w = WillR(h, l, c, p)
            sig = w <= lv

        elif family == "MFI_Oversold":
            p = int(params.get("period", 14)); lv = float(params.get("enter_level", 20.0))
            m = MFI(h, l, c, v, p)
            sig = m <= lv

        elif family == "Donchian_Breakout":
            p = int(params.get("lookback", 20))
            upper, _ = Donchian(h, l, p)
            sig = c >= upper

        elif family == "ADX_DI_Cross":
            p = int(params.get("period", 14))
            plus_di, minus_di, _ = ADX(h, l, c, p)
            sig = (plus_di > minus_di) & (np.roll(plus_di,1) <= np.roll(minus_di,1))

        elif family == "Aroon_Cross":
            p = int(params.get("period", 25))
            up, dn, _ = Aroon(h, l, p)
            sig = (up > dn) & (np.roll(up,1) <= np.roll(dn,1))

        elif family == "ROC_Threshold":
            p = int(params.get("period", 10)); thr = float(params.get("enter_thr", 0.0))
            r = ROC(c, p)
            sig = r > thr

        elif family == "KAMA_Cross":
            p = int(params.get("period", 10))
            k = KAMA(c, p)
            sig = (c > k) & (np.roll(c,1) <= np.roll(k,1))

        elif family == "TRIX_Cross":
            p = int(params.get("period", 15))
            t = TRIX(c, p)
            sig = (t > 0) & (np.roll(t,1) <= 0)

        elif family == "DPO_Revert":
            p = int(params.get("period", 20))
            dpo = DPO(c, p)
            sig = dpo < 0

        elif family == "CMF_Threshold":
            p = int(params.get("period", 20)); thr = float(params.get("enter_thr", 0.0))
            cmf = CMF(h, l, c, v, p)
            sig = cmf > thr

        elif family == "OBV_Slope":
            obv = OBV(c, v)
            slope = obv - np.roll(obv, 1)
            sig = slope > 0

        elif family == "EFI_Threshold":
            p = int(params.get("period", 13)); thr = float(params.get("enter_thr", 0.0))
            efi = EFI(c, v, p)
            sig = efi > thr

        elif family == "ATR_Band_Break":
            p = int(params.get("period", 14)); k = float(params.get("mult", 1.0))
            a = ATR(h, l, c, p)
            base = SMA(c, p)
            upper = base + k * a
            sig = c > upper

        elif family == "Vortex_Cross":
            p = int(params.get("period", 14))
            vmp, vmm = Vortex(h, l, c, p)
            sig = (vmp > vmm) & (np.roll(vmp,1) <= np.roll(vmm,1))

        elif family == "OB_FVG":
            param_N = int(params.get("N", 3))
            param_r = float(params.get("r", 0.001))
            param_h = int(params.get("h", 20))
            param_g = float(params.get("g", 1.0))
            param_a = float(params.get("a", 0.99))
            param_rise_thr = float(params.get("rise_thr", 1.002))
            param_x = float(params.get("x", 1.0))
            param_y = float(params.get("y", 1.0))
            param_monitor_window = int(params.get("monitor_window", 20))
            param_rsi_period = int(params.get("rsi_period", 14))
            param_rsi_diff = float(params.get("rsi_diff", 0.0))

            Bars = len(c)
            sig_arr = np.zeros(Bars, dtype=bool)
            rsi_arr = RSI(c, param_rsi_period)
            vol_avg = pd.Series(v).rolling(param_h).mean().values
            
            for i in range(param_h + 1, Bars - param_N):
                if c[i-1] >= o[i-1]: continue
                is_trend = True
                for k in range(param_N):
                    idx = i + k
                    if idx >= Bars or c[idx] <= o[idx]:
                        is_trend = False; break
                    if (c[idx]-o[idx])/o[idx] <= param_r:
                        is_trend = False; break
                    ref_vol = vol_avg[idx-1] if not np.isnan(vol_avg[idx-1]) else 0.0
                    if v[idx] <= ref_vol * param_g:
                        is_trend = False; break
                
                if is_trend:
                    ob_high = h[i-1]
                    ob_low = l[i-1]
                    trend_end_idx = i + param_N - 1
                    monitor_start = trend_end_idx + 1
                    monitor_end = min(Bars, monitor_start + param_monitor_window)
                    state = 0
                    thresh = ob_high * param_rise_thr
                    dip = ob_low * param_a
                    
                    for k in range(monitor_start, monitor_end):
                        if state == 0 and h[k] >= thresh: state = 1
                        if state == 1 and l[k] <= dip: state = 2
                        ob_rsi_val = rsi_arr[i-1]
                        if state == 2 and c[k] > ob_high and rsi_arr[k] > ob_rsi_val * (1.0 + param_rsi_diff):
                            if not sig_arr[k]: sig_arr[k] = True
                            break
            sig = sig_arr

        elif family == "PVO_Cross":
            f = int(params.get("fast", 12)); s = int(params.get("slow", 26)); sg = int(params.get("signal", 9))
            pvo, sigline, _ = PVO(v, f, s, sg)
            sig = (pvo > sigline) & (np.roll(pvo,1) <= np.roll(sigline,1))

        elif family == "DEMA_Cross":
            f = int(params.get("fast", 10)); s = int(params.get("slow", 30))
            d1 = DEMA(c, f)
            d2 = DEMA(c, s)
            sig = (d1 > d2) & (np.roll(d1, 1) <= np.roll(d2, 1))

        elif family == "TEMA_Cross":
            f = int(params.get("fast", 10)); s = int(params.get("slow", 30))
            t1 = TEMA(c, f)
            t2 = TEMA(c, s)
            sig = (t1 > t2) & (np.roll(t1, 1) <= np.roll(t2, 1))

        elif family == "WMA_Cross":
            f = int(params.get("fast", 10)); s = int(params.get("slow", 30))
            w1 = WMA(c, f)
            w2 = WMA(c, s)
            sig = (w1 > w2) & (np.roll(w1, 1) <= np.roll(w2, 1))

        elif family == "BB_PercentB_Revert":
            p = int(params.get("period", 20)); nstd = float(params.get("nstd", 2.0)); thr = float(params.get("enter_thr", 0.05))
            _, _, _, pb = BBANDS(c, p, nstd)
            sig = pb <= thr

        elif family == "ADL_Slope":
            adl = ADL(h, l, c, v)
            sig = adl > np.roll(adl, 1)

        elif family == "Aroon_Osc_Threshold":
            p = int(params.get("period", 25)); thr = float(params.get("enter_thr", 0.0))
            _, _, osc = Aroon(h, l, p)
            sig = osc > thr

        elif family == "Volatility_Squeeze":
            p = int(params.get("period", 20)); nstd = float(params.get("nstd", 2.0)); q = float(params.get("quantile", 0.2))
            mid, up, low, _ = BBANDS(c, p, nstd)
            bw = (up - low) / mid
            thresh = np.nanquantile(bw, q)
            squeeze = bw <= thresh
            sig = squeeze & (c > up)

        elif family == "RSI_ATR":
            rsi_p1 = int(params.get("rsi_p1", 14))
            rsi_enter = float(params.get("rsi_enter", 30))
            atr_p = int(params.get("atr_p", 14))
            atr_thr = float(params.get("atr_thr", 0.0))
            atr_dir = str(params.get("atr_dir", "above")).lower()

            rsi1 = RSI(c, rsi_p1)
            atr = ATR(h, l, c, atr_p)

            if atr_dir == "below":
                atr_filter = atr < atr_thr
            else:
                atr_filter = atr > atr_thr

            sig = (rsi1 < rsi_enter) & atr_filter

        elif family == "SMC":
            param_len = int(params.get("length", 14))
            param_ob_limit = int(params.get("ob_limit", 300))
            param_reverse = bool(params.get("reverse", False))

            Bars = len(c)
            sig_arr = np.zeros(Bars, dtype=bool)

            MAX_OBS = 500
            obs = np.zeros((MAX_OBS, 5), dtype=np.float64)
            ob_count = 0
            ph_price = np.nan
            pl_price = np.nan
            start_i = max(param_len * 2 + 1, 4)

            for i in range(start_i, Bars):
                p_idx = i - param_len
                is_ph = True
                curr_h = h[p_idx]
                for k in range(1, param_len + 1):
                    if h[p_idx - k] > curr_h: is_ph = False; break
                    if h[p_idx + k] > curr_h: is_ph = False; break
                if is_ph: ph_price = curr_h

                is_pl = True
                curr_l = l[p_idx]
                for k in range(1, param_len + 1):
                    if l[p_idx - k] < curr_l: is_pl = False; break
                    if l[p_idx + k] < curr_l: is_pl = False; break
                if is_pl: pl_price = curr_l
                    
                three_green = (c[i] > o[i]) and (c[i-1] > o[i-1]) and (c[i-2] > o[i-2])
                three_red   = (c[i] < o[i]) and (c[i-1] < o[i-1]) and (c[i-2] < o[i-2])
                
                if three_green and not np.isnan(ph_price):
                    if c[i] > ph_price and c[i-3] < ph_price and c[i-3] < o[i-3]:
                        slot = -1
                        for k in range(MAX_OBS):
                            if obs[k, 3] == 0: slot = k; break
                        if slot == -1:
                            slot = ob_count % MAX_OBS
                            ob_count += 1
                        obs[slot, 0] = h[i-3]
                        obs[slot, 1] = l[i-3]
                        obs[slot, 2] = i-3
                        obs[slot, 3] = 1
                        obs[slot, 4] = 1
                
                if three_red and not np.isnan(pl_price):
                    if c[i] < pl_price and c[i-3] > pl_price and c[i-3] > o[i-3]:
                        slot = -1
                        for k in range(MAX_OBS):
                            if obs[k, 3] == 0: slot = k; break
                        if slot == -1:
                            slot = ob_count % MAX_OBS
                            ob_count += 1
                        obs[slot, 0] = h[i-3]
                        obs[slot, 1] = l[i-3]
                        obs[slot, 2] = i-3
                        obs[slot, 3] = 1
                        obs[slot, 4] = -1

                sig_break_bull = False
                sig_break_bear = False
                
                for k in range(MAX_OBS):
                    if obs[k, 3] == 1:
                        if (i - obs[k, 2]) > param_ob_limit:
                            obs[k, 3] = 0; continue
                        if obs[k, 4] == 1:
                            if c[i] < obs[k, 1]: obs[k, 3] = 0; sig_break_bull = True
                        else:
                            if c[i] > obs[k, 0]: obs[k, 3] = 0; sig_break_bear = True

                if not param_reverse:
                    if sig_break_bear: sig_arr[i] = True
                else:
                    if sig_break_bull: sig_arr[i] = True
            sig = sig_arr

        elif family == "LaguerreRSI_TEMA":
            p_tema_len = int(params.get("tema_len", 30))
            p_gamma = float(params.get("gamma", 0.5))
            p_ema1_w = int(params.get("ema1_w", 9))
            p_ema2_w = int(params.get("ema2_w", 20))
            p_ema3_w = int(params.get("ema3_w", 40))
            
            ts_arr = params.get("_ts", None)
            if ts_arr is None:
                ts_arr = pd.date_range(start="2000-01-01", periods=len(c), freq="30min")

            hl2 = (h + l) / 2.0
            e1 = EMA(hl2, p_tema_len)
            e2 = EMA(e1, p_tema_len)
            e3 = EMA(e2, p_tema_len)
            tema_val = 3 * e1 - 3 * e2 + e3

            lag_rsi = _calc_laguerre_rsi(c, p_gamma)

            idx_ts = pd.DatetimeIndex(ts_arr)
            ser_c = pd.Series(c, index=idx_ts)
            w_close = ser_c.resample('W-MON').last()
            
            def _pd_ema(ser, span): return ser.ewm(span=span, adjust=False).mean()
            we1 = _pd_ema(w_close, p_ema1_w)
            we2 = _pd_ema(w_close, p_ema2_w)
            we3 = _pd_ema(w_close, p_ema3_w)
            
            w_ema1 = np.nan_to_num(we1.reindex(idx_ts).ffill().values)
            w_ema2 = np.nan_to_num(we2.reindex(idx_ts).ffill().values)
            w_ema3 = np.nan_to_num(we3.reindex(idx_ts).ffill().values)

            logic_rsi_up = (lag_rsi > np.roll(lag_rsi, 1))
            logic_ma_stack = (w_ema1 > w_ema2) & (w_ema2 > w_ema3)
            sig = logic_rsi_up & logic_ma_stack & np.isfinite(tema_val) & (w_ema1 > 0)

        elif family == "TEMA_RSI":
            p_fast_len = int(params.get("fast_len", 3))
            p_slow_len = int(params.get("slow_len", 100))
            p_rsi_len = int(params.get("rsi_len", 14))
            p_rsi_thr = int(params.get("rsi_thr", 20))

            fast_ema = TEMA(c, p_fast_len)
            slow_ema = TEMA(c, p_slow_len)
            rsi_val = RSI(c, p_rsi_len)

            def is_rising_vec(arr, length):
                out = np.full_like(arr, True, dtype=bool)
                for k in range(length):
                    out &= (np.roll(arr, k) > np.roll(arr, k+1))
                return out
                
            def is_falling_vec(arr, length):
                out = np.full_like(arr, True, dtype=bool)
                for k in range(length):
                    out &= (np.roll(arr, k) < np.roll(arr, k+1))
                return out

            c1 = fast_ema < slow_ema
            c2 = is_rising_vec(fast_ema, 3) & is_falling_vec(slow_ema, 3)
            c4 = is_rising_vec(fast_ema, 4) & is_rising_vec(slow_ema, 3)
            c5 = (fast_ema > slow_ema) & (np.roll(fast_ema, 1) <= np.roll(slow_ema, 1))
            c6 = rsi_val > p_rsi_thr
            rsi_cross = (rsi_val > 30) & (np.roll(rsi_val, 1) <= 30)

            sig = (c1 & c2 & c6) | (c4 & c6) | (c5 & c6) | rsi_cross
        else:
            log(f"未實作的家族: {family}")
            
    except Exception as e:
        log(f"策略 {family} 計算錯誤: {e}\n{traceback.format_exc()}")
        sig = np.zeros(N, dtype=bool)

    # 專家級防護：無縫過濾來自回測系統的回傳 tuple (例如只取第一個信號陣列)
    if isinstance(sig, tuple):
        sig = sig[0]
        
    sig = sig & np.isfinite(c)
    sig[:2] = False
    return sig
    


    

def _synth_klines(interval: str, limit: int, anchor_price: float=2000.0) -> pd.DataFrame:
    """本地合成隨機漫步 K 線（占位用，不可用於真實交易）。"""
    iv = INTERVAL_MS.get(interval, 60000)
    now_ms = int(time.time()*1000)
    start = now_ms - (limit-1)*iv
    prices = []
    px = float(anchor_price)
    for _ in range(limit):
        step = random.gauss(0, max(0.0008, 0.0003 if interval in ("1m","3m") else 0.0006))
        px = max(1.0, px * (1 + step))
        o = px * (1 + random.uniform(-0.0006, 0.0006))
        c = px * (1 + random.uniform(-0.0006, 0.0006))
        h = max(o, c) * (1 + random.uniform(0.0001, 0.0012))
        l = min(o, c) * (1 - random.uniform(0.0001, 0.0012))
        v = random.uniform(10, 1000)
        prices.append([o,h,l,c,v])
    rows = []
    t = start
    for i in range(limit):
        rows.append([t, *prices[i]])
        t += iv
    df = pd.DataFrame(rows, columns=["time","open_price","high_price","low_price","close_price","volume"])
    df["ts"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    df.attrs["source"] = "synthetic"
    return df

def fetch_klines(client: "BitmartClient", symbol: str, interval: str, limit: int=200, safe: bool=True) -> pd.DataFrame:
    """
    [專家翻新] 高可用 K 線獲取模組：支持 30m 自動對齊與異常回退。
    修正：嚴格限制單次請求最大 500 筆 (BitMart API 限制)。
    """
    # [API 限制修正] BitMart 接口限制單次最多 500 筆，強制截斷
    limit = min(limit, 500)

    step_map = {"1m":1, "3m":3, "5m":5, "15m":15, "30m":30, "1h":60, "2h":120, "4h":240}
    step = step_map.get(interval, 30) # 預設對應 30m
    
    # [防呆] 時間戳對齊：確保 start_time 為 step 的整數倍，防止數據空洞
    # BitMart V2 API 接受的是「秒」為單位的 start_time / end_time
    now_sec = int(time.time())
    # 確保請求範圍涵蓋到最新的完整 K 棒
    end_ts = now_sec - (now_sec % (step * 60)) + (step * 60)
    start_ts = end_ts - (step * 60 * limit)
    
    # [Expert Fix] BitMart V2 K-Line 端點必須使用標準格式 ETHUSDT (無底線)
    sym_bm = symbol.replace("-", "").replace("_", "")

    try:
        # 使用 BitMart V2 Futures K-Line
        url = "https://api-cloud-v2.bitmart.com/contract/public/kline"
        params = {
            "symbol": sym_bm,
            "step": step,
            "start_time": start_ts,
            "end_time": end_ts
        }
        
        # 即使是 Public 接口，加入 User-Agent 也能減少被 WAF 阻擋機率
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        
        sess = getattr(client, "session", None) or requests
        resp = sess.get(url, params=params, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        if int(data.get("code", 0)) != 1000:
             raise ValueError(f"API Return Code: {data.get('code')} Msg: {data.get('message')}")

        raw_list = data.get("data", [])
        rows = []
        for item in raw_list:
            if isinstance(item, dict):
                # V2 timestamp 通常為秒，但也可能為毫秒，這裡做自動判斷
                ts_raw = int(item.get("timestamp", 0))
                # 簡單判定：若時間戳小於 100 億，判定為秒，轉為毫秒供 pandas 使用
                if ts_raw < 10000000000: 
                    t = ts_raw * 1000
                else:
                    t = ts_raw
                
                o = float(item.get("open_price", 0))
                h = float(item.get("high_price", 0))
                l = float(item.get("low_price", 0))
                cl = float(item.get("close_price", 0))
                v = float(item.get("volume", 0))
                rows.append([t, o, h, l, cl, v])

        
        if rows:
            rows.sort(key=lambda x: x[0])
            # [去重] 防止 API 邊界回傳重複數據
            df = pd.DataFrame(rows, columns=["time","open_price","high_price","low_price","close_price","volume"])
            df.drop_duplicates(subset=["time"], keep="last", inplace=True)
            
            # 轉換時間格式
            df["ts"] = pd.to_datetime(df["time"], unit="ms", utc=True)
            df.attrs["source"] = "live"
            return df
        else:
            raise ValueError("Empty data list from API")

    except Exception as e:
        log(f"行情數據讀取受阻 ({sym_bm})，系統將重試: {e}")
        pass

    if safe:
        anchor = 2000.0
        try:
            anchor = client.get_last_price(symbol) or 2000.0
        except: pass
        
        df = _synth_klines(interval, max(50, limit), anchor_price=anchor)
        log("【系統警示】無法取得交易所即時 K 線，已切換至「模擬數據模式」維持運轉")
        return df

    raise RuntimeError("K 線獲取失敗且未啟用安全模式")


def round_to_step(x: float, step: float, mode: str="round") -> float:
    try:
        x = float(x); step = float(step)
    except Exception:
        return float(x)
    if step <= 0: return float(x)
    n = x / step
    if mode == "floor":
        n = math.floor(n + 1e-12)
    elif mode == "ceil":
        n = math.ceil(n - 1e-12)
    else:
        n = round(n)
    return float(n * step)

# ============ WebSocket 行情引擎 (極速 0 延遲 - 多幣種支援) ============
class MultiWsKlineManager:
    def __init__(self, client, sym_intervals: List[Tuple[str, str]]):
        self.client = client
        self.sym_intervals = list(set(sym_intervals))
        self.dfs = {}
        self.latest_closes = {}
        self.lock = threading.Lock()
        self.ws = None
        self.ready = False
        
        self.step_map = {"1m":"1m", "3m":"3m", "5m":"5m", "15m":"15m", "30m":"30m", "1h":"1H", "2h":"2H", "4h":"4H"}
        
        for sym, iv in self.sym_intervals:
            sym_bm = sym.replace("-", "").replace("_", "").upper()
            key = f"{sym_bm}_{iv}"
            self.dfs[key] = pd.DataFrame()
            self.latest_closes[sym_bm] = 0.0

    def start(self):
        if not WS_OK:
            log("【致命錯誤】系統缺乏 websocket-client，請在終端機執行: pip install websocket-client")
            return
        log(f"[WS] 準備啟動多幣種 WebSocket 行情引擎: {self.sym_intervals}...")
        t0 = time.perf_counter()
        
        for sym, iv in self.sym_intervals:
            sym_bm = sym.replace("-", "").replace("_", "").upper()
            key = f"{sym_bm}_{iv}"
            df = fetch_klines(self.client, sym_bm, iv, 500, safe=True)
            self.dfs[key] = df
            if not df.empty:
                self.latest_closes[sym_bm] = float(df["close_price"].iloc[-1])
                
        log(f"[WS] REST 初始化歷史 K 線完成，耗時: {time.perf_counter() - t0:.4f} 秒")
        self.ready = True
        threading.Thread(target=self._run_ws, daemon=True).start()

    def _run_ws(self):
        while not stop_event.is_set():
            try:
                url = "wss://openapi-ws-v2.bitmart.com/api?protocol=1.1"
                self.ws = websocket.WebSocketApp(
                    url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                log(f"[WS] 異常中斷，準備重連: {e}")
            time.sleep(3)

    def _on_open(self, ws):
        args = []
        for sym, iv in self.sym_intervals:
            sym_bm = sym.replace("-", "").replace("_", "").upper()
            ws_step = self.step_map.get(iv, "1m")
            args.append(f"futures/klineBin{ws_step}:{sym_bm}")
            args.append(f"futures/ticker:{sym_bm}")
            
        args = list(set(args)) # 去重
        sub = {"action": "subscribe", "args": args}
        ws.send(json.dumps(sub))
        log(f"[WS] 訂閱送出: {len(args)} 個通道")

    def _on_error(self, ws, error):
        log(f"[WS] 異常: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        log(f"[WS] 連線關閉: {close_msg}")

    def _on_message(self, ws, msg):
        if "pong" in msg.lower(): 
            return
        try:
            data = json.loads(msg)
            if "data" not in data:
                return

            group = data.get("group", "")
            payload = data.get("data", {})
            
            if isinstance(payload, list):
                if len(payload) == 0: return
                payload_dict = payload[0]
            else:
                payload_dict = payload
                
            if not isinstance(payload_dict, dict):
                return
            
            # Ticker 更新
            if "ticker" in group:
                sym_bm = group.split(":")[-1].upper()
                px = float(payload_dict.get("last_price", 0))
                if px > 0:
                    self.latest_closes[sym_bm] = px
                    with self.lock:
                        for key, df in self.dfs.items():
                            if key.startswith(f"{sym_bm}_") and not df.empty:
                                df.loc[df.index[-1], "close_price"] = px
                                curr_h = float(df.loc[df.index[-1], "high_price"])
                                curr_l = float(df.loc[df.index[-1], "low_price"])
                                if px > curr_h: df.loc[df.index[-1], "high_price"] = px
                                if px < curr_l: df.loc[df.index[-1], "low_price"] = px
                return

            # K 線更新
            if "kline" in group:
                parts = group.split(":")
                if len(parts) == 2:
                    ch_prefix = parts[0]
                    sym_bm = parts[1].upper()
                    ws_step_str = ch_prefix.replace("futures/klineBin", "")
                    inv_step_map = {v:k for k,v in self.step_map.items()}
                    iv = inv_step_map.get(ws_step_str, "1m")
                    key = f"{sym_bm}_{iv}"
                    
                    if "items" in payload_dict:
                        for k in payload_dict["items"]:
                            self._update_kline(key, sym_bm, k)
                            
        except Exception as e:
            pass

    def _update_kline(self, key, sym_bm, k):
        with self.lock:
            df = self.dfs.get(key)
            if df is None or df.empty: return
            
            t_raw = int(k.get("timestamp", k.get("time", k.get("t", k.get("ts", 0)))))
            if t_raw == 0: return
            t_ms = t_raw * 1000 if t_raw < 10000000000 else t_raw
            
            o = float(k.get("open_price", k.get("o", 0)))
            h = float(k.get("high_price", k.get("h", 0)))
            l = float(k.get("low_price", k.get("l", 0)))
            c = float(k.get("close_price", k.get("c", 0)))
            v = float(k.get("volume", k.get("v", 0)))

            self.latest_closes[sym_bm] = c

            last_t = int(df["time"].iloc[-1])
            if t_ms == last_t:
                df.loc[df.index[-1], ["open_price", "high_price", "low_price", "close_price", "volume"]] = [o, h, l, c, v]
            elif t_ms > last_t:
                new_row = {"time": t_ms, "open_price": o, "high_price": h, "low_price": l, "close_price": c, "volume": v, "ts": pd.to_datetime(t_ms, unit="ms", utc=True)}
                self.dfs[key] = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                if len(self.dfs[key]) > 500:
                    self.dfs[key] = self.dfs[key].iloc[-500:].reset_index(drop=True)

    def get_df(self, symbol: str, interval: str):
        sym_bm = symbol.replace("-", "").replace("_", "").upper()
        key = f"{sym_bm}_{interval}"
        with self.lock:
            df = self.dfs.get(key)
            return df.copy() if df is not None else pd.DataFrame()

    def get_latest_close(self, symbol: str) -> float:
        sym_bm = symbol.replace("-", "").replace("_", "").upper()
        return float(self.latest_closes.get(sym_bm, 0.0))

# ============ 交易器（無 ATR；含本地日內停利/停損） ============
class Trader:
    """
    - 多幣種與多策略並發支援，每個策略可獨立指定 symbol 與 interval。
    - 不使用 ATR。
    - 日內停利/停損採**本地記帳**（僅本程式期間），重啟即重置。
    - K 線來源若為合成（synthetic）→ 絕不開倉，僅等待數據恢復。
    - 交易所觸發單拒絕 → 啟用本地 TP/SL 守護，確保可出場與記帳。
    """
    def __init__(self, client: BitmartClient, cfg: dict):
        self.c = client
        self.cfg = cfg

        # 基本全局設定 (作為策略缺省值)
        self.global_symbol = cfg["symbol"].upper()
        self.global_interval = cfg["interval"]
        self.use_mark = bool(cfg.get("use_mark_price", True))
        self.sleep_pad = float(cfg.get("sleep_padding_sec", 0.5))
        self.trade_fetch_interval = int(cfg.get("trade_fetch_interval", 60))
        self.verbose = bool(cfg.get("verbose", True))
        self.default_qty = float(cfg.get("order_qty_token", 1))
        self.fee_bps = float(cfg.get("fee_bps", 2.0))
        self.slip_bps = float(cfg.get("slip_bps", 0.0))
        self.max_retries = int(cfg.get("max_retries", 3))
        self.max_retry_total_sec = float(cfg.get("max_retry_total_sec", 3.5))

        # 本地日內停利/停損
        dguard = cfg.get("daily_guard", {})
        self.enable_daily_guard = bool(dguard.get("enable", True))
        self.daily_limit_pct = clamp(dguard.get("limit_pct", 1.0), 0.0, 100.0) / 100.0
        self.daily_limit_usdt = max(0.0, float(dguard.get("limit_usdt", 0.0)))

        # 本地計帳
        self.local_realized_usdt = 0.0
        self.local_day_anchor = datetime.now(TZ8).date()
        self.local_equity_baseline = self._safe_get_equity()

        # [多策略相容系統] 載入所有策略家族配置並萃取所有 Symbol/Interval
        self.strategies_cfg = {}
        self.active_sym_intervals = []
        
        mode = cfg.get("mode", "single")
        if mode == "multi":
            try:
                multi_json_raw = cfg.get("multi_strategies_json", "[]")
                if isinstance(multi_json_raw, (list, dict)):
                    multi_json = multi_json_raw
                else:
                    multi_json_text = str(multi_json_raw or "").strip()
                    multi_json = [] if not multi_json_text else json.loads(multi_json_text)
                self.strategies_cfg, self.active_sym_intervals = normalize_multi_strategy_entries(
                    multi_json,
                    self.global_symbol,
                    self.global_interval,
                    float(cfg.get('TEMA_RSI', {}).get('stake_pct', 95.0)),
                )
            except Exception as e:
                log(f"解析多策略 JSON 失敗: {e}，回退至單一模式。")
                mode = "single"

        if not self.strategies_cfg or mode == "single":
            p = cfg.get("TEMA_RSI", {})
            default_direction = normalize_direction(p.get("direction"), reverse=p.get("reverse"), default="long")
            p = dict(p or {})
            p["direction"] = default_direction
            p["reverse"] = default_direction == "short"
            self.strategies_cfg["DEFAULT_STRAT"] = {
                "family": cfg.get("single_family", "TEMA_RSI"),
                "params": p,
                "direction": default_direction,
                "tp_pct": float(p.get("tp_pct_strat", 0.1)),
                "sl_pct": float(p.get("sl_pct_strat", 0.1)),
                "max_hold": int(p.get("max_hold_list", [300])[0] if p.get("max_hold_list") else 300),
                "stake_pct": float(p.get("stake_pct", 95.0)),
                "symbol": self.global_symbol,
                "interval": self.global_interval
            }
            self.active_sym_intervals.append((self.global_symbol, self.global_interval))

        # 去重 symbol 組合
        self.active_sym_intervals = list(set(self.active_sym_intervals))
        self.all_symbols = list(set([s for s, i in self.active_sym_intervals]))

        self.is_multi_pos = len(self.strategies_cfg) > 1
        if self.is_multi_pos:
            log("【系統提示】啟用多策略並發，強制轉為純本地守護模式以防止交易所掛單衝突。")

        self.positions: Dict[str, Dict[str, Any]] = {}
        for strat_id, strat_cfg in self.strategies_cfg.items():
            self.positions[strat_id] = {
                "in_pos": None,
                "position_id": None,
                "entry_bar_index": None,
                "entry_avg": None,
                "entry_qty": None,
                "tp_price": None,
                "sl_price": None,
                "local_guard_active": False,
                "entry_open_ms": None,
                "trailing_active": False,
                "trailing_max_price": 0.0,
                "entry_price_snapshot": 0.0,
                "fixed_tp_price": 0.0,
                "fixed_sl_price": 0.0,
                "cooldown": 0,
                "cfg": strat_cfg
            }

        # 獲取多幣種過濾器資訊
        self.symbol_info = {}
        self._init_symbol_filters()

        self.daily_halt_active = False
        self.daily_halt_until: Optional[datetime.date] = None
        self.manual_test_trigger = False 

        self.last_trade_fetch_ts = 0.0
        self._last_status_ts = 0.0 

        self.guard_poll_sec = float(cfg.get("guard_poll_sec", 1.0))
        self._last_guard_ts = 0.0

        # 初始化多幣種 WS 引擎
        self.ws_kline = MultiWsKlineManager(self.c, self.active_sym_intervals)

        # 設定所有幣種的模式
        self._ensure_modes()

    def _fmt_price(self, symbol: str, price: float) -> str:
        if price is None or price <= 0:
            return None
        
        info = self.symbol_info.get(symbol.upper(), {"price_step": 0.0001})
        raw_step = info["price_step"]
        try:
            d_price = Decimal(f"{price:.20f}")
            d_step = Decimal(str(raw_step))
            
            if "ETHUSDT" in symbol.replace("-","") and d_step < 1:
                 if d_step != Decimal("0.01"):
                     d_step = Decimal("0.01")

            quantized = d_price.quantize(d_step, rounding=ROUND_FLOOR)
            return "{:f}".format(quantized)
        except Exception as e:
            log(f"fmt_price error for {symbol}: {e}")
            return str(price)

    def _safe_get_equity(self) -> Optional[float]:
        t_start = time.perf_counter()
        if hasattr(self, '_cached_eq') and hasattr(self, '_cached_eq_ts'):
            if t_start - self._cached_eq_ts < 20.0:
                return self._cached_eq
        try:
            bal = self.c.get_balance()
            cand = []
            def _add(v):
                try:
                    if v is not None: cand.append(float(v))
                except Exception: pass
            d = bal.get("data") or {}
            b = d.get("balance") or {}
            for k in ("equity","totalEquity","marginBalance","totalBalance","walletBalance","availableBalance","availableMargin"):
                _add(b.get(k)); _add(d.get(k))
            res = max(cand) if cand else None
            if res is not None:
                self._cached_eq = res
                self._cached_eq_ts = time.perf_counter()
            return res
        except Exception as e:
            log(f"取得淨值失敗: {e}")
            return None

    def _safe_get_mark_price(self, symbol: str) -> float:
        if hasattr(self, 'ws_kline'):
            px = self.ws_kline.get_latest_close(symbol)
            if px > 0:
                return px
        try:
            if self.use_mark:
                px = float(self.c.get_mark_price(symbol) or 0.0)
            else:
                px = float(self.c.get_last_price(symbol) or 0.0)
            if px > 0:
                return px
        except Exception:
            pass
        return 0.0

    def _init_symbol_filters(self):
        """獲取所有涉及幣種的步進與限制"""
        try:
            info = self.c.get_contracts()
            contracts = info.get("data", []) or []
        except Exception as e:
            log(f"取得合約清單失敗：{e}")
            contracts = []

        for sym in self.all_symbols:
            sym_clean = sym.replace("-", "").replace("_", "")
            
            # 強制硬編碼防護
            if "ETHUSDT" in sym_clean:
                self.symbol_info[sym] = {"price_step": 0.01, "qty_step": 0.001, "min_qty": 0.001}
                log(f"交易規格鎖定 {sym} | 價格跳動: 0.01 | 最小數量: 0.001")
                continue
            if "BTCUSDT" in sym_clean:
                self.symbol_info[sym] = {"price_step": 0.1, "qty_step": 0.001, "min_qty": 0.001}
                continue

            # API 讀取
            matched = False
            for it in contracts:
                if it.get("symbol") == sym or it.get("symbol") == sym_clean:
                    p_step = safe_float(it.get("priceStep") or it.get("tickSize") or 0.0001, 0.0001)
                    q_step = safe_float(it.get("quantityStep") or it.get("stepSize") or 1, 1)
                    m_qty = safe_float(it.get("minQty") or it.get("minOrderSize") or 1, 1)
                    self.symbol_info[sym] = {"price_step": p_step, "qty_step": q_step, "min_qty": m_qty}
                    matched = True
                    break
            
            if not matched:
                self.symbol_info[sym] = {"price_step": 0.0001, "qty_step": 1.0, "min_qty": 1.0}

    def _ensure_modes(self):
        try:
            m = self.c.get_position_mode()
            dual = bool((m.get("data") or {}).get("dualSidePosition", True))
            if not dual:
                log("偵測到單邊持倉模式，正在切換至雙向持倉 (Hedge Mode)...")
                self.c.set_position_mode(True)
        except Exception as e:
            log(f"持倉模式校正失敗 (非致命): {e}")

        for sym in self.all_symbols:
            try:
                mt = self.c.get_margin_type(sym)
                marginType = (mt.get("data") or {}).get("marginType","").upper()
                if marginType in ("ISOLATED","SEPARATE","SEPARATED","SEPARATE_ISOLATED"):
                    log(f"偵測到 {sym} 逐倉模式，正在切換至全倉 (Cross Mode)...")
                    self.c.set_margin_type(sym, "CROSSED")
            except Exception as e:
                log(f"保證金模式校正失敗 ({sym}): {e}")
            
            try:
                self.c.set_leverage(sym, 5)
                log(f"系統已強制設定 {sym} 為 5x 槓桿以匹配下單參數")
            except Exception as e:
                log(f"槓桿初始化警示 ({sym}): {e}")
    def _loop(self):
        try:
            if stop_event.is_set():
                return
            time.sleep(0.01)
        except Exception as e:
            log(f"_loop internal error（忽略）: {e}")

    # ----- 訊號 -----
    def compute_signals(self) -> Dict[str, Tuple[bool, bool, int]]:
        """
        [多幣種適配 - 高效快取版] 遍歷所有載入的策略並計算各自的訊號。
        引入 K 棒時間戳快取機制，避免每 0.1 秒重複計算龐大指標，同時精準定位訊號來源。
        """
        signals = {}

        for sid, strat in self.strategies_cfg.items():
            sym = strat["symbol"]
            iv = strat["interval"]
            df = self.ws_kline.get_df(sym, iv)
            
            if df.empty:
                signals[sid] = (False, False, -1)
                continue
                
            iv_ms = INTERVAL_MS.get(iv, 60000)
            eval_i = _last_closed_kline_index(df, iv_ms)

            if eval_i < 1:
                signals[sid] = (False, False, -1)
                continue

            closed_kline_ts = int(df["time"].iloc[eval_i])
            pos_data = self.positions[sid]
            
            # 若時間戳未變更，直接回傳快取結果 (節省 99.9% 運算資源)
            if pos_data.get("last_eval_ts") == closed_kline_ts:
                signals[sid] = pos_data.get("cached_sig", (False, False, closed_kline_ts))
                continue

            o = df["open_price"].values.astype(np.float64)
            h = df["high_price"].values.astype(np.float64)
            l = df["low_price"].values.astype(np.float64)
            c = df["close_price"].values.astype(np.float64)
            v = df["volume"].values.astype(np.float64)

            o_sub = o[:eval_i+1]
            h_sub = h[:eval_i+1]
            l_sub = l[:eval_i+1]
            c_sub = c[:eval_i+1]
            v_sub = v[:eval_i+1]

            fam = strat["family"]
            prm = strat["params"]
            
            try:
                prm_run = prm.copy()
                prm_run["_ts"] = df["ts"].iloc[:eval_i+1].values
                direction = normalize_direction(strat.get("direction"), reverse=prm_run.get("reverse"), default="long")
                prm_run["direction"] = direction
                prm_run["reverse"] = direction == "short"
                
                sig_arr = signal_from_family(fam, o_sub, h_sub, l_sub, c_sub, v_sub, prm_run)
                if isinstance(sig_arr, tuple):
                    sig_arr = sig_arr[0]
                    
                trigger_sig = bool(sig_arr[-1])
                long_sig = bool(trigger_sig and direction == "long")
                short_sig = bool(trigger_sig and direction == "short")
                    
                ans = (long_sig, short_sig, closed_kline_ts)
                signals[sid] = ans
                
                pos_data["last_eval_ts"] = closed_kline_ts
                pos_data["cached_sig"] = ans
                
                if long_sig or short_sig:
                    log(f"【訊號捕捉】{sid} ({fam} - {sym}) 條件成立！(K棒時間: {closed_kline_ts})")
            except Exception as e:
                log(f"策略 {sid} 計算異常: {e}\n{traceback.format_exc()}")
                signals[sid] = (False, False, closed_kline_ts)
                
        return signals

    # ----- 本地日內停利/停損 -----
    def _reset_day_if_needed(self):
        today = datetime.now(TZ8).date()
        if today != self.local_day_anchor:
            self.local_day_anchor = today
            self.local_realized_usdt = 0.0
            self.local_equity_baseline = self._safe_get_equity()
            self.daily_halt_active = False
            self.daily_halt_until = None
            log(f"New day. Reset PnL. Base equity: {self.local_equity_baseline}")

    def daily_guard_check(self):
        if not self.enable_daily_guard:
            return
        self._reset_day_if_needed()

        pct_hit = False
        pct_val = None
        if self.local_equity_baseline and self.local_equity_baseline > 0:
            pct_val = self.local_realized_usdt / self.local_equity_baseline
            if abs(pct_val) >= self.daily_limit_pct:
                pct_hit = True

        amt_hit = (self.daily_limit_usdt > 0 and abs(self.local_realized_usdt) >= self.daily_limit_usdt)

        if pct_hit or amt_hit:
            pct_show = f"{pct_val*100:.2f}%" if pct_val is not None else "N/A"
            log(f"DAILY LIMIT HIT: PnL {self.local_realized_usdt:.2f} ({pct_show}). Halt trading.")
            self.flat_all()
            self.daily_halt_active = True
            self.daily_halt_until = datetime.now(TZ8).date() + timedelta(days=1)

    @staticmethod
    def _bps(bps: float) -> float:
        return bps / 10_000.0

    def _apply_cost_side(self, tag: str, px: float, fee_bps=2.0, slip_bps=0.0) -> float:
        mult = 1.0 - self._bps(fee_bps)
        slip = self._bps(slip_bps)
        if tag == "LONG_ENTRY":
            price = px * (1 + slip)
        elif tag == "SHORT_ENTRY":
            price = px * (1 - slip)
        elif tag == "LONG_EXIT":
            price = px * (1 - slip)
        else:
            price = px * (1 + slip)
        return price * mult

    def _calc_qty_from_stake(self, strat_id: str) -> float:
        strat_cfg = self.strategies_cfg.get(strat_id, {})
        sym = strat_cfg.get("symbol", self.global_symbol)
        qty_step = self.symbol_info.get(sym.upper(), {}).get("qty_step", 1.0)
        min_qty = self.symbol_info.get(sym.upper(), {}).get("min_qty", 1.0)

        t_calc_start = time.perf_counter()
        log(f"[測速] 開始執行 _calc_qty_from_stake [{strat_id} - {sym}]...")
        try:
            equity = self._safe_get_equity()
            if equity is None or equity <= 0:
                log(f"[{strat_id}] 獲取淨值失敗，為保護帳戶放棄開倉。")
                return 0.0 
            
            mark = self._safe_get_mark_price(sym)
            if mark <= 0: 
                return 0.0

            # [專家級防護] 導入槓桿換算與嚴格餘額預先驗資
            stake_pct = float(strat_cfg.get("stake_pct", 95.0))
            leverage = 5.0 # 系統全域強制設定為 5x
            
            # 計算該策略計畫使用的保證金 (Margin)
            target_margin = equity * (stake_pct / 100.0)
            
            # 轉換為合約價值 (Notional) 與 絕對數量 (Qty)
            target_notional = target_margin * leverage
            raw_qty = target_notional / mark
            
            # 計算該幣種「最小下單量」的基礎保證金要求
            min_notional = min_qty * mark
            min_margin_req = min_notional / leverage
            
            # [物理極限防護] 如果帳戶「全部可用淨值的 98%」都買不起最小數量，直接回傳 0 避免 API 撞牆報錯
            if (equity * 0.98) < min_margin_req:
                log(f"[{strat_id}] 物理餘額不足！開倉需 {min_margin_req:.2f} U 保證金，總淨值僅 {equity:.2f} U，放棄開倉。")
                return 0.0
                
            final_qty = max(min_qty, round_to_step(raw_qty, qty_step, "floor"))
            
            # [動態資金防護] 檢查硬拉到 min_qty 後，所需的保證金是否嚴重超標
            actual_margin_req = (final_qty * mark) / leverage
            if actual_margin_req > target_margin:
                log(f"[{strat_id}] 提示：目標保證金 {target_margin:.2f} U 不足最小門檻，自動提升至最低要求 {actual_margin_req:.2f} U")
                # 如果提升後的保證金超過總淨值的 95%，代表即使買得起最小量，也會導致帳戶幾乎滿倉，直接攔截
                if actual_margin_req > (equity * 0.95):
                    log(f"[{strat_id}] 提升後超出總淨值安全線 (95%)，為保護帳戶安全，取消下單。")
                    return 0.0
            
            t_calc_end = time.perf_counter()
            log(f"倉位計算 [{strat_id}]: 淨值={equity:.2f} * {stake_pct}% -> 目標保證金:{target_margin:.2f} U (合約價值:{target_notional:.2f}) @ 價格{mark:.2f} -> 實際下單數量:{final_qty}")
            return final_qty
        except Exception as e:
            log(f"[{strat_id}] 倉位計算錯誤: {e}")
            return 0.0

    def open_market(self, side: str, strat_id: str) -> Tuple[Optional[str], float, float, float, float]:
        t_open_market_start = time.perf_counter()
        strat_sym = self.strategies_cfg[strat_id]["symbol"]
        log(f"[測速] =================== 進入 open_market ({side}) [{strat_id} - {strat_sym}] ===================")
        qty = self._calc_qty_from_stake(strat_id)
        if qty <= 0:
            log(f"[測速] open_market 提早退出 (qty<=0)")
            return None, 0.0, 0.0, 0.0, 0.0

        side_u = (side or "").upper()
        if side_u not in ("LONG", "SHORT"):
            log(f"開倉參數錯誤: side={side}")
            return None, 0.0, 0.0, 0.0, 0.0

        sym_norm = strat_sym.replace("-", "").replace("_", "").upper()
        pos_side = "LONG" if side_u == "LONG" else "SHORT"
        side_bm = "BUY" if side_u == "LONG" else "SELL"
        tag = "LONG_ENTRY" if side_u == "LONG" else "SHORT_ENTRY"
        position_id = f"{sym_norm}:{pos_side}"

        max_retries = self.max_retries
        t0 = time.perf_counter()
        last_err = None

        for attempt in range(1, max_retries + 1):
            try:
                threading.Thread(target=self.c.cancel_all_open_orders, args=(strat_sym,), daemon=True).start()
                px_ref = self._safe_get_mark_price(strat_sym)
                if px_ref <= 0:
                    raise RuntimeError("參考價格取得失敗")

                log(f"下單請求: {strat_sym} | MARKET | side={pos_side} qty={qty}")

                req_t0 = time.perf_counter()
                res = self.c.place_order(strat_sym, side_bm, pos_side, "MARKET", qty=qty)
                exec_delay = time.perf_counter() - req_t0
                d = res.get("data") or {}
                order_id = d.get("orderId") or d.get("order_id")

                entry_avg = 0.0
                filled_qty = 0.0

                if not self.c.dry_run:
                    for _ in range(8):
                        try:
                            pos = self.c.get_positions().get("data", [])
                            for p in pos:
                                if str(p.get("positionId")) != position_id:
                                    continue
                                entry_avg = safe_float(p.get("entryPrice"), 0.0)
                                amt_contract = abs(safe_float(p.get("positionAmt"), 0.0))
                                c_size = self.c.get_contract_size(sym_norm)
                                if c_size and c_size > 0:
                                    filled_qty = amt_contract * c_size
                                break
                        except Exception:
                            pass
                        if entry_avg > 0 and filled_qty > 0:
                            break
                        time.sleep(0.25)

                if (not order_id) or str(order_id).lower() == "none":
                    if entry_avg > 0 and filled_qty > 0:
                        order_id = "UNKNOWN"
                    else:
                        raise RuntimeError(f"下單回報缺少 order_id: {res}")

                if entry_avg <= 0:
                    entry_avg = self._apply_cost_side(tag, px_ref, self.fee_bps, self.slip_bps)
                if filled_qty <= 0:
                    filled_qty = qty

                log(f"開倉完成: order_id={order_id} position_id={position_id} entry={entry_avg:.4f} qty={filled_qty} 延遲={exec_delay:.4f}秒")
                return position_id, px_ref, entry_avg, filled_qty, exec_delay

            except Exception as e:
                last_err = e
                msg = str(e)
                elapsed = time.perf_counter() - t0
                
                # [專家級防護] 若明確為餘額不足 (balance not enough)，立即放棄重試，避免無意義的 API 消耗與重試延遲
                if "balance not enough" in msg.lower() or "insufficient" in msg.lower():
                    log(f"[{strat_id}] 餘額不足 (40012)，立即放棄重試。")
                    break
                    
                if elapsed >= getattr(self, "max_retry_total_sec", 3.5):
                    break
                if "40012" in msg or "occupied" in msg.lower():
                    try:
                        self.c.cancel_all_open_orders(strat_sym)
                    except Exception:
                        pass
                if attempt < max_retries:
                    sleep_s = 0.8 * (1.6 ** (attempt - 1))
                    if "40012" in msg:
                        sleep_s = max(sleep_s, 2.0)
                    remaining = getattr(self, "max_retry_total_sec", 3.5) - elapsed
                    sleep_s = min(sleep_s, max(0.0, remaining))
                    if sleep_s > 0: time.sleep(sleep_s)
                else:
                    break

        log(f"開倉失敗: {last_err}")
        return None, 0.0, 0.0, 0.0, 0.0

    def arm_tp_sl_sid(self, sid: str, side: str, entry_ref_px: float, position_id: str):
        pos_data = self.positions[sid]
        cfg_p = pos_data["cfg"]
        sym = cfg_p["symbol"]
        price_step = self.symbol_info.get(sym.upper(), {}).get("price_step", 0.0001)
        
        pos_data["local_guard_active"] = True 
        pos_data["trailing_active"] = False
        pos_data["trailing_max_price"] = entry_ref_px
        pos_data["entry_price_snapshot"] = entry_ref_px
        
        tp_ratio = max(0.0, float(cfg_p.get("tp_pct", 0.0))) / 100.0
        sl_ratio = max(0.0, float(cfg_p.get("sl_pct", 0.0))) / 100.0
        
        if side == "LONG":
            pos_data["fixed_tp_price"] = round_to_step(entry_ref_px * (1.0 + tp_ratio), price_step)
            pos_data["fixed_sl_price"] = round_to_step(entry_ref_px * (1.0 - sl_ratio), price_step)
        else:
            pos_data["fixed_tp_price"] = round_to_step(entry_ref_px * (1.0 - tp_ratio), price_step)
            pos_data["fixed_sl_price"] = round_to_step(entry_ref_px * (1.0 + sl_ratio), price_step)
            
        pos_data["tp_price"] = pos_data["fixed_tp_price"] if tp_ratio > 0 else None
        pos_data["sl_price"] = pos_data["fixed_sl_price"] if sl_ratio > 0 else None

        log(f"[{sid}]【風控部署】{side} @ {entry_ref_px} | 預設止盈: {pos_data['fixed_tp_price']} | 預設止損: {pos_data['fixed_sl_price']}")

        if not self.is_multi_pos:
            try:
                if tp_ratio > 0:
                    _tp = self._fmt_price(sym, pos_data["fixed_tp_price"])
                    self.c.submit_tp_sl_order(
                        symbol=sym, position_side=side, tp_sl_type="take_profit",
                        trigger_price=_tp, executive_price=_tp,
                        price_type=(2 if self.use_mark else 1), category="market"
                    )
                if sl_ratio > 0:
                    _sl = self._fmt_price(sym, pos_data["fixed_sl_price"])
                    self.c.submit_tp_sl_order(
                        symbol=sym, position_side=side, tp_sl_type="stop_loss",
                        trigger_price=_sl, executive_price=_sl,
                        price_type=(2 if self.use_mark else 1), category="market"
                    )
            except Exception as e:
                log(f"[{sid}] 交易所 TP/SL 設定受阻：{e} | 保留本地守護作為後備")

    def _book_realized_local(self, side: str, entry_avg: float, exit_px_ref: float, qty: float):
        if entry_avg is None or exit_px_ref is None or qty is None:
            return
        pnl = (exit_px_ref - entry_avg) * qty if side == "LONG" else (entry_avg - exit_px_ref) * qty
        self.local_realized_usdt += float(pnl)
        tag = "獲利" if pnl>=0 else "虧損"
        log(f"本地記帳：{tag} {pnl:.4f} USDT；當日累計 {self.local_realized_usdt:.4f} USDT")

    def local_guard_check_sid(self, sid: str, side: str):
        pos_data = self.positions[sid]
        if not pos_data["local_guard_active"] or not pos_data["position_id"]:
            return

        try:
            sym = pos_data["cfg"]["symbol"]
            curr_price = self._safe_get_mark_price(sym)
            if curr_price <= 0: return

            params = pos_data["cfg"].get("params", {})
            act_pct = float(params.get("activation_pct", 0.0))
            trail_ticks = int(params.get("trail_ticks", 0))
            mintick = float(params.get("mintick", 0.01))
            offset_val = trail_ticks * mintick
            price_step = self.symbol_info.get(sym.upper(), {}).get("price_step", 0.0001)

            if act_pct > 0 and trail_ticks > 0:
                if side == "LONG":
                    if curr_price > pos_data["trailing_max_price"]:
                        pos_data["trailing_max_price"] = curr_price

                    act_price = pos_data["entry_price_snapshot"] * (1.0 + act_pct / 100.0)
                    if (not pos_data["trailing_active"]) and (pos_data["trailing_max_price"] >= act_price):
                        pos_data["trailing_active"] = True
                        log(f"[{sid}]【追蹤止損】條件激活！max={pos_data['trailing_max_price']:.2f} >= 激活價:{act_price:.2f}")

                    if pos_data["trailing_active"]:
                        dynamic_sl = pos_data["trailing_max_price"] - offset_val
                        dynamic_sl = round_to_step(dynamic_sl, price_step)
                        if pos_data["sl_price"] is None or dynamic_sl > pos_data["sl_price"]:
                            pos_data["sl_price"] = dynamic_sl

            trig_type = None
            if side == "LONG":
                if pos_data["sl_price"] is not None and curr_price <= pos_data["sl_price"]:
                    trig_type = f"觸發止損 (現價={curr_price:.2f} <= {pos_data['sl_price']:.2f})"
                elif pos_data["tp_price"] is not None and curr_price >= pos_data["tp_price"]:
                    trig_type = f"觸發止盈 (現價={curr_price:.2f} >= {pos_data['tp_price']:.2f})"

            if trig_type:
                log(f"[{sid}]【風控執行】{trig_type} -> 執行平倉")
                try:
                    self.c.close_position_by_id(pos_data["position_id"], close_qty=pos_data["entry_qty"])
                except Exception as e:
                    log(f"[{sid}] 平倉失敗: {e}")

                if pos_data["entry_avg"] is not None and pos_data["entry_qty"]:
                    self._book_realized_local(pos_data["in_pos"], pos_data["entry_avg"], curr_price, pos_data["entry_qty"])
                self._reset_pos(sid)

        except Exception as e:
            log(f"[{sid}] 本地風控異常（忽略本輪）：{e}")

    def _reset_pos(self, strat_id: str):
        if strat_id not in self.positions: return
        self.positions[strat_id].update({
            "in_pos": None, "position_id": None, "entry_bar_index": None,
            "entry_avg": None, "entry_qty": None, "tp_price": None, "sl_price": None,
            "local_guard_active": False, "trailing_active": False,
            "trailing_max_price": 0.0, "entry_price_snapshot": 0.0,
            "fixed_tp_price": 0.0, "fixed_sl_price": 0.0
        })

    def _sync_positions_passive_close(self):
        if self.c.dry_run: return
        try:
            pos = self.c.get_positions().get("data", [])
            pid_set = {str(p.get("positionId")) for p in pos if p.get("positionId") is not None}
            
            for strat_id, pos_data in self.positions.items():
                if not pos_data["in_pos"] or not pos_data["position_id"]: continue
                if str(pos_data["position_id"]) not in pid_set:
                    sym = pos_data["cfg"]["symbol"]
                    mark = self._safe_get_mark_price(sym)
                    log(f"[{strat_id}] 偵測倉位已不存在（可能被動觸發/強平）mark={mark}，進行本地記帳")
                    if pos_data["entry_avg"] is not None and pos_data["entry_qty"]:
                        self._book_realized_local(pos_data["in_pos"], pos_data["entry_avg"], mark, pos_data["entry_qty"])
                    self._reset_pos(strat_id)
        except Exception as e:
            log(f"被動同步倉位失敗（忽略）：{e}")

    def flat_all(self):
        for strat_id, pos_data in self.positions.items():
            if pos_data["position_id"]:
                sym = pos_data["cfg"]["symbol"]
                try:
                    try:
                        self.c.cancel_all_open_orders(sym)
                    except Exception: pass
                    
                    self.c.close_position_by_id(pos_data["position_id"], close_qty=pos_data["entry_qty"])
                    mark = self._safe_get_mark_price(sym)
                    if pos_data["in_pos"] and pos_data["entry_avg"] is not None and pos_data["entry_qty"]:
                        self._book_realized_local(pos_data["in_pos"], pos_data["entry_avg"], mark, pos_data["entry_qty"])
                        log(f"[{strat_id}] 強制平倉完成並記錄本地損益 (獨立結算數量: {pos_data['entry_qty']})")
                except Exception as e:
                    log(f"[{strat_id}] 平倉失敗（忽略）：{e}\n{traceback.format_exc()}")
                finally:
                    self._reset_pos(strat_id)

    # ----- 主循環 -----
    def run(self):
        log("系統初始化：啟動多幣種 WebSocket 行情引擎...")
        self.ws_kline.start()
        
        while not self.ws_kline.ready and not stop_event.is_set():
            time.sleep(0.1)

        # [專家新增] 確保第一次執行時，所有幣種的初始價格都已精確載入 (價格預熱)
        log("正在同步各幣種初始價格與精度狀態...")
        for sym in self.all_symbols:
            px = self._safe_get_mark_price(sym)
            log(f"[{sym}] 初始市場標記價格快取就緒: {px}")

        log("多策略並發引擎已啟動。")
        self.last_eval_kline_time = {} 

        while not stop_event.is_set():
            try:
                self._loop()
                
                if time.time() - self.last_trade_fetch_ts > max(20, self.trade_fetch_interval):
                    self.daily_guard_check()
                    self._sync_positions_passive_close()
                    self.last_trade_fetch_ts = time.time()

                time.sleep(0.1)
                
                if not hasattr(self, '_last_eq_warm_ts') or time.time() - getattr(self, '_last_eq_warm_ts', 0) > 8.0:
                    self._last_eq_warm_ts = time.time()
                    threading.Thread(target=self._safe_get_equity, daemon=True).start()

                # 動態抓取每個策略對應的 K 線進行訊號檢查
                active_signals = self.compute_signals()
                
                if time.time() - self._last_guard_ts >= self.guard_poll_sec:
                    self._last_guard_ts = time.time()
                    for sid, pos_data in self.positions.items():
                        if pos_data["in_pos"] is not None and pos_data.get("local_guard_active", False):
                            try:
                                self.local_guard_check_sid(sid, pos_data["in_pos"])
                            except Exception:
                                pass
                
                if time.time() - self._last_status_ts > 5:
                    self._last_status_ts = time.time()
                    try:
                        status_msgs = []
                        for sid, pos_data in self.positions.items():
                            sym = pos_data["cfg"]["symbol"]
                            if pos_data["in_pos"] is not None:
                                if pos_data.get("trailing_active", False):
                                    sl_p = pos_data.get("sl_price")
                                    sl_str = f"{sl_p:.2f}" if sl_p else "N/A"
                                    trail_status = f"運行中(SL:{sl_str})"
                                else:
                                    params = pos_data["cfg"].get("params", {})
                                    act_pct = float(params.get("activation_pct", 0.0))
                                    trail_ticks = int(params.get("trail_ticks", 0))
                                    if act_pct > 0 and trail_ticks > 0:
                                        snap = pos_data.get("entry_price_snapshot", 0.0)
                                        act_price = snap * (1.0 + act_pct/100.0) if snap else 0.0
                                        trail_status = f"未激活(激活價:{act_price:.2f})"
                                    else:
                                        trail_status = "停用"
                                status_msgs.append(f"[{sid}]持:{pos_data['in_pos']} 追:{trail_status}")
                        
                        if status_msgs:
                            log("監控中 | " + " | ".join(status_msgs))
                            self._idle_ticks = 0
                        else:
                            self._idle_ticks = getattr(self, '_idle_ticks', 0) + 1
                            # 每 5 秒進入一次，當達到 6 次 (30秒) 打印一次心跳
                            if self._idle_ticks >= 6:
                                log("監控中 | 偵測進場訊號中...")
                                self._idle_ticks = 0
                    except Exception:
                        pass

                # 手動測試區塊
                if self.manual_test_trigger:
                    self.manual_test_trigger = False
                    log("【測試模式】暫不支援多幣種全域測試。若需測試請單獨手動下單。")
                    continue

                for sid, pos_data in self.positions.items():
                    sym = pos_data["cfg"]["symbol"]
                    iv = pos_data["cfg"]["interval"]
                    df = self.ws_kline.get_df(sym, iv)
                    if df.empty:
                        continue
                        
                    live_ok = len(df) >= 50
                    i = len(df) - 1

                    # Time Stop 退出
                    if pos_data["in_pos"] and pos_data["entry_open_ms"] is not None:
                        iv_ms = INTERVAL_MS[iv]
                        bar_open_ms = int(df["time"].iloc[i]) + iv_ms
                        held = max(0, int((bar_open_ms - pos_data["entry_open_ms"]) // iv_ms))
                        max_h = pos_data["cfg"].get("max_hold", 300)
                        if max_h > 0 and held >= max_h:
                            log(f"[{sid}]【時間止損】持倉過久 ({held}/{max_h} K棒)，強制平倉。")
                            try:
                                self.c.close_position_by_id(pos_data["position_id"], close_qty=pos_data["entry_qty"])
                            except Exception as e:
                                log(f"[{sid}] 平倉失敗（忽略）：{e}")
                            mark = self._safe_get_mark_price(sym)
                            if pos_data["entry_avg"] is not None and pos_data["entry_qty"]:
                                self._book_realized_local(pos_data["in_pos"], pos_data["entry_avg"], mark, pos_data["entry_qty"])
                            self._reset_pos(sid)

                    if pos_data["in_pos"]:
                        self.local_guard_check_sid(sid, pos_data["in_pos"])

                    if pos_data["cooldown"] > 0:
                        pos_data["cooldown"] -= 1

                    if live_ok and not pos_data["in_pos"] and pos_data["cooldown"] == 0 and not self.daily_halt_active:
                        l_sig, s_sig, sig_ts = active_signals.get(sid, (False, False, -1))
                        
                        # 只有當訊號有效，且這根 K 棒的訊號沒有被嘗試過，才進行開倉
                        if (l_sig or s_sig) and pos_data.get("last_attempted_bar_ts") != sig_ts:
                            side_str = "LONG" if l_sig else "SHORT"
                            log(f"[{sid}] 執行: 進場 ({side_str}) 對象: {sym}")
                            
                            # 標記已嘗試過此根 K 棒，徹底避免無限重試洗頻與 30013 封鎖
                            pos_data["last_attempted_bar_ts"] = sig_ts
                            
                            now_ts_sec = time.time()
                            iv_sec = INTERVAL_MS[iv] / 1000.0
                            theoretical_boundary = now_ts_sec - (now_ts_sec % iv_sec)
                            signal_delay = now_ts_sec - theoretical_boundary
                            
                            pid, px_ref, entry_est, q, exec_delay = self.open_market(side_str, strat_id=sid)
                            if not pid:
                                log(f"[{sid}] 開倉失敗 (如餘額不足等)，已取消該次訊號的進場嘗試。")
                                continue
                                
                            mode_str = "DRY_RUN" if self.c.dry_run else "LIVE"
                            log_execution_csv(mode_str, side_str, signal_delay, exec_delay, px_ref, entry_est)

                            pos_data["in_pos"] = side_str
                            pos_data["position_id"] = pid
                            pos_data["entry_avg"] = entry_est
                            pos_data["entry_bar_index"] = i
                            pos_data["entry_qty"] = q
                            pos_data["entry_open_ms"] = int(df["time"].iloc[i])
                            
                            # [專家級優化] 將 TP/SL 的計算基準從「下單前標記價 (px_ref)」改為「實際成交價 (entry_est)」，確保風控精準度 100%
                            self.arm_tp_sl_sid(sid, side_str, entry_est, pid)
                            pos_data["cooldown"] = pos_data["cfg"].get("params", {}).get("cooldown", 0)

            except SystemExit:
                log("接到系統結束訊號，退出主循環")
                break
            except Exception as e:
                log(f"致命錯誤：{e}\n{traceback.format_exc()}")
                time.sleep(1.0)

# ============ 因子池自動同步與權重計算 (背景聖杯建構引擎) ============
def detect_api_base(host_url):
    host_url = host_url.rstrip('/')
    prefixes = ["/sheep123", "/api", ""]
    for p in prefixes:
        test_url = f"{host_url}{p}/healthz"
        try:
            res = requests.get(test_url, verify=False, timeout=5)
            if res.status_code == 200 and "ok" in res.text.lower():
                return f"{host_url}{p}"
        except requests.exceptions.RequestException:
            pass
    return host_url

def _new_holy_grail_runtime() -> HolyGrailRuntime:
    return HolyGrailRuntime(bt_module=bt, log=log)


def fetch_factor_pool_data():
    try:
        strategies, _ = _new_holy_grail_runtime().fetch_factor_pool_data()
        return strategies
    except Exception as e:
        log(f"【聖杯引擎】拉取因子池失敗: {e}")
        return []


def flatten_strategies_to_dataframe(strategies):
    return _new_holy_grail_runtime().flatten_strategies_to_dataframe(strategies)


def load_kline_data_for_backtest(symbol: str, timeframe_min: int, cache: dict) -> pd.DataFrame:
    runtime = _new_holy_grail_runtime()
    if isinstance(cache, dict):
        runtime._kline_cache = cache
    return runtime.load_kline_data(symbol, timeframe_min)


def build_daily_equity_curve_for_backtest(trades_detail: list) -> pd.Series:
    return HolyGrailRuntime.build_daily_equity_curve(trades_detail)


def normalize_multi_strategy_entries(
    raw_multi_json,
    default_symbol: str = "",
    default_interval: str = "",
    default_stake_pct: float = 0.0,
):
    strategies_cfg: Dict[str, Dict[str, Any]] = {}
    active_sym_intervals = set()

    iterable = normalize_strategy_batch(
        raw_multi_json,
        default_symbol=default_symbol,
        default_interval=default_interval,
    )

    for idx, raw_entry in enumerate(iterable):
        entry = dict(raw_entry or {})
        family = str(entry.get("family") or "UNKNOWN").strip() or "UNKNOWN"
        direction = normalize_direction(entry.get("direction"), default="long")
        params = dict(entry.get("family_params") or {})
        params["direction"] = direction
        params["reverse"] = direction == "short"
        strategy_ref = entry.get("strategy_key") or entry.get("strategy_id") or idx
        sid = f"{family}_{strategy_ref}"
        sym = str(entry.get("symbol") or default_symbol).upper()
        iv = str(entry.get("interval") or default_interval)
        strategies_cfg[sid] = {
            "family": family,
            "params": params,
            "direction": direction,
            "tp_pct": float(entry.get("tp_pct") or 0.0),
            "sl_pct": float(entry.get("sl_pct") or 0.0),
            "max_hold": int(entry.get("max_hold") or 0),
            "stake_pct": float(entry.get("stake_pct") or default_stake_pct),
            "symbol": sym,
            "interval": iv,
            "strategy_key": str(strategy_ref),
        }
        active_sym_intervals.add((sym, iv))

    return strategies_cfg, sorted(active_sym_intervals)


class FactorPoolUpdater:
    def __init__(self, ui_app):
        self.ui = ui_app
        self.running = True
        self.sync_interval_sec = 300
        self.seen_warnings = set()
        try:
            self.last_good_json = (self.ui.multi_json_text.get("1.0", tk.END) or "").strip()
        except Exception:
            self.last_good_json = ""

    def _log_runtime_warnings(self, warnings_list):
        for warning in warnings_list or []:
            if warning in self.seen_warnings:
                continue
            self.seen_warnings.add(warning)
            log(warning)

    def _factor_pool_runtime_kwargs(self):
        def _ui_value(var_name, env_name, default=""):
            value = ""
            ui_var = getattr(self.ui, var_name, None)
            if ui_var is not None:
                try:
                    value = str(ui_var.get() or "").strip()
                except Exception:
                    value = ""
            if not value:
                value = str(os.environ.get(env_name, default)).strip()
            return value

        return {
            "factor_pool_url": _ui_value("factor_pool_url_var", "SHEEP_FACTOR_POOL_URL", "https://sheep123.com"),
            "factor_pool_token": _ui_value("factor_pool_token_var", "SHEEP_FACTOR_POOL_TOKEN"),
            "factor_pool_user": _ui_value("factor_pool_user_var", "SHEEP_FACTOR_POOL_USER"),
            "factor_pool_pass": _ui_value("factor_pool_pass_var", "SHEEP_FACTOR_POOL_PASS"),
        }

    def _runtime_sync_url(self, factor_pool_url: str) -> str:
        return f"{detect_api_base(factor_pool_url).rstrip('/')}/runtime/portfolio/sync"

    def _collect_runtime_position_items(self):
        trader = getattr(self.ui, "active_trader", None)
        if trader is None:
            return []

        try:
            raw_positions = list((trader.c.get_positions() or {}).get("data") or [])
        except Exception as exc:
            log(f"【網站同步】持倉快照讀取失敗: {exc}")
            return []

        raw_by_id = {}
        for row in raw_positions:
            try:
                raw_by_id[str(row.get("positionId") or "")] = dict(row or {})
            except Exception:
                continue

        try:
            account_equity = float(trader._safe_get_equity() or 0.0)
        except Exception:
            account_equity = 0.0

        items = []
        for strat_id, pos_data in list(getattr(trader, "positions", {}).items()):
            cfg = dict((pos_data or {}).get("cfg") or {})
            position_id = str((pos_data or {}).get("position_id") or "")
            in_pos = (pos_data or {}).get("in_pos")
            raw = raw_by_id.get(position_id) or {}
            if not position_id and raw:
                position_id = str(raw.get("positionId") or "")
            if not position_id and not raw:
                continue
            if in_pos is None and not raw:
                continue

            symbol = str(cfg.get("symbol") or raw.get("symbol") or "").replace("-", "").replace("_", "").upper()
            if not symbol:
                continue
            direction = normalize_direction(cfg.get("direction") or raw.get("positionSide"), default="long")
            entry_price = safe_float(raw.get("entryPrice") or pos_data.get("entry_avg"), 0.0)
            mark_price = safe_float(raw.get("markPrice"), 0.0)
            if mark_price <= 0:
                try:
                    mark_price = safe_float(trader._safe_get_mark_price(symbol), 0.0)
                except Exception:
                    mark_price = 0.0
            qty = safe_float(pos_data.get("entry_qty"), 0.0)
            if qty <= 0:
                qty = abs(safe_float(raw.get("positionAmt"), 0.0))
                if qty > 0:
                    try:
                        contract_size = safe_float(trader.c.get_contract_size(symbol), 0.0)
                    except Exception:
                        contract_size = 0.0
                    if contract_size > 0:
                        qty *= contract_size
            position_usdt = safe_float(raw.get("positionValue"), 0.0)
            if position_usdt <= 0 and qty > 0 and mark_price > 0:
                position_usdt = abs(qty) * mark_price
            margin_usdt = safe_float(raw.get("margin"), 0.0)
            if margin_usdt <= 0 and position_usdt > 0:
                margin_usdt = position_usdt / 5.0
            unrealized_pnl_usdt = safe_float(raw.get("unrealizedPnl"), 0.0)
            if abs(unrealized_pnl_usdt) <= 0 and qty > 0 and entry_price > 0 and mark_price > 0:
                price_delta = mark_price - entry_price if direction == "long" else entry_price - mark_price
                unrealized_pnl_usdt = price_delta * abs(qty)
            liquidation_price = safe_float(raw.get("liquidationPrice"), 0.0)
            margin_ratio_pct = safe_float(raw.get("marginRatePct"), 0.0)
            if margin_ratio_pct <= 0 and account_equity > 0 and margin_usdt > 0:
                margin_ratio_pct = (margin_usdt / account_equity) * 100.0
            unrealized_pnl_pct = 0.0
            if margin_usdt > 0:
                unrealized_pnl_pct = (unrealized_pnl_usdt / margin_usdt) * 100.0

            items.append(
                {
                    "position_key": position_id or str(cfg.get("strategy_key") or strat_id),
                    "position_id": position_id,
                    "strategy_key": str(cfg.get("strategy_key") or strat_id),
                    "family": str(cfg.get("family") or ""),
                    "symbol": symbol,
                    "direction": direction,
                    "interval": str(cfg.get("interval") or ""),
                    "entry_price": entry_price,
                    "mark_price": mark_price,
                    "liquidation_price": liquidation_price,
                    "position_qty": abs(qty),
                    "position_usdt": position_usdt,
                    "margin_usdt": margin_usdt,
                    "margin_ratio_pct": margin_ratio_pct,
                    "unrealized_pnl_usdt": unrealized_pnl_usdt,
                    "unrealized_pnl_pct": unrealized_pnl_pct,
                }
            )

        items.sort(key=lambda item: (-safe_float(item.get("position_usdt"), 0.0), str(item.get("symbol") or "")))
        return items

    def _runtime_sync_payload(self, scope: str, result, runtime_kwargs: dict) -> dict:
        now_iso = datetime.now(timezone.utc).isoformat()
        items = []
        for idx, item in enumerate(list(result.multi_payload or []), start=1):
            payload = dict(item or {})
            payload.setdefault("rank", idx)
            payload.setdefault("strategy_key", payload.get("strategy_key") or payload.get("name") or f"{payload.get('family', 'UNKNOWN')}_{idx}")
            items.append(payload)
        summary = {
            "portfolio_metrics": dict(result.portfolio_metrics or {}),
            "selected_count": int(result.selected_count or len(items)),
            "candidate_count": int(result.candidate_count or 0),
            "backtested_count": int(result.backtested_count or 0),
            "report_paths": dict(result.report_paths or {}),
            "position_items": self._collect_runtime_position_items(),
        }
        return {
            "scope": scope,
            "updated_at": now_iso,
            "source": "holy_grail_runtime",
            "summary": summary,
            "items": items,
        }

    def _has_runtime_sync_auth(self, runtime_kwargs: dict) -> bool:
        token = str(runtime_kwargs.get("factor_pool_token") or "").strip()
        user = str(runtime_kwargs.get("factor_pool_user") or "").strip()
        password = str(runtime_kwargs.get("factor_pool_pass") or "").strip()
        return bool(token or (user and password))

    def _post_runtime_snapshot(self, scope: str, payload: dict, runtime_kwargs: dict) -> bool:
        sync_url = self._runtime_sync_url(runtime_kwargs.get("factor_pool_url", "https://sheep123.com"))
        token = str(runtime_kwargs.get("factor_pool_token") or "").strip()
        user = str(runtime_kwargs.get("factor_pool_user") or "").strip()
        password = str(runtime_kwargs.get("factor_pool_pass") or "").strip()
        attempts = []
        if token:
            attempts.append(("token", {"Authorization": f"Bearer {token}"}, dict(payload)))
        if user and password:
            password_payload = dict(payload)
            password_payload["username"] = user
            password_payload["password"] = password
            attempts.append(("password", {}, password_payload))
        if not attempts:
            log(f"【網站同步】{scope} runtime 快照略過：未設定可用的同步憑證。")
            return False

        last_detail = ""
        for mode, headers, body in attempts:
            try:
                resp = requests.post(sync_url, json=body, headers=headers, timeout=20, verify=False)
                data = resp.json() if resp.headers.get("content-type", "").lower().startswith("application/json") else {}
                if resp.status_code == 200 and bool(data.get("ok")):
                    snapshot = data.get("snapshot") or {}
                    log(f"【網站同步】{scope} runtime 快照已更新：{int(snapshot.get('strategy_count') or 0)} 組策略。")
                    return True
                last_detail = str(data.get("detail") or data.get("error") or resp.text[:300])
                if mode == "token" and resp.status_code in {401, 403} and len(attempts) > 1:
                    log(f"【網站同步】{scope} token 已失效，改用帳密重試同步。")
                    continue
                log(f"【網站同步】{scope} runtime 快照更新失敗: HTTP {resp.status_code} | {last_detail}")
                return False
            except Exception as e:
                last_detail = str(e)
                if mode == "token" and len(attempts) > 1:
                    log(f"【網站同步】{scope} token 同步異常，改用帳密重試：{e}")
                    continue
                log(f"【網站同步】{scope} runtime 快照同步異常: {e}")
                return False

        if last_detail:
            log(f"【網站同步】{scope} runtime 快照最終仍失敗: {last_detail}")
        return False

    def _sync_runtime_snapshot(self, scope: str, result, runtime_kwargs: dict):
        payload = self._runtime_sync_payload(scope, result, runtime_kwargs)
        self._post_runtime_snapshot(scope, payload, runtime_kwargs)

    def _sync_cached_runtime_snapshot(self, scope: str, runtime_kwargs: dict, *, reason: str):
        cached_json = str(self.last_good_json or "").strip()
        if not cached_json:
            return
        try:
            raw_batch = json.loads(cached_json)
        except Exception as exc:
            log(f"【網站同步】{scope} cached runtime 快照解析失敗: {exc}")
            return
        items = []
        for idx, item in enumerate(
            normalize_strategy_batch(
                raw_batch,
                default_symbol="",
                default_interval="",
            ),
            start=1,
        ):
            payload = dict(item or {})
            payload.setdefault("rank", idx)
            payload.setdefault(
                "strategy_key",
                payload.get("strategy_key") or payload.get("strategy_id") or payload.get("name") or f"{payload.get('family', 'UNKNOWN')}_{idx}",
            )
            items.append(payload)
        if not items:
            return
        payload = {
            "scope": scope,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "source": f"holy_grail_cached_{reason}",
            "summary": {
                "selected_count": len(items),
                "candidate_count": len(items),
                "backtested_count": len(items),
                "fallback_reason": reason,
                "position_items": self._collect_runtime_position_items(),
            },
            "items": items,
        }
        self._post_runtime_snapshot(scope, payload, runtime_kwargs)

    def start(self):
        if bt is None:
            log(f"【致命錯誤】聖杯引擎停用，無法匯入 backtest runtime: {HOLY_GRAIL_IMPORT_ERROR}")
            self.running = False
            return
        t = threading.Thread(target=self._loop, daemon=True)
        t.start()
        log("【系統服務】全自動聖杯建構引擎已啟動，每 5 分鐘自動尋找並熱更新對沖組合。")
        runtime_kwargs = self._factor_pool_runtime_kwargs()
        if self.last_good_json and self._has_runtime_sync_auth(runtime_kwargs):
            def _startup_sync():
                try:
                    self._sync_cached_runtime_snapshot("personal", runtime_kwargs, reason="startup")
                    self._sync_cached_runtime_snapshot("global", runtime_kwargs, reason="startup")
                except Exception as exc:
                    log(f"【網站同步】startup cached runtime 快照同步異常: {exc}")

            threading.Thread(target=_startup_sync, daemon=True).start()

    def _loop(self):
        while self.running:
            try:
                self._build_holy_grail()
            except Exception as e:
                log(f"【聖杯引擎異常攔截】發生錯誤: {e}\n{traceback.format_exc()}")

            for _ in range(self.sync_interval_sec):
                if not self.running:
                    break
                time.sleep(1)

    def _build_holy_grail(self):
        if bt is None:
            log(f"【致命錯誤】聖杯引擎停用，無法匯入 backtest runtime: {HOLY_GRAIL_IMPORT_ERROR}")
            self.running = False
            return

        base_stake_var = getattr(self.ui, "global_stake_pct_var", None)
        try:
            base_stake = float(base_stake_var.get()) if base_stake_var is not None else 95.0
        except Exception:
            base_stake = 95.0
        runtime_kwargs = self._factor_pool_runtime_kwargs()
        if not self._has_runtime_sync_auth(runtime_kwargs):
            warning = "【聖杯引擎】未設定因子池帳密或同步憑證，背景熱更新暫停；沿用目前策略組合。"
            if warning not in self.seen_warnings:
                self.seen_warnings.add(warning)
                log(warning)
            return
        if not bool(getattr(bt, "NUMBA_OK", True)):
            warning = "【聖杯引擎】目前 Python 環境未啟用 Numba，已改用純 Python 回測路徑；速度較慢，但仍會持續同步。"
            if warning not in self.seen_warnings:
                self.seen_warnings.add(warning)
                log(warning)
        if self.last_good_json:
            self._sync_cached_runtime_snapshot("personal", runtime_kwargs, reason="in_progress")
            self._sync_cached_runtime_snapshot("global", runtime_kwargs, reason="in_progress")
        log("【聖杯引擎】開始拉取實盤因子池進行背景計算...")
        result = run_holy_grail_build(
            bt_module=bt,
            log=log,
            base_stake_pct=float(base_stake),
            factor_pool_url=runtime_kwargs["factor_pool_url"],
            factor_pool_token=runtime_kwargs["factor_pool_token"],
            factor_pool_user=runtime_kwargs["factor_pool_user"],
            factor_pool_pass=runtime_kwargs["factor_pool_pass"],
        )
        if not result.ok:
            log(f"【聖杯引擎】本輪更新失敗: {result.message}")
            if self.last_good_json:
                log("【聖杯引擎】保留上一版有效的對沖組合，不進行熱更新。")
                self._sync_cached_runtime_snapshot("personal", runtime_kwargs, reason="failure")
                self._sync_cached_runtime_snapshot("global", runtime_kwargs, reason="failure")
            self._log_runtime_warnings(result.warnings)
            return

        new_json_str = (result.multi_strategies_json or "[]").strip()
        if new_json_str in {"", "[]"}:
            log("【聖杯引擎】回測無產生可用組合，保留上一版策略。")
            if self.last_good_json:
                self._sync_cached_runtime_snapshot("personal", runtime_kwargs, reason="empty_result")
                self._sync_cached_runtime_snapshot("global", runtime_kwargs, reason="empty_result")
            return

        self.last_good_json = new_json_str
        self.ui.update_multi_json(new_json_str)
        self._sync_runtime_snapshot("personal", result, runtime_kwargs)
        self._sync_runtime_snapshot("global", result, runtime_kwargs)

        metrics = result.portfolio_metrics or {}
        log(
            "【聖杯引擎】更新完成: "
            f"Sharpe {float(metrics.get('sharpe', 0.0)):.2f} | "
            f"CAGR {float(metrics.get('cagr_pct', 0.0)):.2f}% | "
            f"MaxDD {float(metrics.get('max_drawdown_pct', 0.0)):.2f}%"
        )
        self._log_runtime_warnings(result.warnings)
        if result.report_paths:
            log(f"【聖杯引擎】摘要報告: {result.report_paths.get('summary_report', '')}")


class AnimatedUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1180x760")
        self.minsize(1024, 640)
        self.configure(bg="#0b0f14")
        self._make_style()

        # 總資金池設定
        self.global_stake_pct_var = tk.DoubleVar(value=95.0)

        # 單一畫布：背景 + 粒子
        self._build_canvas_and_background()
        self._build_glass_panel()
        self._build_tabs()
        self._build_footer()
        self._load_cfg_safely()

        self.after(50, self._drain_logs)
        self.bind("<Configure>", self._on_window_resize)
        
        # 啟動因子池同步器
        self.factor_updater = FactorPoolUpdater(self)
        self.factor_updater.start()

    def update_multi_json(self, new_json_str):
        """核心級別的動態 JSON 更新與實盤熱對接"""
        def _do_update():
            try:
                # 覆寫唯讀面板
                self.multi_json_text.config(state="normal")
                self.multi_json_text.delete("1.0", tk.END)
                self.multi_json_text.insert("1.0", new_json_str)
                self.multi_json_text.config(state="disabled") 
                log("【面板更新】動態因子 JSON 面板已覆寫為最新 Top 20 組合。")
                
                # 若實盤啟動中，進行硬派的 Hot Reloading 注入
                if self.active_trader is not None:
                    log("【實盤熱對接】偵測到交易核心運行中，啟動無縫熱切換 (Hot Reloading)...")
                    try:
                        multi_json = json.loads(new_json_str)
                        new_strat_cfg, normalized_sym_ivs = normalize_multi_strategy_entries(
                            multi_json,
                            self.active_trader.global_symbol,
                            self.active_trader.global_interval,
                            5.0,
                        )
                        active_sym_ivs = set(normalized_sym_ivs)
                        
                        # 原子級別覆寫與狀態傳遞
                        with self.active_trader.ws_kline.lock:
                            old_positions = self.active_trader.positions
                            self.active_trader.strategies_cfg = new_strat_cfg.copy()
                            self.active_trader.positions = {}
                            
                            # 防丟單護城河升級：將有持倉但已被移出 Top 20 的策略強制保留
                            for old_sid, pos_data in old_positions.items():
                                if pos_data["in_pos"] is not None and old_sid not in new_strat_cfg:
                                    self.active_trader.strategies_cfg[old_sid] = pos_data["cfg"]
                                    self.active_trader.positions[old_sid] = pos_data
                                    active_sym_ivs.add((pos_data["cfg"]["symbol"], pos_data["cfg"]["interval"]))
                            
                            # 處理在 Top 20 內的策略
                            for sid, strat_cfg in new_strat_cfg.items():
                                if sid in old_positions:
                                    self.active_trader.positions[sid] = old_positions[sid]
                                    self.active_trader.positions[sid]["cfg"] = strat_cfg
                                else:
                                    self.active_trader.positions[sid] = {
                                        "in_pos": None, "position_id": None, "entry_bar_index": None,
                                        "entry_avg": None, "entry_qty": None, "tp_price": None, "sl_price": None,
                                        "local_guard_active": False, "entry_open_ms": None, "trailing_active": False,
                                        "trailing_max_price": 0.0, "entry_price_snapshot": 0.0,
                                        "fixed_tp_price": 0.0, "fixed_sl_price": 0.0, "cooldown": 0, "cfg": strat_cfg
                                    }
                            
                            for sym, iv in active_sym_ivs:
                                if (sym, iv) not in self.active_trader.ws_kline.sym_intervals:
                                    self.active_trader.ws_kline.sym_intervals.append((sym, iv))
                                    # 動態拉取新幣種 K 線並綁入 WS 引擎
                                    df = fetch_klines(self.active_trader.c, sym.replace("-", "").replace("_", "").upper(), iv, 500, safe=True)
                                    sym_bm = sym.replace("-", "").replace("_", "").upper()
                                    key = f"{sym_bm}_{iv}"
                                    self.active_trader.ws_kline.dfs[key] = df
                                    if self.active_trader.ws_kline.ws:
                                        ws_step = self.active_trader.ws_kline.step_map.get(iv, "1m")
                                        sub = {"action": "subscribe", "args": [f"futures/klineBin{ws_step}:{sym_bm}", f"futures/ticker:{sym_bm}"]}
                                        try: self.active_trader.ws_kline.ws.send(json.dumps(sub))
                                        except: pass
                                        
                            self.active_trader.active_sym_intervals = list(active_sym_ivs)
                            self.active_trader.all_symbols = list(set([s for s, _ in active_sym_ivs]))
                            self.active_trader._init_symbol_filters()
                            log("【實盤熱對接】底層交易引擎已成功切換至最新因子與權重配置，絕不干擾現有持倉！")
                            
                    except Exception as hot_e:
                        log(f"【實盤熱對接異常】發生崩潰 (強制隔離保護): {hot_e}\n{traceback.format_exc()}")
            except Exception as e:
                log(f"更新動態 JSON 面板時發生 UI 錯誤: {e}")
                
        self.after(0, _do_update)

    def _make_style(self):
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except Exception:
            pass
        self.palette = {
            "bg":"#0b0f14","panel":"#0f1824","panel2":"#0e1420","accent":"#3b82f6",
            "text":"#cbd5e1","muted":"#94a3b8","success":"#22c55e","warn":"#f59e0b","danger":"#ef4444"
        }
        p = self.palette
        style.configure(".", background=p["panel"], foreground=p["text"], fieldbackground=p["panel2"])
        style.configure("TNotebook", background=p["panel"], borderwidth=0)
        style.configure("TNotebook.Tab", padding=[16,10], background=p["panel2"], foreground=p["text"])
        style.map("TNotebook.Tab", background=[("selected", p["panel"])], foreground=[("selected","#e2e8f0")])
        style.configure("TLabel", background=p["panel"], foreground=p["text"])
        style.configure("TEntry", fieldbackground=p["panel2"])
        style.configure("TCombobox", fieldbackground=p["panel2"])
        style.configure("Accent.TButton", background=p["accent"], foreground="#ffffff", padding=10)
        style.map("Accent.TButton", background=[("active","#2563eb")])
        style.configure("Danger.TButton", background=p["danger"], foreground="#ffffff", padding=10)
        style.map("Danger.TButton", background=[("active","#dc2626")])
        style.configure("TProgressbar", troughcolor=p["panel2"], background=p["accent"])

    # ----- 單一 Canvas：背景 + 粒子 -----
    def _build_canvas_and_background(self):
        # 一張 Canvas 同時負責背景與粒子，徹底避免遮擋問題
        self.canvas = tk.Canvas(self, bd=0, highlightthickness=0, relief="flat", bg=self.palette["bg"])
        self.canvas.place(relx=0, rely=0, relwidth=1, relheight=1)

        # 背景相關
        self.bg_img_obj = None           # 必須持有 PhotoImage 參考
        self.bg_item_id = None           # Canvas 上的背景 image item
        self.bg_url_var = tk.StringVar(value=DEFAULT_BG_URL)
        self._bg_loader_lock = threading.Lock()
        self._bg_request_id = 0
        self._bg_last_image: Optional["Image.Image"] = None
        
        # 交易實例參照 (用於手動觸發測試)
        self.active_trader: Optional[Trader] = None

        # 粒子
        self.particles = []
        self._spawn_particles()

        # 首次載入背景
        self._reload_bg_async()

    def _spawn_particles(self):
        self.particles.clear()
        outline_color = self.palette["panel"]
        fill_color = "#94a3b8"
        for _ in range(36):
            x = random.random(); y = random.random()
            r = random.randint(2,5)
            spx = (random.random()-0.5)*0.28
            spy = (random.random()-0.5)*0.28
            pid = self.canvas.create_oval(0,0,0,0, outline=outline_color, width=0, fill=fill_color, tags=("particle",))
            self.particles.append([x,y,r,spx,spy,pid])
        self._animate_particles()

    def _animate_particles(self):
        try:
            w = max(1, self.winfo_width())
            h = max(1, self.winfo_height())
            for p in self.particles:
                p[0] = (p[0] + p[3]/max(1,w)) % 1.0
                p[1] = (p[1] + p[4]/max(1,h)) % 1.0
                cx = int(p[0]*w); cy = int(p[1]*h); r = p[2]
                self.canvas.coords(p[5], cx-r, cy-r, cx+r, cy+r)
            # 確保顆粒在背景之上
            self.canvas.tag_lower("bgimage")
            self.canvas.tag_raise("particle")
        except Exception:
            pass
        self.after(50, self._animate_particles)

    def _draw_fallback_gradient(self):
        try:
            self.canvas.delete("bgimage")
            self.canvas.delete("overlay")
            self.canvas.delete("bgrect")
            w = max(1, self.winfo_width())
            h = max(1, self.winfo_height())
            # 背景漸層（以一系列細長矩形模擬）
            for i in range(0, h, 3):
                c = int(15 + (i/h)*30)
                r = max(0, min(255, c))
                g = max(0, min(255, c+10))
                b = max(0, min(255, c+20))
                self.canvas.create_rectangle(0,i,w,i+3, fill=f"#{r:02x}{g:02x}{b:02x}", width=0, tags=("bgrect",))
            # 蓋一層半透明網點
            self.canvas.create_rectangle(0,0,w,h, fill="black", stipple="gray25", width=0, tags=("overlay",))
            # 粒子置頂
            self.canvas.tag_raise("particle")
        except Exception:
            pass

    def _set_bg_image_obj(self, pil_img: "Image.Image"):
        try:
            w = max(1, self.winfo_width())
            h = max(1, self.winfo_height())
            if w*h > 0:
                img = pil_img.resize((w, h), Image.LANCZOS).filter(ImageFilter.GaussianBlur(radius=1.6))
                self.bg_img_obj = ImageTk.PhotoImage(img)
                if self.bg_item_id is None:
                    self.bg_item_id = self.canvas.create_image(0,0, image=self.bg_img_obj, anchor="nw", tags=("bgimage",))
                else:
                    self.canvas.itemconfig(self.bg_item_id, image=self.bg_img_obj)
                # 清掉舊的漸層備援
                self.canvas.delete("bgrect")
                self.canvas.delete("overlay")
                # 再蓋一層半透明網點（柔化）
                self.canvas.create_rectangle(0,0,w,h, fill="black", stipple="gray25", width=0, tags=("overlay",))
                # 確保層級：背景 < overlay < 粒子
                self.canvas.tag_lower("bgimage")
                self.canvas.tag_raise("overlay")
                self.canvas.tag_raise("particle")
        except Exception as e:
            log(f"背景套圖失敗，改用漸層：{e}")
            self._draw_fallback_gradient()

    def _load_image_from_any(self, src: str) -> Optional["Image.Image"]:
        """
        Robust loader for background image. 會檢查多種來源（data:, file://, 本機路徑, http(s)）
        並在載入前嘗試驗證影像格式（避免伺服器回 HTML 等非影像內容時崩潰）。
        若 Pillow 未安裝或驗證失敗，回傳 None（上層會使用 fallback gradient）。
        """
        if not PIL_OK:
            return None
        s = (src or "").strip()

        # 嘗試安全導入 ImageFile / UnidentifiedImageError
        try:
            from PIL import ImageFile, UnidentifiedImageError
            ImageFile.LOAD_TRUNCATED_IMAGES = True
        except Exception:
            UnidentifiedImageError = Exception  # 若無法取得，使用一般 Exception 作為後備

        try:
            # 1) data URI (base64)
            if s.startswith("data:image/"):
                header, b64 = s.split(",", 1)
                import base64
                raw = base64.b64decode(b64)
                return Image.open(io.BytesIO(raw)).convert("RGB")

            # 2) file://
            if s.startswith("file://"):
                path = s[7:]
                with open(path, "rb") as f:
                    raw = f.read()
                return Image.open(io.BytesIO(raw)).convert("RGB")

            # 3) 本機路徑
            if os.path.exists(s):
                with open(s, "rb") as f:
                    raw = f.read()
                img = Image.open(io.BytesIO(raw))
                img.verify()
                return Image.open(io.BytesIO(raw)).convert("RGB")

            # 4) HTTP/HTTPS
            if s.startswith("http://") or s.startswith("https://"):
                r = requests.get(s, timeout=8)
                r.raise_for_status()
                raw = r.content
                try:
                    img = Image.open(io.BytesIO(raw))
                    img.verify()
                    return Image.open(io.BytesIO(raw)).convert("RGB")
                except UnidentifiedImageError:
                    log("BG Error: invalid image data.")
                    return None

        except Exception as e:
            log(f"BG Error: {e}")
        return None


    def _reload_bg_async(self):
        # 單一入口：每次呼叫都刷新 request_id，僅最後一次請求會生效
        if not PIL_OK:
            self._draw_fallback_gradient()
            return
        with self._bg_loader_lock:
            self._bg_request_id += 1
            req_id = self._bg_request_id
            src = self.bg_url_var.get().strip()

        def worker():
            img = self._load_image_from_any(src)
            def apply_if_fresh():
                with self._bg_loader_lock:
                    if req_id != self._bg_request_id:
                        return
                    if img is None:
                        self._draw_fallback_gradient()
                    else:
                        self._bg_last_image = img
                        self._set_bg_image_obj(img)
            try:
                self.after(0, apply_if_fresh)
            except Exception:
                pass

        threading.Thread(target=worker, daemon=True).start()

    @debounce(120)
    def _on_window_resize(self, *_):
        if self._bg_last_image is not None:
            try:
                self._set_bg_image_obj(self._bg_last_image)
                return
            except Exception:
                pass
        self._draw_fallback_gradient()

    # ----- 主內容面板 -----
    def _build_glass_panel(self):
        self.panel = tk.Frame(self, bg=self.palette["panel"], highlightthickness=0)
        self.panel.place(relx=0.5, rely=0.5, anchor="center", relwidth=0.9, relheight=0.86)
        self.panel.update_idletasks()
        self.panel.lift()

    # ----- Tabs -----
    def _build_tabs(self):
        nb = ttk.Notebook(self.panel)
        nb.pack(fill="both", expand=True, padx=14, pady=14)

        self.api_tab   = ttk.Frame(nb); nb.add(self.api_tab, text="API & 端點")
        self.mkt_tab   = ttk.Frame(nb); nb.add(self.mkt_tab, text="品種 & 場景")
        self.strat_tab = ttk.Frame(nb); nb.add(self.strat_tab, text="策略 TEMA_RSI")
        self.risk_tab  = ttk.Frame(nb); nb.add(self.risk_tab, text="風控（本地日內）")
        self.adv_tab   = ttk.Frame(nb); nb.add(self.adv_tab, text="進階 / 背景")

        # API & 端點
        self.api_key_var     = tk.StringVar(value=os.environ.get("SHEEP_BITMART_API_KEY", "").strip())
        self.secret_var      = tk.StringVar(value=os.environ.get("SHEEP_BITMART_SECRET", "").strip())
        self.memo_var        = tk.StringVar(value=os.environ.get("SHEEP_BITMART_MEMO", "").strip())
        self.trade_base_var  = tk.StringVar(value=os.environ.get("SHEEP_BITMART_TRADE_BASE", "https://api-cloud-v2.bitmart.com/").strip())
        self.quote_base_var  = tk.StringVar(value=os.environ.get("SHEEP_BITMART_QUOTE_BASE", "https://api-cloud-v2.bitmart.com/").strip())
        self.dry_run_var     = tk.BooleanVar(value=DRY_RUN_DEFAULT)
        self.timeout_var     = tk.IntVar(value=15)
        self.retries_var     = tk.IntVar(value=3)

        self._grid(self.api_tab, [
            ("API Key", self.api_key_var, 48),
            ("Secret", self.secret_var, 48, True),
            ("Memo (BitMart)", self.memo_var, 48),
            ("Base URL", self.trade_base_var, 48),
            ("請求 Timeout(秒)", self.timeout_var, 8),
            ("API 重試次數", self.retries_var, 8),
        ], cols=2)
        ttk.Checkbutton(self.api_tab, text="乾跑模式（不下單，只記錄）", variable=self.dry_run_var).grid(row=3, column=0, sticky="w", padx=10, pady=6, columnspan=2)

        # 品種 & 場景
        self.symbol_var      = tk.StringVar(value="ETHUSDT")
        self.interval_var    = tk.StringVar(value="30m")
        self.qty_var         = tk.DoubleVar(value=6.0)
        self.use_mark_var    = tk.BooleanVar(value=True)
        self.sleep_pad_var   = tk.DoubleVar(value=0.5)

        ttk.Label(self.mkt_tab, text="Interval").grid(row=0, column=0, sticky="e", padx=10, pady=6)
        cmb = ttk.Combobox(self.mkt_tab, textvariable=self.interval_var, values=list(INTERVAL_MS.keys()), state="readonly", width=10)
        cmb.grid(row=0, column=1, sticky="w", padx=10, pady=6)
        items = [
            ("Symbol", self.symbol_var, 16),
            ("每次下單數量(顆)", self.qty_var, 12),
            ("逼近新K 緩衝秒", self.sleep_pad_var, 12),
        ]
        self._grid(self.mkt_tab, items, start_row=1, cols=2)
        ttk.Checkbutton(self.mkt_tab, text="觸發單使用標記價（保守）", variable=self.use_mark_var).grid(row=5, column=0, sticky="w", padx=10, pady=6, columnspan=2)

        # 策略設定區 (動態自動同步 OOS 因子池)
        self.mode_var = tk.StringVar(value="multi")
        ttk.Radiobutton(self.strat_tab, text="單一策略(相容模式)", variable=self.mode_var, value="single").grid(row=0, column=0, sticky="w", padx=10, pady=5)
        ttk.Radiobutton(self.strat_tab, text="動態因子池 (自動同步前20名)", variable=self.mode_var, value="multi").grid(row=0, column=1, sticky="w", padx=10, pady=5)
        
        ttk.Label(self.strat_tab, text="總資金池權重限制(%) (預設 95%)").grid(row=0, column=2, sticky="e", padx=5, pady=5)
        ttk.Entry(self.strat_tab, textvariable=self.global_stake_pct_var, width=6).grid(row=0, column=3, sticky="w", padx=5, pady=5)
        
        ttk.Label(self.strat_tab, text="動態因子 JSON\n(系統自動讀取 CSV 更新)\n(已鎖定防竄改)").grid(row=1, column=0, sticky="nw", padx=10, pady=5)
        
        # 建立附帶捲動條的 JSON 展示區域
        json_frame = tk.Frame(self.strat_tab)
        json_frame.grid(row=1, column=1, columnspan=3, sticky="nsew", padx=10, pady=5)
        self.multi_json_text = tk.Text(json_frame, height=8, width=70, bg=self.palette["panel2"], fg=self.palette["text"], state="disabled")
        scroll_y = ttk.Scrollbar(json_frame, orient="vertical", command=self.multi_json_text.yview)
        self.multi_json_text.configure(yscrollcommand=scroll_y.set)
        self.multi_json_text.pack(side="left", fill="both", expand=True)
        scroll_y.pack(side="right", fill="y")
        
        # 單一模式參數保留以供退回
        self.fast_len_var = tk.IntVar(value=DEFAULT_FAST_LEN)
        self.slow_len_var = tk.IntVar(value=DEFAULT_SLOW_LEN)
        self.rsi_len_var  = tk.IntVar(value=DEFAULT_RSI_LEN)
        self.rsi_thr_var  = tk.DoubleVar(value=DEFAULT_RSI_THR)
        self.act_pct_var     = tk.DoubleVar(value=DEFAULT_ACT_PCT)
        self.trail_ticks_var = tk.IntVar(value=DEFAULT_TRAIL_TICKS)
        self.mintick_var     = tk.DoubleVar(value=DEFAULT_MINTICK)
        self.stake_pct_var   = tk.DoubleVar(value=DEFAULT_STAKE_PCT)
        self.tp_pct_strat_var= tk.DoubleVar(value=DEFAULT_TP_PCT)
        self.sl_pct_strat_var= tk.DoubleVar(value=DEFAULT_SL_PCT)
        self.max_hold_var = tk.IntVar(value=DEFAULT_MAX_HOLD)
        self.cooldown_var = tk.IntVar(value=0)
        self.enable_daily_guard_var = tk.BooleanVar(value=True)
        self.daily_limit_pct_var    = tk.DoubleVar(value=1.0)   # 1.0 表 1%
        self.daily_limit_usdt_var   = tk.DoubleVar(value=0.0)   # 0 代表忽略
        self.trade_fetch_int_var    = tk.IntVar(value=60)
        self.verbose_var            = tk.BooleanVar(value=True)
        self.factor_pool_url_var    = tk.StringVar(value=os.environ.get("SHEEP_FACTOR_POOL_URL", "https://sheep123.com").strip())
        self.factor_pool_token_var  = tk.StringVar(value=os.environ.get("SHEEP_FACTOR_POOL_TOKEN", "").strip())
        self.factor_pool_user_var   = tk.StringVar(value=os.environ.get("SHEEP_FACTOR_POOL_USER", "").strip())
        self.factor_pool_pass_var   = tk.StringVar(value=os.environ.get("SHEEP_FACTOR_POOL_PASS", "").strip())

        ttk.Checkbutton(self.risk_tab, text="啟用日內停利/停損（本地）", variable=self.enable_daily_guard_var).grid(row=0, column=0, sticky="w", padx=10, pady=6)
        self._grid(self.risk_tab, [
            ("幅度限制(%，預設 1%)", self.daily_limit_pct_var, 10),
            ("金額限制(USDT，可為 0 忽略)", self.daily_limit_usdt_var, 14),
            ("巡檢間隔(秒)", self.trade_fetch_int_var, 10),
        ], cols=2, start_row=1)
        ttk.Checkbutton(self.risk_tab, text="Verbose 日誌", variable=self.verbose_var).grid(row=3, column=0, sticky="w", padx=10, pady=6)
        ttk.Label(self.adv_tab, text="背景圖來源（URL / file:// / 本機路徑 / data:image/...）：").grid(row=0, column=0, sticky="w", padx=10, pady=6)
        # ※不要重新建立新的 StringVar，務必使用 _build_canvas_and_background 中已建立的 self.bg_url_var
        e = ttk.Entry(self.adv_tab, textvariable=self.bg_url_var, width=80); e.grid(row=1, column=0, sticky="w", padx=10, pady=6)
        ttk.Button(self.adv_tab, text="重新載入背景", style="Accent.TButton", command=self._reload_bg_async).grid(row=1, column=1, sticky="w", padx=10, pady=6)
        ttk.Button(self.adv_tab, text="選擇本機圖片", command=self._choose_bg_file).grid(row=1, column=2, sticky="w", padx=10, pady=6)
        ttk.Label(self.adv_tab, text="提示：背景載入在後台，不會卡 UI；若失敗會自動改用漸層備援。").grid(row=2, column=0, sticky="w", padx=10, pady=6, columnspan=3)
        self._grid(self.adv_tab, [
            ("因子池 Base URL", self.factor_pool_url_var, 48),
            ("Runtime Sync Token", self.factor_pool_token_var, 48, True),
            ("因子池帳號", self.factor_pool_user_var, 24),
            ("因子池密碼", self.factor_pool_pass_var, 24, True),
        ], cols=2, start_row=3)
    def _choose_bg_file(self):
        try:
            path = filedialog.askopenfilename(title="選擇背景圖片", filetypes=[("圖片","*.jpg;*.jpeg;*.png;*.bmp;*.gif;*.webp"),("全部檔案","*.*")])
            if not path: return
            self.bg_url_var.set(path)
            self._reload_bg_async()
        except Exception as e:
            messagebox.showerror("選擇失敗", str(e))
    def _grid(self, parent, items, cols=2, start_row=0, secret=False):
        """
        items = [(label, tkVariable, width[, is_secret_bool]), ...]
        • width 可為任意型別（int/float/str/None），最終安全化為 >=1 的 int
        • 徹底修正 TclError: expected integer but got "0.5"
        """
        r = start_row; c = 0
        for label, var, width, *rest in items:
            is_secret = bool(rest and rest[0])
            ttk.Label(parent, text=label).grid(row=r, column=c*2, sticky="e", padx=10, pady=6)
            try:
                w = int(max(1, math.ceil(float(width))))
            except Exception:
                w = 12
            e = ttk.Entry(parent, textvariable=var, width=w, show="•" if is_secret else "")
            e.grid(row=r, column=c*2+1, sticky="w", padx=10, pady=6)
            c += 1
            if c>=cols:
                c = 0; r += 1

    # ----- Footer：控制與日誌 -----
    def _build_footer(self):
        wrap = tk.Frame(self.panel, bg=self.palette["panel"])
        wrap.pack(fill="x", padx=14, pady=(0,14))

        self.run_btn = ttk.Button(wrap, text="啟動", style="Accent.TButton", command=self._on_run)
        self.stop_btn = ttk.Button(wrap, text="停止", style="Danger.TButton", command=self._on_stop, state="disabled")
        self.test_trade_btn = ttk.Button(wrap, text="交易測試", command=self._on_test_trade)
        self.check_btn = ttk.Button(wrap, text="健康檢查", command=self._on_check)
        self.save_btn = ttk.Button(wrap, text="保存設定", command=self._save_cfg_safely)
        self.load_btn = ttk.Button(wrap, text="載入設定", command=self._load_cfg_dialog)

        self.run_btn.pack(side="left", padx=6, pady=6)
        self.stop_btn.pack(side="left", padx=6, pady=6)
        self.test_trade_btn.pack(side="left", padx=6, pady=6)
        self.check_btn.pack(side="left", padx=6, pady=6)
        self.save_btn.pack(side="right", padx=6, pady=6)
        self.load_btn.pack(side="right", padx=6, pady=6)

        log_frame = tk.Frame(self.panel, bg=self.palette["panel"])
        log_frame.pack(fill="both", expand=True, padx=14, pady=(0,14))
        self.log_text = tk.Text(log_frame, height=12, bg=self.palette["panel2"], fg="#cbd5e1", insertbackground="#e2e8f0",
                                bd=0, highlightthickness=1, highlightcolor="#1f2937", relief="flat")
        self.log_text.pack(fill="both", expand=True)
        self.pb = ttk.Progressbar(self.panel, mode="indeterminate")
        self.pb.pack(fill="x", padx=14, pady=(0,14))

    # ----- 設定存取 -----
    def _collect_cfg(self) -> dict:
        symbol = (self.symbol_var.get() or "").strip().upper()
        if not symbol :
            raise ValueError("Symbol 格式錯誤，例：ETH-USDT")
        interval = self.interval_var.get()
        if interval not in INTERVAL_MS:
            raise ValueError("Interval 僅支援：" + ", ".join(INTERVAL_MS.keys()))
        qty = clamp(self.qty_var.get(), 0.000001, 1e12)
        
        # TEMA_RSI Params Collection
        cooldown = max(0, int(self.cooldown_var.get()))

        cfg = {
            "config_version": 2,
            "api_key": (self.api_key_var.get() or "").strip(),
            "secret": (self.secret_var.get() or "").strip(),
            "memo": (self.memo_var.get() or "").strip(),
            "trade_base": (self.trade_base_var.get() or "").strip(),
            "quote_base": (self.quote_base_var.get() or "").strip(),
            "timeout": max(5, int(self.timeout_var.get() or 15)),
            "retries": max(1, int(self.retries_var.get() or 3)),
            "dry_run": bool(self.dry_run_var.get()),
            "symbol": symbol,
            "interval": interval,
            "order_qty_token": float(qty),
            "use_mark_price": bool(self.use_mark_var.get()),
            "sleep_padding_sec": clamp(self.sleep_pad_var.get(), 0.0, 10.0),
            "trade_fetch_interval": max(15, int(self.trade_fetch_int_var.get() or 60)),
            "verbose": bool(self.verbose_var.get()),
            "global_stake_pct": float(self.global_stake_pct_var.get() or 95.0),
            "factor_pool_url": (self.factor_pool_url_var.get() or "").strip(),
            "factor_pool_token": (self.factor_pool_token_var.get() or "").strip(),
            "factor_pool_user": (self.factor_pool_user_var.get() or "").strip(),
            "factor_pool_pass": (self.factor_pool_pass_var.get() or "").strip(),

            # 本地日內停利/停損
            "daily_guard": {
                "enable": bool(self.enable_daily_guard_var.get()),
                "limit_pct": clamp(self.daily_limit_pct_var.get(), 0.0, 100.0),  # 1.0 = 1%
                "limit_usdt": max(0.0, float(self.daily_limit_usdt_var.get() or 0.0))
            },
            
            "mode": self.mode_var.get(),
            "multi_strategies_json": self.multi_json_text.get("1.0", tk.END).strip(),
            
            # 單一模式備用
            "single_family": "TEMA_RSI",
            "TEMA_RSI":{
                "fast_len": int(self.fast_len_var.get()),
                "slow_len": int(self.slow_len_var.get()),
                "rsi_len": int(self.rsi_len_var.get()),
                "rsi_thr": float(self.rsi_thr_var.get()),
                "activation_pct": float(self.act_pct_var.get()),
                "trail_ticks": int(self.trail_ticks_var.get()),
                "mintick": float(self.mintick_var.get()),
                "stake_pct": float(self.stake_pct_var.get()),
                "tp_pct_strat": float(self.tp_pct_strat_var.get()),
                "sl_pct_strat": float(self.sl_pct_strat_var.get()),
                "max_hold_list": [int(self.max_hold_var.get())], 
                "cooldown": cooldown
            },

            # 成本估計（本地記帳與守護）
            "fee_bps": 2.0,
            "slip_bps": 0.0,

            "_ui":{"bg_url": self.bg_url_var.get().strip()}
        }
        if not cfg["dry_run"]:
            if len(cfg["api_key"])<20 or len(cfg["secret"])<20:
                raise ValueError("API Key/Secret 似乎無效，若要實單請填寫正確或開啟乾跑模式")
        return cfg

    def _save_cfg_safely(self):
        try:
            cfg = self._collect_cfg()
            cfg_path = ensure_parent(CFG_FILE)
            with open(cfg_path, "w", encoding="utf-8") as f:
                json.dump(cfg, f, ensure_ascii=False, indent=2)
            log(f"設定已保存至 {cfg_path}（本機私密設定檔）")
        except Exception as e:
            messagebox.showerror("保存失敗", str(e))

    def _load_cfg_safely(self):
        try:
            for candidate in [Path(CFG_FILE), Path(CFG_FALLBACK_FILE), Path(CFG_PUBLIC_FILE), Path(CFG_TEMPLATE_FILE)]:
                if not candidate.exists():
                    continue
                with open(candidate, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                self._apply_cfg(cfg)
                log(f"已載入設定 {candidate}")
                return
        except Exception as e:
            log(f"載入設定失敗：{e}")

    def _load_cfg_dialog(self):
        path = filedialog.askopenfilename(title="選擇設定檔", filetypes=[("JSON","*.json")])
        if not path: return
        try:
            with open(path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            self._apply_cfg(cfg)
            log(f"已載入設定 {path}")
        except Exception as e:
            messagebox.showerror("載入失敗", str(e))

    def _apply_cfg(self, cfg: dict):
        def g(k, d=None): return cfg.get(k, d)
        self.api_key_var.set(g("api_key","")); self.secret_var.set(g("secret",""))
        self.memo_var.set(g("memo", ""))
        self.trade_base_var.set(g("trade_base", self.trade_base_var.get()))
        self.quote_base_var.set(g("quote_base", self.quote_base_var.get()))
        self.timeout_var.set(int(g("timeout", self.timeout_var.get())))
        self.retries_var.set(int(g("retries", self.retries_var.get())))
        self.dry_run_var.set(bool(g("dry_run", self.dry_run_var.get())))
        self.symbol_var.set(g("symbol", self.symbol_var.get()))
        self.interval_var.set(g("interval", self.interval_var.get()))
        self.qty_var.set(float(g("order_qty_token", self.qty_var.get())))
        self.use_mark_var.set(bool(g("use_mark_price", self.use_mark_var.get())))
        self.sleep_pad_var.set(float(g("sleep_padding_sec", self.sleep_pad_var.get())))
        self.trade_fetch_int_var.set(int(g("trade_fetch_interval", self.trade_fetch_int_var.get())))
        self.verbose_var.set(bool(g("verbose", self.verbose_var.get())))
        self.mode_var.set(g("mode", self.mode_var.get()))
        self.global_stake_pct_var.set(float(g("global_stake_pct", self.global_stake_pct_var.get())))
        self.factor_pool_url_var.set(g("factor_pool_url", self.factor_pool_url_var.get()))
        self.factor_pool_token_var.set(g("factor_pool_token", self.factor_pool_token_var.get()))
        self.factor_pool_user_var.set(g("factor_pool_user", self.factor_pool_user_var.get()))
        self.factor_pool_pass_var.set(g("factor_pool_pass", self.factor_pool_pass_var.get()))

        multi_json_raw = g("multi_strategies_json", None)
        if multi_json_raw is not None:
            if isinstance(multi_json_raw, (list, dict)):
                multi_json_text = json.dumps(multi_json_raw, ensure_ascii=False, indent=2)
            else:
                multi_json_text = str(multi_json_raw or "").strip()
            self.multi_json_text.config(state="normal")
            self.multi_json_text.delete("1.0", tk.END)
            if multi_json_text:
                self.multi_json_text.insert("1.0", multi_json_text)
            self.multi_json_text.config(state="disabled")

        dguard = g("daily_guard", {})
        self.enable_daily_guard_var.set(bool(dguard.get("enable", self.enable_daily_guard_var.get())))
        self.daily_limit_pct_var.set(float(dguard.get("limit_pct", self.daily_limit_pct_var.get())))
        self.daily_limit_usdt_var.set(float(dguard.get("limit_usdt", self.daily_limit_usdt_var.get())))
        TR = g("TEMA_RSI", {})
        if TR:
            self.fast_len_var.set(int(TR.get("fast_len", DEFAULT_FAST_LEN)))
            self.slow_len_var.set(int(TR.get("slow_len", DEFAULT_SLOW_LEN)))
            self.rsi_len_var.set(int(TR.get("rsi_len", DEFAULT_RSI_LEN)))
            self.rsi_thr_var.set(float(TR.get("rsi_thr", DEFAULT_RSI_THR)))
            
            self.act_pct_var.set(float(TR.get("activation_pct", DEFAULT_ACT_PCT)))
            self.trail_ticks_var.set(int(TR.get("trail_ticks", DEFAULT_TRAIL_TICKS)))
            self.mintick_var.set(float(TR.get("mintick", DEFAULT_MINTICK)))
            self.stake_pct_var.set(float(TR.get("stake_pct", DEFAULT_STAKE_PCT)))
            self.tp_pct_strat_var.set(float(TR.get("tp_pct_strat", DEFAULT_TP_PCT)))
            self.sl_pct_strat_var.set(float(TR.get("sl_pct_strat", DEFAULT_SL_PCT)))
            
            mh_list = TR.get("max_hold_list", [DEFAULT_MAX_HOLD])
            self.max_hold_var.set(int(mh_list[0] if mh_list else DEFAULT_MAX_HOLD))
            self.cooldown_var.set(int(TR.get("cooldown", self.cooldown_var.get())))

        ui = g("_ui",{})
        if ui: self.bg_url_var.set(ui.get("bg_url", self.bg_url_var.get()))

    def _on_test_trade(self):
        """手動觸發測試交易流程"""
        if not self.active_trader:
            messagebox.showwarning("無法測試", "請先點擊「啟動」讓機器人運轉，才能進行即時交易測試。")
            return
        if not messagebox.askyesno("確認測試", "確定要執行「真實下單測試」嗎？\n\n這將會：\n1. 市價開多倉\n2. 掛上止盈止損\n3. 等待 10 秒\n4. 市價全平\n\n(若是實盤將產生手續費與盈虧)"):
            return
        self.active_trader.manual_test_trigger = True
        log("指令已發送：等待下一次循環執行測試流程...")

    def _on_check(self):
        try:
            cfg = self._collect_cfg()
        except Exception as e:
            messagebox.showerror("輸入錯誤", str(e)); return
        self.pb.start(8)
        def work():
            try:
                c = BitmartClient(cfg["api_key"], cfg["secret"], cfg["memo"], cfg["trade_base"], cfg["quote_base"],
                                timeout=cfg["timeout"], retries=cfg["retries"], retry_sleep=0.8, dry_run=True)
                j = c.get_contracts()
                assert "data" in j, "contracts 無 data"
                log("合約清單 正常")
                price_step=0.0001; qty_step=1.0; min_qty=1.0
                for it in j.get("data", []):
                    if it.get("symbol")==cfg["symbol"]:
                        price_step=float(it.get("priceStep",0.0001)); qty_step=float(it.get("quantityStep",1)); min_qty=float(it.get("minQty",1))
                        break
                log(f"步進單位 正常: 價格跳動={price_step} 數量單位={qty_step} 最小數量={min_qty}")
                df = fetch_klines(c, cfg["symbol"], cfg["interval"], 100, safe=True)
                src = df.attrs.get("source")
                log(f"K 線資料 正常: 筆數={len(df)} 來源={src}")
                if not cfg["dry_run"] and (len(cfg["api_key"])<20 or len(cfg["secret"])<20):
                    log("警告: API Key/Secret 似乎無效，建議乾跑或補齊")
                log("健康檢查完成")
            except Exception as e:
                log(f"健康檢查失敗：{e}\n{traceback.format_exc()}")
            finally:
                self.pb.stop()
        threading.Thread(target=work, daemon=True).start()

    # ----- 啟停 -----
    def _on_run(self):
        with ui_running_lock:
            if self.run_btn["state"] == "disabled":
                return
            try:
                cfg = self._collect_cfg()
            except Exception as e:
                messagebox.showerror("輸入錯誤", str(e)); return
            stop_event.clear()
            self.run_btn.config(state="disabled")
            self.stop_btn.config(state="normal")
            self.pb.start(8)
            t = threading.Thread(target=self._run_worker, args=(cfg,), daemon=True)
            t.start()

    def _run_worker(self, cfg: dict):
        try:
            c = BitmartClient(cfg["api_key"], cfg["secret"], cfg["memo"], cfg["trade_base"], cfg["quote_base"],
                            timeout=cfg["timeout"], retries=cfg["retries"], retry_sleep=0.8, dry_run=cfg["dry_run"])
            try:
                bal = c.get_balance()
                # log(f"Balance: {fmt_json(bal)}") # Verbose
            except Exception as e:
                log(f"帳戶餘額讀取失敗: {e}")
            trader = Trader(c, cfg)
            self.active_trader = trader
            
            # [專家級修正] 適應多幣種規格字典輸出，並修正 qty 為 default_qty
            for sym, info in trader.symbol_info.items():
                log(f"交易規格確認 [{sym}]：最小跳動={info.get('price_step', 0.0001)}, 最小張數={info.get('qty_step', 1.0)}, 最小數量={info.get('min_qty', 1.0)}")
            log(f"動態倉位基準數量：{trader.default_qty} (預設)")
            
            if trader.local_equity_baseline is not None:
                log(f"本日淨值基準 ({trader.local_day_anchor}): {trader.local_equity_baseline:.2f} U | 風控限制: {trader.daily_limit_pct*100:.1f}% / {trader.daily_limit_usdt} U")
            else:
                log("警告: 無法取得淨值，百分比風控已自動停用。")
            trader.run()
        except SystemExit:
            pass
        except Exception as e:
            log(f"致命：{e}\n{traceback.format_exc()}")
        finally:
            self.pb.stop()
            try:
                self.run_btn.config(state="normal")
                self.stop_btn.config(state="disabled")
            except Exception:
                pass

    def _on_stop(self):
        stop_event.set()
        self.active_trader = None
        log("Stopping...")

    # ----- 日誌輸出 -----
    def _drain_logs(self):
        try:
            drained = 0
            while drained < 200:
                line = log_q.get_nowait()
                self.log_text.insert("end", line+"\n")
                self.log_text.see("end")
                drained += 1
        except queue.Empty:
            pass
        self.after(50 if not log_q.empty() else 80, self._drain_logs)

# ============ 主程式入口 ============
def main():
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"\n====== {APP_NAME} start {now_ts()} ======\n")
    except Exception:
        pass
    
    init_csv_log() # [專家新增] 初始化本次運行的 CSV 紀錄檔
    app = AnimatedUI()
    try:
        app.mainloop()
    except Exception as e:
        # 絕不讓 UI 例外崩潰終端
        log(f"UI 崩潰攔截：{e}\n{traceback.format_exc()}")

if __name__ == "__main__":
    main()

