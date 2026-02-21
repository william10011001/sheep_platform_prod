# backtest_panel.py
# ------------------------------------------------------------
# 多指標格點搜尋回測面板（單檔版本）
# - 嚴謹的交易模型：次一根開盤入場、內含 TP/SL/持倉m出場、0.02% 單邊費用、0%滑點
# - 30+ 技術指標本地計算，供格點掃描
# - 可選 Numba JIT 加速（如環境支援），不支援則自動降級
# - UI: Streamlit。輸出：CSV、Top-N 面板、最佳組合的淨值曲線
# ------------------------------------------------------------

import os
import math
import json
import time
import itertools
import warnings
from datetime import datetime, timezone
from functools import lru_cache
from typing import Dict, Tuple, List, Optional
import traceback

import numpy as np
import pandas as pd
import requests
import threading
from pathlib import Path
from dateutil.relativedelta import relativedelta
from filelock import FileLock, Timeout

# UI 與視覺
import streamlit as st
import plotly.graph_objects as go
# ----------------------------- BitMart 行情資料同步 ----------------------------- #

BITMART_BASE_URL = "https://api-cloud.bitmart.com"
BITMART_SPOT_KLINES_ENDPOINT = "/spot/quotation/v3/klines"
BITMART_KLINE_STEP_MINUTES = [1, 5, 15, 30, 60, 120, 240, 1440, 10080, 43200]

BITMART_UI_TIMEFRAMES = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "2h": 120,
    "4h": 240,
    "1d": 1440,
}

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)


class BitMartApiError(RuntimeError):
    pass


def _step_min_to_label(step_min: int) -> str:
    if step_min < 60:
        return f"{step_min}m"
    if step_min < 1440 and step_min % 60 == 0:
        return f"{step_min // 60}h"
    if step_min == 1440:
        return "1d"
    if step_min == 10080:
        return "1w"
    if step_min == 43200:
        return "1mo"
    return f"{step_min}m"


def _floor_to_step(ts_sec: int, step_sec: int) -> int:
    if step_sec <= 0:
        return ts_sec
    return (ts_sec // step_sec) * step_sec


def _last_closed_open_ts(step_min: int, now_ts_sec: Optional[int] = None) -> int:
    step_sec = int(step_min) * 60
    if now_ts_sec is None:
        now_ts_sec = int(time.time())
    open_now = _floor_to_step(now_ts_sec, step_sec)
    return int(open_now - step_sec)


def _bitmart_csv_paths(symbol: str, step_min: int) -> Tuple[Path, Path, Path]:
    safe_symbol = str(symbol).strip().replace("/", "_").replace(":", "_")
    label = _step_min_to_label(int(step_min))
    csv_path = DATA_DIR / f"{safe_symbol}_{label}_3y.csv"
    meta_path = DATA_DIR / f"{safe_symbol}_{label}_3y.meta.json"
    lock_path = Path(str(csv_path) + ".lock")
    return csv_path, meta_path, lock_path


def _read_last_data_line(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    try:
        with path.open("rb") as f:
            f.seek(0, os.SEEK_END)
            end = f.tell()
            if end <= 0:
                return None
            buf = bytearray()
            pos = end - 1
            while pos >= 0:
                f.seek(pos)
                b = f.read(1)
                if b == b"\n" and buf:
                    break
                if b != b"\n" and b != b"\r":
                    buf.extend(b)
                pos -= 1
                if len(buf) > 1024 * 1024:
                    break
            line = buf[::-1].decode("utf-8", errors="ignore").strip()
            return line if line else None
    except Exception:
        return None


def _read_first_data_line(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            header = f.readline()
            if not header:
                return None
            line = f.readline()
            return line.strip() if line else None
    except Exception:
        return None


def _parse_csv_ts_to_sec(ts_str: str) -> Optional[int]:
    ts_str = str(ts_str).strip()
    if not ts_str:
        return None
    try:
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        try:
            dt = pd.to_datetime(ts_str, utc=True, errors="coerce")
            if pd.isna(dt):
                return None
            return int(dt.to_pydatetime().timestamp())
        except Exception:
            return None


def _csv_quick_status(symbol: str, step_min: int) -> Dict[str, object]:
    csv_path, meta_path, lock_path = _bitmart_csv_paths(symbol, step_min)
    out: Dict[str, object] = {
        "symbol": str(symbol),
        "step_min": int(step_min),
        "csv_path": str(csv_path),
        "meta_path": str(meta_path),
        "exists": csv_path.exists(),
        "meta_exists": meta_path.exists(),
        "start_ts_sec": None,
        "end_ts_sec": None,
        "is_current": False,
    }
    if not csv_path.exists():
        return out

    try:
        with FileLock(str(lock_path), timeout=5):
            first = _read_first_data_line(csv_path)
            last = _read_last_data_line(csv_path)
    except Exception:
        first = _read_first_data_line(csv_path)
        last = _read_last_data_line(csv_path)

    if first:
        first_ts = first.split(",", 1)[0]
        out["start_ts_sec"] = _parse_csv_ts_to_sec(first_ts)
    if last:
        last_ts = last.split(",", 1)[0]
        out["end_ts_sec"] = _parse_csv_ts_to_sec(last_ts)

    end_closed = _last_closed_open_ts(step_min)
    if out["end_ts_sec"] is not None and int(out["end_ts_sec"]) >= int(end_closed):
        out["is_current"] = True
    return out


# ----------------------------- BitMart 公共端點節流（程序內） ----------------------------- #

_BM_THROTTLE_LOCK = threading.Lock()
_BM_NEXT_REQUEST_TS = 0.0
_BM_MIN_INTERVAL_S = 0.08


def _bm_wait_request_slot() -> None:
    """避免多執行緒同時拉 K 線導致 429/30013。"""
    global _BM_NEXT_REQUEST_TS
    now = time.time()
    with _BM_THROTTLE_LOCK:
        wait_s = float(_BM_NEXT_REQUEST_TS - now)
        if wait_s < 0.0:
            wait_s = 0.0
        _BM_NEXT_REQUEST_TS = max(float(_BM_NEXT_REQUEST_TS), now) + float(_BM_MIN_INTERVAL_S)
    if wait_s > 0.0:
        time.sleep(wait_s)


def _bm_bump_cooldown(seconds: float) -> None:
    global _BM_NEXT_REQUEST_TS
    s = float(max(0.0, seconds))
    now = time.time()
    with _BM_THROTTLE_LOCK:
        _BM_NEXT_REQUEST_TS = max(float(_BM_NEXT_REQUEST_TS), now + s)


class BitMartRestClient:
    def __init__(self, base_url: str = BITMART_BASE_URL, timeout: Tuple[float, float] = (3.05, 30.0)):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()

    def get_klines(self,
                  symbol: str,
                  step_min: int,
                  limit: int = 200,
                  before: Optional[int] = None,
                  after: Optional[int] = None) -> Tuple[List[List[str]], Dict[str, str]]:
        if int(step_min) not in BITMART_KLINE_STEP_MINUTES:
            raise ValueError(f"BitMart step 不支援：{step_min}")

        def _parse_reset_seconds(headers: Dict[str, str]) -> float:
            v = headers.get("X-BM-RateLimit-Reset")
            if v is None:
                return 0.0
            try:
                x = float(v)
            except Exception:
                return 0.0

            now = time.time()
            if x > 1e12:
                return float(max(0.0, (x / 1000.0) - now))
            if x > 1e9:
                return float(max(0.0, x - now))
            return float(max(0.0, x))

        def _sleep_backoff(attempt: int, headers: Optional[Dict[str, str]] = None) -> None:
            wait_s = 0.0
            if headers:
                ra = headers.get("Retry-After") or headers.get("retry-after")
                if ra is not None:
                    try:
                        wait_s = float(ra)
                    except Exception:
                        wait_s = 0.0
                if wait_s <= 0.0:
                    wait_s = _parse_reset_seconds(headers)

            if wait_s <= 0.0:
                base = 0.6 * (2.0 ** float(min(6, max(0, int(attempt)))))
                wait_s = float(min(8.0, base))

            frac = float(time.time() - math.floor(time.time()))
            jitter = float(min(0.25, 0.05 + frac * 0.18))
            wait_s = float(max(0.05, wait_s + jitter))

            _bm_bump_cooldown(wait_s)
            time.sleep(wait_s)

        params: Dict[str, object] = {
            "symbol": str(symbol).strip(),
            "step": int(step_min),
            "limit": int(min(200, max(1, limit))),
        }
        if before is not None:
            params["before"] = int(before)
        if after is not None:
            params["after"] = int(after)

        url = f"{self.base_url}{BITMART_SPOT_KLINES_ENDPOINT}"

        last_err: Optional[str] = None
        max_attempts = 8

        for attempt in range(max_attempts):
            _bm_wait_request_slot()
            try:
                resp = self._session.get(url, params=params, timeout=self.timeout)
            except Exception as e:
                last_err = f"network_error: {e}"
                _sleep_backoff(attempt)
                continue

            headers = {k: str(v) for k, v in resp.headers.items()}

            if int(resp.status_code) in (429, 418, 503, 502, 504):
                self._apply_rate_limit(headers, force_sleep=False)
                last_err = f"http_{int(resp.status_code)}"
                _sleep_backoff(attempt, headers)
                continue

            if resp.status_code != 200:
                raise BitMartApiError(f"HTTP {resp.status_code}: {resp.text[:300]}")

            try:
                payload = resp.json()
            except Exception as e:
                raise BitMartApiError(f"JSON 解析失敗：{e}") from e

            code = payload.get("code")
            if int(code or 0) == 1000:
                data = payload.get("data") or []
                if not isinstance(data, list):
                    data = []
                self._apply_rate_limit(headers, force_sleep=False)
                return data, headers

            msg = str(payload.get("message") or "")

            if int(code or 0) == 30013:
                self._apply_rate_limit(headers, force_sleep=False)
                last_err = f"code_30013: {msg}"
                _sleep_backoff(attempt, headers)
                continue

            if int(code or 0) in (70002, 71001, 71002, 71003, 71004, 71005):
                raise BitMartApiError(f"BitMart 回應 code={code}, message={msg}")

            last_err = f"code_{code}: {msg}"
            if attempt < max_attempts - 1:
                _sleep_backoff(attempt, headers)
                continue
            raise BitMartApiError(f"BitMart 回應 code={code}, message={msg}, data={payload.get('data')}")

        raise BitMartApiError(f"BitMart 請求失敗：{last_err or 'unknown'}")

    def _apply_rate_limit(self, headers: Dict[str, str], force_sleep: bool = False) -> None:
        remaining_s = headers.get("X-BM-RateLimit-Remaining")
        limit_s = headers.get("X-BM-RateLimit-Limit")
        reset_s = headers.get("X-BM-RateLimit-Reset")

        try:
            remaining = int(float(remaining_s)) if remaining_s is not None else -1
            limit = int(float(limit_s)) if limit_s is not None else -1
            reset_raw = float(reset_s) if reset_s is not None else 0.0
        except Exception:
            return

        now = time.time()
        reset_wait = 0.0
        if reset_raw > 1e12:
            reset_wait = max(0.0, (reset_raw / 1000.0) - now)
        elif reset_raw > 1e9:
            reset_wait = max(0.0, reset_raw - now)
        else:
            reset_wait = max(0.0, reset_raw)

        if limit > 0 and remaining >= 0 and remaining <= 0 and reset_wait > 0.0:
            wait_s = float(min(30.0, reset_wait + 0.25))
            _bm_bump_cooldown(wait_s)
            if force_sleep:
                time.sleep(wait_s)


def _bitmart_rows_to_df(rows: List[List[str]]) -> pd.DataFrame:
    out_rows = []
    for r in rows:
        if not isinstance(r, (list, tuple)) or len(r) < 6:
            continue
        try:
            ts_raw = int(float(r[0]))
            # 部分端點/節點可能回傳毫秒 timestamp
            ts_sec = int(ts_raw // 1000) if ts_raw > 1000000000000 else int(ts_raw)
        except Exception:
            continue
        out_rows.append({
            "_ts_sec": ts_sec,
            "ts": datetime.fromtimestamp(ts_sec, tz=timezone.utc).isoformat(),
            "open": r[1],
            "high": r[2],
            "low": r[3],
            "close": r[4],
            "volume": r[5],
        })
    if not out_rows:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume", "_ts_sec"])
    df = pd.DataFrame(out_rows)
    df = df.sort_values("_ts_sec").reset_index(drop=True)
    return df


def _write_meta(meta_path: Path, payload: Dict[str, object]) -> None:
    tmp = meta_path.with_suffix(meta_path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(str(tmp), str(meta_path))


def _full_sync_bitmart_csv(symbol: str,
                           step_min: int,
                           years: int = 3,
                           progress_cb: Optional[callable] = None) -> str:
    csv_path, meta_path, lock_path = _bitmart_csv_paths(symbol, step_min)
    step_min = int(step_min)
    step_sec = step_min * 60

    now_utc = datetime.now(timezone.utc)
    start_dt = now_utc - relativedelta(years=int(years))
    start_ts = _floor_to_step(int(start_dt.timestamp()), step_sec)
    end_ts = _last_closed_open_ts(step_min, int(now_utc.timestamp()))

    after = int(start_ts - step_sec)
    expected_total = int((end_ts - start_ts) // step_sec) + 1
    written = 0
    guard = 0

    fetch_lock_path = Path(str(csv_path) + ".net.lock")
    fetch_timeout = 60.0 if not csv_path.exists() else 2.0

    try:
        with FileLock(str(fetch_lock_path), timeout=float(fetch_timeout)):
            client = BitMartRestClient()

            tmp_csv = csv_path.with_suffix(csv_path.suffix + ".tmp")
            with tmp_csv.open("w", encoding="utf-8", newline="") as f:
                f.write("ts,open,high,low,close,volume\n")

            while True:
                guard += 1
                if guard > 1000000:
                    raise RuntimeError("同步迴圈超過限制次數")

                rows, _hdr = client.get_klines(symbol=symbol, step_min=step_min, limit=200, after=after)
                df = _bitmart_rows_to_df(rows)
                if df.empty:
                    break

                df = df[df["_ts_sec"] > after]
                df = df[(df["_ts_sec"] >= start_ts) & (df["_ts_sec"] <= end_ts)]
                if df.empty:
                    if after >= end_ts:
                        break
                    after = int(after + 200 * step_sec)
                    continue

                with tmp_csv.open("a", encoding="utf-8", newline="") as f:
                    df.drop(columns=["_ts_sec"]).to_csv(f, index=False, header=False)

                written += int(len(df))
                after = int(df["_ts_sec"].max())

                if progress_cb is not None and expected_total > 0:
                    progress_cb(min(1.0, written / expected_total), f"{_step_min_to_label(step_min)} 已寫入 {written} / {expected_total}")

                if after >= end_ts:
                    break
    except Timeout:
        if csv_path.exists():
            return str(csv_path)
        raise RuntimeError("資料同步鎖定超時，請稍後重試。")

    if written <= 0:
        raise RuntimeError("同步結果為空，請確認交易對與時間級別是否有資料")

    try:
        with FileLock(str(lock_path), timeout=10):
            os.replace(str(tmp_csv), str(csv_path))
            _write_meta(meta_path, {
                "exchange": "bitmart",
                "symbol": str(symbol).strip(),
                "step_min": int(step_min),
                "years": int(years),
                "start_ts_sec": int(start_ts),
                "end_ts_sec": int(end_ts),
                "last_sync_utc": datetime.now(timezone.utc).isoformat(),
                "rows_written": int(written),
                "csv": str(csv_path),
            })
    except Timeout:
        try:
            if tmp_csv.exists():
                tmp_csv.unlink()
        except Exception:
            pass
        if csv_path.exists():
            return str(csv_path)
        raise RuntimeError("資料檔正在被其他程序更新，無法完成覆蓋寫入。")

    return str(csv_path)


def _append_update_bitmart_csv(symbol: str,
                               step_min: int,
                               years: int = 3) -> str:
    csv_path, meta_path, lock_path = _bitmart_csv_paths(symbol, step_min)
    step_min = int(step_min)
    step_sec = step_min * 60

    if not csv_path.exists() or not meta_path.exists():
        return _full_sync_bitmart_csv(symbol=symbol, step_min=step_min, years=years, progress_cb=None)

    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return _full_sync_bitmart_csv(symbol=symbol, step_min=step_min, years=years, progress_cb=None)

    if str(meta.get("exchange", "")).lower() != "bitmart":
        return _full_sync_bitmart_csv(symbol=symbol, step_min=step_min, years=years, progress_cb=None)
    if str(meta.get("symbol", "")).strip() != str(symbol).strip():
        return _full_sync_bitmart_csv(symbol=symbol, step_min=step_min, years=years, progress_cb=None)
    if int(meta.get("step_min", -1)) != int(step_min):
        return _full_sync_bitmart_csv(symbol=symbol, step_min=step_min, years=years, progress_cb=None)
    if int(meta.get("years", years)) != int(years):
        return _full_sync_bitmart_csv(symbol=symbol, step_min=step_min, years=years, progress_cb=None)

    try:
        with csv_path.open("r", encoding="utf-8", errors="ignore") as f:
            header = f.readline().strip()
    except Exception:
        return _full_sync_bitmart_csv(symbol=symbol, step_min=step_min, years=years, progress_cb=None)

    if header != "ts,open,high,low,close,volume":
        return _full_sync_bitmart_csv(symbol=symbol, step_min=step_min, years=years, progress_cb=None)

    status = _csv_quick_status(symbol, step_min)
    end_closed = _last_closed_open_ts(step_min)
    last_ts = status.get("end_ts_sec")
    if last_ts is None:
        return _full_sync_bitmart_csv(symbol=symbol, step_min=step_min, years=years, progress_cb=None)

    last_ts = int(last_ts)
    if last_ts >= end_closed:
        return str(csv_path)

    dfs = []
    fetched = 0
    guard = 0

    fetch_lock_path = Path(str(csv_path) + ".net.lock")
    try:
        with FileLock(str(fetch_lock_path), timeout=1.0):
            client = BitMartRestClient()

            status = _csv_quick_status(symbol, step_min)
            last_ts2 = status.get("end_ts_sec")
            if last_ts2 is not None and int(last_ts2) >= end_closed:
                return str(csv_path)
            if last_ts2 is None:
                return _full_sync_bitmart_csv(symbol=symbol, step_min=step_min, years=years, progress_cb=None)

            after = int(last_ts2)
            while True:
                guard += 1
                if guard > 1000000:
                    raise RuntimeError("更新迴圈超過限制次數")

                rows, _hdr = client.get_klines(symbol=symbol, step_min=step_min, limit=200, after=after)
                df = _bitmart_rows_to_df(rows)
                if df.empty:
                    break

                df = df[df["_ts_sec"] > after]
                df = df[df["_ts_sec"] <= end_closed]
                if df.empty:
                    break

                dfs.append(df)
                fetched += int(len(df))
                after = int(df["_ts_sec"].max())

                if after >= end_closed:
                    break
    except Timeout:
        return str(csv_path)

    if fetched <= 0:
        try:
            with FileLock(str(lock_path), timeout=3):
                _write_meta(meta_path, {
                    "exchange": "bitmart",
                    "symbol": str(symbol).strip(),
                    "step_min": int(step_min),
                    "years": int(years),
                    "last_sync_utc": datetime.now(timezone.utc).isoformat(),
                    "last_closed_open_ts_sec": int(end_closed),
                    "rows_appended": 0,
                    "csv": str(csv_path),
                })
        except Timeout:
            pass
        return str(csv_path)

    try:
        with FileLock(str(lock_path), timeout=3):
            last_line = _read_last_data_line(csv_path)
            last_now = None
            if last_line:
                last_now = _parse_csv_ts_to_sec(last_line.split(",", 1)[0])
            if last_now is None:
                return _full_sync_bitmart_csv(symbol=symbol, step_min=step_min, years=years, progress_cb=None)

            rows_written = 0
            with csv_path.open("a", encoding="utf-8", newline="") as f:
                for d in dfs:
                    d2 = d[d["_ts_sec"] > int(last_now)]
                    if d2.empty:
                        continue
                    d2.drop(columns=["_ts_sec"]).to_csv(f, index=False, header=False)
                    rows_written += int(len(d2))
                    last_now = int(d2["_ts_sec"].max())

            now_utc = datetime.now(timezone.utc)
            start_dt = now_utc - relativedelta(years=int(years))
            keep_from_ts = _floor_to_step(int(start_dt.timestamp()), step_sec)
            buffer_sec = 86400
            first_line = _read_first_data_line(csv_path)
            first_ts = _parse_csv_ts_to_sec(first_line.split(",", 1)[0]) if first_line else None

            if first_ts is not None and int(first_ts) < int(keep_from_ts - buffer_sec):
                tmp_csv = csv_path.with_suffix(csv_path.suffix + ".compact.tmp")
                with csv_path.open("r", encoding="utf-8", errors="ignore") as fin, tmp_csv.open("w", encoding="utf-8", newline="") as fout:
                    header = fin.readline()
                    if header:
                        fout.write(header)
                    for line in fin:
                        if not line.strip():
                            continue
                        ts_part = line.split(",", 1)[0]
                        ts_sec = _parse_csv_ts_to_sec(ts_part)
                        if ts_sec is None:
                            continue
                        if int(ts_sec) >= int(keep_from_ts):
                            fout.write(line)
                os.replace(str(tmp_csv), str(csv_path))

            _write_meta(meta_path, {
                "exchange": "bitmart",
                "symbol": str(symbol).strip(),
                "step_min": int(step_min),
                "years": int(years),
                "last_sync_utc": datetime.now(timezone.utc).isoformat(),
                "last_closed_open_ts_sec": int(end_closed),
                "rows_appended": int(rows_written),
                "csv": str(csv_path),
            })
    except Timeout:
        return str(csv_path)

    return str(csv_path)


class BitMartUpdateService:
    def __init__(self):
        self._lock = threading.Lock()
        self._workers: Dict[Tuple[str, int], Tuple[threading.Thread, threading.Event]] = {}

    def ensure_worker(self, symbol: str, step_min: int, years: int = 3) -> None:
        key = (str(symbol).strip(), int(step_min))
        with self._lock:
            existing = self._workers.get(key)
            if existing and existing[0].is_alive():
                return
            stop_evt = threading.Event()
            th = threading.Thread(
                target=self._worker_loop,
                args=(key[0], key[1], years, stop_evt),
                daemon=True,
                name=f"bm_sync_{key[0]}_{key[1]}m"
            )
            self._workers[key] = (th, stop_evt)
            th.start()

    def _worker_loop(self, symbol: str, step_min: int, years: int, stop_evt: threading.Event) -> None:


        step_sec = int(step_min) * 60
        while not stop_evt.is_set():
            now_ts = int(time.time())
            next_boundary = _floor_to_step(now_ts, step_sec) + step_sec
            sleep_for = float(max(0, next_boundary - now_ts + 2))
            if stop_evt.wait(sleep_for):
                break
            try:
                _append_update_bitmart_csv(symbol=symbol, step_min=step_min, years=years)
            except Exception:
                stop_evt.wait(min(15.0, float(step_sec)))


BITMART_UPDATE_SERVICE = BitMartUpdateService()


def ensure_bitmart_data(symbol: str,
                        main_step_min: int,
                        years: int = 3,
                        auto_sync: bool = True,
                        force_full: bool = False,
                        progress_cb: Optional[callable] = None) -> Tuple[str, str]:
    symbol = str(symbol).strip()
    main_step_min = int(main_step_min)

    csv_1m, _, _ = _bitmart_csv_paths(symbol, 1)
    csv_main, _, _ = _bitmart_csv_paths(symbol, main_step_min)

    need_1m = bool(force_full) or (not csv_1m.exists())
    need_main = bool(force_full) or (not csv_main.exists())

    if need_1m:
        _full_sync_bitmart_csv(symbol=symbol, step_min=1, years=years, progress_cb=progress_cb)
    else:
        if not bool(auto_sync):
            _append_update_bitmart_csv(symbol=symbol, step_min=1, years=years)

    if need_main:
        _full_sync_bitmart_csv(symbol=symbol, step_min=main_step_min, years=years, progress_cb=progress_cb)
    else:
        if not bool(auto_sync):
            _append_update_bitmart_csv(symbol=symbol, step_min=main_step_min, years=years)

    if auto_sync:
        BITMART_UPDATE_SERVICE.ensure_worker(symbol, 1, years=years)
        BITMART_UPDATE_SERVICE.ensure_worker(symbol, main_step_min, years=years)

    return str(csv_main), str(csv_1m)

# GPU (Torch) 選配
try:
    import torch
    HAS_TORCH = True
    try:
        import torch_directml  # type: ignore[import-not-found]
        HAS_DML = True
    except Exception:
        HAS_DML = False
    def get_torch_device():
        import torch, warnings
        if torch.cuda.is_available():
            cc = torch.cuda.get_device_capability(0)  # e.g. (12, 0)
            # 有些 build 提供此函式，可檢查內建 sm 清單
            archs = getattr(torch.cuda, "get_arch_list", lambda: [])()
            sm = f"sm_{cc[0]}{cc[1]}"
            if (not archs) or (sm in archs):
                return torch.device("cuda")
            warnings.warn(f"PyTorch build lacks {sm}, falling back to CPU.")
        return torch.device("cpu")

except Exception:
    HAS_TORCH = False
    HAS_DML = False
    def get_torch_device():
        return None

# 可選加速
# ----------------------------- 全域快取（無論 Numba 是否可用都必須存在） ----------------------------- #
# 注意：這兩個 cache 不能只在 NUMBA_OK=False 時才定義，否則會在格點搜尋（Numba 路徑）直接 NameError。
OB_FVG_cache: Dict[Tuple, np.ndarray] = {}
GENERIC_SIG_CACHE: Dict[str, List[np.ndarray]] = {}

# --- Ultra-fast JSON dumps (prefer orjson if available) ---
def _json_default(o):
    # 讓參數 dict 裡就算混到 numpy 型別也不會炸
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return float(o)
    if isinstance(o, (np.ndarray,)):
        return o.tolist()
    if isinstance(o, (np.bool_, bool)):
        return bool(o)
    try:
        import pandas as pd
        if isinstance(o, (pd.Timestamp, pd.Timedelta)):
            return str(o)
    except Exception:
        pass
    return str(o)

try:
    import orjson as _orjson

    def _fast_json_dumps(obj) -> str:
        try:
            return _orjson.dumps(obj, option=_orjson.OPT_SERIALIZE_NUMPY).decode("utf-8")
        except Exception:
            return json.dumps(obj, ensure_ascii=False, default=_json_default)
except Exception:

    def _fast_json_dumps(obj) -> str:
        return json.dumps(obj, ensure_ascii=False, default=_json_default)

# --- RSI cache (avoid recompute in grid search) ---
_RSI_CACHE: Dict[Tuple[int, int, int], np.ndarray] = {}

try:
    from numba import njit
    NUMBA_OK = True
except Exception:
    NUMBA_OK = False

# --- Ultra-fast RSI helpers (global, once) ---
if NUMBA_OK:
    @njit(cache=True, fastmath=True)
    def _rsi_from_updown_nb(up: np.ndarray, dn: np.ndarray, period: int) -> np.ndarray:
        n = up.size
        out = np.empty(n, dtype=np.float64)
        for i in range(n):
            out[i] = np.nan
        if period <= 1 or n == 0:
            return out
        alpha = 1.0 / period
        su = 0.0; sd = 0.0
        end = period if period < n else n
        for i in range(end):
            su += up[i]; sd += dn[i]
        au = su / period; ad = sd / period
        idx = period - 1
        if idx < n:
            if ad == 0.0:
                out[idx] = 100.0
            else:
                rs = au / ad
                out[idx] = 100.0 - 100.0 / (1.0 + rs)
        for i in range(period, n):
            au = (1.0 - alpha) * au + alpha * up[i]
            ad = (1.0 - alpha) * ad + alpha * dn[i]
            if ad == 0.0:
                out[i] = 100.0
            else:
                rs = au / ad
                out[i] = 100.0 - 100.0 / (1.0 + rs)
        return out

    @njit(cache=True, fastmath=True)
    def _calc_laguerre_rsi_nb(src: np.ndarray, gamma: float) -> np.ndarray:
        """John Ehlers Laguerre RSI (Numba Optimized)"""
        n = len(src)
        out = np.full(n, np.nan, dtype=np.float64)
        l0 = 0.0; l1 = 0.0; l2 = 0.0; l3 = 0.0
        # Initialize with first value to avoid huge swing at start
        if n > 0:
            l0, l1, l2, l3 = src[0], src[0], src[0], src[0]

        for i in range(n):
            prev_l0, prev_l1, prev_l2, prev_l3 = l0, l1, l2, l3
            
            l0 = (1.0 - gamma) * src[i] + gamma * prev_l0
            l1 = -gamma * l0 + prev_l0 + gamma * prev_l1
            l2 = -gamma * l1 + prev_l1 + gamma * prev_l2
            l3 = -gamma * l2 + prev_l2 + gamma * prev_l3
            
            cu = 0.0
            cd = 0.0
            if l0 >= l1: cu += (l0 - l1)
            else:        cd += (l1 - l0)
            
            if l1 >= l2: cu += (l1 - l2)
            else:        cd += (l2 - l1)
            
            if l2 >= l3: cu += (l2 - l3)
            else:        cd += (l3 - l2)
            
            if (cu + cd) != 0.0:
                out[i] = cu / (cu + cd)
            else:
                out[i] = 0.0
        return out

    @njit(cache=True, fastmath=True)
    def _rsi_nb(close: np.ndarray, period: int) -> np.ndarray:
        n = close.size
        up = np.empty(n, dtype=np.float64)
        dn = np.empty(n, dtype=np.float64)
        up[0] = 0.0; dn[0] = 0.0
        for i in range(1, n):
            diff = close[i] - close[i-1]
            if diff > 0.0:
                up[i] = diff; dn[i] = 0.0
            else:
                up[i] = 0.0; dn[i] = -diff
        return _rsi_from_updown_nb(up, dn, period)

warnings.filterwarnings("ignore")

# ----------------------------- UI 日誌與 GPU 調優輔助 ----------------------------- #
class UiLogger:
    """將執行過程輸出到面板（含相對時間）。"""
    def __init__(self, enabled: bool):
        self.enabled = enabled
        self._buf: List[str] = []
        self._box = st.empty() if enabled else None
        self._t0 = time.perf_counter()

    def _normalize_msg(self, msg: str) -> str:
        m = str(msg)

        # 統一用語（讓輸出更像一般應用程式日誌）
        m = m.replace("開始：初始化與檢查環境", "初始化：環境與輸入檢查")
        m = m.replace("Numba 路徑：", "Numba：")
        m = m.replace("CPU 路徑：", "CPU：")
        m = m.replace("初始化 Numba 事件引擎", "初始化：事件引擎")
        m = m.replace("1m 批次撮合已啟用：", "1m 撮合：已啟用，")
        m = m.replace("torch.compile / GPU 已停用：改走 Numba 快路徑", "Torch：未啟用（使用 Numba）")
        m = m.replace("Torch 編譯加速已啟用，嘗試使用 GPU 執行", "Torch：編譯已啟用")
        m = m.replace("Torch 編譯失敗，改用 Numba：", "Torch：編譯失敗，改用 Numba：")
        m = m.replace("批次門檻設定：", "批次設定：")

        # 複雜策略：統一描述
        key = " (複雜策略/動態風控模式)，使用 CPU/Numba 逐筆模擬繞過 GPU"
        if key in m:
            m = m.replace(key, "：逐筆模擬（路徑相依），略過 GPU 批次")

        return m

    def __call__(self, msg: str):
        if not self.enabled:
            return
        msg = self._normalize_msg(msg)
        dt = time.perf_counter() - self._t0
        line = f"[+{dt:7.3f}s] {msg}"
        self._buf.append(line)
        # 用 code 區塊固定寬字型，易讀
        self._box.code("\n".join(self._buf), language="text")

def setup_gpu_runtime_for_speed(device):
    """盡可能壓榨 NVIDIA CUDA 的推算效率。"""
    info = {}
    try:
        if HAS_TORCH and isinstance(device, torch.device) and device.type == "cuda":
            try:
                torch.backends.cuda.matmul.allow_tf32 = True  # TF32 對 CC>=8 有效
                info["tf32"] = True
            except Exception:
                info["tf32"] = False
            try:
                torch.set_float32_matmul_precision("high")
                info["precision"] = "high"
            except Exception:
                info["precision"] = "default"
    except Exception:
        pass
    return info

# ----------------------------- 公用工具 ----------------------------- #

def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)


def infer_bar_seconds(ts_series: pd.Series) -> float:
    """推斷K線時間間隔（秒），用中位數避免離群值。"""
    if len(ts_series) < 3:
        return 60.0
    t = pd.to_datetime(ts_series, utc=True, errors="coerce")
    dt = t.diff().dt.total_seconds().dropna()
    if len(dt) == 0:
        return 60.0
    return float(np.median(dt.values))


def bars_per_year(bar_seconds: float) -> float:
    seconds_per_year = 365.25 * 24 * 3600
    if bar_seconds <= 0:
        return 365.25
    return seconds_per_year / bar_seconds

# ----------------------------- Intrabar 1m 擬合（可選） ----------------------------- #
# 設計原則：
# - 訊號仍以「主資料時間級別」計算（例如 30m/15m/1h）。
# - 但「進場價 / 出場價 / TP/SL 同根觸發先後」改用 1m CSV 做逐分鐘撮合，讓成交價更貼近真實市場。

INTRABAR_1M_CTX: Optional[Dict[str, object]] = None


def build_intrabar_1m_context(df_main: pd.DataFrame, df_1m: pd.DataFrame) -> Dict[str, object]:
    """建立主K -> 1m 的索引對應表（intrabar 撮合用）。

    要求：
    - df_main["ts"] 與 df_1m["ts"] 都必須是「該K線開盤時間」(UTC) 並已排序
    - df_1m 必須是 1 分鐘級別（以 infer_bar_seconds 檢查）
    - df_1m 必須覆蓋 df_main 全期間，且每一根主K的開盤分鐘必須存在於 df_1m
      （否則進場價會變成 "下一個存在的分鐘"，這不叫精準撮合）
    """
    if df_main is None or df_1m is None or len(df_main) == 0 or len(df_1m) == 0:
        raise ValueError("主資料或 1m 資料為空，無法建立 intrabar 對應。")

    sec_1m = infer_bar_seconds(df_1m["ts"])
    if abs(sec_1m - 60.0) > 1.0:
        raise ValueError(f"1m CSV 時間級別異常：推斷間隔為 {sec_1m:.2f} 秒（應接近 60 秒）。")

    main_ts = pd.to_datetime(df_main["ts"], utc=True, errors="coerce").values.astype("datetime64[ns]")
    lt_ts = pd.to_datetime(df_1m["ts"], utc=True, errors="coerce").values.astype("datetime64[ns]")

    if np.isnat(main_ts).any():
        raise ValueError("主資料 ts 欄位含有無法解析的時間。")

    if np.isnat(lt_ts).any():
        raise ValueError("1m 資料 ts 欄位含有無法解析的時間。")

    lt_o = df_1m["open"].values.astype(np.float64)
    lt_h = df_1m["high"].values.astype(np.float64)
    lt_l = df_1m["low"].values.astype(np.float64)
    lt_c = df_1m["close"].values.astype(np.float64)

    # 確保排序（避免使用者提供亂序 CSV）
    lt_ns = lt_ts.astype(np.int64)
    if lt_ns.size >= 2 and (np.diff(lt_ns) < 0).any():
        order = np.argsort(lt_ns)
        lt_ts = lt_ts[order]
        lt_ns = lt_ns[order]
        lt_o = lt_o[order]
        lt_h = lt_h[order]
        lt_l = lt_l[order]
        lt_c = lt_c[order]

    main_ns = main_ts.astype(np.int64)

    # 主K 的結束時間：用下一根主K的開盤時間；最後一根用推斷的 bar_sec 補上
    bar_sec = infer_bar_seconds(df_main["ts"])
    bar_ns = int(round(bar_sec * 1e9))
    main_end_ns = np.empty_like(main_ns)
    main_end_ns[:-1] = main_ns[1:]
    main_end_ns[-1] = main_ns[-1] + bar_ns

    start_idx = np.searchsorted(lt_ns, main_ns, side="left")
    end_idx = np.searchsorted(lt_ns, main_end_ns, side="left")

    # 覆蓋檢查：每根主K 至少要有 1 分鐘資料
    bad = np.where((start_idx >= lt_ns.size) | (end_idx <= start_idx))[0]
    if bad.size > 0:
        i0 = int(bad[0])
        raise ValueError(
            f"1m 資料覆蓋不足：共有 {int(bad.size)} 根主K找不到對應 1m 區間。"
            f"第一個缺失主K index={i0}, ts={str(pd.to_datetime(main_ts[i0], utc=True))}"
        )

    # 對齊檢查：主K 的開盤分鐘必須精準存在於 1m
    mis = np.where(lt_ns[start_idx] != main_ns)[0]
    if mis.size > 0:
        i0 = int(mis[0])
        raise ValueError(
            "1m 資料與主資料時間軸未對齊：主K開盤分鐘在 1m CSV 裡找不到完全一致的 ts。"
            f"第一個不對齊主K index={i0}, main_ts={str(pd.to_datetime(main_ts[i0], utc=True))}"
        )

    # 直接從 1m 聚合出主K的 OHLC（intrabar 撮合以 1m 為準）
    bar_o_1m = lt_o[start_idx]
    bar_c_1m = lt_c[end_idx - 1]

    bar_h_1m = np.empty_like(bar_o_1m)
    bar_l_1m = np.empty_like(bar_o_1m)
    for i in range(len(start_idx)):
        s = int(start_idx[i])
        e = int(end_idx[i])
        bar_h_1m[i] = float(np.max(lt_h[s:e]))
        bar_l_1m[i] = float(np.min(lt_l[s:e]))

    return {
        "lt_ts": lt_ts,
        "lt_o": lt_o,
        "lt_h": lt_h,
        "lt_l": lt_l,
        "lt_c": lt_c,
        "bar_ltf_start": start_idx.astype(np.int64),
        "bar_ltf_end": end_idx.astype(np.int64),
        "bar_o_1m": bar_o_1m,
        "bar_h_1m": bar_h_1m,
        "bar_l_1m": bar_l_1m,
        "bar_c_1m": bar_c_1m,
    }


def _intrabar_ctx():
    """給模擬核心取用的 1m context。"""
    return INTRABAR_1M_CTX

def rolling_max_drawdown(equity: np.ndarray) -> Tuple[float, int, int]:
    """回傳最大回撤(百分比)、高點索引、低點索引。"""
    peak = equity[0]
    max_dd = 0.0
    peak_idx = 0
    trough_idx = 0
    temp_peak_idx = 0
    for i in range(1, len(equity)):
        if equity[i] > peak:
            peak = equity[i]
            temp_peak_idx = i
        drawdown = (equity[i] / peak) - 1.0
        if drawdown < max_dd:
            max_dd = drawdown
            peak_idx = temp_peak_idx
            trough_idx = i
    return float(abs(max_dd) * 100.0), int(peak_idx), int(trough_idx)


def sharpe_ratio(returns: np.ndarray, bpy: float, rf_annual: float = 0.0) -> float:
    """年化 Sharpe。returns 為每筆交易的複利報酬不可直接換算，這裡需用等頻收益。
       我們用逐K淨值曲線推導的每K報酬計算 Sharpe。"""
    if returns.size < 3:
        return 0.0
    mu = np.nanmean(returns)
    sd = np.nanstd(returns, ddof=1)
    if sd == 0 or np.isnan(sd):
        return 0.0
    # 將 rf_annual 換算到每K
    rf_per_bar = (1.0 + rf_annual) ** (1.0 / bpy) - 1.0
    excess = mu - rf_per_bar
    return float((excess / sd) * math.sqrt(bpy))


def sortino_ratio(returns: np.ndarray, bpy: float, rf_annual: float = 0.0) -> float:
    if returns.size < 3:
        return 0.0
    downside = returns[returns < 0]
    if downside.size == 0:
        return 0.0
    mu = np.nanmean(returns)
    rf_per_bar = (1.0 + rf_annual) ** (1.0 / bpy) - 1.0
    dd = np.nanstd(downside, ddof=1)
    if dd == 0 or np.isnan(dd):
        return 0.0
    return float((mu - rf_per_bar) / dd * math.sqrt(bpy))


def calmar_ratio(cagr: float, max_dd_pct: float) -> float:
    if max_dd_pct <= 0:
        return 0.0
    return float(cagr / (max_dd_pct / 100.0))


def annualized_return_from_equity(equity: np.ndarray, bpy: float) -> float:
    """從每K淨值計算年化報酬（CAGR）。"""
    if equity.size < 2:
        return 0.0
    total_return = equity[-1] / equity[0] - 1.0
    years = equity.size / bpy
    if years <= 0:
        return 0.0
    return float((1.0 + total_return) ** (1.0 / years) - 1.0)


def pct(x: float) -> float:
    return round(float(x) * 100.0, 4)


# ----------------------------- 技術指標庫（30+） ----------------------------- #
# 所有指標皆回傳 numpy.ndarray，遇到無法計算的部位填 np.nan

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
    # Welford 不錯，但這裡用簡化法：E[x^2]-E[x]^2
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
    if period <= 0 or period > len(close):
        return out
    weights = np.arange(1, period+1)
    wsum = weights.sum()
    for i in range(period-1, len(close)):
        window = close[i-period+1:i+1]
        if np.any(np.isnan(window)):
            out[i] = np.nan
        else:
            out[i] = np.dot(window, weights) / wsum
    return out

def HMA(close: np.ndarray, period: int) -> np.ndarray:
    # Hull MA
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
    if period <= 0:
        return out
    out[period:] = (close[period:] / close[:-period]) - 1.0
    return out

def RSI(close: np.ndarray, period: int) -> np.ndarray:
    """優先走 Numba 超速路徑；不可用時退回舊實作。

    重要：格點掃描會高頻呼叫 RSI。這裡做「零拷貝 + 快取」：
    - 避免每次都 .astype(float64) 造成 O(N) 複製
    - 同一個 close buffer、同一個 period 只計算一次
    """
    close = np.asarray(close)
    p = int(period)

    if NUMBA_OK:
        try:
            # Key: (data_ptr, length, period) — 同一次執行中 close buffer 不變就能命中
            ptr = int(close.__array_interface__["data"][0])
            key = (ptr, int(close.size), p)
            cached = _RSI_CACHE.get(key, None)
            if cached is not None:
                return cached

            out = _rsi_wilder_nb(close, p)

            # 控制快取上限，避免 Streamlit 長時間跑把 RAM 撐爆
            if len(_RSI_CACHE) > 256:
                _RSI_CACHE.clear()
            _RSI_CACHE[key] = out
            return out
        except Exception:
            # 任何狀況都退回安全版
            pass

    # ----- fallback: 原生 Wilder（慢，但正確） -----
    close = close.astype(np.float64, copy=False)
    diff = np.diff(close, prepend=np.nan)
    up = np.where(diff > 0, diff, 0.0)
    dn = np.where(diff < 0, -diff, 0.0)

    def wilder(arr):
        out = np.full_like(arr, np.nan, dtype=np.float64)
        alpha = 1.0 / p
        s = np.nan
        for i, v in enumerate(arr):
            if np.isnan(v):
                continue
            if np.isnan(s):
                s = v
            else:
                s = (1 - alpha) * s + alpha * v
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
    # Wilder's DMI/ADX
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
    # Elder Force Index
    fi = (close - np.roll(close, 1)); fi[0] = np.nan
    raw = fi * volume
    return _np_ema(raw, period)

def KAMA(close: np.ndarray, period: int, fast: int = 2, slow: int = 30) -> np.ndarray:
    # 近似 Kaufman AMA
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


# 指標名錄（>30）
INDICATOR_FAMILIES = [
    "RSI",
    "SMA_Cross",
    "EMA_Cross",
    "HMA_Cross",
    "MACD_Cross",
    "PPO_Cross",
    "Bollinger_Touch",
    "Stoch_Oversold",
    "CCI_Oversold",
    "WillR_Oversold",
    "MFI_Oversold",
    "Donchian_Breakout",
    "ADX_DI_Cross",
    "Aroon_Cross",
    "ROC_Threshold",
    "KAMA_Cross",
    "TRIX_Cross",
    "DPO_Revert",
    "CMF_Threshold",
    "OBV_Slope",
    "EFI_Threshold",
    "ATR_Band_Break",
    "Vortex_Cross",
    "PVO_Cross",
    "DEMA_Cross",
    "TEMA_Cross",
    "WMA_Cross",
    "BB_PercentB_Revert",
    "ADL_Slope",
    "Aroon_Osc_Threshold",
    "Volatility_Squeeze",
    "OB_FVG",
    "SMC",  # Smart Money Concepts
    "LaguerreRSI_TEMA",  # Pine Script: SQ Strategy 0.60842
    "TEMA_RSI"  # Pine Script: 帥 (TEMA + RSI + Trailing Stop)
]


# ----------------------------- 訊號模板 ----------------------------- #

def signal_from_family(family: str,
                       o: np.ndarray, h: np.ndarray, l: np.ndarray, c: np.ndarray, v: np.ndarray,
                       params: Dict) -> np.ndarray:
    """回傳布林陣列：True 表示當根形成「入場訊號」，實際入場會在下一根開盤。"""
    N = len(c)
    sig = np.zeros(N, dtype=bool)

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
        cross = (ma_f > ma_s) & (np.roll(ma_f, 1) <= np.roll(ma_s, 1))
        sig = cross

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
        sig = dpo < 0  # 均值回歸式入場

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
        # 參數映射
        param_N = int(params.get("N", 3))
        param_r = float(params.get("r", 0.001))
        param_h = int(params.get("h", 20))
        param_g = float(params.get("g", 1.0))
        param_a = float(params.get("a", 0.99))
        param_rise_thr = float(params.get("rise_thr", 1.002))
        param_x = float(params.get("x", 1.0))
        param_y = float(params.get("y", 1.0))
        param_monitor_window = int(params.get("monitor_window", 20))
        # RSI 動能濾網參數 (rsi_diff 即為 j%，若 j=10 代表需比OB高10%)
        param_rsi_period = int(params.get("rsi_period", 14))
        param_rsi_diff = float(params.get("rsi_diff", 0.0))

        # 呼叫 Numba 實作以保持一致性
        if NUMBA_OK:
            return _signal_from_ob_fvg_nb(o, h, l, c, v, 
                                          param_N, param_r, param_h, param_g, param_a, 
                                          param_rise_thr, param_x, param_y, param_monitor_window,
                                          param_rsi_period, param_rsi_diff)
        else:
            # 純 Python 實作 (備用)
            Bars = len(c)
            sig = np.zeros(Bars, dtype=bool)
            # Python 版計算 RSI
            rsi_arr = RSI(c, param_rsi_period)
            
            ob_top_arr = np.full(Bars, np.nan, dtype=np.float32)
            ob_bottom_arr = np.full(Bars, np.nan, dtype=np.float32)
            fvg_top_arr = np.full(Bars, np.nan, dtype=np.float32)
            fvg_bottom_arr = np.full(Bars, np.nan, dtype=np.float32)
            ob_idx_arr = np.full(Bars, -1, dtype=np.int32)
            highest_arr = np.full(Bars, np.nan, dtype=np.float32)
            lowest_arr = np.full(Bars, np.nan, dtype=np.float32)
            
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
                    ob_idx = i-1
                    
                    trend_end_idx = i + param_N - 1
                    monitor_start = trend_end_idx + 1
                    monitor_end = min(Bars, monitor_start + param_monitor_window)
                    
                    state = 0
                    thresh = ob_high * param_rise_thr
                    dip = ob_low * param_a
                    
                    for k in range(monitor_start, monitor_end):
                        if state == 0 and h[k] >= thresh: state = 1
                        if state == 1 and l[k] <= dip: state = 2
                        # 增加 RSI 判斷: 當下 RSI > OB第一根RSI * (1 + j%)
                        # ob_idx 為 i-1
                        ob_rsi_val = rsi_arr[i-1]
                        if state == 2 and c[k] > ob_high and rsi_arr[k] > ob_rsi_val * (1.0 + param_rsi_diff):
                            if not sig[k]:
                                sig[k] = True
                                ob_top_arr[k] = ob_high
                                ob_bottom_arr[k] = ob_low
                                ob_idx_arr[k] = ob_idx
                                highest_arr[k] = ob_high
                                lowest_arr[k] = ob_low
                                fvg_top_arr[k] = c[i] * param_y
                                fvg_bottom_arr[k] = o[i] * param_x
                            break
            return sig, (ob_top_arr, ob_bottom_arr, fvg_top_arr, fvg_bottom_arr, ob_idx_arr, highest_arr, lowest_arr)

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
        # 布林帶寬度低於分位數，當作壓縮；突破時入場
        p = int(params.get("period", 20)); nstd = float(params.get("nstd", 2.0)); q = float(params.get("quantile", 0.2))
        mid, up, low, _ = BBANDS(c, p, nstd)
        bw = (up - low) / mid
        thresh = np.nanquantile(bw, q)
        squeeze = bw <= thresh
        sig = squeeze & (c > up)
    elif family == "RSI_ATR":
        # RSI + ATR 結合策略
        # params:
        #   rsi_p1: 進場 RSI 週期
        #   rsi_enter: RSI 進場閾值（低於此值買入）
        #   rsi_p2: 出場 RSI 週期
        #   rsi_exit: RSI 出場閾值（高於此值賣出）
        #   atr_p: ATR 週期
        #   atr_thr: ATR 閾值
        #   atr_dir: "above" 表示波動大於閾值才交易，"below" 表示波動低於閾值才交易

        rsi_p1 = int(params.get("rsi_p1", 14))
        rsi_enter = float(params.get("rsi_enter", 30))
        rsi_p2 = int(params.get("rsi_p2", 14))
        rsi_exit = float(params.get("rsi_exit", 70))
        atr_p = int(params.get("atr_p", 14))
        atr_thr = float(params.get("atr_thr", 0.0))
        atr_dir = str(params.get("atr_dir", "above")).lower()

        # 計算 RSI 與 ATR
        rsi1 = RSI(c, rsi_p1)
        rsi2 = RSI(c, rsi_p2)
        atr = ATR(h, l, c, atr_p)

        # ATR 濾波條件
        if atr_dir == "below":
            atr_filter = atr < atr_thr
        else:
            atr_filter = atr > atr_thr

        # 訊號產生
        long_entry = (rsi1 < rsi_enter) & atr_filter
        long_exit = (rsi2 > rsi_exit) & atr_filter

        # 統一成 sig
        sig = np.where(long_entry, 1, np.where(long_exit, -1, 0))

    elif family == "SMC":
        p_len = int(params.get("length", 14))
        p_limit = int(params.get("ob_limit", 300))
        p_rev = bool(params.get("reverse", False))
        
        if NUMBA_OK:
            return _signal_from_smc_nb(o, h, l, c, v, p_len, p_limit, p_rev)
        else:
            return np.zeros(len(c), dtype=bool), (np.full(len(c), np.nan), np.full(len(c), np.nan), np.full(len(c), np.nan), np.full(len(c), np.nan), np.full(len(c), -1), np.full(len(c), np.nan), np.full(len(c), np.nan))

    elif family == "LaguerreRSI_TEMA":
        # --- Laguerre RSI + TEMA + MTF Weekly EMA ---
        p_tema_len = int(params.get("tema_len", 30))
        p_gamma = float(params.get("gamma", 0.5))
        p_ema1_w = int(params.get("ema1_w", 9))
        p_ema2_w = int(params.get("ema2_w", 20))
        p_ema3_w = int(params.get("ema3_w", 40))
        p_low_lookback = int(params.get("low_lookback", 10))
        
        # ATR Periods
        p_atr_sltp_len = int(params.get("atr_sltp_len", 15))
        p_atr_trail_len = int(params.get("atr_trail_len", 18))
        p_atr_act_len = int(params.get("atr_act_len", 20))

        # Time Series for MTF
        ts_arr = params.get("_ts", None)
        if ts_arr is None:
            # Fallback prevention: create dummy index
            ts_arr = pd.date_range(start="2000-01-01", periods=len(c), freq="30min")

        # 1. Indicators Calculation
        # A. TEMA on HL2
        hl2 = (h + l) / 2.0
        # Calculate standard EMAs for TEMA: 3*E1 - 3*E2 + E3
        e1 = EMA(hl2, p_tema_len)
        e2 = EMA(e1, p_tema_len)
        e3 = EMA(e2, p_tema_len)
        tema_val = 3 * e1 - 3 * e2 + e3

        # B. Laguerre RSI
        if NUMBA_OK:
            lag_rsi = _calc_laguerre_rsi_nb(c, p_gamma)
        else:
            lag_rsi = np.zeros_like(c) # Should imply numba required

        # C. Weekly EMAs (Strict MTF Logic: Request Security "1W" [1])
        # Resample to Weekly (Ending FRI/SUN), calc EMA, Shift 1, Broadcast back
        idx_ts = pd.DatetimeIndex(ts_arr)
        ser_c = pd.Series(c, index=idx_ts)
        # Resample logic: 'W-MON' matches weekly logic
        # request.security(..., lookahead=barmerge.lookahead_on)
        w_close = ser_c.resample('W-MON').last()
        
        # Calculate Weekly EMAs
        def _pd_ema(ser, span): return ser.ewm(span=span, adjust=False).mean()
        # Pine request.security(..., [1]) with lookahead=on fetches the previous closed bar value.
        # Pandas resample('W-MON', label='right') puts W1 close value at 'Mon 00:00'.
        # Reindexing to 10m 'Mon 00:00' (Start of W2) picks up this W1 value immediately.
        # Thus, NO .shift(1) is needed. Shift(1) would push it to W3 (Pine [2]).
        we1 = _pd_ema(w_close, p_ema1_w)
        we2 = _pd_ema(w_close, p_ema2_w)
        we3 = _pd_ema(w_close, p_ema3_w)
        
        # Broadcast (reindex & ffill)
        # Note: reindex introduces NaNs at start where weekly data isn't ready
        w_ema1 = we1.reindex(idx_ts).ffill().values
        w_ema2 = we2.reindex(idx_ts).ffill().values
        w_ema3 = we3.reindex(idx_ts).ffill().values
        
        # Fill NaNs with 0 to allow logic comparison (nz function)
        w_ema1 = np.nan_to_num(w_ema1)
        w_ema2 = np.nan_to_num(w_ema2)
        w_ema3 = np.nan_to_num(w_ema3)

        # D. ATRs for Risk Management
        atr_sltp = ATR(h, l, c, p_atr_sltp_len)
        atr_trail_arr = ATR(h, l, c, p_atr_trail_len)
        atr_act_arr = ATR(h, l, c, p_atr_act_len)

        # E. Lowest Open Structure (Pine: ta.lowest(open, 10))
        # rolling(10).min() at index i includes i. Matches Pine.
        lowest_open = pd.Series(o).rolling(p_low_lookback).min().values

        # 2. Logic Evaluation
        # A. Entry: LagRSI Rising & Weekly EMA Stack
        # Pine: lag_rsi > lag_rsi[1]
        logic_rsi_up = (lag_rsi > np.roll(lag_rsi, 1))
        # Pine: w_ema1 > w_ema2 and w_ema2 > w_ema3
        logic_ma_stack = (w_ema1 > w_ema2) & (w_ema2 > w_ema3)
        
        long_entry = logic_rsi_up & logic_ma_stack & np.isfinite(tema_val) & (w_ema1 > 0)

        # B. Exit Logic
        # Pine: (tema[1] < close[1]) and (tema > close)
        # This implies TEMA crossed OVER Price (or Price crossed UNDER TEMA)
        tema_prev = np.roll(tema_val, 1)
        close_prev = np.roll(c, 1)
        logic_cross_under = (tema_prev < close_prev) & (tema_val > c)
        
        # Pine: lowest_open[2] < open[2]
        # Check if the lowest open of the window ending at [2] was strictly lower than open[2]
        # This implies open[2] was NOT the lowest point (structure break?)
        lo_prev2 = np.roll(lowest_open, 2)
        open_prev2 = np.roll(o, 2)
        logic_struct_break = lo_prev2 < open_prev2
        
        logic_exit_sig = logic_cross_under & logic_struct_break
        logic_exit_arr = logic_exit_sig.astype(np.float64)

        # Pack for Simulator
        # (ATR_SL, ATR_SL, ATR_Trail, ATR_Act, Logic_Exit, ...)
        # Note: First two are for SL and TP calc, they use same ATR length in this strategy (15)
        return long_entry, (atr_sltp, atr_sltp, atr_trail_arr, atr_act_arr, logic_exit_arr, np.full_like(c, np.nan), np.full_like(c, np.nan))

    elif family == "TEMA_RSI":
        # --- Pine Script: 帥 (TEMA + RSI + Trailing Stop) ---
        # 嚴格對照 Pine Script 邏輯移植
        p_fast_len = int(params.get("fast_len", 3))
        p_slow_len = int(params.get("slow_len", 100))
        p_rsi_len = int(params.get("rsi_len", 14))
        p_rsi_thr = int(params.get("rsi_thr", 20))
        
        # Risk Params (Pine: activationPercent, trailOffsetTicks, profitTargetPct, stopLossPct)
        p_activation_pct = float(params.get("activation_pct", 1.0)) / 100.0
        p_trail_ticks = int(params.get("trail_ticks", 800))
        p_mintick = float(params.get("mintick", 0.01)) # 預設 0.01 (需依幣種調整)
        p_tp_pct = float(params.get("tp_pct_strat", 2.2)) / 100.0
        p_sl_pct = float(params.get("sl_pct_strat", 6.0)) / 100.0
        # Pine: stakePercentage = input.float(95, ...)
        p_stake_pct = float(params.get("stake_pct", 95.0)) / 100.0

        # 1. Indicators
        def calc_tema(src, length):
            e1 = EMA(src, length)
            e2 = EMA(e1, length)
            e3 = EMA(e2, length)
            return 3 * e1 - 3 * e2 + e3
        
        fast_ema = calc_tema(c, p_fast_len)
        slow_ema = calc_tema(c, p_slow_len)
        rsi_val = RSI(c, p_rsi_len)

        # 2. Conditions (Vectorized)
        # Helper for rising/falling matching Pine's ta.rising(x, length)
        # Pine: rising(x, 3) checks if x is rising for 3 bars: x[0]>x[1], x[1]>x[2], x[2]>x[3]
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

        # c1: fastEma < slowEma
        c1 = fast_ema < slow_ema

        # c2: rising(fast, 3) and falling(slow, 3)
        c2 = is_rising_vec(fast_ema, 3) & is_falling_vec(slow_ema, 3)
        
        # c4: rising(fast, 4) and rising(slow, 3)
        c4 = is_rising_vec(fast_ema, 4) & is_rising_vec(slow_ema, 3)
        
        # c5: crossover(fast, slow)
        c5 = (fast_ema > slow_ema) & (np.roll(fast_ema, 1) <= np.roll(slow_ema, 1))
        
        # c6: rsi > threshold
        c6 = rsi_val > p_rsi_thr
        
        # rsi_cross: crossover(rsi, 30) (Implicit c7)
        rsi_cross = (rsi_val > 30) & (np.roll(rsi_val, 1) <= 30)

        # Entry Signal & Reason Classification
        # Reason 1: Pullback (c1 & c2 & c6)
        # Reason 2: Momentum (c4 & c6)
        # Reason 3: Cross (c5 & c6)
        # Reason 4: RSI Revert (rsi_cross)
        
        cond1 = (c1 & c2 & c6)
        cond2 = (c4 & c6)
        cond3 = (c5 & c6)
        cond4 = rsi_cross
        
        sig = cond1 | cond2 | cond3 | cond4
        
        # Build Entry Reason Array (Priority: 1 > 2 > 3 > 4)
        # 0: None, 1: Pullback, 2: Momentum, 3: Cross, 4: RSI_Revert
        N = len(c)
        reason_arr = np.zeros(N, dtype=np.int32)
        # Use np.where logic or mask assignment. Since conditions can overlap, priority matters.
        # Assign in reverse priority so higher priority overwrites, or just simple separate assignments
        reason_arr[cond4] = 4
        reason_arr[cond3] = 3
        reason_arr[cond2] = 2
        reason_arr[cond1] = 1 # Highest priority in logic description usually goes first or last depending on preference. Here: Pullback is dominant.
        
        # Pack parameters for the specialized simulator (_simulate_tema_rsi_nb)
        p_act_arr = np.full(N, p_activation_pct)
        # Convert Ticks to Price Distance: Ticks * MinTick
        p_off_arr = np.full(N, float(p_trail_ticks * p_mintick))
        p_tp_arr = np.full(N, p_tp_pct)
        p_sl_arr = np.full(N, p_sl_pct)
        p_stake_arr = np.full(N, p_stake_pct)
        
        # Tuple 結構: (Act, Off, TP, SL, Stake, ReasonArr, Dummy)
        # We pass reason_arr in the 6th slot (replacing first dummy)
        return sig, (p_act_arr, p_off_arr, p_tp_arr, p_sl_arr, p_stake_arr, reason_arr.astype(np.float64), np.zeros(N))
    else:
        raise ValueError(f"未知指標家族: {family}")

    # 避免第一根、NaN 誤觸
    sig = sig & np.isfinite(c)
    sig[:2] = False
    return sig

# ======== FAST CORE: Numba-accelerated RSI grid + batched event-driven simulator ========
try:
    from numba import njit, prange
except Exception:
    pass  # 上面已設 NUMBA_OK

if NUMBA_OK:
    @njit(cache=True, fastmath=True)
    def _rsi_wilder_nb(close, period):
        n = close.size
        out = np.empty(n, np.float64)
        out[:] = np.nan
        ag = 0.0  # avg gain
        al = 0.0  # avg loss
        for i in range(1, n):
            diff = close[i] - close[i-1]
            g = diff if diff > 0 else 0.0
            d = -diff if diff < 0 else 0.0
            if i <= period:
                ag += g
                al += d
                if i == period:
                    ag /= period
                    al /= period
                    if al == 0.0:
                        out[i] = 100.0
                    else:
                        rs = ag / al
                        out[i] = 100.0 - (100.0 / (1.0 + rs))
            else:
                ag = (ag * (period - 1) + g) / period
                al = (al * (period - 1) + d) / period
                if al == 0.0:
                    out[i] = 100.0
                else:
                    rs = ag / al
                    out[i] = 100.0 - (100.0 / (1.0 + rs))
        return out

    @njit(cache=True, fastmath=True, parallel=True)
    def _rsi_grid_nb(close, periods):
        P = periods.shape[0]
        n = close.shape[0]
        out = np.empty((P, n), np.float64)
        for pi in prange(P):
            out[pi, :] = _rsi_wilder_nb(close, int(periods[pi]))
        return out

    @njit(cache=True, fastmath=True, parallel=True)
    def _metrics_from_sigs_batch_nb(o, h, l, c, sigs, tp_vec, sl_vec,
                                        max_hold, fee_side, slippage, worst_case_i8,
                                        T, bpy):

        # sigs: [B, T] bool
        B = sigs.shape[0]

        total_return_pct    = np.zeros(B, np.float64)
        cagr_pct            = np.zeros(B, np.float64)
        mdd_pct             = np.zeros(B, np.float64)
        sharpe              = np.zeros(B, np.float64)
        sortino             = np.zeros(B, np.float64)
        calmar              = np.zeros(B, np.float64)
        trades_arr          = np.zeros(B, np.int32)
        entries_arr         = np.zeros(B, np.int32)
        win_rate_pct        = np.zeros(B, np.float64)
        avg_win_pct         = np.zeros(B, np.float64)
        avg_loss_pct        = np.zeros(B, np.float64)
        payoff              = np.zeros(B, np.float64)
        profit_factor       = np.zeros(B, np.float64)
        expectancy_pct      = np.zeros(B, np.float64)
        avg_hold_bars       = np.zeros(B, np.float64)
        time_in_market_pct  = np.zeros(B, np.float64)
        equity_final        = np.ones(B, np.float64)

        # rf_annual=0 → rf_per_bar=0
        for b in prange(B):
            tp = tp_vec[b]
            sl = sl_vec[b]

            i = 0
            trades = 0
            entries = 0
            time_in_mkt = 0.0

            wins_cnt = 0
            wins_sum = 0.0
            losses_sum_abs = 0.0

            sum_r = 0.0       # Σ per-trade return
            sum_r2 = 0.0      # Σ r^2
            sum_r_dn = 0.0    # Σ r for r<0（Sortino 用）
            sum_r_dn2 = 0.0   # Σ r^2 for r<0
            dn_cnt = 0        # r<0 的筆數（Sortino / avg_loss 用）

            equity = 1.0
            peak = 1.0
            mdd = 0.0  # fraction

            while i < T - 1:
                if sigs[b, i]:
                    entries += 1
                    entry_idx = i + 1
                    if entry_idx >= T:
                        break
                    entry_px = o[entry_idx]
                    end = entry_idx + max_hold
                    if end >= T:
                        end = T - 1

                    tp_level = entry_px * (1.0 + tp)
                    sl_level = entry_px * (1.0 - sl)

                    exit_idx = end
                    exit_px = c[end]

                    # 掃最長持倉區間（事件驅動，通常很短）
                    for j in range(entry_idx, end + 1):
                        hit_tp = (h[j] >= tp_level)
                        hit_sl = (l[j] <= sl_level)
                        if hit_tp and hit_sl:
                            exit_idx = j
                            exit_px = sl_level if (worst_case_i8 == 1) else tp_level
                            break
                        elif hit_sl:
                            exit_idx = j
                            exit_px = sl_level
                            break
                        elif hit_tp:
                            exit_idx = j
                            exit_px = tp_level
                            break

                    exec_entry = entry_px + slippage
                    exec_exit = exit_px - slippage
                    if exec_entry <= 0.0:
                        exec_entry = 1e-12
                    if exec_exit <= 0.0:
                        exec_exit = 1e-12
                    net_ret = ((exec_exit * (1.0 - fee_side)) / (exec_entry * (1.0 + fee_side))) - 1.0

                    trades += 1
                    time_in_mkt += (exit_idx - entry_idx + 1)

                    sum_r += net_ret
                    sum_r2 += net_ret * net_ret

                    if net_ret > 0.0:
                        wins_cnt += 1
                        wins_sum += net_ret
                    elif net_ret < 0.0:
                        losses_sum_abs += -net_ret
                        sum_r_dn += net_ret
                        sum_r_dn2 += net_ret * net_ret
                        dn_cnt += 1
                    else:
                        # net_ret == 0.0 → 不算 win / loss / downside
                        pass

                    equity *= (1.0 + net_ret)
                    if equity > peak:
                        peak = equity
                    dd = (equity / peak) - 1.0
                    if dd < -mdd:
                        mdd = -dd

                    i = exit_idx + 1
                else:
                    i += 1

            equity_final[b] = equity
            total_return = equity - 1.0
            total_return_pct[b] = total_return * 100.0

            years = T / bpy
            cagr = (equity ** (1.0 / years) - 1.0) if years > 0 else 0.0
            cagr_pct[b] = cagr * 100.0

            # --- Sharpe / Sortino：完全對齊 Python 版（逐K perbar 報酬、ddof=1） ---
            mu = sum_r / T  # per-bar mean（非交易 mean）
            if T > 1:
                var = (sum_r2 - T * mu * mu) / (T - 1.0)  # sample variance (ddof=1)
            else:
                var = 0.0
            sd = math.sqrt(var) if var > 0.0 else 0.0
            sharpe[b] = (mu / sd) * math.sqrt(bpy) if sd > 0.0 else 0.0

            sd_dn = 0.0
            if dn_cnt > 1:
                mu_dn = sum_r_dn / dn_cnt
                var_dn = (sum_r_dn2 - dn_cnt * mu_dn * mu_dn) / (dn_cnt - 1.0)  # ddof=1 on downside
                if var_dn > 0.0:
                    sd_dn = math.sqrt(var_dn)
            sortino[b] = (mu / sd_dn) * math.sqrt(bpy) if sd_dn > 0.0 else 0.0

            mdd_pct[b] = mdd * 100.0
            calmar[b] = (cagr / mdd) if mdd > 0.0 else 0.0

            trades_arr[b] = trades
            entries_arr[b] = entries

            win_rate_pct[b] = (wins_cnt / trades * 100.0) if trades > 0 else 0.0
            avg_win = (wins_sum / wins_cnt) if wins_cnt > 0 else 0.0
            avg_loss = (-losses_sum_abs / dn_cnt) if dn_cnt > 0 else 0.0  # 只算 net_ret<0
            avg_win_pct[b]  = avg_win * 100.0
            avg_loss_pct[b] = avg_loss * 100.0
            payoff[b] = (avg_win / abs(avg_loss)) if avg_loss != 0.0 else 0.0
            profit_factor[b] = (wins_sum / losses_sum_abs) if losses_sum_abs > 0.0 else 0.0
            expectancy_pct[b] = ((sum_r / trades) * 100.0) if trades > 0 else 0.0
            avg_hold_bars[b] = (time_in_mkt / trades) if trades > 0 else 0.0
            time_in_market_pct[b] = (time_in_mkt / T) * 100.0

        return (
            total_return_pct, cagr_pct, mdd_pct, sharpe, sortino, calmar,
            trades_arr, entries_arr, win_rate_pct, avg_win_pct, avg_loss_pct,
            payoff, profit_factor, expectancy_pct, avg_hold_bars, time_in_market_pct,
            equity_final
        )

    @njit(cache=True, fastmath=True, parallel=True)
    def _metrics_from_sigs_batch_short_nb(o, h, l, c, sigs, tp_vec, sl_vec,
                                          max_hold, fee_side, slippage, worst_case_i8,
                                          T, bpy):

        # sigs: [B, T] bool (True 表示做空訊號)
        B = sigs.shape[0]

        total_return_pct    = np.zeros(B, np.float64)
        cagr_pct            = np.zeros(B, np.float64)
        mdd_pct             = np.zeros(B, np.float64)
        sharpe              = np.zeros(B, np.float64)
        sortino             = np.zeros(B, np.float64)
        calmar              = np.zeros(B, np.float64)
        trades_arr          = np.zeros(B, np.int32)
        entries_arr         = np.zeros(B, np.int32)
        win_rate_pct        = np.zeros(B, np.float64)
        avg_win_pct         = np.zeros(B, np.float64)
        avg_loss_pct        = np.zeros(B, np.float64)
        payoff              = np.zeros(B, np.float64)
        profit_factor       = np.zeros(B, np.float64)
        expectancy_pct      = np.zeros(B, np.float64)
        avg_hold_bars       = np.zeros(B, np.float64)
        time_in_market_pct  = np.zeros(B, np.float64)
        equity_final        = np.ones(B, np.float64)

        for b in prange(B):
            tp = tp_vec[b]
            sl = sl_vec[b]

            i = 0
            trades = 0
            entries = 0
            time_in_mkt = 0.0

            wins_cnt = 0
            wins_sum = 0.0
            losses_sum_abs = 0.0

            sum_r = 0.0
            sum_r2 = 0.0
            sum_r_dn = 0.0
            sum_r_dn2 = 0.0
            dn_cnt = 0

            equity = 1.0
            peak = 1.0
            mdd = 0.0

            while i < T - 1:
                if sigs[b, i]:
                    entries += 1
                    entry_idx = i + 1
                    if entry_idx >= T:
                        break
                    entry_px = o[entry_idx]
                    end = entry_idx + max_hold
                    if end >= T:
                        end = T - 1

                    # 做空：TP 在下方，SL 在上方
                    tp_level = entry_px * (1.0 - tp)
                    sl_level = entry_px * (1.0 + sl)

                    exit_idx = end
                    exit_px = c[end]

                    for j in range(entry_idx, end + 1):
                        hit_tp = (l[j] <= tp_level) # 低點觸及 TP
                        hit_sl = (h[j] >= sl_level) # 高點觸及 SL
                        if hit_tp and hit_sl:
                            exit_idx = j
                            # worst_case=1(True) -> 先 SL (觸發較高價的SL)
                            exit_px = sl_level if (worst_case_i8 == 1) else tp_level
                            break
                        elif hit_sl:
                            exit_idx = j
                            exit_px = sl_level
                            break
                        elif hit_tp:
                            exit_idx = j
                            exit_px = tp_level
                            break

                    # 做空淨利潤公式：(賣出獲得 - 買回成本) / 賣出獲得
                    # 賣出獲得 = entry_px * (1 - fee)
                    # 買回成本 = exit_px * (1 + fee)
                    exec_entry = entry_px - slippage
                    exec_exit  = exit_px  + slippage
                    if exec_entry <= 0.0:
                        exec_entry = 1e-12
                    if exec_exit <= 0.0:
                        exec_exit = 1e-12
                    revenue = exec_entry * (1.0 - fee_side)
                    cost = exec_exit * (1.0 + fee_side)
                    net_ret = (revenue - cost) / exec_entry

                    trades += 1
                    time_in_mkt += (exit_idx - entry_idx + 1)

                    sum_r += net_ret
                    sum_r2 += net_ret * net_ret

                    if net_ret > 0.0:
                        wins_cnt += 1
                        wins_sum += net_ret
                    elif net_ret < 0.0:
                        losses_sum_abs += -net_ret
                        sum_r_dn += net_ret
                        sum_r_dn2 += net_ret * net_ret
                        dn_cnt += 1

                    equity *= (1.0 + net_ret)
                    if equity > peak:
                        peak = equity
                    dd = (equity / peak) - 1.0
                    if dd < -mdd:
                        mdd = -dd

                    i = exit_idx + 1
                else:
                    i += 1

            equity_final[b] = equity
            total_return = equity - 1.0
            total_return_pct[b] = total_return * 100.0

            years = T / bpy
            cagr = (equity ** (1.0 / years) - 1.0) if years > 0 else 0.0
            cagr_pct[b] = cagr * 100.0

            mu = sum_r / T
            if T > 1:
                var = (sum_r2 - T * mu * mu) / (T - 1.0)
            else:
                var = 0.0
            sd = math.sqrt(var) if var > 0.0 else 0.0
            sharpe[b] = (mu / sd) * math.sqrt(bpy) if sd > 0.0 else 0.0

            sd_dn = 0.0
            if dn_cnt > 1:
                mu_dn = sum_r_dn / dn_cnt
                var_dn = (sum_r_dn2 - dn_cnt * mu_dn * mu_dn) / (dn_cnt - 1.0)
                if var_dn > 0.0:
                    sd_dn = math.sqrt(var_dn)
            sortino[b] = (mu / sd_dn) * math.sqrt(bpy) if sd_dn > 0.0 else 0.0

            mdd_pct[b] = mdd * 100.0
            calmar[b] = (cagr / mdd) if mdd > 0.0 else 0.0

            trades_arr[b] = trades
            entries_arr[b] = entries

            win_rate_pct[b] = (wins_cnt / trades * 100.0) if trades > 0 else 0.0
            avg_win = (wins_sum / wins_cnt) if wins_cnt > 0 else 0.0
            avg_loss = (-losses_sum_abs / dn_cnt) if dn_cnt > 0 else 0.0
            avg_win_pct[b]  = avg_win * 100.0
            avg_loss_pct[b] = avg_loss * 100.0
            payoff[b] = (avg_win / abs(avg_loss)) if avg_loss != 0.0 else 0.0
            profit_factor[b] = (wins_sum / losses_sum_abs) if losses_sum_abs > 0.0 else 0.0
            expectancy_pct[b] = ((sum_r / trades) * 100.0) if trades > 0 else 0.0
            avg_hold_bars[b] = (time_in_mkt / trades) if trades > 0 else 0.0
            time_in_market_pct[b] = (time_in_mkt / T) * 100.0

        return (
            total_return_pct, cagr_pct, mdd_pct, sharpe, sortino, calmar,
            trades_arr, entries_arr, win_rate_pct, avg_win_pct, avg_loss_pct,
            payoff, profit_factor, expectancy_pct, avg_hold_bars, time_in_market_pct,
            equity_final
        )
    @njit(cache=True, fastmath=True, parallel=True)
    def _metrics_from_sigs_batch_1m_nb(o1, h1, l1, c1,
                                       bar_1m_start, bar_1m_end, m1_to_htf,
                                       sigs, tp_vec, sl_vec,
                                       max_hold, fee_side, slippage, worst_case_i8,
                                       T, bpy):
        # 1m 精準撮合 + 事件驅動（Long）
        # sigs: [B, T] bool (訊號在 HTF bar 上)
        B = sigs.shape[0]

        total_return_pct    = np.zeros(B, np.float64)
        cagr_pct            = np.zeros(B, np.float64)
        mdd_pct             = np.zeros(B, np.float64)
        sharpe              = np.zeros(B, np.float64)
        sortino             = np.zeros(B, np.float64)
        calmar              = np.zeros(B, np.float64)
        trades_arr          = np.zeros(B, np.int32)
        entries_arr         = np.zeros(B, np.int32)
        win_rate_pct        = np.zeros(B, np.float64)
        avg_win_pct         = np.zeros(B, np.float64)
        avg_loss_pct        = np.zeros(B, np.float64)
        payoff              = np.zeros(B, np.float64)
        profit_factor       = np.zeros(B, np.float64)
        expectancy_pct      = np.zeros(B, np.float64)
        avg_hold_bars       = np.zeros(B, np.float64)
        time_in_market_pct  = np.zeros(B, np.float64)
        equity_final        = np.ones(B, np.float64)

        m1_len = c1.shape[0]

        for b in prange(B):
            tp = tp_vec[b]
            sl = sl_vec[b]

            i = 0
            trades = 0
            entries = 0
            time_in_mkt = 0.0

            wins_cnt = 0
            wins_sum = 0.0
            losses_sum_abs = 0.0

            sum_r = 0.0
            sum_r2 = 0.0
            sum_r_dn = 0.0
            sum_r_dn2 = 0.0
            dn_cnt = 0

            equity = 1.0
            peak = 1.0
            mdd = 0.0

            while i < T - 1:
                if sigs[b, i]:
                    entries += 1
                    entry_htf = i + 1
                    if entry_htf >= T:
                        break

                    end_htf = entry_htf + max_hold
                    if end_htf >= T:
                        end_htf = T - 1

                    m_s = int(bar_1m_start[entry_htf])
                    if m_s < 0:
                        m_s = 0
                    if m_s >= m1_len:
                        # 1m 資料不足，直接終止
                        break

                    m_e = int(bar_1m_end[end_htf])
                    if m_e > m1_len:
                        m_e = m1_len

                    # 進場：entry bar 的第一根 1m open
                    entry_px = o1[m_s]

                    # 如果範圍無效，強制用 entry minute 當 TIME exit
                    if m_e <= m_s:
                        exit_m = m_s
                        exit_px = c1[exit_m]
                        exit_htf = entry_htf
                    else:
                        tp_level = entry_px * (1.0 + tp)
                        sl_level = entry_px * (1.0 - sl)

                        exit_m = m_e - 1
                        exit_px = c1[exit_m]
                        exit_htf = end_htf

                        for mj in range(m_s, m_e):
                            hit_tp = (h1[mj] >= tp_level)
                            hit_sl = (l1[mj] <= sl_level)
                            if hit_tp or hit_sl:
                                exit_m = mj
                                if hit_tp and hit_sl:
                                    exit_px = sl_level if (worst_case_i8 == 1) else tp_level
                                elif hit_sl:
                                    exit_px = sl_level
                                else:
                                    exit_px = tp_level
                                exit_htf = int(m1_to_htf[exit_m])
                                if exit_htf < entry_htf:
                                    exit_htf = entry_htf
                                if exit_htf > end_htf:
                                    exit_htf = end_htf
                                break

                    # slippage 為「絕對價格距離」
                    exec_entry = entry_px + slippage
                    exec_exit = exit_px - slippage
                    if exec_entry <= 0.0:
                        exec_entry = 1e-12
                    if exec_exit <= 0.0:
                        exec_exit = 1e-12
                    net_ret = ((exec_exit * (1.0 - fee_side)) / (exec_entry * (1.0 + fee_side))) - 1.0

                    trades += 1
                    time_in_mkt += (exit_htf - entry_htf + 1)

                    sum_r += net_ret
                    sum_r2 += net_ret * net_ret

                    if net_ret > 0.0:
                        wins_cnt += 1
                        wins_sum += net_ret
                    elif net_ret < 0.0:
                        losses_sum_abs += -net_ret
                        sum_r_dn += net_ret
                        sum_r_dn2 += net_ret * net_ret
                        dn_cnt += 1

                    equity *= (1.0 + net_ret)
                    if equity > peak:
                        peak = equity
                    dd = (equity / peak) - 1.0
                    if dd < -mdd:
                        mdd = -dd

                    i = exit_htf + 1
                else:
                    i += 1

            equity_final[b] = equity
            total_return = equity - 1.0
            total_return_pct[b] = total_return * 100.0

            years = T / bpy
            cagr = (equity ** (1.0 / years) - 1.0) if years > 0 else 0.0
            cagr_pct[b] = cagr * 100.0

            mu = sum_r / T
            if T > 1:
                var = (sum_r2 - T * mu * mu) / (T - 1.0)
            else:
                var = 0.0
            sd = math.sqrt(var) if var > 0.0 else 0.0
            sharpe[b] = (mu / sd) * math.sqrt(bpy) if sd > 0.0 else 0.0

            sd_dn = 0.0
            if dn_cnt > 1:
                mu_dn = sum_r_dn / dn_cnt
                var_dn = (sum_r_dn2 - dn_cnt * mu_dn * mu_dn) / (dn_cnt - 1.0)
                if var_dn > 0.0:
                    sd_dn = math.sqrt(var_dn)
            sortino[b] = (mu / sd_dn) * math.sqrt(bpy) if sd_dn > 0.0 else 0.0

            mdd_pct[b] = mdd * 100.0
            calmar[b] = (cagr / mdd) if mdd > 0.0 else 0.0

            trades_arr[b] = trades
            entries_arr[b] = entries

            win_rate_pct[b] = (wins_cnt / trades * 100.0) if trades > 0 else 0.0
            avg_win = (wins_sum / wins_cnt) if wins_cnt > 0 else 0.0
            avg_loss = (-losses_sum_abs / dn_cnt) if dn_cnt > 0 else 0.0
            avg_win_pct[b]  = avg_win * 100.0
            avg_loss_pct[b] = avg_loss * 100.0
            payoff[b] = (avg_win / abs(avg_loss)) if avg_loss != 0.0 else 0.0
            profit_factor[b] = (wins_sum / losses_sum_abs) if losses_sum_abs > 0.0 else 0.0
            expectancy_pct[b] = ((sum_r / trades) * 100.0) if trades > 0 else 0.0
            avg_hold_bars[b] = (time_in_mkt / trades) if trades > 0 else 0.0
            time_in_market_pct[b] = (time_in_mkt / T) * 100.0

        return (
            total_return_pct, cagr_pct, mdd_pct, sharpe, sortino, calmar,
            trades_arr, entries_arr, win_rate_pct, avg_win_pct, avg_loss_pct,
            payoff, profit_factor, expectancy_pct, avg_hold_bars, time_in_market_pct,
            equity_final
        )

    @njit(cache=True, fastmath=True, parallel=True)
    def _metrics_from_sigs_batch_short_1m_nb(o1, h1, l1, c1,
                                             bar_1m_start, bar_1m_end, m1_to_htf,
                                             sigs, tp_vec, sl_vec,
                                             max_hold, fee_side, slippage, worst_case_i8,
                                             T, bpy):
        # 1m 精準撮合 + 事件驅動（Short）
        B = sigs.shape[0]

        total_return_pct    = np.zeros(B, np.float64)
        cagr_pct            = np.zeros(B, np.float64)
        mdd_pct             = np.zeros(B, np.float64)
        sharpe              = np.zeros(B, np.float64)
        sortino             = np.zeros(B, np.float64)
        calmar              = np.zeros(B, np.float64)
        trades_arr          = np.zeros(B, np.int32)
        entries_arr         = np.zeros(B, np.int32)
        win_rate_pct        = np.zeros(B, np.float64)
        avg_win_pct         = np.zeros(B, np.float64)
        avg_loss_pct        = np.zeros(B, np.float64)
        payoff              = np.zeros(B, np.float64)
        profit_factor       = np.zeros(B, np.float64)
        expectancy_pct      = np.zeros(B, np.float64)
        avg_hold_bars       = np.zeros(B, np.float64)
        time_in_market_pct  = np.zeros(B, np.float64)
        equity_final        = np.ones(B, np.float64)

        m1_len = c1.shape[0]

        for b in prange(B):
            tp = tp_vec[b]
            sl = sl_vec[b]

            i = 0
            trades = 0
            entries = 0
            time_in_mkt = 0.0

            wins_cnt = 0
            wins_sum = 0.0
            losses_sum_abs = 0.0

            sum_r = 0.0
            sum_r2 = 0.0
            sum_r_dn = 0.0
            sum_r_dn2 = 0.0
            dn_cnt = 0

            equity = 1.0
            peak = 1.0
            mdd = 0.0

            while i < T - 1:
                if sigs[b, i]:
                    entries += 1
                    entry_htf = i + 1
                    if entry_htf >= T:
                        break

                    end_htf = entry_htf + max_hold
                    if end_htf >= T:
                        end_htf = T - 1

                    m_s = int(bar_1m_start[entry_htf])
                    if m_s < 0:
                        m_s = 0
                    if m_s >= m1_len:
                        break

                    m_e = int(bar_1m_end[end_htf])
                    if m_e > m1_len:
                        m_e = m1_len

                    entry_px = o1[m_s]

                    if m_e <= m_s:
                        exit_m = m_s
                        exit_px = c1[exit_m]
                        exit_htf = entry_htf
                    else:
                        tp_level = entry_px * (1.0 - tp)
                        sl_level = entry_px * (1.0 + sl)

                        exit_m = m_e - 1
                        exit_px = c1[exit_m]
                        exit_htf = end_htf

                        for mj in range(m_s, m_e):
                            hit_tp = (l1[mj] <= tp_level)
                            hit_sl = (h1[mj] >= sl_level)
                            if hit_tp or hit_sl:
                                exit_m = mj
                                if hit_tp and hit_sl:
                                    exit_px = sl_level if (worst_case_i8 == 1) else tp_level
                                elif hit_sl:
                                    exit_px = sl_level
                                else:
                                    exit_px = tp_level
                                exit_htf = int(m1_to_htf[exit_m])
                                if exit_htf < entry_htf:
                                    exit_htf = entry_htf
                                if exit_htf > end_htf:
                                    exit_htf = end_htf
                                break

                    exec_entry = entry_px - slippage
                    exec_exit = exit_px + slippage
                    if exec_entry <= 0.0:
                        exec_entry = 1e-12
                    if exec_exit <= 0.0:
                        exec_exit = 1e-12

                    revenue = exec_entry * (1.0 - fee_side)
                    cost = exec_exit * (1.0 + fee_side)
                    net_ret = (revenue - cost) / exec_entry

                    trades += 1
                    time_in_mkt += (exit_htf - entry_htf + 1)

                    sum_r += net_ret
                    sum_r2 += net_ret * net_ret

                    if net_ret > 0.0:
                        wins_cnt += 1
                        wins_sum += net_ret
                    elif net_ret < 0.0:
                        losses_sum_abs += -net_ret
                        sum_r_dn += net_ret
                        sum_r_dn2 += net_ret * net_ret
                        dn_cnt += 1

                    equity *= (1.0 + net_ret)
                    if equity > peak:
                        peak = equity
                    dd = (equity / peak) - 1.0
                    if dd < -mdd:
                        mdd = -dd

                    i = exit_htf + 1
                else:
                    i += 1

            equity_final[b] = equity
            total_return = equity - 1.0
            total_return_pct[b] = total_return * 100.0

            years = T / bpy
            cagr = (equity ** (1.0 / years) - 1.0) if years > 0 else 0.0
            cagr_pct[b] = cagr * 100.0

            mu = sum_r / T
            if T > 1:
                var = (sum_r2 - T * mu * mu) / (T - 1.0)
            else:
                var = 0.0
            sd = math.sqrt(var) if var > 0.0 else 0.0
            sharpe[b] = (mu / sd) * math.sqrt(bpy) if sd > 0.0 else 0.0

            sd_dn = 0.0
            if dn_cnt > 1:
                mu_dn = sum_r_dn / dn_cnt
                var_dn = (sum_r_dn2 - dn_cnt * mu_dn * mu_dn) / (dn_cnt - 1.0)
                if var_dn > 0.0:
                    sd_dn = math.sqrt(var_dn)
            sortino[b] = (mu / sd_dn) * math.sqrt(bpy) if sd_dn > 0.0 else 0.0

            mdd_pct[b] = mdd * 100.0
            calmar[b] = (cagr / mdd) if mdd > 0.0 else 0.0

            trades_arr[b] = trades
            entries_arr[b] = entries

            win_rate_pct[b] = (wins_cnt / trades * 100.0) if trades > 0 else 0.0
            avg_win = (wins_sum / wins_cnt) if wins_cnt > 0 else 0.0
            avg_loss = (-losses_sum_abs / dn_cnt) if dn_cnt > 0 else 0.0
            avg_win_pct[b]  = avg_win * 100.0
            avg_loss_pct[b] = avg_loss * 100.0
            payoff[b] = (avg_win / abs(avg_loss)) if avg_loss != 0.0 else 0.0
            profit_factor[b] = (wins_sum / losses_sum_abs) if losses_sum_abs > 0.0 else 0.0
            expectancy_pct[b] = ((sum_r / trades) * 100.0) if trades > 0 else 0.0
            avg_hold_bars[b] = (time_in_mkt / trades) if trades > 0 else 0.0
            time_in_market_pct[b] = (time_in_mkt / T) * 100.0

        return (
            total_return_pct, cagr_pct, mdd_pct, sharpe, sortino, calmar,
            trades_arr, entries_arr, win_rate_pct, avg_win_pct, avg_loss_pct,
            payoff, profit_factor, expectancy_pct, avg_hold_bars, time_in_market_pct,
            equity_final
        )
# ======== /FAST CORE ========

# ----------------------------- 回測核心（JIT 友善版） ----------------------------- #

def _simulate_long_core_py(o, h, l, c, entry_sig, tp_pct, sl_pct, max_hold, fee_side=0.0002, slippage=0.0, worst_case=True):
    """
    純 Python 版本：只做數值計算與索引陣列，回傳：
      perbar, equity,
      entry_idx_arr, exit_idx_arr,
      entry_px_arr, exit_px_arr,
      net_ret_arr, bars_held_arr, reason_arr(int8: 1=SL,2=TP,3=SL_samebar,4=TP_samebar,5=TIME)
    任何「交易明細的 dict」都放到 Python 外面組，避免 JIT 崩潰。
    """
    n = len(c)
    perbar = np.zeros(n, dtype=np.float64)

    # 最大筆數不會超過訊號數（最後一根不能進場）
    max_trades = int(np.sum(entry_sig[:-1]))
    entry_idx_arr = np.full(max_trades, -1, dtype=np.int64)
    exit_idx_arr  = np.full(max_trades, -1, dtype=np.int64)
    entry_px_arr  = np.zeros(max_trades, dtype=np.float64)
    exit_px_arr   = np.zeros(max_trades, dtype=np.float64)
    net_ret_arr   = np.zeros(max_trades, dtype=np.float64)
    bars_held_arr = np.zeros(max_trades, dtype=np.int32)
    reason_arr    = np.zeros(max_trades, dtype=np.int8)

    tcount = 0
    in_pos = False
    entry_idx = -1
    entry_price = 0.0

    for i in range(n-1):
        if not in_pos:
            if entry_sig[i]:
                entry_idx = i + 1
                if entry_idx >= n:
                    break
                entry_price = o[entry_idx]
                in_pos = True
        else:
            start = entry_idx
            end = entry_idx + max_hold
            if end >= n:
                end = n - 1

            tp_level = entry_price * (1.0 + tp_pct)
            sl_level = entry_price * (1.0 - sl_pct)

            exit_idx = -1
            exit_price = 0.0
            reason = 0

            j = start
            while j <= end:
                hit_tp = h[j] >= tp_level
                hit_sl = l[j] <= sl_level
                if hit_tp and hit_sl:
                    exit_idx = j
                    if worst_case:
                        exit_price = sl_level
                        reason = 3  # SL_samebar
                    else:
                        exit_price = tp_level
                        reason = 4  # TP_samebar
                    break
                elif hit_sl:
                    exit_idx = j
                    exit_price = sl_level
                    reason = 1  # SL
                    break
                elif hit_tp:
                    exit_idx = j
                    exit_price = tp_level
                    reason = 2  # TP
                    break
                j += 1

            if exit_idx == -1:
                exit_idx = end
                exit_price = c[exit_idx]
                reason = 5  # TIME

            # slippage 以「價格絕對距離」表示（ticks * tick_size），不是百分比
            exec_entry = entry_price + slippage
            exec_exit = exit_price - slippage
            if exec_entry <= 0.0:
                exec_entry = 1e-12
            if exec_exit <= 0.0:
                exec_exit = 1e-12
            net_ret = ((exec_exit * (1.0 - fee_side)) / (exec_entry * (1.0 + fee_side))) - 1.0

            perbar[exit_idx] += net_ret

            if tcount < max_trades:
                entry_idx_arr[tcount] = entry_idx
                exit_idx_arr[tcount]  = exit_idx
                entry_px_arr[tcount]  = entry_price
                exit_px_arr[tcount]   = exit_price
                net_ret_arr[tcount]   = net_ret
                bars_held_arr[tcount] = exit_idx - entry_idx + 1
                reason_arr[tcount]    = reason
                tcount += 1

            in_pos = False
            entry_idx = -1
            entry_price = 0.0

    equity = np.ones(n, dtype=np.float64)
    for k in range(1, n):
        equity[k] = equity[k-1] * (1.0 + perbar[k])

    # 回傳有效區間
    return (perbar,
            equity,
            entry_idx_arr[:tcount],
            exit_idx_arr[:tcount],
            entry_px_arr[:tcount],
            exit_px_arr[:tcount],
            net_ret_arr[:tcount],
            bars_held_arr[:tcount],
            reason_arr[:tcount])

if NUMBA_OK:
    # JIT 版本：與 _py 同簽章，同回傳
    @njit(cache=True, fastmath=True, nogil=True)
    def _simulate_long_core_nb(o, h, l, c, entry_sig, tp_pct, sl_pct, max_hold, fee_side=0.0002, slippage=0.0, worst_case=True):

        n = len(c)
        perbar = np.zeros(n, np.float64)
        max_trades = 0
        for ii in range(n-1):
            if entry_sig[ii]:
                max_trades += 1
        entry_idx_arr = np.full(max_trades, -1, np.int64)
        exit_idx_arr  = np.full(max_trades, -1, np.int64)
        entry_px_arr  = np.zeros(max_trades, np.float64)
        exit_px_arr   = np.zeros(max_trades, np.float64)
        net_ret_arr   = np.zeros(max_trades, np.float64)
        bars_held_arr = np.zeros(max_trades, np.int32)
        reason_arr    = np.zeros(max_trades, np.int8)

        tcount = 0
        in_pos = False
        entry_idx = -1
        entry_price = 0.0

        for i in range(n-1):
            if not in_pos:
                if entry_sig[i]:
                    entry_idx = i + 1
                    if entry_idx >= n:
                        break
                    entry_price = o[entry_idx]
                    in_pos = True
            else:
                start = entry_idx
                end = entry_idx + max_hold
                if end >= n:
                    end = n - 1

                tp_level = entry_price * (1.0 + tp_pct)
                sl_level = entry_price * (1.0 - sl_pct)

                exit_idx = -1
                exit_price = 0.0
                reason = 0

                j = start
                while j <= end:
                    hit_tp = h[j] >= tp_level
                    hit_sl = l[j] <= sl_level
                    if hit_tp and hit_sl:
                        exit_idx = j
                        if worst_case:
                            exit_price = sl_level
                            reason = 3
                        else:
                            exit_price = tp_level
                            reason = 4
                        break
                    elif hit_sl:
                        exit_idx = j
                        exit_price = sl_level
                        reason = 1
                        break
                    elif hit_tp:
                        exit_idx = j
                        exit_price = tp_level
                        reason = 2
                        break
                    j += 1

                if exit_idx == -1:
                    exit_idx = end
                    exit_price = c[exit_idx]
                    reason = 5

                exec_entry = entry_price + slippage
                exec_exit = exit_price - slippage
                if exec_entry <= 0.0:
                    exec_entry = 1e-12
                if exec_exit <= 0.0:
                    exec_exit = 1e-12
                net_ret = ((exec_exit * (1.0 - fee_side)) / (exec_entry * (1.0 + fee_side))) - 1.0
                perbar[exit_idx] += net_ret

                if tcount < max_trades:
                    entry_idx_arr[tcount] = entry_idx
                    exit_idx_arr[tcount]  = exit_idx
                    entry_px_arr[tcount]  = entry_price
                    exit_px_arr[tcount]   = exit_price
                    net_ret_arr[tcount]   = net_ret
                    bars_held_arr[tcount] = exit_idx - entry_idx + 1
                    reason_arr[tcount]    = reason
                    tcount += 1

                in_pos = False
                entry_idx = -1
                entry_price = 0.0

        equity = np.ones(n, np.float64)
        for k in range(1, n):
            equity[k] = equity[k-1] * (1.0 + perbar[k])

        return (perbar,
                equity,
                entry_idx_arr[:tcount],
                exit_idx_arr[:tcount],
                entry_px_arr[:tcount],
                exit_px_arr[:tcount],
                net_ret_arr[:tcount],
                bars_held_arr[:tcount],
                reason_arr[:tcount])

# 統一入口：Py 或 Numba 版本
    @njit(cache=True, fastmath=True, parallel=True)
    def _signal_from_ob_fvg_nb(o: np.ndarray, h: np.ndarray, l: np.ndarray, c: np.ndarray, v: np.ndarray,
                               param_N: int, param_r: float, param_h: int, param_g: float,
                               param_a: float, param_rise_thr: float,
                               param_x: float, param_y: float,
                               param_monitor_window: int,
                               param_rsi_period: int, param_rsi_diff: float) -> Tuple[np.ndarray, Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]]:
        """
        Numba 加速核心：嚴格遵守 OB_FVG 定義
        1. 趨勢: 連續N根(C-O)/O > r 且 V > avg(h)*g
        2. OB: 趨勢前一根紅K (Low~High)
        3. FVG: 趨勢中每根綠K (Open*x ~ Close*y)
        4. 開單: 形成後 monitor_window 分鐘內，價位需滿足：先漲至 OB_High * rise_thr -> 跌回 OB_Low * a -> 漲回 OB_High
        5. 濾網: 突破當下 RSI > OB第一根K棒RSI * (1 + param_rsi_diff)
        """
        Bars = c.shape[0]
        sig = np.zeros(Bars, dtype=np.bool_)
        
        # 輸出用的陣列
        ob_top_arr = np.full(Bars, np.nan, dtype=np.float32)
        ob_bottom_arr = np.full(Bars, np.nan, dtype=np.float32)
        fvg_top_arr = np.full(Bars, np.nan, dtype=np.float32)
        fvg_bottom_arr = np.full(Bars, np.nan, dtype=np.float32)
        ob_idx_arr = np.full(Bars, -1, dtype=np.int32)
        highest_arr = np.full(Bars, np.nan, dtype=np.float32)
        lowest_arr = np.full(Bars, np.nan, dtype=np.float32)

        # 0. 預先計算 RSI (使用 Numba 優化版 _rsi_wilder_nb)
        rsi_arr = _rsi_wilder_nb(c, param_rsi_period)

        # 1. 預先計算 Volume Rolling Average (Simple Moving Average)
        vol_avg = np.zeros(Bars, dtype=np.float64)
        if param_h > 0:
            current_sum = 0.0
            for i in range(Bars):
                current_sum += v[i]
                if i >= param_h:
                    current_sum -= v[i - param_h]
                    vol_avg[i] = current_sum / param_h
                elif i >= 0:
                    vol_avg[i] = current_sum / (i + 1)
        
        # 2. 掃描趨勢並標記 Zone
        # 我們需要「掃描」過去的 Zone，並在當前 K 線檢查是否滿足開單條件
        # 為了效率，當發現一個 Zone (OB) 時，我們向後掃描 monitor_window 分鐘來判斷開單
        
        for i in range(param_h + 1, Bars - param_N):
            # 檢查 i 為起點的趨勢 (i 必須是趨勢第一根綠K)
            # 趨勢定義：連續 param_N 根
            # 前一根 i-1 必須是紅K (OB母體)
            if c[i-1] >= o[i-1]:
                continue
            
            is_trend = True
            # 檢查連續 N 根
            for k in range(param_N):
                idx = i + k
                if idx >= Bars:
                    is_trend = False
                    break
                
                # 條件 1: 綠K
                if c[idx] <= o[idx]:
                    is_trend = False
                    break
                
                # 條件 2: 實體漲幅 > r
                body_change = (c[idx] - o[idx]) / o[idx]
                if body_change <= param_r:
                    is_trend = False
                    break
                
                # 條件 3: 成交量 > 過去 h 分鐘平均 * g
                # 注意：過去 h 分鐘相對於該 K 線 (idx)
                # 使用 idx-1 的 avg 避免包含當根 (或者根據定義 "過去 h 分鐘" 包含當下?)
                # 通常 backtest 習慣看當根量 vs 前 h 根均量
                ref_vol = vol_avg[idx-1] if idx > 0 else 0.0
                if v[idx] <= ref_vol * param_g:
                    is_trend = False
                    break
            
            if is_trend:
                # 定義 OB 區間 (紅K i-1)
                ob_high = h[i-1]
                ob_low = l[i-1]
                ob_idx = i-1
                
                # 定義 FVG (趨勢中的綠K) - 這裡主要用於視覺化記錄，取最後一個或全部
                # 為了簡化，記錄第一根綠K的 FVG 或整個趨勢區域，
                # 但根據需求"各為N個...FVG"，我們只在觸發訊號時填入當下的 FVG 資訊
                # 這裡先記錄 OB 資訊供訊號觸發時使用
                
                # 3. 開單邏輯監測 (狀態機)
                # 有效期：形成後 monitor_window 分鐘 (K棒)
                # 形成結束點為 i + param_N - 1
                trend_end_idx = i + param_N - 1
                monitor_start = trend_end_idx + 1
                monitor_end = min(Bars, monitor_start + param_monitor_window)
                
                # 狀態: 0=初始, 1=已漲過門檻, 2=已跌回下軌修正
                state = 0
                threshold_price = ob_high * param_rise_thr
                dip_price = ob_low * param_a
                
                for k in range(monitor_start, monitor_end):
                    curr_h = h[k]
                    curr_l = l[k]
                    curr_c = c[k]
                    
                    if state == 0:
                        # 等待漲過門檻
                        if curr_h >= threshold_price:
                            state = 1
                            # 同一根 K 線也可能完成後續動作，繼續檢查
                    
                    if state == 1:
                        # 等待跌回 OB 下軌 * a
                        if curr_l <= dip_price:
                            state = 2
                            # 同一根 K 線可能完成
                    
                    if state == 2:
                        # 等待漲回 OB 上軌 (進場)
                        if curr_c > ob_high:
                            # 觸發進場！加入 RSI 動能濾網
                            # 條件：當下 RSI > OB第一根(i-1) RSI * (1 + j%)
                            ob_rsi_val = rsi_arr[i-1]
                            # 簡單防呆: 若 ob_rsi_val 無效則不開，或視需求調整
                            if not np.isnan(ob_rsi_val) and rsi_arr[k] > ob_rsi_val * (1.0 + param_rsi_diff):
                                if not sig[k]: # 避免重複標記
                                    sig[k] = True
                                    
                                    # 記錄資訊
                                    ob_top_arr[k] = ob_high
                                ob_bottom_arr[k] = ob_low
                                ob_idx_arr[k] = ob_idx
                                highest_arr[k] = ob_high # 這裡指 OB 上軌
                                lowest_arr[k] = ob_low   # 這裡指 OB 下軌
                                
                                # 記錄對應的 FVG 資訊 (取趨勢中最近的一根或第一根，這裡取 i)
                                # 需求: "趨勢...的3個綠K...各為N個FVG"
                                # 我們記錄第一根綠K的 FVG 區間供參考
                                fvg_top_arr[k] = c[i] * param_y
                                fvg_bottom_arr[k] = o[i] * param_x
                            
                            # 該 Zone 觸發後即結束監測 (單一 Zone 只做一次)
                            break
                            
        return sig, (ob_top_arr, ob_bottom_arr, fvg_top_arr, fvg_bottom_arr, ob_idx_arr, highest_arr, lowest_arr)

    @njit(cache=True, fastmath=True)
    def _signal_from_smc_nb(o: np.ndarray, h: np.ndarray, l: np.ndarray, c: np.ndarray, v: np.ndarray,
                            param_len: int,
                            param_ob_limit: int,
                            param_reverse: bool
                           ) -> Tuple[np.ndarray, Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]]:
        """
        SMC 核心邏輯：
        完全對照 Pine Script 版本：
        1. Pivot (Len, Len)
        2. 3 Consecutive Candles (Green/Red)
        3. OB Creation: 3 Green + Break PH -> Bullish OB; 3 Red + Break PL -> Bearish OB
        4. Signal:
           - Break Bearish OB (Red) -> Signal Long
           - Break Bullish OB (Green) -> Signal Short
        """
        Bars = c.shape[0]
        sig = np.zeros(Bars, dtype=np.bool_)
        
        # 為了相容性保留這些回傳陣列 (雖不計算動態 TP/SL，但保留結構以防報錯)
        dummy_arr = np.full(Bars, np.nan, dtype=np.float32)
        
        # OB 結構陣列 (模擬 Array<OrderBlock>)
        # Columns: [top, btm, start_idx, active(1/0), direction(1=Bull, -1=Bear)]
        MAX_OBS = 500
        obs = np.zeros((MAX_OBS, 5), dtype=np.float64)
        ob_count = 0
        
        ph_price = np.nan
        pl_price = np.nan
        
        # 我們從 param_len * 2 開始，確保 pivot 計算穩定
        start_i = max(param_len * 2 + 1, 4)
        
        for i in range(start_i, Bars):
            # 1. Update Pivots (Pine: ta.pivothigh(length, length))
            # 在 bar i，若 i-length 是 pivot，則 ph_price 更新為 high[i-length]
            # 檢查 high[i-length] 是否為 range [i-2*length, i] 的最大值
            
            p_idx = i - param_len
            
            # Check Pivot High
            is_ph = True
            curr_h = h[p_idx]
            # Check left & right
            for k in range(1, param_len + 1):
                if h[p_idx - k] > curr_h: is_ph = False; break
                if h[p_idx + k] > curr_h: is_ph = False; break
            
            if is_ph:
                ph_price = curr_h

            # Check Pivot Low
            is_pl = True
            curr_l = l[p_idx]
            for k in range(1, param_len + 1):
                if l[p_idx - k] < curr_l: is_pl = False; break
                if l[p_idx + k] < curr_l: is_pl = False; break
                
            if is_pl:
                pl_price = curr_l
                
            # 2. Check 3 Consecutive Candles
            # Pine: close > open and close[1] > open[1] and close[2] > open[2]
            three_green = (c[i] > o[i]) and (c[i-1] > o[i-1]) and (c[i-2] > o[i-2])
            three_red   = (c[i] < o[i]) and (c[i-1] < o[i-1]) and (c[i-2] < o[i-2])
            
            # 3. Create OBs
            # Bullish OB: 3 Green + Break PH (close > ph) + Previous was below PH (close[3] < ph)
            # OB Source: index i-3. Must be Red (c < o)
            if three_green and not np.isnan(ph_price):
                if c[i] > ph_price and c[i-3] < ph_price:
                    # Check i-3 is Red
                    if c[i-3] < o[i-3]:
                        # Create Bullish OB
                        # Find slot
                        slot = -1
                        for k in range(MAX_OBS):
                            if obs[k, 3] == 0: # inactive
                                slot = k; break
                        if slot == -1: # overwrite oldest (simple circular logic or just slot 0)
                            slot = ob_count % MAX_OBS
                            ob_count += 1
                        
                        obs[slot, 0] = h[i-3] # top
                        obs[slot, 1] = l[i-3] # btm
                        obs[slot, 2] = i-3    # start_idx
                        obs[slot, 3] = 1      # active
                        obs[slot, 4] = 1      # Bullish
            
            # Bearish OB: 3 Red + Break PL (close < pl) + Previous was above PL (close[3] > pl)
            # OB Source: index i-3. Must be Green (c > o)
            if three_red and not np.isnan(pl_price):
                if c[i] < pl_price and c[i-3] > pl_price:
                    # Check i-3 is Green
                    if c[i-3] > o[i-3]:
                        # Create Bearish OB
                        slot = -1
                        for k in range(MAX_OBS):
                            if obs[k, 3] == 0:
                                slot = k; break
                        if slot == -1:
                            slot = ob_count % MAX_OBS
                            ob_count += 1
                        
                        obs[slot, 0] = h[i-3] # top
                        obs[slot, 1] = l[i-3] # btm
                        obs[slot, 2] = i-3
                        obs[slot, 3] = 1
                        obs[slot, 4] = -1     # Bearish

            # 4. Manage OBs & Get Signals
            sig_break_bull = False # -> Short
            sig_break_bear = False # -> Long
            
            for k in range(MAX_OBS):
                if obs[k, 3] == 1:
                    # Check Expiration
                    if (i - obs[k, 2]) > param_ob_limit:
                        obs[k, 3] = 0 # Expire
                        continue
                    
                    is_bull = (obs[k, 4] == 1)
                    ob_top = obs[k, 0]
                    ob_btm = obs[k, 1]
                    
                    if is_bull:
                        # Bullish OB (Green) -> Check if broken below
                        if c[i] < ob_btm:
                            # Broken
                            obs[k, 3] = 0
                            sig_break_bull = True
                    else:
                        # Bearish OB (Red) -> Check if broken above
                        if c[i] > ob_top:
                            # Broken
                            obs[k, 3] = 0
                            sig_break_bear = True

            # 5. Final Signal Assignment
            # Pine:
            # if signal_long (break bear) -> Long
            # if signal_short (break bull) -> Short
            
            if not param_reverse:
                # Standard Mode: Want Longs
                if sig_break_bear:
                    sig[i] = True
            else:
                # Reverse Mode: Want Shorts
                if sig_break_bull:
                    sig[i] = True

        return sig, (dummy_arr, dummy_arr, dummy_arr, dummy_arr, np.zeros(Bars, dtype=np.int32), dummy_arr, dummy_arr)

def simulate_long_core(o, h, l, c, entry_sig, tp_pct, sl_pct, max_hold, fee_side=0.0002, slippage=0.0, worst_case=True):
    if NUMBA_OK:
        try:
            return _simulate_long_core_nb(o, h, l, c, entry_sig, tp_pct, sl_pct, max_hold, fee_side, slippage, worst_case)
        except Exception:
            return _simulate_long_core_py(o, h, l, c, entry_sig, tp_pct, sl_pct, max_hold, fee_side, slippage, worst_case)
    else:
        return _simulate_long_core_py(o, h, l, c, entry_sig, tp_pct, sl_pct, max_hold, fee_side, slippage, worst_case)


def simulate_short_core(o, h, l, c, entry_sig, tp_pct, sl_pct, max_hold, fee_side=0.0002, slippage=0.0, worst_case=True):
    if NUMBA_OK:
        try:
            return _simulate_short_core_nb(o, h, l, c, entry_sig, tp_pct, sl_pct, max_hold, fee_side, slippage, worst_case)
        except Exception:
            return _simulate_short_core_py(o, h, l, c, entry_sig, tp_pct, sl_pct, max_hold, fee_side, slippage, worst_case)
    else:
        return _simulate_short_core_py(o, h, l, c, entry_sig, tp_pct, sl_pct, max_hold, fee_side, slippage, worst_case)
def simulate_short_core_per_entry(o, h, l, c,
entry_sig, zone_ob_top, zone_ob_bottom, zone_fvg_top, zone_fvg_bottom,
ob_idx_arr, highest_arr, lowest_arr,
tp_pct, sl_pct, max_hold,
fee_side=0.0002, slippage=0.0, worst_case=True):
    """
    單筆逐筆模擬版本（做空）：依每筆交易的 OB/FVG 區間計算獨立 TP/SL 價位。
    OB_FVG 做空邏輯：當做多訊號出現時反向做空。
    假設：
    Long: TP = OB_High * (1+p), SL = OB_Low * (1-s)
    Short (Reverse): TP = OB_Low * (1 - p), SL = OB_High * (1 + s)
    注意：此處 tp_pct 與 sl_pct 在 Grid 中通常為 1% (0.01)。
    若 ob_range_based 開啟，傳入的 tp_pct/sl_pct 代表係數 (如 1.02/0.98)。
    為了安全起見，若數值接近 1.0，視為係數；若接近 0.0，視為百分比。
    """
    n = len(c)
    perbar = np.zeros(n, dtype=np.float64)
    max_trades = int(np.sum(entry_sig[:-1]))
    entry_idx_arr = np.full(max_trades, -1, dtype=np.int64)
    exit_idx_arr = np.full(max_trades, -1, dtype=np.int64)
    entry_px_arr = np.zeros(max_trades, dtype=np.float64)
    exit_px_arr = np.zeros(max_trades, dtype=np.float64)
    net_ret_arr = np.zeros(max_trades, dtype=np.float64)
    bars_held_arr = np.zeros(max_trades, dtype=np.int32)
    reason_arr = np.zeros(max_trades, dtype=np.int8)
    tcount = 0
    in_pos = False
    entry_idx = -1
    entry_price = 0.0
    for i in range(n-1):
        if not in_pos:
            if entry_sig[i]:
                entry_idx = i + 1
                if entry_idx >= n:
                    break
                entry_price = o[entry_idx]
                in_pos = True
        else:
            start = entry_idx
            end = entry_idx + max_hold
            if end >= n:
                end = n - 1
            base_idx = entry_idx - 1
            # 取得 OB 高低點
            high_val = highest_arr[base_idx] # OB Top
            low_val = lowest_arr[base_idx]   # OB Bottom
            
            # 判斷參數類型（係數或百分比）
            # 反向策略：TP 應設在下方，SL 應設在上方
            # 若為係數模式 (e.g. TP=1.02)，Long是 High*1.02。Short 則用 Low * (2 - TP)? 
            # 這裡簡化：若 range_based 啟用，我們假設使用者希望 Short 的 TP 是 "OB Low 下方某處"，SL 是 "OB High 上方某處"
            
            # 百分比模式 (通常 Grid Search 傳入 0.01~0.05)
            # TP level = Entry * (1 - tp_pct) -> 這是 standard
            # 這裡我們使用 OB 邊界作為基準：
            # TP = Low_val * (1.0 - tp_pct)
            # SL = High_val * (1.0 + sl_pct)
            
            is_ratio = (tp_pct > 0.5) # 簡單判斷
            
            if is_ratio:
                # 若傳入的是 1.02 (Long TP)，轉為 Short TP (0.98)
                # 若傳入的是 0.98 (Long SL)，轉為 Short SL (1.02)
                # 這裡假設使用者在 range_based 模式下，reverse 會希望依賴 OB 邊界
                _tp_ratio = 1.0 - (tp_pct - 1.0) if tp_pct >= 1.0 else tp_pct
                _sl_ratio = 1.0 + (1.0 - sl_pct) if sl_pct <= 1.0 else sl_pct
                tp_level = low_val * _tp_ratio
                sl_level = high_val * _sl_ratio
            else:
                tp_level = low_val * (1.0 - tp_pct)
                sl_level = high_val * (1.0 + sl_pct)

            exit_idx = -1
            exit_price = 0.0
            reason = 0
            j = start
            while j <= end:
                hit_tp = (l[j] <= tp_level)
                hit_sl = (h[j] >= sl_level)
                if hit_tp and hit_sl:
                    exit_idx = j
                    if worst_case:
                        exit_price = sl_level
                        reason = 3
                    else:
                        exit_price = tp_level
                        reason = 4
                    break
                elif hit_sl:
                    exit_idx = j
                    exit_price = sl_level
                    reason = 1
                    break
                elif hit_tp:
                    exit_idx = j
                    exit_price = tp_level
                    reason = 2
                    break
                j += 1
            if exit_idx == -1:
                exit_idx = end
                exit_price = c[exit_idx]
                reason = 5

            # slippage 以「價格絕對距離」表示（ticks * tick_size），不是百分比
            exec_entry = entry_price - slippage
            exec_exit = exit_price + slippage
            if exec_entry <= 0.0:
                exec_entry = 1e-12
            if exec_exit <= 0.0:
                exec_exit = 1e-12
            revenue = exec_entry * (1.0 - fee_side)
            cost = exec_exit * (1.0 + fee_side)
            net_ret = (revenue - cost) / exec_entry

            
            perbar[exit_idx] += net_ret
            if tcount < max_trades:
                entry_idx_arr[tcount] = entry_idx
                exit_idx_arr[tcount] = exit_idx
                entry_px_arr[tcount] = entry_price
                exit_px_arr[tcount] = exit_price
                net_ret_arr[tcount] = net_ret
                bars_held_arr[tcount] = exit_idx - entry_idx + 1
                reason_arr[tcount] = reason
                tcount += 1
            in_pos = False
            entry_idx = -1
            entry_price = 0.0
            
    equity = np.ones(n, dtype=np.float64)
    for k in range(1, n):
        equity[k] = equity[k-1] * (1.0 + perbar[k])
    return (perbar, equity, entry_idx_arr[:tcount], exit_idx_arr[:tcount],
            entry_px_arr[:tcount], exit_px_arr[:tcount], net_ret_arr[:tcount],
            bars_held_arr[:tcount], reason_arr[:tcount])
def simulate_long_core_per_entry(o, h, l, c,
entry_sig, zone_ob_top, zone_ob_bottom, zone_fvg_top, zone_fvg_bottom,
ob_idx_arr, highest_arr, lowest_arr,
tp_pct, sl_pct, max_hold,
fee_side=0.0002, slippage=0.0, worst_case=True):

    """
    單筆逐筆模擬版本：依每筆交易的 OB/FVG 區間計算獨立 TP/SL 價位。
    回傳結構與 simulate_long_core 相同。
    """
    n = len(c)
    perbar = np.zeros(n, dtype=np.float64)
    # 預估最多交易數不超過 entry 訊號數
    max_trades = int(np.sum(entry_sig[:-1]))
    entry_idx_arr = np.full(max_trades, -1, dtype=np.int64)
    exit_idx_arr = np.full(max_trades, -1, dtype=np.int64)
    entry_px_arr = np.zeros(max_trades, dtype=np.float64)
    exit_px_arr = np.zeros(max_trades, dtype=np.float64)
    net_ret_arr = np.zeros(max_trades, dtype=np.float64)
    bars_held_arr = np.zeros(max_trades, dtype=np.int32)
    reason_arr = np.zeros(max_trades, dtype=np.int8)
    tcount = 0
    in_pos = False
    entry_idx = -1
    entry_price = 0.0
    for i in range(n-1):
        if not in_pos:
            if entry_sig[i]:
                entry_idx = i + 1
                if entry_idx >= n:
                    break
                entry_price = o[entry_idx]
                in_pos = True
        else:
            start = entry_idx
            end = entry_idx + max_hold
            if end >= n:
                end = n - 1
            # 取得該筆交易的區間上下軌
            base_idx = entry_idx - 1
            z_ob_top = zone_ob_top[base_idx]
            z_ob_bottom = zone_ob_bottom[base_idx]
            z_fvg_top = zone_fvg_top[base_idx]
            z_fvg_bottom = zone_fvg_bottom[base_idx]
            if not np.isnan(z_fvg_top):
                # FVG 觸發交易，採用 FVG 區間
                zone_top = float(z_fvg_top)
                zone_bottom = float(z_fvg_bottom)
            else:
                # OB 觸發交易，採用 OB 區間
                zone_top = float(z_ob_top)
                zone_bottom = float(z_ob_bottom)
            high_val = highest_arr[base_idx] # OB Top
            low_val = lowest_arr[base_idx]   # OB Bottom
            
            # 判斷參數類型（係數或百分比），與 Short 邏輯保持一致
            is_ratio = (tp_pct > 0.5)
            
            if is_ratio:
                # 係數模式 (e.g. 1.02)
                # 做多：TP 在上 (High * >1)，SL 在下 (Low * <1)
                tp_level = high_val * tp_pct
                sl_level = low_val * sl_pct
            else:
                # 百分比模式 (e.g. 0.01)
                # 做多：TP = High * (1+p), SL = Low * (1-s)
                tp_level = high_val * (1.0 + tp_pct)
                sl_level = low_val * (1.0 - sl_pct)

            exit_idx = -1
            exit_price = 0.0
            reason = 0
            j = start
            while j <= end:
                hit_tp = (h[j] >= tp_level)
                hit_sl = (l[j] <= sl_level)
                if hit_tp and hit_sl:
                    exit_idx = j
                    if worst_case:
                        exit_price = sl_level
                        reason = 3  # SL_samebar
                    else:
                        exit_price = tp_level
                        reason = 4  # TP_samebar
                    break
                elif hit_sl:
                    exit_idx = j
                    exit_price = sl_level
                    reason = 1  # SL
                    break
                elif hit_tp:
                    exit_idx = j
                    exit_price = tp_level
                    reason = 2  # TP
                    break
                j += 1
            if exit_idx == -1:
                exit_idx = end
                exit_price = c[exit_idx]
                reason = 5  # TIME
            # 計算淨利潤（考慮手續費）
            exec_entry = entry_price + slippage
            exec_exit = exit_price - slippage
            if exec_entry <= 0.0:
                exec_entry = 1e-12
            if exec_exit <= 0.0:
                exec_exit = 1e-12
            net_ret = ((exec_exit * (1.0 - fee_side)) / (exec_entry * (1.0 + fee_side))) - 1.0
            perbar[exit_idx] += net_ret
            if tcount < max_trades:
                entry_idx_arr[tcount] = entry_idx
                exit_idx_arr[tcount] = exit_idx
                entry_px_arr[tcount] = entry_price
                exit_px_arr[tcount] = exit_price
                net_ret_arr[tcount] = net_ret
                bars_held_arr[tcount] = exit_idx - entry_idx + 1
                reason_arr[tcount] = reason
                tcount += 1
            in_pos = False
            entry_idx = -1
            entry_price = 0.0
    # 計算淨值曲線
    equity = np.ones(n, dtype=np.float64)
    for k in range(1, n):
        equity[k] = equity[k-1] * (1.0 + perbar[k])
    return (perbar,
            equity,
            entry_idx_arr[:tcount],
            exit_idx_arr[:tcount],
            entry_px_arr[:tcount],
            exit_px_arr[:tcount],
            net_ret_arr[:tcount],
            bars_held_arr[:tcount],
            reason_arr[:tcount])

def _simulate_short_core_py(o, h, l, c, entry_sig, tp_pct, sl_pct, max_hold, fee_side=0.0002, slippage=0.0, worst_case=True):
    """純 Python 做空模擬版本"""
    n = len(c)
    perbar = np.zeros(n, dtype=np.float64)
    max_trades = int(np.sum(entry_sig[:-1]))
    entry_idx_arr = np.full(max_trades, -1, dtype=np.int64)
    exit_idx_arr  = np.full(max_trades, -1, dtype=np.int64)
    entry_px_arr  = np.zeros(max_trades, dtype=np.float64)
    exit_px_arr   = np.zeros(max_trades, dtype=np.float64)
    net_ret_arr   = np.zeros(max_trades, dtype=np.float64)
    bars_held_arr = np.zeros(max_trades, dtype=np.int32)
    reason_arr    = np.zeros(max_trades, dtype=np.int8)

    tcount = 0
    in_pos = False
    entry_idx = -1
    entry_price = 0.0

    for i in range(n-1):
        if not in_pos:
            if entry_sig[i]:
                entry_idx = i + 1
                if entry_idx >= n:
                    break
                entry_price = o[entry_idx]
                in_pos = True
        else:
            start = entry_idx
            end = entry_idx + max_hold
            if end >= n:
                end = n - 1

            # 做空：TP 在下，SL 在上
            tp_level = entry_price * (1.0 - tp_pct)
            sl_level = entry_price * (1.0 + sl_pct)

            exit_idx = -1
            exit_price = 0.0
            reason = 0

            j = start
            while j <= end:
                hit_tp = l[j] <= tp_level
                hit_sl = h[j] >= sl_level
                if hit_tp and hit_sl:
                    exit_idx = j
                    if worst_case:
                        exit_price = sl_level
                        reason = 3  # SL_samebar
                    else:
                        exit_price = tp_level
                        reason = 4  # TP_samebar
                    break
                elif hit_sl:
                    exit_idx = j
                    exit_price = sl_level
                    reason = 1  # SL
                    break
                elif hit_tp:
                    exit_idx = j
                    exit_price = tp_level
                    reason = 2  # TP
                    break
                j += 1

            if exit_idx == -1:
                exit_idx = end
                exit_price = c[exit_idx]
                reason = 5  # TIME

            # 做空回報
            exec_entry = entry_price - slippage
            exec_exit = exit_price + slippage
            if exec_entry <= 0.0:
                exec_entry = 1e-12
            if exec_exit <= 0.0:
                exec_exit = 1e-12
            revenue = exec_entry * (1.0 - fee_side)
            cost = exec_exit * (1.0 + fee_side)
            net_ret = (revenue - cost) / exec_entry            
            perbar[exit_idx] += net_ret

            if tcount < max_trades:
                entry_idx_arr[tcount] = entry_idx
                exit_idx_arr[tcount]  = exit_idx
                entry_px_arr[tcount]  = entry_price
                exit_px_arr[tcount]   = exit_price
                net_ret_arr[tcount]   = net_ret
                bars_held_arr[tcount] = exit_idx - entry_idx + 1
                reason_arr[tcount]    = reason
                tcount += 1

            in_pos = False
            entry_idx = -1
            entry_price = 0.0

    equity = np.ones(n, dtype=np.float64)
    for k in range(1, n):
        equity[k] = equity[k-1] * (1.0 + perbar[k])

    return (perbar, equity, entry_idx_arr[:tcount], exit_idx_arr[:tcount],
            entry_px_arr[:tcount], exit_px_arr[:tcount], net_ret_arr[:tcount],
            bars_held_arr[:tcount], reason_arr[:tcount])

if NUMBA_OK:
    @njit(cache=True, fastmath=True, nogil=True)
    def _simulate_short_core_nb(o, h, l, c, entry_sig, tp_pct, sl_pct, max_hold, fee_side=0.0002, slippage=0.0, worst_case=True):
        n = len(c)
        perbar = np.zeros(n, np.float64)
        max_trades = 0
        for ii in range(n-1):
            if entry_sig[ii]:
                max_trades += 1
        entry_idx_arr = np.full(max_trades, -1, np.int64)
        exit_idx_arr  = np.full(max_trades, -1, np.int64)
        entry_px_arr  = np.zeros(max_trades, np.float64)
        exit_px_arr   = np.zeros(max_trades, np.float64)
        net_ret_arr   = np.zeros(max_trades, np.float64)
        bars_held_arr = np.zeros(max_trades, np.int32)
        reason_arr    = np.zeros(max_trades, np.int8)

        tcount = 0
        in_pos = False
        entry_idx = -1
        entry_price = 0.0

        for i in range(n-1):
            if not in_pos:
                if entry_sig[i]:
                    entry_idx = i + 1
                    if entry_idx >= n:
                        break
                    entry_price = o[entry_idx]
                    in_pos = True
            else:
                start = entry_idx
                end = entry_idx + max_hold
                if end >= n:
                    end = n - 1

                tp_level = entry_price * (1.0 - tp_pct)
                sl_level = entry_price * (1.0 + sl_pct)

                exit_idx = -1
                exit_price = 0.0
                reason = 0

                j = start
                while j <= end:
                    hit_tp = l[j] <= tp_level
                    hit_sl = h[j] >= sl_level
                    if hit_tp and hit_sl:
                        exit_idx = j
                        if worst_case:
                            exit_price = sl_level
                            reason = 3
                        else:
                            exit_price = tp_level
                            reason = 4
                        break
                    elif hit_sl:
                        exit_idx = j
                        exit_price = sl_level
                        reason = 1
                        break
                    elif hit_tp:
                        exit_idx = j
                        exit_price = tp_level
                        reason = 2
                        break
                    j += 1

                if exit_idx == -1:
                    exit_idx = end
                    exit_price = c[exit_idx]
                    reason = 5

                exec_entry = entry_price - slippage
                exec_exit = exit_price + slippage
                if exec_entry <= 0.0:
                    exec_entry = 1e-12
                if exec_exit <= 0.0:
                    exec_exit = 1e-12
                revenue = exec_entry * (1.0 - fee_side)
                cost = exec_exit * (1.0 + fee_side)
                net_ret = (revenue - cost) / exec_entry

                
                perbar[exit_idx] += net_ret

                if tcount < max_trades:
                    entry_idx_arr[tcount] = entry_idx
                    exit_idx_arr[tcount]  = exit_idx
                    entry_px_arr[tcount]  = entry_price
                    exit_px_arr[tcount]   = exit_price
                    net_ret_arr[tcount]   = net_ret
                    bars_held_arr[tcount] = exit_idx - entry_idx + 1
                    reason_arr[tcount]    = reason
                    tcount += 1

                in_pos = False
                entry_idx = -1
                entry_price = 0.0

        equity = np.ones(n, np.float64)
        for k in range(1, n):
            equity[k] = equity[k-1] * (1.0 + perbar[k])

        return (perbar, equity, entry_idx_arr[:tcount], exit_idx_arr[:tcount],
                entry_px_arr[:tcount], exit_px_arr[:tcount], net_ret_arr[:tcount],
                bars_held_arr[:tcount], reason_arr[:tcount])

    @njit(cache=True, fastmath=True, nogil=True)
    def _simulate_laguerre_tema_nb(o, h, l, c,
    entry_sig,
    atr_sltp_arr, atr_trail_arr, atr_act_arr, logic_exit_arr,
    sl_coef, tp_coef, ts_dist_coef, ts_act_coef,
    slippage,
    max_hold, fee_side=0.0002):
        """
        Strategy 0.60842 模擬核心
        Strictly aligned with Pine Script 'strategy.exit' behavior.
        """
        n = len(c)
        perbar = np.zeros(n, np.float64)
        
        # Estimate max trades
        max_trades = int(np.sum(entry_sig)) + 1000
        entry_idx_arr = np.full(max_trades, -1, np.int64)
        exit_idx_arr  = np.full(max_trades, -1, np.int64)
        entry_px_arr  = np.zeros(max_trades, np.float64)
        exit_px_arr   = np.zeros(max_trades, np.float64)
        net_ret_arr   = np.zeros(max_trades, np.float64)
        bars_held_arr = np.zeros(max_trades, np.int32)
        reason_arr    = np.zeros(max_trades, np.int8) # 1:SL, 2:TP, 3:Trail, 4:Logic, 5:Time

        tcount = 0
        in_pos = False
        entry_idx = -1
        entry_price = 0.0
        
        # Position States
        curr_sl = 0.0
        curr_tp = 0.0
        curr_trail_act_price = 0.0 # Activation Price level
        curr_trail_offset_val = 0.0 # Offset value (in price)
        is_trailing_active = False
        
        for i in range(n-1):
            if not in_pos:
                # Pine: Entry logic happens on Close, execution on Next Open
                if entry_sig[i]:
                    entry_idx = i + 1
                    if entry_idx >= n: break
                    entry_price = o[entry_idx] # Entry at Next Bar Open
                    
                    # Snapshot ATRs at the moment of signal (bar i)
                    # Pine: entry_atr_* := atr_* (assigned when strategy.entry is called)
                    base_atr_sltp = atr_sltp_arr[i]
                    base_atr_trail = atr_trail_arr[i]
                    base_atr_act = atr_act_arr[i]
                    
                    # Calculate Levels
                    # SL = Entry - Coef * ATR
                    curr_sl = entry_price - (sl_coef * base_atr_sltp)
                    # TP = Entry + Coef * ATR
                    curr_tp = entry_price + (tp_coef * base_atr_sltp)
                    
                    # Trailing Setup
                    # Activation = Entry + Coef * ATR
                    curr_trail_act_price = entry_price + (ts_act_coef * base_atr_act)
                    # Offset = Coef * ATR
                    curr_trail_offset_val = ts_dist_coef * base_atr_trail
                    
                    is_trailing_active = False
                    
                    in_pos = True
            else:
                # In Position: Evaluate bar 'i' (which is >= entry_idx)
                # This bar is 'Current'
                
                # --- 1. Update Trailing Logic (High-based) ---
                bar_high = h[i]
                bar_low = l[i]
                
                if not is_trailing_active:
                    # Check activation
                    if bar_high >= curr_trail_act_price:
                        is_trailing_active = True
                        # Pine: Once activated, SL = High - Offset
                        # Immediate update based on the high that activated it
                        new_sl = bar_high - curr_trail_offset_val
                        if new_sl > curr_sl:
                            curr_sl = new_sl
                else:
                    # Already active, update if new high pushes SL up
                    new_sl = bar_high - curr_trail_offset_val
                    if new_sl > curr_sl:
                        curr_sl = new_sl
                
                # --- 2. Check Exits (SL / TP / Logic / Time) ---
                # Pine 'strategy.exit' process intrabar.
                # Priority: 
                # If Low <= SL -> Hit SL.
                # If High >= TP -> Hit TP.
                # If both hit in same bar -> 'Worst Case' (usually SL first unless gap) or check open.
                # Logic exit happens at Close of bar i -> Execution at Open of i+1? 
                # Wait, "strategy.close" in Pine executes on the *next* tick/bar open.
                # But here we simulate daily/H1 bars usually. 
                # If logic_exit_arr[i] is true (signal at close of i), we exit at Close[i] 
                # to approximate "market on close" or "open of i+1".
                # Standard practice in this panel: Logic exit uses Close[i].
                
                exit_type = 0 
                exit_p = 0.0
                
                # Check Hard Stops first (Intrabar)
                hit_sl = (bar_low <= curr_sl)
                hit_tp = (bar_high >= curr_tp)
                
                if hit_sl and hit_tp:
                    # Both hit. Conservative: SL.
                    exit_type = 3 if is_trailing_active else 1
                    exit_p = curr_sl
                elif hit_sl:
                    exit_type = 3 if is_trailing_active else 1
                    exit_p = curr_sl
                elif hit_tp:
                    exit_type = 2
                    exit_p = curr_tp
                
                # If survived Intrabar stops, check Logic Exit (Signal at Close)
                if exit_type == 0:
                    # Check Logic Signal
                    # logic_exit_arr[i] means signal generated at Close of bar i.
                    # We exit at Close[i] (or Open[i+1], effectively Close[i] for PnL).
                    if logic_exit_arr[i] > 0.5:
                        exit_type = 4
                        exit_p = c[i]
                    
                    # Check Time Exit
                    elif (i - entry_idx + 1) >= max_hold:
                        exit_type = 5
                        exit_p = c[i]
                
                # --- 3. Execute Exit ---
                if exit_type > 0:
                    exec_entry = entry_price + slippage
                    exec_exit = exit_p - slippage
                    if exec_entry <= 0.0:
                        exec_entry = 1e-12
                    if exec_exit <= 0.0:
                        exec_exit = 1e-12
                    revenue = exec_exit * (1.0 - fee_side)
                    cost = exec_entry * (1.0 + fee_side)
                    net_ret = (revenue - cost) / exec_entry

                    perbar[i] += net_ret
                    
                    if tcount < max_trades:
                        entry_idx_arr[tcount] = entry_idx
                        exit_idx_arr[tcount] = i
                        entry_px_arr[tcount] = entry_price
                        exit_px_arr[tcount] = exit_p
                        net_ret_arr[tcount] = net_ret
                        bars_held_arr[tcount] = i - entry_idx + 1
                        reason_arr[tcount] = exit_type
                        tcount += 1
                    
                    in_pos = False
                    entry_idx = -1

        equity = np.ones(n, np.float64)
        for k in range(1, n):
            equity[k] = equity[k-1] * (1.0 + perbar[k])
            
        return (perbar,
                equity,
                entry_idx_arr[:tcount],
                exit_idx_arr[:tcount],
                entry_px_arr[:tcount],
                exit_px_arr[:tcount],
                net_ret_arr[:tcount],
                bars_held_arr[:tcount],
                reason_arr[:tcount])

    @njit(cache=True, fastmath=True, nogil=True)
    def _simulate_tema_rsi_nb(o, h, l, c,
entry_sig,
act_pct_arr, trail_offset_arr, tp_pct_arr, sl_pct_arr, stake_pct_arr,
entry_reason_input_arr, # New Argument
slippage, max_hold, fee_side=0.0002):
        """
        Specialized Simulator for TEMA_RSI Strategy ('帥')
        Logic:
          - Fixed SL % at Entry
          - Fixed TP % at Entry (Can be disabled if -100, here handled as normal)
          - Trailing Stop:
             1. Activates when High >= Entry * (1 + activation_pct)
             2. Once activated, StopPrice = Max(High - Offset, Prev_StopPrice)
             3. Exit if Low <= StopPrice
          - Stake Percentage (Pine: strategy.equity * stake / close) -> Scale returns by stake
        """
        n = len(c)
        perbar = np.zeros(n, np.float64)
        
        max_trades = int(np.sum(entry_sig)) + 1000
        entry_idx_arr = np.full(max_trades, -1, np.int64)
        exit_idx_arr  = np.full(max_trades, -1, np.int64)
        entry_px_arr  = np.zeros(max_trades, np.float64)
        exit_px_arr   = np.zeros(max_trades, np.float64)
        net_ret_arr   = np.zeros(max_trades, np.float64)
        bars_held_arr = np.zeros(max_trades, np.int32)
        reason_arr    = np.zeros(max_trades, np.int8) # 1:SL, 2:TP, 3:Trailing, 5:Time
        entry_reason_record = np.zeros(max_trades, np.int8) # Record Entry Reason

        tcount = 0
        in_pos = False
        entry_idx = -1
        entry_price = 0.0
        
        # Position State
        curr_fixed_sl = 0.0
        curr_fixed_tp = 0.0
        curr_stake = 1.0 # default 100%
        
        curr_trail_activation_price = 0.0
        curr_trail_offset = 0.0
        curr_trail_stop_price = 0.0 # The dynamic stop line
        is_trail_active = False
        
        for i in range(n-1):
            if not in_pos:
                if entry_sig[i]:
                    entry_idx = i + 1
                    if entry_idx >= n: break
                    entry_price = o[entry_idx]
                    
                    # Setup Strategy Parameters from arrays (at signal time i)
                    # Note: Pine executes entry on i+1 open.
                    
                    # Fixed Risk
                    sl_pct = sl_pct_arr[i]
                    tp_pct = tp_pct_arr[i]
                    curr_stake = stake_pct_arr[i]
                    
                    curr_fixed_sl = entry_price * (1.0 - sl_pct)
                    
                    if tp_pct <= -0.99: # Disable TP if approx -100%
                        curr_fixed_tp = 1e12 # Extremely high number
                    else:
                        curr_fixed_tp = entry_price * (1.0 + tp_pct)
                        
                    # Trailing Setup
                    act_pct = act_pct_arr[i]
                    curr_trail_offset = trail_offset_arr[i]
                    
                    curr_trail_activation_price = entry_price * (1.0 + act_pct)
                    curr_trail_stop_price = 0.0 # Not valid yet
                    is_trail_active = False
                    
                    # Record Entry Reason
                    if tcount < max_trades:
                        entry_reason_record[tcount] = int(entry_reason_input_arr[i])

                    in_pos = True
            else:
                # In Position processing
                current_bar_h = h[i]
                current_bar_l = l[i]
                
                # 1. Update Trailing Logic
                if not is_trail_active:
                    if current_bar_h >= curr_trail_activation_price:
                        is_trail_active = True
                        # Init trailing stop based on the High that activated it?
                        # Pine: if high >= trailActivationPrice ... trailingStopPrice := math.max(high - offset, nz(prev))
                        # When first activated, prev is nan (or 0), so it's high - offset.
                        curr_trail_stop_price = current_bar_h - curr_trail_offset
                else:
                    # Update if high pushes it up (Pine math.max logic)
                    potential_new_stop = current_bar_h - curr_trail_offset
                    if potential_new_stop > curr_trail_stop_price:
                        curr_trail_stop_price = potential_new_stop
                
                # 2. Check Exits (Intrabar)
                # Priorities: 
                # Usually SL is checked first for safety, then TP.
                # Trailing Stop is checked if active.
                
                exit_type = 0
                exit_p = 0.0
                
                # Determine Effective Stop (Pine strategy.exit logic: hits the highest stop line)
                effective_sl = curr_fixed_sl
                effective_sl_type = 1 # 1: Fixed SL
                
                if is_trail_active and curr_trail_stop_price > effective_sl:
                    effective_sl = curr_trail_stop_price
                    effective_sl_type = 3 # 3: Trailing SL

                # Check SL (Fixed or Trailing) -> 優先檢查止損 (含追蹤)
                if current_bar_l <= effective_sl:
                    exit_type = effective_sl_type
                    exit_p = effective_sl
                
                # Check Fixed TP (if not already SLed)
                if exit_type == 0:
                    if current_bar_h >= curr_fixed_tp:
                        exit_type = 2
                        exit_p = curr_fixed_tp
                
                # Check Time Exit
                if exit_type == 0:
                     if (i - entry_idx + 1) >= max_hold:
                        exit_type = 5
                        exit_p = c[i]

                # Execute Exit
                if exit_type > 0:
                    exec_entry = entry_price + slippage
                    exec_exit = exit_p - slippage
                    if exec_entry <= 0.0:
                        exec_entry = 1e-12
                    if exec_exit <= 0.0:
                        exec_exit = 1e-12
                    revenue = exec_exit * (1.0 - fee_side)
                    cost = exec_entry * (1.0 + fee_side)
                    raw_net_ret = (revenue - cost) / exec_entry
                    net_ret = raw_net_ret * curr_stake

                    
                    perbar[i] += net_ret
                    
                    if tcount < max_trades:
                        entry_idx_arr[tcount] = entry_idx
                        exit_idx_arr[tcount] = i
                        entry_px_arr[tcount] = entry_price
                        exit_px_arr[tcount] = exit_p
                        net_ret_arr[tcount] = net_ret
                        bars_held_arr[tcount] = i - entry_idx + 1
                        reason_arr[tcount] = exit_type
                        tcount += 1
                        
                    in_pos = False
                    entry_idx = -1

        equity = np.ones(n, np.float64)
        for k in range(1, n):
            equity[k] = equity[k-1] * (1.0 + perbar[k])
            
        return (perbar,
                equity,
                entry_idx_arr[:tcount],
                exit_idx_arr[:tcount],
                entry_px_arr[:tcount],
                exit_px_arr[:tcount],
                net_ret_arr[:tcount],
                bars_held_arr[:tcount],
                reason_arr[:tcount],
                entry_reason_record[:tcount])


def run_backtest(df: pd.DataFrame,
family: str,
family_params: Dict,
tp_pct: float,
sl_pct: float,
max_hold: int,
fee_side: float = 0.0002,
slippage: float = 0.0,
worst_case: bool = True,
reverse_mode: bool = False) -> Dict:
    """單一組合回測，回傳績效摘要 + 明細。"""
    # 從 params 檢查是否 override reverse_mode (特別是 OB_FVG 或 SMC)
    if not reverse_mode and (family == "OB_FVG" or family == "SMC"):
        reverse_mode = bool(family_params.get("reverse", False))

    o = df["open"].values.astype(np.float64)
    h = df["high"].values.astype(np.float64)
    l = df["low"].values.astype(np.float64)
    c = df["close"].values.astype(np.float64)
    v = df["volume"].values.astype(np.float64)

    # 修正：注入時間戳以供跨週期計算
    family_params = family_params.copy()
    family_params["_ts"] = df["ts"].values

    # 通用訊號產生（自動判斷是否回傳 tuple）
    sig_result = signal_from_family(family, o, h, l, c, v, family_params)
    zone_arrays = None
    if isinstance(sig_result, tuple):
        sig, zone_arrays = sig_result
    else:
        sig = sig_result

    # 判斷是否需要走 per-entry 模擬 (OB_FVG 區間模式 或 SMC)
    is_ob_range = (family == "OB_FVG" and family_params.get("ob_range_based", False))
    is_smc = (family == "SMC")

    if (is_ob_range or is_smc) and zone_arrays is not None:
        # 區間模式：使用 per-entry 模擬
        zone_ob_top, zone_ob_bottom, zone_fvg_top, zone_fvg_bottom, ob_idx_arr, highest_arr, lowest_arr = zone_arrays
        if reverse_mode:
            sim_func = simulate_short_core_per_entry
        else:
            sim_func = simulate_long_core_per_entry
            
        (perbar, equity,
         e_idx, x_idx, e_px, x_px, tr_ret, bars_held, reasons) = sim_func(
             o, h, l, c, sig.astype(np.bool_),
             zone_ob_top, zone_ob_bottom, zone_fvg_top, zone_fvg_bottom,
             ob_idx_arr, highest_arr, lowest_arr,
             float(tp_pct), float(sl_pct), int(max_hold),
             fee_side=float(fee_side), slippage=float(slippage), worst_case=bool(worst_case)
     )


    elif family == "LaguerreRSI_TEMA" and zone_arrays is not None:
        # 策略模式：使用專用模擬核心

        # Unpack ATR arrays
        # (atr_sltp, atr_sltp, atr_trail, atr_act, logic_exit_arr, ...)
        atr_sltp_arr, _, atr_trail_arr, atr_act_arr, logic_exit_arr, _, _ = zone_arrays
        
        # 讀取係數
        p_sl_coef = float(family_params.get("sl_coef", 1.1))
        p_tp_coef = float(family_params.get("tp_coef", 1.9))
        p_ts_dist_coef = float(family_params.get("ts_dist_coef", 1.1))
        p_ts_act_coef = float(family_params.get("ts_act_coef", 1.1))
        
        # 呼叫 Numba 核心
        if NUMBA_OK:
            (perbar, equity,
             e_idx, x_idx, e_px, x_px, tr_ret, bars_held, reasons) = _simulate_laguerre_tema_nb(
                 o, h, l, c, sig.astype(np.bool_),
                 atr_sltp_arr, atr_trail_arr, atr_act_arr, logic_exit_arr,
                p_sl_coef, p_tp_coef, p_ts_dist_coef, p_ts_act_coef,
             float(slippage), int(max_hold), float(fee_side)
         )

        else:
            # Fallback to empty if no Numba (Or implement Python version if needed)
             (perbar, equity,
             e_idx, x_idx, e_px, x_px, tr_ret, bars_held, reasons) = (np.zeros_like(c), np.ones_like(c), [], [], [], [], [], [], [])

    elif family == "TEMA_RSI" and zone_arrays is not None:
         # 策略模式：TEMA_RSI
        # Unpack arrays: (act_pct, offset, tp, sl, stake, reason_input, ...)
        p_act_arr, p_off_arr, p_tp_arr, p_sl_arr, p_stake_arr, p_reason_in, _ = zone_arrays
        
        # 預設 entry reasons
        entry_reasons_arr = []

        if NUMBA_OK:
            # 修正：確保正確接收 entry_reasons_arr 回傳
            (perbar, equity,
             e_idx, x_idx, e_px, x_px, tr_ret, bars_held, reasons, entry_reasons_arr) = _simulate_tema_rsi_nb(
                o, h, l, c, sig.astype(np.bool_),
                p_act_arr, p_off_arr, p_tp_arr, p_sl_arr, p_stake_arr,
            p_reason_in,
            float(slippage), int(max_hold), float(fee_side)
         )

        else:
             # Fallback: 補齊回傳數量
             (perbar, equity,
             e_idx, x_idx, e_px, x_px, tr_ret, bars_held, reasons, entry_reasons_arr) = (np.zeros_like(c), np.ones_like(c), [], [], [], [], [], [], [], [])

    else:
        # 一般模式：使用固定 TP/SL 模擬
        if reverse_mode:
            sim_func = simulate_short_core
        else:
            sim_func = simulate_long_core
            
        (perbar, equity,
         e_idx, x_idx, e_px, x_px, tr_ret, bars_held, reasons) = sim_func(
            o, h, l, c, sig.astype(np.bool_),
        float(tp_pct), float(sl_pct), int(max_hold),
        fee_side=float(fee_side), slippage=float(slippage), worst_case=bool(worst_case)

         )


    # -------------------- 1m 精準撮合（可選） -------------------- #
    micro_ctx = None
    entry_1m_idx_arr = None
    exit_1m_idx_arr = None

    use_1m_fill = False
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        _ctx = get_script_run_ctx()
    except Exception:
        _ctx = None

    if _ctx is not None:
        use_1m_fill = bool(st.session_state.get("use_1m_fill", False))
        micro_ctx = st.session_state.get("microfill_ctx", None)
        if use_1m_fill and micro_ctx is None:
            raise ValueError("已勾選 1m 精準撮合，但尚未成功載入 1m CSV（microfill_ctx 不存在）。請先在側邊欄上傳 1m CSV。")

    if use_1m_fill:
        # per-entry 模式：每筆交易的 TP/SL 會依區間重新計算（OB_FVG 區間模式 / SMC）
        per_entry_mode = bool((family == "OB_FVG" and bool(family_params.get("ob_range_based", False))) or (family == "SMC"))

        # --- 策略特性：這兩個 family 的出場為路徑相依，不能用固定 TP/SL 模板 --- #
        if family == "LaguerreRSI_TEMA":
            if zone_arrays is None:
                raise ValueError("1m 精準撮合：family=LaguerreRSI_TEMA 需要 signal_from_family 回傳的 zone_arrays（ATR/logic_exit），但 zone_arrays 為 None。")

            atr_sltp_arr, _, atr_trail_arr, atr_act_arr, logic_exit_arr, _, _ = zone_arrays

            p_sl_coef = float(family_params.get("sl_coef", 1.1))
            p_tp_coef = float(family_params.get("tp_coef", 1.9))
            p_ts_dist_coef = float(family_params.get("ts_dist_coef", 1.1))
            p_ts_act_coef = float(family_params.get("ts_act_coef", 1.1))

            (perbar, equity,
             e_idx, x_idx, e_px, x_px, tr_ret, bars_held, reasons,
             entry_1m_idx_arr, exit_1m_idx_arr) = simulate_laguerre_tema_1m_microfill(
                df_htf=df,
                micro_ctx=micro_ctx,
                entry_sig=sig.astype(np.bool_),
                atr_sltp_arr=atr_sltp_arr,
                atr_trail_arr=atr_trail_arr,
                atr_act_arr=atr_act_arr,
                logic_exit_arr=logic_exit_arr,
                sl_coef=p_sl_coef,
                tp_coef=p_tp_coef,
                ts_dist_coef=p_ts_dist_coef,
                ts_act_coef=p_ts_act_coef,
                slippage=float(slippage),
                max_hold=int(max_hold),
                fee_side=float(fee_side),
            )

        elif family == "TEMA_RSI":
            if zone_arrays is None:
                raise ValueError("1m 精準撮合：family=TEMA_RSI 需要 signal_from_family 回傳的 zone_arrays（act/off/tp/sl/stake/reason），但 zone_arrays 為 None。")

            p_act_arr, p_off_arr, p_tp_arr, p_sl_arr, p_stake_arr, p_reason_in, _ = zone_arrays

            (perbar, equity,
             e_idx, x_idx, e_px, x_px, tr_ret, bars_held, reasons, entry_reasons_arr,
             entry_1m_idx_arr, exit_1m_idx_arr) = simulate_tema_rsi_1m_microfill(
                df_htf=df,
                micro_ctx=micro_ctx,
                entry_sig=sig.astype(np.bool_),
                act_pct_arr=p_act_arr,
                trail_offset_arr=p_off_arr,
                tp_pct_arr=p_tp_arr,
                sl_pct_arr=p_sl_arr,
                stake_pct_arr=p_stake_arr,
                entry_reason_input_arr=p_reason_in,
                slippage=float(slippage),
                max_hold=int(max_hold),
                fee_side=float(fee_side),
            )

        else:
            (perbar, equity,
             e_idx, x_idx, e_px, x_px, tr_ret, bars_held, reasons,
             entry_1m_idx_arr, exit_1m_idx_arr) = apply_1m_microfill(
                df_htf=df,
                micro_ctx=micro_ctx,
                entry_idx_arr=e_idx,
                exit_idx_arr=x_idx,
                reverse_mode=bool(reverse_mode),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                max_hold=int(max_hold),
                fee_side=float(fee_side),
                slippage=float(slippage),
                worst_case=bool(worst_case),
                per_entry_mode=per_entry_mode,
                highest_arr=highest_arr if per_entry_mode and "highest_arr" in locals() else None,
                lowest_arr=lowest_arr if per_entry_mode and "lowest_arr" in locals() else None,
            )


    trades = []
    # 時區顯示：Asia/Taipei
    ts = df["ts"].dt.tz_convert("Asia/Taipei")

    # 出場原因代碼：不同 family 的 reason code 定義不同（這裡做正確對照）
    if family == "LaguerreRSI_TEMA":
        reason_map = {1: "SL", 2: "TP", 3: "TRAIL", 4: "LOGIC", 5: "TIME", 0: "NA"}
    elif family == "TEMA_RSI":
        reason_map = {1: "SL", 2: "TP", 3: "TRAIL", 5: "TIME", 0: "NA"}
    else:
        reason_map = {1: "SL", 2: "TP", 3: "SL_samebar", 4: "TP_samebar", 5: "TIME", 0: "NA"}

    # TEMA_RSI Entry Reason Map
    entry_map_tema = {1: "Pullback", 2: "Momentum", 3: "Cross", 4: "RSI_Revert", 0: "Unknown"}

    # 準備聚合統計用的容器
    stats_by_entry = {} # {reason_str: [net_ret, ...]}
    
    # 修正：根據 reverse_mode 決定毛報酬計算公式
    for i in range(len(e_idx)):
        if reverse_mode:
            g_ret = float((e_px[i] - x_px[i]) / e_px[i])
        else:
            g_ret = float((x_px[i] / e_px[i]) - 1.0)
        
        # Determine Entry Reason String
        e_reason_str = "Standard"
        if family == "TEMA_RSI" and 'entry_reasons_arr' in locals() and i < len(entry_reasons_arr):
            e_code = int(entry_reasons_arr[i])
            e_reason_str = entry_map_tema.get(e_code, f"Type_{e_code}")

        # 1m 精準撮合：若有 minute 索引，就用 minute 的真實時間（否則退回主週期 bar open）
        entry_ts_str = str(ts.iloc[int(e_idx[i])])
        exit_ts_str = str(ts.iloc[int(x_idx[i])])
        entry_1m_i = None
        exit_1m_i = None
        if micro_ctx is not None:
            if entry_1m_idx_arr is not None and i < len(entry_1m_idx_arr):
                _k0 = int(entry_1m_idx_arr[i])
                if 0 <= _k0 < micro_ctx.m1_len:
                    entry_1m_i = _k0
                    entry_ts_str = str(pd.to_datetime(int(micro_ctx.ts_1m_ns[_k0]), utc=True).tz_convert("Asia/Taipei"))
            if exit_1m_idx_arr is not None and i < len(exit_1m_idx_arr):
                _k1 = int(exit_1m_idx_arr[i])
                if 0 <= _k1 < micro_ctx.m1_len:
                    exit_1m_i = _k1
                    exit_ts_str = str(pd.to_datetime(int(micro_ctx.ts_1m_ns[_k1]), utc=True).tz_convert("Asia/Taipei"))

        # Collect stats
        if e_reason_str not in stats_by_entry:
            stats_by_entry[e_reason_str] = []
        stats_by_entry[e_reason_str].append(float(tr_ret[i]))

        trades.append({
            "entry_index": int(e_idx[i]),
            "entry_ts": entry_ts_str,
            "entry_price": float(e_px[i]),
            "exit_index": int(x_idx[i]),
            "exit_ts": exit_ts_str,
            "exit_price": float(x_px[i]),
            "gross_return": g_ret,
            "net_return": float(tr_ret[i]),
            "bars_held": int(bars_held[i]),
            "reason": reason_map.get(int(reasons[i]), "NA"),
            "entry_reason": e_reason_str, # 新增欄位
            "tp_pct": float(tp_pct),
            "sl_pct": float(sl_pct),
            "entry_1m_index": entry_1m_i,
            "exit_1m_index": exit_1m_i,
        })
    
    # 計算各進場原因的聚合績效 (Summary string)
    breakdown_summary = []
    for r_key, r_rets in stats_by_entry.items():
        arr = np.array(r_rets)
        cnt = len(arr)
        wins = arr[arr > 0]
        wr = (len(wins) / cnt * 100.0) if cnt > 0 else 0.0
        avg = np.mean(arr) * 100.0 if cnt > 0 else 0.0
        # Simple MaxDD approximation for sub-strategy (not perfect as time isn't sorted per reason, but useful)
        cum = np.cumprod(1 + arr)
        peak = np.maximum.accumulate(cum)
        dd = (cum - peak) / peak
        mdd = np.min(dd) * 100.0 if len(dd) > 0 else 0.0
        
        breakdown_summary.append(f"{r_key}: N={cnt}, WR={wr:.1f}%, Avg={avg:.2f}%, MDD={mdd:.1f}%")
    
    breakdown_str = " | ".join(breakdown_summary)
    if family == "OB_FVG":
        # zone_arrays: (ob_top_arr, ob_bottom_arr, fvg_top_arr, fvg_bottom_arr, ob_idx_arr, highest_arr, lowest_arr)
        zone_ob_top, zone_ob_bottom, zone_fvg_top, zone_fvg_bottom, ob_idx_arr, highest_arr, lowest_arr = zone_arrays

        # 用 lookback 限制倒查 OB 形成 K 棒，避免掃到遠古時代
        _lb = int(family_params.get("lookback", 120))

        for t in trades:
            # base_idx = 訊號觸發那根（entry 的前一根），因為 entry 在下一根 open 進
            base_idx = t["entry_index"] - 1
            if base_idx < 0 or base_idx >= len(c):
                continue

            ob_top_val = zone_ob_top[base_idx]
            ob_bottom_val = zone_ob_bottom[base_idx]
            fvg_top_val = zone_fvg_top[base_idx]
            fvg_bottom_val = zone_fvg_bottom[base_idx]

            is_fvg = np.isfinite(fvg_top_val)
            zone_form_idx = int(ob_idx_arr[base_idx]) if base_idx < len(ob_idx_arr) else -1

            # ---------- 倒查 OB 形成 index ----------
            ob_form_idx = -1
            if np.isfinite(ob_top_val) and np.isfinite(ob_bottom_val):
                search_end = zone_form_idx if zone_form_idx >= 0 else base_idx
                search_start = max(0, search_end - _lb - 5)

                for j in range(search_end, search_start - 1, -1):
                    if (np.isclose(h[j], ob_top_val, rtol=1e-6, atol=1e-8) and
                        np.isclose(l[j], ob_bottom_val, rtol=1e-6, atol=1e-8) and
                        c[j] < o[j]):  # red candle 才是 OB 的母體
                        ob_form_idx = j
                        break

            # ---------- FVG 形成 index ----------
            fvg_form_idx = zone_form_idx if is_fvg else -1

            # ---------- 時間 ----------
            ob_start_ts = str(ts.iloc[ob_form_idx]) if ob_form_idx >= 0 else None
            ob_end_ts = str(ts.iloc[base_idx])  # 被觸發的那根（判斷當下）
            fvg_start_ts = str(ts.iloc[fvg_form_idx]) if fvg_form_idx >= 0 else None
            fvg_end_ts = str(ts.iloc[base_idx]) if fvg_form_idx >= 0 else None

            # ---------- 價格範圍 ----------
            t["ob_top"] = None if not np.isfinite(ob_top_val) else float(ob_top_val)
            t["ob_bottom"] = None if not np.isfinite(ob_bottom_val) else float(ob_bottom_val)
            t["fvg_top"] = None if not np.isfinite(fvg_top_val) else float(fvg_top_val)
            t["fvg_bottom"] = None if not np.isfinite(fvg_bottom_val) else float(fvg_bottom_val)

            # ---------- OB/FVG 起訖資訊 ----------
            t["ob_start_index"] = int(ob_form_idx) if ob_form_idx >= 0 else None
            t["ob_end_index"] = int(base_idx)
            t["ob_start_ts"] = ob_start_ts
            t["ob_end_ts"] = ob_end_ts

            t["fvg_start_index"] = int(fvg_form_idx) if fvg_form_idx >= 0 else None
            t["fvg_end_index"] = int(base_idx) if fvg_form_idx >= 0 else None
            t["fvg_start_ts"] = fvg_start_ts
            t["fvg_end_ts"] = fvg_end_ts

            if is_fvg and np.isfinite(ob_top_val) and np.isfinite(ob_bottom_val):
                t["zone_trigger_type"] = "OB+FVG"
            elif is_fvg:
                t["zone_trigger_type"] = "FVG"
            else:
                if family != "OB_FVG":
                    for t in trades:
                        t["ob_top"] = None
                        t["ob_bottom"] = None
                        t["ob_start_index"] = None
                        t["ob_end_index"] = None
                        t["ob_start_ts"] = None
                        t["ob_end_ts"] = None
                        t["fvg_top"] = None
                        t["fvg_bottom"] = None
                        t["fvg_start_index"] = None
                        t["fvg_end_index"] = None
                        t["fvg_start_ts"] = None
                        t["fvg_end_ts"] = None
                        t["zone_trigger_type"] = None


    # 度量
    bar_sec = infer_bar_seconds(df["ts"])
    bpy = bars_per_year(bar_sec)
    bar_returns = perbar
    cagr = annualized_return_from_equity(equity, bpy)
    maxdd_pct, dd_start, dd_end = rolling_max_drawdown(equity)
    sh = sharpe_ratio(bar_returns, bpy)
    so = sortino_ratio(bar_returns, bpy)
    cal = calmar_ratio(cagr, maxdd_pct)

    trade_returns = tr_ret
    wins = trade_returns[trade_returns > 0]
    losses = trade_returns[trade_returns <= 0]
    win_rate = (len(wins) / len(trade_returns) * 100.0) if len(trade_returns) > 0 else 0.0
    avg_win = wins.mean() if wins.size else 0.0
    avg_loss = losses.mean() if losses.size else 0.0
    payoff = (avg_win / abs(avg_loss)) if avg_loss != 0 else np.nan
    pf = (wins.sum() / abs(losses.sum())) if losses.size else np.nan
    expectancy = trade_returns.mean() if trade_returns.size else 0.0
    avg_hold = np.mean(bars_held) if bars_held.size else 0.0
    time_in_mkt = (np.sum(bars_held) / len(df) * 100.0) if len(df) > 0 else 0.0

    # 修正：序列化前過濾掉 _ts，避免 json.dumps 失敗，並增強 NumPy Scalar 判斷
    safe_params = {k: v for k, v in family_params.items() if not k.startswith("_")}

    result = {
        "family": family,
        "family_params": json.dumps(safe_params, ensure_ascii=False, default=lambda o: o.tolist() if hasattr(o, "tolist") else (o.item() if hasattr(o, "item") and getattr(o, "ndim", 0) == 0 else str(o))),
        "tp_pct": float(tp_pct) * 100.0,
        "sl_pct": float(sl_pct) * 100.0,
        "max_hold": max_hold,
        "fee_side": fee_side,
        "slippage": float(slippage),
        "entries": int(np.sum(sig)),
        "trades": int(len(trades)),
        "win_rate_pct": pct(win_rate/100.0),
        "avg_win_pct": pct(avg_win),
        "avg_loss_pct": pct(avg_loss),
        "payoff": 0.0 if np.isnan(payoff) else round(float(payoff), 4),
        "profit_factor": 0.0 if np.isnan(pf) else round(float(pf), 4),
        "expectancy_pct": pct(expectancy),
        "total_return_pct": pct(equity[-1] - 1.0),
        "cagr_pct": pct(cagr),
        "max_drawdown_pct": round(maxdd_pct, 4),
        "sharpe": round(sh, 4),
        "sortino": round(so, 4),
        "calmar": round(cal, 4),
        "avg_hold_bars": round(float(avg_hold), 2),
        "time_in_market_pct": round(float(time_in_mkt), 2),
        "bars": int(len(df)),
        "start_ts": str(ts.iloc[0]),
        "end_ts": str(ts.iloc[-1]),
        "bpy": round(float(bpy), 2),
        "dd_start_idx": int(dd_start),
        "dd_end_idx": int(dd_end),
        "equity_curve": equity,
        "perbar_returns": bar_returns,
        "trades_detail": trades,
        "stats_breakdown": breakdown_str if 'breakdown_str' in locals() else "",
    }
    return result
def run_backtest_from_entry_sig(df: pd.DataFrame,
entry_sig: np.ndarray,
tp_pct: float,
sl_pct: float,
max_hold: int,
fee_side: float = 0.0002,
slippage: float = 0.0,
worst_case: bool = True,
reverse_mode: bool = False) -> Dict:
    """與 run_backtest 等價，但用外部給的 entry_sig（支援多指標合成）。"""
    o = df["open"].values.astype(np.float64)
    h = df["high"].values.astype(np.float64)
    l = df["low"].values.astype(np.float64)
    c = df["close"].values.astype(np.float64)
    v = df["volume"].values.astype(np.float64)

    if reverse_mode:
        sim_func = simulate_short_core
    else:
        sim_func = simulate_long_core

    (perbar, equity,
     e_idx, x_idx, e_px, x_px, tr_ret, bars_held, reasons) = sim_func(
         o, h, l, c, entry_sig.astype(np.bool_), float(tp_pct), float(sl_pct), int(max_hold),
    fee_side=float(fee_side), slippage=float(slippage), worst_case=bool(worst_case)

    )

    trades = []
    ts = df["ts"].dt.tz_convert("Asia/Taipei")
    reason_map = {1: "SL", 2: "TP", 3: "SL_samebar", 4: "TP_samebar", 5: "TIME"}
    for i in range(len(e_idx)):
        # 修正：根據 reverse_mode 決定毛報酬計算公式
        if reverse_mode:
            g_ret = float((e_px[i] - x_px[i]) / e_px[i])
        else:
            g_ret = float((x_px[i] / e_px[i]) - 1.0)
            
        trades.append({
            "entry_index": int(e_idx[i]),
            "entry_ts": str(ts.iloc[int(e_idx[i])]),
            "entry_price": float(e_px[i]),
            "exit_index": int(x_idx[i]),
            "exit_ts": str(ts.iloc[int(x_idx[i])]),
            "exit_price": float(x_px[i]),
            "gross_return": g_ret,
            "net_return": float(tr_ret[i]),
            "bars_held": int(bars_held[i]),
            "reason": reason_map.get(int(reasons[i]), "NA"),
            "tp_pct": float(tp_pct),
            "sl_pct": float(sl_pct),

        })
    for t in trades:
        t["ob_top"] = None
        t["ob_bottom"] = None
        t["ob_start_index"] = None
        t["ob_end_index"] = None
        t["ob_start_ts"] = None
        t["ob_end_ts"] = None
        t["fvg_top"] = None
        t["fvg_bottom"] = None
        t["fvg_start_index"] = None
        t["fvg_end_index"] = None
        t["fvg_start_ts"] = None
        t["fvg_end_ts"] = None
        t["zone_trigger_type"] = None

    bar_sec = infer_bar_seconds(df["ts"])
    bpy = bars_per_year(bar_sec)
    cagr = annualized_return_from_equity(equity, bpy)
    maxdd_pct, dd_start, dd_end = rolling_max_drawdown(equity)
    sh = sharpe_ratio(perbar, bpy)
    so = sortino_ratio(perbar, bpy)
    cal = calmar_ratio(cagr, maxdd_pct)

    trade_returns = tr_ret
    wins = trade_returns[trade_returns > 0]
    losses = trade_returns[trade_returns <= 0]
    win_rate = (len(wins) / len(trade_returns) * 100.0) if len(trade_returns) > 0 else 0.0
    avg_win = wins.mean() if wins.size else 0.0
    avg_loss = losses.mean() if losses.size else 0.0
    payoff = (avg_win / abs(avg_loss)) if avg_loss != 0 else np.nan
    pf = (wins.sum() / abs(losses.sum())) if losses.size else np.nan
    expectancy = trade_returns.mean() if trade_returns.size else 0.0
    avg_hold = np.mean(bars_held) if bars_held.size else 0.0
    time_in_mkt = (np.sum(bars_held) / len(df) * 100.0) if len(df) > 0 else 0.0

    return {
        "family": "MULTI",
        "family_params": "",  # 由呼叫端填回
        "tp_pct": float(tp_pct) * 100.0,
        "sl_pct": float(sl_pct) * 100.0,
        "max_hold": max_hold,
        "fee_side": fee_side,
    "slippage": float(slippage),
        "entries": int(np.sum(entry_sig)),
        "trades": int(len(trades)),
        "win_rate_pct": pct(win_rate/100.0),
        "avg_win_pct": pct(avg_win),
        "avg_loss_pct": pct(avg_loss),
        "payoff": 0.0 if np.isnan(payoff) else round(float(payoff), 4),
        "profit_factor": 0.0 if np.isnan(pf) else round(float(pf), 4),
        "expectancy_pct": pct(expectancy),
        "total_return_pct": pct(equity[-1] - 1.0),
        "cagr_pct": pct(cagr),
        "max_drawdown_pct": round(maxdd_pct, 4),
        "sharpe": round(sh, 4),
        "sortino": round(so, 4),
        "calmar": round(cal, 4),
        "avg_hold_bars": round(float(avg_hold), 2),
        "time_in_market_pct": round(float(time_in_mkt), 2),
        "bars": int(len(df)),
        "start_ts": str(ts.iloc[0]),
        "end_ts":   str(ts.iloc[-1]),
        "bpy": round(float(bpy), 2),
        "equity_curve": equity,
        "perbar_returns": perbar,
        "trades_detail": trades,
    }


def _simulate_long_core_torch_batch(o_t, h_t, l_t, c_t,
                                    entry_sigs_t, tp_pcts_t, sl_pcts_t,
                                    max_hold: int, fee_side: float, worst_case: bool,
                                    bpy: float, rf_annual: float = 0.0, **kwargs):
    """
    GPU 向量化回測核心（批次同時模擬多組合）。
    新增功能：
      - 會回傳 trade_records（list），每筆 trade 包含 entry/exit 時的指標快照（若呼叫時透過 kwargs 傳入 'indicators'）。
    使用方式（可選的 kwargs）:
      indicators: dict{name -> Tensor}，Tensor 可為 [T] 或 [B, T]。若是 [T]，則視為所有組合共用同一條序列。
    回傳（主要欄位）:
      - total_return_pct, cagr_pct, ...（原本欄位）
      - trade_records: list of lists (for each batch element B, a list of trade dicts)
    """
    assert entry_sigs_t.dim() == 2
    device = o_t.device
    B, T = entry_sigs_t.shape[0], entry_sigs_t.shape[1]

    o_t = o_t.float(); h_t = h_t.float(); l_t = l_t.float(); c_t = c_t.float()
    tp_pcts_t = tp_pcts_t.float(); sl_pcts_t = sl_pcts_t.float()

    # optional indicators dict from kwargs
    indicators = kwargs.get("indicators", None)

    active = torch.zeros(B, dtype=torch.bool, device=device)
    entry_price = torch.zeros(B, dtype=torch.float32, device=device)
    bars_in_pos = torch.zeros(B, dtype=torch.int32, device=device)
    entry_bar = torch.full((B,), -1, dtype=torch.int32, device=device)  # record entry bar index

    equity = torch.ones(B, dtype=torch.float32, device=device)
    peak = equity.clone()
    max_dd = torch.zeros(B, dtype=torch.float32, device=device)  # keep most negative

    # Welford for per-bar returns (zeros except exit bars)
    count = torch.zeros(B, dtype=torch.int32, device=device)
    mean = torch.zeros(B, dtype=torch.float32, device=device)
    m2 = torch.zeros(B, dtype=torch.float32, device=device)

    # Downside-only Welford
    count_dn = torch.zeros(B, dtype=torch.int32, device=device)
    mean_dn = torch.zeros(B, dtype=torch.float32, device=device)
    m2_dn = torch.zeros(B, dtype=torch.float32, device=device)

    # trade aggregates
    trade_count = torch.zeros(B, dtype=torch.int32, device=device)
    wins_count = torch.zeros(B, dtype=torch.int32, device=device)
    wins_sum = torch.zeros(B, dtype=torch.float32, device=device)
    losses_sum_abs = torch.zeros(B, dtype=torch.float32, device=device)
    expectancy_sum = torch.zeros(B, dtype=torch.float32, device=device)
    hold_sum = torch.zeros(B, dtype=torch.float32, device=device)
    time_in_mkt_sum = torch.zeros(B, dtype=torch.float32, device=device)
    entries_count = torch.zeros(B, dtype=torch.int32, device=device)

    # per-benchmark risk-free per bar
    rf_per_bar = (1.0 + rf_annual) ** (1.0 / bpy) - 1.0

    # place to collect trade detail records (Python lists)
    trade_records = [[] for _ in range(B)]

    for t in range(T-1):
        # entries happen at next open o_t[t+1]
        new_entries = (~active) & entry_sigs_t[:, t]
        if new_entries.any():
            entry_price = torch.where(new_entries, o_t[t+1], entry_price)
            active = active | new_entries
            bars_in_pos = torch.where(new_entries, torch.zeros_like(bars_in_pos), bars_in_pos)
            entries_count = entries_count + new_entries.to(torch.int32)
            entry_bar = torch.where(new_entries, torch.full_like(entry_bar, t+1), entry_bar)

        r = torch.zeros(B, dtype=torch.float32, device=device)

        if active.any():
            bars_in_pos = torch.where(active, bars_in_pos + 1, bars_in_pos)

            tp_level = entry_price * (1.0 + tp_pcts_t)
            sl_level = entry_price * (1.0 - sl_pcts_t)

            # check price hits at this bar (using high/low of bar t)
            hit_tp = (h_t[t] >= tp_level) & active
            hit_sl = (l_t[t] <= sl_level) & active
            both = hit_tp & hit_sl
            timeout = (bars_in_pos >= max_hold) & active
            exit_now = hit_tp | hit_sl | timeout

            # exit_price precedence
            exit_price = torch.where(hit_sl, sl_level, torch.where(hit_tp, tp_level, c_t[t]))
            if both.any():
                # if both triggered, choose worst/ best depending on worst_case flag
                chosen = sl_level if worst_case else tp_level
                exit_price = torch.where(both, chosen, exit_price)

            # net return accounting fees
            net_ret = ((exit_price * (1.0 - fee_side)) / (entry_price * (1.0 + fee_side))) - 1.0
            r = torch.where(exit_now, net_ret, r)

            closed = exit_now
            if closed.any():
                trade_count = trade_count + closed.to(torch.int32)
                is_win = (net_ret > 0) & closed
                wins_count = wins_count + is_win.to(torch.int32)
                wins_sum = wins_sum + torch.where(is_win, net_ret, torch.zeros_like(net_ret))
                is_loss = (~is_win) & closed
                losses_sum_abs = losses_sum_abs + torch.where(is_loss, (-net_ret).clamp_min(0.0), torch.zeros_like(net_ret))
                expectancy_sum = expectancy_sum + torch.where(closed, net_ret, torch.zeros_like(net_ret))
                hold_sum = hold_sum + torch.where(closed, bars_in_pos.to(torch.float32), torch.zeros_like(net_ret))
                time_in_mkt_sum = time_in_mkt_sum + torch.where(closed, bars_in_pos.to(torch.float32), torch.zeros_like(net_ret))

                # build trade detail records for closed trades
                closed_idx = torch.nonzero(closed, as_tuple=False).squeeze(1).detach().cpu().numpy().tolist()
                entry_bar_np = entry_bar.detach().cpu().numpy()
                exit_bar_np = (t * torch.ones_like(entry_bar, dtype=torch.int32)).detach().cpu().numpy()  # exit at current bar index
                entry_price_np = entry_price.detach().cpu().numpy()
                exit_price_np = exit_price.detach().cpu().numpy()
                net_ret_np = net_ret.detach().cpu().numpy()
                tp_flags = hit_tp.detach().cpu().numpy()
                sl_flags = hit_sl.detach().cpu().numpy()
                timeout_flags = timeout.detach().cpu().numpy()

                # if indicators provided, convert them to numpy now for indexing
                indicators_np = {}
                if indicators:
                    for k, v in indicators.items():
                        if isinstance(v, torch.Tensor):
                            indicators_np[k] = v.detach().cpu().numpy()
                        else:
                            indicators_np[k] = np.asarray(v)

                for idx in closed_idx:
                    ebar = int(entry_bar_np[idx]) if entry_bar_np[idx] >= 0 else None
                    xbar = int(exit_bar_np[idx])
                    trec = {
                        "batch_idx": int(idx),
                        "entry_bar": ebar,
                        "exit_bar": xbar,
                        "entry_price": float(entry_price_np[idx]) if ebar is not None else None,
                        "exit_price": float(exit_price_np[idx]),
                        "net_return": float(net_ret_np[idx]),
                        "hit_tp": bool(tp_flags[idx]),
                        "hit_sl": bool(sl_flags[idx]),
                        "timeout": bool(timeout_flags[idx]),
                        "hold_bars": float(bars_in_pos.detach().cpu().numpy()[idx])
                    }
                    # attach indicator snapshot: for each indicator, try to get value at entry_bar (prefer), else at exit bar
                    if indicators:
                        for iname, arr in indicators_np.items():
                            val = None
                            try:
                                if arr.ndim == 1:
                                    # shared series
                                    if ebar is not None:
                                        val = float(arr[ebar])
                                    else:
                                        val = float(arr[xbar])
                                elif arr.ndim == 2:
                                    # per-batch series
                                    val = float(arr[idx, ebar]) if ebar is not None else float(arr[idx, xbar])
                                else:
                                    val = None
                            except Exception:
                                val = None
                            trec[f"ind_{iname}"] = val
                    # ---- OB/FVG snapshot (only if provided in indicators) ----
                    try:
                        base_idx = (ebar - 1) if ebar is not None else None
                        if base_idx is not None and base_idx >= 0 and indicators_np:
                            def _pick(arr, bidx, batch_i):
                                if arr is None:
                                    return None
                                if getattr(arr, "ndim", 1) == 1:
                                    return float(arr[bidx]) if np.isfinite(arr[bidx]) else None
                                if getattr(arr, "ndim", 1) == 2:
                                    return float(arr[batch_i, bidx]) if np.isfinite(arr[batch_i, bidx]) else None
                                return None

                            z_ob_top = indicators_np.get("zone_ob_top")
                            z_ob_bottom = indicators_np.get("zone_ob_bottom")
                            z_fvg_top = indicators_np.get("zone_fvg_top")
                            z_fvg_bottom = indicators_np.get("zone_fvg_bottom")
                            z_form = indicators_np.get("ob_idx_arr")

                            ob_top_val = _pick(z_ob_top, base_idx, idx)
                            ob_bottom_val = _pick(z_ob_bottom, base_idx, idx)
                            fvg_top_val = _pick(z_fvg_top, base_idx, idx)
                            fvg_bottom_val = _pick(z_fvg_bottom, base_idx, idx)

                            is_fvg = fvg_top_val is not None
                            form_idx_val = None
                            if z_form is not None:
                                try:
                                    form_idx_val = int(z_form[base_idx]) if z_form.ndim == 1 else int(z_form[idx, base_idx])
                                except Exception:
                                    form_idx_val = None

                            trec["ob_top"] = ob_top_val
                            trec["ob_bottom"] = ob_bottom_val
                            trec["fvg_top"] = fvg_top_val
                            trec["fvg_bottom"] = fvg_bottom_val

                            trec["ob_start_index"] = form_idx_val if (form_idx_val is not None and not is_fvg) else None
                            trec["ob_end_index"] = base_idx
                            trec["ob_start_ts"] = None
                            trec["ob_end_ts"] = None

                            trec["fvg_start_index"] = form_idx_val if (form_idx_val is not None and is_fvg) else None
                            trec["fvg_end_index"] = base_idx if is_fvg else None
                            trec["fvg_start_ts"] = None
                            trec["fvg_end_ts"] = None

                            if is_fvg and ob_top_val is not None and ob_bottom_val is not None:
                                trec["zone_trigger_type"] = "OB+FVG"
                            elif is_fvg:
                                trec["zone_trigger_type"] = "FVG"
                            else:
                                trec["zone_trigger_type"] = "OB"
                    except Exception:
                        pass

                    trade_records[idx].append(trec)

                # reset states for closed trades
                entry_price = torch.where(closed, torch.zeros_like(entry_price), entry_price)
                active = active & (~closed)
                bars_in_pos = torch.where(closed, torch.zeros_like(bars_in_pos), bars_in_pos)
                entry_bar = torch.where(closed, torch.full_like(entry_bar, -1), entry_bar)

        equity = equity * (1.0 + r)
        peak = torch.maximum(peak, equity)
        dd = (equity / peak) - 1.0
        max_dd = torch.minimum(max_dd, dd)

        # update online stats
        count = count + 1
        delta = r - mean
        mean = mean + (delta / count.clamp_min(1))
        delta2 = r - mean
        m2 = m2 + delta * delta2

        neg = (r < 0)
        count_dn = count_dn + neg.to(torch.int32)
        prev_mean_dn = mean_dn
        mean_dn = torch.where(neg, mean_dn + (r - mean_dn) / count_dn.clamp_min(1), mean_dn)
        m2_dn = torch.where(neg, m2_dn + (r - prev_mean_dn) * (r - mean_dn), m2_dn)

    # to numpy and final stats
    equity_np = equity.detach().cpu().numpy()
    total_return = equity_np - 1.0
    years = float(T) / float(bpy)
    cagr = (1.0 + total_return) ** (1.0 / years) - 1.0 if years > 0 else np.zeros_like(total_return)

    cnt = count.detach().cpu().numpy().astype(np.float64)
    mu = mean.detach().cpu().numpy()
    sd = np.sqrt(np.maximum(m2.detach().cpu().numpy() / np.clip(cnt - 1.0, 1.0, None), 0.0))
    sharpe = np.where(sd > 0, ((mu - rf_per_bar) / sd) * np.sqrt(bpy), 0.0)

    cnt_dn = count_dn.detach().cpu().numpy().astype(np.float64)
    sd_dn = np.sqrt(np.maximum(m2_dn.detach().cpu().numpy() / np.clip(cnt_dn - 1.0, 1.0, None), 0.0))
    sortino = np.where(sd_dn > 0, ((mu - rf_per_bar) / sd_dn) * np.sqrt(bpy), 0.0)

    mdd_pct = np.abs(max_dd.detach().cpu().numpy()) * 100.0
    calmar = np.where(mdd_pct > 0.0, cagr / (mdd_pct / 100.0), 0.0)

    trades = trade_count.detach().cpu().numpy().astype(int)
    entries = entries_count.detach().cpu().numpy().astype(int)
    wins_cnt = wins_count.detach().cpu().numpy().astype(int)
    wins_sum_np = wins_sum.detach().cpu().numpy()
    losses_sum_abs_np = losses_sum_abs.detach().cpu().numpy()
    expectancy = np.divide(expectancy_sum.detach().cpu().numpy(), np.clip(trades, 1, None))
    avg_hold = np.divide(hold_sum.detach().cpu().numpy(), np.clip(trades, 1, None))
    time_in_mkt_pct = (np.divide(time_in_mkt_sum.detach().cpu().numpy(), float(T)) * 100.0)

    win_rate = np.where(trades > 0, wins_cnt / trades * 100.0, 0.0)
    avg_win = np.where(wins_cnt > 0, wins_sum_np / np.clip(wins_cnt, 1, None), 0.0)
    avg_loss = np.where((trades - wins_cnt) > 0, -losses_sum_abs_np / np.clip(trades - wins_cnt, 1, None), 0.0)
    payoff = np.where(avg_loss != 0, avg_win / np.abs(avg_loss), 0.0)
    profit_factor = np.where(losses_sum_abs_np > 0, wins_sum_np / losses_sum_abs_np, np.nan)

    return {
        "total_return_pct": total_return * 100.0,
        "cagr_pct": cagr * 100.0,
        "max_drawdown_pct": mdd_pct,
        "sharpe": sharpe,
        "sortino": sortino,
        "calmar": calmar,
        "trades": trades,
        "entries": entries,
        "win_rate_pct": win_rate,
        "avg_win_pct": avg_win * 100.0,
        "avg_loss_pct": avg_loss * 100.0,
        "payoff": payoff,
        "profit_factor": np.nan_to_num(profit_factor, nan=0.0),
        "expectancy_pct": expectancy * 100.0,
        "avg_hold_bars": avg_hold,
        "time_in_market_pct": time_in_mkt_pct,
        "equity_final": equity_np,
        "trade_records": trade_records,
    }



def _combine_signals_logic(signals: List[np.ndarray], logic: str) -> np.ndarray:
    if isinstance(signals, tuple):
        signals = list(signals)
    signals = [s[0] if isinstance(s, tuple) else s for s in signals]
    out = signals[0].copy()
    if logic.upper() == "AND":
        for s in signals[1:]:
            out &= s
    else:
        for s in signals[1:]:
            out |= s
    out[:2] = False
    return out


def recompute_best_detail(df: pd.DataFrame, best: Dict) -> Dict:
    """GPU 掃描可能沒有明細；這裡回頭用 CPU 產生最佳組合的淨值與交易明細。"""
    tp = float(best["tp_pct"]) / 100.0
    sl = float(best["sl_pct"]) / 100.0
    max_hold = int(best["max_hold"]); fee_side = float(best["fee_side"])
    slippage = float(best.get("slippage", 0.0))
    worst_case = bool(best.get("worst_case", True))
    fam = str(best["family"])
    
    # 判斷是否為反向模式 (從 best 的 params 裡讀取)
    try:
        _prm_check = json.loads(best["family_params"])
        reverse_mode = bool(_prm_check.get("reverse", False))
        if fam.startswith("MULTI") and not reverse_mode:
             # 多指標時，reverse 可能藏在 families 的 OB_FVG 裡
             if "families" in _prm_check:
                 for fkey, fval in _prm_check["families"].items():
                     if fkey == "OB_FVG" and fval.get("reverse", False):
                         reverse_mode = True
                         break
    except:
        reverse_mode = False

    if fam.startswith("MULTI"):
        cfg = json.loads(best["family_params"])
        logic = cfg.get("_logic", "AND")
        o = df["open"].values.astype(np.float64)
        h = df["high"].values.astype(np.float64)
        l = df["low"].values.astype(np.float64)
        c = df["close"].values.astype(np.float64)
        v = df["volume"].values.astype(np.float64)
        sigs = []
        target_ob_data = None  # 用於儲存 OB_FVG 的陣列資料

        for f, prm in cfg["families"].items():
            # 注入時間戳
            prm = prm.copy()
            prm["_ts"] = df["ts"].values
            
            result = signal_from_family(f, o, h, l, c, v, prm)
            if isinstance(result, tuple):
                # 如果是 OB_FVG，result 會是 (sig, zone_arrays)
                sig = result[0]
                if f == "OB_FVG":
                    # 暫存陣列資料與 lookback 參數，若有多個 OB_FVG 則以此為準
                    target_ob_data = (result[1], int(prm.get("ob_lookback", 120)))
            else:
                sig = result
            sigs.append(sig)
        entry_sig = _combine_signals_logic(sigs, logic)

        res = run_backtest_from_entry_sig(df, entry_sig, tp, sl, max_hold, fee_side, slippage, worst_case, reverse_mode=reverse_mode)

        # 若多指標組合中包含 OB_FVG，則回填詳細資訊
        if target_ob_data is not None:
            zone_arrays, _lb = target_ob_data
            zone_ob_top, zone_ob_bottom, zone_fvg_top, zone_fvg_bottom, ob_idx_arr, highest_arr, lowest_arr = zone_arrays
            
            # 準備時間序列 (台北時間)
            ts_series = df["ts"].dt.tz_convert("Asia/Taipei")
            
            for t in res["trades_detail"]:
                # base_idx 為進場訊號 K 線 (entry_index - 1)
                base_idx = t["entry_index"] - 1
                if base_idx < 0 or base_idx >= len(c):
                    continue

                ob_top_val = zone_ob_top[base_idx]
                ob_bottom_val = zone_ob_bottom[base_idx]
                fvg_top_val = zone_fvg_top[base_idx]
                fvg_bottom_val = zone_fvg_bottom[base_idx]
                is_fvg = np.isfinite(fvg_top_val)
                zone_form_idx = int(ob_idx_arr[base_idx]) if base_idx < len(ob_idx_arr) else -1

                # 倒查 OB 形成 index（回溯）
                ob_form_idx = -1
                if np.isfinite(ob_top_val) and np.isfinite(ob_bottom_val):
                    search_end = zone_form_idx if zone_form_idx >= 0 else base_idx
                    search_start = max(0, search_end - _lb - 5)
                    for j in range(search_end, search_start - 1, -1):
                        if (np.isclose(h[j], ob_top_val, rtol=1e-6, atol=1e-8) and
                            np.isclose(l[j], ob_bottom_val, rtol=1e-6, atol=1e-8) and
                            c[j] < o[j]):
                            ob_form_idx = j
                            break

                fvg_form_idx = zone_form_idx if is_fvg else -1

                # 填入完整數值與時間
                t["ob_top"] = float(ob_top_val) if np.isfinite(ob_top_val) else None
                t["ob_bottom"] = float(ob_bottom_val) if np.isfinite(ob_bottom_val) else None
                t["fvg_top"] = float(fvg_top_val) if np.isfinite(fvg_top_val) else None
                t["fvg_bottom"] = float(fvg_bottom_val) if np.isfinite(fvg_bottom_val) else None

                t["ob_start_index"] = int(ob_form_idx) if ob_form_idx >= 0 else None
                t["ob_end_index"] = int(base_idx)
                t["ob_start_ts"] = str(ts_series.iloc[ob_form_idx]) if ob_form_idx >= 0 else None
                t["ob_end_ts"] = str(ts_series.iloc[base_idx])

                t["fvg_start_index"] = int(fvg_form_idx) if fvg_form_idx >= 0 else None
                t["fvg_end_index"] = int(base_idx) if fvg_form_idx >= 0 else None
                t["fvg_start_ts"] = str(ts_series.iloc[fvg_form_idx]) if fvg_form_idx >= 0 else None
                t["fvg_end_ts"] = str(ts_series.iloc[base_idx]) if fvg_form_idx >= 0 else None

                if is_fvg and np.isfinite(ob_top_val) and np.isfinite(ob_bottom_val):
                    t["zone_trigger_type"] = "OB+FVG"
                elif is_fvg:
                    t["zone_trigger_type"] = "FVG"
                elif np.isfinite(ob_top_val):
                    t["zone_trigger_type"] = "OB"
                else:
                    t["zone_trigger_type"] = None
        
        # 若為 SMC，使用類似 OB_FVG 的邏輯來解包陣列，但欄位用途稍有不同
        if target_ob_data is None and fam == "SMC":
            # 重新執行一次訊號產生以獲取詳細陣列
            smc_p = json.loads(best["family_params"])
            if NUMBA_OK:
                _, zone_arrays = _signal_from_smc_nb(o, h, l, c, v, 
                                                     int(smc_p.get("len", 5)), 
                                                     int(smc_p.get("htf_mult", 4)),
                                                     float(smc_p.get("risk_reward", 2.0)),
                                                     bool(smc_p.get("trend_filter", True)),
                                                     bool(smc_p.get("reverse", False)))
                # Unpack: (debug_ob_top, debug_ob_bottom, target_arr, stop_arr, ...)
                d_ob_top, d_ob_bottom, _, _, _, _, _ = zone_arrays
                
                # 回填到 trades
                for t in res["trades_detail"]:
                    idx = t["entry_index"]
                    if idx < len(d_ob_top):
                        # SMC 的 OB 資訊在進場當下已知
                        t["ob_top"] = float(d_ob_top[idx]) if np.isfinite(d_ob_top[idx]) else None
                        t["ob_bottom"] = float(d_ob_bottom[idx]) if np.isfinite(d_ob_bottom[idx]) else None
                        t["zone_trigger_type"] = "SMC_Entry"

        res["family"] = fam
        res["family_params"] = best["family_params"]
        res["worst_case"] = worst_case
        return res
    else:
        # Recompute logic for single family (Standard)
        prm = json.loads(best["family_params"])
        # Important: Ensure TEMA_RSI re-run gets the breakdown string
        res = run_backtest(df, fam, prm, tp, sl, max_hold, fee_side, slippage, worst_case, reverse_mode=reverse_mode)
        # res now contains 'stats_breakdown' from run_backtest modification
        return res


def run_grid_gpu(df: pd.DataFrame,
                 single_family: Optional[str],
                 single_ui: Optional[Dict],
                 multi_mode: bool,
                 selected_families: List[str],
                 multi_ui_by_family: Dict[str, Dict],
                 signal_logic: str,
                 tp_list: List[float],
                 sl_list: List[float],
                 max_hold: int,
                 fee_side: float,
                 slippage: float,
                 worst_case: bool,
                 batch_size: int,
                 progress,
                 total_jobs_count: int,
                 *,
                 logger=None,
                 signal_mode: str = "LOGIC",
                 use_torch_compile: bool = False,
                 explicit_combos: Optional[List[Dict]] = None,
                 micro_ctx: Optional["MicroFillContext"] = None) -> List[Dict]:

    assert NUMBA_OK, "需要 Numba 以啟用極速批次模擬（pip install numba）"

    # 內部日誌
    _log = (lambda msg: logger(msg) if callable(logger) else None)

    # 準備 numpy 資料（一次）
    o_np = df["open"].values.astype(np.float64)
    h_np = df["high"].values.astype(np.float64)
    l_np = df["low"].values.astype(np.float64)
    c_np = df["close"].values.astype(np.float64)
    v_np = df["volume"].values.astype(np.float64)

    bar_sec = infer_bar_seconds(df["ts"])
    bpy = bars_per_year(bar_sec)
    T = len(df)

    # 常數：開始/結束時間（避免在內層 for 反覆 pd.to_datetime(df["ts"]) 把時間全燒在解析）
    try:
        _ts0 = df["ts"].iloc[0]
        _ts1 = df["ts"].iloc[-1]
        # 若是 epoch seconds（int/float），轉成人類可讀一次就好
        if isinstance(_ts0, (int, float, np.integer, np.floating)):
            start_ts_str = str(pd.to_datetime(float(_ts0), unit="s", utc=True).tz_convert(None))
            end_ts_str = str(pd.to_datetime(float(_ts1), unit="s", utc=True).tz_convert(None))
        else:
            start_ts_str = str(_ts0)
            end_ts_str = str(_ts1)
    except Exception:
        start_ts_str = ""
        end_ts_str = ""

    # ----------------- 1m 精準撮合：Numba 批次內核所需的 micro_ctx ----------------- #

    use_1m_micro = False
    m1_o = m1_h = m1_l = m1_c = None
    bar_1m_start = bar_1m_end = m1_to_htf = None
    if micro_ctx is not None:
        try:
            if int(getattr(micro_ctx, "htf_len", -1)) != int(T):
                raise ValueError(f"micro_ctx.htf_len={int(getattr(micro_ctx, 'htf_len', -1))} 與目前 HTF bars={int(T)} 不一致（你可能換了主資料但沿用舊的 1m ctx）")
            use_1m_micro = True
            m1_o = np.asarray(getattr(micro_ctx, "o_1m"), dtype=np.float64)
            m1_h = np.asarray(getattr(micro_ctx, "h_1m"), dtype=np.float64)
            m1_l = np.asarray(getattr(micro_ctx, "l_1m"), dtype=np.float64)
            m1_c = np.asarray(getattr(micro_ctx, "c_1m"), dtype=np.float64)
            bar_1m_start = np.asarray(getattr(micro_ctx, "bar_1m_start"), dtype=np.int64)
            bar_1m_end = np.asarray(getattr(micro_ctx, "bar_1m_end"), dtype=np.int64)
            m1_to_htf = getattr(micro_ctx, "m1_to_htf", None)
            if m1_to_htf is None:
                # 舊版 ctx 沒有 m1_to_htf：在此即時建立（仍只做一次）
                _tmp = np.full(int(getattr(micro_ctx, "m1_len", len(m1_o))), -1, dtype=np.int64)
                for bi in range(int(T)):
                    s = int(bar_1m_start[bi])
                    e = int(bar_1m_end[bi])
                    if s < 0:
                        s = 0
                    if e > _tmp.size:
                        e = _tmp.size
                    for mj in range(s, e):
                        _tmp[mj] = bi
                last = -1
                for mj in range(_tmp.size):
                    v = int(_tmp[mj])
                    if v >= 0:
                        last = v
                    else:
                        _tmp[mj] = last
                first = -1
                for mj in range(_tmp.size - 1, -1, -1):
                    v = int(_tmp[mj])
                    if v >= 0:
                        first = v
                    else:
                        _tmp[mj] = first
                m1_to_htf = np.clip(_tmp, 0, int(T) - 1).astype(np.int64)
            else:
                m1_to_htf = np.asarray(m1_to_htf, dtype=np.int64)
        except Exception as _e:
            raise ValueError(f"1m 精準撮合 ctx 不可用：{_e}")

    _log(f"初始化 Numba 事件引擎（bpy={bpy:.2f}，T={T}）")
    if use_1m_micro:
        _log(f"1m 批次撮合已啟用：m1_len={int(len(m1_o))}")
    if use_torch_compile and HAS_TORCH:
        device = get_torch_device()
        _log(f"嘗試使用 GPU 裝置：{device}")
        setup_gpu_runtime_for_speed(device)
        try:
            _log("Torch 編譯加速已啟用，嘗試使用 GPU 執行")
            # （在此可加入 PyTorch 版回測實作，如 torch.compile 模式）
        except Exception as e:
            _log(f"Torch 編譯失敗，改用 Numba：{e}")
            use_torch_compile = False
    else:
        _log("torch.compile / GPU 已停用：改走 Numba 快路徑")
    flush_threshold = min(int(batch_size), 128)
    _log(f"批次門檻設定：flush_threshold={flush_threshold}（batch_size={batch_size}，total_jobs={total_jobs_count}）")


    results: List[Dict] = []
    done = 0

    # ----------------- 單家族模式 -----------------
    if not multi_mode:
        # 若外部有傳入明確的參數列表（JSON），則優先使用；否則從 UI 生成
        if explicit_combos is not None:
            plist = explicit_combos
        else:
            plist = grid_combinations_from_ui(single_family, single_ui)
        
        # 修改：SMC 改回一般模式 (使用固定 TP/SL %)，僅 OB_FVG 若開啟區間模式才走 CPU 逐筆模擬
        # LaguerreRSI_TEMA 與 TEMA_RSI 擁有獨特的出場邏輯，需走獨立迴圈
        complex_families = ["LaguerreRSI_TEMA", "TEMA_RSI"]
        if (single_family in complex_families) or (single_family == "OB_FVG" and bool(single_ui.get("ob_range_based", False))):
            _log(f"{single_family} (複雜策略/動態風控模式)，使用 CPU/Numba 逐筆模擬繞過 GPU")
            results = []
            done = 0
            
            # 對於特殊家族，我們使用傳入的 tp_list/sl_list 或策略內部參數
            current_tp_list = tp_list
            current_sl_list = sl_list
            if single_family in ["LaguerreRSI_TEMA", "TEMA_RSI"]:
                # 這些策略不使用外部 Grid TP/SL，而是使用內建於 params 的設定
                current_tp_list = [0.0]
                current_sl_list = [0.0]

            total = max(1, len(plist) * len(current_tp_list) * len(current_sl_list))

            # 準備時間戳
            ts_vals = df["ts"].values

            if single_family == "TEMA_RSI":
                # ---------------- TEMA_RSI 優化：指標計算與模擬解耦 ---------------- #
                # 1. 根據「影響指標計算」的參數進行分組
                grouped_prms = {}
                for p in plist:
                    # Key: (fast, slow, rsi_len(def 14), rsi_thr)
                    # 這些參數決定了 TEMA 和 RSI 的數值以及入場訊號，必須重算
                    # 而 tp/sl/trail 等參數只影響出場，可共用同一組訊號
                    k = (p['fast_len'], p['slow_len'], p.get('rsi_len', 14), p['rsi_thr'])
                    if k not in grouped_prms: 
                        grouped_prms[k] = []
                    grouped_prms[k].append(p)
                
                # 2. 遍歷分組
                for key_tuple, sub_plist in grouped_prms.items():
                    # 取第一組參數代表計算指標 (需注入 _ts)
                    first_prm = sub_plist[0].copy()
                    first_prm["_ts"] = ts_vals
                    
                    # 計算指標與訊號 (最耗時步驟，每組只做一次)
                    # sig: Boolean Entry Signal
                    # zone_arrays: 這裡面包含的是基於 first_prm 的 TP/SL 陣列，我們稍後會捨棄並重組
                    sig_res = signal_from_family(single_family, o_np, h_np, l_np, c_np, v_np, first_prm)
                    # Unpack updated tuple: (Act, Off, TP, SL, Stake, ReasonArr, Dummy)
                    base_sig, zone_arr_temp = sig_res 
                    base_sig_bool = base_sig.astype(np.bool_)
                    # Extract Reason Array from the 6th element (index 5)
                    base_reason_arr = zone_arr_temp[5]

                    # 3. 遍歷該組下的所有風控參數組合 (快速模擬)
                    N = len(c_np)
                    # 預先分配記憶體 (重用以節省開銷)
                    p_act_arr = np.empty(N, dtype=np.float64)
                    p_off_arr = np.empty(N, dtype=np.float64)
                    p_tp_arr  = np.empty(N, dtype=np.float64)
                    p_sl_arr  = np.empty(N, dtype=np.float64)
                    p_stake_arr = np.empty(N, dtype=np.float64)

                    for prm in sub_plist:
                        # 快速填充風控陣列 (O(N) fill)
                        # 注意：這裡將 scalar 參數擴展為 vector，這是 _simulate_tema_rsi_nb 的要求
                        p_act_arr.fill(float(prm.get("activation_pct", 1.0)) / 100.0)
                        p_off_arr.fill(float(prm.get("trail_ticks", 800) * prm.get("mintick", 0.01)))
                        p_tp_arr.fill(float(prm.get("tp_pct_strat", 2.2)) / 100.0)
                        p_sl_arr.fill(float(prm.get("sl_pct_strat", 6.0)) / 100.0)
                        p_stake_arr.fill(float(prm.get("stake_pct", 95.0)) / 100.0)

                        # 呼叫 Numba 模擬核心 (Updated Signature)
                        # 注意：_simulate_tema_rsi_nb 回傳的最後一個值是 entry_reasons_arr (已在前面定義)
                        (perbar, equity,
                         e_idx, x_idx, e_px, x_px, tr_ret, bars_held, reasons, entry_reasons_arr_ret) = _simulate_tema_rsi_nb(
                            o_np, h_np, l_np, c_np, base_sig_bool,
                            p_act_arr, p_off_arr, p_tp_arr, p_sl_arr, p_stake_arr,
                            base_reason_arr, # Pass the reason array
                            float(slippage), int(max_hold), float(fee_side)
                         )
                        
                        # ---- 績效統計 (與原程式碼一致) ----
                        trade_returns = tr_ret
                        entries_count = int(np.sum(base_sig_bool))
                        trades_count = len(trade_returns)
                        wins = trade_returns[trade_returns > 0]
                        losses = trade_returns[trade_returns <= 0]
                        win_rate = (len(wins) / len(trade_returns) * 100.0) if trades_count > 0 else 0.0
                        avg_win = wins.mean() if wins.size else 0.0
                        avg_loss = losses.mean() if losses.size else 0.0
                        payoff = (avg_win / abs(avg_loss)) if avg_loss != 0 else np.nan
                        pf = (wins.sum() / abs(losses.sum())) if losses.size else np.nan
                        expectancy = trade_returns.mean() if trade_returns.size else 0.0
                        avg_hold = np.mean(bars_held) if bars_held.size else 0.0
                        time_in_mkt = (np.sum(bars_held) / T * 100.0) if trades_count > 0 else 0.0
                        
                        final_equity = equity[-1]
                        total_return = final_equity - 1.0
                        cagr = annualized_return_from_equity(equity, bpy)
                        maxdd_pct, dd_start, dd_end = rolling_max_drawdown(equity)
                        sh = sharpe_ratio(perbar, bpy)
                        so = sortino_ratio(perbar, bpy)
                        cal = calmar_ratio(cagr, maxdd_pct)

                        # ---- TEMA_RSI 進場策略細分統計（格點模式） ----
                        # 在格點掃描時直接計算各策略的細項，寫入 stats_breakdown
                        entry_map_tema = {1: "Pullback", 2: "Momentum", 3: "Cross", 4: "RSI_Revert", 0: "Unknown"}
                        stats_by_entry = {}
                        
                        # entry_reasons_arr_ret 對應每筆交易的進場原因
                        if len(entry_reasons_arr_ret) == len(trade_returns):
                            for tr_i in range(len(trade_returns)):
                                r_code = int(entry_reasons_arr_ret[tr_i])
                                r_str = entry_map_tema.get(r_code, f"Type_{r_code}")
                                if r_str not in stats_by_entry:
                                    stats_by_entry[r_str] = []
                                stats_by_entry[r_str].append(float(trade_returns[tr_i]))
                        
                        breakdown_summary = []
                        # 排序方便閱讀: Pullback, Momentum, Cross, RSI_Revert
                        preferred_order = ["Pullback", "Momentum", "Cross", "RSI_Revert"]
                        existing_keys = list(stats_by_entry.keys())
                        sorted_keys = [k for k in preferred_order if k in existing_keys] + [k for k in existing_keys if k not in preferred_order]

                        for r_key in sorted_keys:
                            r_rets = np.array(stats_by_entry[r_key])
                            cnt = len(r_rets)
                            if cnt > 0:
                                sub_wins = r_rets[r_rets > 0]
                                sub_wr = (len(sub_wins) / cnt * 100.0)
                                sub_sum = np.sum(r_rets) * 100.0 # 總報酬%
                                # 簡易 MDD (該策略獨立運作時的回撤)
                                sub_cum = np.cumprod(1 + r_rets)
                                sub_peak = np.maximum.accumulate(sub_cum)
                                sub_dd = (sub_cum - sub_peak) / sub_peak
                                sub_mdd = np.min(sub_dd) * 100.0 if len(sub_dd) > 0 else 0.0
                                
                                breakdown_summary.append(f"【{r_key}】筆數:{cnt}, WR:{sub_wr:.1f}%, 總利:{sub_sum:.1f}%, MDD:{sub_mdd:.1f}%")
                        
                        breakdown_str = " || ".join(breakdown_summary) if breakdown_summary else "No Trades"

                        results.append({
                            "family": single_family,
                            "family_params": json.dumps(prm, ensure_ascii=False),
                            "tp_pct": 0.0,
                            "sl_pct": 0.0,
                            "max_hold": int(max_hold),
                            "fee_side": float(fee_side),
                            "slippage": float(slippage),
                            "entries": int(entries_count),
                            "trades": int(trades_count),
                            "win_rate_pct": round(win_rate, 4),
                            "avg_win_pct": round(float(avg_win * 100.0), 4),
                            "avg_loss_pct": round(float(avg_loss * 100.0), 4),
                            "payoff": 0.0 if np.isnan(payoff) else round(float(payoff), 4),
                            "profit_factor": 0.0 if np.isnan(pf) else round(float(pf), 4),
                            "expectancy_pct": round(float(expectancy * 100.0), 4),
                            "total_return_pct": round(float(total_return * 100.0), 4),
                            "cagr_pct": round(float(cagr * 100.0), 4),
                            "max_drawdown_pct": round(float(maxdd_pct), 4),
                            "sharpe": round(sh, 4),
                            "sortino": round(so, 4),
                            "calmar": round(cal, 4),
                            "avg_hold_bars": round(float(avg_hold), 2),
                            "time_in_market_pct": round(float(time_in_mkt), 2),
                            "bars": int(T),
                            "start_ts": start_ts_str,
                            "end_ts": end_ts_str,
                            "bpy": round(float(bpy), 2),
                            "worst_case": bool(worst_case),
                            "stats_breakdown": breakdown_str, 
                        })
                        done += 1
                        progress.progress(min(1.0, done / total))
            else:
                for prm in plist:
                    if single_family == "LaguerreRSI_TEMA":
                        # Numba Call for Laguerre
                        # 注入時間戳
                        prm = prm.copy()
                        prm["_ts"] = ts_vals
                        
                        sig_res = signal_from_family(single_family, o_np, h_np, l_np, c_np, v_np, prm)
                        sig, zone_arrays = sig_res
                        
                        atr_sltp_arr, _, atr_trail_arr, atr_act_arr, logic_exit_arr, _, _ = zone_arrays
                        
                        p_sl_coef = float(prm.get("sl_coef", 1.1))
                        p_tp_coef = float(prm.get("tp_coef", 1.9))
                        p_ts_dist_coef = float(prm.get("ts_dist_coef", 1.1))
                        p_ts_act_coef = float(prm.get("ts_act_coef", 1.1))
                        
                        # 執行 Numba 模擬 (使用傳入的單一 max_hold)
                        (perbar, equity,
                    e_idx, x_idx, e_px, x_px, tr_ret, bars_held, reasons) = _simulate_laguerre_tema_nb(
                        o_np, h_np, l_np, c_np, sig.astype(np.bool_),
                        atr_sltp_arr, atr_trail_arr, atr_act_arr, logic_exit_arr,
                        p_sl_coef, p_tp_coef, p_ts_dist_coef, p_ts_act_coef,
                        float(slippage), int(max_hold), float(fee_side)
                    )

                        
                        # 績效統計 (單次)
                        trade_returns = tr_ret
                        entries_count = int(np.sum(sig))
                        trades_count = len(trade_returns)
                        wins = trade_returns[trade_returns > 0]
                        losses = trade_returns[trade_returns <= 0]
                        win_rate = (len(wins) / len(trade_returns) * 100.0) if trades_count > 0 else 0.0
                        avg_win = wins.mean() if wins.size else 0.0
                        avg_loss = losses.mean() if losses.size else 0.0
                        payoff = (avg_win / abs(avg_loss)) if avg_loss != 0 else np.nan
                        pf = (wins.sum() / abs(losses.sum())) if losses.size else np.nan
                        expectancy = trade_returns.mean() if trade_returns.size else 0.0
                        avg_hold = np.mean(bars_held) if bars_held.size else 0.0
                        time_in_mkt = (np.sum(bars_held) / T * 100.0) if trades_count > 0 else 0.0
                        
                        final_equity = equity[-1]
                        total_return = final_equity - 1.0
                        cagr = annualized_return_from_equity(equity, bpy)
                        maxdd_pct, dd_start, dd_end = rolling_max_drawdown(equity)
                        sh = sharpe_ratio(perbar, bpy)
                        so = sortino_ratio(perbar, bpy)
                        cal = calmar_ratio(cagr, maxdd_pct)
                        
                        
                        # [Expert Fix] 序列化前移除 _ts，避免大量數據導致 JSON 轉換失敗
                        clean_prm = {k: v for k, v in prm.items() if not k.startswith("_")}
                        results.append({
                            "family": single_family,
                            "family_params": json.dumps(clean_prm, ensure_ascii=False),
                            "tp_pct": 0.0,
                            "sl_pct": 0.0,
                            "max_hold": int(max_hold),
                            "fee_side": float(fee_side),
                            "entries": int(entries_count),
                            "trades": int(trades_count),
                            "win_rate_pct": round(win_rate, 4),
                            "avg_win_pct": round(float(avg_win * 100.0), 4),
                            "avg_loss_pct": round(float(avg_loss * 100.0), 4),
                            "payoff": 0.0 if np.isnan(payoff) else round(float(payoff), 4),
                            "profit_factor": 0.0 if np.isnan(pf) else round(float(pf), 4),
                            "expectancy_pct": round(float(expectancy * 100.0), 4),
                            "total_return_pct": round(float(total_return * 100.0), 4),
                            "cagr_pct": round(float(cagr * 100.0), 4),
                            "max_drawdown_pct": round(float(maxdd_pct), 4),
                            "sharpe": round(sh, 4),
                            "sortino": round(so, 4),
                            "calmar": round(cal, 4),
                            "avg_hold_bars": round(float(avg_hold), 2),
                            "time_in_market_pct": round(float(time_in_mkt), 2),
                            "bars": int(T),
                            "start_ts": start_ts_str,
                            "end_ts": end_ts_str,
                            "bpy": round(float(bpy), 2),
                            "worst_case": bool(worst_case),
                            "stats_breakdown": "Grid Mode (See Best Detail)",
                        })
                        done += 1
                        progress.progress(min(1.0, done / total))

                    elif single_family == "OB_FVG":
                        # OB_FVG Range Based Logic
                        zone_arrays = None
                        if NUMBA_OK:
                            sig, zone_arrays = _signal_from_ob_fvg_nb(o_np, h_np, l_np, c_np, v_np,
                                                                    int(prm.get("N", 3)),
                                                                    float(prm.get("r", 0.001)),
                                                                    int(prm.get("h", 20)),
                                                                    float(prm.get("g", 1.0)),
                                                                    float(prm.get("a", 0.99)),
                                                                    float(prm.get("rise_thr", 1.002)),
                                                                    float(prm.get("x", 1.0)),
                                                                    float(prm.get("y", 1.0)),
                                                                    int(prm.get("monitor_window", 20)),
                                                                    int(prm.get("rsi_period", 14)),
                                                                    float(prm.get("rsi_diff", 0.0)))
                        else:
                            sig, zone_arrays = signal_from_family(single_family, o_np, h_np, l_np, c_np, v_np, prm)
                        
                        zone_ob_top, zone_ob_bottom, zone_fvg_top, zone_fvg_bottom, ob_idx_arr, highest_arr, lowest_arr = zone_arrays
                        
                        rev_mode = bool(prm.get("reverse", False))
                        if rev_mode:
                            sim_func_per = simulate_short_core_per_entry
                        else:
                            sim_func_per = simulate_long_core_per_entry

                        for tp in current_tp_list:
                            for sl in current_sl_list:
                                (perbar, equity,
                                 e_idx, x_idx, e_px, x_px, tr_ret, bars_held, reasons) = sim_func_per(
                                    o_np, h_np, l_np, c_np,
                                    sig.astype(np.bool_),
                                    zone_ob_top, zone_ob_bottom, zone_fvg_top, zone_fvg_bottom,
                                    ob_idx_arr, highest_arr, lowest_arr,
                                    float(tp), float(sl), int(max_hold),
                                    fee_side=float(fee_side), slippage=float(slippage), worst_case=bool(worst_case)
                                )

                                # 計算績效
                                trade_returns = tr_ret
                                entries_count = int(np.sum(sig))
                                trades_count = len(trade_returns)
                                wins = trade_returns[trade_returns > 0]
                                losses = trade_returns[trade_returns <= 0]
                                win_rate = (len(wins) / len(trade_returns) * 100.0) if trades_count > 0 else 0.0
                                avg_win = wins.mean() if wins.size else 0.0
                                avg_loss = losses.mean() if losses.size else 0.0
                                payoff = (avg_win / abs(avg_loss)) if avg_loss != 0 else np.nan
                                pf = (wins.sum() / abs(losses.sum())) if losses.size else np.nan
                                expectancy = trade_returns.mean() if trade_returns.size else 0.0
                                avg_hold = np.mean(bars_held) if bars_held.size else 0.0
                                time_in_mkt = (np.sum(bars_held) / T * 100.0) if trades_count > 0 else 0.0
                                final_equity = equity[-1]
                                total_return = final_equity - 1.0
                                cagr = annualized_return_from_equity(equity, bpy)
                                maxdd_pct, dd_start, dd_end = rolling_max_drawdown(equity)
                                sh = sharpe_ratio(perbar, bpy)
                                so = sortino_ratio(perbar, bpy)
                                cal = calmar_ratio(cagr, maxdd_pct)
                                results.append({
                                    "family": single_family,
                                    "family_params": json.dumps(prm, ensure_ascii=False),
                                    "tp_pct": round(float(tp * 100.0), 4),
                                    "sl_pct": round(float(sl * 100.0), 4),
                                    "max_hold": int(max_hold),
                                    "fee_side": float(fee_side),
                                    "entries": int(entries_count),
                                    "trades": int(trades_count),
                                    "win_rate_pct": round(win_rate, 4),
                                    "avg_win_pct": round(float(avg_win * 100.0), 4),
                                    "avg_loss_pct": round(float(avg_loss * 100.0), 4),
                                    "payoff": 0.0 if np.isnan(payoff) else round(float(payoff), 4),
                                    "profit_factor": 0.0 if np.isnan(pf) else round(float(pf), 4),
                                    "expectancy_pct": round(float(expectancy * 100.0), 4),
                                    "total_return_pct": round(float(total_return * 100.0), 4),
                                    "cagr_pct": round(float(cagr * 100.0), 4),
                                    "max_drawdown_pct": round(float(maxdd_pct), 4),
                                    "sharpe": round(sh, 4),
                                    "sortino": round(so, 4),
                                    "calmar": round(cal, 4),
                                    "avg_hold_bars": round(float(avg_hold), 2),
                                    "time_in_market_pct": round(float(time_in_mkt), 2),
                                    "bars": int(T),
                                    "start_ts": start_ts_str,
                                    "end_ts": end_ts_str,
                                    "bpy": round(float(bpy), 2),
                                    "worst_case": bool(worst_case),
                                    "stats_breakdown": "Grid Mode (See Best Detail)",
                                })
                        del zone_arrays
                        done += len(current_tp_list) * len(current_sl_list)
                        progress.progress(min(1.0, done / total))
                        _log(f"進度：{done}/{total} 組")

            return results
        else:
            _log(f"建立 {single_family} 訊號快取，組合數={len(plist)}")
            sig_cache = build_cache_for_family(df, single_family, plist, logger=_log)
            batch_sigs, batch_tp, batch_sl, batch_meta = [], [], [], []
            t_run0 = time.perf_counter()
            total_local = max(1, len(plist) * len(tp_list) * len(sl_list))
            def flush_batch():
                nonlocal results, done, batch_sigs, batch_tp, batch_sl, batch_meta
                if not batch_sigs:
                    return
                es = np.stack(batch_sigs, axis=0)
                tps = np.array(batch_tp, dtype=np.float64)
                sls = np.array(batch_sl, dtype=np.float64)

                B = es.shape[0]
                t0 = time.perf_counter()

                # 判斷是否為反向 (取第一個 meta 的設定，單家族模式下所有 reverse 設定應相同)
                # 這裡假設 batch 內的 reverse 設定一致，這在 grid 產生邏輯下是成立的
                is_reverse = False
                if batch_meta and batch_meta[0]["family"] == "OB_FVG":
                    is_reverse = batch_meta[0]["family_params"].get("reverse", False)

                if use_1m_micro:
                    _log(f"→ 1m 批次撮合：batch={B}, T={T}")
                    if is_reverse:
                        batch_core = _metrics_from_sigs_batch_short_1m_nb
                    else:
                        batch_core = _metrics_from_sigs_batch_1m_nb

                    (tr_pct, cagr_pct, mdd_pct, sharpe, sortino, calmar,
                     trades, entries, win_rate, avg_win, avg_loss, payoff, pf,
                     expectancy, avg_hold, tim_pct, eq_final) = batch_core(
                        m1_o, m1_h, m1_l, m1_c,
                        bar_1m_start, bar_1m_end, m1_to_htf,
                        es, tps, sls,
                        int(max_hold), float(fee_side), float(slippage),
                        1 if worst_case else 0,
                        int(T), float(bpy)
                    )
                else:
                    _log(f"→ HTF 批次：batch={B}, T={T}")
                    if is_reverse:
                        batch_core = _metrics_from_sigs_batch_short_nb
                    else:
                        batch_core = _metrics_from_sigs_batch_nb

                    (tr_pct, cagr_pct, mdd_pct, sharpe, sortino, calmar,
                     trades, entries, win_rate, avg_win, avg_loss, payoff, pf,
                     expectancy, avg_hold, tim_pct, eq_final) = batch_core(
                        o_np, h_np, l_np, c_np, es, tps, sls,
                        int(max_hold), float(fee_side), float(slippage),
                        1 if worst_case else 0,
                        int(T), float(bpy)
                    )

                dt = time.perf_counter() - t0

                for k in range(B):
                    m = batch_meta[k]
                    results.append({
                        "family": m["family"],
                        "family_params": m.get("family_params_json", _fast_json_dumps(m["family_params"])),
                        "tp_pct": round(float(batch_tp[k] * 100.0), 4),
                        "sl_pct": round(float(batch_sl[k] * 100.0), 4),
                        "max_hold": int(max_hold),
                        "fee_side": float(fee_side),
                        "slippage": float(slippage),
                        "entries": int(entries[k]),
                        "trades": int(trades[k]),
                        "win_rate_pct": round(float(win_rate[k]), 4),
                        "avg_win_pct": round(float(avg_win[k]), 4),
                        "avg_loss_pct": round(float(avg_loss[k]), 4),
                        "payoff": round(float(payoff[k]), 4),
                        "profit_factor": round(float(pf[k]), 4),
                        "expectancy_pct": round(float(expectancy[k]), 4),
                        "total_return_pct": round(float(tr_pct[k]), 4),
                        "cagr_pct": round(float(cagr_pct[k]), 4),
                        "max_drawdown_pct": round(float(mdd_pct[k]), 4),
                        "sharpe": round(float(sharpe[k]), 4),
                        "sortino": round(float(sortino[k]), 4),
                        "calmar": round(float(calmar[k]), 4),
                        "avg_hold_bars": round(float(avg_hold[k]), 2),
                        "time_in_market_pct": round(float(tim_pct[k]), 2),
                        "bars": int(T),
                        "start_ts": start_ts_str,
                        "end_ts": end_ts_str,
                        "bpy": round(float(bpy), 2),
                        "worst_case": bool(worst_case),
                        "stats_breakdown": "Grid Mode (See Best Detail)",
                    })

                done += B
                elapsed = time.perf_counter() - t_run0
                spd = (done / elapsed) if elapsed > 0 else 0.0
                eta = ((total_local - done) / spd) if (spd > 0 and total_local > done) else 0.0
                _log(f"← 批次完成 dt={dt:.2f}s | done={done}/{total_local} | {spd:.1f} 組/s | ETA={eta:.1f}s")

                progress.progress(min(1.0, done / max(1, total_jobs_count)))
                batch_sigs, batch_tp, batch_sl, batch_meta = [], [], [], []
            for i, prm in enumerate(plist):
                sig = sig_cache[i]
                prm_json = _fast_json_dumps(prm)
                for tp in tp_list:
                    for sl in sl_list:
                        batch_sigs.append(sig)
                        batch_tp.append(tp)
                        batch_sl.append(sl)
                        batch_meta.append({"family": single_family, "family_params": prm, "family_params_json": prm_json})
                        if len(batch_sigs) >= flush_threshold:
                            flush_batch()
            flush_batch()
            return results

        batch_sigs, batch_tp, batch_sl, batch_meta = [], [], [], []

        def flush_batch():
            nonlocal results, done, batch_sigs, batch_tp, batch_sl, batch_meta
            if not batch_sigs:
                return
            es = np.stack(batch_sigs, axis=0)  # [B, T]
            tps = np.array(batch_tp, dtype=np.float64)
            sls = np.array(batch_sl, dtype=np.float64)

            _log(f"→ Numba 批次：size={es.shape[0]}, T={T}")
            t0 = time.perf_counter()
            (tr_pct, cagr_pct, mdd_pct, sharpe, sortino, calmar,
             trades, entries, win_rate, avg_win, avg_loss, payoff, pf,
             expectancy, avg_hold, tim_pct, eq_final) = _metrics_from_sigs_batch_nb(
                o_np, h_np, l_np, c_np, es, tps, sls,
                int(max_hold), float(fee_side), float(slippage),
                1 if worst_case else 0,
                int(T), float(bpy)
            )
            _log(f"← 批次完成，用時 {time.perf_counter()-t0:.2f}s")

            B = es.shape[0]
            for k in range(B):
                m = batch_meta[k]
                results.append({
                    "family": m["family"],
                    "family_params": m.get("family_params_json", _fast_json_dumps(m["family_params"])),
                    "tp_pct": float(batch_tp[k]),
                    "sl_pct": float(batch_sl[k]),
                    "max_hold": int(max_hold),
                    "fee_side": float(fee_side),
                    "entries": int(entries[k]),
                    "trades": int(trades[k]),
                    "win_rate_pct": round(float(win_rate[k]), 4),
                    "avg_win_pct": round(float(avg_win[k]), 4),
                    "avg_loss_pct": round(float(avg_loss[k]), 4),
                    "payoff": round(float(payoff[k]), 4),
                    "profit_factor": round(float(pf[k]), 4),

                    "expectancy_pct": round(float(expectancy[k]), 4),
                    "total_return_pct": round(float(tr_pct[k]), 4),
                    "cagr_pct": round(float(cagr_pct[k]), 4),
                    "max_drawdown_pct": round(float(mdd_pct[k]), 4),
                    "sharpe": round(float(sharpe[k]), 4),
                    "sortino": round(float(sortino[k]), 4),
                    "calmar": round(float(calmar[k]), 4),
                    "avg_hold_bars": round(float(avg_hold[k]), 2),
                    "time_in_market_pct": round(float(tim_pct[k]), 2),
                    "bars": int(T),
                    "start_ts": start_ts_str,
                    "end_ts": end_ts_str,
                    "bpy": round(float(bpy), 2),
                    "worst_case": bool(worst_case),
                })
            done += B
            progress.progress(min(1.0, done / max(1, total_jobs_count)))
            _log(f"進度：{done}/{total_jobs_count} 組")

            batch_sigs, batch_tp, batch_sl, batch_meta = [], [], [], []

        for i, prm in enumerate(plist):
            sig = sig_cache[i]
            prm_json = _fast_json_dumps(prm)
            for tp in tp_list:
                for sl in sl_list:
                    batch_sigs.append(sig)
                    batch_tp.append(tp)
                    batch_sl.append(sl)
                    batch_meta.append({"family": single_family, "family_params": prm, "family_params_json": prm_json})
                    if len(batch_sigs) >= flush_threshold:
                        flush_batch()

        flush_batch()
        return results

    # ----------------- 多家族模式（AND/OR/EACH_OR） -----------------
    fam_plists = {f: grid_combinations_from_ui(f, multi_ui_by_family[f]) for f in selected_families}
    fam_sig_cache = {f: build_cache_for_family(df, f, fam_plists[f], logger=_log) for f in selected_families}

    _log(f"多指標家族：{selected_families}")
    for f in selected_families:
        _log(f"  - {f}: 組合數={len(fam_plists[f])}")
    fam_idx_ranges = [range(len(fam_plists[f])) for f in selected_families]

    batch_sigs, batch_tp, batch_sl, batch_meta = [], [], [], []

    def flush_batch_multi():
        nonlocal results, done, batch_sigs, batch_tp, batch_sl, batch_meta
        if not batch_sigs:
            return
        es = np.stack(batch_sigs, axis=0)
        tps = np.array(batch_tp, dtype=np.float64)
        sls = np.array(batch_sl, dtype=np.float64)

        _log(f"→ Numba 批次：size={es.shape[0]}, T={T}")
        t0 = time.perf_counter()
        
        # 多指標模式下，判斷該 batch 是否觸發 reverse
        # 對於 MULTI[logic]，我們檢查其內部包含的 OB_FVG 是否有 reverse=True
        # 若是 EACH_OR (單家族批次)，直接看 params
        
        # 這裡採用逐筆檢查或 batch 統一檢查？
        # 因 _metrics...nb 是一次性處理整塊矩陣，這意味著整塊矩陣必須是同方向的。
        # 幸運的是，在下面的 loop 構造中，我們是按 param 組合填入 batch 的。
        # 如果是 MULTI 模式，reverse 取決於 family_params 結構。
        
        # 為了效能，我們檢測 batch 中第一筆 meta。如果 batch 混合了 reverse True/False，邏輯會錯。
        # 但下面的迴圈是 "for idx_tuple ... append"，同一 batch 來自同一組 params 邏輯嗎？
        # 不，batch 是累積多個不同的 param 組合。
        # 如果有些組合 reverse=True，有些 False，就不能一起送進同一個 batch kernel。
        # **修正方案**：我們在 append 到 batch 前，若 reverse 狀態改變，先 flush。
        
        # 由於實作複雜度，我們簡單假設：若當前 batch 第一筆是 Reverse，則全 batch 用 Short 核心。
        # 這要求我们在 loop 裡控制 batch 分組。
        
        # 暫時實作：讀取第一筆。
        # (更嚴謹的做法是在 loop 裡檢查 `current_reverse_state`，若變更則 flush)
        
        is_reverse = False
        if batch_meta:
            m = batch_meta[0]
            if m["family"] == "OB_FVG":
                is_reverse = m["family_params"].get("reverse", False)
            elif str(m["family"]).startswith("MULTI"):
                # 檢查內部是否有 OB_FVG 且 reverse=True
                fp = m["family_params"]
                if "families" in fp:
                    for fkey, fval in fp["families"].items():
                        if fkey == "OB_FVG" and fval.get("reverse", False):
                            is_reverse = True
                            break
        
        if is_reverse:
            batch_core = _metrics_from_sigs_batch_short_nb
        else:
            batch_core = _metrics_from_sigs_batch_nb

        (tr_pct, cagr_pct, mdd_pct, sharpe, sortino, calmar,
         trades, entries, win_rate, avg_win, avg_loss, payoff, pf,
         expectancy, avg_hold, tim_pct, eq_final) = batch_core(
            o_np, h_np, l_np, c_np, es, tps, sls,
            int(max_hold), float(fee_side), float(slippage),
            1 if worst_case else 0,
            int(T), float(bpy)
        )
        _log(f"← 批次完成，用時 {time.perf_counter()-t0:.2f}s")

        B = es.shape[0]
        for k in range(B):
            m = batch_meta[k]
            results.append({
                "family": m["family"],
                "family_params": m.get("family_params_json", _fast_json_dumps(m["family_params"])),
                "tp_pct": round(float(batch_tp[k] * 100.0), 4),
                "sl_pct": round(float(batch_sl[k] * 100.0), 4),
                "max_hold": int(max_hold),
                "fee_side": float(fee_side),
                "entries": int(entries[k]),
                "trades": int(trades[k]),
                "win_rate_pct": round(float(win_rate[k]), 4),
                "avg_win_pct": round(float(avg_win[k]), 4),
                "avg_loss_pct": round(float(avg_loss[k]), 4),
                "payoff": round(float(payoff[k]), 4),
                "profit_factor": round(float(pf[k]), 4),
                "expectancy_pct": round(float(expectancy[k]), 4),
                "total_return_pct": round(float(tr_pct[k]), 4),
                "cagr_pct": round(float(cagr_pct[k]), 4),
                "max_drawdown_pct": round(float(mdd_pct[k]), 4),
                "sharpe": round(float(sharpe[k]), 4),
                "sortino": round(float(sortino[k]), 4),
                "calmar": round(float(calmar[k]), 4),
                "avg_hold_bars": round(float(avg_hold[k]), 2),
                "time_in_market_pct": round(float(tim_pct[k]), 2),
                "bars": int(T),
                "start_ts": start_ts_str,
                "end_ts": end_ts_str,
                "bpy": round(float(bpy), 2),
                "worst_case": bool(worst_case),
                "stats_breakdown": "Grid Mode (See Best Detail)",
            })
        done += B
        progress.progress(min(1.0, done / max(1, total_jobs_count)))
        _log(f"進度：{done}/{total_jobs_count} 組")

        batch_sigs, batch_tp, batch_sl, batch_meta = [], [], [], []

    # 輔助函數：檢測 meta 的 reverse 屬性
    def _is_rev(meta_item):
        if not meta_item: return False
        if meta_item["family"] == "OB_FVG":
            return meta_item["family_params"].get("reverse", False)
        if str(meta_item["family"]).startswith("MULTI"):
            fp = meta_item["family_params"]
            if "families" in fp:
                for fkey, fval in fp["families"].items():
                    if fkey == "OB_FVG" and fval.get("reverse", False):
                        return True
        return False

    # EACH_OR：每個家族單跑 + ALL：合併（AND/OR）
    if signal_mode == "EACH_OR":
        _log("模式：EACH（每個家族單獨回測）")
        for fam in selected_families:
            cache = fam_sig_cache[fam]
            plist = fam_plists[fam]
            for i, prm in enumerate(plist):
                sig = cache[i]
                # 準備 meta 以檢測 reverse 變更
                current_meta = {"family": fam, "family_params": prm, "family_params_json": _fast_json_dumps(prm)}
                rev_now = _is_rev(current_meta)
                
                # 若 batch 非空且 reverse 狀態與當前不符，先 flush
                if batch_meta:
                    rev_prev = _is_rev(batch_meta[0])
                    if rev_now != rev_prev:
                        flush_batch_multi()

                for tp in tp_list:
                    for sl in sl_list:
                        batch_sigs.append(sig)
                        batch_tp.append(tp)
                        batch_sl.append(sl)
                        batch_meta.append(current_meta)
                        if len(batch_sigs) >= batch_size:
                            flush_batch_multi()
        flush_batch_multi()

    # ALL：把所有家族一次 AND/OR（comb_logic）
    comb_logic = "OR" if signal_mode == "EACH_OR" else signal_logic
    _log(f"模式：ALL（合併邏輯={comb_logic}）")
    for idx_tuple in itertools.product(*fam_idx_ranges):
        sigs = [fam_sig_cache[f][i] for f, i in zip(selected_families, idx_tuple)]
        combined = _combine_signals_logic(sigs, comb_logic)
        param_map = {f: fam_plists[f][i] for f, i in zip(selected_families, idx_tuple)}
        fam_meta = {
            "family": f"MULTI[{comb_logic}]",
            "family_params": {"_logic": comb_logic, "families": param_map},
            "family_params_json": _fast_json_dumps({"_logic": comb_logic, "families": param_map})
        }
        
        rev_now = _is_rev(fam_meta)
        if batch_meta:
            rev_prev = _is_rev(batch_meta[0])
            if rev_now != rev_prev:
                flush_batch_multi()

        for tp in tp_list:
            for sl in sl_list:
                batch_sigs.append(combined)
                batch_tp.append(tp)
                batch_sl.append(sl)
                batch_meta.append(fam_meta)
                if len(batch_sigs) >= batch_size:
                    flush_batch_multi()

    flush_batch_multi()
    return results




# ----------------------------- 格點掃描 ----------------------------- #
def build_cache_for_family(df: pd.DataFrame,
                           fam: str,
                           plist: List[Dict],
                           logger=None) -> List[np.ndarray]:
    """依家族與參數清單，批量產生 entry signal（布林陣列），含記憶體快取。"""
    _log = logger if callable(logger) else (lambda msg: None)

    # 0. 檢查通用快取 (Expert Cache Check)
    # 使用 JSON dump 參數列表作為指紋 (Key)，確保參數一致時直接命中
    try:
        cache_key = f"{fam}_{json.dumps(plist, sort_keys=True)}"
        if cache_key in GENERIC_SIG_CACHE:
            _log(f"   {fam} 訊號快取命中 (Skip Calculation)")
            return GENERIC_SIG_CACHE[cache_key]
    except Exception:
        pass # 若無法序列化則跳過快取

    # 轉 numpy（一次）
    # 統一 float64：避免 Numba 因 dtype 不同重複編譯 + 避免 RSI 每次轉型複製
    o_np = df["open"].to_numpy(dtype=np.float64, copy=False)
    h_np = df["high"].to_numpy(dtype=np.float64, copy=False)
    l_np = df["low"].to_numpy(dtype=np.float64, copy=False)
    c_np = df["close"].to_numpy(dtype=np.float64, copy=False)
    v_np = df["volume"].to_numpy(dtype=np.float64, copy=False)

    out: List[np.ndarray] = []
    Np = len(plist)
    last_t = time.perf_counter()

    # [專家修正] 準備時間戳 (供需要時間感知的策略使用)
    ts_vals = df["ts"].values

    if fam == "RSI" and Np > 0:
        # 先把 close 有效值的 mask 算一次，避免每個組合都重算 np.isfinite
        finite_mask = np.isfinite(c_np)

        # 先收集所有需要的 period，避免同一 period 反覆計算 RSI
        try:
            uniq_periods = sorted({int(p.get("period", 14)) for p in plist})
        except Exception:
            uniq_periods = []

        rsi_cache_local: Dict[int, np.ndarray] = {}

        # 若資料本身已經有 rsi_{period} 欄位（你的 data_3y_rsi.csv 就是這種），直接零成本取用
        for per in uniq_periods:
            col = f"rsi_{int(per)}"
            if col in df.columns:
                rsi_cache_local[int(per)] = df[col].to_numpy(dtype=np.float64, copy=False)
            else:
                rsi_cache_local[int(per)] = RSI(c_np, int(per))

        for i, prm in enumerate(plist, start=1):
            per = int(prm.get("period", 14))
            thr = float(prm.get("enter_level", 30.0))

            r = rsi_cache_local.get(per)
            if r is None:
                # 安全防呆：plist 若被外部插入奇怪 period，仍可正確計算
                r = RSI(c_np, per)
                rsi_cache_local[per] = r

            sig = (r <= thr) & finite_mask
            sig[:2] = False
            out.append(sig.astype(np.bool_, copy=False))

            now_t = time.perf_counter()
            if now_t - last_t >= 0.5:
                _log(f"   RSI 快取 {i}/{Np}（{i/Np*100:.1f}%）")
                last_t = now_t

    elif fam == "OB_FVG" and Np > 0:
        global OB_FVG_cache
        out = []
        last_t = time.perf_counter()
        
        for i, prm in enumerate(plist, start=1):
            pN = int(prm.get("N", 3))
            pr = float(prm.get("r", 0.001))
            ph = int(prm.get("h", 20))
            pg = float(prm.get("g", 1.0))
            pa = float(prm.get("a", 0.99))
            p_rise = float(prm.get("rise_thr", 1.002))
            px = float(prm.get("x", 1.0))
            py = float(prm.get("y", 1.0))
            pw = int(prm.get("monitor_window", 20))
            p_rsi_p = int(prm.get("rsi_period", 14))
            p_rsi_diff = float(prm.get("rsi_diff", 0.0))
            
            key = (len(c_np), float(c_np[0]) if len(c_np)>0 else 0, float(c_np[-1]) if len(c_np)>0 else 0,
                   pN, pr, ph, pg, pa, p_rise, px, py, pw, p_rsi_p, p_rsi_diff)
            
            if key in OB_FVG_cache:
                sig = OB_FVG_cache[key]
            else:
                if NUMBA_OK:
                    sig, _ = _signal_from_ob_fvg_nb(o_np, h_np, l_np, c_np, v_np, 
                                                    pN, pr, ph, pg, pa, p_rise, px, py, pw, p_rsi_p, p_rsi_diff)
                else:
                    res = signal_from_family(fam, o_np, h_np, l_np, c_np, v_np, prm)
                    sig = res[0] if isinstance(res, tuple) else res
                OB_FVG_cache[key] = sig
                
            out.append(sig.astype(np.bool_, copy=False))
            
            now_t = time.perf_counter()
            if now_t - last_t >= 0.5:
                _log(f"   OB_FVG 快取 {i}/{Np}（{i/Np*100:.1f}%）")
                last_t = now_t
    else:
        # 其他所有家族 (包含 TEMA_RSI, Laguerre 等)
        for i, prm in enumerate(plist, start=1):
            # [專家修正] 注入時間戳，確保依賴 _ts 的策略能正常運作
            prm_run = prm.copy()
            prm_run["_ts"] = ts_vals
            
            res = signal_from_family(fam, o_np, h_np, l_np, c_np, v_np, prm_run)
            
            # 若回傳 (sig, arrays)，只保留 sig 以節省記憶體並相容批次運算
            if isinstance(res, tuple):
                sig_only = res[0]
            else:
                sig_only = res
            
            out.append(np.asarray(sig_only, dtype=np.bool_))
            
            now_t = time.perf_counter()
            if now_t - last_t >= 0.5:
                _log(f"   {fam} 快取 {i}/{Np}（{i/Np*100:.1f}%）")
                last_t = now_t

    _log(f"   {fam} 訊號快取完成，寫入全域快取...")
    # 寫入快取
    try:
        GENERIC_SIG_CACHE[cache_key] = out
    except Exception:
        pass # Ignore cache write errors

    return out
def grid_combinations_from_ui(family: str, ui: Dict) -> List[Dict]:
    """根據 UI 參數構建該指標家族的參數格點。"""
    combos = []

    def irange(a, b, step=1):
        return list(range(int(a), int(b)+1, int(step)))

    def frange(a, b, step):
        vals = []
        x = a
        # 避免浮點殘差
        while x <= b + 1e-12:
            vals.append(round(x, 6))
            x += step
        return vals

    if family == "RSI":
        for p in irange(ui["rsi_p_min"], ui["rsi_p_max"], ui["rsi_p_step"]):
            for lv in irange(ui["rsi_lv_min"], ui["rsi_lv_max"], ui["rsi_lv_step"]):
                combos.append({"period": p, "enter_level": float(lv)})

    elif family in ["SMA_Cross", "EMA_Cross", "HMA_Cross", "DEMA_Cross", "TEMA_Cross", "WMA_Cross"]:
        for f in irange(ui["fast_min"], ui["fast_max"], ui["fast_step"]):
            for s in irange(ui["slow_min"], ui["slow_max"], ui["slow_step"]):
                if f < s:
                    combos.append({"fast": f, "slow": s})

    elif family in ["MACD_Cross", "PPO_Cross", "PVO_Cross"]:
        for fa in irange(ui["fast_min"], ui["fast_max"], ui["fast_step"]):
            for sl in irange(ui["slow_min"], ui["slow_max"], ui["slow_step"]):
                if fa < sl:
                    for sg in irange(ui["sig_min"], ui["sig_max"], ui["sig_step"]):
                        combos.append({"fast": fa, "slow": sl, "signal": sg})

    elif family == "Bollinger_Touch":
        for p in irange(ui["bb_p_min"], ui["bb_p_max"], ui["bb_p_step"]):
            for n in frange(ui["bb_n_min"], ui["bb_n_max"], ui["bb_n_step"]):
                combos.append({"period": p, "nstd": n})

    elif family in ["Stoch_Oversold"]:
        for k in irange(ui["k_min"], ui["k_max"], ui["k_step"]):
            for d in irange(ui["d_min"], ui["d_max"], ui["d_step"]):
                for lv in irange(ui["stoch_lv_min"], ui["stoch_lv_max"], ui["stoch_lv_step"]):
                    combos.append({"k": k, "d": d, "enter_level": float(lv)})

    elif family in ["CCI_Oversold", "WillR_Oversold", "MFI_Oversold"]:
        for p in irange(ui["p_min"], ui["p_max"], ui["p_step"]):
            for lv in irange(ui["lv_min"], ui["lv_max"], ui["lv_step"]):
                combos.append({"period": p, "enter_level": float(lv)})

    elif family in ["Donchian_Breakout"]:
        for p in irange(ui["look_min"], ui["look_max"], ui["look_step"]):
            combos.append({"lookback": p})

    elif family in ["ADX_DI_Cross", "Aroon_Cross", "Aroon_Osc_Threshold"]:
        for p in irange(ui["p_min"], ui["p_max"], ui["p_step"]):
            if family == "Aroon_Osc_Threshold":
                for thr in frange(ui["thr_min"], ui["thr_max"], ui["thr_step"]):
                    combos.append({"period": p, "enter_thr": float(thr)})
            else:
                combos.append({"period": p})

    elif family in ["ROC_Threshold", "CMF_Threshold", "EFI_Threshold", "BB_PercentB_Revert"]:
        for p in irange(ui["p_min"], ui["p_max"], ui["p_step"]):
            for thr in frange(ui["thr_min"], ui["thr_max"], ui["thr_step"]):
                if family == "BB_PercentB_Revert":
                    combos.append({"period": p, "nstd": ui["nstd"], "enter_thr": float(thr)})
                else:
                    combos.append({"period": p, "enter_thr": float(thr)})

    elif family in ["KAMA_Cross", "TRIX_Cross", "DPO_Revert", "ATR_Band_Break", "Vortex_Cross", "Volatility_Squeeze"]:
        for p in irange(ui["p_min"], ui["p_max"], ui["p_step"]):
            if family == "ATR_Band_Break":
                for mult in frange(ui["mult_min"], ui["mult_max"], ui["mult_step"]):
                    combos.append({"period": p, "mult": float(mult)})
            elif family == "Volatility_Squeeze":
                for n in frange(ui["nstd_min"], ui["nstd_max"], ui["nstd_step"]):
                    for q in frange(ui["q_min"], ui["q_max"], ui["q_step"]):
                        combos.append({"period": p, "nstd": float(n), "quantile": float(q)})
            else:
                combos.append({"period": p})
    elif family == "OB_FVG":
        for n in irange(ui["obfvg_n_min"], ui["obfvg_n_max"], ui["obfvg_n_step"]):
            for r in frange(ui["obfvg_r_min"], ui["obfvg_r_max"], ui["obfvg_r_step"]):
                for h in irange(ui["obfvg_h_min"], ui["obfvg_h_max"], ui["obfvg_h_step"]):
                    for g in frange(ui["obfvg_g_min"], ui["obfvg_g_max"], ui["obfvg_g_step"]):
                        for a in frange(ui["obfvg_a_min"], ui["obfvg_a_max"], ui["obfvg_a_step"]):
                            for thr in frange(ui["obfvg_thr_min"], ui["obfvg_thr_max"], ui["obfvg_thr_step"]):
                                for w in irange(ui.get("obfvg_w_min", 20), ui.get("obfvg_w_max", 20), ui.get("obfvg_w_step", 10)):
                                    for rsi_p in irange(ui.get("obfvg_rsi_p_min", 14), ui.get("obfvg_rsi_p_max", 14), ui.get("obfvg_rsi_p_step", 1)):
                                        for rsi_d in frange(ui.get("obfvg_rsi_diff_min", 0.0), ui.get("obfvg_rsi_diff_max", 0.0), ui.get("obfvg_rsi_diff_step", 0.05)):
                                            prm = {
                                                "N": int(n),
                                                "r": float(r),
                                                "h": int(h),
                                                "g": float(g),
                                                "a": float(a),
                                                "rise_thr": float(thr),
                                                "x": float(ui.get("obfvg_x", 1.0)),
                                                "y": float(ui.get("obfvg_y", 1.0)),
                                                "monitor_window": int(w),
                                                "rsi_period": int(rsi_p),
                                                "rsi_diff": float(rsi_d),
                                                "ob_range_based": bool(ui.get("obfvg_ob_range_based", ui.get("ob_range_based", False))),
                                                "reverse": bool(ui.get("obfvg_reverse", ui.get("reverse", False)))
                                            }
                                            combos.append(prm)

    elif family == "SMC":
        # Expert SMC Fusion 格點
        for ln in irange(ui["smc_len_min"], ui["smc_len_max"], ui["smc_len_step"]):
            for limit in irange(ui["smc_limit_min"], ui["smc_limit_max"], ui["smc_limit_step"]):
                prm = {
                    "length": int(ln),
                    "ob_limit": int(limit),
                    "reverse": bool(ui.get("smc_reverse", False))
                }
                combos.append(prm)

    elif family == "LaguerreRSI_TEMA":
        # Expert Grid for Strategy 0.60842
        # Fixed Params from Strategy:
        # EMA Weekly: 9, 20, 40
        # Lowest Open: 10
        # ATR Periods: 15 (SL/TP), 18 (TrailDist), 20 (TrailAct)
        
        # Gamma
        for gamma in frange(ui["gamma_min"], ui["gamma_max"], ui["gamma_step"]):
            # TEMA Period (Default 30)
            for t_len in irange(ui["tema_min"], ui["tema_max"], ui["tema_step"]):
                # Risk Coefs Grid
                for sl_c in frange(ui["sl_c_min"], ui["sl_c_max"], ui["sl_c_step"]):
                    for tp_c in frange(ui["tp_c_min"], ui["tp_c_max"], ui["tp_c_step"]):
                        for ts_dist in frange(ui["tsd_min"], ui["tsd_max"], ui["tsd_step"]):
                            for ts_act in frange(ui["tsa_min"], ui["tsa_max"], ui["tsa_step"]):
                                prm = {
                                    "tema_len": int(t_len),
                                    "gamma": float(gamma),
                                    "ema1_w": 9, 
                                    "ema2_w": 20,
                                    "ema3_w": 40,
                                    "low_lookback": 10,
                                    "sl_coef": float(sl_c),
                                    "tp_coef": float(tp_c),
                                    "ts_dist_coef": float(ts_dist),
                                    "ts_act_coef": float(ts_act),
                                    "atr_sltp_len": 15,
                                    "atr_trail_len": 18,
                                    "atr_act_len": 20
                                }
                                combos.append(prm)

    elif family == "TEMA_RSI":
        # Pine Script: 帥 Strategy Grid
        # [專家修正] 確保所有 range 函數輸入皆為數值，並正確處理 JSON 傳入的參數
        for fast in irange(ui.get("fast_min", 3), ui.get("fast_max", 3), ui.get("fast_step", 1)):
            for slow in irange(ui.get("slow_min", 100), ui.get("slow_max", 100), ui.get("slow_step", 10)):
                for rsi_t in irange(ui.get("rsi_thr_min", 20), ui.get("rsi_thr_max", 20), ui.get("rsi_thr_step", 5)):
                    # Fixed Risk / Trailing Params Grid
                    for tp in frange(ui.get("tp_min", 2.2), ui.get("tp_max", 2.2), ui.get("tp_step", 0.1)):
                        for sl in frange(ui.get("sl_min", 6.0), ui.get("sl_max", 6.0), ui.get("sl_step", 0.1)):
                            for act in frange(ui.get("act_min", 1.0), ui.get("act_max", 1.0), ui.get("act_step", 0.1)):
                                for tr_ticks in irange(ui.get("tr_tick_min", 800), ui.get("tr_tick_max", 800), ui.get("tr_tick_step", 100)):
                                    prm = {
                                        "fast_len": int(fast),
                                        "slow_len": int(slow),
                                        "rsi_len": 14, # Fixed
                                        "rsi_thr": int(rsi_t),
                                        "tp_pct_strat": float(tp),
                                        "sl_pct_strat": float(sl),
                                        "activation_pct": float(act),
                                        "trail_ticks": int(tr_ticks),
                                        "mintick": float(ui.get("mintick", 0.01)),
                                        # 保留原始 range 設定以便除錯或顯示，非模擬核心必要
                                        "act_max": float(ui.get("act_max", 1.0)),
                                        "act_step": float(ui.get("act_step", 0.1)),
                                        "tr_tick_min": int(ui.get("tr_tick_min", 800)),
                                        "tr_tick_max": int(ui.get("tr_tick_max", 800)),
                                        "tr_tick_step": int(ui.get("tr_tick_step", 100)),
                                        "stake_pct": float(ui.get("stake_pct", 95.0))
                                    }
                                    combos.append(prm)

    elif family in ["OBV_Slope", "ADL_Slope"]:
        combos.append({})  # 無參數

    else:
        raise ValueError(f"未為 {family} 設定格點參數")

    return combos


# ----------------------------- UI 與主流程 ----------------------------- #

def load_and_validate_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"找不到檔案：{path}")

    lock_path = str(path) + ".lock"
    try:
        with FileLock(lock_path, timeout=3):
            df = pd.read_csv(path)
    except Timeout:
        # 背景同步佔用鎖時：用容錯讀取避免整個流程失敗（最後一行半寫入也不會炸）
        try:
            df = pd.read_csv(path, on_bad_lines="skip")
        except Exception:
            df = pd.read_csv(path, engine="python", on_bad_lines="skip")
    need_cols = ["ts", "open", "high", "low", "close", "volume"]
    for col in need_cols:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少必要欄位：{col}")

    # 型別與排序
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.sort_values("ts").reset_index(drop=True)

    # 強制數值欄位為 numeric（避免字串導致後續計算爆炸）
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 去除無效行
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["ts", "open", "high", "low", "close", "volume"])

    # 去除重複時間戳（避免 intrabar/searchsorted 行為異常）
    df = df.drop_duplicates(subset=["ts"], keep="last").reset_index(drop=True)

    # 基本合理性
    if (df["high"] < df["low"]).any():
        raise ValueError("資料錯誤：出現 high < low")

    return df

# ----------------------------- 1m 精準撮合（Micro Fill） ----------------------------- #
# 目的：當使用者勾選「1m 精準撮合」時，所有進出場、TP/SL 觸發都改用 1m CSV 來決定
#      - 進場：使用 entry bar 的第一根 1m open 作為入場價（要求 1m ts 必須對齊主週期 bar open）
#      - 出場：在允許的持倉視窗內，逐根 1m 檢查 TP/SL，取「最先觸發」者；若同 1m 同時命中，沿用 worst_case 規則
#      - TIME：使用最後一根 1m close 作為出場價（理論上等於 HTF close）
#
# 注意：為了「不動原本策略邏輯」，這裡只做撮合層（fill layer）修正：
#      - 訊號(entry_sig) 仍然由主週期(例如 30min)計算
#      - max_hold 仍然以主週期 bar 數為準
#      - 只把價格與 TP/SL 觸發順序「改用 1m」重新計算
#
# 重要限制（刻意嚴格，避免你拿到假結果）：
#      - 目前僅支援「標準 SL/TP/時間出場」與 OB_FVG/SMC 的 per-entry SL/TP。
#      - 像 TEMA_RSI / LaguerreRSI_TEMA 這類有自訂動態出場邏輯的策略，若要做到 1m 精準，
#        需要把整套出場邏輯搬到 1m 逐筆模擬（工作量很大且容易引入錯誤），因此本版本直接拒絕執行。


def load_and_validate_csv_1m(path: str) -> pd.DataFrame:
    """讀取並驗證 1m CSV（不做 30min 強制重採樣）。"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"找不到檔案：{path}")

    lock_path = str(path) + ".lock"
    try:
        with FileLock(lock_path, timeout=3):
            df = pd.read_csv(path)
    except Timeout:
        # 背景同步佔用鎖時：用容錯讀取避免整個流程失敗（最後一行半寫入也不會炸）
        try:
            df = pd.read_csv(path, on_bad_lines="skip")
        except Exception:
            df = pd.read_csv(path, engine="python", on_bad_lines="skip")
    need_cols = ["ts", "open", "high", "low", "close", "volume"]
    for col in need_cols:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少必要欄位：{col}")

    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.sort_values("ts").reset_index(drop=True)

    # 強制數值欄位為 numeric（避免字串導致 resample/聚合錯誤）
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 去除無效行 / 無限值
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["ts", "open", "high", "low", "close", "volume"])

    # 去重（同一分鐘重覆，以最後一筆為準）
    df = df.drop_duplicates(subset=["ts"], keep="last").reset_index(drop=True)

    # 1m 精準撮合需要「每分鐘都存在」，否則主週期開盤對齊會失敗。
    # 因此這裡仍然先 resample 到 1min，但遇到缺分鐘會以「前一分鐘 close」補齊 OHLC，volume=0。
    df = df.set_index("ts").resample("1min").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum"
    })

    _gap_mask = df["close"].isna()
    _gap_cnt = int(_gap_mask.sum())
    if _gap_cnt > 0:
        # 缺分鐘補齊：open/high/low/close = 前一分鐘 close（連續缺口會連續沿用），volume=0
        df["close"] = df["close"].ffill()
        df["open"] = df["open"].fillna(df["close"])
        df["high"] = df["high"].fillna(df["close"])
        df["low"] = df["low"].fillna(df["close"])
        df["volume"] = df["volume"].fillna(0.0)

        # 若檔案開頭就缺資料（ffill 仍為 NaN），這些行無法用來對齊，直接剔除
        df = df.dropna(subset=["open", "high", "low", "close"])

        # 紀錄缺口資訊供上層 UI 顯示/除錯
        try:
            df.attrs["gap_filled_count"] = _gap_cnt
            _m = _gap_mask.to_numpy(dtype=np.int8)
            _max_run = 0
            _run = 0
            for _v in _m:
                if _v == 1:
                    _run += 1
                    if _run > _max_run:
                        _max_run = _run
                else:
                    _run = 0
            df.attrs["gap_filled_max_run"] = int(_max_run)
        except Exception:
            pass

    df = df.reset_index()

    # 基本合理性
    if (df["high"] < df["low"]).any():
        raise ValueError("資料錯誤：出現 high < low")

    # 防呆：確認時間間距大多數接近 1min（不是硬性要求，但能抓到你上錯檔）
    if len(df) >= 3:
        dt_ns = np.diff(df["ts"].astype("int64").to_numpy())
        med = int(np.median(dt_ns))
        if med > int(90 * 1e9):  # > 90s
            raise ValueError("你上傳的 1m CSV 看起來不是 1 分鐘級別（時間間距過大）。")

    return df


class MicroFillContext:
    """把 HTF bar 對齊到 1m index 範圍，避免每次回測都重做 searchsorted。"""

    def __init__(self, df_htf: pd.DataFrame, df_1m: pd.DataFrame):
        if df_htf is None or df_1m is None:
            raise ValueError("MicroFillContext 需要 df_htf 與 df_1m")

        if "ts" not in df_htf.columns or "ts" not in df_1m.columns:
            raise ValueError("df_htf/df_1m 必須包含 ts 欄位")

        self.htf_len = int(len(df_htf))
        self.m1_len = int(len(df_1m))
        if self.htf_len < 2:
            raise ValueError("HTF 資料筆數不足（至少需要 2 根 K）")
        if self.m1_len < 10:
            raise ValueError("1m 資料筆數過少，無法進行精準撮合")

        # ts 轉 int64(ns)（UTC）
        self.ts_htf_ns = df_htf["ts"].astype("int64").to_numpy()
        self.ts_1m_ns = df_1m["ts"].astype("int64").to_numpy()

        # HTF bar 長度（用中位數推斷，避免少量缺 Bar 影響）
        d = np.diff(self.ts_htf_ns)
        d = d[d > 0]
        if d.size == 0:
            raise ValueError("HTF ts 不是嚴格遞增，請檢查資料")
        self.tf_ns = int(np.median(d))

        # 檢查 1m 覆蓋範圍是否足夠
        htf_start = int(self.ts_htf_ns[0])
        htf_end_excl = int(self.ts_htf_ns[-1] + self.tf_ns)
        if self.ts_1m_ns[0] > htf_start or self.ts_1m_ns[-1] < htf_end_excl - int(60 * 1e9):
            raise ValueError("1m CSV 的時間範圍不足以覆蓋主週期資料，請確認兩份 CSV 是同一段期間。")

        # 1m OHLC
        self.o_1m = df_1m["open"].to_numpy(dtype=np.float64)
        self.h_1m = df_1m["high"].to_numpy(dtype=np.float64)
        self.l_1m = df_1m["low"].to_numpy(dtype=np.float64)
        self.c_1m = df_1m["close"].to_numpy(dtype=np.float64)

        # 每根 HTF bar 對應到 1m index slice: [start, end)
        end_htf_ns = np.empty_like(self.ts_htf_ns)
        end_htf_ns[:-1] = self.ts_htf_ns[1:]
        end_htf_ns[-1] = self.ts_htf_ns[-1] + self.tf_ns

        self.bar_1m_start = np.searchsorted(self.ts_1m_ns, self.ts_htf_ns, side="left").astype(np.int64)
        self.bar_1m_end = np.searchsorted(self.ts_1m_ns, end_htf_ns, side="left").astype(np.int64)

        # 保底裁切（理論上不該需要，但避免極端資料造成 out-of-bound）
        self.bar_1m_start = np.clip(self.bar_1m_start, 0, self.m1_len)
        self.bar_1m_end = np.clip(self.bar_1m_end, 0, self.m1_len)

        # 1m index -> HTF bar index（O(1) 回推 minute 所屬的 HTF bar，給 Numba 批次撮合用）
        self.m1_to_htf = np.full(self.m1_len, -1, dtype=np.int64)
        for bi in range(self.htf_len):
            s = int(self.bar_1m_start[bi])
            e = int(self.bar_1m_end[bi])
            if s < 0:
                s = 0
            if e > self.m1_len:
                e = self.m1_len
            for mj in range(s, e):
                self.m1_to_htf[mj] = bi

        # 補齊極端邊界（理論上不會發生，但避免 -1 造成後續 out-of-bound）
        last = -1
        for mj in range(self.m1_len):
            v = int(self.m1_to_htf[mj])
            if v >= 0:
                last = v
            else:
                self.m1_to_htf[mj] = last

        first = -1
        for mj in range(self.m1_len - 1, -1, -1):
            v = int(self.m1_to_htf[mj])
            if v >= 0:
                first = v
            else:
                self.m1_to_htf[mj] = first

        self.m1_to_htf = np.clip(self.m1_to_htf, 0, self.htf_len - 1).astype(np.int64)

        # 進一步對齊檢查：每一根 HTF bar open 都必須在 1m time axis 中有完全一致的分鐘
        # 這個檢查放在建 ctx 時做，避免格點掃描跑很久才因缺分鐘中斷。
        _mis_i = -1
        for _i in range(self.htf_len):
            _s = int(self.bar_1m_start[_i])
            if _s < 0 or _s >= self.m1_len:
                _mis_i = _i
                break
            if int(self.ts_1m_ns[_s]) != int(self.ts_htf_ns[_i]):
                _mis_i = _i
                break
        if _mis_i >= 0:
            _s = int(self.bar_1m_start[_mis_i])
            _ts_htf = int(self.ts_htf_ns[_mis_i])
            _ts_1m = int(self.ts_1m_ns[_s]) if 0 <= _s < self.m1_len else -1
            _dt_htf = str(pd.to_datetime(_ts_htf, utc=True))
            _dt_1m = str(pd.to_datetime(_ts_1m, utc=True)) if _ts_1m > 0 else "NA"
            raise ValueError(
                "1m 精準撮合對齊失敗：主週期開盤分鐘在 1m CSV 中找不到完全一致的 ts。"
                f" first_mismatch_htf_index={_mis_i}, htf_ts={_dt_htf}, 1m_ts_at_start={_dt_1m}"
            )


def apply_1m_microfill(
    df_htf: pd.DataFrame,
    micro_ctx: "MicroFillContext",
    entry_idx_arr: np.ndarray,
    exit_idx_arr: np.ndarray,
    reverse_mode: bool,
    tp_pct: float,
    sl_pct: float,
    max_hold: int,
    fee_side: float,
    slippage: float,
    worst_case: bool,
    per_entry_mode: bool = False,
    highest_arr: Optional[np.ndarray] = None,
    lowest_arr: Optional[np.ndarray] = None,
):
    """把既有回測結果用 1m 做撮合精修，回傳同結構的 perbar/equity/trade arrays + entry/exit 的 1m index。"""

    n_htf = int(len(df_htf))
    n_tr = int(len(entry_idx_arr))

    if micro_ctx is None:
        raise ValueError("micro_ctx 不存在（你勾了 1m 精準撮合但沒有成功載入 1m CSV）")

    if int(getattr(micro_ctx, "htf_len", -1)) != n_htf:
        raise ValueError("micro_ctx 與目前 HTF 資料不一致（可能你換了主 CSV 但沒重載 1m CSV）")

    # 輸出陣列
    e_idx_new = entry_idx_arr.astype(np.int64, copy=True)
    x_idx_new = np.empty(n_tr, dtype=np.int64)
    e_px_new = np.empty(n_tr, dtype=np.float64)
    x_px_new = np.empty(n_tr, dtype=np.float64)
    tr_ret_new = np.empty(n_tr, dtype=np.float64)
    bars_held_new = np.empty(n_tr, dtype=np.int32)
    reasons_new = np.empty(n_tr, dtype=np.int8)

    entry_1m_idx_arr = np.full(n_tr, -1, dtype=np.int64)
    exit_1m_idx_arr = np.full(n_tr, -1, dtype=np.int64)

    for t in range(n_tr):
        entry_htf = int(entry_idx_arr[t])
        if entry_htf < 0 or entry_htf >= n_htf:
            raise ValueError(f"交易索引異常：entry_htf={entry_htf} 超界 (t={t})")

        # max_hold 仍以 HTF bar 數為準
        end_htf = entry_htf + int(max_hold)
        if end_htf >= n_htf:
            end_htf = n_htf - 1

        s1 = int(micro_ctx.bar_1m_start[entry_htf])
        e1 = int(micro_ctx.bar_1m_end[end_htf])
        if e1 > micro_ctx.m1_len:
            e1 = micro_ctx.m1_len

        if s1 >= e1 or s1 < 0 or s1 >= micro_ctx.m1_len:
            raise ValueError(f"1m 資料缺口：找不到 entry_htf={entry_htf} 到 end_htf={end_htf} 的 1m 區間 (t={t})")

        # 嚴格對齊：1m 的第一根必須剛好等於 entry bar 的 bar open
        if int(micro_ctx.ts_1m_ns[s1]) != int(micro_ctx.ts_htf_ns[entry_htf]):
            raise ValueError("1m CSV 的 ts 與主週期 ts 未對齊（請確認兩份 CSV 的 ts 都是 bar open 且同為 UTC）。")

        # 進場：用 entry bar 的第一根 1m open
        entry_1m_idx_arr[t] = s1
        entry_price = float(micro_ctx.o_1m[s1])

        # 取得該筆交易的 TP/SL 價位
        if per_entry_mode:
            if highest_arr is None or lowest_arr is None:
                raise ValueError("per_entry_mode=True 但沒有提供 highest_arr/lowest_arr")
            base_idx = entry_htf - 1
            if base_idx < 0 or base_idx >= len(highest_arr) or base_idx >= len(lowest_arr):
                raise ValueError("per_entry_mode 的 base_idx 超界，請檢查訊號/資料對齊")

            high_val = float(highest_arr[base_idx])
            low_val = float(lowest_arr[base_idx])

            is_ratio = (tp_pct > 0.5)

            if reverse_mode:
                # 對齊 simulate_short_core_per_entry 的邏輯
                if is_ratio:
                    _tp_ratio = 1.0 - (tp_pct - 1.0) if tp_pct >= 1.0 else tp_pct
                    _sl_ratio = 1.0 + (1.0 - sl_pct) if sl_pct <= 1.0 else sl_pct
                    tp_level = low_val * _tp_ratio
                    sl_level = high_val * _sl_ratio
                else:
                    tp_level = low_val * (1.0 - tp_pct)
                    sl_level = high_val * (1.0 + sl_pct)
            else:
                # 對齊 simulate_long_core_per_entry 的邏輯
                if is_ratio:
                    tp_level = high_val * tp_pct
                    sl_level = low_val * sl_pct
                else:
                    tp_level = high_val * (1.0 + tp_pct)
                    sl_level = low_val * (1.0 - sl_pct)
        else:
            # 標準模式：TP/SL 以 entry_price 為基準
            if reverse_mode:
                tp_level = entry_price * (1.0 - tp_pct)
                sl_level = entry_price * (1.0 + sl_pct)
            else:
                tp_level = entry_price * (1.0 + tp_pct)
                sl_level = entry_price * (1.0 - sl_pct)

        # 在 [s1, e1) 逐根 1m 檢查觸發
        h1 = micro_ctx.h_1m[s1:e1]
        l1 = micro_ctx.l_1m[s1:e1]

        if reverse_mode:
            hit_tp = (l1 <= tp_level)
            hit_sl = (h1 >= sl_level)
        else:
            hit_tp = (h1 >= tp_level)
            hit_sl = (l1 <= sl_level)

        hit_any = hit_tp | hit_sl
        hit_idx_rel = np.flatnonzero(hit_any)

        if hit_idx_rel.size > 0:
            k_rel = int(hit_idx_rel[0])
            k = s1 + k_rel
            exit_1m_idx_arr[t] = int(k)

            both = bool(hit_tp[k_rel] and hit_sl[k_rel])
            if both:
                if worst_case:
                    exit_price = float(sl_level)
                    reason = 3  # SL_samebar（同 1m 同時命中）
                else:
                    exit_price = float(tp_level)
                    reason = 4  # TP_samebar
            else:
                if bool(hit_sl[k_rel]):
                    exit_price = float(sl_level)
                    reason = 1
                else:
                    exit_price = float(tp_level)
                    reason = 2
        else:
            # TIME：用最後一根 1m close
            k = e1 - 1
            exit_1m_idx_arr[t] = int(k)
            exit_price = float(micro_ctx.c_1m[k])
            reason = 5

        # exit 對應到 HTF bar index（用 exit 1m 的 ts 反推）
        exit_ts_ns = int(micro_ctx.ts_1m_ns[exit_1m_idx_arr[t]])
        exit_htf = int(np.searchsorted(micro_ctx.ts_htf_ns, exit_ts_ns, side="right") - 1)
        if exit_htf < entry_htf:
            exit_htf = entry_htf
        if exit_htf > end_htf:
            exit_htf = end_htf

        # 計算淨報酬（對齊原本 simulate_* 核心）
        if reverse_mode:
            exec_entry = entry_price - float(slippage)
            exec_exit = exit_price + float(slippage)
            if exec_entry <= 0.0:
                exec_entry = 1e-12
            if exec_exit <= 0.0:
                exec_exit = 1e-12
            revenue = exec_entry * (1.0 - float(fee_side))
            cost = exec_exit * (1.0 + float(fee_side))
            net_ret = (revenue - cost) / exec_entry
        else:
            exec_entry = entry_price + float(slippage)
            exec_exit = exit_price - float(slippage)
            if exec_entry <= 0.0:
                exec_entry = 1e-12
            if exec_exit <= 0.0:
                exec_exit = 1e-12
            net_ret = ((exec_exit * (1.0 - float(fee_side))) / (exec_entry * (1.0 + float(fee_side)))) - 1.0

        x_idx_new[t] = int(exit_htf)
        e_px_new[t] = float(entry_price)
        x_px_new[t] = float(exit_price)
        tr_ret_new[t] = float(net_ret)
        bars_held_new[t] = int(exit_htf - entry_htf + 1)
        reasons_new[t] = int(reason)

    # 重新建 perbar/equity（因為 net_ret 可能已變）
    perbar_new = np.zeros(n_htf, dtype=np.float64)
    for t in range(n_tr):
        xi = int(x_idx_new[t])
        if 0 <= xi < n_htf:
            perbar_new[xi] += float(tr_ret_new[t])

    equity_new = np.ones(n_htf, dtype=np.float64)
    for k in range(1, n_htf):
        equity_new[k] = equity_new[k-1] * (1.0 + perbar_new[k])

    return (
        perbar_new,
        equity_new,
        e_idx_new,
        x_idx_new,
        e_px_new,
        x_px_new,
        tr_ret_new,
        bars_held_new,
        reasons_new,
        entry_1m_idx_arr,
        exit_1m_idx_arr,
    )

def simulate_laguerre_tema_1m_microfill(
    df_htf: pd.DataFrame,
    micro_ctx: "MicroFillContext",
    entry_sig: np.ndarray,
    atr_sltp_arr: np.ndarray,
    atr_trail_arr: np.ndarray,
    atr_act_arr: np.ndarray,
    logic_exit_arr: np.ndarray,
    sl_coef: float,
    tp_coef: float,
    ts_dist_coef: float,
    ts_act_coef: float,
    slippage: float,
    max_hold: int,
    fee_side: float = 0.0002,
):
    """
    LaguerreRSI_TEMA 專用：用 1m 資料做「逐分鐘」精準撮合。
    - 進場訊號仍以主週期（HTF）計算：訊號出現在 bar i，於 bar i+1 的 open 進場（同原策略）。
    - 出場（SL/TP/Trail/Logic/Time）改用 1m OHLC 逐分鐘判斷，避免只用 HTF high/low 的路徑誤差。
    - 注意：此策略 max_hold 語意與一般 core 不同：時間出場條件是「bars_held >= max_hold」。
      所以 max_hold=1 代表只持有 1 根（進場那根）到 close 就出場。

    回傳結構對齊原本 simulator：
    (perbar, equity, entry_idx_arr, exit_idx_arr, entry_px_arr, exit_px_arr, net_ret_arr, bars_held_arr, reason_arr, entry_1m_idx_arr, exit_1m_idx_arr)
    """
    n = len(df_htf)
    if micro_ctx is None:
        raise ValueError("simulate_laguerre_tema_1m_microfill: micro_ctx is None")
    if getattr(micro_ctx, "htf_len", None) != n:
        raise ValueError(f"simulate_laguerre_tema_1m_microfill: micro_ctx.htf_len={getattr(micro_ctx, 'htf_len', None)} != len(df_htf)={n}")

    entry_sig = np.asarray(entry_sig, dtype=np.bool_)
    # 多預留一些空間，避免極端情況爆掉
    max_trades = int(np.sum(entry_sig)) + 1000

    perbar = np.zeros(n, dtype=np.float64)

    entry_idx_arr = np.full(max_trades, -1, dtype=np.int64)
    exit_idx_arr  = np.full(max_trades, -1, dtype=np.int64)
    entry_px_arr  = np.zeros(max_trades, dtype=np.float64)
    exit_px_arr   = np.zeros(max_trades, dtype=np.float64)
    net_ret_arr   = np.zeros(max_trades, dtype=np.float64)
    bars_held_arr = np.zeros(max_trades, dtype=np.int32)
    reason_arr    = np.zeros(max_trades, dtype=np.int8)   # 1:SL, 2:TP, 3:Trail, 4:Logic, 5:Time
    entry_1m_idx_arr = np.full(max_trades, -1, dtype=np.int64)
    exit_1m_idx_arr  = np.full(max_trades, -1, dtype=np.int64)

    # 快取 micro_ctx 欄位，避免 Python attribute lookup 太慢
    m1_len = int(micro_ctx.m1_len)
    ts_1m_ns = micro_ctx.ts_1m_ns
    ts_htf_ns = micro_ctx.ts_htf_ns
    bar_1m_start = micro_ctx.bar_1m_start
    bar_1m_end = micro_ctx.bar_1m_end
    o1 = micro_ctx.o_1m
    h1 = micro_ctx.h_1m
    l1 = micro_ctx.l_1m
    c1 = micro_ctx.c_1m

    tcount = 0
    in_pos = False

    entry_idx = -1
    entry_price = 0.0
    entry_1m_idx = -1

    # Position state
    curr_sl = 0.0
    curr_tp = 0.0
    curr_trail_act_price = 0.0
    curr_trail_offset_val = 0.0
    is_trailing_active = False

    _max_hold = int(max_hold)
    if _max_hold < 1:
        _max_hold = 1

    for i in range(n - 1):
        if not in_pos:
            # 訊號 at close(i)，進場 at open(i+1)
            if entry_sig[i]:
                entry_idx = i + 1
                if entry_idx >= n:
                    break

                s1 = int(bar_1m_start[entry_idx])
                if s1 < 0 or s1 >= m1_len:
                    raise ValueError(f"1m 精準撮合對齊失敗：entry_htf={entry_idx}, bar_1m_start={s1}, m1_len={m1_len}")

                # 嚴格對齊：HTF bar open 必須存在於 1m time axis
                if int(ts_1m_ns[s1]) != int(ts_htf_ns[entry_idx]):
                    raise ValueError(
                        f"1m 精準撮合對齊失敗：entry_htf={entry_idx}, ts_htf={int(ts_htf_ns[entry_idx])}, ts_1m_at_start={int(ts_1m_ns[s1])}"
                    )

                entry_1m_idx = s1
                entry_price = float(o1[s1])

                # Snapshot ATRs at signal bar i (與原 Numba 版本一致)
                base_atr_sltp = float(atr_sltp_arr[i])
                base_atr_trail = float(atr_trail_arr[i])
                base_atr_act = float(atr_act_arr[i])

                # Levels
                curr_sl = entry_price - (float(sl_coef) * base_atr_sltp)
                curr_tp = entry_price + (float(tp_coef) * base_atr_sltp)

                # Trailing setup
                curr_trail_act_price = entry_price + (float(ts_act_coef) * base_atr_act)
                curr_trail_offset_val = float(ts_dist_coef) * base_atr_trail
                is_trailing_active = False

                in_pos = True
        else:
            # 在持倉中：評估 bar i（i >= entry_idx）
            bar_s = int(bar_1m_start[i])
            bar_e = int(bar_1m_end[i])

            if bar_s < 0:
                bar_s = 0
            if bar_e > m1_len:
                bar_e = m1_len
            if bar_s >= bar_e:
                raise ValueError(f"1m 精準撮合：HTF bar {i} 對應不到任何 1m 資料（bar_s={bar_s}, bar_e={bar_e}）")

            exit_type = 0
            exit_p = 0.0
            exit_1m_idx = -1

            j = bar_s
            while j < bar_e:
                hh = float(h1[j])
                ll = float(l1[j])

                # --- 1) 更新 Trailing（以 minute high 逐步逼近；仍維持保守順序：先 high 再 low） ---
                if not is_trailing_active:
                    if hh >= curr_trail_act_price:
                        is_trailing_active = True
                        new_sl = hh - curr_trail_offset_val
                        if new_sl > curr_sl:
                            curr_sl = new_sl
                else:
                    new_sl = hh - curr_trail_offset_val
                    if new_sl > curr_sl:
                        curr_sl = new_sl

                # --- 2) 檢查出場（SL 優先於 TP，符合原策略的保守行為） ---
                hit_sl = (ll <= curr_sl)
                hit_tp = (hh >= curr_tp)

                if hit_sl:
                    exit_type = 3 if is_trailing_active else 1
                    exit_p = curr_sl
                    exit_1m_idx = j
                    break
                if hit_tp:
                    exit_type = 2
                    exit_p = curr_tp
                    exit_1m_idx = j
                    break

                j += 1

            # 若此 bar 內沒打到硬性停損/停利，再檢查邏輯出場與時間出場（都在 bar close）
            if exit_type == 0:
                if float(logic_exit_arr[i]) > 0.5:
                    exit_type = 4
                    exit_1m_idx = bar_e - 1
                    exit_p = float(c1[exit_1m_idx])
                elif (i - entry_idx + 1) >= _max_hold:
                    exit_type = 5
                    exit_1m_idx = bar_e - 1
                    exit_p = float(c1[exit_1m_idx])

            if exit_type > 0:
                # 回報計算：沿用原 Laguerre Numba 版本（revenue - cost / exec_entry）
                exec_entry = entry_price + float(slippage)
                exec_exit = exit_p - float(slippage)
                if exec_entry <= 0.0:
                    exec_entry = 1e-12
                if exec_exit <= 0.0:
                    exec_exit = 1e-12
                revenue = exec_exit * (1.0 - float(fee_side))
                cost = exec_entry * (1.0 + float(fee_side))
                net_ret = (revenue - cost) / exec_entry

                perbar[i] += net_ret

                if tcount < max_trades:
                    entry_idx_arr[tcount] = int(entry_idx)
                    exit_idx_arr[tcount] = int(i)
                    entry_px_arr[tcount] = float(entry_price)
                    exit_px_arr[tcount] = float(exit_p)
                    net_ret_arr[tcount] = float(net_ret)
                    bars_held_arr[tcount] = int(i - entry_idx + 1)
                    reason_arr[tcount] = int(exit_type)
                    entry_1m_idx_arr[tcount] = int(entry_1m_idx)
                    exit_1m_idx_arr[tcount] = int(exit_1m_idx)
                    tcount += 1

                # Reset position
                in_pos = False
                entry_idx = -1
                entry_price = 0.0
                entry_1m_idx = -1
                curr_sl = 0.0
                curr_tp = 0.0
                curr_trail_act_price = 0.0
                curr_trail_offset_val = 0.0
                is_trailing_active = False

    equity = np.ones(n, dtype=np.float64)
    for k in range(1, n):
        equity[k] = equity[k - 1] * (1.0 + perbar[k])

    return (
        perbar,
        equity,
        entry_idx_arr[:tcount],
        exit_idx_arr[:tcount],
        entry_px_arr[:tcount],
        exit_px_arr[:tcount],
        net_ret_arr[:tcount],
        bars_held_arr[:tcount],
        reason_arr[:tcount],
        entry_1m_idx_arr[:tcount],
        exit_1m_idx_arr[:tcount],
    )


def simulate_tema_rsi_1m_microfill(
    df_htf: pd.DataFrame,
    micro_ctx: "MicroFillContext",
    entry_sig: np.ndarray,
    act_pct_arr: np.ndarray,
    trail_offset_arr: np.ndarray,
    tp_pct_arr: np.ndarray,
    sl_pct_arr: np.ndarray,
    stake_pct_arr: np.ndarray,
    entry_reason_input_arr: np.ndarray,
    slippage: float,
    max_hold: int,
    fee_side: float = 0.0002,
):
    """
    TEMA_RSI 專用：用 1m 資料做「逐分鐘」精準撮合。
    - 進場訊號仍以主週期（HTF）計算：訊號出現在 bar i，於 bar i+1 的 open 進場（同原策略）。
    - 出場（SL/TP/Trailing/Time）改用 1m OHLC 逐分鐘判斷。
    - 注意：此策略 max_hold 語意：時間出場條件是「bars_held >= max_hold」。

    回傳結構：
    (perbar, equity, entry_idx_arr, exit_idx_arr, entry_px_arr, exit_px_arr, net_ret_arr, bars_held_arr, reason_arr, entry_reasons_arr, entry_1m_idx_arr, exit_1m_idx_arr)
    """
    n = len(df_htf)
    if micro_ctx is None:
        raise ValueError("simulate_tema_rsi_1m_microfill: micro_ctx is None")
    if getattr(micro_ctx, "htf_len", None) != n:
        raise ValueError(f"simulate_tema_rsi_1m_microfill: micro_ctx.htf_len={getattr(micro_ctx, 'htf_len', None)} != len(df_htf)={n}")

    entry_sig = np.asarray(entry_sig, dtype=np.bool_)
    max_trades = int(np.sum(entry_sig)) + 1000

    perbar = np.zeros(n, dtype=np.float64)

    entry_idx_arr = np.full(max_trades, -1, dtype=np.int64)
    exit_idx_arr  = np.full(max_trades, -1, dtype=np.int64)
    entry_px_arr  = np.zeros(max_trades, dtype=np.float64)
    exit_px_arr   = np.zeros(max_trades, dtype=np.float64)
    net_ret_arr   = np.zeros(max_trades, dtype=np.float64)
    bars_held_arr = np.zeros(max_trades, dtype=np.int32)
    reason_arr    = np.zeros(max_trades, dtype=np.int8)   # 1:SL, 2:TP, 3:Trailing, 5:Time
    entry_reasons_arr = np.zeros(max_trades, dtype=np.int8)
    entry_1m_idx_arr = np.full(max_trades, -1, dtype=np.int64)
    exit_1m_idx_arr  = np.full(max_trades, -1, dtype=np.int64)

    # 快取 micro_ctx 欄位
    m1_len = int(micro_ctx.m1_len)
    ts_1m_ns = micro_ctx.ts_1m_ns
    ts_htf_ns = micro_ctx.ts_htf_ns
    bar_1m_start = micro_ctx.bar_1m_start
    bar_1m_end = micro_ctx.bar_1m_end
    o1 = micro_ctx.o_1m
    h1 = micro_ctx.h_1m
    l1 = micro_ctx.l_1m
    c1 = micro_ctx.c_1m

    tcount = 0
    in_pos = False

    entry_idx = -1
    entry_price = 0.0
    entry_1m_idx = -1

    # Position state
    curr_fixed_sl = 0.0
    curr_fixed_tp = 0.0
    curr_stake = 1.0
    curr_trail_activation_price = 0.0
    curr_trail_offset = 0.0
    curr_trail_stop_price = 0.0
    is_trail_active = False

    _max_hold = int(max_hold)
    if _max_hold < 1:
        _max_hold = 1

    for i in range(n - 1):
        if not in_pos:
            if entry_sig[i]:
                entry_idx = i + 1
                if entry_idx >= n:
                    break

                s1 = int(bar_1m_start[entry_idx])
                if s1 < 0 or s1 >= m1_len:
                    raise ValueError(f"1m 精準撮合對齊失敗：entry_htf={entry_idx}, bar_1m_start={s1}, m1_len={m1_len}")

                if int(ts_1m_ns[s1]) != int(ts_htf_ns[entry_idx]):
                    raise ValueError(
                        f"1m 精準撮合對齊失敗：entry_htf={entry_idx}, ts_htf={int(ts_htf_ns[entry_idx])}, ts_1m_at_start={int(ts_1m_ns[s1])}"
                    )

                entry_1m_idx = s1
                entry_price = float(o1[s1])

                # 參數以 signal bar i 為準（與原 Numba 版本一致）
                sl_pct = float(sl_pct_arr[i])
                tp_pct = float(tp_pct_arr[i])
                act_pct = float(act_pct_arr[i])

                curr_stake = float(stake_pct_arr[i])
                curr_trail_offset = float(trail_offset_arr[i])

                curr_fixed_sl = entry_price * (1.0 - sl_pct)

                # TP 可被停用：tp_pct <= -0.99
                if tp_pct <= -0.99:
                    curr_fixed_tp = 1e12
                else:
                    curr_fixed_tp = entry_price * (1.0 + tp_pct)

                curr_trail_activation_price = entry_price * (1.0 + act_pct)
                curr_trail_stop_price = 0.0
                is_trail_active = False

                # entry reason（用於拆解績效）
                if tcount < max_trades:
                    entry_reasons_arr[tcount] = int(entry_reason_input_arr[i])

                in_pos = True
        else:
            bar_s = int(bar_1m_start[i])
            bar_e = int(bar_1m_end[i])

            if bar_s < 0:
                bar_s = 0
            if bar_e > m1_len:
                bar_e = m1_len
            if bar_s >= bar_e:
                raise ValueError(f"1m 精準撮合：HTF bar {i} 對應不到任何 1m 資料（bar_s={bar_s}, bar_e={bar_e}）")

            exit_type = 0
            exit_p = 0.0
            exit_1m_idx = -1

            j = bar_s
            while j < bar_e:
                hh = float(h1[j])
                ll = float(l1[j])

                # --- 1) 更新 trailing ---
                if not is_trail_active:
                    if hh >= curr_trail_activation_price:
                        is_trail_active = True
                        curr_trail_stop_price = hh - curr_trail_offset
                else:
                    potential_new_stop = hh - curr_trail_offset
                    if potential_new_stop > curr_trail_stop_price:
                        curr_trail_stop_price = potential_new_stop

                # --- 2) 算 effective SL（fixed vs trailing 擇大） ---
                effective_sl = curr_fixed_sl
                effective_sl_type = 1
                if is_trail_active and (curr_trail_stop_price > effective_sl):
                    effective_sl = curr_trail_stop_price
                    effective_sl_type = 3

                # --- 3) 檢查出場（SL 優先於 TP） ---
                if ll <= effective_sl:
                    exit_type = effective_sl_type
                    exit_p = effective_sl
                    exit_1m_idx = j
                    break

                if hh >= curr_fixed_tp:
                    exit_type = 2
                    exit_p = curr_fixed_tp
                    exit_1m_idx = j
                    break

                j += 1

            if exit_type == 0:
                if (i - entry_idx + 1) >= _max_hold:
                    exit_type = 5
                    exit_1m_idx = bar_e - 1
                    exit_p = float(c1[exit_1m_idx])

            if exit_type > 0:
                # 回報計算：沿用原 TEMA_RSI Numba 版本（raw_net_ret * stake）
                exec_entry = entry_price + float(slippage)
                exec_exit = exit_p - float(slippage)
                if exec_entry <= 0.0:
                    exec_entry = 1e-12
                if exec_exit <= 0.0:
                    exec_exit = 1e-12
                revenue = exec_exit * (1.0 - float(fee_side))
                cost = exec_entry * (1.0 + float(fee_side))
                raw_net_ret = (revenue - cost) / exec_entry
                net_ret = raw_net_ret * curr_stake

                perbar[i] += net_ret

                if tcount < max_trades:
                    entry_idx_arr[tcount] = int(entry_idx)
                    exit_idx_arr[tcount] = int(i)
                    entry_px_arr[tcount] = float(entry_price)
                    exit_px_arr[tcount] = float(exit_p)
                    net_ret_arr[tcount] = float(net_ret)
                    bars_held_arr[tcount] = int(i - entry_idx + 1)
                    reason_arr[tcount] = int(exit_type)
                    entry_1m_idx_arr[tcount] = int(entry_1m_idx)
                    exit_1m_idx_arr[tcount] = int(exit_1m_idx)
                    tcount += 1

                # Reset position
                in_pos = False
                entry_idx = -1
                entry_price = 0.0
                entry_1m_idx = -1

                curr_fixed_sl = 0.0
                curr_fixed_tp = 0.0
                curr_stake = 1.0
                curr_trail_activation_price = 0.0
                curr_trail_offset = 0.0
                curr_trail_stop_price = 0.0
                is_trail_active = False

    equity = np.ones(n, dtype=np.float64)
    for k in range(1, n):
        equity[k] = equity[k - 1] * (1.0 + perbar[k])

    return (
        perbar,
        equity,
        entry_idx_arr[:tcount],
        exit_idx_arr[:tcount],
        entry_px_arr[:tcount],
        exit_px_arr[:tcount],
        net_ret_arr[:tcount],
        bars_held_arr[:tcount],
        reason_arr[:tcount],
        entry_reasons_arr[:tcount],
        entry_1m_idx_arr[:tcount],
        exit_1m_idx_arr[:tcount],
    )


def app():
    # ---- 預設值（頁面重新整理後仍固定這些預設）----
    # 只在 key 不存在時 setdefault，避免覆蓋使用者當次操作
    st.session_state.setdefault("use_1m_fill", True)
    st.session_state.setdefault("default_family", "TEMA_RSI")
    st.session_state.setdefault("slippage_ticks", 120)
    st.session_state.setdefault("tick_size", 0.01)
    st.session_state.setdefault("use_torch_compile", True)

    st.title("多指標格點回測控制台 (羊肉爐團隊專屬)")
    st.caption("以次根開盤成交；支援停利、停損與最長持倉；手續費與滑點可自訂。")

    with st.sidebar:
        st.subheader("行情資料")

        symbol_default = str(st.session_state.get("bm_symbol", "BTC_USDT")).strip()
        symbol = st.text_input("交易對", value=symbol_default)

        tf_default = str(st.session_state.get("bm_timeframe", "30m")).strip()
        if tf_default not in BITMART_UI_TIMEFRAMES:
            tf_default = "30m"
        tf_options = list(BITMART_UI_TIMEFRAMES.keys())
        tf_label = st.selectbox("主週期", options=tf_options, index=tf_options.index(tf_default))
        main_step_min = int(BITMART_UI_TIMEFRAMES[tf_label])

        auto_sync = st.checkbox("自動更新", value=bool(st.session_state.get("bm_auto_sync", True)))
        force_full = st.checkbox("重建資料檔（全量）", value=False)

        st.session_state["bm_symbol"] = symbol
        st.session_state["bm_timeframe"] = tf_label
        st.session_state["bm_auto_sync"] = bool(auto_sync)

        csv_status = _csv_quick_status(symbol, main_step_min)
        csv_1m_status = _csv_quick_status(symbol, 1)

        col1, col2 = st.columns(2)
        do_sync = col1.button("立即同步", use_container_width=True)
        show_paths = col2.button("檔案路徑", use_container_width=True)

        


        progress = None
        progress_text = None
        if do_sync or force_full or (not bool(csv_status.get("exists"))) or (not bool(csv_1m_status.get("exists"))):
            progress = st.progress(0.0)
            progress_text = st.empty()

            def _ui_progress(p: float, msg: str) -> None:
                if progress is not None:
                    progress.progress(float(max(0.0, min(1.0, p))))
                if progress_text is not None:
                    progress_text.text(str(msg))

            path, path_1m = ensure_bitmart_data(
                symbol=symbol,
                main_step_min=main_step_min,
                years=3,
                auto_sync=bool(auto_sync),
                force_full=bool(force_full),
                progress_cb=_ui_progress
            )
        else:
            path, path_1m = ensure_bitmart_data(
                symbol=symbol,
                main_step_min=main_step_min,
                years=3,
                auto_sync=bool(auto_sync),
                force_full=False,
                progress_cb=None
            )

        if show_paths:
            st.code(f"主週期: {path}\n1m: {path_1m}")

        st.markdown("---")

        use_1m_fill = st.checkbox(
            "啟用 1m 撮合",
            value=bool(st.session_state.get("use_1m_fill", True)),
            help="啟用後：進場、出場、停利、停損以 1 分鐘資料逐筆檢查。"
        )

        
        st.session_state["use_1m_fill"] = bool(use_1m_fill)
        st.session_state["path_1m_csv"] = str(path_1m).strip()

        st.markdown("---")
        st.subheader("策略與參數範圍")

        _default_family = str(st.session_state.get("default_family", "TEMA_RSI"))
        if _default_family not in INDICATOR_FAMILIES:
            _default_family = INDICATOR_FAMILIES[0]
        family = st.selectbox(
            "指標家族",
            options=INDICATOR_FAMILIES,
            index=INDICATOR_FAMILIES.index(_default_family),
            key="family_selectbox"
        )
        st.session_state["default_family"] = str(family)


        ui_params = {}
        # 動態表單
        if family == "RSI":
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["rsi_p_min"] = st.number_input("RSI週期(K棒計算) 最小", 2, 200, 29)
                ui_params["rsi_p_step"] = st.number_input("RSI週期(K棒計算) 搜尋步長", 1, 50, 1)
            with col2:
                ui_params["rsi_p_max"] = st.number_input("RSI週期(K棒計算)", 2, 400, 30)
            with col3:
                st.write("")
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["rsi_lv_min"] = st.number_input("進場門檻最小", 1, 99, 20)
                ui_params["rsi_lv_step"] = st.number_input("門檻搜尋步長", 1, 20, 5)
            with col2:
                ui_params["rsi_lv_max"] = st.number_input("進場門檻最大", 1, 99, 25)
            with col3:
                st.markdown("進場條件：RSI ≤ 門檻")

        elif family in ["SMA_Cross", "EMA_Cross", "HMA_Cross", "DEMA_Cross", "TEMA_Cross", "WMA_Cross"]:
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["fast_min"] = st.number_input("快線最小", 2, 200, 29)
                ui_params["fast_step"] = st.number_input("快線搜尋步長", 1, 50, 1)
            with col2:
                ui_params["fast_max"] = st.number_input("快線最大", 2, 500, 30)
            with col3:
                st.markdown("**入場**：快線上穿慢線")

            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["slow_min"] = st.number_input("慢線最小", 3, 400, 59)
                ui_params["slow_step"] = st.number_input("慢線搜尋步長", 1, 50, 1)
            with col2:
                ui_params["slow_max"] = st.number_input("慢線最大", 3, 800, 60)
            with col3:
                st.write("")

        elif family in ["MACD_Cross", "PPO_Cross", "PVO_Cross"]:
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["fast_min"] = st.number_input("快EMA最小", 2, 200, 8)
                ui_params["fast_step"] = st.number_input("搜尋步長", 1, 50, 1)
            with col2:
                ui_params["fast_max"] = st.number_input("快EMA最大", 2, 400, 18)
            with col3:
                st.write("")
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["slow_min"] = st.number_input("慢EMA最小", 3, 400, 17)
                ui_params["slow_step"] = st.number_input("搜尋步長", 1, 50, 1)
            with col2:
                ui_params["slow_max"] = st.number_input("慢EMA最大", 3, 800, 34)
            with col3:
                st.write("")
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["sig_min"] = st.number_input("Signal 最小", 2, 100, 5)
                ui_params["sig_step"] = st.number_input("搜尋步長", 1, 20, 1)
            with col2:
                ui_params["sig_max"] = st.number_input("Signal 最大", 2, 200, 12)
            with col3:
                st.markdown("**入場**：線上穿 signal")

        elif family == "Bollinger_Touch":
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["bb_p_min"] = st.number_input("Period 最小", 5, 200, 29)
                ui_params["bb_p_step"] = st.number_input("搜尋步長", 1, 50, 1)
            with col2:
                ui_params["bb_p_max"] = st.number_input("Period 最大", 5, 400, 30)
            with col3:
                st.write("")
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["bb_n_min"] = st.number_input("Std 最小", 1.0, 5.0, 1.5, step=0.1)
                ui_params["bb_n_step"] = st.number_input("Std 搜尋步長", 0.1, 2.0, 0.1, step=0.1)
            with col2:
                ui_params["bb_n_max"] = st.number_input("Std 最大", 1.0, 5.0, 2.5, step=0.1)
            with col3:
                st.markdown("**入場**：收盤觸及下軌")

        elif family == "Stoch_Oversold":
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["k_min"] = st.number_input("%K 最小", 3, 100, 10)
                ui_params["k_step"] = st.number_input("搜尋步長", 1, 20, 1)
            with col2:
                ui_params["k_max"] = st.number_input("%K 最大", 3, 200, 20)
            with col3:
                st.write("")
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["d_min"] = st.number_input("%D 最小", 2, 50, 3)
                ui_params["d_step"] = st.number_input("搜尋步長", 1, 20, 1)
            with col2:
                ui_params["d_max"] = st.number_input("%D 最大", 2, 100, 5)
            with col3:
                st.write("")
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["stoch_lv_min"] = st.number_input("進場門檻最小", 1, 50,25)
                ui_params["stoch_lv_step"] = st.number_input("搜尋步長", 1, 20, 5)
            with col2:
                ui_params["stoch_lv_max"] = st.number_input("進場門檻最大", 1, 80, 25)
            with col3:
                st.markdown("**入場**：%K ≤ 門檻")

        elif family in ["CCI_Oversold", "WillR_Oversold", "MFI_Oversold"]:
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["p_min"] = st.number_input("Period 最小", 5, 200, 10)
                ui_params["p_step"] = st.number_input("搜尋步長", 1, 50, 1)
            with col2:
                ui_params["p_max"] = st.number_input("Period 最大", 5, 400, 30)
            with col3:
                st.write("")
            col1, col2, col3 = st.columns(3)
            default_lv = -100 if family == "CCI_Oversold" else (-80 if family == "WillR_Oversold" else 20)
            with col1:
                ui_params["lv_min"] = st.number_input("進場門檻最小", -200, 200, default_lv-20)
                ui_params["lv_step"] = st.number_input("搜尋步長", 1, 50, 5)
            with col2:
                ui_params["lv_max"] = st.number_input("進場門檻最大", -200, 200, default_lv+20)
            with col3:
                st.markdown("**入場**：指標 ≤ 門檻")

        elif family in ["Donchian_Breakout"]:
            ui_params["look_min"] = st.number_input("Lookback 最小", 5, 200, 20)
            ui_params["look_step"] = st.number_input("搜尋步長", 1, 50, 1)
            ui_params["look_max"] = st.number_input("Lookback 最大", 5, 400, 55)

        elif family in ["ADX_DI_Cross", "Aroon_Cross", "Aroon_Osc_Threshold", "KAMA_Cross", "TRIX_Cross", "DPO_Revert", "Vortex_Cross"]:
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["p_min"] = st.number_input("Period 最小", 5, 200, 10)
                ui_params["p_step"] = st.number_input("搜尋步長", 1, 50, 1)
            with col2:
                ui_params["p_max"] = st.number_input("Period 最大", 5, 400, 30)
            with col3:
                st.write("")
            if family == "Aroon_Osc_Threshold":
                col1, col2, col3 = st.columns(3)
                with col1:
                    ui_params["thr_min"] = st.number_input("門檻最小", -100.0, 100.0, 0.0, step=1.0)
                    ui_params["thr_step"] = st.number_input("門檻搜尋步長", 0.1, 50.0, 5.0, step=0.1)
                with col2:
                    ui_params["thr_max"] = st.number_input("門檻最大", -100.0, 100.0, 20.0, step=1.0)
                with col3:
                    st.write("")

        elif family in ["ROC_Threshold", "CMF_Threshold", "EFI_Threshold"]:
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["p_min"] = st.number_input("Period 最小", 2, 200, 10)
                ui_params["p_step"] = st.number_input("搜尋步長", 1, 50, 1)
            with col2:
                ui_params["p_max"] = st.number_input("Period 最大", 2, 400, 30)
            with col3:
                st.write("")
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["thr_min"] = st.number_input("門檻最小", -5.0, 5.0, 0.0, step=0.1)
                ui_params["thr_step"] = st.number_input("搜尋步長", 0.1, 2.0, 0.1, step=0.1)
            with col2:
                ui_params["thr_max"] = st.number_input("門檻最大", -5.0, 5.0, 1.0, step=0.1)
            with col3:
                st.write("")

        elif family == "ATR_Band_Break":
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["p_min"] = st.number_input("ATR Period 最小", 2, 200, 10)
                ui_params["p_step"] = st.number_input("搜尋步長", 1, 50, 1)
            with col2:
                ui_params["p_max"] = st.number_input("ATR Period 最大", 2, 400, 30)
            with col3:
                st.write("")
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["mult_min"] = st.number_input("倍數最小", 0.5, 10.0, 1.0, step=0.1)
                ui_params["mult_step"] = st.number_input("搜尋步長", 0.1, 5.0, 0.1, step=0.1)
            with col2:
                ui_params["mult_max"] = st.number_input("倍數最大", 0.5, 10.0, 2.0, step=0.1)
            with col3:
                st.write("")

        elif family == "BB_PercentB_Revert":
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["p_min"] = st.number_input("BB Period 最小", 5, 200, 10)
                ui_params["p_step"] = st.number_input("搜尋步長", 1, 50, 1)
            with col2:
                ui_params["p_max"] = st.number_input("BB Period 最大", 5, 400, 30)
            with col3:
                ui_params["nstd"] = st.number_input("Std", 1.0, 5.0, 2.0, step=0.1)
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["thr_min"] = st.number_input("%B 門檻最小", 0.0, 1.0, 0.02, step=0.01)
                ui_params["thr_step"] = st.number_input("搜尋步長", 0.01, 0.5, 0.03, step=0.01)
            with col2:
                ui_params["thr_max"] = st.number_input("%B 門檻最大", 0.0, 1.0, 0.15, step=0.01)
            with col3:
                st.write("")

        elif family == "Volatility_Squeeze":
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["p_min"] = st.number_input("Period 最小", 5, 200, 20)
                ui_params["p_step"] = st.number_input("搜尋步長", 1, 50, 1)
            with col2:
                ui_params["p_max"] = st.number_input("Period 最大", 5, 400, 40)
            with col3:
                st.write("")
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["nstd_min"] = st.number_input("Std 最小", 1.0, 5.0, 2.0, step=0.1)
                ui_params["nstd_step"] = st.number_input("搜尋步長", 0.1, 2.0, 0.1, step=0.1)
            with col2:
                ui_params["nstd_max"] = st.number_input("Std 最大", 1.0, 5.0, 3.0, step=0.1)
            with col3:
                st.write("")
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["q_min"] = st.number_input("縮窄分位最小", 0.05, 0.5, 0.2, step=0.01)
                ui_params["q_step"] = st.number_input("搜尋步長", 0.01, 0.2, 0.01, step=0.01)
            with col2:
                ui_params["q_max"] = st.number_input("縮窄分位最大", 0.05, 0.9, 0.4, step=0.01)
            with col3:
                st.write("")
        elif family == "OB_FVG":
            st.markdown("##### 趨勢與OB定義")
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["obfvg_n_min"] = st.number_input("連續 N 根(分)最小", 2, 20, 3)
                ui_params["obfvg_n_step"] = st.number_input("N 步長", 1, 5, 1)
            with col2:
                ui_params["obfvg_n_max"] = st.number_input("連續 N 根(分)最大", 2, 20, 5)
            with col3:
                st.caption("定義趨勢K棒數")

            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["obfvg_w_min"] = st.number_input("監測窗口 W 最小", 5, 200, 20)
                ui_params["obfvg_w_step"] = st.number_input("W 步長", 1, 50, 10)
            with col2:
                ui_params["obfvg_w_max"] = st.number_input("監測窗口 W 最大", 5, 200, 40)
            with col3:
                st.caption("形成後 W 根內有效 (Breakout-Retest)")

            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["obfvg_r_min"] = st.number_input("實體漲幅 r 最小", 0.0, 0.05, 0.001, format="%.4f")
                ui_params["obfvg_r_step"] = st.number_input("r 步長", 0.0001, 0.01, 0.0005, format="%.4f")
            with col2:
                ui_params["obfvg_r_max"] = st.number_input("實體漲幅 r 最大", 0.0, 0.05, 0.005, format="%.4f")
            with col3:
                st.caption("(收-開)/開 > r")

            st.markdown("##### 成交量條件")
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["obfvg_h_min"] = st.number_input("均量週期 h 最小", 5, 100, 20)
                ui_params["obfvg_h_step"] = st.number_input("h 步長", 5, 20, 10)
            with col2:
                ui_params["obfvg_h_max"] = st.number_input("均量週期 h 最大", 5, 100, 60)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["obfvg_g_min"] = st.number_input("量倍數 g 最小", 0.5, 5.0, 1.0)
                ui_params["obfvg_g_step"] = st.number_input("g 步長", 0.1, 2.0, 0.5)
            with col2:
                ui_params["obfvg_g_max"] = st.number_input("量倍數 g 最大", 0.5, 10.0, 2.0)
            with col3:
                st.caption("Vol > Avg(h) * g")

            st.markdown("##### 區間與進場參數")
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["obfvg_a_min"] = st.number_input("OB下軌偏移 a 最小", 0.0, 2.0, 0.98, format="%.3f")
                ui_params["obfvg_a_step"] = st.number_input("a 步長", 0.001, 0.1, 0.01, format="%.3f")
            with col2:
                ui_params["obfvg_a_max"] = st.number_input("OB下軌偏移 a 最大", 0.0, 2.0, 1.0, format="%.3f")
            with col3:
                st.caption("回測需跌至 OB下軌 * a")

            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["obfvg_thr_min"] = st.number_input("上漲確認門檻 最小", 0.0, 2.0, 1.002, format="%.4f") # 相對 OB上軌
                ui_params["obfvg_thr_step"] = st.number_input("門檻 步長", 0.001, 0.1, 0.001, format="%.4f")
            with col2:
                ui_params["obfvg_thr_max"] = st.number_input("上漲確認門檻 最大", 0.0, 2.0, 1.005, format="%.4f")
            with col3:
                st.caption("進場前需先漲至 OB上軌 * 門檻")

            st.markdown("##### RSI 動能濾網")
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["obfvg_rsi_p_min"] = st.number_input("RSI週期 最小", 2, 200, 14)
                ui_params["obfvg_rsi_p_step"] = st.number_input("RSI週期 步長", 1, 50, 1)
            with col2:
                ui_params["obfvg_rsi_p_max"] = st.number_input("RSI週期 最大", 2, 200, 14)
            with col3:
                st.caption("計算RSI的K棒週期")

            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["obfvg_rsi_diff_min"] = st.number_input("RSI差距 j% 最小", -1.0, 2.0, 0.0, step=0.01)
                ui_params["obfvg_rsi_diff_step"] = st.number_input("RSI差距 j% 步長", 0.01, 0.5, 0.05, step=0.01)
            with col2:
                ui_params["obfvg_rsi_diff_max"] = st.number_input("RSI差距 j% 最大", -1.0, 2.0, 0.1, step=0.01)
            with col3:
                st.caption("入場條件：RSI > OB_RSI * (1 + j%)")
            
            # FVG 參數固定或設為較寬範圍讓程式自動計算
            ui_params["obfvg_x"] = 1.0
            ui_params["obfvg_y"] = 1.0
            ui_params["obfvg_ob_range_based"] = st.checkbox("啟用區間 TP/SL 模式", value=False, key="obfvg_ob_range_based")
            ui_params["obfvg_reverse"] = st.checkbox("啟用反向開倉 (Short)", value=False, help="若勾選，遇到做多訊號時將改為做空 (Entry Short)，TP/SL 方向亦會反轉。", key="obfvg_reverse")

        elif family == "SMC":
            st.markdown("##### SMC Fusion 參數")
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["smc_len_min"] = st.number_input("Pivot Lookback 最小", 5, 50, 14)
                ui_params["smc_len_step"] = st.number_input("Lookback 步長", 1, 10, 1)
            with col2:
                ui_params["smc_len_max"] = st.number_input("Pivot Lookback 最大", 5, 50, 14)
            with col3:
                st.caption("定義 Pivot 高低點的週期")

            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["smc_limit_min"] = st.number_input("OB 過期K數 最小", 50, 500, 300)
                ui_params["smc_limit_step"] = st.number_input("過期K數 步長", 10, 100, 50)
            with col2:
                ui_params["smc_limit_max"] = st.number_input("OB 過期K數 最大", 50, 500, 300)
            with col3:
                st.caption("OB 形成後多少根K棒內有效")
            
            ui_params["smc_reverse"] = st.checkbox("啟用反向 (Break Bullish OB -> Short)", value=False)

        elif family in ["OBV_Slope", "ADL_Slope"]:
            st.info("此家族無需參數。")

        elif family == "LaguerreRSI_TEMA":
            st.markdown("##### 指標參數 (Gamma / TEMA)")
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["gamma_min"] = st.number_input("Gamma 最小", 0.1, 0.9, 0.5, step=0.1)
                ui_params["gamma_step"] = st.number_input("Gamma 步長", 0.05, 0.5, 0.1, step=0.05)
            with col2:
                ui_params["gamma_max"] = st.number_input("Gamma 最大", 0.1, 0.9, 0.5, step=0.1)
            with col3:
                st.caption("Laguerre RSI Gamma (Def: 0.5)")

            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["tema_min"] = st.number_input("TEMA 週期最小", 10, 100, 30)
                ui_params["tema_step"] = st.number_input("TEMA 步長", 1, 20, 10)
            with col2:
                ui_params["tema_max"] = st.number_input("TEMA 週期最大", 10, 100, 30)
            with col3:
                st.caption("TEMA Period (Def: 30)")
            
            st.markdown("##### ATR 風控係數 (SL/TP/Trailing)")
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["sl_c_min"] = st.number_input("ATR SL係數 Min", 0.1, 5.0, 1.1, step=0.1)
                ui_params["sl_c_step"] = st.number_input("SL係數 Step", 0.1, 1.0, 0.1, step=0.1)
            with col2:
                ui_params["sl_c_max"] = st.number_input("ATR SL係數 Max", 0.1, 5.0, 1.1, step=0.1)
            with col3:
                st.caption("Stop Loss = Entry - Coef*ATR")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["tp_c_min"] = st.number_input("ATR TP係數 Min", 0.5, 10.0, 1.9, step=0.1)
                ui_params["tp_c_step"] = st.number_input("TP係數 Step", 0.1, 2.0, 0.1, step=0.1)
            with col2:
                ui_params["tp_c_max"] = st.number_input("ATR TP係數 Max", 0.5, 10.0, 1.9, step=0.1)
            with col3:
                st.caption("Take Profit = Entry + Coef*ATR")

            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["tsd_min"] = st.number_input("TS距離係數 Min", 0.5, 5.0, 1.1, step=0.1)
                ui_params["tsd_step"] = st.number_input("TS距離 Step", 0.1, 1.0, 0.1, step=0.1)
            with col2:
                ui_params["tsd_max"] = st.number_input("TS距離係數 Max", 0.5, 5.0, 1.1, step=0.1)
            with col3:
                st.caption("Trailing Offset")

            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["tsa_min"] = st.number_input("TS啟動係數 Min", 0.5, 5.0, 1.1, step=0.1)
                ui_params["tsa_step"] = st.number_input("TS啟動 Step", 0.1, 1.0, 0.1, step=0.1)
            with col2:
                ui_params["tsa_max"] = st.number_input("TS啟動係數 Max", 0.5, 5.0, 1.1, step=0.1)
            with col3:
                st.caption("Trailing Activation Price")
        
        elif family == "TEMA_RSI":
            st.markdown("##### 指標參數（TEMA / RSI）")
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["fast_min"] = st.number_input("快線 TEMA 週期最小", 1, 200, 3)
                ui_params["fast_step"] = st.number_input("快線步長", 1, 10, 1)
            with col2:
                ui_params["fast_max"] = st.number_input("快線 TEMA 週期最大", 1, 200, 3)
            with col3:
                st.caption("快線週期")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["slow_min"] = st.number_input("慢線 TEMA 週期最小", 10, 500, 100)
                ui_params["slow_step"] = st.number_input("慢線步長", 1, 50, 10)
            with col2:
                ui_params["slow_max"] = st.number_input("慢線 TEMA 週期最大", 10, 500, 100)
            with col3:
                st.caption("慢線週期")

            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["rsi_thr_min"] = st.number_input("RSI 門檻最小", 1, 99, 20)
                ui_params["rsi_thr_step"] = st.number_input("RSI 步長", 1, 10, 5)
            with col2:
                ui_params["rsi_thr_max"] = st.number_input("RSI 門檻最大", 1, 99, 20)
            with col3:
                st.caption("入場條件：RSI 高於門檻")
            
            st.markdown("##### 策略內建風控（覆蓋下方通用設定）")
            ui_params["mintick"] = st.number_input("最小跳動（MinTick）", 0.000001, 100.0, 0.01, format="%.6f", help="用於將追蹤距離（ticks）換算為價格距離")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["tp_min"] = st.number_input("停利（%）最小", -100.0, 100.0, 2.2, step=0.1)
                ui_params["tp_step"] = st.number_input("停利步長（%）", 0.1, 5.0, 0.1, step=0.1)
            with col2:
                ui_params["tp_max"] = st.number_input("停利（%）最大", -100.0, 100.0, 2.2, step=0.1)
            with col3:
                st.caption("停利（-100 表示停用）")

            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["sl_min"] = st.number_input("停損（%）最小", 0.1, 100.0, 6.0, step=0.1)
                ui_params["sl_step"] = st.number_input("停損步長（%）", 0.1, 5.0, 0.1, step=0.1)
            with col2:
                ui_params["sl_max"] = st.number_input("停損（%）最大", 0.1, 100.0, 6.0, step=0.1)
            with col3:
                st.caption("停損")

            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["act_min"] = st.number_input("追蹤啟動（%）最小", 0.1, 10.0, 1.0, step=0.1)
                ui_params["act_step"] = st.number_input("追蹤啟動步長（%）", 0.1, 1.0, 0.1, step=0.1)
            with col2:
                ui_params["act_max"] = st.number_input("追蹤啟動（%）最大", 0.1, 10.0, 1.0, step=0.1)
            with col3:
                st.caption("達到啟動幅度後開始追蹤")

            col1, col2, col3 = st.columns(3)
            with col1:
                ui_params["tr_tick_min"] = st.number_input("追蹤距離（Ticks）最小", 10, 5000, 800, step=10)
                ui_params["tr_tick_step"] = st.number_input("追蹤距離步長（Ticks）", 10, 100, 10, step=10)
            with col2:
                ui_params["tr_tick_max"] = st.number_input("追蹤距離（Ticks）最大", 10, 5000, 800, step=10)
            with col3:
                st.caption("追蹤距離（以 Tick 計）")
            
            ui_params["stake_pct"] = st.number_input("下單資金比例（%）", 1.0, 100.0, 95.0, step=1.0, help="下單數量依淨值比例換算（以 close 計算）。")

        # [教授修正] 將單一指標家族的 JSON 輸入通用化至所有策略
        
        st.markdown("---")
        single_fam_json_text = st.text_area(f"【進階】{family} 參數 JSON (若填寫則覆蓋上方滑桿)", height=100, help="在此貼上完整的指標參數 List[Dict] JSON，可直接覆蓋上方的滑桿設定。")

        st.subheader("風控與出場")
        # 專家級修改：當選擇 LaguerreRSI_TEMA 或 TEMA_RSI 時，隱藏通用 TP/SL 面板
        if family not in ["LaguerreRSI_TEMA", "TEMA_RSI"]:
            col1, col2, col3 = st.columns(3)
            with col1:
                tp_min = st.number_input("TP% 最小", 0.1, 50.0, 1.0, step=0.1, key="tp_min_inp")
            with col2:
                tp_max = st.number_input("TP% 最大", 0.1, 50.0, 3.0, step=0.1, key="tp_max_inp")
            with col3:
                tp_step = st.number_input("TP% 搜尋步長", 0.1, 10.0, 0.5, step=0.1, key="tp_step_inp")
            col1, col2, col3 = st.columns(3)
            with col1:
                sl_min = st.number_input("SL% 最小", 0.1, 50.0, 0.5, step=0.1, key="sl_min_inp")
            with col2:
                sl_max = st.number_input("SL% 最大", 0.1, 50.0, 2.0, step=0.1, key="sl_max_inp")
            with col3:
                sl_step = st.number_input("SL% 搜尋步長", 0.1, 10.0, 0.5, step=0.1, key="sl_step_inp")
        else:
            # 為了避免變數未定義錯誤，給予 Dummy 值 (策略內部不使用)
            st.info("注意：此策略使用內建 ATR 動態風控 (Trailing + SL/TP)，上方固定 TP/SL 設定已停用。")
            tp_min, tp_max, tp_step = 0.0, 0.0, 1.0
            sl_min, sl_max, sl_step = 0.0, 0.0, 1.0

        col1, col2, col3 = st.columns(3)
        with col1:
            max_hold_min = st.number_input("持倉 m 最小（K數）", 1, 10000, 10, step=1, key="mh_min_inp")
            max_hold_step = st.number_input("持倉 m 搜尋步長", 1, 1000, 5, step=1, key="mh_step_inp")
        with col2:
            max_hold_max = st.number_input("持倉 m 最大（K數）", 1, 10000, 60, step=1, key="mh_max_inp")
        with col3:
            st.write("")
        risk_json_text = st.text_area(
            "風控 JSON（可選）",
            value="",
            height=120,
            help="支援鍵：tp_min,tp_max,tp_step,sl_min,sl_max,sl_step,max_hold_min,max_hold_max,max_hold_step，或直接給列表 tp_list/sl_list/max_hold_list（%用數字如 1.5 表示 1.5%）"
        )

        st.subheader("撮合與成本設定")

        # 滑點改為 ticks（跳）模式：先把 ticks * tick_size 轉成「絕對價格距離」，再由撮合/回測核心使用
        col_sl1, col_sl2 = st.columns(2)
        with col_sl1:
            _tick_default = float(ui_params.get("mintick", st.session_state.get("tick_size", 0.01)))
            tick_size = st.number_input(
                "最小跳動（Tick Size）",
                0.00000001, 100.0, _tick_default,
                format="%.8f",
                help="將 tick 數換算為價格距離：ticks × tick_size。"
            )
        with col_sl2:
            slippage_ticks = st.number_input(
                "滑點（Ticks）",
                0, 1000000, int(st.session_state.get("slippage_ticks", 120)),
                step=1,
                help="做多：進場加價、出場減價；做空：進場減價、出場加價。"
            )

        st.session_state["tick_size"] = float(tick_size)
        st.session_state["slippage_ticks"] = int(slippage_ticks)

        # 內部統一用「絕對價格距離」代表滑點（避免百分比在不同幣價下失真）
        slippage = float(slippage_ticks) * float(tick_size)

        fee_side = st.number_input("單邊手續費率（%）", 0.0, 1.0, 0.06, step=0.01) / 100.0
        worst_case = st.selectbox("同一根K線同時觸發時優先順序", ["先 SL", "先 TP"], index=0) == "先 SL"
        topN = st.number_input("顯示前 N 名", 1, 100, 20)
        max_combo_cap = st.number_input("組合數上限", 100, 100000000, 2000000)
        st.subheader("效能設定")
        use_gpu = st.checkbox("啟用 GPU（PyTorch / DirectML）", value=True if HAS_TORCH else False)
        multi_mode = st.checkbox("多指標模式（2–8 個，AND/OR）", value=False)
        # 訊號組合模式：新增「A、B、A+B(OR)」
        signal_mode_ui = st.selectbox("訊號組合模式", ["AND", "OR", "逐家族 + 全部（OR）"], index=0)
        signal_logic = "AND" if signal_mode_ui == "AND" else ("OR" if signal_mode_ui == "OR" else "EACH_OR")
        signal_mode = "EACH_OR" if signal_logic == "EACH_OR" else "LOGIC"
        # 啟動階段詳細日誌 + 最高速選項
        verbose_start_log = st.checkbox("顯示執行紀錄", value=True)
        use_torch_compile = st.checkbox("啟用 Torch 編譯", value=bool(st.session_state.get("use_torch_compile", True)))
        st.session_state["use_torch_compile"] = bool(use_torch_compile)
        gpu_batch_size = st.number_input("批次大小（組合/批）", 256, 65536, 4096, step=256)
        skip_trade_details = st.checkbox("略過交易明細（加速）", value=True)

        # 多指標的家族選擇與格點（用 JSON 輸入，鍵名沿用單指標 UI 的鍵）
    selected_families = []
    multi_ui_params_by_family = {}
    run_btn = False

    if multi_mode:
        # 選家族（UI）
        selected_families = st.multiselect("選擇家族（2–8）", INDICATOR_FAMILIES, default=["ATR_Band_Break"])

        # 如果沒選，直接提醒並停止，避免後面當空 list 用
        if not selected_families:
            st.error("多指標模式已啟用，但未選擇任何家族。請至少選擇一個家族後再執行。")
            st.stop()

        # 建立每個選擇家族的預設範本（展示在 TextArea，使用者可編輯）
        default_cfg = {}
        for fam in selected_families:
            if fam == "RSI":
                default_cfg[fam] = {
                    "rsi_p_min": 29, "rsi_p_max": 30, "rsi_p_step": 1,
                    "rsi_lv_min": 20, "rsi_lv_max": 25, "rsi_lv_step": 5
                }
            elif fam in ["SMA_Cross", "EMA_Cross", "HMA_Cross", "DEMA_Cross", "TEMA_Cross", "WMA_Cross"]:
                default_cfg[fam] = {"fast_min": 9, "fast_max": 12, "fast_step": 1, "slow_min": 20, "slow_max": 26, "slow_step": 2}
            elif fam in ["MACD_Cross", "PPO_Cross", "PVO_Cross"]:
                default_cfg[fam] = {"fast_min": 8, "fast_max": 12, "fast_step": 2, "slow_min": 17, "slow_max": 26, "slow_step": 3, "sig_min": 5, "sig_max": 9, "sig_step": 2}
            elif fam == "Bollinger_Touch":
                default_cfg[fam] = {"bb_p_min": 20, "bb_p_max": 20, "bb_p_step": 1, "bb_n_min": 2.0, "bb_n_max": 2.0, "bb_n_step": 0.1}
            elif fam == "ATR_Band_Break":
                # ATR 家族需要 mult_min/mult_max/mult_step —— 給合理缺省
                default_cfg[fam] = {
                    "p_min": 10, "p_max": 30, "p_step": 1,
                    "mult_min": 0.5, "mult_max": 2.0, "mult_step": 0.5
                }
            elif fam == "OB_FVG":
                default_cfg[fam] = {
                    "obfvg_n_min": 3, "obfvg_n_max": 5, "obfvg_n_step": 1,
                    "obfvg_w_min": 20, "obfvg_w_max": 40, "obfvg_w_step": 10,
                    "obfvg_r_min": 0.001, "obfvg_r_max": 0.005, "obfvg_r_step": 0.0005,
                    "obfvg_h_min": 20, "obfvg_h_max": 60, "obfvg_h_step": 10,
                    "obfvg_g_min": 1.0, "obfvg_g_max": 2.0, "obfvg_g_step": 0.5,
                    "obfvg_a_min": 0.98, "obfvg_a_max": 1.0, "obfvg_a_step": 0.01,
                    "obfvg_thr_min": 1.002, "obfvg_thr_max": 1.005, "obfvg_thr_step": 0.001,
                    "obfvg_rsi_p_min": 14, "obfvg_rsi_p_max": 14, "obfvg_rsi_p_step": 1,
                    "obfvg_rsi_diff_min": 0.0, "obfvg_rsi_diff_max": 0.1, "obfvg_rsi_diff_step": 0.05,
                    "obfvg_x": 1.0, "obfvg_y": 1.0,
                    "obfvg_ob_range_based": False,
                    "reverse": False
                }
            elif fam == "SMC":
                default_cfg[fam] = {
                    "smc_len_min": 14, "smc_len_max": 14, "smc_len_step": 1,
                    "smc_limit_min": 300, "smc_limit_max": 300, "smc_limit_step": 10,
                    "smc_reverse": False
                }
            elif fam == "LaguerreRSI_TEMA":
                default_cfg[fam] = {
                    "gamma_min": 0.5, "gamma_max": 0.5, "gamma_step": 0.1,
                    "tema_min": 30, "tema_max": 30, "tema_step": 1,
                    "sl_c_min": 1.1, "sl_c_max": 1.1, "sl_c_step": 0.1,
                    "tp_c_min": 1.9, "tp_c_max": 1.9, "tp_c_step": 0.1,
                    "tsd_min": 1.1, "tsd_max": 1.1, "tsd_step": 0.1,
                    "tsa_min": 1.1, "tsa_max": 1.1, "tsa_step": 0.1
                }
            elif fam == "TEMA_RSI":
                default_cfg[fam] = {
                    "fast_min": 3, "fast_max": 3, "fast_step": 1,
                    "slow_min": 100, "slow_max": 100, "slow_step": 10,
                    "rsi_thr_min": 20, "rsi_thr_max": 20, "rsi_thr_step": 5,
                    "tp_min": 2.2, "tp_max": 2.2, "tp_step": 0.1,
                    "sl_min": 6.0, "sl_max": 6.0, "sl_step": 0.1,
                    "act_min": 1.0, "act_max": 1.0, "act_step": 0.1,
                    "tr_tick_min": 800, "tr_tick_max": 800, "tr_tick_step": 100,
                    "mintick": 0.01
                }

            else:
                default_cfg[fam] = {"p_min": 10, "p_max": 20, "p_step": 5, "thr_min": 0.0, "thr_max": 0.0, "thr_step": 0.1}

        cfg_text = st.text_area("每個家族的格點 JSON（可改）", value=json.dumps(default_cfg, ensure_ascii=False, indent=2))

        # 解析使用者輸入的 JSON（若空就用預設）
        try:
            parsed = json.loads(cfg_text) if cfg_text.strip() else {}
        except Exception as _e:
            st.error(f"多指標 JSON 解析失敗：{_e}")
            multi_mode = False  # 回退
            parsed = {}

        # ----- 補齊並驗證每個 family 的必要欄位（避免 KeyError） -----
        def _ensure_params_for_family(fam: str, d: dict) -> dict:
            """回傳一個補齊後的 dict（只補必要鍵），避免 grid_combinations_from_ui 抱 KeyError"""
            out = {}
            if fam == "RSI":
                out = {
                    "rsi_p_min": int(d.get("rsi_p_min", 29)),
                    "rsi_p_max": int(d.get("rsi_p_max", 30)),
                    "rsi_p_step": int(d.get("rsi_p_step", 1)),
                    "rsi_lv_min": int(d.get("rsi_lv_min", 20)),
                    "rsi_lv_max": int(d.get("rsi_lv_max", 25)),
                    "rsi_lv_step": int(d.get("rsi_lv_step", 5)),
                }
            elif fam in ["SMA_Cross", "EMA_Cross", "HMA_Cross", "DEMA_Cross", "TEMA_Cross", "WMA_Cross"]:
                out = {
                    "fast_min": int(d.get("fast_min", 9)),
                    "fast_max": int(d.get("fast_max", 12)),
                    "fast_step": int(d.get("fast_step", 1)),
                    "slow_min": int(d.get("slow_min", 20)),
                    "slow_max": int(d.get("slow_max", 26)),
                    "slow_step": int(d.get("slow_step", 2)),
                }
            elif fam in ["MACD_Cross", "PPO_Cross", "PVO_Cross"]:
                out = {
                    "fast_min": int(d.get("fast_min", 8)),
                    "fast_max": int(d.get("fast_max", 12)),
                    "fast_step": int(d.get("fast_step", 2)),
                    "slow_min": int(d.get("slow_min", 17)),
                    "slow_max": int(d.get("slow_max", 26)),
                    "slow_step": int(d.get("slow_step", 3)),
                    "sig_min": int(d.get("sig_min", 5)),
                    "sig_max": int(d.get("sig_max", 9)),
                    "sig_step": int(d.get("sig_step", 2)),
                }
            elif fam == "Bollinger_Touch":
                out = {
                    "bb_p_min": int(d.get("bb_p_min", 20)),
                    "bb_p_max": int(d.get("bb_p_max", 20)),
                    "bb_p_step": int(d.get("bb_p_step", 1)),
                    "bb_n_min": float(d.get("bb_n_min", 2.0)),
                    "bb_n_max": float(d.get("bb_n_max", 2.0)),
                    "bb_n_step": float(d.get("bb_n_step", 0.1)),
                }
            elif fam == "OB_FVG":
                out = {
                    "obfvg_n_min": int(d.get("obfvg_n_min", 3)),
                    "obfvg_n_max": int(d.get("obfvg_n_max", 5)),
                    "obfvg_n_step": int(d.get("obfvg_n_step", 1)),
                    "obfvg_w_min": int(d.get("obfvg_w_min", 20)),
                    "obfvg_w_max": int(d.get("obfvg_w_max", 40)),
                    "obfvg_w_step": int(d.get("obfvg_w_step", 10)),
                    "obfvg_r_min": float(d.get("obfvg_r_min", 0.001)),
                    "obfvg_r_max": float(d.get("obfvg_r_max", 0.005)),
                    "obfvg_r_step": float(d.get("obfvg_r_step", 0.0005)),
                    "obfvg_h_min": int(d.get("obfvg_h_min", 20)),
                    "obfvg_h_max": int(d.get("obfvg_h_max", 60)),
                    "obfvg_h_step": int(d.get("obfvg_h_step", 10)),
                    "obfvg_g_min": float(d.get("obfvg_g_min", 1.0)),
                    "obfvg_g_max": float(d.get("obfvg_g_max", 2.0)),
                    "obfvg_g_step": float(d.get("obfvg_g_step", 0.5)),
                    "obfvg_a_min": float(d.get("obfvg_a_min", 0.98)),
                    "obfvg_a_max": float(d.get("obfvg_a_max", 1.0)),
                    "obfvg_a_step": float(d.get("obfvg_a_step", 0.01)),
                    "obfvg_thr_min": float(d.get("obfvg_thr_min", 1.002)),
                    "obfvg_thr_max": float(d.get("obfvg_thr_max", 1.005)),
                    "obfvg_thr_step": float(d.get("obfvg_thr_step", 0.001)),
                    "obfvg_rsi_p_min": int(d.get("obfvg_rsi_p_min", 14)),
                    "obfvg_rsi_p_max": int(d.get("obfvg_rsi_p_max", 14)),
                    "obfvg_rsi_p_step": int(d.get("obfvg_rsi_p_step", 1)),
                    "obfvg_rsi_diff_min": float(d.get("obfvg_rsi_diff_min", 0.0)),
                    "obfvg_rsi_diff_max": float(d.get("obfvg_rsi_diff_max", 0.1)),
                    "obfvg_rsi_diff_step": float(d.get("obfvg_rsi_diff_step", 0.05)),
                    "obfvg_x": float(d.get("obfvg_x", 1.0)),
                    "obfvg_y": float(d.get("obfvg_y", 1.0)),
                    "obfvg_ob_range_based": bool(d.get("obfvg_ob_range_based", False)),
                    "reverse": bool(d.get("reverse", False))
                } 

            elif fam == "SMC":
                out = {
                    "smc_len_min": int(d.get("smc_len_min", 14)),
                    "smc_len_max": int(d.get("smc_len_max", 14)),
                    "smc_len_step": int(d.get("smc_len_step", 1)),
                    "smc_limit_min": int(d.get("smc_limit_min", 300)),
                    "smc_limit_max": int(d.get("smc_limit_max", 300)),
                    "smc_limit_step": int(d.get("smc_limit_step", 50)),
                    "smc_reverse": bool(d.get("smc_reverse", False)),
                }
            elif fam == "LaguerreRSI_TEMA":
                out = {
                    "gamma_min": float(d.get("gamma_min", 0.5)),
                    "gamma_max": float(d.get("gamma_max", 0.5)),
                    "gamma_step": float(d.get("gamma_step", 0.1)),
                    "tema_min": int(d.get("tema_min", 30)),
                    "tema_max": int(d.get("tema_max", 30)),
                    "tema_step": int(d.get("tema_step", 1)),
                    "sl_c_min": float(d.get("sl_c_min", 1.1)),
                    "sl_c_max": float(d.get("sl_c_max", 1.1)),
                    "sl_c_step": float(d.get("sl_c_step", 0.1)),
                    "tp_c_min": float(d.get("tp_c_min", 1.9)),
                    "tp_c_max": float(d.get("tp_c_max", 1.9)),
                    "tp_c_step": float(d.get("tp_c_step", 0.1)),
                    "tsd_min": float(d.get("tsd_min", 1.1)),
                    "tsd_max": float(d.get("tsd_max", 1.1)),
                    "tsd_step": float(d.get("tsd_step", 0.1)),
                    "tsa_min": float(d.get("tsa_min", 1.1)),
                    "tsa_max": float(d.get("tsa_max", 1.1)),
                    "tsa_step": float(d.get("tsa_step", 0.1)),
                }
            elif fam == "TEMA_RSI":
                out = {
                    "fast_min": int(d.get("fast_min", 3)),
                    "fast_max": int(d.get("fast_max", 3)),
                    "fast_step": int(d.get("fast_step", 1)),
                    "slow_min": int(d.get("slow_min", 100)),
                    "slow_max": int(d.get("slow_max", 100)),
                    "slow_step": int(d.get("slow_step", 10)),
                    "rsi_thr_min": int(d.get("rsi_thr_min", 20)),
                    "rsi_thr_max": int(d.get("rsi_thr_max", 20)),
                    "rsi_thr_step": int(d.get("rsi_thr_step", 5)),
                    "tp_min": float(d.get("tp_min", 2.2)),
                    "tp_max": float(d.get("tp_max", 2.2)),
                    "tp_step": float(d.get("tp_step", 0.1)),
                    "sl_min": float(d.get("sl_min", 6.0)),
                    "sl_max": float(d.get("sl_max", 6.0)),
                    "sl_step": float(d.get("sl_step", 0.1)),
                    "act_min": float(d.get("act_min", 1.0)),
                    "act_max": float(d.get("act_max", 1.0)),
                    "act_step": float(d.get("act_step", 0.1)),
                    "tr_tick_min": int(d.get("tr_tick_min", 800)),
                    "tr_tick_max": int(d.get("tr_tick_max", 800)),
                    "tr_tick_step": int(d.get("tr_tick_step", 100)),
                    "mintick": float(d.get("mintick", 0.01)),
                    "stake_pct": float(d.get("stake_pct", 95.0)),
                }

            elif fam == "ATR_Band_Break":
                out = {
                    "p_min": int(d.get("p_min", 10)),
                    "p_max": int(d.get("p_max", 30)),
                    "p_step": int(d.get("p_step", 1)),
                    "mult_min": float(d.get("mult_min", 0.5)),
                    "mult_max": float(d.get("mult_max", 2.0)),
                    "mult_step": float(d.get("mult_step", 0.5)),
                }
            else:
                # 其他家族使用一般形式（盡量補齊常見鍵）
                out = {
                    "p_min": int(d.get("p_min", 10)),
                    "p_max": int(d.get("p_max", 20)),
                    "p_step": int(d.get("p_step", 5)),
                    "thr_min": float(d.get("thr_min", 0.0)),
                    "thr_max": float(d.get("thr_max", 0.0)),
                    "thr_step": float(d.get("thr_step", 0.1)),
                }
            return out

        # 針對每個被選家族，建立「已驗證、補齊」的 multi_ui_params_by_family
        sanitized = {}
        for fam in selected_families:
            raw = parsed.get(fam, {})
            sanitized[fam] = _ensure_params_for_family(fam, raw)
            # 若原始 JSON 沒有提供該 fam，提醒使用者（但不阻斷）
            if fam not in parsed:
                st.warning(f"家族 {fam} 在 JSON 中未找到對應設定，已使用系統預設格點：{sanitized[fam]}")

        multi_ui_params_by_family = sanitized

    # ------------------------------------------------------------
    # 鎖定畫面（下載模式）
    # Streamlit 點任何 download_button 都會 rerun；
    # 舊寫法用 st.stop() 會把下面所有下載按鈕都吃掉 → 只能下載一次。
    # 新做法：把上一次的下載資料存在 session_state，鎖定時只渲染下載區，不重跑回測。
    # ------------------------------------------------------------
    def _render_frozen_payload(payload: dict):
        # Top-N 表（上次結果）
        top_df_display = payload.get("top_df_display")
        if top_df_display is not None:
            st.subheader(f"最佳 {payload.get('topN', len(top_df_display))} 組合（上次結果）")
            st.dataframe(top_df_display, use_container_width=True)

        # 下載按鈕（都用 bytes，避免再讀大檔）
        if payload.get("full_csv_bytes") is not None:
            st.download_button(
                " 下載完整結果（CSV，所有組合）",
                data=payload["full_csv_bytes"],
                file_name=payload.get("full_csv_name", "grid_results_all.csv"),
                mime="text/csv",
                help="包含此次搜尋的所有組合，不只 Top-N"
            )
        if payload.get("full_parquet_bytes") is not None:
            st.download_button(
                " 下載完整結果（Parquet，所有組合）",
                data=payload["full_parquet_bytes"],
                file_name=payload.get("full_parquet_name", "grid_results_all.parquet"),
                mime="application/octet-stream",
                help="Parquet 保留型別，適合 Python/R 之後再分析"
            )
        if payload.get("perf_csv_bytes") is not None:
            st.download_button(
                "下載績效表",
                data=payload["perf_csv_bytes"],
                file_name=payload.get("perf_csv_name", "grid_results_pretty.csv"),
                mime="text/csv"
            )
        if payload.get("best_trades_bytes") is not None:
            st.download_button(
                "下載最佳組合交易明細",
                data=payload["best_trades_bytes"],
                file_name=payload.get("best_trades_name", "best_trades.csv"),
                mime="text/csv"
            )

        if payload.get("zip_bytes") is not None:

            st.download_button(
                "下載 ZIP",
                data=payload["zip_bytes"],
                file_name=payload.get("zip_file_name", "selected_details.zip"),
                mime="application/zip"
            )

    run_btn = st.button(
        "開始格點搜尋",
        disabled=bool(st.session_state.get("freeze_after_run", False))
    )

    # 如果畫面被鎖定：只顯示上一次下載區，不重跑
    if st.session_state.get("freeze_after_run", False):
        st.info("目前畫面鎖定中（等待你下載）。按主畫面的「確認並下一步」後可再次搜尋。")
        _payload = st.session_state.get("last_run_payload")
        if isinstance(_payload, dict) and _payload:
            _render_frozen_payload(_payload)
        st.stop()



    # 主頁內容


    # 主頁內容
    INTRABAR_1M_CTX = None

    try:
        df = load_and_validate_csv(path)
    except Exception as e:
        st.error(f"主資料讀取失敗：{e}")
        st.stop()

    st.success(f"主週期資料已載入：{len(df)} 根，{df['ts'].iloc[0]} ～ {df['ts'].iloc[-1]}")

    # -------------------- 1m 精準撮合：載入 1m CSV + 建立對齊 context -------------------- #
    use_1m_fill = False
    micro_ctx = None
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        _ctx = get_script_run_ctx()
    except Exception:
        _ctx = None

    if _ctx is not None:
        use_1m_fill = bool(st.session_state.get("use_1m_fill", False))
        micro_ctx = st.session_state.get("microfill_ctx", None)
        if use_1m_fill and micro_ctx is None:
            raise ValueError("已勾選 1m 精準撮合，但尚未成功載入 1m CSV（microfill_ctx 不存在）。請先在側邊欄上傳 1m CSV。")

    if use_1m_fill:


        # 精準撮合必須走 CPU 路徑（GPU 批次內核目前沒有接 1m 逐筆檢查）
        if bool(use_gpu):
            st.warning("已啟用 1m 精準撮合。")
        use_gpu = False

        path_1m_eff = str(st.session_state.get("path_1m_csv", "")).strip()
        if not path_1m_eff:
            st.error("未設定 1 分鐘資料路徑，無法啟用精準撮合。請先同步或指定 1m 檔案。")
            st.stop()

        try:
            df_1m = load_and_validate_csv_1m(path_1m_eff)
        except Exception as e:
            st.error(f"1 分鐘資料讀取失敗：{e}")
            st.stop()

        try:
            st.session_state["microfill_ctx"] = MicroFillContext(df, df_1m)
        except Exception as e:
            st.error(f"1m 對齊失敗：{e}")
            st.stop()

        st.success(f"1 分鐘資料已載入：{len(df_1m)} 根，{df_1m['ts'].iloc[0]} ～ {df_1m['ts'].iloc[-1]}")

        # 顯示 1m 缺分鐘補齊資訊（若有）
        try:
            _gap_cnt = int(getattr(df_1m, "attrs", {}).get("gap_filled_count", 0))
            _gap_run = int(getattr(df_1m, "attrs", {}).get("gap_filled_max_run", 0))
            if _gap_cnt > 0:
                st.warning(f"1 分鐘資料存在缺口：已補齊 {_gap_cnt} 分鐘（以前一分鐘 close 補價、量=0），最大連續缺口 {_gap_run} 分鐘。")
        except Exception:
            pass
    else:
        # 沒勾選就清掉舊 ctx，避免你換資料後還在用舊的 1m 對齊
        st.session_state.pop("microfill_ctx", None)


    # [專家修正] 建構格點：優先檢查 JSON 輸入
    # 邏輯：若 JSON 包含 "_min" / "_max" 等範圍定義鍵，則視為「參數覆蓋」並更新 ui_params 後重新生成格點；
    #       若 JSON 為明確數值列表 (無 range key)，則視為「明確組合列表(Explicit List)」。
    combos_family = []
    json_applied_as_explicit = False

    if "single_fam_json_text" in locals() and single_fam_json_text.strip():
        try:
            _parsed = json.loads(single_fam_json_text)
            # 正規化為 list 以便檢查
            if isinstance(_parsed, dict):
                _parsed = [_parsed]
            
            if isinstance(_parsed, list) and len(_parsed) > 0:
                # 檢查第一項是否包含範圍定義關鍵字 (_min, _max, _step)
                # 若有，代表使用者想透過 JSON 設定 Range，而非指定單一參數
                first_item = _parsed[0]
                is_range_def = any(k.endswith(("_min", "_max", "_step")) for k in first_item.keys())
                
                if is_range_def:
                    # 模式 A: JSON 作為 UI 參數的覆蓋源 -> 更新 ui_params -> 呼叫 grid_combinations_from_ui
                    # 只取第一項做為設定檔
                    override_params = first_item
                    for k, v in override_params.items():
                        # 強制覆蓋 ui_params 內的數值
                        ui_params[k] = v
                    
                    st.info("已套用 JSON 範圍設定，已重新產生格點。")
                    # 這裡不設定 json_applied_as_explicit，讓流程往下走到 grid_combinations_from_ui
                else:
                    # 模式 B: JSON 為明確組合列表 (例如已經生成好的 param dicts)
                    combos_family = _parsed
                    st.info(f"已套用 JSON 明細列表：{len(combos_family)} 組。")
                    json_applied_as_explicit = True

        except Exception as e:
            st.error(f"指標參數 JSON 解析失敗：{e}（已忽略，沿用介面設定）")
    
    # 若沒有被視為明確列表 (即：原本無 JSON 或 JSON 為 Range 定義)，則執行標準格點生成
    if not json_applied_as_explicit:
        combos_family = grid_combinations_from_ui(family, ui_params)

    # TP/SL 網格
    def frange(a, b, step):
        vals = []
        x = a
        while x <= b + 1e-12:
            vals.append(round(x, 6))
            x += step
        return vals
    tp_list = [x/100.0 for x in frange(tp_min, tp_max, tp_step)]
    sl_list = [x/100.0 for x in frange(sl_min, sl_max, sl_step)]
    # 持倉 m（K數）格點
    mh_list = list(range(int(max_hold_min), int(max_hold_max) + 1, int(max_hold_step)))

    # 風控 JSON 覆蓋（可選）
    risk_cfg = {}
    if isinstance(risk_json_text, str) and risk_json_text.strip():
        try:
            risk_cfg = json.loads(risk_json_text)
        except Exception as _e:
            st.warning(f"風控 JSON 解析失敗：{_e}（已忽略，沿用介面設定）")
            risk_cfg = {}

    if isinstance(risk_cfg, dict):
        # 1) 直接列表（百分比：>1 視為百分數）
        if "tp_list" in risk_cfg:
            _tpl = [float(x) for x in risk_cfg["tp_list"]]
            # [教授修正] 強制統一視為百分比輸入 (e.g., 0.5 = 0.5%)，消除模糊空間
            tp_list = [x/100.0 for x in _tpl]
        elif all(k in risk_cfg for k in ("tp_min","tp_max","tp_step")):
            tp_list = [x/100.0 for x in frange(float(risk_cfg["tp_min"]),
                                               float(risk_cfg["tp_max"]),
                                               float(risk_cfg["tp_step"]))]

        if "sl_list" in risk_cfg:
            _sll = [float(x) for x in risk_cfg["sl_list"]]
            # [教授修正] 強制統一視為百分比輸入 (e.g., 0.5 = 0.5%)
            sl_list = [x/100.0 for x in _sll]
        elif all(k in risk_cfg for k in ("sl_min","sl_max","sl_step")):
            sl_list = [x/100.0 for x in frange(float(risk_cfg["sl_min"]),
                                               float(risk_cfg["sl_max"]),
                                               float(risk_cfg["sl_step"]))]

        if "max_hold_list" in risk_cfg:
            mh_list = [int(x) for x in risk_cfg["max_hold_list"]]
        elif all(k in risk_cfg for k in ("max_hold_min","max_hold_max","max_hold_step")):
            mh_list = list(range(int(risk_cfg["max_hold_min"]),
                                 int(risk_cfg["max_hold_max"]) + 1,
                                 int(risk_cfg["max_hold_step"])))

    # [專家修正] 針對內建風控格點的策略 (TEMA_RSI, LaguerreRSI_TEMA) 進行特殊處理
    # 這些策略的 combos_family 已經包含了 TP/SL/Stake/Trailing 的變化
    # 因此必須強制忽略外部的 tp_list / sl_list (設為 dummy)，否則會造成組合數錯誤乘算
    if family in ["LaguerreRSI_TEMA", "TEMA_RSI"]:
        tp_list_run = [0.0]
        sl_list_run = [0.0]
    else:
        tp_list_run = tp_list
        sl_list_run = sl_list

    all_jobs = []
    for prm in combos_family:
        for tpv in tp_list_run:
            for slv in sl_list_run:
                all_jobs.append((prm, tpv, slv))

    # 單指標用 all_jobs， 多指標改動態估算
    total_jobs_count = len(all_jobs) * max(1, len(mh_list))
    if multi_mode and selected_families:
        try:
            fam_counts = [len(grid_combinations_from_ui(f, multi_ui_params_by_family[f])) for f in selected_families]
            risk_count = max(1, len(tp_list) * len(sl_list) * max(1, len(mh_list)))
            if signal_mode == "EACH_OR":
                singles = sum(fam_counts)
                all_or = 1
                for cnt in fam_counts:
                    all_or *= max(1, cnt)
                total_jobs_count = (singles + all_or) * risk_count
            else:
                indic_combo_count = 1
                for cnt in fam_counts:
                    indic_combo_count *= max(1, cnt)
                total_jobs_count = indic_combo_count * risk_count
        except Exception:
            pass
    st.info(f"待評估組合：{total_jobs_count}")
    if total_jobs_count > max_combo_cap:
        st.error("組合數超過上限，請縮小範圍或提高上限。")
        st.stop()

    # 進行格點回測
    if run_btn:
        progress = st.progress(0.0)
        results = []
        ui_logger = UiLogger(enabled=bool(verbose_start_log))
        ui_logger("開始：初始化與檢查環境")
        t0 = time.time()
        try:
            # 1m intrabar 擬合：為了保證成交價精準，強制走 CPU 逐組回測
            if INTRABAR_1M_CTX is not None:
                if multi_mode:
                    st.error("已啟用 1m 擬合：目前不支援多指標模式（因為 GPU/批次核心無法逐筆使用 1m 重新撮合）。請先關閉多指標或關閉 1m 擬合。")
                    st.stop()
                if use_gpu:
                    st.warning("已啟用 1m 擬合：為了確保逐筆成交價正確，將自動停用 GPU/批次加速，改走 CPU。")
                use_gpu = False

            if use_gpu or multi_mode:
                if not HAS_TORCH:
                    st.error("未偵測到 PyTorch，請先安裝（上面有指令）。")
                    st.stop()
                dev = get_torch_device()
                cfg = setup_gpu_runtime_for_speed(dev)
                ui_logger(f"GPU: {dev} | TF32={cfg.get('tf32', False)} | precision={cfg.get('precision', 'default')}")
                ui_logger(f"批量大小（組合數/批）：{int(gpu_batch_size)}；任務總數：{int(total_jobs_count)}")

                results = []
                for mh in mh_list:
                    _part = run_grid_gpu(
                        df=df,
                        single_family=family,
                        single_ui=ui_params,
                        multi_mode=multi_mode,
                        selected_families=selected_families,
                        multi_ui_by_family=multi_ui_params_by_family,
                        signal_logic=("AND" if signal_logic=="AND" else "OR"),  # 內核只需要純邏輯；EACH_OR 由 signal_mode 控制
                        tp_list=tp_list,
                        sl_list=sl_list,
                        max_hold=int(mh),
                        fee_side=float(fee_side),
                        slippage=float(slippage),
                        worst_case=bool(worst_case),
                        batch_size=int(gpu_batch_size),
                        progress=progress,
                        total_jobs_count=int(total_jobs_count),
                        logger=ui_logger,
                        signal_mode=signal_mode,
                        use_torch_compile=bool(use_torch_compile),
                        explicit_combos=combos_family if not multi_mode else None,  # 傳入解析後的參數列表
                        micro_ctx=st.session_state.get("microfill_ctx", None)
                    )
                    results.extend(_part)
            else:
                if NUMBA_OK:
                    ui_logger("Numba 路徑：批次計算開始")
                    results = []
                    for mh in mh_list:
                        _part = run_grid_gpu(
                            df=df,
                            single_family=family,
                            single_ui=ui_params,
                            multi_mode=multi_mode,
                            selected_families=selected_families,
                            multi_ui_by_family=multi_ui_params_by_family,
                            signal_logic=("AND" if signal_logic=="AND" else "OR"),  # 內核只需要純邏輯；EACH_OR 由 signal_mode 控制
                            tp_list=tp_list,
                            sl_list=sl_list,
                            max_hold=int(mh),
                            fee_side=float(fee_side),
                            slippage=float(slippage),
                            worst_case=bool(worst_case),
                            batch_size=int(gpu_batch_size),
                            progress=progress,
                            total_jobs_count=int(total_jobs_count),
                            logger=ui_logger,
                            signal_mode=signal_mode,
                            use_torch_compile=False,
                            explicit_combos=combos_family if not multi_mode else None,
                            micro_ctx=st.session_state.get("microfill_ctx", None)
                        )
                        results.extend(_part)
                    ui_logger("Numba 路徑：批次計算完成")
                else:
                    ui_logger("CPU 路徑：逐組計算開始")
                    for mh in mh_list:
                        for idx, (prm, tpv, slv) in enumerate(all_jobs, start=1):
                            # 檢查 reverse
                            rev_mode = False
                            if family == "OB_FVG":
                                 rev_mode = bool(prm.get("reverse", False))

                            res = run_backtest(
                            df, family, prm, tpv, slv, int(mh),
                            fee_side=float(fee_side), slippage=float(slippage), worst_case=bool(worst_case),
                            reverse_mode=rev_mode
                        )

                            results.append(res)
                            if idx % max(1, len(all_jobs)//200) == 0:
                                progress.progress(min(1.0, min(len(all_jobs), idx)/max(1, len(all_jobs))))
                    ui_logger("CPU 路徑：逐組計算完成")
        except Exception as e:
            st.error("回測核心發生致命錯誤，已停止執行。")
            ui_logger(f"錯誤：{e}\n{traceback.format_exc()}")
            # 將完整錯誤軌跡直接印在前端畫面上，確保錯誤「最大化顯示」
            st.code(traceback.format_exc(), language="python")
            st.stop()
        t1 = time.time()
        st.success(f"計算完成：{t1-t0:.2f} 秒")
        # 鎖住當前結果畫面：直到按下「確認並下一步」才清空
        if "freeze_after_run" not in st.session_state:
            st.session_state["freeze_after_run"] = True
        else:
            st.session_state["freeze_after_run"] = True

        with st.container(border=True):
            st.markdown("### 計算完成（輸出已鎖定）")
            st.caption("結果已保留。完成下載後可解除鎖定並返回設定。")
            if st.button("解除鎖定並返回設定", type="primary", use_container_width=True, key="confirm_next"):
                # 解鎖：恢復 UI 互動
                st.session_state["freeze_after_run"] = False
                st.rerun()

        # [教授修正] 匯整 DataFrame - 移除獨立 TP/SL/MaxHold 欄位，改用 Risk JSON
        rows = []
        for r in results:
            # 構建可直接貼回程式的 Risk JSON
            # 這裡我們將數值轉為 list 以符合輸入格式
            # 注意：r["tp_pct"] 與 r["sl_pct"] 在修正後已統一為百分比數值 (例如 1.5)，符合輸入 parser (x/100 if x>1 else x) 的邏輯
            _risk_obj = {
                "tp_list": [float(r["tp_pct"])],
                "sl_list": [float(r["sl_pct"])],
                "max_hold_list": [int(r["max_hold"])]
            }
            # 使用 separators 生成緊湊的 JSON string
            _risk_str = json.dumps(_risk_obj, separators=(',', ':'))

            rows.append({
                "family": r["family"],
                "family_params": r["family_params"], # 已經是可複製的 JSON string
                "risk_json": _risk_str,              # 新增：可複製的風控 JSON
                "fee_side": r["fee_side"],
                "entries": r["entries"],
                "trades": r["trades"],
                "win_rate_pct": r["win_rate_pct"],
                "avg_win_pct": r["avg_win_pct"],
                "avg_loss_pct": r["avg_loss_pct"],
                "payoff": r["payoff"],
                "profit_factor": r["profit_factor"],
                "expectancy_pct": r["expectancy_pct"],
                "total_return_pct": r["total_return_pct"],
                "cagr_pct": r["cagr_pct"],
                "max_drawdown_pct": r["max_drawdown_pct"],
                "sharpe": r["sharpe"],
                "sortino": r["sortino"],
                "calmar": r["calmar"],
                "avg_hold_bars": r["avg_hold_bars"],
                "time_in_market_pct": r["time_in_market_pct"],
                "bars": r["bars"],
                "start_ts": r["start_ts"],
                "end_ts": r["end_ts"],
                "bpy": r["bpy"],
                "stats_breakdown": r.get("stats_breakdown", "N/A")
            })
        resdf = pd.DataFrame(rows)

        # 排序邏輯：先依 CAGR，其次 PF，其次 MaxDD 由小到大
        # 但對 path-dependent（Trailing / Logic Exit）的策略，快速掃描可能過度樂觀；
        # 因此：先用快速結果排出 Top-K，再用 1m 精準撮合回補驗證，避免「表格很賺但淨值很虧」。
        resdf["verified"] = False

        # 先做一次快速排序（用掃描值）
        resdf["_sort_cagr"] = resdf["cagr_pct"].astype(float)
        resdf["_sort_pf"] = resdf["profit_factor"].replace({0: np.nan}).astype(float).fillna(0.0)
        resdf["_sort_dd"] = -resdf["max_drawdown_pct"].astype(float)
        resdf = resdf.sort_values(by=["_sort_cagr", "_sort_pf", "_sort_dd"], ascending=[False, False, False])

        # 只針對複雜策略做 Top-K 1m 驗證（快篩 + 精準回補）
        if (family in ["TEMA_RSI", "LaguerreRSI_TEMA"]) and bool(st.session_state.get("use_1m_fill", False)) and (st.session_state.get("microfill_ctx", None) is not None):
            verify_k = int(min(len(resdf), max(int(topN) * 10, 50)))
            verify_uids = list(resdf.index[:verify_k])

            st.info(f"已偵測到複雜策略（{family}）：正在用 1m 精準撮合驗證 Top-{verify_k}，避免快速掃描過度樂觀…")
            _vp = st.progress(0.0)

            for ii, uid in enumerate(verify_uids):
                try:
                    det = recompute_best_detail(df, results[int(uid)])

                    # 把回補結果寫回 results（後面畫淨值 / 匯出交易明細會直接用）
                    for _k in [
                        "total_return_pct", "cagr_pct", "max_drawdown_pct",
                        "sharpe", "sortino", "calmar",
                        "trades", "entries", "win_rate_pct",
                        "avg_win_pct", "avg_loss_pct", "payoff",
                        "profit_factor", "expectancy_pct",
                        "avg_hold_bars", "time_in_market_pct",
                        "bars", "start_ts", "end_ts", "bpy"
                    ]:
                        if _k in det:
                            results[int(uid)][_k] = det[_k]
                            resdf.at[uid, _k] = det[_k]

                    if "stats_breakdown" in det:
                        results[int(uid)]["stats_breakdown"] = det["stats_breakdown"]
                        resdf.at[uid, "stats_breakdown"] = det["stats_breakdown"]

                    if "equity_curve" in det:
                        results[int(uid)]["equity_curve"] = det["equity_curve"]
                    if "trades_detail" in det:
                        results[int(uid)]["trades_detail"] = det["trades_detail"]

                    results[int(uid)]["verified"] = True
                    resdf.at[uid, "verified"] = True
                except Exception:
                    pass

                _vp.progress((ii + 1) / float(verify_k))

            _vp.empty()
            st.success(f"1m 驗證完成：Top-{verify_k} 已回補 verified 欄位，可直接用 verified 結果排序。")

        # 最終排序：verified 置頂，再依 CAGR/PF/DD 排
        resdf["_sort_verified"] = resdf["verified"].astype(int)
        resdf["_sort_cagr"] = resdf["cagr_pct"].astype(float)
        resdf["_sort_pf"] = resdf["profit_factor"].replace({0: np.nan}).astype(float).fillna(0.0)
        resdf["_sort_dd"] = -resdf["max_drawdown_pct"].astype(float)

        resdf = resdf.sort_values(
            by=["_sort_verified", "_sort_cagr", "_sort_pf", "_sort_dd"],
            ascending=[False, False, False, False]
        ).drop(columns=["_sort_verified", "_sort_cagr", "_sort_pf", "_sort_dd"])

        # 輸出 CSV
        ensure_dir("outputs")
        ts_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        csv_path = os.path.join("outputs", f"grid_results_{family}_{ts_tag}.csv")
        
        # [教授修正] 欄位定義：移除 tp_pct, sl_pct, max_hold，加入 risk_json
        pretty_cols = [
            "family", "family_params", "risk_json", "fee_side", "verified",
            "entries", "trades", "win_rate_pct", "avg_win_pct", "avg_loss_pct", 
            "payoff", "profit_factor", "expectancy_pct",
            "total_return_pct", "cagr_pct", "max_drawdown_pct", 
            "sharpe", "sortino", "calmar", "avg_hold_bars", "time_in_market_pct",
            "bars", "bpy", "start_ts", "end_ts", "stats_breakdown"
        ]
        
        # [教授修正] 確保所有輸出的 CSV 都是中文 Header
        col_zh = {
            "stats_breakdown": "進場原因績效分析",
            "family": "指標家族",
            "family_params": "指標參數JSON",
            "risk_json": "風控JSON",
            "fee_side": "單邊手續費",
            "verified": "已驗證(1m)",            
            "entries": "訊號數",
            "trades": "交易筆數",
            "win_rate_pct": "勝率%",
            "avg_win_pct": "平均獲利%",
            "avg_loss_pct": "平均虧損%",
            "payoff": "盈虧比",
            "profit_factor": "獲利因子",
            "expectancy_pct": "單筆期望%",
            "total_return_pct": "總報酬%",
            "cagr_pct": "年化報酬%",
            "max_drawdown_pct": "最大回撤%",
            "sharpe": "夏普",
            "sortino": "索提諾",
            "calmar": "卡瑪",
            "avg_hold_bars": "平均持倉K數",
            "time_in_market_pct": "在市時間%",
            "bars": "K線數",
            "bpy": "每年K數",
            "start_ts": "開始時間",
            "end_ts": "結束時間"
        }
        
        # 建立全中文的 DataFrame
        resdf_out = resdf[pretty_cols].rename(columns=col_zh)
        
        # 儲存 Top-N (或完整，視需求) 到標準路徑
        resdf_out.to_csv(csv_path, index=False, encoding="utf-8-sig")
        st.success("已輸出績效表")

        # === 下載所有組合（完整結果，不只 Top‑N） ===
        # 這裡也必須使用 resdf_out (中文版)，確保所有下載檔案格式一致
        full_csv_path = os.path.join("outputs", f"grid_results_all_{family}_{ts_tag}.csv")
        full_parquet_path = os.path.join("outputs", f"grid_results_all_{family}_{ts_tag}.parquet")
        
        resdf_out.to_csv(full_csv_path, index=False, encoding="utf-8-sig")
        try:
            # Parquet 建議保留英文欄位以便程式讀取，但若要求全中文，則存中文版
            # 考量使用者需求為「全中文版」，這裡存中文版
            resdf_out.to_parquet(full_parquet_path, index=False)
        except Exception:
            full_parquet_path = None
        # --- 保存下載 payload（供鎖定畫面 rerun 用） ---
        try:
            full_csv_bytes = open(full_csv_path, "rb").read()
        except Exception:
            full_csv_bytes = None

        full_parquet_bytes = None
        if full_parquet_path:
            try:
                full_parquet_bytes = open(full_parquet_path, "rb").read()
            except Exception:
                full_parquet_bytes = None

        try:
            perf_csv_bytes = open(csv_path, "rb").read()
        except Exception:
            perf_csv_bytes = None

        st.session_state["last_run_payload"] = {
            "topN": topN,
            "full_csv_bytes": full_csv_bytes,
            "full_csv_name": os.path.basename(full_csv_path),
            "full_parquet_bytes": full_parquet_bytes,
            "full_parquet_name": os.path.basename(full_parquet_path) if full_parquet_path else None,
            "perf_csv_bytes": perf_csv_bytes,
            "perf_csv_name": os.path.basename(csv_path),
        }

        st.download_button(
            " 下載完整結果（CSV，所有組合）",
            data=full_csv_bytes if full_csv_bytes is not None else b"",
            file_name=os.path.basename(full_csv_path),
            mime="text/csv",
            help="包含此次搜尋的所有組合，不只 Top‑N"
        )
        if full_parquet_path:
            st.download_button(
                "下載完整結果（Parquet，所有組合）",
                data=full_parquet_bytes if full_parquet_bytes is not None else b"",
                file_name=os.path.basename(full_parquet_path),
                mime="application/octet-stream",
                help="Parquet 保留型別，適合 Python/R 之後再分析"
            )

        st.download_button("下載績效表", data=perf_csv_bytes if perf_csv_bytes is not None else b"", file_name=os.path.basename(csv_path), mime="text/csv")

        # 顯示 Top‑N
        st.subheader(f"最佳 {topN} 組合（依年化報酬率、獲利因子、最大回撤排序）")
        # --- Top-N 多選區 ---
        top_df = resdf_out.head(topN).copy()
        # 給每組一個 uid，利於索引
        top_df["_uid"] = np.arange(len(top_df))
        # --- 保存 Top-N 顯示（供鎖定畫面 rerun 用） ---
        if "last_run_payload" in st.session_state:
            st.session_state["last_run_payload"]["top_df_display"] = top_df.drop(columns=["_uid"])

        st.dataframe(top_df.drop(columns=["_uid"]), use_container_width=True)

        # 提供多選
        selected_uids = st.multiselect(
            "選擇要下載的組合（可多選，無上限）",
            options=list(top_df["_uid"]),
            default=list(top_df["_uid"]),
            format_func=lambda i: f"{top_df.iloc[i]['指標家族']} | {top_df.iloc[i]['指標參數JSON']} | CAGR {top_df.iloc[i]['年化報酬%']}% | MDD {top_df.iloc[i]['最大回撤%']}%"
        )

        # 打包下載：選到的每組 → 交易明細 CSV + 淨值曲線 PNG + 清單 CSV → ZIP
        import io, zipfile

        # --- 依選取內容快取 ZIP，避免每次 rerun 重做 ---
        _sel_key = (family, ts_tag, tuple(sorted([int(i) for i in selected_uids])))
        _zip_cache = st.session_state.get("selected_zip_cache")
        if not isinstance(_zip_cache, dict):
            _zip_cache = {}
            st.session_state["selected_zip_cache"] = _zip_cache

        if _sel_key in _zip_cache:
            zip_bytes = _zip_cache[_sel_key]
        else:
            zbuf = io.BytesIO()
            with zipfile.ZipFile(zbuf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                # 先記錄一份選取清單
                pick_rows = []
                for i in selected_uids:
                    row = top_df.iloc[int(i)]
                    pick_rows.append(row.to_dict())
                picks_csv = pd.DataFrame(pick_rows)
                picks_name = f"selected_top_{len(selected_uids)}_{family}_{ts_tag}.csv"
                zf.writestr(picks_name, picks_csv.to_csv(index=False, encoding="utf-8-sig"))

                # 對每個被選組合，回補明細並輸出
                for i in selected_uids:
                    # 找回原始 result 物件（用排名映射）
                    # [專家修正] 修正索引映射邏輯：resdf 已經排序過，top_df 是 resdf 的切片。
                    # top_df.index[int(i)] 取得的 label 本身就是原始 results list 的索引值 (因為 resdf 建立時 index 是預設 range)
                    # 原寫法 resdf.index[...] 會造成二次位移，導致下載到錯誤的策略明細。
                    res_idx = top_df.index[int(i)]
                    r = results[int(res_idx)]
                    # GPU 扫描時可能沒明細：回補
                    if ("equity_curve" not in r) or ("trades_detail" not in r) or skip_trade_details:
                        r = recompute_best_detail(df, r)

                    # 交易明細 CSV
                    tdf = pd.DataFrame(r["trades_detail"]).rename(columns={
                        "entry_index":"進場索引","entry_ts":"進場時間","entry_price":"進場價格",
                        "exit_index":"出場索引","exit_ts":"出場時間","exit_price":"出場價格",
                        "gross_return":"毛報酬","net_return":"淨報酬","bars_held":"持倉K數",
                        "reason":"出場原因","tp_pct":"停利%","sl_pct":"停損%"
                    })
                    base = f"{r['family']}_{abs(hash(r['family_params'])) % 10**8}_{ts_tag}"
                    zf.writestr(f"{base}_trades.csv", tdf.to_csv(index=False, encoding="utf-8-sig"))

                    # 淨值曲線 PNG
                    try:
                        import plotly.io as pio
                        eq = r["equity_curve"]
                        tss = pd.to_datetime(df["ts"])
                        fig2 = go.Figure()
                        fig2.add_trace(go.Scatter(x=tss, y=eq, mode="lines", name="Equity"))
                        fig2.update_layout(
                            title=f"{r['family']} {r['family_params']} | CAGR {r['cagr_pct']}% | MDD {r['max_drawdown_pct']}%",
                            xaxis_title="Time", yaxis_title="Equity (base=1.0)", height=420,
                        )
                        png_bytes = pio.to_image(fig2, format="png", width=1280, height=640, scale=2)
                        zf.writestr(f"{base}_equity.png", png_bytes)
                    except Exception:
                        # 沒有 kaleido 也不會壞：跳過圖
                        pass

            zbuf.seek(0)
            zip_bytes = zbuf.getvalue()
            _zip_cache[_sel_key] = zip_bytes

        zip_file_name = f"selected_details_{family}_{ts_tag}.zip"

        # --- 保存到 payload，確保 download_button rerun 後仍存在 ---
        if "last_run_payload" not in st.session_state or not isinstance(st.session_state.get("last_run_payload"), dict):
            st.session_state["last_run_payload"] = {}
        st.session_state["last_run_payload"]["zip_bytes"] = zip_bytes
        st.session_state["last_run_payload"]["zip_file_name"] = zip_file_name

        st.download_button(
            " 下載所選組合：交易明細 + 淨值曲線（ZIP）",
            data=zip_bytes if zip_bytes is not None else b"",
            file_name=zip_file_name,
            mime="application/zip",
            key="download_selected_zip_btn",
            help="包含：每個所選組合的交易明細 CSV、淨值曲線 PNG，以及一份選取清單 CSV"
        )



        st.dataframe(resdf_out.head(topN), use_container_width=True)

        # 畫最佳組合的淨值曲線（GPU 掃描可能無明細，必要時回補）
        best = results[resdf.index[0]]
        if ("equity_curve" not in best) or ("trades_detail" not in best) or skip_trade_details:
            best = recompute_best_detail(df, best)

        eq = best["equity_curve"]
        tss = pd.to_datetime(df["ts"])
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=tss, y=eq, mode="lines", name="Equity"))
        fig.update_layout(
            title=f"最佳組合淨值曲線 | {best['family']} {best['family_params']} | CAGR {best['cagr_pct']}% | MDD {best['max_drawdown_pct']}%",
            xaxis_title="Time",
            yaxis_title="Equity (base=1.0)",
            height=420,
        )
        st.plotly_chart(fig, use_container_width=True)

        # 交易細項導出（此時一定有）
        trades_path = os.path.join("outputs", f"best_trades_{best['family']}_{ts_tag}.csv")
        tdf = pd.DataFrame(best["trades_detail"])
        tdf = tdf.rename(columns={
            "entry_index":"進場索引",
            "entry_ts":"進場時間",
            "entry_price":"進場價格",
            "exit_index":"出場索引",
            "exit_ts":"出場時間",
            "exit_price":"出場價格",
            "gross_return":"毛報酬",
            "net_return":"淨報酬",
            "bars_held":"持倉K數",
            "reason":"出場原因",
            "tp_pct":"停利%",
            "sl_pct":"停損%",
            "ob_start_index":"OB起始索引",
            "ob_end_index":"OB結束索引",
            "ob_start_ts":"OB起始時間",
            "ob_end_ts":"OB結束時間",
            "ob_top":"OB高點",
            "ob_bottom":"OB低點",
            "fvg_start_index":"FVG起始索引",
            "fvg_end_index":"FVG結束索引",
            "fvg_start_ts":"FVG起始時間",
            "fvg_end_ts":"FVG結束時間",
            "fvg_top":"FVG高點",
            "fvg_bottom":"FVG低點",
            "zone_trigger_type":"觸發類型",
            "entry_reason": "進場策略原因"

        })
        tdf.to_csv(trades_path, index=False, encoding="utf-8-sig")
        try:
            best_trades_bytes = open(trades_path, "rb").read()
        except Exception:
            best_trades_bytes = None

        # 保存到 payload，讓下載按鈕 rerun 後不會消失
        if "last_run_payload" not in st.session_state or not isinstance(st.session_state.get("last_run_payload"), dict):
            st.session_state["last_run_payload"] = {}
        st.session_state["last_run_payload"]["best_trades_bytes"] = best_trades_bytes
        st.session_state["last_run_payload"]["best_trades_name"] = os.path.basename(trades_path)

        st.download_button(
            "下載交易明細",
            data=best_trades_bytes if best_trades_bytes is not None else b"",
            file_name=os.path.basename(trades_path),
            mime="text/csv"
        )



if __name__ == "__main__":
    st.set_page_config(page_title="格點回測控制台", layout="wide")
    app()
