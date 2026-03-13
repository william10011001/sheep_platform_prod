import argparse
import json
import os
import random
import hashlib
import sys
import time
import uuid
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

import backtest_panel2 as bt

# =========================================================
# 專家級多進程核心：加入終端機詳細調試輸出，揭露假死真相
# =========================================================
def _process_eval_chunk(args):
    import os, time, traceback
    pid = os.getpid()
    is_fast, f_params, e_sig, risk_grid_chunk, df, family, fee_side, slippage, worst_case, reverse_mode = args
    
    print(f"\n[進程 {pid}] 📥 成功接收區塊！準備運算 {len(risk_grid_chunk)} 個參數組合 (FastMode: {is_fast})", flush=True)
    t0 = time.time()
    
    results = []
    try:
        import backtest_panel2 as bt
        for tp, sl, mh in risk_grid_chunk:
            if is_fast:
                res = bt.run_backtest_from_entry_sig(df, e_sig, tp, sl, mh, fee_side=fee_side, slippage=slippage, worst_case=worst_case, reverse_mode=reverse_mode)
            else:
                res = bt.run_backtest(df, family, dict(f_params), float(tp), float(sl), int(mh), fee_side=fee_side, slippage=slippage, worst_case=worst_case, reverse_mode=reverse_mode)
            
            metrics = {
                "total_return_pct": float(res.get("total_return_pct", 0.0)),
                "max_drawdown_pct": float(res.get("max_drawdown_pct", 0.0)),
                "sharpe": float(res.get("sharpe", 0.0)),
                "trades": int(res.get("trades", 0)),
                "win_rate_pct": float(res.get("win_rate_pct", 0.0)),
                "profit_factor": float(res.get("profit_factor", 0.0)),
                "cagr_pct": float(res.get("cagr_pct", 0.0)),
            }
            score = float(metrics["total_return_pct"]) + 5.0 * float(metrics["sharpe"]) - 0.6 * float(metrics["max_drawdown_pct"])
            params = {"family": family, "family_params": dict(f_params), "tp": float(tp), "sl": float(sl), "max_hold": int(mh)}
            results.append((score, params, metrics))
    except Exception as e:
        print(f"\n[進程 {pid}] ❌ 執行期間發生嚴重崩潰: {e}", flush=True)
        traceback.print_exc()
        
    t1 = time.time()
    print(f"[進程 {pid}] 🏁 區塊運算完畢！耗時: {t1-t0:.2f} 秒，產出 {len(results)} 筆結果。", flush=True)
    return results

WORKER_VERSION = "2.1.0"
WORKER_PROTOCOL = 2


def parse_semver(v: str) -> Tuple[int, int, int]:
    try:
        parts = str(v or "").strip().split(".", 2)
        if len(parts) != 3:
            return (0, 0, 0)
        return (int(parts[0]), int(parts[1]), int(parts[2].split("-", 1)[0].split("+", 1)[0]))
    except Exception:
        return (0, 0, 0)


def semver_gte(a: str, b: str) -> bool:
    return parse_semver(a) >= parse_semver(b)


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


_DATA_HASH_CACHE: Dict[str, Tuple[int, int, str]] = {}


def _sha256_file_cached(path: str) -> str:
    try:
        st = os.stat(path)
        size = int(st.st_size)
        mtime = int(st.st_mtime)
        cached = _DATA_HASH_CACHE.get(path)
        if cached and cached[0] == size and cached[1] == mtime:
            return cached[2]
        h = _sha256_file(path)
        _DATA_HASH_CACHE[path] = (size, mtime, h)
        return h
    except Exception:
        return ""


@dataclass
class Thresholds:
    min_trades: int
    min_total_return_pct: float
    max_drawdown_pct: float
    min_sharpe: float
    keep_top_n: int

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Thresholds":
        return Thresholds(
            min_trades=int(d.get("min_trades", 30)),
            min_total_return_pct=float(d.get("min_total_return_pct", 3.0)),
            max_drawdown_pct=float(d.get("max_drawdown_pct", 25.0)),
            min_sharpe=float(d.get("min_sharpe", 0.6)),
            keep_top_n=int(d.get("keep_top_n", 30)),
        )


def _load_or_create_worker_id(path: str) -> str:
    path = str(path or "").strip() or ".sheep_worker_id"
    try:
        if os.path.exists(path):
            s = open(path, "r", encoding="utf-8").read().strip()
            if s:
                return s
    except Exception:
        pass
    wid = uuid.uuid4().hex
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(wid)
    except Exception:
        pass
    return wid


class ApiClient:
    def __init__(self, base_url: str, token: str, worker_id: str):
        self.base_url = str(base_url or "").rstrip("/")
        self.token = str(token or "").strip()
        self.worker_id = str(worker_id or "").strip()

        if not self.base_url.startswith("http"):
            raise ValueError("base_url must start with http:// or https://")
        if not self.token:
            raise ValueError("token is required")
        if not self.worker_id:
            raise ValueError("worker_id is required")

        self._session = requests.Session()

    def _headers(self) -> Dict[str, str]:
        h = {
            "Authorization": f"Bearer {self.token}",
            "X-Worker-Id": self.worker_id,
            "X-Worker-Version": WORKER_VERSION,
            "X-Worker-Protocol": str(WORKER_PROTOCOL),
        }
        try:
            cur_tid = getattr(self, "_current_task_id", None)
            if cur_tid is not None:
                h["X-Current-Task-Id"] = str(int(cur_tid))
        except Exception:
            pass
        return h

    def set_current_task_id(self, task_id: Optional[int]) -> None:
        try:
            self._current_task_id = int(task_id) if task_id is not None else None
        except Exception:
            self._current_task_id = None

    def _request(self, method: str, path: str, *, json_body: Optional[Dict[str, Any]] = None, timeout_s: float = 30.0):
        url = self.base_url + path
        last_error_msg = ""
        for attempt in range(6):
            try:
                r = self._session.request(method, url, headers=self._headers(), json=json_body, timeout=timeout_s)
            except requests.exceptions.RequestException as e:
                # 網路異常或超時，攔截錯誤並進行重試
                last_error_msg = str(e)
                wait_s = 2.0 + attempt * 2.0
                time.sleep(wait_s)
                continue
                
            if r.status_code in (429, 503, 502):
                # backoff
                ra = r.headers.get("Retry-After")
                try:
                    wait_s = float(ra) if ra else (1.0 + attempt * 1.0)
                except Exception:
                    wait_s = 1.0 + attempt * 1.0
                time.sleep(min(15.0, max(0.5, wait_s)))
                continue
            if r.status_code == 426:
                try:
                    detail = r.json()
                except Exception:
                    detail = r.text
                raise RuntimeError(f"upgrade_required: {detail}")
            if r.status_code >= 400:
                raise RuntimeError(f"api_error {r.status_code}: {r.text}")
            if not r.content:
                return None
            return r.json()
        raise RuntimeError(f"api_unavailable: Failed after 6 attempts. Last network error: {last_error_msg}")

    def manifest(self) -> Dict[str, Any]:
        url = self.base_url + "/manifest"
        r = self._session.get(url, timeout=10)
        if r.status_code >= 400:
            raise RuntimeError(f"manifest_error {r.status_code}: {r.text}")
        return r.json()

    def flags(self) -> Dict[str, Any]:
        return self._request("GET", "/flags", timeout_s=10) or {}

    def get_thresholds(self) -> Dict[str, Any]:
        return self._request("GET", "/settings/thresholds", timeout_s=10) or {}

    def get_settings_snapshot(self) -> Dict[str, Any]:
        return self._request("GET", "/settings/snapshot", timeout_s=10) or {}

    def heartbeat(self, current_task_id: Optional[int] = None) -> None:
        headers = self._headers()
        if current_task_id is not None:
            headers["X-Current-Task-Id"] = str(int(current_task_id))
        url = self.base_url + "/workers/heartbeat"
        try:
            r = self._session.post(url, headers=headers, timeout=10)
            if r.status_code in (429, 503, 502):
                return
            if r.status_code >= 400:
                # don't crash on heartbeat
                return
        except requests.exceptions.RequestException:
            pass

    def claim_task(self) -> Optional[Dict[str, Any]]:
        # [極致防護] 大幅延長領取任務的超時時間至 600 秒。
        # 避免伺服器端正在同步歷史 K 線資料時，客戶端因為預設的 20 秒超時而斷開連線，
        # 導致任務在伺服器變成「幽靈執行中(Zombie)」卡死，而客戶端卻顯示「無可用任務」。
        return self._request("POST", "/tasks/claim", timeout_s=600.0)
    def claim_oos_task(self, worker_id: str, version: str, protocol: int) -> Optional[dict]:
        headers = self._headers(worker_id, version, protocol)
        try:
            r = requests.post(f"{self.base_url}/tasks/oos/claim", headers=headers, timeout=10)
            if r.status_code == 200:
                return r.json().get("task")
        except Exception:
            pass
        return None

    def finish_oos_task(self, task_id: int, worker_id: str, version: str, protocol: int, passed: bool, metrics: dict) -> bool:
        headers = self._headers(worker_id, version, protocol)
        try:
            r = requests.post(
                f"{self.base_url}/tasks/oos/{task_id}/finish", 
                json={"passed": passed, "metrics": metrics}, 
                headers=headers, timeout=10
            )
            return r.status_code == 200
        except Exception:
            return False
    def progress(self, task_id: int, lease_id: str, progress: Dict[str, Any]) -> None:
        self._request("POST", f"/tasks/{int(task_id)}/progress", json_body={"lease_id": str(lease_id), "progress": progress}, timeout_s=20)

    def release(self, task_id: int, lease_id: str, progress: Dict[str, Any]) -> None:
        self._request("POST", f"/tasks/{int(task_id)}/release", json_body={"lease_id": str(lease_id), "progress": progress}, timeout_s=20)

    def finish(self, task_id: int, lease_id: str, candidates: List[Dict[str, Any]], final_progress: Dict[str, Any]) -> Dict[str, Any]:
        return self._request(
            "POST",
            f"/tasks/{int(task_id)}/finish",
            json_body={"lease_id": str(lease_id), "candidates": candidates, "final_progress": final_progress},
            timeout_s=60,
        ) or {}


_SPECIAL_FAMILIES = {
    "TEMA_RSI",
    "LaguerreRSI_TEMA",
    "TEMA_Lag",
    "Laguerre_RSI",
}


def _build_risk_grid(risk_spec: Dict[str, Any]) -> List[Tuple[float, float, int]]:
    tp_min = float(risk_spec.get("tp_min", 0.3))
    tp_max = float(risk_spec.get("tp_max", 1.2))
    tp_step = max(1e-9, float(risk_spec.get("tp_step", 0.1)))

    sl_min = float(risk_spec.get("sl_min", 0.3))
    sl_max = float(risk_spec.get("sl_max", 1.2))
    sl_step = max(1e-9, float(risk_spec.get("sl_step", 0.1)))

    mh_min = int(risk_spec.get("max_hold_min", 4))
    mh_max = int(risk_spec.get("max_hold_max", 80))
    mh_step = max(1, int(risk_spec.get("max_hold_step", 4)))

    tp_list = []
    x = tp_min
    while x <= tp_max + 1e-12:
        tp_list.append(round(float(x) / 100.0, 6))
        x += tp_step

    sl_list = []
    x = sl_min
    while x <= sl_max + 1e-12:
        sl_list.append(round(float(x) / 100.0, 6))
        x += sl_step

    mh_list = list(range(mh_min, mh_max + 1, mh_step))
    out: List[Tuple[float, float, int]] = []
    for tp in tp_list:
        for sl in sl_list:
            for mh in mh_list:
                out.append((float(tp), float(sl), int(mh)))
    return out


def _passes_thresholds(metrics: Dict[str, Any], thr: Thresholds) -> bool:
    try:
        trades = int(metrics.get("trades", 0))
        total_return_pct = float(metrics.get("total_return_pct", -1e9))
        max_dd_pct = float(metrics.get("max_drawdown_pct", 1e9))
        sharpe = float(metrics.get("sharpe", -1e9))
    except Exception:
        return False

    if trades < thr.min_trades:
        return False
    if total_return_pct < thr.min_total_return_pct:
        return False
    if max_dd_pct > thr.max_drawdown_pct:
        return False
    if sharpe < thr.min_sharpe:
        return False
    return True


def _metrics_from_bt_result(res: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "total_return_pct": float(res.get("total_return_pct", 0.0)),
        "max_drawdown_pct": float(res.get("max_drawdown_pct", 0.0)),
        "sharpe": float(res.get("sharpe", 0.0)),
        "trades": int(res.get("trades", 0)),
        "win_rate_pct": float(res.get("win_rate_pct", 0.0)),
        "profit_factor": float(res.get("profit_factor", 0.0)),
        "cagr_pct": float(res.get("cagr_pct", 0.0)),
    }


def _score(metrics: Dict[str, Any]) -> float:
    ret = float(metrics.get("total_return_pct", 0.0))
    dd = float(metrics.get("max_drawdown_pct", 0.0))
    sh = float(metrics.get("sharpe", 0.0))
    return ret + 5.0 * sh - 0.6 * dd


def run_task(api: ApiClient, task: Dict[str, Any], thr: Thresholds, flag_poll_s: float, commit_every: int) -> None:
    task_id = int(task["task_id"])
    try:
        api.set_current_task_id(int(task_id))
    except Exception:
        pass
    lease_id = str(task.get("lease_id") or "")
    if not lease_id:
        raise RuntimeError("missing_lease_id")

    family = str(task["family"])
    grid_spec = dict(task.get("grid_spec") or {})
    risk_spec = dict(task.get("risk_spec") or {})
    partition_idx = int(task["partition_idx"])
    num_partitions = int(task["num_partitions"])

    # resume state
    progress = dict(task.get("progress") or {})
    if not progress:
        progress = {
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
        }

    resume_done = int(progress.get("combos_done") or 0)

    best_pass: List[Tuple[float, Dict[str, Any], Dict[str, Any]]] = []
    for c in progress.get("checkpoint_candidates") or []:
        try:
            best_pass.append((float(c.get("score") or 0.0), dict(c.get("params") or {}), dict(c.get("metrics") or {})))
        except Exception:
            pass
    best_pass.sort(key=lambda x: x[0], reverse=True)

    best_any_score: Optional[float] = None
    best_any_metrics: Optional[Dict[str, Any]] = None
    best_any_params: Optional[Dict[str, Any]] = None
    best_any_passed: bool = False

    if progress.get("best_any_score") is not None:
        try:
            best_any_score = float(progress.get("best_any_score"))
        except Exception:
            best_any_score = None
    if isinstance(progress.get("best_any_metrics"), dict):
        best_any_metrics = dict(progress.get("best_any_metrics") or {})
    if isinstance(progress.get("best_any_params"), dict):
        best_any_params = dict(progress.get("best_any_params") or {})
    best_any_passed = bool(progress.get("best_any_passed") or False)

    last_flag_check = 0.0

    def _should_stop() -> bool:
        if globals().get("GUI_PAUSED", False):
            return True
        nonlocal last_flag_check
        now = time.time()
        if now - last_flag_check < flag_poll_s:
            return False
        last_flag_check = now
        try:
            f = api.flags()
            return not bool(f.get("run_enabled"))
        except Exception:
            return False

    last_sync_api_ts = 0.0
    last_sync_gui_ts = 0.0

    def _progress_cb(frac: float, msg: str) -> None:
        # 【強制中斷機制】如果偵測到使用者按下關閉視窗或暫停，立即拋出例外打斷底層冗長的下載阻塞！
        if globals().get("GUI_PAUSED", False):
            raise RuntimeError("Task stopped/paused by user during sync")

        import re
        nonlocal last_sync_api_ts, last_sync_gui_ts
        mmsg = str(msg)
        f = float(frac)
        now = time.time()

        # 終極修復：清除隱藏的終端機色彩碼 (ANSI) 與換行符號
        clean_msg = re.sub(r'\x1b\[[0-9;]*m', '', mmsg).strip()

        # 放棄嚴格的 match，改用 search 尋找字串中的數字，徹底無視多餘的前後綴
        m = re.search(r"(\S*)\s*已寫入\s*(\d+)\s*/\s*(\d+)", clean_msg)
        
        done_i = 0
        total_i = 0
        label = "sync"
        if m:
            label = str(m.group(1)) or "sync"
            done_i = int(m.group(2))
            total_i = int(m.group(3))
            if total_i > 0:
                f = float(done_i) / float(total_i)

        # 1. 更新本機端 GUI (高頻：每 0.1 秒刷新一次，保證視覺平滑)
        if now - last_sync_gui_ts > 0.1 or f >= 1.0:
            last_sync_gui_ts = now
            if globals().get("GUI_QUEUE"):
                globals()["GUI_QUEUE"].put({"type": "status", "msg": clean_msg, "frac": f})

        # 2. 更新伺服器進度 (極低頻：每 2.5 秒刷新一次)
        # 這是解決「等待分配算力」的核心：避免一秒打上千次 API 導致被伺服器阻斷丟包！
        if now - last_sync_api_ts > 2.5 or f >= 1.0:
            last_sync_api_ts = now
            
            progress["phase"] = "sync_data"
            progress["phase_progress"] = f
            progress["phase_msg"] = clean_msg
            
            if m and total_i > 0:
                # 同步給網站前端的進度與組合數，確保綠色進度條與數字完全一致
                progress["combos_done"] = done_i
                progress["combos_total"] = total_i
                
                sync = progress.get("sync")
                if not isinstance(sync, dict):
                    sync = {"items": {}, "current": ""}
                items = sync.get("items")
                if not isinstance(items, dict):
                    items = {}
                items[label] = {"done": done_i, "total": total_i}
                sync["items"] = items
                sync["current"] = label
                progress["sync"] = sync

            try:
                api.progress(task_id, lease_id, progress)
            except Exception:
                pass

    progress["phase"] = "sync_data"
    progress["phase_progress"] = 0.0
    progress["phase_msg"] = ""
    api.progress(task_id, lease_id, progress)

    if globals().get("GUI_QUEUE"):
        globals()["GUI_QUEUE"].put({"type": "progress", "done": 0, "total": 1, "speed": 0.0})
        globals()["GUI_QUEUE"].put({"type": "status", "msg": "準備同步 K 線資料...", "frac": 0.0})

    try:
        years = int(task.get("years") or 0) or 3
        csv_main, _ = bt.ensure_bitmart_data(
            symbol=str(task["symbol"]),
            main_step_min=int(task["timeframe_min"]),
            years=int(years),
            auto_sync=True,
            force_full=False,
            progress_cb=_progress_cb,
        )
        df = bt.load_and_validate_csv(csv_main)

        expected_hash = str(task.get("data_hash") or "").strip()
        local_hash = ""
        try:
            local_hash = _sha256_file_cached(csv_main)
        except Exception:
            local_hash = ""
        progress["data_hash"] = local_hash

        if expected_hash and local_hash and expected_hash != local_hash:
            progress["phase"] = "sync_data"
            progress["phase_progress"] = 0.0
            progress["phase_msg"] = "資料版本不同，重新同步"
            api.progress(task_id, lease_id, progress)

            csv_main, _ = bt.ensure_bitmart_data(
                symbol=str(task["symbol"]),
                main_step_min=int(task["timeframe_min"]),
                years=int(years),
                auto_sync=True,
                force_full=True,
                progress_cb=_progress_cb,
            )
            df = bt.load_and_validate_csv(csv_main)

            try:
                local_hash = _sha256_file_cached(csv_main)
            except Exception:
                local_hash = ""
            progress["data_hash"] = local_hash

            if local_hash and expected_hash != local_hash:
                progress["phase"] = "error"
                progress["last_error"] = "data_hash_mismatch"
                api.release(task_id, lease_id, progress)
                return

        if _should_stop():
            progress["phase"] = "stopped"
            api.release(task_id, lease_id, progress)
            return

        progress["phase"] = "build_grid"
        progress["phase_progress"] = 1.0
        progress["phase_msg"] = ""
        api.progress(task_id, lease_id, progress)
        
        if globals().get("GUI_QUEUE"):
            globals()["GUI_QUEUE"].put({"type": "status", "msg": "正在建構參數網格...", "frac": 0.0})

        combos = bt.grid_combinations_from_ui(family, grid_spec)
        seed = int(task.get("seed") or 0) ^ (task_id & 0x7FFFFFFF)
        rng = random.Random(seed)
        rng.shuffle(combos)

        part = combos[partition_idx::num_partitions]

        risk_grid = _build_risk_grid(risk_spec)
        if family in ("TEMA_RSI", "LaguerreRSI_TEMA"):
            mh_min = int(risk_spec.get("max_hold_min", 4))
            mh_max = int(risk_spec.get("max_hold_max", 80))
            mh_step = max(1, int(risk_spec.get("max_hold_step", 4)))
            mh_list = list(range(mh_min, mh_max + 1, mh_step))
            risk_grid = [(0.0, 0.0, int(mh)) for mh in mh_list]

        combos_total = int(len(part) * max(1, len(risk_grid)))
        progress["phase"] = "grid_search"
        progress["combos_total"] = combos_total
        api.progress(task_id, lease_id, progress)

        if globals().get("GUI_QUEUE"):
            globals()["GUI_QUEUE"].put({"type": "status", "msg": f"開始進行格點搜尋...", "frac": 0.0})
            globals()["GUI_QUEUE"].put({"type": "progress", "done": resume_done, "total": combos_total, "speed": 0.0})

        fee_side = float(risk_spec.get("fee_side", 0.0002))
        slippage = float(risk_spec.get("slippage", 0.0))
        worst_case = bool(risk_spec.get("worst_case", True))
        reverse_mode = bool(risk_spec.get("reverse_mode", False))

        keep_top = int(thr.keep_top_n)

        done = min(resume_done, combos_total)
        last_commit = done
        last_commit_ts = time.time()
        t0 = time.time()

        def _commit(force: bool = False) -> None:
            nonlocal last_commit, last_commit_ts
            
            elapsed = float(max(0.001, time.time() - t0))
            current_speed = float(done / elapsed) if elapsed > 0 else 0.0
            
            if globals().get("GUI_QUEUE"):
                globals()["GUI_QUEUE"].put({
                    "type": "progress",
                    "done": done,
                    "total": combos_total,
                    "speed": current_speed
                })

            if not force and (done - last_commit) < commit_every and (time.time() - last_commit_ts) < 10.0:
                return
                
            last_commit = done
            last_commit_ts = time.time()
            eta = float((combos_total - done) / current_speed) if current_speed > 0 and combos_total > done else None
            
            progress["combos_done"] = int(done)
            progress["elapsed_s"] = round(elapsed, 3)
            progress["speed_cps"] = round(current_speed, 6)
            progress["eta_s"] = round(eta, 3) if eta is not None else None
            progress["best_any_score"] = float(best_any_score) if best_any_score is not None else None
            progress["best_any_metrics"] = dict(best_any_metrics) if best_any_metrics is not None else None
            progress["best_any_params"] = dict(best_any_params) if best_any_params is not None else None
            progress["best_any_passed"] = bool(best_any_passed)
            progress["best_score"] = float(best_pass[0][0]) if best_pass else None
            progress["checkpoint_candidates"] = [{"score": float(s), "params": dict(p), "metrics": dict(m)} for s, p, m in best_pass[:keep_top]]
            api.progress(task_id, lease_id, progress)

        if combos_total <= 0:
            _commit(force=True)
            if globals().get("GUI_QUEUE"):
                globals()["GUI_QUEUE"].put({"type": "progress", "done": 1, "total": 1, "speed": 0.0})
            api.finish(task_id, lease_id, [], progress)
            return

        # compute resume indices
        risk_n = max(1, len(risk_grid))
        start_i = int(done // risk_n)
        start_j = int(done % risk_n)

        use_fast_path = bool(
            family not in _SPECIAL_FAMILIES
            and hasattr(bt, "build_cache_for_family")
            and hasattr(bt, "run_backtest_from_entry_sig")
        )

        sig_cache = None
        if use_fast_path and part:
            try:
                sig_cache = bt.build_cache_for_family(df, family, part, logger=None)
            except Exception:
                sig_cache = None
                use_fast_path = False

        def _consider_candidate(score: float, params: Dict[str, Any], metrics: Dict[str, Any], passed: bool) -> None:
            nonlocal best_any_score, best_any_metrics, best_any_params, best_any_passed, best_pass
            if best_any_score is None or score > best_any_score:
                best_any_score = float(score)
                best_any_metrics = dict(metrics)
                best_any_params = dict(params)
                best_any_passed = bool(passed)

            if passed:
                best_pass.append((float(score), dict(params), dict(metrics)))
                best_pass.sort(key=lambda x: x[0], reverse=True)
                if len(best_pass) > keep_top:
                    best_pass = best_pass[:keep_top]

        import concurrent.futures
        import os
        import time

        print("\n" + "="*60, flush=True)
        print(f"[系統調試] 🛠️ 準備切割任務群，策略: {family}, FastMode: {use_fast_path}", flush=True)

        chunks = []
        t_chunk_start = time.time()
        if use_fast_path and sig_cache is not None:
            for i in range(start_i, len(part)):
                f_params = part[i]
                e_sig = sig_cache[i]
                j0 = start_j if i == start_i else 0
                r_grid = risk_grid[j0:]
                if not r_grid: continue
                chunks.append((True, f_params, e_sig, r_grid, df, family, fee_side, slippage, worst_case, reverse_mode))
                start_j = 0
        else:
            for i in range(start_i, len(part)):
                f_params = part[i]
                j0 = start_j if i == start_i else 0
                r_grid = risk_grid[j0:]
                if not r_grid: continue
                chunks.append((False, f_params, None, r_grid, df, family, fee_side, slippage, worst_case, reverse_mode))
                start_j = 0

        print(f"[系統調試] 📦 區塊打包完畢！共 {len(chunks)} 個 Chunks。耗時: {time.time() - t_chunk_start:.2f} 秒。", flush=True)

        max_workers = max(1, (os.cpu_count() or 4) - 1)
        print(f"[系統調試] 🚀 準備喚醒 {max_workers} 個實體 CPU 核心的進程池...", flush=True)
        
        t_dispatch = time.time()
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            print(f"[系統調試] ⚠️ 開始將 {len(chunks)} 個含 DataFrame 的區塊塞入通訊管道 (IPC Pipe)...", flush=True)
            print(f"[系統調試] (若程式卡在此處毫無反應，代表在 Windows 下序列化 DataFrame 的代價過高，導致系統死鎖！)", flush=True)
            
            future_to_chunk = {executor.submit(_process_eval_chunk, c): c for c in chunks}
            
            print(f"[系統調試] ✅ 所有區塊已成功塞入進程池！IPC 傳輸總耗時: {time.time() - t_dispatch:.2f} 秒。等待運算結果...", flush=True)
            
            for future in concurrent.futures.as_completed(future_to_chunk):
                if _should_stop():
                    print("[系統調試] 🛑 收到中斷信號，強制終止進程池...", flush=True)
                    progress["phase"] = "stopped"
                    _commit(force=True)
                    api.release(task_id, lease_id, progress)
                    executor.shutdown(wait=False, cancel_futures=True)
                    return
                
                try:
                    res_list = future.result()
                    print(f"[系統調試] 🟢 成功回收一個區塊，獲得 {len(res_list)} 筆結果。目前總進度: {done + len(res_list)}", flush=True)
                    for score, params, metrics in res_list:
                        passed = _passes_thresholds(metrics, thr)
                        _consider_candidate(score, params, metrics, passed)
                        done += 1
                except Exception as e:
                    import traceback
                    print(f"\n[系統調試] 🚨 主進程回收結果時遭遇例外或子進程死亡: {e}", flush=True)
                    traceback.print_exc()
                
                _commit()

        _commit(force=True)
        cands = [{"score": float(s), "params": dict(p), "metrics": dict(m)} for s, p, m in best_pass[:keep_top]]
        api.finish(task_id, lease_id, cands, progress)
        if globals().get("GUI_QUEUE"):
            globals()["GUI_QUEUE"].put({"type": "status", "msg": "任務完成，準備回報...", "frac": 1.0})
            globals()["GUI_QUEUE"].put({"type": "progress", "done": combos_total, "total": combos_total, "speed": 0.0})
        return

    except Exception as e:
        progress["phase"] = "error"
        progress["last_error"] = str(e)
        try:
            _commit(force=True)
        except Exception:
            pass
        try:
            api.release(task_id, lease_id, progress)
        except Exception:
            pass
    try:
        api.set_current_task_id(None)
    except Exception:
        pass
        return


def _issue_token(base_url: str, username: str, password: str, ttl_seconds: int, name: str) -> str:
    url = str(base_url).rstrip("/") + "/token"
    body = {"username": username, "password": password, "ttl_seconds": int(ttl_seconds), "name": str(name)}
    r = requests.post(url, json=body, timeout=20)
    if r.status_code >= 400:
        raise RuntimeError(f"token_issue_failed {r.status_code}: {r.text}")
    j = r.json()
    return str(j.get("token") or "")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="worker_config.json")
    ap.add_argument("--server", default=None, help="API base URL, e.g. http://127.0.0.1:8000")
    ap.add_argument("--username", default=None)
    ap.add_argument("--password", default=None)
    ap.add_argument("--token_name", default="worker")
    ap.add_argument("--ttl_seconds", type=int, default=86400)
    ap.add_argument("--settings_poll_s", type=float, default=30.0)
    ap.add_argument("--flag_poll_s", type=float, default=5.0)
    ap.add_argument("--commit_every", type=int, default=25)
    ap.add_argument("--idle_sleep_s", type=float, default=2.0)
    args = ap.parse_args()

    cfg: Dict[str, Any] = {}
    if args.config and os.path.exists(args.config):
        with open(args.config, "r", encoding="utf-8") as f:
            cfg = json.load(f) or {}

    base_url = args.server or cfg.get("base_url") or cfg.get("api_url") or "http://127.0.0.1:8000"

    worker_id = cfg.get("worker_id") or _load_or_create_worker_id(cfg.get("worker_id_file") or ".sheep_worker_id")

    token = cfg.get("token") or ""
    if args.username and args.password:
        token = _issue_token(base_url, str(args.username), str(args.password), int(args.ttl_seconds), str(args.token_name))
        # Print once so you can paste into config if you want long-running workers.
        print(f"token_issued: {token[:6]}...{token[-6:]}")

    if not token:
        raise SystemExit("Missing token. Provide it in worker_config.json or use --username/--password to issue one.")

    api = ApiClient(base_url=base_url, token=token, worker_id=worker_id)

    mf = api.manifest()
    min_proto = int(mf.get("worker_min_protocol", 2))
    min_ver = str(mf.get("worker_min_version", "2.0.0"))
    latest_ver = str(mf.get("worker_latest_version", min_ver))

    if WORKER_PROTOCOL < min_proto:
        raise SystemExit(f"worker_protocol_too_old: required>={min_proto} current={WORKER_PROTOCOL}")

    if not semver_gte(WORKER_VERSION, min_ver):
        dl = mf.get("worker_download_url") or ""
        raise SystemExit(f"worker_version_too_old: required>={min_ver} current={WORKER_VERSION} latest={latest_ver} download={dl}")

    if semver_gte(latest_ver, WORKER_VERSION) and parse_semver(latest_ver) > parse_semver(WORKER_VERSION):
        dl = mf.get("worker_download_url") or ""
        print(f"[warn] Newer worker available: {latest_ver} (you: {WORKER_VERSION}). {dl}", file=sys.stderr)

    thr = Thresholds.from_dict(api.get_thresholds())
    last_settings = time.time()

    while True:
        if time.time() - last_settings > float(args.settings_poll_s):
            try:
                snap = api.get_settings_snapshot()
                thr = Thresholds.from_dict(snap.get("thresholds") or {})
            except Exception:
                pass
            last_settings = time.time()

        try:
            flags = api.flags()
            if not bool(flags.get("run_enabled")):
                api.heartbeat(None)
                time.sleep(float(args.idle_sleep_s))
                continue

            task = api.claim_task()
            if not task:
                api.heartbeat(None)
                time.sleep(float(args.idle_sleep_s))
                continue

            api.heartbeat(int(task.get("task_id") or 0))
            run_task(api, task, thr, float(args.flag_poll_s), int(args.commit_every))

        except RuntimeError as e:
            raise SystemExit(str(e))
        except Exception:
            time.sleep(float(args.idle_sleep_s))


if __name__ == "__main__":
    main()
