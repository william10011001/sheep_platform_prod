import json
import os
import sys
import time
import uuid
import traceback
from typing import Any, Dict, Optional

import requests

import sheep_worker_client as wc


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)) or default)
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)) or default)
    except Exception:
        return int(default)


def _env_str(name: str, default: str = "") -> str:
    return str(os.environ.get(name, default) or default).strip()


def _get_worker_id(path: str = "data/.sheep_compute_worker_id") -> str:
    p = path
    try:
        os.makedirs(os.path.dirname(p), exist_ok=True)
    except Exception:
        pass
    try:
        if os.path.exists(p):
            s = open(p, "r", encoding="utf-8").read().strip()
            if s:
                return s
    except Exception:
        pass
    wid = uuid.uuid4().hex
    try:
        with open(p, "w", encoding="utf-8") as f:
            f.write(wid)
    except Exception:
        pass
    return wid


def _issue_compute_token(base_url: str, username: str, password: str, ttl_seconds: int) -> str:
    url = base_url.rstrip("/") + "/token"
    payload = {"username": username, "password": password, "ttl_seconds": int(ttl_seconds), "name": "compute"}
    r = requests.post(url, json=payload, timeout=15)
    if r.status_code >= 400:
        raise RuntimeError(f"issue_token_failed {r.status_code}: {r.text}")
    j = r.json()
    tok = str(j.get("token") or "").strip()
    if not tok:
        raise RuntimeError("empty_compute_token")
    return tok


def main() -> None:
    base_url = _env_str("SHEEP_COMPUTE_API_URL", "http://api:8000")
    user = _env_str("SHEEP_COMPUTE_USER", "sheep")
    pwd = _env_str("SHEEP_COMPUTE_PASS", "")
    ttl_seconds = _env_int("SHEEP_COMPUTE_TTL_SECONDS", 2592000)
    idle_s = _env_float("SHEEP_COMPUTE_IDLE_S", 0.20)
    commit_every = _env_int("SHEEP_COMPUTE_COMMIT_EVERY", 50)
    flag_poll_s = _env_float("SHEEP_COMPUTE_FLAG_POLL_S", 1.0)

    if not pwd:
        raise RuntimeError("SHEEP_COMPUTE_PASS is empty")

    worker_id = _get_worker_id()
    print(f"[compute] boot worker_id={worker_id} base_url={base_url}", flush=True)

    token = ""
    last_issue_ts = 0.0
    next_reissue_s = 24 * 3600  # daily refresh (simple + safe)

    api: Optional[wc.ApiClient] = None
    thr = wc.Thresholds.from_dict({})

    while True:
        try:
            now = time.time()

            if (not token) or ((now - last_issue_ts) >= float(next_reissue_s)):
                token = _issue_compute_token(base_url, user, pwd, ttl_seconds=ttl_seconds)
                last_issue_ts = now
                api = wc.ApiClient(base_url=base_url, token=token, worker_id=worker_id)
                # warm thresholds once
                try:
                    thr = wc.Thresholds.from_dict((api.get_thresholds() or {}))
                except Exception:
                    thr = wc.Thresholds.from_dict({})
                print("[compute] token refreshed", flush=True)

            assert api is not None

            # ── [極致系統升級] 優先檢查是否有排隊中的 OOS (過擬合) 驗證任務 ──
            try:
                # 動態構建安全標頭，跨過 client 封裝直接打真實 API
                headers = {
                    "Authorization": f"Bearer {token}", 
                    "X-Worker-Id": worker_id, 
                    "X-Worker-Version": "2.0.0", 
                    "X-Worker-Protocol": "2"
                }
                oos_res = requests.post(f"{base_url}/tasks/oos/claim", headers=headers, timeout=10)
                
                if oos_res.status_code == 200:
                    oos_task = oos_res.json().get("task")
                    if oos_task:
                        print(f"[*] 接收到 OOS 真實驗證任務 (Task #{oos_task.get('id')})，啟動本地全域回測...", flush=True)
                        try:
                            cand = dict(oos_task.get("candidate") or {})
                            params = dict(cand.get("params_json") or {})
                            family = str(oos_task.get("family", "KAMA_Cross"))
                            symbol = str(oos_task.get("symbol", "BTC_USDT"))
                            tf = int(oos_task.get("timeframe_min", 15))
                            years = int(oos_task.get("years", 3))
                            
                            import backtest_panel2 as bt
                            # 1. 真實讀取本地/下載歷史 K 線，VM 伺服器維持 0% 負擔！
                            csv_main, _ = bt.ensure_bitmart_data(symbol, tf, years, auto_sync=True, skip_1m=True)
                            df = bt.load_and_validate_csv(csv_main)
                            
                            # 2. 啟動用戶端 CPU 進行核心回測引擎重組與演算
                            family_params = dict(params.get("family_params") or {})
                            if not family_params:
                                family_params = {k: v for k, v in params.items() if k not in ["family", "tp", "sl", "max_hold"]}
                                
                            res = bt.run_backtest(
                                df, params.get("family", family), family_params, 
                                float(params.get("tp", 1.0)), float(params.get("sl", 1.0)), int(params.get("max_hold", 100))
                            )
                            
                            metrics = {
                                "total_return_pct": float(res.get("total_return_pct", 0.0)),
                                "max_drawdown_pct": float(res.get("max_drawdown_pct", 0.0)),
                                "sharpe": float(res.get("sharpe", 0.0)),
                                "trades": int(res.get("trades", 0))
                            }
                            
                            # 3. 嚴苛標準防禦：Sharpe 必須仍維持在 0.3 以上，且具備真實交易次數
                            passed = bool(metrics["sharpe"] > 0.3 and metrics["total_return_pct"] > 0 and metrics["trades"] > 5)
                            print(f"[*]驗證結束: {'通過 ' if passed else '失敗'} (Sharpe: {metrics['sharpe']:.4f})", flush=True)
                            
                            # 4. 將真實算出的數據回傳伺服器審核
                            requests.post(f"{base_url}/tasks/oos/{oos_task['id']}/finish", json={"passed": passed, "metrics": metrics}, headers=headers, timeout=10)
                        except Exception as e:
                            print(f"[!] 驗證發生異常: {e}", flush=True)
                            requests.post(f"{base_url}/tasks/oos/{oos_task['id']}/finish", json={"passed": False, "metrics": {"error": str(e)}}, headers=headers, timeout=10)
                        
                        time.sleep(2)
                        continue # OOS 執行完畢，提早進入下一輪迴圈，避免跟一般運算任務卡撞
            except Exception as e:
                print(f"OOS Polling Error: {e}", flush=True)
            # ────────────────────────────────────────────────────────

            # claim next task (compute token -> server will dispatch across all users)
            task = api.claim_task()
            if not task:
                time.sleep(max(0.05, idle_s))
                continue

            # run compute-heavy task (grid search)
            try:
                wc.run_task(api, dict(task), thr, flag_poll_s=float(flag_poll_s), commit_every=int(commit_every))
            except Exception as run_err:
                # best-effort release with error info (avoid stuck running)
                try:
                    task_id = int(task.get("task_id") or 0)
                    lease_id = str(task.get("lease_id") or "")
                    prog = dict(task.get("progress") or {})
                    prog["phase"] = "error"
                    prog["last_error"] = f"compute_worker_exception: {str(run_err)}"
                    prog["debug_traceback"] = traceback.format_exc()
                    prog["updated_at"] = wc.time.strftime("%Y-%m-%dT%H:%M:%S", wc.time.gmtime())
                    if task_id and lease_id:
                        api.release(task_id, lease_id, prog)
                except Exception:
                    pass

        except Exception as e:
            print(f"[compute] loop_error: {e}", file=sys.stderr, flush=True)
            try:
                print(traceback.format_exc(), file=sys.stderr, flush=True)
            except Exception:
                pass
            time.sleep(1.0)


if __name__ == "__main__":
    main()