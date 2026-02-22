import json
import re
import random
import threading
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import sheep_platform_db as db


def _json_load(s: str) -> Dict[str, Any]:
    try:
        return json.loads(s)
    except Exception:
        return {}


def _frange(a: float, b: float, step: float) -> List[float]:
    vals: List[float] = []
    x = float(a)
    b = float(b)
    step = float(step)
    if step <= 0:
        return vals
    while x <= b + 1e-12:
        vals.append(round(x, 8))
        x += step
    return vals


def _build_risk_grid(risk_spec: Dict[str, Any]) -> List[Tuple[float, float, int]]:
    tp_list = _frange(risk_spec.get("tp_min", 0.3), risk_spec.get("tp_max", 1.2), risk_spec.get("tp_step", 0.1))
    sl_list = _frange(risk_spec.get("sl_min", 0.3), risk_spec.get("sl_max", 1.2), risk_spec.get("sl_step", 0.1))
    mh_list = list(
        range(
            int(risk_spec.get("max_hold_min", 4)),
            int(risk_spec.get("max_hold_max", 80)) + 1,
            int(risk_spec.get("max_hold_step", 4)),
        )
    )
    return [(tp / 100.0, sl / 100.0, int(mh)) for tp in tp_list for sl in sl_list for mh in mh_list]


def _metrics_from_bt_result(res: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "total_return_pct": float(res.get("total_return_pct", 0.0)),
        "max_drawdown_pct": float(res.get("max_drawdown_pct", 0.0)),
        "sharpe": float(res.get("sharpe", 0.0)),
        "trades": int(res.get("trades", 0)),
        "win_rate_pct": float(res.get("win_rate_pct", 0.0)),
    }


def _passes_thresholds(metrics: Dict[str, Any], min_trades: int, min_ret: float, max_dd: float, min_sh: float) -> bool:
    return bool(
        int(metrics.get("trades", 0)) >= int(min_trades)
        and float(metrics.get("total_return_pct", 0.0)) >= float(min_ret)
        and float(metrics.get("max_drawdown_pct", 0.0)) <= float(max_dd)
        and float(metrics.get("sharpe", 0.0)) >= float(min_sh)
    )


def _score(metrics: Dict[str, Any]) -> float:
    trades = int(metrics.get("trades", 0))
    if trades <= 0:
        return -1e18
    ret = float(metrics.get("total_return_pct", 0.0))
    dd = float(metrics.get("max_drawdown_pct", 0.0))
    sh = float(metrics.get("sharpe", 0.0))
    return ret + 5.0 * sh - 0.6 * dd


_SPECIAL_FAMILIES = {"OB_FVG", "SMC", "LaguerreRSI_TEMA", "TEMA_RSI"}


class JobManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._threads: Dict[int, threading.Thread] = {}
        self._stop_flags: Dict[int, threading.Event] = {}

        try:
            conn = db._conn()
            conn.execute("UPDATE mining_tasks SET status = 'assigned' WHERE status = 'running'")
            conn.commit()
            conn.close()
            print("[SYSTEM] 系統啟動，已重置中止的任務狀態。")
        except Exception as e:
            print(f"[SYSTEM ERROR] 任務狀態重置失敗: {e}")

        self._queue_by_user: Dict[int, Deque[Tuple[int, Any]]] = {}
        self._queued_set_by_user: Dict[int, Set[int]] = {}
        self._rr_users: Deque[int] = deque()

        self._scheduler = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._scheduler.start()
        self._last_zombie_clean = time.time()

    def is_running(self, task_id: int) -> bool:
        with self._lock:
            th = self._threads.get(int(task_id))
            return bool(th and th.is_alive())

    def is_queued(self, user_id: int, task_id: int) -> bool:
        with self._lock:
            s = self._queued_set_by_user.get(int(user_id))
            return bool(s and int(task_id) in s)

    def queue_len(self, user_id: int) -> int:
        with self._lock:
            q = self._queue_by_user.get(int(user_id))
            return int(len(q) if q else 0)

    def enqueue_many(self, user_id: int, task_ids: List[int], bt_module) -> Dict[str, int]:
        user_id = int(user_id)
        added = 0
        skipped = 0
        with self._lock:
            q = self._queue_by_user.setdefault(user_id, deque())
            s = self._queued_set_by_user.setdefault(user_id, set())
            if user_id not in self._rr_users:
                self._rr_users.append(user_id)

            for tid in task_ids:
                tid = int(tid)
                if tid in s:
                    skipped += 1
                    continue
                th = self._threads.get(tid)
                if th is not None and th.is_alive():
                    skipped += 1
                    continue
                q.append((tid, bt_module))
                s.add(tid)
                added += 1
        return {"queued": int(added), "skipped": int(skipped)}

    def stop(self, task_id: int) -> None:
        with self._lock:
            f = self._stop_flags.get(int(task_id))
            if f is not None:
                f.set()

    def stop_all_for_user(self, user_id: int) -> None:
        user_id = int(user_id)
        running_ids: List[int] = []
        with self._lock:
            for tid, th in self._threads.items():
                if th.is_alive():
                    running_ids.append(int(tid))

        for tid in running_ids:
            t = db.get_task(int(tid))
            if t and int(t.get("user_id") or 0) == user_id:
                self.stop(int(tid))

        with self._lock:
            self._queue_by_user.pop(user_id, None)
            self._queued_set_by_user.pop(user_id, None)
            self._rr_users = deque([u for u in self._rr_users if int(u) != user_id])

    def start(self, task_id: int, bt_module) -> bool:
        task_id = int(task_id)
        conn = db._conn()
        try:
            limit = int(db.get_setting(conn, "max_concurrent_jobs", 2))
        finally:
            conn.close()

        with self._lock:
            self._cleanup_finished_locked()
            alive = sum(1 for t in self._threads.values() if t.is_alive())
            if alive >= max(1, limit):
                return False
            th = self._threads.get(task_id)
            if th is not None and th.is_alive():
                return False

            flag = threading.Event()
            th = threading.Thread(target=self._run_task, args=(task_id, bt_module, flag), daemon=True)
            self._threads[task_id] = th
            self._stop_flags[task_id] = flag
            th.start()
            return True

    def _cleanup_finished_locked(self) -> None:
        for k, t in list(self._threads.items()):
            if not t.is_alive():
                self._threads.pop(k, None)
                self._stop_flags.pop(k, None)

    def _pick_next_locked(self) -> Optional[Tuple[int, Any, int]]:
        while self._rr_users:
            uid = int(self._rr_users.popleft())
            q = self._queue_by_user.get(uid)
            if not q:
                self._queue_by_user.pop(uid, None)
                self._queued_set_by_user.pop(uid, None)
                continue

            task_id, bt_module = q.popleft()
            s = self._queued_set_by_user.get(uid)
            if s is not None:
                s.discard(int(task_id))

            if q:
                self._rr_users.append(uid)
            else:
                self._queue_by_user.pop(uid, None)
                self._queued_set_by_user.pop(uid, None)

            return int(task_id), bt_module, uid
        return None

    def _scheduler_loop(self) -> None:
        try:
            import psutil
            has_psutil = True
        except ImportError:
            has_psutil = False

        while True:
            try:
                if time.time() - getattr(self, "_last_zombie_clean", 0) > 300:
                    self._last_zombie_clean = time.time()
                    try:
                        cleared = db.clean_zombie_tasks(timeout_minutes=15)
                        if cleared > 0:
                            print(f"[SYSTEM] 偵測到斷線，已重置 {cleared} 個無回應任務。")
                    except Exception:
                        pass

                conn = db._conn()
                try:
                    limit = int(db.get_setting(conn, "max_concurrent_jobs", 2))
                finally:
                    conn.close()

                started_any = False
                
                mem_free_pct = 1.0
                if has_psutil:
                    mem_free_pct = psutil.virtual_memory().available / psutil.virtual_memory().total
                
                with self._lock:
                    self._cleanup_finished_locked()
                    alive = sum(1 for t in self._threads.values() if t.is_alive())

                    if mem_free_pct < 0.15:
                        if alive > 0:
                             print(f"[SYSTEM] 系統記憶體低於安全閾值 ({mem_free_pct:.1%})，暫緩新任務發放。")
                        pass
                    else:
                        while alive < max(1, limit):
                            item = self._pick_next_locked()
                            if item is None:
                                break
                            task_id, bt_module, _uid = item
    
                            th = self._threads.get(task_id)
                            if th is not None and th.is_alive():
                                continue
    
                            trow = db.get_task(int(task_id))
                            if not trow or str(trow.get("status")) not in ("assigned", "queued"):
                                continue
    
                            flag = threading.Event()
                            th = threading.Thread(target=self._run_task, args=(int(task_id), bt_module, flag), daemon=True)
                            self._threads[int(task_id)] = th
                            self._stop_flags[int(task_id)] = flag
                            
                            th.start()
                            alive += 1
                            started_any = True

                time.sleep(0.05 if started_any else 0.5)
            except Exception as e:
                import traceback
                import sys
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
                print(f"\n[{timestamp}] [SCHEDULER ERROR] 迴圈異常: {e}\n{traceback.format_exc()}\n", file=sys.stderr)
                time.sleep(1.0)

    def _run_task(self, task_id: int, bt_module, stop_flag: threading.Event) -> None:
        task_id = int(task_id)
        task = db.get_task(task_id)
        if not task:
            return

        try:
            if not db.claim_task_for_run(task_id):
                return

            user_id = int(task["user_id"])
            pool_id = int(task["pool_id"])
            partition_idx = int(task["partition_idx"])
            num_partitions = int(task["num_partitions"])

            pool = db.get_pool(pool_id)
            if not pool or int(pool.get("active") or 0) != 1:
                db.update_task_status(task_id, "revoked", finished=True)
                return

            family = str(pool["family"])
            grid_spec = dict(pool.get("grid_spec") or {})
            risk_spec = dict(pool.get("risk_spec") or {})

            progress = _json_load(task.get("progress_json") or "{}")
            progress.update(
                {
                    "combos_total": 0,
                    "combos_done": 0,
                    "best_score": None,
                    "best_candidate_id": None,
                    "best_any_score": None,
                    "best_any_metrics": None,
                    "best_any_params": None,
                    "best_any_passed": False,
                    "phase": "sync_data",
                    "phase_progress": 0.0,
                    "phase_msg": "",
                    "last_error": None,
                    "elapsed_s": 0.0,
                    "speed_cps": 0.0,
                    "eta_s": None,
                    "updated_at": db.utc_now_iso(),
                }
            )
            db.update_task_progress(task_id, progress)

            _SYNC_RE = re.compile(r"^\s*(\S+)\s+已寫入\s+(\d+)\s*/\s*(\d+)\s*$")
            t0 = time.time()

            def _progress_cb(frac: float, msg: str) -> None:
                if stop_flag.is_set():
                    return
                progress["phase"] = "sync_data"
                progress["phase_progress"] = float(frac)
                progress["phase_msg"] = str(msg)
                progress["elapsed_s"] = round(float(max(0.0, time.time() - t0)), 3)

                m = _SYNC_RE.match(str(msg))
                if m:
                    label = str(m.group(1))
                    done_i = int(m.group(2))
                    total_i = int(m.group(3))

                    sync = progress.get("sync")
                    if not isinstance(sync, dict):
                        sync = {"items": {}, "current": ""}
                    items = sync.get("items")
                    if not isinstance(items, dict):
                        items = {}
                    items[label] = {"done": int(done_i), "total": int(total_i)}
                    sync["items"] = items
                    sync["current"] = label

                    od = 0
                    ot = 0
                    for v in items.values():
                        try:
                            od += int(v.get("done") or 0)
                            ot += int(v.get("total") or 0)
                        except Exception:
                            pass
                    sync["overall_done"] = int(od)
                    sync["overall_total"] = int(ot)
                    progress["sync"] = sync

                progress["updated_at"] = db.utc_now_iso()
                try:
                    db.update_task_progress(task_id, progress)
                except Exception as db_err:
                    print(f"[WARN] K線同步進度寫入 DB 失敗 (可忽略): {db_err}")

            progress["phase"] = "sync_data"
            progress["phase_msg"] = f"準備向交易所拉取 {pool['symbol']} ({pool['timeframe_min']}m) 歷史 K 線資料..."
            try:
                db.update_task_progress(task_id, progress)
            except Exception as e:
                print(f"[WARN] 初始化 K 線狀態寫入 DB 失敗: {e}")

            csv_main, _ = bt_module.ensure_bitmart_data(
                symbol=str(pool["symbol"]),
                main_step_min=int(pool["timeframe_min"]),
                years=int(pool.get("years") or 3),
                auto_sync=True,
                force_full=False,
                progress_cb=_progress_cb,
            )
            
            progress["phase_msg"] = "K 線資料拉取完成，正在驗證與載入記憶體..."
            db.update_task_progress(task_id, progress)
            df = bt_module.load_and_validate_csv(csv_main)

            if stop_flag.is_set():
                db.update_task_status(task_id, "assigned")
                return

            progress["phase"] = "build_grid"
            progress["phase_msg"] = "正在展開格點參數組合..."
            db.update_task_progress(task_id, progress)

            combos = bt_module.grid_combinations_from_ui(family, grid_spec)
            
            if not combos:
                raise ValueError(f"格點參數展開失敗或為空，請檢查策略池 ({pool['name']}) 的參數設定範圍。")
            
            safe_cycle_id = int(task.get("cycle_id") if task.get("cycle_id") is not None else 0)
            safe_pool_seed = int(pool.get("seed") if pool.get("seed") is not None else 0)
            safe_partition_idx = int(partition_idx if partition_idx is not None else 0)
            safe_num_parts = int(num_partitions if num_partitions is not None else 1)
            
            final_seed = safe_pool_seed ^ (safe_cycle_id & 0x7FFFFFFF)
            rng = random.Random(final_seed)
            rng.shuffle(combos)
            
            # 嚴格分片，防止 partition_idx 超出 num_partitions
            if safe_num_parts <= 0: safe_num_parts = 1
            part = combos[safe_partition_idx % safe_num_parts :: safe_num_parts]

            risk_grid = _build_risk_grid(risk_spec)
            if family in ("TEMA_RSI", "LaguerreRSI_TEMA"):
                mh_min = int(risk_spec.get("max_hold_min", 4))
                mh_max = int(risk_spec.get("max_hold_max", 80))
                mh_step = max(1, int(risk_spec.get("max_hold_step", 4)))
                mh_list = list(range(mh_min, mh_max + 1, mh_step))
                risk_grid = [(0.0, 0.0, int(mh)) for mh in mh_list]

            combos_total = int(len(part) * max(1, len(risk_grid)))

            sconn = db._conn()
            try:
                min_trades = int(db.get_setting(sconn, "min_trades", 40))
                min_ret = float(db.get_setting(sconn, "min_total_return_pct", 15.0))
                max_dd = float(db.get_setting(sconn, "max_drawdown_pct", 25.0))
                min_sh = float(db.get_setting(sconn, "min_sharpe", 0.6))
                keep_top = int(db.get_setting(sconn, "candidate_keep_top_n", 30))
            finally:
                sconn.close()

            db.clear_candidates_for_task(task_id)

            best_pass: List[Tuple[float, Dict[str, Any], Dict[str, Any]]] = []
            best_any_score: Optional[float] = None
            best_any_metrics: Optional[Dict[str, Any]] = None
            best_any_params: Optional[Dict[str, Any]] = None
            best_any_passed: bool = False

            done = 0
            last_commit = 0
            last_commit_ts = time.time()
            # t0 = time.time() # [專家級修復] 已移至 _progress_cb 上方以計算總體耗時

            progress["phase"] = "grid_search"
            progress["combos_total"] = int(combos_total)
            progress["combos_done"] = 0
            progress["updated_at"] = db.utc_now_iso()
            db.update_task_progress(task_id, progress)

            fee_side = float(risk_spec.get("fee_side", 0.0002))
            slippage = float(risk_spec.get("slippage", 0.0))
            worst_case = bool(risk_spec.get("worst_case", True))
            reverse_mode = bool(risk_spec.get("reverse_mode", False))

            use_fast_path = bool(
                family not in _SPECIAL_FAMILIES
                and hasattr(bt_module, "build_cache_for_family")
                and hasattr(bt_module, "run_backtest_from_entry_sig")
            )

            sig_cache = None
            if use_fast_path and part:
                try:
                    def _cache_logger(msg: str):
                        progress["phase"] = "build_grid"
                        progress["phase_msg"] = f"指標計算中: {msg}"
                        progress["updated_at"] = db.utc_now_iso()
                        db.update_task_progress(task_id, progress)
                    
                    progress["phase"] = "build_grid"
                    progress["phase_msg"] = f" 準備計算 {family} 參數快取 ({len(part)} 組)..."
                    db.update_task_progress(task_id, progress)
                    
                    sig_cache = bt_module.build_cache_for_family(df, family, part, logger=_cache_logger)
                except Exception as cache_err:
                    import traceback
                    err_str = f"快取計算失敗: {str(cache_err)}\n{traceback.format_exc()}"
                    print(f"[CACHE ERROR] {err_str}")
                    progress["last_error"] = err_str
                    db.update_task_progress(task_id, progress)
                    sig_cache = None
                    use_fast_path = False

            def _commit() -> None:
                nonlocal last_commit, last_commit_ts
                last_commit = done
                last_commit_ts = time.time()
                elapsed = float(max(0.0, time.time() - t0))
                speed = float(done / elapsed) if elapsed > 0 else 0.0
                eta = float((combos_total - done) / speed) if speed > 0 and combos_total > done else None
                progress["combos_done"] = int(done)
                progress["elapsed_s"] = round(elapsed, 3)
                progress["speed_cps"] = round(speed, 6)
                progress["eta_s"] = round(eta, 3) if eta is not None else None
                progress["best_any_score"] = float(best_any_score) if best_any_score is not None else None
                progress["best_any_metrics"] = dict(best_any_metrics) if best_any_metrics is not None else None
                progress["best_any_params"] = dict(best_any_params) if best_any_params is not None else None
                progress["best_any_passed"] = bool(best_any_passed)
                progress["best_score"] = float(best_pass[0][0]) if best_pass else None
                progress["updated_at"] = db.utc_now_iso()
                db.update_task_progress(task_id, progress)

            if combos_total > 0:
                if use_fast_path and sig_cache is not None:
                    for family_params, entry_sig in zip(part, sig_cache):
                        if stop_flag.is_set():
                            db.update_task_status(task_id, "assigned")
                            return
                        for tp, sl, mh in risk_grid:
                            if stop_flag.is_set():
                                db.update_task_status(task_id, "assigned")
                                return

                            res = bt_module.run_backtest_from_entry_sig(
                                df, entry_sig, tp, sl, mh,
                                fee_side=fee_side, slippage=slippage,
                                worst_case=worst_case, reverse_mode=reverse_mode,
                            )
                            metrics = _metrics_from_bt_result(res)
                            sc = _score(metrics)
                            passed = _passes_thresholds(metrics, min_trades, min_ret, max_dd, min_sh)

                            if best_any_score is None or float(sc) > float(best_any_score):
                                best_any_score = float(sc)
                                best_any_metrics = dict(metrics)
                                best_any_params = {"family": family, "family_params": family_params, "tp": float(tp), "sl": float(sl), "max_hold": int(mh)}
                                best_any_passed = bool(passed)

                            if passed:
                                full_params = {"family": family, "family_params": family_params, "tp": float(tp), "sl": float(sl), "max_hold": int(mh)}
                                best_pass.append((float(sc), full_params, metrics))
                                best_pass.sort(key=lambda x: x[0], reverse=True)
                                if len(best_pass) > keep_top:
                                    best_pass = best_pass[:keep_top]

                            done += 1
                            if done - last_commit >= 50 or (time.time() - last_commit_ts) >= 1.0:
                                _commit()
                else:
                    for family_params in part:
                        if stop_flag.is_set():
                            db.update_task_status(task_id, "assigned")
                            return
                        for tp, sl, mh in risk_grid:
                            if stop_flag.is_set():
                                db.update_task_status(task_id, "assigned")
                                return

                            res = bt_module.run_backtest(
                                df, family, family_params, tp, sl, mh,
                                fee_side=fee_side, slippage=slippage,
                                worst_case=worst_case, reverse_mode=reverse_mode,
                            )
                            metrics = _metrics_from_bt_result(res)
                            sc = _score(metrics)
                            passed = _passes_thresholds(metrics, min_trades, min_ret, max_dd, min_sh)

                            if best_any_score is None or float(sc) > float(best_any_score):
                                best_any_score = float(sc)
                                best_any_metrics = dict(metrics)
                                best_any_params = {"family": family, "family_params": family_params, "tp": float(tp), "sl": float(sl), "max_hold": int(mh)}
                                best_any_passed = bool(passed)

                            if passed:
                                full_params = {"family": family, "family_params": family_params, "tp": float(tp), "sl": float(sl), "max_hold": int(mh)}
                                best_pass.append((float(sc), full_params, metrics))
                                best_pass.sort(key=lambda x: x[0], reverse=True)
                                if len(best_pass) > keep_top:
                                    best_pass = best_pass[:keep_top]

                            done += 1
                            if done - last_commit >= 50 or (time.time() - last_commit_ts) >= 1.0:
                                _commit()

                _commit()

            best_candidate_id = None
            
            disk_data = {
                "task_info": task,
                "timestamp": db.utc_now_iso(),
                "best_pass_count": len(best_pass),
                "candidates": []
            }

            for sc, full_params, metrics in best_pass:
                cid = db.insert_candidate(task_id, user_id, pool_id, full_params, metrics, float(sc))
                if best_candidate_id is None:
                    best_candidate_id = int(cid)
                disk_data["candidates"].append({"id": cid, "score": sc, "params": full_params, "metrics": metrics})

            db.save_candidate_to_disk(task_id, user_id, pool_id, disk_data)

            progress["best_candidate_id"] = int(best_candidate_id) if best_candidate_id is not None else None
            progress["storage_status"] = "DISK_BACKUP_OK"
            progress["updated_at"] = db.utc_now_iso()
            db.update_task_progress(task_id, progress)
            db.update_task_status(task_id, "completed", finished=True)
            
            import gc
            gc.collect()

        except Exception as e:
            import traceback, sys
            err_trace = traceback.format_exc()
            
            # 在控制台輸出錯誤以便即時監控
            print(f"\n{'='*60}\n[TASK EXCEPTION] Task ID: {task_id}\n{err_trace}\n{'='*60}", file=sys.stderr)
            
            try:
                # 重新讀取任務以確保 progress_json 是最新的
                current_t = db.get_task(task_id)
                if current_t:
                    prog = _json_load(current_t.get("progress_json") or "{}")
                    prog["phase"] = "error"
                    # 將錯誤訊息與完整堆疊寫入 JSON
                    prog["last_error"] = f"Runtime Error: {str(e)}"
                    prog["debug_traceback"] = err_trace
                    prog["error_ts"] = db.utc_now_iso()
                    
                    # 寫入資料庫：更新進度與狀態
                    db.update_task_progress(task_id, prog)
                    db.update_task_status(task_id, "error")
                    
                    # 寫入審計日誌
                    db.write_audit_log(
                        user_id=int(current_t.get("user_id") or 0),
                        action="task_execution_failed",
                        payload={"task_id": task_id, "exception": str(e), "trace": err_trace[:4000]}
                    )
                    
                    db.write_audit_log(
                        user_id=int(current_t.get("user_id") or 0),
                        action="task_execution_failed",
                        payload={"task_id": task_id, "exception": str(e), "trace": err_trace[:2000]}
                    )
                else:
                    db.update_task_status(task_id, "error")
            except Exception as nested_err:
                print(f"[CRITICAL] Error Handler Failed: {nested_err}\n{traceback.format_exc()}", file=sys.stderr)
            finally:
                with self._lock:
                    self._threads.pop(task_id, None)
                    self._stop_flags.pop(task_id, None)

JOB_MANAGER = JobManager()
