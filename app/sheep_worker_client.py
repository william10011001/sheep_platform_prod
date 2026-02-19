import argparse
import json
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

import backtest_panel2 as bt


WORKER_VERSION = "2.0.0"
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
        return {
            "Authorization": f"Bearer {self.token}",
            "X-Worker-Id": self.worker_id,
            "X-Worker-Version": WORKER_VERSION,
            "X-Worker-Protocol": str(WORKER_PROTOCOL),
        }

    def _request(self, method: str, path: str, *, json_body: Optional[Dict[str, Any]] = None, timeout_s: float = 30.0):
        url = self.base_url + path
        for attempt in range(6):
            r = self._session.request(method, url, headers=self._headers(), json=json_body, timeout=timeout_s)
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
        raise RuntimeError("api_unavailable")

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
        r = self._session.post(url, headers=headers, timeout=10)
        if r.status_code in (429, 503, 502):
            return
        if r.status_code >= 400:
            # don't crash on heartbeat
            return

    def claim_task(self) -> Optional[Dict[str, Any]]:
        return self._request("POST", "/tasks/claim", timeout_s=20)

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


def run_task(api: ApiClient, task: Dict[str, Any], thr: Thresholds, flag_poll_s: float, commit_every: int) -> None:
    task_id = int(task["task_id"])
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

    def _progress_cb(frac: float, msg: str) -> None:
        progress["phase"] = "sync_data"
        progress["phase_progress"] = float(frac)
        progress["phase_msg"] = str(msg)
        try:
            api.progress(task_id, lease_id, progress)
        except Exception:
            pass

    progress["phase"] = "sync_data"
    progress["phase_progress"] = 0.0
    progress["phase_msg"] = ""
    api.progress(task_id, lease_id, progress)

    try:
        csv_main, _ = bt.ensure_bitmart_data(
            symbol=str(task["symbol"]),
            main_step_min=int(task["timeframe_min"]),
            years=int(task.get("years") or 3),
            auto_sync=True,
            force_full=False,
            progress_cb=_progress_cb,
        )
        df = bt.load_and_validate_csv(csv_main)

        if _should_stop():
            progress["phase"] = "stopped"
            api.release(task_id, lease_id, progress)
            return

        progress["phase"] = "build_grid"
        progress["phase_progress"] = 1.0
        progress["phase_msg"] = ""
        api.progress(task_id, lease_id, progress)

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
            if not force and (done - last_commit) < commit_every and (time.time() - last_commit_ts) < 10.0:
                return
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
            progress["checkpoint_candidates"] = [{"score": float(s), "params": dict(p), "metrics": dict(m)} for s, p, m in best_pass[:keep_top]]
            api.progress(task_id, lease_id, progress)

        if combos_total <= 0:
            _commit(force=True)
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

        if use_fast_path and sig_cache is not None:
            for i in range(start_i, len(part)):
                if _should_stop():
                    progress["phase"] = "stopped"
                    _commit(force=True)
                    api.release(task_id, lease_id, progress)
                    return

                family_params = part[i]
                entry_sig = sig_cache[i]
                j0 = start_j if i == start_i else 0

                for j in range(j0, len(risk_grid)):
                    if _should_stop():
                        progress["phase"] = "stopped"
                        _commit(force=True)
                        api.release(task_id, lease_id, progress)
                        return

                    tp, sl, mh = risk_grid[j]
                    res = bt.run_backtest_from_entry_sig(
                        df,
                        entry_sig,
                        tp,
                        sl,
                        mh,
                        fee_side=fee_side,
                        slippage=slippage,
                        worst_case=worst_case,
                        reverse_mode=reverse_mode,
                    )

                    metrics = dict(res.get("metrics") or {})
                    score = float(metrics.get("total_return_pct") or -1e9)
                    params = dict(family_params)
                    params.update({"tp": float(tp), "sl": float(sl), "max_hold": int(mh)})

                    passed = _passes_thresholds(metrics, thr)
                    _consider_candidate(score, params, metrics, passed)

                    done += 1
                    _commit()

        else:
            for i in range(start_i, len(part)):
                if _should_stop():
                    progress["phase"] = "stopped"
                    _commit(force=True)
                    api.release(task_id, lease_id, progress)
                    return

                family_params = part[i]
                j0 = start_j if i == start_i else 0

                for j in range(j0, len(risk_grid)):
                    if _should_stop():
                        progress["phase"] = "stopped"
                        _commit(force=True)
                        api.release(task_id, lease_id, progress)
                        return

                    tp, sl, mh = risk_grid[j]
                    res = bt.run_one(
                        df,
                        family=family,
                        family_params=family_params,
                        tp=float(tp),
                        sl=float(sl),
                        max_hold=int(mh),
                        fee_side=fee_side,
                        slippage=slippage,
                        worst_case=worst_case,
                        reverse_mode=reverse_mode,
                    )

                    metrics = dict(res.get("metrics") or {})
                    score = float(metrics.get("total_return_pct") or -1e9)
                    params = dict(family_params)
                    params.update({"tp": float(tp), "sl": float(sl), "max_hold": int(mh)})

                    passed = _passes_thresholds(metrics, thr)
                    _consider_candidate(score, params, metrics, passed)

                    done += 1
                    _commit()

        _commit(force=True)
        cands = [{"score": float(s), "params": dict(p), "metrics": dict(m)} for s, p, m in best_pass[:keep_top]]
        api.finish(task_id, lease_id, cands, progress)
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
