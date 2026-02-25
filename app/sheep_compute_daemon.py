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