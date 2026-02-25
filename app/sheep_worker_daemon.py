import os
import sys
import time
import traceback

import sheep_platform_db as db
from sheep_platform_jobs import JOB_MANAGER


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)) or default)
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)) or default)
    except Exception:
        return float(default)


def main() -> None:
    print("[worker] boot: init db...", flush=True)
    db.init_db()

    # 週期 rollover 不是每秒要跑，但 worker 起來先補一次，避免沒 cycle / 沒 pool
    try:
        db.ensure_cycle_rollover()
    except Exception as e:
        print(f"[worker] ensure_cycle_rollover failed: {e}\n{traceback.format_exc()}", file=sys.stderr, flush=True)

    interval_s = _env_float("SHEEP_ASSIGN_INTERVAL_S", 15.0)
    interval_s = max(3.0, min(120.0, interval_s))

    min_tasks = _env_int("SHEEP_MIN_TASKS", 2)
    max_tasks = _env_int("SHEEP_MAX_TASKS", 6)

    print(f"[worker] running. assign_interval={interval_s}s min_tasks={min_tasks} max_tasks={max_tasks}", flush=True)

    while True:
        try:
            users = db.list_users(limit=5000)
            for u in users:
                try:
                    uid = int(u.get("id") or 0)
                    if uid <= 0:
                        continue
                    if int(u.get("disabled") or 0) == 1:
                        continue
                    if int(u.get("run_enabled") or 0) != 1:
                        continue

                    db.assign_tasks_for_user(uid, min_tasks=int(min_tasks), max_tasks=int(max_tasks))
                except Exception as e:
                    print(f"[worker] assign_tasks_for_user failed: user_id={u.get('id')} err={e}", file=sys.stderr, flush=True)

            time.sleep(interval_s)
        except Exception as e:
            print(f"[worker] loop error: {e}\n{traceback.format_exc()}", file=sys.stderr, flush=True)
            time.sleep(2.0)


if __name__ == "__main__":
    main()