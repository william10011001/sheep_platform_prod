import os
import sys
import time
import traceback

import sheep_platform_db as db


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
    interval_s = max(5.0, min(180.0, interval_s))

    min_tasks = _env_int("SHEEP_MIN_TASKS", 2)
    max_tasks = _env_int("SHEEP_MAX_TASKS", 6)
    runnable_user_limit = _env_int("SHEEP_RUNNABLE_USER_LIMIT", 1000)

    print(
        f"[worker] running. assign_interval={interval_s}s min_tasks={min_tasks} "
        f"max_tasks={max_tasks} runnable_user_limit={runnable_user_limit}",
        flush=True,
    )

    while True:
        try:
            users = db.list_runnable_users(limit=runnable_user_limit)
            if not users:
                time.sleep(max(interval_s, 15.0))
                continue
            for u in users:
                try:
                    uid = int(u.get("id") or 0)
                    if uid <= 0:
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
