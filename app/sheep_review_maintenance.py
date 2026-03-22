import argparse
import json

import sheep_platform_db as db
from sheep_review import rebuild_review_state


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild legacy review/oos state for completed tasks.")
    parser.add_argument("--limit", type=int, default=0, help="Optional max number of completed tasks to scan.")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be updated without writing changes.")
    parser.add_argument("--task-id", dest="task_ids", action="append", type=int, default=[], help="Restrict maintenance to one or more task ids.")
    args = parser.parse_args()

    if hasattr(db, "init_db"):
        db.init_db()

    summary = rebuild_review_state(
        db_module=db,
        task_ids=args.task_ids or None,
        limit=int(args.limit or 0),
        dry_run=bool(args.dry_run),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
