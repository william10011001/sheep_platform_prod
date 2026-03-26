from __future__ import annotations

import argparse
import sys

import sheep_platform_db as db
from sheep_review import rebuild_review_state


def _provision_compute_admin() -> None:
    import os

    user = str(os.environ.get("SHEEP_COMPUTE_USER", "") or "").strip()
    password = str(os.environ.get("SHEEP_COMPUTE_PASS", "") or "").strip()
    if not user or not password:
        return
    from sheep_platform_security import hash_password, normalize_username

    user_norm = normalize_username(user)
    existing = db.get_user_by_username(user)
    pw_hashed = hash_password(password)
    pw_text = pw_hashed.decode("utf-8") if isinstance(pw_hashed, bytes) else str(pw_hashed)
    if not existing:
        db.create_user(user, pw_text, role="admin")
        return
    conn = db._conn()
    try:
        conn.execute(
            "UPDATE users SET role = 'admin', run_enabled = 1, disabled = 0 WHERE username_norm = ?",
            (user_norm,),
        )
        conn.commit()
    finally:
        conn.close()


def _rebuild_review_state() -> None:
    rebuild_review_state(db_module=db)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sheep bootstrap and maintenance CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init-db")
    subparsers.add_parser("provision-compute-admin")
    subparsers.add_parser("rebuild-review-state")
    subparsers.add_parser("init-and-bootstrap")
    args = parser.parse_args(argv)

    if args.command in {"init-db", "init-and-bootstrap"}:
        db.init_db()
    if args.command in {"provision-compute-admin", "init-and-bootstrap"}:
        _provision_compute_admin()
    if args.command in {"rebuild-review-state"}:
        _rebuild_review_state()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
