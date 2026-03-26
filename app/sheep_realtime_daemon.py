from __future__ import annotations

import argparse
import json
import sys

from sheep_realtime.config import sanitized_effective_config
from sheep_realtime.service import RealtimeService, run_healthcheck


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sheep realtime daemon")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="run realtime daemon")
    run_parser.add_argument("--mode", choices=("shadow", "live"), default="shadow")

    health_parser = subparsers.add_parser("healthcheck", help="check realtime daemon heartbeat")
    health_parser.add_argument("--max-age-s", type=int, default=45)

    subparsers.add_parser("print-effective-config", help="print sanitized effective config")

    args = parser.parse_args(argv)
    if args.command == "print-effective-config":
        print(json.dumps(sanitized_effective_config(), ensure_ascii=False, indent=2))
        return 0
    if args.command == "healthcheck":
        return run_healthcheck(max_age_s=int(args.max_age_s))
    service = RealtimeService(initial_mode=str(args.mode))
    try:
        service.run_forever()
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
