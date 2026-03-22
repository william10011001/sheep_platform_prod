import argparse
import json
import sys
import urllib.error
import urllib.request
from typing import Callable, Dict, List, Optional


EXPECTED_ENDPOINTS = [
    "/sheep123/healthz",
    "/sheep123/api/healthz",
    "/api/healthz",
    "/app/_stcore/health",
]


def _normalize_content_type(value: str) -> str:
    return str(value or "").split(";", 1)[0].strip().lower()


def check_endpoint(
    base_url: str,
    path: str,
    *,
    timeout: float = 10.0,
    opener: Optional[Callable[..., object]] = None,
) -> Dict[str, object]:
    base = str(base_url or "").rstrip("/")
    normalized_path = "/" + str(path or "").lstrip("/")
    url = f"{base}{normalized_path}"
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    open_fn = opener or urllib.request.urlopen

    try:
        response = open_fn(request, timeout=timeout)
        status = int(getattr(response, "status", response.getcode()))
        content_type = _normalize_content_type(response.headers.get("Content-Type", ""))
        body = response.read().decode("utf-8", errors="replace")
        body_json = json.loads(body)
        ok = status == 200 and content_type == "application/json" and isinstance(body_json, dict)
        return {
            "path": normalized_path,
            "url": url,
            "ok": ok,
            "status": status,
            "content_type": content_type,
            "body": body_json,
        }
    except urllib.error.HTTPError as exc:
        content_type = _normalize_content_type(exc.headers.get("Content-Type", ""))
        body = exc.read().decode("utf-8", errors="replace")
        return {
            "path": normalized_path,
            "url": url,
            "ok": False,
            "status": int(exc.code),
            "content_type": content_type,
            "body": body,
        }
    except Exception as exc:
        return {
            "path": normalized_path,
            "url": url,
            "ok": False,
            "status": None,
            "content_type": "",
            "body": str(exc),
        }


def run_checks(base_url: str) -> List[Dict[str, object]]:
    return [check_endpoint(base_url, path) for path in EXPECTED_ENDPOINTS]


def main() -> int:
    parser = argparse.ArgumentParser(description="Check public health endpoints and ensure they return JSON, not SPA HTML.")
    parser.add_argument("--base-url", default="https://sheep123.com", help="Public base URL to validate.")
    args = parser.parse_args()

    results = run_checks(args.base_url)
    failed = [item for item in results if not bool(item.get("ok"))]

    for item in results:
        print(json.dumps(item, ensure_ascii=False))

    if failed:
        print(f"\nHealth endpoint validation failed for {len(failed)} endpoint(s).", file=sys.stderr)
        return 1

    print("\nAll public health endpoints returned HTTP 200 with JSON content.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
