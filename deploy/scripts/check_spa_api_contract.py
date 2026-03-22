import re
import sys
from pathlib import Path


def _normalize_route(route: str) -> str:
    path = str(route or "").strip()
    if not path:
        return ""
    path = path.split("?", 1)[0]
    path = re.sub(r"\$\{[^}]+\}", "{param}", path)
    path = re.sub(r"\{[^}]+\}", "{param}", path)
    return path


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]
    spa_path = repo_root / "deploy" / "nginx" / "html" / "index.html"
    api_path = repo_root / "app" / "sheep_platform_api.py"

    spa_text = spa_path.read_text(encoding="utf-8", errors="replace")
    api_text = api_path.read_text(encoding="utf-8", errors="replace")

    spa_literals = re.findall(r"fetchApi\(\s*('([^']+)'|`([^`]+)`)", spa_text)
    spa_routes = sorted(
        {
            _normalize_route((single or template))
            for _, single, template in spa_literals
            if _normalize_route(single or template)
        }
    )
    api_routes = sorted(
        {
            _normalize_route(route)
            for route in re.findall(r'@app\.(?:get|post|put|delete|api_route)\("([^"]+)"', api_text)
            if _normalize_route(route)
        }
    )

    missing = [route for route in spa_routes if route not in api_routes]

    print("SPA fetchApi routes:")
    for route in spa_routes:
        print(f"  {route}")

    print("\nExplicit FastAPI routes:")
    for route in api_routes:
        print(f"  {route}")

    if missing:
        print("\nMissing routes:")
        for route in missing:
            print(f"  {route}")
        return 1

    print("\nContract check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
