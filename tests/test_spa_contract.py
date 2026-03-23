import importlib.util
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEPLOY_SCRIPTS = ROOT / "deploy" / "scripts"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_spa_api_contract_script_passes():
    script = DEPLOY_SCRIPTS / "check_spa_api_contract.py"
    result = subprocess.run([sys.executable, str(script)], cwd=ROOT, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stdout + result.stderr


def test_spa_index_contains_mobile_rating_and_leaderboard_contracts():
    html = (ROOT / "deploy" / "nginx" / "html" / "index.html").read_text(encoding="utf-8", errors="replace")

    required_snippets = [
        "personal_review_pipeline_hint",
        "personal_runtime_portfolio_count",
        "personal_runtime_portfolio_items",
        "global_runtime_portfolio_count",
        "global_runtime_portfolio_items",
        "global_runtime_portfolio_updated_at",
        "setRatingView('qualified')",
        "setRatingView('archive')",
        "qualifiedTasks",
        "rejectedArchiveTasks",
        "paginatedRatingTasks",
        "leaderboardSections",
        "formatLeaderboardValue",
        "catalogImportText",
        "runCatalogImport",
        "/admin/catalog/import",
        "hidden sm:block",
        "sm:hidden",
        "overflow-x-auto",
        "sheep_worker_token",
        "fetchApi('/workers/token'",
    ]
    for snippet in required_snippets:
        assert snippet in html


def test_nginx_conf_exposes_json_health_aliases():
    conf = (ROOT / "deploy" / "nginx" / "conf.d" / "app_https.conf").read_text(encoding="utf-8", errors="replace")

    assert "location = /healthz" in conf
    assert "location = /api {" in conf
    assert "location = /api/" in conf
    assert "location = /api/healthz" in conf
    assert "location /api/" in conf
    assert "proxy_pass http://$api_host:8000/manifest;" in conf
    assert "rewrite ^/api/(.*)$ /$1 break;" in conf
    assert "location = /app/_stcore/health" in conf
    assert '{"ok":true,"service":"spa","ui":"static"}' in conf


def test_public_health_check_script_requires_json_content_type():
    module = _load_module(DEPLOY_SCRIPTS / "check_public_health_endpoints.py", "check_public_health_endpoints")

    class _FakeHeaders(dict):
        def get(self, key, default=None):
            return super().get(key, default)

    class _FakeResponse:
        def __init__(self, *, status: int, content_type: str, payload: dict):
            self.status = status
            self.headers = _FakeHeaders({"Content-Type": content_type})
            self._payload = payload

        def getcode(self):
            return self.status

        def read(self):
            return json.dumps(self._payload).encode("utf-8")

    def _json_opener(request, timeout=10.0):
        return _FakeResponse(status=200, content_type="application/json; charset=utf-8", payload={"ok": True, "path": request.full_url})

    result = module.check_endpoint("https://sheep123.com", "/healthz", opener=_json_opener)
    assert result["ok"] is True
    assert result["content_type"] == "application/json"

    def _html_opener(request, timeout=10.0):
        return _FakeResponse(status=200, content_type="text/html; charset=utf-8", payload={"ok": True})

    result = module.check_endpoint("https://sheep123.com", "/healthz", opener=_html_opener)
    assert result["ok"] is False
    assert result["content_type"] == "text/html"
