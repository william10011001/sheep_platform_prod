import importlib
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import sheep_platform_db as db
from sheep_http import resolve_tls_verify
from sheep_secrets import REDACTION, contains_potential_secret, redact_text


def test_public_realtime_configs_do_not_contain_tracked_secrets():
    config_paths = [
        ROOT / "實盤程式" / "tema_rsi_gui_config.json",
        ROOT / "實盤程式" / "tema_rsi_gui_config.example.json",
    ]
    for path in config_paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["telegram_bot_token"] == ""
        assert payload["telegram_chat_id"] == ""
        assert contains_potential_secret(path.read_text(encoding="utf-8")) is False


def test_log_sys_event_redacts_sensitive_values(tmp_path, monkeypatch):
    monkeypatch.setenv("SHEEP_DB_URL", "")
    monkeypatch.setenv("SHEEP_DB_PATH", str(tmp_path / "security.sqlite3"))
    db_module = sys.modules.get("sheep_platform_db")
    if db_module is None:
        db_module = importlib.import_module("sheep_platform_db")
    db_module = importlib.reload(db_module)
    db_module.init_db()
    token = "1234567890:ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789"
    db_module.log_sys_event(
        "UNKNOWN_ROUTE",
        None,
        f"telegram send failed token={token}",
        {"telegram_bot_token": token, "telegram_chat_id": "6071244154"},
    )
    conn = db_module._conn()
    try:
        row = conn.execute(
            "SELECT message, detail_json FROM sys_monitor_events WHERE event_type = ? ORDER BY id DESC LIMIT 1",
            ("UNKNOWN_ROUTE",),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    row_dict = dict(row)
    assert token not in row_dict["message"]
    assert token not in row_dict["detail_json"]
    assert REDACTION in row_dict["message"] or "12..." in row_dict["message"]
    assert "60..." in row_dict["detail_json"] or REDACTION in row_dict["detail_json"]


def test_tls_verification_defaults_to_enabled(monkeypatch):
    monkeypatch.delenv("SHEEP_CA_BUNDLE", raising=False)
    monkeypatch.delenv("SHEEP_ALLOW_INSECURE_TLS", raising=False)
    assert resolve_tls_verify(default=True) is True
    monkeypatch.setenv("SHEEP_ALLOW_INSECURE_TLS", "1")
    assert resolve_tls_verify(default=True) is False
    assert REDACTION in redact_text("Authorization: Bearer secret-token")
