from __future__ import annotations

import copy
import json
import os
from pathlib import Path
from typing import Any, Dict

from sheep_runtime_paths import (
    ensure_parent,
    realtime_config_template_path,
    realtime_local_config_path,
    realtime_public_config_path,
)
from sheep_secrets import redact_value


SECRET_FIELDS = {
    "api_key",
    "secret",
    "factor_pool_token",
    "factor_pool_user",
    "factor_pool_pass",
    "telegram_bot_token",
    "telegram_chat_id",
}


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return dict(data or {}) if isinstance(data, dict) else {}
    except Exception:
        return {}


def _merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = copy.deepcopy(dict(base or {}))
    for key, value in dict(override or {}).items():
        if isinstance(merged.get(key), dict) and isinstance(value, dict):
            merged[key] = _merge(dict(merged.get(key) or {}), value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def sanitize_public_config(raw: Dict[str, Any]) -> Dict[str, Any]:
    data = copy.deepcopy(dict(raw or {}))
    for key in SECRET_FIELDS:
        if key in data:
            data[key] = ""
    if "telegram_enabled" in data and not data.get("telegram_bot_token") and not data.get("telegram_chat_id"):
        data["telegram_enabled"] = bool(data.get("telegram_enabled", False))
    return data


def load_effective_config() -> Dict[str, Any]:
    template = _load_json(realtime_config_template_path())
    public_cfg = _load_json(realtime_public_config_path())
    local_cfg = _load_json(realtime_local_config_path())
    cfg = _merge(template, public_cfg)
    cfg = _merge(cfg, local_cfg)
    for key, env_name in (
        ("factor_pool_url", "SHEEP_FACTOR_POOL_URL"),
        ("factor_pool_token", "SHEEP_FACTOR_POOL_TOKEN"),
        ("factor_pool_user", "SHEEP_FACTOR_POOL_USER"),
        ("factor_pool_pass", "SHEEP_FACTOR_POOL_PASS"),
        ("telegram_bot_token", "SHEEP_TG_BOT_TOKEN"),
        ("telegram_chat_id", "SHEEP_TG_CHAT_ID"),
        ("api_key", "SHEEP_BITMART_API_KEY"),
        ("secret", "SHEEP_BITMART_API_SECRET"),
        ("memo", "SHEEP_BITMART_MEMO"),
    ):
        raw = str(os.environ.get(env_name, "") or "").strip()
        if raw:
            cfg[key] = raw
    return cfg


def ensure_example_config() -> Path:
    example_path = realtime_config_template_path()
    if not example_path.exists():
        public_cfg = _load_json(realtime_public_config_path())
        ensure_parent(example_path)
        example_path.write_text(
            json.dumps(sanitize_public_config(public_cfg), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    return example_path


def sanitized_effective_config() -> Dict[str, Any]:
    return redact_value(load_effective_config())
