from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable


REDACTION = "[REDACTED]"
SENSITIVE_KEYS = {
    "api_key",
    "authorization",
    "bearer",
    "bot_token",
    "chat_id",
    "factor_pool_pass",
    "factor_pool_token",
    "password",
    "secret",
    "sheep_compute_pass",
    "telegram_bot_token",
    "telegram_chat_id",
    "token",
    "x-bm-key",
    "x-bm-sign",
}
STRUCTURED_SENSITIVE_KEYS = {str(key).strip().lower() for key in SENSITIVE_KEYS}
_KEY_RE = re.compile("|".join(re.escape(key) for key in sorted(SENSITIVE_KEYS)), re.IGNORECASE)
_TELEGRAM_TOKEN_RE = re.compile(r"\b\d{8,12}:[A-Za-z0-9_-]{20,}\b")
_BITMART_KEY_RE = re.compile(r"\b[a-fA-F0-9]{32,96}\b")
_AUTH_HEADER_RE = re.compile(r"(?i)(authorization\s*[:=]\s*bearer\s+)([A-Za-z0-9._\-]+)")
_PASSWORD_PAIR_RE = re.compile(r'(?i)("?(?:password|secret|token|chat_id|api_key|bot_token)"?\s*[:=]\s*"?)([^",\s}]+)')


def _mask(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= 8:
        return REDACTION
    return f"{text[:2]}...{text[-2:]}"


def redact_text(value: Any) -> str:
    text = str(value or "")
    if not text:
        return ""
    text = _AUTH_HEADER_RE.sub(r"\1" + REDACTION, text)
    text = _PASSWORD_PAIR_RE.sub(lambda m: f"{m.group(1)}{_mask(m.group(2))}", text)
    text = _TELEGRAM_TOKEN_RE.sub(REDACTION, text)
    text = _BITMART_KEY_RE.sub(lambda m: _mask(m.group(0)), text)
    return text


def redact_value(value: Any, *, key_hint: str = "") -> Any:
    hint = str(key_hint or "").strip().lower()
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(k): redact_value(v, key_hint=str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [redact_value(item, key_hint=hint) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_value(item, key_hint=hint) for item in value)
    if isinstance(value, str):
        if hint and _KEY_RE.search(hint):
            return _mask(value)
        return redact_text(value)
    if hint and _KEY_RE.search(hint):
        return REDACTION
    return value


def redact_json(value: Any) -> str:
    try:
        return json.dumps(redact_value(value), ensure_ascii=False, sort_keys=True)
    except Exception:
        return redact_text(value)


def _normalized_key(value: Any) -> str:
    return str(value or "").strip().lower()


def _is_structured_sensitive_key(value: Any) -> bool:
    return _normalized_key(value) in STRUCTURED_SENSITIVE_KEYS


def _payload_contains_secret(value: Any) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            if _is_structured_sensitive_key(key):
                if isinstance(item, str):
                    if item.strip():
                        return True
                elif item not in (None, "", 0, 0.0, False):
                    return True
            if _payload_contains_secret(item):
                return True
        return False
    if isinstance(value, list):
        return any(_payload_contains_secret(item) for item in value)
    if isinstance(value, tuple):
        return any(_payload_contains_secret(item) for item in value)
    if isinstance(value, str):
        return bool(_TELEGRAM_TOKEN_RE.search(value) or _AUTH_HEADER_RE.search(value))
    return False


def contains_potential_secret(value: Any) -> bool:
    text = str(value or "")
    if not text:
        return False
    try:
        payload = json.loads(text)
    except Exception:
        payload = None
    if payload is not None:
        return _payload_contains_secret(payload)
    if _TELEGRAM_TOKEN_RE.search(text):
        return True
    if _AUTH_HEADER_RE.search(text):
        return True
    if _PASSWORD_PAIR_RE.search(text):
        return True
    return False


def iter_sensitive_strings(payload: Dict[str, Any], *, prefix: str = "") -> Iterable[str]:
    for key, value in dict(payload or {}).items():
        next_prefix = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict):
            yield from iter_sensitive_strings(value, prefix=next_prefix)
            continue
        if isinstance(value, list):
            for index, item in enumerate(value):
                if isinstance(item, dict):
                    yield from iter_sensitive_strings(item, prefix=f"{next_prefix}[{index}]")
                elif contains_potential_secret(item):
                    yield f"{next_prefix}[{index}]={item}"
            continue
        if _is_structured_sensitive_key(key) and str(value or "").strip():
            yield f"{next_prefix}={value}"
        elif contains_potential_secret(value):
            yield f"{next_prefix}={value}"
