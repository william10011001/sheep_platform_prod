from __future__ import annotations

import json
from typing import Any, Dict, Optional


VALID_DIRECTIONS = {"long", "short"}


def parse_json_object(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def normalize_direction(
    direction: Any = None,
    *,
    reverse: Any = None,
    default: str = "long",
) -> str:
    text = str(direction or "").strip().lower()
    if text in VALID_DIRECTIONS:
        return text
    if reverse is not None:
        reverse_text = str(reverse).strip().lower()
        if reverse_text in {"true", "1", "1.0", "yes", "short"}:
            return "short"
        if reverse_text in {"false", "0", "0.0", "no", "long"}:
            return "long"
    return str(default or "long").strip().lower() if str(default or "").strip().lower() in VALID_DIRECTIONS else "long"


def direction_to_reverse(direction: Any) -> bool:
    return normalize_direction(direction) == "short"


def unwrap_family_params(raw_params: Any) -> Dict[str, Any]:
    params = parse_json_object(raw_params)
    nested = params.get("family_params")
    if isinstance(nested, dict):
        wrapper_keys = {"family", "family_params", "tp", "sl", "max_hold", "direction", "reverse"}
        if set(params.keys()).issubset(wrapper_keys):
            return dict(nested)
    return params


def normalize_runtime_strategy_entry(
    entry: Dict[str, Any],
    *,
    default_symbol: str = "",
    default_interval: str = "",
) -> Dict[str, Any]:
    raw = dict(entry or {})
    wrapper = parse_json_object(raw.get("family_params"))
    family_params = unwrap_family_params(raw.get("family_params"))

    direction = normalize_direction(
        raw.get("direction"),
        reverse=raw.get("reverse", wrapper.get("reverse")),
        default="long",
    )

    tp_pct = raw.get("tp_pct")
    if tp_pct in (None, "") and wrapper.get("tp") is not None:
        try:
            tp_pct = float(wrapper.get("tp") or 0.0) * 100.0
        except Exception:
            tp_pct = 0.0

    sl_pct = raw.get("sl_pct")
    if sl_pct in (None, "") and wrapper.get("sl") is not None:
        try:
            sl_pct = float(wrapper.get("sl") or 0.0) * 100.0
        except Exception:
            sl_pct = 0.0

    max_hold = raw.get("max_hold")
    if max_hold in (None, "") and wrapper.get("max_hold") is not None:
        max_hold = wrapper.get("max_hold")

    family = str(raw.get("family") or wrapper.get("family") or "").strip()
    symbol = str(raw.get("symbol") or default_symbol or "").strip().upper()
    interval = str(raw.get("interval") or default_interval or "").strip()

    normalized = {
        "strategy_id": raw.get("strategy_id"),
        "family": family,
        "family_params": family_params,
        "direction": direction,
        "tp_pct": float(tp_pct or 0.0),
        "sl_pct": float(sl_pct or 0.0),
        "max_hold": int(max_hold or 0),
        "stake_pct": float(raw.get("stake_pct") or 0.0),
        "symbol": symbol,
        "interval": interval,
    }
    return normalized
