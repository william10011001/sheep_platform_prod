from __future__ import annotations

import copy
import json
import math
import os
import re
import threading
import time
import traceback
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

from sheep_http import create_retry_session, request as http_request, resolve_tls_verify, summarize_http_detail
from sheep_secrets import redact_text

from sheep_runtime_paths import (
    kline_candidate_paths,
    normalize_symbol,
    project_root,
    report_dir,
    timeframe_candidate_labels,
    timeframe_interval_string,
    timeframe_min_to_label,
    unique_existing_paths,
)
from sheep_strategy_schema import normalize_direction

SUPPORTED_TIMEFRAMES = [1, 5, 15, 30, 60, 120, 240, 1440]


@dataclass
class HolyGrailResult:
    ok: bool
    message: str
    api_base: str = ""
    strategies_count: int = 0
    flattened_count: int = 0
    candidate_count: int = 0
    backtested_count: int = 0
    selected_count: int = 0
    selected_portfolio: List[Dict[str, Any]] = field(default_factory=list)
    portfolio_metrics: Dict[str, float] = field(default_factory=dict)
    weights: Dict[str, float] = field(default_factory=dict)
    multi_payload: List[Dict[str, Any]] = field(default_factory=list)
    multi_strategies_json: str = "[]"
    report_paths: Dict[str, str] = field(default_factory=dict)
    cost_basis: Dict[str, Any] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
    diagnostics: Dict[str, Any] = field(default_factory=dict)


class HolyGrailRuntime:
    _shared_factor_pool_cache_lock = threading.Lock()
    _shared_factor_pool_cache: Optional[Tuple[List[Dict[str, Any]], str, str]] = None
    _shared_factor_pool_cache_ts: float = 0.0
    _shared_factor_pool_cache_ttl_sec: float = 900.0

    def __init__(
        self,
        *,
        bt_module: Any,
        log: Optional[Callable[[str], None]] = None,
        factor_pool_url: Optional[str] = None,
        factor_pool_token: Optional[str] = None,
        factor_pool_user: Optional[str] = None,
        factor_pool_pass: Optional[str] = None,
        years: int = 3,
    ) -> None:
        self.bt = bt_module
        self.log = log or (lambda _msg: None)
        self.factor_pool_url = str(factor_pool_url or "").strip()
        self.factor_pool_token = str(factor_pool_token or "").strip()
        self.factor_pool_user = str(factor_pool_user or "").strip()
        self.factor_pool_pass = str(factor_pool_pass or "").strip()
        self.years = int(years)
        self._warning_keys: set[str] = set()
        self._warning_messages: List[str] = []
        self._kline_cache: Dict[str, pd.DataFrame] = {}
        self._last_factor_pool_fetch_used_cached_payload = False
        self._last_factor_pool_cache_age_s: Optional[float] = None
        self._http = create_retry_session(
            user_agent="sheep-holy-grail-runtime/3.0",
            total_retries=3,
            backoff_factor=0.5,
            pool_connections=8,
            pool_maxsize=8,
        )

    def info(self, message: str) -> None:
        self.log(message)

    def warn_once(self, key: str, message: str) -> None:
        if key in self._warning_keys:
            return
        self._warning_keys.add(key)
        self._warning_messages.append(message)
        self.log(message)

    def _factor_pool_creds(self) -> Tuple[str, str, str, str]:
        host = self.factor_pool_url or str(os.environ.get("SHEEP_FACTOR_POOL_URL", "https://sheep123.com")).strip()
        token = self.factor_pool_token or str(os.environ.get("SHEEP_FACTOR_POOL_TOKEN", "")).strip()
        user = self.factor_pool_user or str(os.environ.get("SHEEP_FACTOR_POOL_USER", "")).strip()
        password = self.factor_pool_pass or str(os.environ.get("SHEEP_FACTOR_POOL_PASS", "")).strip()
        return host, token, user, password

    @staticmethod
    def _normalize_scalar(value: Any) -> Any:
        if isinstance(value, np.generic):
            return value.item()
        if isinstance(value, float) and float(value).is_integer():
            return int(value)
        return value

    def detect_api_base(self, host_url: str) -> str:
        host_url = str(host_url or "").rstrip("/")
        if not host_url:
            return "https://sheep123.com/api"
        normalized_candidates: List[str] = []
        if host_url.endswith("/api"):
            normalized_candidates.append(host_url)
        elif host_url.endswith("/sheep123"):
            normalized_candidates.append(f"{host_url[:-9]}/api")
            normalized_candidates.append(host_url)
        else:
            normalized_candidates.append(f"{host_url}/api")
            normalized_candidates.append(f"{host_url}/sheep123")
            normalized_candidates.append(host_url)
        seen: set[str] = set()
        prefixes = []
        for candidate in normalized_candidates:
            candidate = str(candidate or "").rstrip("/")
            if candidate and candidate not in seen:
                seen.add(candidate)
                prefixes.append(candidate)
        for api_base in prefixes:
            test_url = f"{api_base}/healthz"
            try:
                res = http_request(self._http, "GET", test_url, timeout=5, verify=resolve_tls_verify(default=True))
                if res.status_code == 200 and "ok" in res.text.lower():
                    return api_base
            except requests.exceptions.RequestException:
                continue
        return prefixes[0] if prefixes else f"{host_url}/api"

    def _issue_factor_pool_token(self, api_base: str, user: str, password: str) -> str:
        if not user or not password:
            raise RuntimeError(
                "Missing factor pool credentials. Set SHEEP_FACTOR_POOL_TOKEN or SHEEP_FACTOR_POOL_USER and SHEEP_FACTOR_POOL_PASS."
            )
        login_url = f"{api_base}/token"
        payload = {"username": user, "password": password, "name": "compute"}
        resp = http_request(
            self._http,
            "POST",
            login_url,
            timeout=15,
            verify=resolve_tls_verify(default=True),
            json=payload,
        )
        if resp.status_code != 200:
            detail = self._summarize_http_body(resp.text)
            raise RuntimeError(f"factor-pool login failed ({resp.status_code}): {detail}")
        token = str((resp.json() or {}).get("token") or "").strip()
        if not token:
            raise RuntimeError("factor-pool login succeeded but token was empty")
        return token

    @staticmethod
    def _factor_pool_headers(token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    @staticmethod
    def _summarize_http_body(text: Any, *, limit: int = 160) -> str:
        raw = redact_text(text).strip()
        if not raw:
            return ""
        if "<html" in raw.lower():
            match = re.search(r"<title>(.*?)</title>", raw, flags=re.IGNORECASE | re.DOTALL)
            if match:
                raw = match.group(1)
            else:
                raw = re.sub(r"<[^>]+>", " ", raw)
        raw = re.sub(r"\s+", " ", raw).strip()
        return raw[:limit]

    @classmethod
    def _remember_shared_factor_pool_cache(cls, payload: Tuple[List[Dict[str, Any]], str, str]) -> None:
        try:
            cached_payload = copy.deepcopy(payload)
        except Exception:
            cached_payload = payload
        with cls._shared_factor_pool_cache_lock:
            cls._shared_factor_pool_cache = cached_payload
            cls._shared_factor_pool_cache_ts = time.time()

    @classmethod
    def _get_fresh_shared_factor_pool_cache(cls) -> Tuple[Optional[Tuple[List[Dict[str, Any]], str, str]], Optional[float]]:
        with cls._shared_factor_pool_cache_lock:
            payload = cls._shared_factor_pool_cache
            cached_ts = float(cls._shared_factor_pool_cache_ts or 0.0)
        if payload is None or cached_ts <= 0:
            return None, None
        age_s = max(0.0, time.time() - cached_ts)
        if age_s > float(cls._shared_factor_pool_cache_ttl_sec):
            return None, age_s
        try:
            return copy.deepcopy(payload), age_s
        except Exception:
            return payload, age_s

    @staticmethod
    def _is_auth_factor_pool_error(exc: Exception) -> bool:
        text = str(exc or "").lower()
        auth_markers = [
            "(401)",
            "(403)",
            " 401",
            " 403",
            "unauthorized",
            "forbidden",
            "invalid token",
            "token expired",
            "token invalid",
            "auth failed",
            "login failed (401)",
            "login failed (403)",
        ]
        return any(marker in text for marker in auth_markers)

    def _fetch_factor_pool_pages(self, api_base: str, token: str) -> List[Dict[str, Any]]:
        strategies_url = f"{api_base}/admin/strategies"
        collected: List[Dict[str, Any]] = []
        page = 1
        page_size = 200
        while True:
            resp = None
            last_error = ""
            for attempt in range(3):
                try:
                    resp = http_request(
                        self._http,
                        "GET",
                        strategies_url,
                        timeout=60,
                        verify=resolve_tls_verify(default=True),
                        params={"page": page, "page_size": page_size},
                        headers=self._factor_pool_headers(token),
                    )
                except requests.exceptions.RequestException as exc:
                    last_error = f"factor-pool fetch failed: {summarize_http_detail(exc)}"
                    if attempt < 2:
                        self.log(f"[HolyGrail] factor pool page {page} 暫時不可用，{attempt + 1}/3 重試中...")
                        time.sleep(1.5 * (attempt + 1))
                        continue
                    raise RuntimeError(last_error) from exc
                if resp.status_code == 200:
                    break
                last_error = f"factor-pool fetch failed ({resp.status_code}): {self._summarize_http_body(resp.text)}"
                if resp.status_code not in {408, 425, 429, 500, 502, 503, 504, 521, 522, 523, 524}:
                    raise RuntimeError(last_error)
                if attempt < 2:
                    self.log(f"[HolyGrail] factor pool page {page} 暫時不可用，{attempt + 1}/3 重試中...")
                    time.sleep(1.5 * (attempt + 1))
            if resp is None or resp.status_code != 200:
                raise RuntimeError(last_error or "factor-pool fetch failed with empty response")
            body = resp.json() or {}
            batch = list(body.get("items") or body.get("strategies") or [])
            if not batch:
                break
            collected.extend(batch)
            total = body.get("total")
            has_next = bool(body.get("has_next"))
            if has_next:
                page += 1
                continue
            if total not in (None, "") and len(collected) < int(total) and len(batch) >= page_size:
                page += 1
                continue
            break
        return collected

    def _fetch_cost_settings(self, api_base: str, token: str, *, fallback_fee_side: float) -> Dict[str, Any]:
        try:
            resp = http_request(
                self._http,
                "GET",
                f"{api_base}/settings/snapshot",
                timeout=20,
                verify=resolve_tls_verify(default=True),
                headers=self._factor_pool_headers(token),
            )
            if resp.status_code == 200:
                payload = resp.json() or {}
                thresholds = dict(payload.get("thresholds") or {})
                cost_basis = dict(payload.get("cost_basis") or {})
                fee_pct = float(cost_basis.get("fee_pct") if cost_basis.get("fee_pct") is not None else thresholds.get("global_fee_pct") or (fallback_fee_side * 100.0))
                slippage_pct = float(cost_basis.get("slippage_pct") if cost_basis.get("slippage_pct") is not None else thresholds.get("global_slippage_pct") or 0.0)
                return {
                    "fee_pct": fee_pct,
                    "slippage_pct": slippage_pct,
                    "fee_side": fee_pct / 100.0,
                    "slippage": slippage_pct / 100.0,
                    "source_settings_updated_at": str(
                        cost_basis.get("source_settings_updated_at")
                        or (payload.get("updated_at") or {}).get("global_slippage_pct")
                        or (payload.get("updated_at") or {}).get("global_fee_pct")
                        or datetime.now(timezone.utc).isoformat()
                    ),
                }
        except Exception as exc:
            self.warn_once(
                "holy-grail-cost-settings-fetch-failed",
                f"[HolyGrail] cost settings fetch failed, fallback to local defaults: {exc}",
            )
        return {
            "fee_pct": float(fallback_fee_side) * 100.0,
            "slippage_pct": 0.0,
            "fee_side": float(fallback_fee_side),
            "slippage": 0.0,
            "source_settings_updated_at": datetime.now(timezone.utc).isoformat(),
        }

    def fetch_factor_pool_data(self) -> Tuple[List[Dict[str, Any]], str, str]:
        host, token, user, password = self._factor_pool_creds()
        api_base = self.detect_api_base(host)
        attempts: List[str] = []
        self._last_factor_pool_fetch_used_cached_payload = False
        self._last_factor_pool_cache_age_s = None

        if token:
            try:
                payload = (self._fetch_factor_pool_pages(api_base, token), api_base, token)
                self._remember_shared_factor_pool_cache(payload)
                return payload
            except Exception as exc:
                attempts.append(str(exc))
                if not user or not password or not self._is_auth_factor_pool_error(exc):
                    raise
                self.warn_once(
                    "factor-pool-token-fetch-failed",
                    f"[HolyGrail] factor pool token fetch failed, retrying with password auth: {exc}",
                )

        login_token = self._issue_factor_pool_token(api_base, user, password)
        try:
            payload = (self._fetch_factor_pool_pages(api_base, login_token), api_base, login_token)
            self._remember_shared_factor_pool_cache(payload)
            return payload
        except Exception as exc:
            attempts.append(str(exc))
            raise RuntimeError(" | ".join(attempts))

    def flatten_strategies_to_dataframe(self, strategies: Iterable[Dict[str, Any]]) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        for strat in strategies or []:
            base_info = {
                "strategy_id": strat.get("strategy_id"),
                "symbol": strat.get("symbol"),
                "timeframe_min": strat.get("timeframe_min"),
                "pool_name": strat.get("pool_name"),
                "allocation_pct": float(strat.get("allocation_pct") or strat.get("allocationPct") or 0.0),
                "created_at": str(strat.get("created_at") or strat.get("createdAt") or ""),
            }

            params = dict(strat.get("params") or {})
            if not params and "params_json" in strat:
                try:
                    params = json.loads(str(strat.get("params_json") or "{}"))
                except Exception:
                    params = {}

            metrics = dict(strat.get("metrics") or {})
            if not metrics and "metrics_json" in strat:
                try:
                    metrics = json.loads(str(strat.get("metrics_json") or "{}"))
                except Exception:
                    metrics = {}

            family_params = dict(params.get("family_params") or {})
            direction = normalize_direction(
                strat.get("direction") or params.get("direction") or family_params.get("direction"),
                reverse=params.get("reverse", family_params.get("reverse")),
                default="long",
            )
            family = str(params.get("family") or strat.get("family") or "").strip()
            if family:
                row = dict(base_info)
                row["cand_score"] = float(strat.get("score") or metrics.get("sharpe") or 0.0)
                row["family"] = family
                row["param_tp"] = float(params.get("tp") or 0.0)
                row["param_sl"] = float(params.get("sl") or 0.0)
                row["param_max_hold"] = int(params.get("max_hold") or 0)
                row["param_reverse_mode"] = direction == "short"
                row["direction"] = direction
                row["candidate_source"] = "scored_strategy" if metrics else "template"
                row["has_metrics"] = bool(metrics)
                row["metric_sharpe"] = metrics.get("sharpe")
                row["metric_cagr_pct"] = metrics.get("cagr_pct")
                row["metric_max_drawdown_pct"] = metrics.get("max_drawdown_pct")
                for key, value in family_params.items():
                    row[f"param_{key}"] = value
                for key, value in metrics.items():
                    row[f"metric_{key}"] = value
                rows.append(row)
                continue

            progress = strat.get("progress")
            if not progress and "progress_json" in strat:
                try:
                    progress = json.loads(strat["progress_json"] or "{}")
                except Exception:
                    progress = {}
            elif not progress:
                progress = {}

            candidates = list(progress.get("checkpoint_candidates") or [])
            if not candidates and strat.get("metrics") and progress.get("best_any_params"):
                candidates = [
                    {
                        "score": strat.get("score", 0),
                        "params": progress.get("best_any_params", {}),
                        "metrics": strat.get("metrics", {}),
                    }
                ]

            for cand in candidates:
                row = dict(base_info)
                row["cand_score"] = cand.get("score", 0)
                params = dict(cand.get("params") or {})
                row["family"] = params.get("family", "Unknown")
                row["param_tp"] = params.get("tp", 0)
                row["param_sl"] = params.get("sl", 0)
                row["param_max_hold"] = params.get("max_hold", 0)
                cand_direction = normalize_direction(
                    cand.get("direction", params.get("direction")),
                    reverse=params.get("reverse"),
                    default=strat.get("direction") or "long",
                )
                row["param_reverse_mode"] = cand_direction == "short"
                row["direction"] = cand_direction
                row["candidate_source"] = "checkpoint_candidate" if cand.get("metrics") else "template"
                row["has_metrics"] = bool(cand.get("metrics"))
                row["metric_sharpe"] = dict(cand.get("metrics") or {}).get("sharpe")
                row["metric_cagr_pct"] = dict(cand.get("metrics") or {}).get("cagr_pct")
                row["metric_max_drawdown_pct"] = dict(cand.get("metrics") or {}).get("max_drawdown_pct")
                for key, value in dict(params.get("family_params") or {}).items():
                    row[f"param_{key}"] = value
                for key, value in dict(cand.get("metrics") or {}).items():
                    row[f"metric_{key}"] = value
                rows.append(row)
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        for column in ("metric_sharpe", "metric_cagr_pct", "metric_max_drawdown_pct", "allocation_pct", "cand_score"):
            if column not in df.columns:
                df[column] = np.nan
        if "created_at" not in df.columns:
            df["created_at"] = ""
        if "candidate_source" not in df.columns:
            df["candidate_source"] = "template"
        if "has_metrics" not in df.columns:
            df["has_metrics"] = False
        if "direction" not in df.columns:
            reverse_values = df["param_reverse_mode"].tolist() if "param_reverse_mode" in df.columns else [False] * len(df)
            df["direction"] = [
                normalize_direction(reverse=value, default="long") for value in reverse_values
            ]
        return df

    @staticmethod
    def _row_direction(row: pd.Series) -> str:
        return normalize_direction(row.get("direction"), reverse=row.get("param_reverse_mode"), default="long")

    @staticmethod
    def _created_sort_value(value: Any) -> float:
        ts = pd.to_datetime(value, utc=True, errors="coerce")
        if pd.isna(ts):
            return float("-inf")
        return float(ts.value)

    @staticmethod
    def _direction_counts(df_params: pd.DataFrame) -> Dict[str, int]:
        counts: Dict[str, int] = {"long": 0, "short": 0}
        if df_params is None or df_params.empty:
            return counts
        if "direction" not in df_params.columns:
            return counts
        for raw_direction in df_params["direction"].tolist():
            direction = normalize_direction(raw_direction, default="long")
            counts[direction] = int(counts.get(direction) or 0) + 1
        return counts

    @staticmethod
    def _prepare_candidate_sort_columns(df_params: pd.DataFrame) -> pd.DataFrame:
        working = df_params.copy()
        for column in ("metric_sharpe", "metric_cagr_pct", "metric_max_drawdown_pct", "allocation_pct", "cand_score"):
            if column not in working.columns:
                working[column] = np.nan
        if "candidate_source" not in working.columns:
            working["candidate_source"] = "template"
        if "has_metrics" not in working.columns:
            working["has_metrics"] = False
        if "created_at" not in working.columns:
            working["created_at"] = ""
        if "direction" not in working.columns:
            reverse_values = working["param_reverse_mode"].tolist() if "param_reverse_mode" in working.columns else [False] * len(working)
            working["direction"] = [
                normalize_direction(reverse=value, default="long") for value in reverse_values
            ]
        working["metric_sharpe_sort"] = pd.to_numeric(working["metric_sharpe"], errors="coerce").fillna(float("-inf"))
        working["metric_cagr_sort"] = pd.to_numeric(working["metric_cagr_pct"], errors="coerce").fillna(float("-inf"))
        working["metric_max_dd_sort"] = pd.to_numeric(working["metric_max_drawdown_pct"], errors="coerce").fillna(float("inf"))
        working["allocation_pct_sort"] = pd.to_numeric(working["allocation_pct"], errors="coerce").fillna(0.0)
        working["cand_score_sort"] = pd.to_numeric(working["cand_score"], errors="coerce").fillna(0.0)
        working["strategy_id_sort"] = pd.to_numeric(working["strategy_id"], errors="coerce").fillna(0.0)
        working["created_at_sort"] = [HolyGrailRuntime._created_sort_value(value) for value in working["created_at"]]
        return working

    @staticmethod
    def _sorted_preselection_frame(frame: pd.DataFrame) -> pd.DataFrame:
        if frame is None or frame.empty:
            return frame.copy()
        return frame.sort_values(
            by=[
                "metric_sharpe_sort",
                "metric_cagr_sort",
                "metric_max_dd_sort",
                "cand_score_sort",
                "allocation_pct_sort",
                "created_at_sort",
                "strategy_id_sort",
            ],
            ascending=[False, False, True, False, False, False, False],
            kind="mergesort",
        )

    @staticmethod
    def _timeframe_min_value(raw_value: Any) -> int:
        try:
            return int(raw_value or 0)
        except Exception:
            return 0

    def _candidate_param_fingerprint(
        self,
        row: Any,
        *,
        direction: Optional[str] = None,
        include_timeframe: bool = True,
    ) -> str:
        row_dict = dict(getattr(row, "items", lambda: [])())
        param_dict = {
            key: self._normalize_scalar(value)
            for key, value in row_dict.items()
            if str(key).startswith("param_") and pd.notna(value)
        }
        payload = {
            "family": str(row_dict.get("family") or ""),
            "symbol": str(row_dict.get("symbol") or ""),
            "direction": normalize_direction(
                direction if direction is not None else row_dict.get("direction"),
                default="long",
            ),
            "params": param_dict,
        }
        if include_timeframe:
            payload["timeframe_min"] = self._timeframe_min_value(row_dict.get("timeframe_min"))
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)

    def _candidate_group_key(
        self,
        row: Any,
        *,
        direction: Optional[str] = None,
    ) -> Tuple[str, str, str, int]:
        row_dict = dict(getattr(row, "items", lambda: [])())
        return (
            str(row_dict.get("family") or ""),
            str(row_dict.get("symbol") or ""),
            normalize_direction(direction if direction is not None else row_dict.get("direction"), default="long"),
            self._timeframe_min_value(row_dict.get("timeframe_min")),
        )

    def _distinct_group_count(self, frame: pd.DataFrame, *, direction: Optional[str] = None) -> int:
        if frame is None or frame.empty:
            return 0
        group_keys = set()
        for _, row in frame.iterrows():
            group_keys.add(self._candidate_group_key(row, direction=direction))
        return len(group_keys)

    def _diversified_preselection_rows(
        self,
        frame: pd.DataFrame,
        *,
        limit: int,
    ) -> Tuple[List[pd.Series], int]:
        if frame is None or frame.empty or limit <= 0:
            return [], 0

        grouped_rows: Dict[Tuple[int, str, str], List[pd.Series]] = {}
        timeframe_family_order: Dict[int, List[str]] = {}
        family_groups: Dict[Tuple[int, str], List[Tuple[int, str, str]]] = {}
        for _, row in frame.iterrows():
            group_key = (
                self._timeframe_min_value(row.get("timeframe_min")),
                str(row.get("family") or ""),
                str(row.get("symbol") or ""),
            )
            if group_key not in grouped_rows:
                grouped_rows[group_key] = []
                timeframe_min, family, _symbol = group_key
                if timeframe_min not in timeframe_family_order:
                    timeframe_family_order[timeframe_min] = []
                if family not in timeframe_family_order[timeframe_min]:
                    timeframe_family_order[timeframe_min].append(family)
                family_groups.setdefault((timeframe_min, family), []).append(group_key)
            grouped_rows[group_key].append(row.copy())

        selected_rows: List[pd.Series] = []
        selected_fingerprints: set[str] = set()
        diversified_groups_used: set[Tuple[int, str, str]] = set()
        timeframe_order = sorted(timeframe_family_order.keys())

        made_progress = True
        while made_progress and len(selected_rows) < limit:
            made_progress = False
            for timeframe_min in timeframe_order:
                for family in timeframe_family_order.get(timeframe_min) or []:
                    family_key = (timeframe_min, family)
                    for group_key in family_groups.get(family_key) or []:
                        bucket = grouped_rows.get(group_key) or []
                        while bucket:
                            candidate_row = bucket.pop(0)
                            candidate_fp = self._candidate_param_fingerprint(candidate_row)
                            if candidate_fp in selected_fingerprints:
                                continue
                            selected_rows.append(candidate_row)
                            selected_fingerprints.add(candidate_fp)
                            diversified_groups_used.add(group_key)
                            made_progress = True
                            break
                        if made_progress and len(selected_rows) >= limit:
                            break
                    if len(selected_rows) >= limit:
                        break
                if len(selected_rows) >= limit:
                    break

        if len(selected_rows) < limit:
            for _, row in frame.iterrows():
                candidate_fp = self._candidate_param_fingerprint(row)
                if candidate_fp in selected_fingerprints:
                    continue
                selected_rows.append(row.copy())
                selected_fingerprints.add(candidate_fp)
                if len(selected_rows) >= limit:
                    break

        return selected_rows[:limit], len(diversified_groups_used)

    def _augment_directional_coverage(
        self,
        df_params: pd.DataFrame,
        *,
        top_n: int,
    ) -> Tuple[pd.DataFrame, Dict[str, int], Dict[str, int], Dict[str, int]]:
        if df_params.empty:
            empty_counts = {"long": 0, "short": 0}
            return df_params.copy(), empty_counts, empty_counts, {"mirror_diversified_source_count": 0}

        working = self._prepare_candidate_sort_columns(df_params)
        source_counts = self._direction_counts(working)
        per_direction = max(1, int(top_n) // 2)
        augmented_rows: List[Dict[str, Any]] = []
        mirror_diversified_source_count = 0

        for target_direction in ("long", "short"):
            existing_count = int(source_counts.get(target_direction) or 0)
            if existing_count >= per_direction:
                continue

            opposite_direction = "short" if target_direction == "long" else "long"
            source_frame = self._sorted_preselection_frame(
                working[working["direction"] == opposite_direction].copy()
            )
            if source_frame.empty:
                continue

            seen_fingerprints: set[str] = set()
            target_frame = working[working["direction"] == target_direction]
            for _, row in target_frame.iterrows():
                fingerprint = self._candidate_param_fingerprint(
                    row,
                    direction=target_direction,
                    include_timeframe=True,
                )
                seen_fingerprints.add(fingerprint)

            added_count = 0
            source_rows, diversified_count = self._diversified_preselection_rows(
                source_frame,
                limit=max(0, per_direction - existing_count),
            )
            mirror_diversified_source_count += int(diversified_count or 0)
            for row in source_rows:
                mirror = row.copy()
                fingerprint = self._candidate_param_fingerprint(
                    mirror,
                    direction=target_direction,
                    include_timeframe=True,
                )
                if fingerprint in seen_fingerprints:
                    continue

                mirror["direction"] = target_direction
                mirror["param_reverse_mode"] = target_direction == "short"
                mirror["param_direction"] = target_direction
                mirror["param_reverse"] = target_direction == "short"
                if pd.notna(mirror.get("param_curve_key")):
                    mirror["param_curve_key"] = f"{mirror.get('param_curve_key')}__mirror_{target_direction}"
                mirror["candidate_source"] = "synthetic_mirror"
                mirror["has_metrics"] = False
                mirror["metric_sharpe"] = np.nan
                mirror["metric_cagr_pct"] = np.nan
                mirror["metric_max_drawdown_pct"] = np.nan
                mirror["metric_sharpe_sort"] = float("-inf")
                mirror["metric_cagr_sort"] = float("-inf")
                mirror["metric_max_dd_sort"] = float("inf")
                mirror["mirror_parent_direction"] = opposite_direction
                mirror["mirror_source_strategy_id"] = mirror.get("strategy_id")
                mirror["mirror_generated"] = True
                augmented_rows.append(mirror.to_dict())
                seen_fingerprints.add(fingerprint)
                added_count += 1
                if existing_count + added_count >= per_direction:
                    break

            if added_count > 0:
                self.warn_once(
                    f"holy-grail-mirror-{target_direction}-{source_counts.get(target_direction, 0)}-{source_counts.get(opposite_direction, 0)}",
                    "[HolyGrail] source candidates are directionally underfilled; "
                    f"generated {added_count} mirrored {target_direction} exploration candidates "
                    f"from {opposite_direction} strategies.",
                )

        if augmented_rows:
            working = pd.concat([working, pd.DataFrame(augmented_rows)], ignore_index=True, sort=False)
            working = self._prepare_candidate_sort_columns(working)
        augmented_counts = self._direction_counts(working)
        return working, source_counts, augmented_counts, {
            "mirror_diversified_source_count": int(mirror_diversified_source_count),
            "augmented_long_distinct_groups": int(
                self._distinct_group_count(working[working["direction"] == "long"].copy(), direction="long")
            ),
            "augmented_short_distinct_groups": int(
                self._distinct_group_count(working[working["direction"] == "short"].copy(), direction="short")
            ),
        }

    def _candidate_pool(self, df_params: pd.DataFrame, *, top_n: int = 150, max_per_group: int = 3) -> pd.DataFrame:
        if df_params.empty:
            return df_params.copy()

        per_direction = max(1, int(top_n) // 2)
        if per_direction >= 75:
            scored_target = 50
            exploration_target = 25
        else:
            exploration_target = max(0, int(round(per_direction * (25.0 / 75.0))))
            scored_target = max(0, per_direction - exploration_target)

        working = self._prepare_candidate_sort_columns(df_params)
        working["direction"] = [self._row_direction(row) for _, row in working.iterrows()]

        selected_rows: List[pd.Series] = []
        for direction in ("long", "short"):
            dir_df = working[working["direction"] == direction].copy()
            if dir_df.empty:
                continue

            dir_selected: List[pd.Series] = []
            group_counts: Dict[Tuple[str, str, str, int], int] = {}
            seen_params: set[str] = set()

            scored_df = self._sorted_preselection_frame(dir_df[dir_df["has_metrics"] == True].copy())
            explore_df = dir_df[dir_df["has_metrics"] != True].sort_values(
                by=["allocation_pct_sort", "created_at_sort", "strategy_id_sort", "cand_score_sort"],
                ascending=[False, False, False, False],
                kind="mergesort",
            )

            def _extend_from_frame(frame: pd.DataFrame, *, limit: int, bucket: str) -> None:
                if limit <= 0 or frame.empty:
                    return
                bucket_count = lambda: len([item for item in dir_selected if str(item.get("direction_bucket")) == bucket])
                diversity_first = direction == "short"
                pass_limits = list(range(1, int(max_per_group) + 1)) if diversity_first else [int(max_per_group)]
                for pass_limit in pass_limits:
                    if bucket_count() >= limit or len(dir_selected) >= per_direction:
                        break
                    for _, row in frame.iterrows():
                        family = str(row.get("family", "Unknown"))
                        symbol = str(row.get("symbol", "Unknown"))
                        row_direction = str(row.get("direction") or direction)
                        timeframe_min = self._timeframe_min_value(row.get("timeframe_min"))
                        group_key = (family, symbol, row_direction, timeframe_min)
                        param_fingerprint = self._candidate_param_fingerprint(
                            row,
                            direction=row_direction,
                            include_timeframe=True,
                        )
                        if param_fingerprint in seen_params:
                            continue
                        if group_counts.get(group_key, 0) >= min(pass_limit, int(max_per_group)):
                            continue

                        row_copy = row.copy()
                        row_copy["direction_bucket"] = bucket
                        row_copy["preselect_rank"] = len(dir_selected) + 1
                        dir_selected.append(row_copy)
                        group_counts[group_key] = group_counts.get(group_key, 0) + 1
                        seen_params.add(param_fingerprint)
                        if bucket_count() >= limit or len(dir_selected) >= per_direction:
                            break

            _extend_from_frame(scored_df, limit=scored_target, bucket="scored")
            _extend_from_frame(explore_df, limit=exploration_target, bucket="exploration")
            if len(dir_selected) < per_direction:
                _extend_from_frame(scored_df, limit=per_direction, bucket="scored")
            if len(dir_selected) < per_direction:
                _extend_from_frame(explore_df, limit=per_direction, bucket="exploration")

            selected_rows.extend(dir_selected[:per_direction])

        if not selected_rows:
            return working.iloc[0:0].copy()

        pool_df = pd.DataFrame([row.to_dict() if hasattr(row, "to_dict") else dict(row) for row in selected_rows]).reset_index(drop=True)
        return pool_df

    def _load_csv(self, path: Path) -> pd.DataFrame:
        df = pd.read_csv(path)
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        df = df.dropna(subset=["ts"]).sort_values("ts").reset_index(drop=True)
        return df

    def _resample_ohlcv(self, df: pd.DataFrame, target_step_min: int) -> pd.DataFrame:
        if df.empty:
            return df

        renamed = {str(c).lower(): c for c in df.columns}
        required = ["ts", "open", "high", "low", "close", "volume"]
        if any(name not in renamed for name in required):
            raise ValueError(f"Missing OHLCV columns for resample: {required}")

        frame = df.rename(columns={renamed[name]: name for name in required})[required].copy()
        frame["ts"] = pd.to_datetime(frame["ts"], utc=True, errors="coerce")
        frame = frame.dropna(subset=["ts"]).set_index("ts").sort_index()
        rule = f"{int(target_step_min)}min"
        resampled = frame.resample(rule, label="left", closed="left").agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        return resampled.dropna(subset=["open", "high", "low", "close"]).reset_index()

    def _resample_source_candidates(self, symbol: str, target_step_min: int) -> List[Tuple[int, Path]]:
        candidates: List[Tuple[int, Path]] = []
        for source_step in sorted((step for step in SUPPORTED_TIMEFRAMES if step < target_step_min), reverse=True):
            if target_step_min % source_step != 0:
                continue
            existing = unique_existing_paths(kline_candidate_paths(symbol, source_step, years=self.years))
            if existing:
                candidates.append((source_step, existing[0]))
        return candidates

    def load_kline_data(self, symbol: str, timeframe_min: int) -> Optional[pd.DataFrame]:
        cache_key = f"{normalize_symbol(symbol)}_{int(timeframe_min)}"
        if cache_key in self._kline_cache:
            return self._kline_cache[cache_key]

        existing = unique_existing_paths(kline_candidate_paths(symbol, timeframe_min, years=self.years))
        if existing:
            try:
                df = self._load_csv(existing[0])
                self._kline_cache[cache_key] = df
                return df
            except Exception as exc:
                self.warn_once(
                    f"csv-read:{existing[0]}",
                    f"[HolyGrail] failed to read {existing[0]}: {exc}",
                )
                return None

        for source_step, source_path in self._resample_source_candidates(symbol, timeframe_min):
            try:
                source_df = self._load_csv(source_path)
                resampled = self._resample_ohlcv(source_df, int(timeframe_min))
                if not resampled.empty:
                    self.info(
                        "[HolyGrail] resampled "
                        f"{normalize_symbol(symbol)} {timeframe_min_to_label(source_step)} -> {timeframe_min_to_label(timeframe_min)} "
                        f"from {source_path.name}"
                    )
                    self._kline_cache[cache_key] = resampled
                    return resampled
            except Exception as exc:
                self.warn_once(
                    f"resample-failed:{normalize_symbol(symbol)}:{source_step}:{timeframe_min}",
                    f"[HolyGrail] failed to resample {normalize_symbol(symbol)} {source_step} -> {timeframe_min}: {exc}",
                )

        canonical_paths = kline_candidate_paths(symbol, timeframe_min, years=self.years)
        label_info = ", ".join(timeframe_candidate_labels(timeframe_min))
        first_path = str(canonical_paths[0]) if canonical_paths else str(project_root() / "app" / "data")
        self.warn_once(
            f"missing-kline:{normalize_symbol(symbol)}:{int(timeframe_min)}",
            f"[HolyGrail] missing kline for {normalize_symbol(symbol)} {label_info}. Expected near: {first_path}",
        )
        return None

    @staticmethod
    def build_daily_equity_curve(trades_detail: List[Dict[str, Any]]) -> pd.Series:
        if not trades_detail:
            return pd.Series(dtype=float)

        records: List[Dict[str, Any]] = []
        for trade in trades_detail:
            exit_time = trade.get("exit_time") or trade.get("exit_ts") or trade.get("Time")
            net_ret = trade.get("net_return")
            if net_ret is None:
                net_ret = trade.get("net_ret", 0.0)
            if exit_time is not None:
                records.append({"time": exit_time, "ret": float(net_ret or 0.0)})

        if not records:
            return pd.Series(dtype=float)

        df_trades = pd.DataFrame(records)
        try:
            df_trades["time"] = pd.to_datetime(df_trades["time"], utc=True, errors="coerce")
        except Exception:
            df_trades["time"] = pd.to_datetime(df_trades["time"], unit="s", utc=True, errors="coerce")
        df_trades = df_trades.dropna(subset=["time"]).set_index("time").sort_index()
        if df_trades.empty:
            return pd.Series(dtype=float)

        df_trades["equity"] = (1.0 + df_trades["ret"]).cumprod()
        return df_trades["equity"].resample("1D").last().ffill()

    @staticmethod
    def _stable_number(value: Any, *, precision: int = 8) -> Optional[float]:
        try:
            number = float(value)
        except Exception:
            return None
        if math.isnan(number) or math.isinf(number):
            return None
        return round(number, precision)

    @staticmethod
    def _stable_timestamp(value: Any) -> str:
        ts = pd.to_datetime(value, utc=True, errors="coerce")
        if pd.isna(ts):
            return ""
        try:
            return ts.isoformat()
        except Exception:
            return str(ts)

    @classmethod
    def _trade_signature_hash(
        cls,
        trades_detail: List[Dict[str, Any]],
        *,
        symbol: str,
        direction: str,
    ) -> Tuple[str, str]:
        rows: List[Dict[str, Any]] = []
        for trade in trades_detail or []:
            normalized = {
                "symbol": normalize_symbol(symbol),
                "direction": normalize_direction(direction),
                "entry_ts": cls._stable_timestamp(trade.get("entry_ts") or trade.get("entry_time") or trade.get("Time")),
                "exit_ts": cls._stable_timestamp(trade.get("exit_ts") or trade.get("exit_time") or trade.get("Time")),
                "entry_price": cls._stable_number(trade.get("entry_price") or trade.get("entry_avg") or trade.get("entry")),
                "exit_price": cls._stable_number(trade.get("exit_price") or trade.get("exit_avg") or trade.get("exit")),
                "pnl_pct": cls._stable_number(
                    trade.get("pnl_pct")
                    if trade.get("pnl_pct") is not None
                    else trade.get("net_return")
                    if trade.get("net_return") is not None
                    else trade.get("net_ret")
                ),
            }
            rows.append(normalized)
        if not rows:
            return "", ""
        payload = json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest(), "trade"

    @classmethod
    def _equity_signature_hash(cls, equity_curve: pd.Series) -> Tuple[str, str]:
        if equity_curve is None or equity_curve.empty:
            return "", ""
        series = equity_curve.dropna().astype(float)
        if series.empty:
            return "", ""
        base_value = float(series.iloc[0]) if float(series.iloc[0]) != 0 else 1.0
        normalized = [cls._stable_number(v / base_value, precision=10) for v in series.tolist()]
        payload = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest(), "equity"

    @staticmethod
    def _sort_candidate_key(candidate: Dict[str, Any]) -> Tuple[float, float, float, str]:
        perf = candidate.get("perf") or {}
        return (
            -float(perf.get("sharpe") or 0.0),
            -float(perf.get("cagr_pct") or 0.0),
            float(perf.get("max_drawdown_pct") or 0.0),
            str(candidate.get("curve_key") or ""),
        )

    @staticmethod
    def _candidate_score_vector(candidate: Dict[str, Any]) -> Tuple[float, float, float, float]:
        perf = candidate.get("perf") or {}
        row_data = candidate.get("row_data") or {}
        return (
            float(perf.get("sharpe") or 0.0),
            float(perf.get("cagr_pct") or 0.0),
            float(row_data.get("cand_score") or 0.0),
            -float(perf.get("max_drawdown_pct") or 0.0),
        )

    def _select_balanced_pairing(
        self,
        *,
        long_candidates: List[Dict[str, Any]],
        short_candidates: List[Dict[str, Any]],
        corr_matrix: pd.DataFrame,
        corr_threshold: float,
        max_pairs: int,
    ) -> Tuple[List[Tuple[Dict[str, Any], Dict[str, Any]]], int]:
        longs = [candidate for candidate in list(long_candidates or []) if candidate]
        shorts = [candidate for candidate in list(short_candidates or []) if candidate]
        if not longs or not shorts or max_pairs <= 0:
            return [], 0

        candidate_by_key: Dict[str, Dict[str, Any]] = {}
        for candidate in longs + shorts:
            curve_key = str(candidate.get("curve_key") or "")
            if curve_key:
                candidate_by_key[curve_key] = candidate

        compat_cache: Dict[Tuple[str, str], bool] = {}

        def _compatible(left_key: str, right_key: str) -> bool:
            if not left_key or not right_key:
                return False
            if left_key == right_key:
                return False
            cache_key = tuple(sorted((left_key, right_key)))
            cached = compat_cache.get(cache_key)
            if cached is not None:
                return cached
            compatible = abs(self._corr_value(corr_matrix, left_key, right_key)) <= float(corr_threshold)
            compat_cache[cache_key] = compatible
            return compatible

        feasible_shorts_by_long: Dict[str, List[str]] = {}
        feasible_edges_count = 0
        for long_candidate in longs:
            long_key = str(long_candidate.get("curve_key") or "")
            feasible_shorts: List[str] = []
            for short_candidate in shorts:
                short_key = str(short_candidate.get("curve_key") or "")
                if _compatible(long_key, short_key):
                    feasible_shorts.append(short_key)
            feasible_shorts_by_long[long_key] = feasible_shorts
            feasible_edges_count += len(feasible_shorts)

        if feasible_edges_count <= 0:
            return [], 0

        long_keys = [str(candidate.get("curve_key") or "") for candidate in longs if str(candidate.get("curve_key") or "")]
        short_keys = [str(candidate.get("curve_key") or "") for candidate in shorts if str(candidate.get("curve_key") or "")]
        score_vector_by_key = {
            curve_key: self._candidate_score_vector(candidate_by_key[curve_key])
            for curve_key in list(candidate_by_key.keys())
        }

        def _pair_score(long_key: str, short_key: str) -> Tuple[float, float, float, float]:
            left = score_vector_by_key.get(long_key, (0.0, 0.0, 0.0, 0.0))
            right = score_vector_by_key.get(short_key, (0.0, 0.0, 0.0, 0.0))
            return tuple(float(left[idx]) + float(right[idx]) for idx in range(len(left)))

        best_pairs: List[Tuple[str, str]] = []
        best_score: Tuple[float, float, float, float] = (0.0, 0.0, 0.0, 0.0)

        def _search(
            remaining_long_keys: List[str],
            remaining_short_keys: List[str],
            selected_pairs: List[Tuple[str, str]],
            selected_keys: List[str],
            total_score: Tuple[float, float, float, float],
        ) -> None:
            nonlocal best_pairs, best_score

            current_pair_count = len(selected_pairs)
            best_pair_count = len(best_pairs)
            max_possible_pairs = current_pair_count + min(
                max(0, int(max_pairs) - current_pair_count),
                len(remaining_long_keys),
                len(remaining_short_keys),
            )
            if max_possible_pairs < best_pair_count:
                return

            if (
                current_pair_count > best_pair_count
                or (current_pair_count == best_pair_count and total_score > best_score)
            ):
                best_pairs = list(selected_pairs)
                best_score = tuple(total_score)

            if current_pair_count >= int(max_pairs) or not remaining_long_keys or not remaining_short_keys:
                return

            chosen_long_key = ""
            chosen_short_keys: List[str] = []
            for long_key in remaining_long_keys:
                feasible_short_keys: List[str] = []
                for short_key in remaining_short_keys:
                    if short_key not in feasible_shorts_by_long.get(long_key, []):
                        continue
                    if any(not _compatible(long_key, selected_key) for selected_key in selected_keys):
                        continue
                    if any(not _compatible(short_key, selected_key) for selected_key in selected_keys):
                        continue
                    feasible_short_keys.append(short_key)
                if not chosen_long_key or len(feasible_short_keys) < len(chosen_short_keys):
                    chosen_long_key = long_key
                    chosen_short_keys = feasible_short_keys
                    if not chosen_short_keys:
                        break

            if not chosen_long_key:
                return

            next_long_keys = [key for key in remaining_long_keys if key != chosen_long_key]
            for short_key in sorted(
                chosen_short_keys,
                key=lambda key: _pair_score(chosen_long_key, key),
                reverse=True,
            ):
                next_short_keys = [key for key in remaining_short_keys if key != short_key]
                pair_score = _pair_score(chosen_long_key, short_key)
                next_total_score = tuple(
                    float(total_score[idx]) + float(pair_score[idx]) for idx in range(len(total_score))
                )
                _search(
                    next_long_keys,
                    next_short_keys,
                    selected_pairs + [(chosen_long_key, short_key)],
                    selected_keys + [chosen_long_key, short_key],
                    next_total_score,
                )

            _search(
                next_long_keys,
                remaining_short_keys,
                selected_pairs,
                selected_keys,
                total_score,
            )

        _search(long_keys, short_keys, [], [], (0.0, 0.0, 0.0, 0.0))
        ordered_pairs = sorted(best_pairs, key=lambda item: _pair_score(item[0], item[1]), reverse=True)
        return [
            (candidate_by_key[long_key], candidate_by_key[short_key])
            for long_key, short_key in ordered_pairs
            if long_key in candidate_by_key and short_key in candidate_by_key
        ], int(feasible_edges_count)

    @staticmethod
    def _corr_value(corr_matrix: pd.DataFrame, left_key: str, right_key: str) -> float:
        if corr_matrix.empty or left_key not in corr_matrix.index or right_key not in corr_matrix.columns:
            return 0.0
        value = float(corr_matrix.loc[left_key, right_key])
        return 0.0 if math.isnan(value) else value

    def _family_payload_from_row(
        self,
        row: pd.Series,
        standard_params: Iterable[str],
    ) -> Tuple[float, float, int, bool, Dict[str, Any]]:
        tp = float(row.get("param_tp") or 0.0)
        sl = float(row.get("param_sl") or 0.0)
        max_hold = int(row.get("param_max_hold") or 0)

        family_params: Dict[str, Any] = {}
        reverse_mode = False
        for column, value in row.items():
            column = str(column)
            if not column.startswith("param_"):
                continue
            if column in standard_params or pd.isna(value):
                continue
            key = column.replace("param_", "", 1)
            if key == "reverse_mode":
                reverse_mode = str(value).strip().lower() in {"true", "1", "1.0"}
                continue
            family_params[key] = self._normalize_scalar(value)

        return tp, sl, max_hold, reverse_mode, family_params

    def _export_reports(
        self,
        *,
        final_rows: List[Dict[str, Any]],
        summary_rows: List[Dict[str, Any]],
        trades_rows: List[Dict[str, Any]],
    ) -> Dict[str, str]:
        day_dir = report_dir() / datetime.now().strftime("%Y%m%d")
        day_dir.mkdir(parents=True, exist_ok=True)

        final_report = day_dir / "Top20_Holy_Grail_Portfolio_Full_zh.csv"
        summary_report = day_dir / "Top20_Portfolio_Backtest_Summary_zh.csv"
        trades_report = day_dir / "Top20_Portfolio_Trades_Detail_zh.csv"

        pd.DataFrame(final_rows).to_csv(final_report, index=False, encoding="utf-8-sig")
        pd.DataFrame(summary_rows).to_csv(summary_report, index=False, encoding="utf-8-sig")
        pd.DataFrame(trades_rows).to_csv(trades_report, index=False, encoding="utf-8-sig")

        return {
            "final_report": str(final_report),
            "summary_report": str(summary_report),
            "trades_report": str(trades_report),
        }

    def build_portfolio(
        self,
        *,
        base_stake_pct: float = 95.0,
        top_n_candidates: int = 150,
        max_selected: int = 20,
        corr_threshold: float = 0.4,
        fee_side: float = 0.0006,
    ) -> HolyGrailResult:
        if self.bt is None:
            return HolyGrailResult(ok=False, message="backtest module is unavailable")

        try:
            strategies, api_base, access_token = self.fetch_factor_pool_data()
        except Exception as exc:
            return HolyGrailResult(
                ok=False,
                message=str(exc),
                warnings=list(self._warning_messages),
            )

        cost_basis = self._fetch_cost_settings(api_base, access_token, fallback_fee_side=float(fee_side))
        effective_fee_side = float(cost_basis.get("fee_side") or fee_side)
        effective_slippage = float(cost_basis.get("slippage") or 0.0)

        df_params = self.flatten_strategies_to_dataframe(strategies)
        if df_params.empty:
            return HolyGrailResult(
                ok=False,
                message="factor pool returned no checkpoint candidates",
                api_base=api_base,
                strategies_count=len(strategies),
                warnings=list(self._warning_messages),
            )
        if "metric_sharpe" not in df_params.columns:
            return HolyGrailResult(
                ok=False,
                message="factor pool payload is missing metric_sharpe",
                api_base=api_base,
                strategies_count=len(strategies),
                flattened_count=len(df_params),
                warnings=list(self._warning_messages),
            )

        (
            df_params,
            source_direction_counts,
            augmented_direction_counts,
            augment_diagnostics,
        ) = self._augment_directional_coverage(
            df_params,
            top_n=top_n_candidates,
        )
        pool_df = self._candidate_pool(df_params, top_n=top_n_candidates, max_per_group=3)
        if pool_df.empty:
            return HolyGrailResult(
                ok=False,
                message="candidate filter produced no unique strategies",
                api_base=api_base,
                strategies_count=len(strategies),
                flattened_count=len(df_params),
                diagnostics={
                    "source_direction_counts": source_direction_counts,
                    "augmented_direction_counts": augmented_direction_counts,
                    "mirror_diversified_source_count": int(augment_diagnostics.get("mirror_diversified_source_count") or 0),
                },
                warnings=list(self._warning_messages),
            )
        candidate_pool_direction_counts = self._direction_counts(pool_df)
        short_pool = pool_df[pool_df["direction"] == "short"].copy() if "direction" in pool_df.columns else pool_df.iloc[0:0].copy()
        candidate_pool_unique_short_groups = int(self._distinct_group_count(short_pool, direction="short"))
        candidate_pool_unique_short_timeframes = int(
            short_pool["timeframe_min"].nunique(dropna=True)
        ) if not short_pool.empty and "timeframe_min" in short_pool.columns else 0

        equity_curves: Dict[str, pd.Series] = {}
        detailed_results_cache: Dict[str, Dict[str, Any]] = {}
        candidate_records: List[Dict[str, Any]] = []
        standard_params = {
            "strategy_id",
            "symbol",
            "timeframe_min",
            "pool_name",
            "cand_score",
            "family",
            "param_tp",
            "param_sl",
            "param_max_hold",
        }

        success_count = 0
        for idx, row in pool_df.iterrows():
            symbol = str(row.get("symbol") or "").strip()
            timeframe_min = int(row.get("timeframe_min") or 15)
            family = str(row.get("family") or "Unknown").strip()
            if not symbol or not family:
                continue

            df_kline = self.load_kline_data(symbol, timeframe_min)
            if df_kline is None or df_kline.empty:
                continue

            tp, sl, max_hold, reverse_mode, family_params = self._family_payload_from_row(row, standard_params)
            try:
                res = self.bt.run_backtest(
                    df=df_kline,
                    family=family,
                    family_params=family_params,
                    tp_pct=tp,
                    sl_pct=sl,
                    max_hold=max_hold,
                    fee_side=effective_fee_side,
                    slippage=effective_slippage,
                    reverse_mode=reverse_mode,
                )
            except Exception as exc:
                self.warn_once(
                    f"backtest:{family}:{normalize_symbol(symbol)}:{timeframe_min}:{idx}",
                    "[HolyGrail] backtest failed for "
                    f"{family} ({normalize_symbol(symbol)} {timeframe_min_to_label(timeframe_min)}): {exc}",
                )
                continue

            if not res:
                continue

            trades = list(res.get("trades_detail") or [])
            if not trades:
                continue

            daily_eq = self.build_daily_equity_curve(trades)
            if daily_eq.empty:
                continue

            success_count += 1
            curve_key = f"{success_count:03d}_{family}_{normalize_symbol(symbol)}"
            equity_curves[curve_key] = daily_eq
            pool_df.at[idx, "curve_key"] = curve_key
            detailed_results_cache[curve_key] = dict(res)
            direction = normalize_direction(reverse=reverse_mode, default="long")
            trade_signature_hash, behavior_hash_type = self._trade_signature_hash(
                trades,
                symbol=symbol,
                direction=direction,
            )
            equity_signature_hash, _ = self._equity_signature_hash(daily_eq)
            if not trade_signature_hash:
                trade_signature_hash = equity_signature_hash
                behavior_hash_type = "equity"

            candidate_records.append(
                {
                    "curve_key": curve_key,
                    "row_index": int(idx),
                    "row_data": row.to_dict(),
                    "perf": dict(res),
                    "daily_equity": daily_eq,
                    "trades": trades,
                    "direction": direction,
                    "trade_signature_hash": trade_signature_hash,
                    "equity_signature_hash": equity_signature_hash,
                    "behavior_hash_type": behavior_hash_type,
                }
            )

        if not equity_curves:
            return HolyGrailResult(
                ok=False,
                message="backtests did not produce any equity curves",
                api_base=api_base,
                strategies_count=len(strategies),
                flattened_count=len(df_params),
                candidate_count=len(pool_df),
                diagnostics={
                    "source_direction_counts": source_direction_counts,
                    "augmented_direction_counts": augmented_direction_counts,
                    "candidate_pool_direction_counts": candidate_pool_direction_counts,
                },
                warnings=list(self._warning_messages),
            )

        backtested_direction_counts: Dict[str, int] = {"long": 0, "short": 0}
        for candidate in candidate_records:
            direction = normalize_direction(candidate.get("direction"), default="long")
            backtested_direction_counts[direction] = int(backtested_direction_counts.get(direction) or 0) + 1

        equity_df = pd.DataFrame(equity_curves).ffill().fillna(1.0)
        returns_df = equity_df.pct_change().dropna(how="all").fillna(0.0)
        corr_matrix = returns_df.corr().fillna(0.0) if not returns_df.empty else pd.DataFrame()
        duplicate_groups: Dict[str, List[Dict[str, Any]]] = {}
        for candidate in candidate_records:
            signature = str(candidate.get("trade_signature_hash") or candidate.get("equity_signature_hash") or "")
            if not signature:
                signature = str(candidate.get("curve_key") or "")
            duplicate_groups.setdefault(signature, []).append(candidate)

        selected_representatives: Dict[str, Dict[str, Any]] = {}
        duplicate_group_keys = sorted(duplicate_groups.keys())
        for group_idx, group_key in enumerate(duplicate_group_keys, start=1):
            group_members = sorted(duplicate_groups[group_key], key=self._sort_candidate_key)
            duplicate_group_id = f"D{group_idx:03d}"
            duplicate_group_size = len(group_members)
            for duplicate_rank, candidate in enumerate(group_members, start=1):
                candidate["duplicate_group_id"] = duplicate_group_id
                candidate["duplicate_group_size"] = int(duplicate_group_size)
                candidate["duplicate_rank"] = int(duplicate_rank)
                candidate["selection_status"] = "rejected_duplicate" if duplicate_rank > 1 else "candidate"
                candidate["selection_reject_reason"] = "behavior_duplicate" if duplicate_rank > 1 else ""
                candidate["selected_rank"] = None
                candidate["avg_pairwise_corr_to_selected"] = 0.0
                candidate["max_pairwise_corr_to_selected"] = 0.0
            selected_representatives[group_members[0]["curve_key"]] = group_members[0]

        representative_candidates = sorted(selected_representatives.values(), key=self._sort_candidate_key)
        for candidate in representative_candidates:
            candidate["selected_pair_rank"] = None
            candidate["selected_direction_rank"] = None

        def _corr_stats(curve_key: str, against_keys: List[str]) -> Tuple[float, float]:
            if not against_keys:
                return 0.0, 0.0
            pairwise_corrs = [
                abs(self._corr_value(corr_matrix, curve_key, selected_key))
                for selected_key in against_keys
            ]
            if not pairwise_corrs:
                return 0.0, 0.0
            return float(sum(pairwise_corrs) / len(pairwise_corrs)), float(max(pairwise_corrs))

        eligible_by_direction: Dict[str, List[Dict[str, Any]]] = {"long": [], "short": []}
        for candidate in representative_candidates:
            perf = candidate.get("perf") or {}
            if float(perf.get("sharpe") or 0.0) <= 0.0 or float(perf.get("cagr_pct") or 0.0) <= 0.0:
                candidate["selection_status"] = "rejected_performance"
                candidate["selection_reject_reason"] = "non_positive_performance"
                continue
            direction = normalize_direction(candidate.get("direction"), default="long")
            candidate["selection_status"] = "candidate"
            candidate["selection_reject_reason"] = ""
            eligible_by_direction.setdefault(direction, []).append(candidate)
        eligible_direction_counts = {
            "long": len(eligible_by_direction.get("long") or []),
            "short": len(eligible_by_direction.get("short") or []),
        }

        max_pairs = max(0, min(int(max_selected or 0) // 2, 10))
        selected_curve_keys: List[str] = []
        selected_counts_by_direction: Dict[str, int] = {"long": 0, "short": 0}
        pair_count = 0
        selected_pairs, feasible_pair_edges = self._select_balanced_pairing(
            long_candidates=eligible_by_direction.get("long") or [],
            short_candidates=eligible_by_direction.get("short") or [],
            corr_matrix=corr_matrix,
            corr_threshold=float(corr_threshold),
            max_pairs=max_pairs,
        )

        for pair_rank, (long_candidate, short_candidate) in enumerate(selected_pairs, start=1):
            for selected_candidate, direction in (
                (long_candidate, "long"),
                (short_candidate, "short"),
            ):
                curve_key = str(selected_candidate.get("curve_key") or "")
                avg_corr, max_corr = _corr_stats(curve_key, selected_curve_keys)
                selected_curve_keys.append(curve_key)
                selected_counts_by_direction[direction] = int(selected_counts_by_direction.get(direction, 0)) + 1
                selected_candidate["selection_status"] = "selected"
                selected_candidate["selection_reject_reason"] = ""
                selected_candidate["selected_rank"] = len(selected_curve_keys)
                selected_candidate["selected_pair_rank"] = int(pair_rank)
                selected_candidate["selected_direction_rank"] = int(selected_counts_by_direction[direction])
                selected_candidate["avg_pairwise_corr_to_selected"] = float(avg_corr)
                selected_candidate["max_pairwise_corr_to_selected"] = float(max_corr)
            pair_count += 1

        def _has_feasible_opposite_pair(candidate: Dict[str, Any]) -> bool:
            direction = normalize_direction(candidate.get("direction"), default="long")
            opposite_direction = "short" if direction == "long" else "long"
            curve_key = str(candidate.get("curve_key") or "")
            avg_corr, max_corr = _corr_stats(curve_key, selected_curve_keys)
            if max_corr > float(corr_threshold):
                return False
            for opposite_candidate in representative_candidates:
                if opposite_candidate is candidate:
                    continue
                if normalize_direction(opposite_candidate.get("direction"), default="long") != opposite_direction:
                    continue
                if str(opposite_candidate.get("selection_status") or "") == "rejected_performance":
                    continue
                opposite_curve_key = str(opposite_candidate.get("curve_key") or "")
                if not opposite_curve_key:
                    continue
                if abs(self._corr_value(corr_matrix, curve_key, opposite_curve_key)) > float(corr_threshold):
                    continue
                if str(opposite_candidate.get("selection_status") or "") == "selected":
                    return True
                opposite_against = list(selected_curve_keys) + [curve_key]
                _opp_avg_corr, opp_max_corr = _corr_stats(opposite_curve_key, opposite_against)
                if opp_max_corr <= float(corr_threshold):
                    return True
            return False

        for candidate in representative_candidates:
            if str(candidate.get("selection_status") or "") != "candidate":
                continue
            curve_key = str(candidate.get("curve_key") or "")
            avg_corr, max_corr = _corr_stats(curve_key, selected_curve_keys)
            candidate["avg_pairwise_corr_to_selected"] = avg_corr
            candidate["max_pairwise_corr_to_selected"] = max_corr
            if len(selected_curve_keys) >= max_pairs * 2:
                candidate["selection_status"] = "rejected_capacity"
                candidate["selection_reject_reason"] = "balanced_selection_limit_reached"
            elif max_corr > float(corr_threshold):
                candidate["selection_status"] = "rejected_corr"
                candidate["selection_reject_reason"] = f"pairwise_corr>{float(corr_threshold):.4f}"
            elif not _has_feasible_opposite_pair(candidate):
                candidate["selection_status"] = "rejected_corr"
                candidate["selection_reject_reason"] = "no_feasible_pair_under_corr"
            else:
                candidate["selection_status"] = "rejected_balance"
                candidate["selection_reject_reason"] = "no_balanced_pair_available"

        sharpe_dict = {
            curve_key: max(0.0, float((detailed_results_cache.get(curve_key) or {}).get("sharpe") or 0.0))
            for curve_key in selected_curve_keys
        }
        total_positive_sharpe = sum(sharpe_dict.values())
        weights: Dict[str, float] = {}
        if selected_curve_keys:
            if total_positive_sharpe > 0:
                for curve_key in selected_curve_keys:
                    weights[curve_key] = (sharpe_dict[curve_key] / total_positive_sharpe) * (float(base_stake_pct) / 100.0)
            else:
                even_weight = (float(base_stake_pct) / 100.0) / len(selected_curve_keys)
                for curve_key in selected_curve_keys:
                    weights[curve_key] = even_weight

        portfolio_daily_ret = pd.Series(dtype=float)
        for curve_key in selected_curve_keys:
            strat_ret = equity_curves[curve_key].pct_change().fillna(0.0)
            weighted = strat_ret * float(weights.get(curve_key, 0.0))
            if portfolio_daily_ret.empty:
                portfolio_daily_ret = weighted
            else:
                portfolio_daily_ret = portfolio_daily_ret.add(weighted, fill_value=0.0)
        portfolio_daily_ret = portfolio_daily_ret.dropna().sort_index()

        if portfolio_daily_ret.empty:
            portfolio_equity = pd.Series(dtype=float)
        else:
            portfolio_equity = (1.0 + portfolio_daily_ret).cumprod()

        total_return_pct = float((portfolio_equity.iloc[-1] - 1.0) * 100.0) if not portfolio_equity.empty else 0.0
        days = int((portfolio_daily_ret.index[-1] - portfolio_daily_ret.index[0]).days) if len(portfolio_daily_ret) > 1 else 1
        years = max(days / 365.25, 0.001)
        cagr_pct = (
            float((portfolio_equity.iloc[-1] ** (1.0 / years) - 1.0) * 100.0)
            if not portfolio_equity.empty and float(portfolio_equity.iloc[-1]) > 0
            else 0.0
        )
        peak = portfolio_equity.cummax() if not portfolio_equity.empty else pd.Series(dtype=float)
        drawdown = (portfolio_equity - peak) / peak if not portfolio_equity.empty else pd.Series(dtype=float)
        max_drawdown_pct = float(abs(drawdown.min()) * 100.0) if not drawdown.empty else 0.0
        mean_ret = float(portfolio_daily_ret.mean()) if not portfolio_daily_ret.empty else 0.0
        std_ret = float(portfolio_daily_ret.std()) if not portfolio_daily_ret.empty else 0.0
        sharpe = float(mean_ret / std_ret * np.sqrt(365.25)) if std_ret > 0 else 0.0
        dn_ret = portfolio_daily_ret[portfolio_daily_ret < 0]
        dn_std = float(dn_ret.std()) if not dn_ret.empty else 0.0
        sortino = float(mean_ret / dn_std * np.sqrt(365.25)) if dn_std > 0 else 0.0
        calmar = float(cagr_pct / max_drawdown_pct) if max_drawdown_pct > 0 else 0.0

        combined_trades: List[Dict[str, Any]] = []
        for curve_key in selected_curve_keys:
            candidate = next((item for item in candidate_records if item.get("curve_key") == curve_key), {})
            trades = list((candidate.get("perf") or {}).get("trades_detail") or [])
            weight = float(weights.get(curve_key, 0.0))
            for trade in trades:
                net_ret = trade.get("net_return")
                if net_ret is None:
                    net_ret = trade.get("net_ret", 0.0)
                net_ret = float(net_ret or 0.0)
                contribution = net_ret * weight
                trade_row = dict(trade)
                trade_row["strategy_key"] = curve_key
                trade_row["direction"] = str(candidate.get("direction") or "")
                trade_row["weight_pct"] = round(weight * 100.0, 4)
                trade_row["strategy_return_pct"] = round(net_ret * 100.0, 6)
                trade_row["portfolio_contribution_pct"] = round(contribution * 100.0, 6)
                combined_trades.append(trade_row)

        def _parse_ts(value: Any) -> pd.Timestamp:
            return pd.to_datetime(value, utc=True, errors="coerce")

        combined_trades.sort(key=lambda row: _parse_ts(row.get("entry_ts") or row.get("entry_time")))
        contributions = np.array([float(row.get("portfolio_contribution_pct") or 0.0) / 100.0 for row in combined_trades])
        wins = contributions[contributions > 0]
        losses = contributions[contributions <= 0]
        win_rate_pct = float(len(wins) / len(contributions) * 100.0) if len(contributions) else 0.0
        avg_win_pct = float(wins.mean() * 100.0) if len(wins) else 0.0
        avg_loss_pct = float(losses.mean() * 100.0) if len(losses) else 0.0
        profit_factor = float(wins.sum() / abs(losses.sum())) if abs(losses.sum()) > 0 else 0.0
        payoff = float(avg_win_pct / abs(avg_loss_pct)) if abs(avg_loss_pct) > 0 else 0.0

        selected_portfolio: List[Dict[str, Any]] = []
        multi_payload: List[Dict[str, Any]] = []
        final_rows: List[Dict[str, Any]] = []
        for candidate in sorted(candidate_records, key=self._sort_candidate_key):
            curve_key = str(candidate.get("curve_key") or "")
            row_data = pd.Series(candidate.get("row_data") or {})
            perf = candidate.get("perf") or {}
            rank = candidate.get("selected_rank")
            direction = normalize_direction(candidate.get("direction"), default="long")
            interval = timeframe_interval_string(int(row_data.get("timeframe_min") or 15))
            family_params = {
                str(key).replace("param_", "", 1): self._normalize_scalar(value)
                for key, value in row_data.items()
                if str(key).startswith("param_")
                and str(key) not in {"param_tp", "param_sl", "param_max_hold", "param_reverse_mode"}
                and pd.notna(value)
            }

            final_row = {
                "rank": int(rank or 0),
                "strategy_key": curve_key,
                "family": str(row_data.get("family") or ""),
                "symbol": str(row_data.get("symbol") or ""),
                "timeframe_min": int(row_data.get("timeframe_min") or 0),
                "interval": interval,
                "direction": direction,
                "candidate_score": round(float(row_data.get("cand_score") or 0.0), 6),
                "avg_corr_to_portfolio": round(float(candidate.get("avg_pairwise_corr_to_selected") or 0.0), 6),
                "avg_pairwise_corr_to_selected": round(float(candidate.get("avg_pairwise_corr_to_selected") or 0.0), 6),
                "max_pairwise_corr_to_selected": round(float(candidate.get("max_pairwise_corr_to_selected") or 0.0), 6),
                "duplicate_group_id": str(candidate.get("duplicate_group_id") or ""),
                "duplicate_group_size": int(candidate.get("duplicate_group_size") or 0),
                "duplicate_rank": int(candidate.get("duplicate_rank") or 0),
                "behavior_hash_type": str(candidate.get("behavior_hash_type") or ""),
                "trade_signature_hash": str(candidate.get("trade_signature_hash") or ""),
                "equity_signature_hash": str(candidate.get("equity_signature_hash") or ""),
                "candidate_source": str(row_data.get("candidate_source") or ""),
                "has_metrics": bool(row_data.get("has_metrics")),
                "direction_bucket": str(row_data.get("direction_bucket") or ""),
                "preselect_rank": int(row_data.get("preselect_rank") or 0),
                "selection_status": str(candidate.get("selection_status") or ""),
                "selection_reject_reason": str(candidate.get("selection_reject_reason") or ""),
                "selected_pair_rank": int(candidate.get("selected_pair_rank") or 0),
                "selected_direction_rank": int(candidate.get("selected_direction_rank") or 0),
                "weight_pct": round(float(weights.get(curve_key, 0.0)) * 100.0, 6),
                "sharpe": float(perf.get("sharpe") or 0.0),
                "sortino": float(perf.get("sortino") or 0.0),
                "calmar": float(perf.get("calmar") or 0.0),
                "cagr_pct": float(perf.get("cagr_pct") or 0.0),
                "total_return_pct": float(perf.get("total_return_pct") or 0.0),
                "max_drawdown_pct": float(perf.get("max_drawdown_pct") or 0.0),
                "trades": int(perf.get("trades") or 0),
                "win_rate_pct": float(perf.get("win_rate_pct") or 0.0),
                "payoff": float(perf.get("payoff") or 0.0),
                "profit_factor": float(perf.get("profit_factor") or 0.0),
                "avg_win_pct": float(perf.get("avg_win_pct") or 0.0),
                "avg_loss_pct": float(perf.get("avg_loss_pct") or 0.0),
                "expectancy_pct": float(perf.get("expectancy_pct") or 0.0),
                "avg_hold_bars": float(perf.get("avg_hold_bars") or 0.0),
                "time_in_market_pct": float(perf.get("time_in_market_pct") or 0.0),
                "tp_pct_raw": float(row_data.get("param_tp") or 0.0),
                "sl_pct_raw": float(row_data.get("param_sl") or 0.0),
                "max_hold": int(row_data.get("param_max_hold") or 0),
                "family_params_json": json.dumps(family_params, ensure_ascii=False),
            }
            final_rows.append(final_row)
            if curve_key in selected_curve_keys:
                selected_portfolio.append(final_row)
                multi_payload.append(
                    {
                        "strategy_id": int(row_data.get("strategy_id") or 0),
                        "family": str(row_data.get("family") or ""),
                        "family_params": family_params,
                        "direction": direction,
                        "tp_pct": float(row_data.get("param_tp") or 0.0) * 100.0,
                        "sl_pct": float(row_data.get("param_sl") or 0.0) * 100.0,
                        "max_hold": int(row_data.get("param_max_hold") or 0),
                        "stake_pct": round(float(weights.get(curve_key, 0.0)) * 100.0, 4),
                        "symbol": normalize_symbol(str(row_data.get("symbol") or "")).replace("_", ""),
                        "interval": interval,
                        "sharpe": float(perf.get("sharpe") or 0.0),
                        "total_return_pct": float(perf.get("total_return_pct") or 0.0),
                        "max_drawdown_pct": float(perf.get("max_drawdown_pct") or 0.0),
                        "selected_rank": int(rank or 0),
                        "candidate_source": str(row_data.get("candidate_source") or ""),
                        "direction_bucket": str(row_data.get("direction_bucket") or ""),
                        "preselect_rank": int(row_data.get("preselect_rank") or 0),
                        "avg_pairwise_corr_to_selected": round(float(candidate.get("avg_pairwise_corr_to_selected") or 0.0), 6),
                        "max_pairwise_corr_to_selected": round(float(candidate.get("max_pairwise_corr_to_selected") or 0.0), 6),
                        "duplicate_group_id": str(candidate.get("duplicate_group_id") or ""),
                        "duplicate_group_size": int(candidate.get("duplicate_group_size") or 0),
                    }
                )

        selected_portfolio.sort(key=lambda row: (int(row.get("rank") or 0), str(row.get("strategy_key") or "")))
        multi_payload.sort(key=lambda row: (int(row.get("selected_rank") or 0), str(row.get("symbol") or "")))

        summary_rows = [
            {
                "portfolio_name": "Top 20 Holy Grail Portfolio",
                "base_stake_pct": float(base_stake_pct),
                "selected_strategies": len(selected_curve_keys),
                "selected_long_strategies": int(selected_counts_by_direction.get("long") or 0),
                "selected_short_strategies": int(selected_counts_by_direction.get("short") or 0),
                "selected_pairs": int(pair_count),
                "backtested_strategies": len(candidate_records),
                "unique_behavior_groups": len(duplicate_groups),
                "source_long_candidates": int(source_direction_counts.get("long") or 0),
                "source_short_candidates": int(source_direction_counts.get("short") or 0),
                "augmented_long_candidates": int(augmented_direction_counts.get("long") or 0),
                "augmented_short_candidates": int(augmented_direction_counts.get("short") or 0),
                "mirror_diversified_source_count": int(augment_diagnostics.get("mirror_diversified_source_count") or 0),
                "augmented_short_distinct_groups": int(augment_diagnostics.get("augmented_short_distinct_groups") or 0),
                "candidate_pool_long": int(candidate_pool_direction_counts.get("long") or 0),
                "candidate_pool_short": int(candidate_pool_direction_counts.get("short") or 0),
                "candidate_pool_unique_short_groups": int(candidate_pool_unique_short_groups),
                "candidate_pool_unique_short_timeframes": int(candidate_pool_unique_short_timeframes),
                "backtested_long": int(backtested_direction_counts.get("long") or 0),
                "backtested_short": int(backtested_direction_counts.get("short") or 0),
                "eligible_long": int(eligible_direction_counts.get("long") or 0),
                "eligible_short": int(eligible_direction_counts.get("short") or 0),
                "balanced_pair_feasible_edges": int(feasible_pair_edges),
                "balanced_pair_count_max_possible": int(len(selected_pairs)),
                "sharpe": round(sharpe, 6),
                "sortino": round(sortino, 6),
                "calmar": round(calmar, 6),
                "cagr_pct": round(cagr_pct, 6),
                "total_return_pct": round(total_return_pct, 6),
                "max_drawdown_pct": round(max_drawdown_pct, 6),
                "trades": len(combined_trades),
                "win_rate_pct": round(win_rate_pct, 6),
                "payoff": round(payoff, 6),
                "profit_factor": round(profit_factor, 6),
                "avg_win_pct": round(avg_win_pct, 6),
                "avg_loss_pct": round(avg_loss_pct, 6),
            }
        ]

        report_paths = self._export_reports(
            final_rows=final_rows,
            summary_rows=summary_rows,
            trades_rows=combined_trades,
        )

        result_ok = bool(selected_curve_keys)
        empty_result_reason = ""
        if not result_ok:
            if int(candidate_pool_direction_counts.get("long") or 0) == 0 or int(candidate_pool_direction_counts.get("short") or 0) == 0:
                empty_result_reason = "one_sided_candidate_pool"
            elif int(backtested_direction_counts.get("long") or 0) == 0 or int(backtested_direction_counts.get("short") or 0) == 0:
                empty_result_reason = "one_sided_backtest_pool"
            elif int(eligible_direction_counts.get("long") or 0) == 0 or int(eligible_direction_counts.get("short") or 0) == 0:
                empty_result_reason = "one_sided_eligible_pool"
            else:
                empty_result_reason = "no_feasible_balanced_pair"
            self.warn_once(
                f"holy-grail-empty-selection:{empty_result_reason}:{candidate_pool_direction_counts.get('long', 0)}:{candidate_pool_direction_counts.get('short', 0)}",
                "[HolyGrail] no publishable balanced portfolio was produced; "
                f"source(long={int(source_direction_counts.get('long') or 0)}, short={int(source_direction_counts.get('short') or 0)}), "
                f"augmented(long={int(augmented_direction_counts.get('long') or 0)}, short={int(augmented_direction_counts.get('short') or 0)}), "
                f"backtested(long={int(backtested_direction_counts.get('long') or 0)}, short={int(backtested_direction_counts.get('short') or 0)}), "
                f"eligible(long={int(eligible_direction_counts.get('long') or 0)}, short={int(eligible_direction_counts.get('short') or 0)}).",
            )
        result_message = (
            f"selected {len(selected_curve_keys)} balanced strategies"
            if result_ok
            else (
                "no publishable balanced portfolio was produced "
                f"(reason={empty_result_reason}; "
                f"source long={int(source_direction_counts.get('long') or 0)} short={int(source_direction_counts.get('short') or 0)}, "
                f"augmented long={int(augmented_direction_counts.get('long') or 0)} short={int(augmented_direction_counts.get('short') or 0)}, "
                f"backtested long={int(backtested_direction_counts.get('long') or 0)} short={int(backtested_direction_counts.get('short') or 0)}, "
                f"eligible long={int(eligible_direction_counts.get('long') or 0)} short={int(eligible_direction_counts.get('short') or 0)})"
            )
        )

        return HolyGrailResult(
            ok=result_ok,
            message=result_message,
            api_base=api_base,
            strategies_count=len(strategies),
            flattened_count=len(df_params),
            candidate_count=len(pool_df),
            backtested_count=len(equity_curves),
            selected_count=len(selected_curve_keys),
            selected_portfolio=selected_portfolio,
            portfolio_metrics={
                "sharpe": round(sharpe, 6),
                "sortino": round(sortino, 6),
                "calmar": round(calmar, 6),
                "cagr_pct": round(cagr_pct, 6),
                "total_return_pct": round(total_return_pct, 6),
                "max_drawdown_pct": round(max_drawdown_pct, 6),
                "win_rate_pct": round(win_rate_pct, 6),
                "payoff": round(payoff, 6),
                "profit_factor": round(profit_factor, 6),
                "avg_win_pct": round(avg_win_pct, 6),
                "avg_loss_pct": round(avg_loss_pct, 6),
                "trades": len(combined_trades),
            },
            weights={key: round(value, 8) for key, value in weights.items()},
            multi_payload=multi_payload,
            multi_strategies_json=json.dumps(multi_payload, ensure_ascii=False, indent=2),
            report_paths=report_paths,
            cost_basis=cost_basis,
            warnings=list(self._warning_messages),
            diagnostics={
                "project_root": str(project_root()),
                "selected_curve_keys": selected_curve_keys,
                "selected_long_count": int(selected_counts_by_direction.get("long") or 0),
                "selected_short_count": int(selected_counts_by_direction.get("short") or 0),
                "selected_pair_count": int(pair_count),
                "corr_threshold": float(corr_threshold),
                "duplicate_groups": len(duplicate_groups),
                "cost_basis": cost_basis,
                "source_direction_counts": source_direction_counts,
                "augmented_direction_counts": augmented_direction_counts,
                "mirror_diversified_source_count": int(augment_diagnostics.get("mirror_diversified_source_count") or 0),
                "augmented_short_distinct_groups": int(augment_diagnostics.get("augmented_short_distinct_groups") or 0),
                "candidate_pool_direction_counts": candidate_pool_direction_counts,
                "candidate_pool_unique_short_groups": int(candidate_pool_unique_short_groups),
                "candidate_pool_unique_short_timeframes": int(candidate_pool_unique_short_timeframes),
                "backtested_direction_counts": backtested_direction_counts,
                "eligible_direction_counts": eligible_direction_counts,
                "balanced_pair_feasible_edges": int(feasible_pair_edges),
                "balanced_pair_count_max_possible": int(len(selected_pairs)),
                "empty_result_reason": empty_result_reason,
            },
        )


def run_holy_grail_build(
    *,
    bt_module: Any,
    log: Optional[Callable[[str], None]] = None,
    factor_pool_url: Optional[str] = None,
    factor_pool_token: Optional[str] = None,
    factor_pool_user: Optional[str] = None,
    factor_pool_pass: Optional[str] = None,
    years: int = 3,
    base_stake_pct: float = 95.0,
    top_n_candidates: int = 150,
    max_selected: int = 20,
    corr_threshold: float = 0.4,
    fee_side: float = 0.0006,
) -> HolyGrailResult:
    runtime = HolyGrailRuntime(
        bt_module=bt_module,
        log=log,
        factor_pool_url=factor_pool_url,
        factor_pool_token=factor_pool_token,
        factor_pool_user=factor_pool_user,
        factor_pool_pass=factor_pool_pass,
        years=years,
    )
    try:
        return runtime.build_portfolio(
            base_stake_pct=base_stake_pct,
            top_n_candidates=top_n_candidates,
            max_selected=max_selected,
            corr_threshold=corr_threshold,
            fee_side=fee_side,
        )
    except Exception as exc:
        trace = traceback.format_exc()
        if log is not None:
            log(f"[HolyGrail] runtime crashed: {exc}\n{trace}")
        return HolyGrailResult(
            ok=False,
            message=str(exc),
            warnings=list(runtime._warning_messages),
            diagnostics={"traceback": trace},
        )
