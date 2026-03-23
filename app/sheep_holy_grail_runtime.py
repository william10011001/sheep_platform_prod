from __future__ import annotations

import json
import math
import os
import time
import traceback
import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import urllib3

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


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
    warnings: List[str] = field(default_factory=list)
    diagnostics: Dict[str, Any] = field(default_factory=dict)


class HolyGrailRuntime:
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
                res = requests.get(test_url, verify=False, timeout=5)
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
        resp = requests.post(login_url, json=payload, verify=False, timeout=15)
        if resp.status_code != 200:
            raise RuntimeError(f"factor-pool login failed ({resp.status_code}): {resp.text}")
        token = str((resp.json() or {}).get("token") or "").strip()
        if not token:
            raise RuntimeError("factor-pool login succeeded but token was empty")
        return token

    @staticmethod
    def _factor_pool_headers(token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _fetch_factor_pool_pages(self, api_base: str, token: str) -> List[Dict[str, Any]]:
        strategies_url = f"{api_base}/admin/strategies"
        collected: List[Dict[str, Any]] = []
        page = 1
        page_size = 200
        while True:
            resp = None
            last_error = ""
            for attempt in range(3):
                resp = requests.get(
                    strategies_url,
                    params={"page": page, "page_size": page_size},
                    headers=self._factor_pool_headers(token),
                    verify=False,
                    timeout=60,
                )
                if resp.status_code == 200:
                    break
                last_error = f"factor-pool fetch failed ({resp.status_code}): {resp.text}"
                if resp.status_code not in {429, 500, 502, 503, 504}:
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

    def fetch_factor_pool_data(self) -> Tuple[List[Dict[str, Any]], str]:
        host, token, user, password = self._factor_pool_creds()
        api_base = self.detect_api_base(host)
        attempts: List[str] = []

        if token:
            try:
                return self._fetch_factor_pool_pages(api_base, token), api_base
            except Exception as exc:
                attempts.append(str(exc))
                self.warn_once(
                    "factor-pool-token-fetch-failed",
                    f"[HolyGrail] factor pool token fetch failed, retrying with password auth: {exc}",
                )
                if not user or not password:
                    raise

        login_token = self._issue_factor_pool_token(api_base, user, password)
        try:
            return self._fetch_factor_pool_pages(api_base, login_token), api_base
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
                    params.get("direction"),
                    reverse=params.get("reverse"),
                    default=strat.get("direction") or "long",
                )
                row["param_reverse_mode"] = cand_direction == "short"
                for key, value in dict(params.get("family_params") or {}).items():
                    row[f"param_{key}"] = value
                for key, value in dict(cand.get("metrics") or {}).items():
                    row[f"metric_{key}"] = value
                rows.append(row)

        return pd.DataFrame(rows)

    def _candidate_pool(self, df_params: pd.DataFrame, *, top_n: int = 150, max_per_group: int = 3) -> pd.DataFrame:
        if df_params.empty:
            return df_params.copy()

        df_sorted = df_params.sort_values(by="metric_sharpe", ascending=False).reset_index(drop=True)
        selected_indices: List[int] = []
        group_counts: Dict[Tuple[str, str, str], int] = {}
        seen_params: set[str] = set()

        for idx, row in df_sorted.iterrows():
            family = str(row.get("family", "Unknown"))
            symbol = str(row.get("symbol", "Unknown"))
            direction = normalize_direction(reverse=row.get("param_reverse_mode"), default="long")
            group_key = (family, symbol, direction)

            param_dict = {
                key: value
                for key, value in row.items()
                if str(key).startswith("param_") and pd.notna(value)
            }
            param_fingerprint = f"{family}_{symbol}_{direction}_{json.dumps(param_dict, sort_keys=True, default=str)}"
            if param_fingerprint in seen_params:
                continue
            if group_counts.get(group_key, 0) >= max_per_group:
                continue

            selected_indices.append(int(idx))
            group_counts[group_key] = group_counts.get(group_key, 0) + 1
            seen_params.add(param_fingerprint)
            if len(selected_indices) >= int(top_n):
                break

        return df_sorted.loc[selected_indices].copy()

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
            strategies, api_base = self.fetch_factor_pool_data()
        except Exception as exc:
            return HolyGrailResult(
                ok=False,
                message=str(exc),
                warnings=list(self._warning_messages),
            )

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

        pool_df = self._candidate_pool(df_params, top_n=top_n_candidates, max_per_group=3)
        if pool_df.empty:
            return HolyGrailResult(
                ok=False,
                message="candidate filter produced no unique strategies",
                api_base=api_base,
                strategies_count=len(strategies),
                flattened_count=len(df_params),
                warnings=list(self._warning_messages),
            )

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
                    fee_side=fee_side,
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
                warnings=list(self._warning_messages),
            )

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
        selected_curve_keys: List[str] = []
        for candidate in representative_candidates:
            curve_key = str(candidate.get("curve_key") or "")
            perf = candidate.get("perf") or {}
            if float(perf.get("sharpe") or 0.0) <= 0.0 or float(perf.get("cagr_pct") or 0.0) <= 0.0:
                candidate["selection_status"] = "rejected_performance"
                candidate["selection_reject_reason"] = "non_positive_performance"
                continue
            if len(selected_curve_keys) >= int(max_selected):
                candidate["selection_status"] = "rejected_capacity"
                candidate["selection_reject_reason"] = "selection_limit_reached"
                continue

            pairwise_corrs = [
                abs(self._corr_value(corr_matrix, curve_key, selected_key))
                for selected_key in selected_curve_keys
            ]
            candidate["avg_pairwise_corr_to_selected"] = (
                float(sum(pairwise_corrs) / len(pairwise_corrs)) if pairwise_corrs else 0.0
            )
            candidate["max_pairwise_corr_to_selected"] = float(max(pairwise_corrs)) if pairwise_corrs else 0.0
            if pairwise_corrs and float(max(pairwise_corrs)) > float(corr_threshold):
                candidate["selection_status"] = "rejected_corr"
                candidate["selection_reject_reason"] = f"pairwise_corr>{float(corr_threshold):.4f}"
                continue

            selected_curve_keys.append(curve_key)
            candidate["selection_status"] = "selected"
            candidate["selection_reject_reason"] = ""
            candidate["selected_rank"] = len(selected_curve_keys)

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
                "selection_status": str(candidate.get("selection_status") or ""),
                "selection_reject_reason": str(candidate.get("selection_reject_reason") or ""),
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
                        "strategy_id": curve_key,
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
                        "avg_pairwise_corr_to_selected": round(float(candidate.get("avg_pairwise_corr_to_selected") or 0.0), 6),
                        "max_pairwise_corr_to_selected": round(float(candidate.get("max_pairwise_corr_to_selected") or 0.0), 6),
                        "duplicate_group_id": str(candidate.get("duplicate_group_id") or ""),
                        "duplicate_group_size": int(candidate.get("duplicate_group_size") or 0),
                    }
                )

        summary_rows = [
            {
                "portfolio_name": "Top 20 Holy Grail Portfolio",
                "base_stake_pct": float(base_stake_pct),
                "selected_strategies": len(selected_curve_keys),
                "backtested_strategies": len(candidate_records),
                "unique_behavior_groups": len(duplicate_groups),
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

        return HolyGrailResult(
            ok=True,
            message=f"selected {len(selected_curve_keys)} decorrelated strategies",
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
            warnings=list(self._warning_messages),
            diagnostics={
                "project_root": str(project_root()),
                "selected_curve_keys": selected_curve_keys,
                "corr_threshold": float(corr_threshold),
                "duplicate_groups": len(duplicate_groups),
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
