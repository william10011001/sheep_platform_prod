from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

try:
    import psutil
except Exception:
    psutil = None

import sheep_platform_db as db
from sheep_runtime_paths import ensure_parent, runtime_dir
from sheep_secrets import redact_value

from .config import load_effective_config
from . import runtime_legacy as legacy


REALTIME_CONTROL_SETTING_KEY = "realtime_control_v1"
REALTIME_STATUS_FILENAME = "realtime_daemon_status.json"


def _utc_iso(ts: Optional[float] = None) -> str:
    return datetime.fromtimestamp(float(ts or time.time()), tz=timezone.utc).isoformat()


def realtime_status_path() -> Path:
    return ensure_parent(runtime_dir() / REALTIME_STATUS_FILENAME)


def read_realtime_status() -> Dict[str, Any]:
    path = realtime_status_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return dict(data or {}) if isinstance(data, dict) else {}
    except Exception:
        return {}


def write_realtime_status(payload: Dict[str, Any]) -> None:
    path = realtime_status_path()
    path.write_text(json.dumps(dict(payload or {}), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def read_realtime_control() -> Dict[str, Any]:
    payload = db.get_setting(REALTIME_CONTROL_SETTING_KEY, {}) or {}
    if not isinstance(payload, dict):
        payload = {}
    return {
        "desired_state": str(payload.get("desired_state") or "stopped"),
        "mode": str(payload.get("mode") or "shadow"),
        "reason": str(payload.get("reason") or ""),
        "updated_at": str(payload.get("updated_at") or ""),
        "requested_by": int(payload.get("requested_by") or 0),
    }


def write_realtime_control(*, desired_state: str, mode: str, reason: str = "", requested_by: int = 0) -> Dict[str, Any]:
    payload = {
        "desired_state": str(desired_state or "stopped"),
        "mode": str(mode or "shadow"),
        "reason": str(reason or ""),
        "updated_at": _utc_iso(),
        "requested_by": int(requested_by or 0),
    }
    db.set_setting(REALTIME_CONTROL_SETTING_KEY, payload)
    return payload


class _Var:
    def __init__(self, value: Any = None) -> None:
        self._value = value

    def get(self) -> Any:
        return self._value

    def set(self, value: Any) -> None:
        self._value = value


class _TextBuffer:
    def __init__(self, value: str = "") -> None:
        self._value = str(value or "")

    def get(self, *_args: Any) -> str:
        return self._value

    def delete(self, *_args: Any) -> None:
        self._value = ""

    def insert(self, *_args: Any) -> None:
        if len(_args) >= 2:
            self._value = str(_args[1] or "")

    def config(self, **_kwargs: Any) -> None:
        return


class HeadlessUIBridge:
    def __init__(self, config: Dict[str, Any]) -> None:
        cfg = dict(config or {})
        self._ui_perf_state = {
            "animation_mode": "paused",
            "ui_perf_degraded": True,
            "last_drain_ms": 0.0,
            "last_backlog": 0,
        }
        self.global_stake_pct_var = _Var(float(cfg.get("global_stake_pct") or 95.0))
        self.factor_pool_url_var = _Var(str(cfg.get("factor_pool_url") or "https://sheep123.com"))
        self.factor_pool_token_var = _Var(str(cfg.get("factor_pool_token") or ""))
        self.factor_pool_user_var = _Var(str(cfg.get("factor_pool_user") or ""))
        self.factor_pool_pass_var = _Var(str(cfg.get("factor_pool_pass") or ""))
        self.telegram_enabled_var = _Var(bool(cfg.get("telegram_enabled")))
        self.telegram_bot_token_var = _Var(str(cfg.get("telegram_bot_token") or ""))
        self.telegram_chat_id_var = _Var(str(cfg.get("telegram_chat_id") or ""))
        self.telegram_scope_var = _Var(str(cfg.get("telegram_scope") or legacy.DEFAULT_TELEGRAM_SCOPE))
        self.telegram_dedupe_sec_var = _Var(int(cfg.get("telegram_dedupe_sec") or legacy.DEFAULT_TELEGRAM_DEDUPE_SEC))
        self.multi_json_text = _TextBuffer(str(cfg.get("multi_strategies_json") or ""))
        self.active_trader = None
        self.factor_updater = None

    def configure_sidecars(self) -> None:
        legacy.telegram_notifier.configure(
            enabled=bool(self.telegram_enabled_var.get()),
            bot_token=str(self.telegram_bot_token_var.get() or "").strip(),
            chat_id=str(self.telegram_chat_id_var.get() or "").strip(),
            dedupe_sec=max(60, int(self.telegram_dedupe_sec_var.get() or legacy.DEFAULT_TELEGRAM_DEDUPE_SEC)),
            scope=str(self.telegram_scope_var.get() or legacy.DEFAULT_TELEGRAM_SCOPE).strip() or legacy.DEFAULT_TELEGRAM_SCOPE,
        )

    def update_multi_json(self, new_json_str: str, wait: bool = False) -> bool:
        del wait
        try:
            self.multi_json_text.config(state="normal")
            self.multi_json_text.delete("1.0", legacy.tk.END)
            self.multi_json_text.insert("1.0", str(new_json_str or ""))
            self.multi_json_text.config(state="disabled")
            trader = self.active_trader
            if trader is not None:
                multi_json = json.loads(str(new_json_str or "[]"))
                new_strat_cfg, normalized_sym_ivs = legacy.normalize_multi_strategy_entries(
                    multi_json,
                    trader.global_symbol,
                    trader.global_interval,
                    5.0,
                )
                applied = bool(
                    trader.apply_runtime_strategy_config(
                        new_strat_cfg,
                        normalized_sym_ivs,
                        source="daemon_hot_reload",
                    )
                )
                if not applied:
                    raise RuntimeError("daemon hot reload returned false")
            return True
        except Exception as exc:
            legacy.log(f"【HeadlessUI】更新 runtime JSON 失敗: {exc}")
            return False


class RealtimeService:
    def __init__(self, *, initial_mode: str = "shadow") -> None:
        self.initial_mode = "live" if str(initial_mode or "").strip().lower() == "live" else "shadow"
        self.current_mode = "stopped"
        self.current_state = "stopped"
        self.current_reason = "startup"
        self.started_at = time.time()
        self._next_start_retry_at = 0.0
        self._start_retry_delay_s = 15.0
        self._config_issues: list[str] = []
        self.ui: Optional[HeadlessUIBridge] = None
        self.trader = None
        self.factor_updater = None
        self._trader_thread: Optional[threading.Thread] = None
        self._running = False
        self._log_drain_thread: Optional[threading.Thread] = None

    def _drain_logs_forever(self) -> None:
        while self._running:
            try:
                legacy.log_q.get(timeout=1.0)
            except Exception:
                continue

    def _resource_summary(self) -> Dict[str, Any]:
        if psutil is None:
            return {
                "rss_mb": 0.0,
                "vms_mb": 0.0,
                "cpu_percent": 0.0,
                "thread_count": int(threading.active_count()),
                "pid": int(os.getpid()),
            }
        process = psutil.Process()
        with process.oneshot():
            mem = process.memory_info()
            return {
                "rss_mb": round(float(mem.rss) / (1024 * 1024), 2),
                "vms_mb": round(float(mem.vms) / (1024 * 1024), 2),
                "cpu_percent": round(float(process.cpu_percent(interval=None) or 0.0), 2),
                "thread_count": int(process.num_threads()),
                "pid": int(process.pid),
            }

    def _runtime_sync_status(self) -> Dict[str, Any]:
        updater = self.factor_updater
        if updater is None:
            return {}
        out: Dict[str, Any] = {}
        for scope in ("personal", "global"):
            meta = dict((updater._runtime_sync_success_meta or {}).get(scope) or {})
            ts = float(meta.get("ts") or 0.0)
            out[scope] = {
                "ok": bool((updater._runtime_sync_success or {}).get(scope)),
                "last_success_at": _utc_iso(ts) if ts > 0 else "",
                "auth_mode": str(meta.get("auth_mode") or ""),
                "reason": str(meta.get("reason") or ""),
                "age_s": max(0.0, time.time() - ts) if ts > 0 else None,
            }
        return out

    def _symbol_state_items(self) -> list[Dict[str, Any]]:
        trader = self.trader
        if trader is None:
            return []
        try:
            items = list(trader.collect_symbol_state_items() or [])
        except Exception:
            return []
        for item in items:
            actual_qty = float(item.get("actual_qty") or 0.0)
            target_qty = float(item.get("target_qty") or 0.0)
            item["delta_qty"] = round(target_qty - actual_qty, 8)
        return items

    def _status_payload(self) -> Dict[str, Any]:
        control = read_realtime_control()
        updater = self.factor_updater
        diagnostics = {}
        if updater is not None:
            diagnostics = dict(getattr(updater, "_last_holy_grail_diagnostics", {}) or {})
        return {
            "ok": self.current_state == "running",
            "state": self.current_state,
            "mode": self.current_mode,
            "desired_state": str(control.get("desired_state") or "stopped"),
            "desired_mode": str(control.get("mode") or "shadow"),
            "last_heartbeat_at": _utc_iso(),
            "started_at": _utc_iso(self.started_at),
            "shadow_orders_blocked": self.current_mode == "shadow",
            "last_round_ms": float(getattr(updater, "_last_round_ms", 0.0) or 0.0) if updater is not None else 0.0,
            "last_round_started_at": str(diagnostics.get("round_started_at") or ""),
            "last_round_finished_at": str(diagnostics.get("round_finished_at") or ""),
            "resource_summary": self._resource_summary(),
            "runtime_sync": self._runtime_sync_status(),
            "symbol_state_items": self._symbol_state_items(),
            "holy_grail_diagnostics": diagnostics,
            "control": control,
            "reason": self.current_reason,
            "config_issues": list(self._config_issues),
        }

    def _write_status(self) -> None:
        write_realtime_status(redact_value(self._status_payload()))

    def _make_runtime_config(self, mode: str) -> Dict[str, Any]:
        cfg = load_effective_config()
        cfg["dry_run"] = bool(str(mode or "").strip().lower() != "live")
        cfg["realtime_mode"] = "live" if not cfg["dry_run"] else "shadow"
        issues = []
        if not str(cfg.get("symbol") or "").strip():
            issues.append("missing_symbol")
        if not str(cfg.get("interval") or "").strip():
            issues.append("missing_interval")
        if not str(cfg.get("factor_pool_url") or "").strip():
            issues.append("missing_factor_pool_url")
        if not (str(cfg.get("factor_pool_token") or "").strip() or (str(cfg.get("factor_pool_user") or "").strip() and str(cfg.get("factor_pool_pass") or "").strip())):
            issues.append("missing_factor_pool_auth")
        if not bool(cfg.get("dry_run")):
            if not str(cfg.get("api_key") or "").strip():
                issues.append("missing_api_key")
            if not str(cfg.get("secret") or "").strip():
                issues.append("missing_secret")
            if not str(cfg.get("memo") or "").strip():
                issues.append("missing_memo")
        self._config_issues = issues
        return cfg

    def _start_components(self, mode: str) -> None:
        cfg = self._make_runtime_config(mode)
        os.environ["SHEEP_REALTIME_MODE"] = str(cfg.get("realtime_mode") or "shadow")
        self.ui = HeadlessUIBridge(cfg)
        self.ui.configure_sidecars()
        legacy.stop_event.clear()
        client = legacy.BitmartClient(
            cfg.get("api_key", ""),
            cfg.get("secret", ""),
            cfg.get("memo", ""),
            cfg.get("trade_base", ""),
            cfg.get("quote_base", ""),
            timeout=cfg.get("timeout", 15),
            retries=cfg.get("retries", 3),
            retry_sleep=0.8,
            dry_run=bool(cfg.get("dry_run")),
        )
        self.trader = legacy.Trader(client, cfg)
        self.ui.active_trader = self.trader
        self.factor_updater = legacy.FactorPoolUpdater(self.ui)
        self.ui.factor_updater = self.factor_updater
        self.factor_updater.attach_trader(self.trader)
        self.factor_updater.start()
        self._trader_thread = threading.Thread(target=self.trader.run, name="sheep-realtime-trader", daemon=True)
        self._trader_thread.start()
        self.current_mode = str(mode or "shadow")
        self.current_state = "running"
        self.current_reason = f"started_{self.current_mode}"
        self._next_start_retry_at = 0.0

    def _stop_components(self, reason: str) -> None:
        legacy.stop_event.set()
        if self.factor_updater is not None:
            self.factor_updater.running = False
        if self._trader_thread is not None:
            self._trader_thread.join(timeout=15.0)
        self.trader = None
        self.factor_updater = None
        self.ui = None
        self._trader_thread = None
        self.current_mode = "stopped"
        self.current_state = "stopped"
        self.current_reason = str(reason or "stopped")

    def run_forever(self) -> None:
        self._running = True
        if self._log_drain_thread is None:
            self._log_drain_thread = threading.Thread(target=self._drain_logs_forever, name="sheep-realtime-log-drain", daemon=True)
            self._log_drain_thread.start()
        control = read_realtime_control()
        if not control.get("updated_at"):
            write_realtime_control(desired_state="running", mode=self.initial_mode, reason="daemon_boot")
        try:
            while self._running:
                control = read_realtime_control()
                desired_state = str(control.get("desired_state") or "stopped").lower()
                desired_mode = str(control.get("mode") or self.initial_mode).lower() or self.initial_mode
                if desired_state == "running":
                    if self.current_state != "running":
                        if time.time() >= float(self._next_start_retry_at or 0.0):
                            try:
                                self._start_components(desired_mode)
                            except Exception as exc:
                                try:
                                    self._stop_components("startup_failed_cleanup")
                                except Exception:
                                    self.trader = None
                                    self.factor_updater = None
                                    self.ui = None
                                    self._trader_thread = None
                                self.current_mode = str(desired_mode or "shadow")
                                self.current_state = "degraded"
                                self.current_reason = f"start_failed:{type(exc).__name__}:{exc}"
                                self._next_start_retry_at = time.time() + float(self._start_retry_delay_s)
                                legacy.log(
                                    f"【RealtimeDaemon】啟動失敗，{int(self._start_retry_delay_s)} 秒後重試: {exc}"
                                )
                    elif desired_mode != self.current_mode:
                        self._stop_components("mode_switch")
                        self._start_components(desired_mode)
                else:
                    if self.current_state == "running":
                        self._stop_components("control_stop")
                self._write_status()
                time.sleep(3.0)
        finally:
            if self.current_state == "running":
                self._stop_components("shutdown")
            self._write_status()
            self._running = False


def run_healthcheck(max_age_s: int = 45) -> int:
    status = read_realtime_status()
    if not status:
        return 1
    last_heartbeat_at = str(status.get("last_heartbeat_at") or "")
    if not last_heartbeat_at:
        return 1
    try:
        last_ts = datetime.fromisoformat(last_heartbeat_at).timestamp()
    except Exception:
        return 1
    desired_state = str(status.get("desired_state") or "stopped").lower()
    if desired_state == "stopped":
        return 0
    return 0 if (time.time() - last_ts) <= int(max_age_s) else 1
