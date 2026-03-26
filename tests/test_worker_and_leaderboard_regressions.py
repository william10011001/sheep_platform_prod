import asyncio
import importlib.util
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


import sheep_worker_client


def _reset_db_module():
    sys.modules.pop("sheep_platform_db", None)


def _reset_api_module():
    sys.modules.pop("sheep_platform_api", None)


def _load_live_trader_module():
    script_path = ROOT / "實盤程式" / "實盤因子池下單程式.py"
    spec = importlib.util.spec_from_file_location("sheep_live_trader_test", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_worker_client_normalizes_legacy_public_base_urls():
    assert sheep_worker_client.normalize_api_base_url("https://sheep123.com/api") == "https://sheep123.com/sheep123"
    assert sheep_worker_client.normalize_api_base_url("https://sheep123.com/api/") == "https://sheep123.com/sheep123"
    assert sheep_worker_client.normalize_api_base_url("https://sheep123.com") == "https://sheep123.com/sheep123"
    assert sheep_worker_client.normalize_api_base_url("http://127.0.0.1:8000") == "http://127.0.0.1:8000"


def test_factor_pool_updater_pauses_cleanly_without_credentials(monkeypatch):
    monkeypatch.delenv("SHEEP_FACTOR_POOL_USER", raising=False)
    monkeypatch.delenv("SHEEP_FACTOR_POOL_PASS", raising=False)

    module = _load_live_trader_module()
    logs = []
    module.log = logs.append
    module.bt = object()

    def _unexpected_run(**kwargs):
        raise AssertionError("run_holy_grail_build should not be called without factor-pool credentials")

    module.run_holy_grail_build = _unexpected_run

    class _DummyVar:
        def __init__(self, value):
            self._value = value

        def get(self):
            return self._value

    class _DummyText:
        def get(self, *_args):
            return '[{"family":"TEMA_Cross","symbol":"ETHUSDT","interval":"30m"}]'

    class _DummyUI:
        global_stake_pct_var = _DummyVar(95.0)
        multi_json_text = _DummyText()

    updater = module.FactorPoolUpdater(_DummyUI())
    updater._build_holy_grail()

    assert any("未設定因子池帳密" in msg for msg in logs)
    assert not any("本輪更新失敗" in msg for msg in logs)


def test_factor_pool_updater_accepts_token_only_and_syncs_cached_snapshot_on_failure(monkeypatch, tmp_path):
    module = _load_live_trader_module()
    logs = []
    module.log = logs.append
    module.bt = type("_BT", (), {"NUMBA_OK": False})()
    module.STATE_FILE = str(tmp_path / "tema_rsi_state.json")

    build_calls = []
    sync_calls = []

    class _Result:
        ok = False
        message = "backtests did not produce any equity curves"
        warnings = []
        multi_payload = []
        portfolio_metrics = {}
        selected_count = 0
        candidate_count = 0
        backtested_count = 0
        report_paths = {}

    def _fake_build(**kwargs):
        build_calls.append(dict(kwargs))
        return _Result()

    class _SyncResp:
        status_code = 200
        headers = {"content-type": "application/json"}
        text = '{"ok": true}'

        def json(self):
            return {"ok": True, "snapshot": {"strategy_count": 1}}

    def _fake_http_request(_session, method, url, timeout=None, verify=None, json=None, headers=None, **_kwargs):
        if method == "GET":
            return _SyncResp()
        assert method == "POST"
        sync_calls.append({"url": url, "json": json, "headers": headers})
        return _SyncResp()

    module.run_holy_grail_build = _fake_build
    monkeypatch.setattr(module, "http_request", _fake_http_request)

    class _DummyVar:
        def __init__(self, value):
            self._value = value

        def get(self):
            return self._value

    class _DummyText:
        def get(self, *_args):
            return json.dumps(
                {
                    "schema_version": 1,
                    "strategies": [
                        {
                            "strategy_key": "cached-short",
                            "family": "TEMA_RSI",
                            "symbol": "BTCUSDT",
                            "direction": "short",
                            "interval": "4h",
                            "family_params": {"fast_len": 9, "slow_len": 55},
                            "tp_pct": 1.2,
                            "sl_pct": 0.8,
                            "max_hold_bars": 36,
                            "stake_pct": 30.0,
                            "enabled": True,
                        }
                    ],
                },
                ensure_ascii=False,
            )

    class _DummyUI:
        global_stake_pct_var = _DummyVar(95.0)
        multi_json_text = _DummyText()
        factor_pool_url_var = _DummyVar("https://example.com")
        factor_pool_token_var = _DummyVar("runtime-token")
        factor_pool_user_var = _DummyVar("")
        factor_pool_pass_var = _DummyVar("")

    updater = module.FactorPoolUpdater(_DummyUI())
    updater._build_holy_grail()

    assert build_calls, "token-only auth should still trigger Holy Grail build"
    assert build_calls[0]["factor_pool_token"] == "runtime-token"
    assert len(sync_calls) == 0
    assert any("保留上一版有效的對沖組合" in msg for msg in logs)
    assert any("cached runtime 快照缺少有效績效欄位" in msg for msg in logs)


def test_factor_pool_updater_refreshes_cached_runtime_snapshot_before_rebuild(monkeypatch, tmp_path):
    module = _load_live_trader_module()
    logs = []
    module.log = logs.append
    module.bt = type("_BT", (), {"NUMBA_OK": True})()
    module.STATE_FILE = str(tmp_path / "tema_rsi_state.json")

    sync_calls = []

    class _Result:
        ok = True
        message = ""
        warnings = []
        multi_payload = [
            {
                "strategy_key": "fresh-long",
                "family": "TEMA_RSI",
                "symbol": "BTCUSDT",
                "direction": "long",
                "interval": "4h",
                "family_params": {"fast_len": 9, "slow_len": 30},
                "tp_pct": 1.2,
                "sl_pct": 0.8,
                "max_hold_bars": 36,
                "stake_pct": 40.0,
                "enabled": True,
                "sharpe": 2.1,
            }
        ]
        portfolio_metrics = {"sharpe": 2.1, "cagr_pct": 25.0, "max_drawdown_pct": 4.0}
        selected_count = 1
        candidate_count = 5
        backtested_count = 5
        report_paths = {}
        multi_strategies_json = json.dumps({"schema_version": 1, "strategies": multi_payload}, ensure_ascii=False)

    def _fake_build(**kwargs):
        return _Result()

    class _SyncResp:
        status_code = 200
        headers = {"content-type": "application/json"}
        text = '{"ok": true}'

        def json(self):
            return {"ok": True, "snapshot": {"strategy_count": 1}}

    def _fake_http_request(_session, method, url, timeout=None, verify=None, json=None, headers=None, **_kwargs):
        if method == "GET":
            return _SyncResp()
        assert method == "POST"
        sync_calls.append({"url": url, "json": json, "headers": headers})
        return _SyncResp()

    module.run_holy_grail_build = _fake_build
    monkeypatch.setattr(module, "http_request", _fake_http_request)

    class _DummyVar:
        def __init__(self, value):
            self._value = value

        def get(self):
            return self._value

    class _DummyText:
        def get(self, *_args):
            return json.dumps(
                {
                    "schema_version": 1,
                    "strategies": [
                        {
                            "strategy_key": "cached-short",
                            "family": "TEMA_RSI",
                            "symbol": "ETHUSDT",
                            "direction": "short",
                            "interval": "1h",
                            "family_params": {"fast_len": 11, "slow_len": 44},
                            "tp_pct": 1.1,
                            "sl_pct": 0.7,
                            "max_hold_bars": 30,
                            "stake_pct": 25.0,
                            "enabled": True,
                        }
                    ],
                },
                ensure_ascii=False,
            )

    class _DummyUI:
        global_stake_pct_var = _DummyVar(95.0)
        multi_json_text = _DummyText()
        factor_pool_url_var = _DummyVar("https://example.com")
        factor_pool_token_var = _DummyVar("runtime-token")
        factor_pool_user_var = _DummyVar("")
        factor_pool_pass_var = _DummyVar("")

        def update_multi_json(self, _payload, wait=False):
            return True

    updater = module.FactorPoolUpdater(_DummyUI())
    updater._build_holy_grail()

    assert len(sync_calls) == 2
    assert sync_calls[0]["json"]["source"] == "holy_grail_runtime"
    assert sync_calls[1]["json"]["source"] == "holy_grail_runtime"
    assert sync_calls[0]["json"]["items"][0]["direction"] == "long"


def test_factor_pool_updater_recovers_bootstrap_with_persisted_runtime_cache(monkeypatch, tmp_path):
    module = _load_live_trader_module()
    logs = []
    module.log = logs.append
    module.bt = type("_BT", (), {"NUMBA_OK": True})()
    state_path = tmp_path / "tema_rsi_state.json"
    module.STATE_FILE = str(state_path)

    cached_items = [
        {
            "strategy_id": 501,
            "strategy_key": "cached-balanced-short",
            "family": "EMA_Cross",
            "symbol": "BTCUSDT",
            "direction": "short",
            "interval": "1h",
            "family_params": {"fast_len": 9, "slow_len": 30},
            "tp_pct": 1.0,
            "sl_pct": 0.8,
            "max_hold": 24,
            "stake_pct": 50.0,
            "enabled": True,
            "sharpe": 2.4,
            "total_return_pct": 45.0,
            "max_drawdown_pct": 6.0,
        }
    ]
    state_path.write_text(
        json.dumps(
            {
                "holy_grail_runtime_cache": {
                    "updated_at": "2026-03-25T01:00:00+00:00",
                    "multi_strategies_json": json.dumps({"schema_version": 1, "strategies": cached_items}, ensure_ascii=False),
                    "items": cached_items,
                    "summary": {
                        "selected_count": 1,
                        "candidate_count": 5,
                        "backtested_count": 5,
                        "portfolio_metrics": {"sharpe": 2.4, "cagr_pct": 18.0, "max_drawdown_pct": 6.0},
                    },
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    class _Result:
        ok = False
        message = "no publishable balanced portfolio was produced (reason=one_sided_eligible_pool)"
        warnings = []
        multi_payload = []
        portfolio_metrics = {}
        selected_count = 0
        candidate_count = 72
        backtested_count = 72
        report_paths = {}
        multi_strategies_json = "[]"

    sync_calls = []
    update_calls = []

    def _fake_build(**kwargs):
        return _Result()

    class _SyncResp:
        status_code = 200
        headers = {"content-type": "application/json"}
        text = '{"ok": true}'

        def json(self):
            return {"ok": True, "snapshot": {"strategy_count": 1}}

    def _fake_http_request(_session, method, url, timeout=None, verify=None, json=None, headers=None, **_kwargs):
        if method == "GET":
            return _SyncResp()
        assert method == "POST"
        sync_calls.append({"url": url, "json": json, "headers": headers})
        return _SyncResp()

    module.run_holy_grail_build = _fake_build
    monkeypatch.setattr(module, "http_request", _fake_http_request)

    class _DummyVar:
        def __init__(self, value):
            self._value = value

        def get(self):
            return self._value

    class _DummyText:
        def __init__(self, value):
            self._value = value

        def get(self, *_args):
            return self._value

    class _DummyUI:
        global_stake_pct_var = _DummyVar(95.0)
        multi_json_text = _DummyText("[]")
        factor_pool_url_var = _DummyVar("https://example.com")
        factor_pool_token_var = _DummyVar("runtime-token")
        factor_pool_user_var = _DummyVar("")
        factor_pool_pass_var = _DummyVar("")

        @staticmethod
        def update_multi_json(payload, wait=False):
            update_calls.append({"payload": payload, "wait": wait})
            return True

    updater = module.FactorPoolUpdater(_DummyUI())
    updater._build_holy_grail()

    assert updater.allow_new_entries()[0] is True
    assert updater._bootstrap_completed is True
    assert update_calls, "cached runtime should be restored locally before opening entries"
    assert any(str(call["json"].get("source") or "").startswith("holy_grail_cached_") for call in sync_calls)
    assert any("恢復新開倉" in msg for msg in logs)


def test_factor_pool_updater_bootstrap_uses_prior_global_sync_when_retry_sync_fails(tmp_path):
    module = _load_live_trader_module()
    logs = []
    module.log = logs.append
    module.bt = type("_BT", (), {"NUMBA_OK": True})()
    state_path = tmp_path / "tema_rsi_state.json"
    module.STATE_FILE = str(state_path)

    cached_items = [
        {
            "strategy_id": 601,
            "strategy_key": "cached-long",
            "family": "EMA_Cross",
            "symbol": "BTCUSDT",
            "direction": "long",
            "interval": "4h",
            "family_params": {"fast_len": 9, "slow_len": 30},
            "tp_pct": 1.0,
            "sl_pct": 0.8,
            "max_hold": 24,
            "stake_pct": 50.0,
            "enabled": True,
            "sharpe": 2.4,
            "total_return_pct": 45.0,
            "max_drawdown_pct": 6.0,
        }
    ]
    state_path.write_text(
        json.dumps(
            {
                "holy_grail_runtime_cache": {
                    "updated_at": "2026-03-25T01:00:00+00:00",
                    "multi_strategies_json": json.dumps({"schema_version": 1, "strategies": cached_items}, ensure_ascii=False),
                    "items": cached_items,
                    "summary": {
                        "selected_count": 1,
                        "candidate_count": 5,
                        "backtested_count": 5,
                        "portfolio_metrics": {"sharpe": 2.4, "cagr_pct": 18.0, "max_drawdown_pct": 6.0},
                    },
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    class _DummyVar:
        def __init__(self, value):
            self._value = value

        def get(self):
            return self._value

    class _DummyText:
        def __init__(self, value):
            self._value = value

        def get(self, *_args):
            return self._value

    class _DummyUI:
        global_stake_pct_var = _DummyVar(95.0)
        multi_json_text = _DummyText("[]")
        factor_pool_url_var = _DummyVar("https://example.com")
        factor_pool_token_var = _DummyVar("runtime-token")
        factor_pool_user_var = _DummyVar("")
        factor_pool_pass_var = _DummyVar("")

        @staticmethod
        def update_multi_json(_payload, wait=False):
            return True

    updater = module.FactorPoolUpdater(_DummyUI())
    updater._mark_runtime_sync_success("global")
    updater._post_runtime_snapshot = lambda scope, payload, runtime_kwargs: False

    ok = updater._bootstrap_with_cached_runtime(updater._factor_pool_runtime_kwargs(), reason="bootstrap_cached_runtime")

    assert ok is True
    assert updater._bootstrap_completed is True
    assert updater.allow_new_entries()[0] is True
    assert any("沿用本次啟動內已成功同步的上一版全域快照" in msg for msg in logs)


def test_factor_pool_updater_startup_cached_publish_runs_once_per_scope(monkeypatch, tmp_path):
    module = _load_live_trader_module()
    logs = []
    module.log = logs.append
    module.bt = type("_BT", (), {"NUMBA_OK": True})()
    module.STATE_FILE = str(tmp_path / "tema_rsi_state.json")

    cached_items = [
        {
            "strategy_id": 701,
            "strategy_key": "cached-long",
            "family": "EMA_Cross",
            "symbol": "BTCUSDT",
            "direction": "long",
            "interval": "4h",
            "family_params": {"fast_len": 9, "slow_len": 30},
            "tp_pct": 1.0,
            "sl_pct": 0.8,
            "max_hold": 24,
            "stake_pct": 50.0,
            "enabled": True,
            "sharpe": 2.6,
            "total_return_pct": 48.0,
            "max_drawdown_pct": 6.1,
        }
    ]
    cached_json = json.dumps(cached_items, ensure_ascii=False, indent=2)

    class _Result:
        ok = True
        message = "selected 1 balanced strategies"
        warnings = []
        multi_payload = list(cached_items)
        multi_strategies_json = cached_json
        portfolio_metrics = {"sharpe": 2.6, "cagr_pct": 20.0, "max_drawdown_pct": 6.1}
        selected_count = 1
        candidate_count = 4
        backtested_count = 4
        report_paths = {}
        cost_basis = {}

    class _DummyVar:
        def __init__(self, value):
            self._value = value

        def get(self):
            return self._value

    class _DummyText:
        def __init__(self, value):
            self._value = value

        def get(self, *_args):
            return self._value

    update_calls = []

    class _DummyUI:
        global_stake_pct_var = _DummyVar(95.0)
        multi_json_text = _DummyText("[]")
        factor_pool_url_var = _DummyVar("https://example.com")
        factor_pool_token_var = _DummyVar("runtime-token")
        factor_pool_user_var = _DummyVar("")
        factor_pool_pass_var = _DummyVar("")

        @staticmethod
        def update_multi_json(payload, wait=False):
            update_calls.append({"payload": payload, "wait": wait})
            return True

    module.run_holy_grail_build = lambda **_kwargs: _Result()
    updater = module.FactorPoolUpdater(_DummyUI())
    updater.last_good_snapshot = {
        "updated_at": "2026-03-26T00:00:00+00:00",
        "multi_strategies_json": cached_json,
        "items": list(cached_items),
        "summary": {
            "selected_count": 1,
            "candidate_count": 4,
            "backtested_count": 4,
            "portfolio_metrics": {"sharpe": 2.6, "cagr_pct": 20.0, "max_drawdown_pct": 6.1},
            "cost_basis": {},
        },
    }
    updater.last_good_json = cached_json
    updater._last_good_summary_checksum = updater._runtime_publish_summary_checksum(updater.last_good_snapshot["summary"])
    updater._last_good_publish_fingerprint = updater._runtime_publish_fingerprint(
        items=updater.last_good_snapshot["items"],
        summary=updater.last_good_snapshot["summary"],
    )
    updater._pending_cached_runtime_publish_reason = "startup"

    sync_calls = []

    def _fake_sync_cached(scope, runtime_kwargs, *, reason):
        sync_calls.append((scope, reason))
        updater._mark_runtime_sync_success(
            scope,
            payload=updater._cached_runtime_snapshot_payload(scope, reason=reason),
            auth_mode="token",
            reason=f"holy_grail_cached_{reason}",
        )
        return True

    updater._sync_cached_runtime_snapshot = _fake_sync_cached
    updater._sync_runtime_snapshot = lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unchanged publish should skip final runtime sync"))

    updater._build_holy_grail()

    assert sync_calls == [("personal", "startup"), ("global", "startup")]
    assert not update_calls
    assert updater.allow_new_entries()[0] is True
    assert any("略過熱更新與網站重送" in msg for msg in logs)


def test_factor_pool_updater_unchanged_result_skips_hot_reload_and_runtime_sync(monkeypatch, tmp_path):
    module = _load_live_trader_module()
    logs = []
    module.log = logs.append
    module.bt = type("_BT", (), {"NUMBA_OK": True})()
    module.STATE_FILE = str(tmp_path / "tema_rsi_state.json")

    cached_items = [
        {
            "strategy_id": 801,
            "strategy_key": "cached-short",
            "family": "TEMA_RSI",
            "symbol": "ETHUSDT",
            "direction": "short",
            "interval": "1h",
            "family_params": {"fast_len": 12, "slow_len": 50},
            "tp_pct": 1.3,
            "sl_pct": 0.9,
            "max_hold": 18,
            "stake_pct": 42.0,
            "enabled": True,
            "sharpe": 2.9,
            "total_return_pct": 52.0,
            "max_drawdown_pct": 7.4,
        }
    ]
    cached_json = json.dumps(cached_items, ensure_ascii=False, indent=2)

    class _Result:
        ok = True
        message = "selected 1 balanced strategies"
        warnings = []
        multi_payload = list(cached_items)
        multi_strategies_json = cached_json
        portfolio_metrics = {"sharpe": 2.9, "cagr_pct": 22.0, "max_drawdown_pct": 7.4}
        selected_count = 1
        candidate_count = 5
        backtested_count = 5
        report_paths = {}
        cost_basis = {}

    class _DummyVar:
        def __init__(self, value):
            self._value = value

        def get(self):
            return self._value

    class _DummyText:
        def __init__(self, value):
            self._value = value

        def get(self, *_args):
            return self._value

    update_calls = []

    class _DummyUI:
        global_stake_pct_var = _DummyVar(95.0)
        multi_json_text = _DummyText(cached_json)
        factor_pool_url_var = _DummyVar("https://example.com")
        factor_pool_token_var = _DummyVar("runtime-token")
        factor_pool_user_var = _DummyVar("")
        factor_pool_pass_var = _DummyVar("")

        @staticmethod
        def update_multi_json(payload, wait=False):
            update_calls.append({"payload": payload, "wait": wait})
            return True

    module.run_holy_grail_build = lambda **_kwargs: _Result()
    updater = module.FactorPoolUpdater(_DummyUI())
    updater.last_good_snapshot = {
        "updated_at": "2026-03-26T00:00:00+00:00",
        "multi_strategies_json": cached_json,
        "items": list(cached_items),
        "summary": {
            "selected_count": 1,
            "candidate_count": 5,
            "backtested_count": 5,
            "portfolio_metrics": {"sharpe": 2.9, "cagr_pct": 22.0, "max_drawdown_pct": 7.4},
            "cost_basis": {},
        },
    }
    updater.last_good_json = cached_json
    updater._last_good_summary_checksum = updater._runtime_publish_summary_checksum(updater.last_good_snapshot["summary"])
    updater._last_good_publish_fingerprint = updater._runtime_publish_fingerprint(
        items=updater.last_good_snapshot["items"],
        summary=updater.last_good_snapshot["summary"],
    )
    updater._bootstrap_completed = True
    updater._mark_runtime_sync_success("global")
    updater._set_entry_gate("ready", "global_runtime_synced")
    updater._sync_cached_runtime_snapshot = lambda *args, **kwargs: True
    updater._sync_runtime_snapshot = lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unchanged result should not POST runtime snapshot"))

    updater._build_holy_grail()

    assert not update_calls
    assert updater.allow_new_entries()[0] is True
    assert any("略過熱更新與網站重送" in msg for msg in logs)


def test_runtime_snapshot_post_retries_transient_failures(monkeypatch):
    module = _load_live_trader_module()
    logs = []
    module.log = logs.append

    class _DummyText:
        def get(self, *_args):
            return "[]"

    class _DummyUI:
        multi_json_text = _DummyText()

    updater = module.FactorPoolUpdater(_DummyUI())

    attempts = {"count": 0}

    class _Resp:
        def __init__(self, status_code, payload=None, text=""):
            self.status_code = status_code
            self.headers = {"content-type": "application/json"}
            self._payload = payload or {}
            self.text = text or json.dumps(self._payload, ensure_ascii=False)

        def json(self):
            return self._payload

    def _fake_http_request(_session, method, url, timeout=None, verify=None, json=None, headers=None, **_kwargs):
        if method == "GET":
            return _Resp(200, {"ok": True})
        assert method == "POST"
        attempts["count"] += 1
        if attempts["count"] < 3:
            return _Resp(502, {"ok": False, "detail": "bad gateway"})
        return _Resp(200, {"ok": True, "snapshot": {"strategy_count": 1}})

    monkeypatch.setattr(module, "http_request", _fake_http_request)
    monkeypatch.setattr(module.time, "sleep", lambda _s: None)

    ok = updater._post_runtime_snapshot(
        "global",
        {"scope": "global", "items": [], "summary": {}, "updated_at": "2026-03-25T00:00:00+00:00"},
        {
            "factor_pool_url": "https://example.com",
            "factor_pool_token": "runtime-token",
            "factor_pool_user": "",
            "factor_pool_pass": "",
        },
    )

    assert ok is True
    assert attempts["count"] == 3
    assert updater._has_runtime_sync_success("global") is True
    assert any("runtime 快照暫時失敗" in msg for msg in logs)


def test_cached_runtime_snapshot_deduplicates_recent_success(monkeypatch):
    module = _load_live_trader_module()
    logs = []
    module.log = logs.append

    class _DummyText:
        def get(self, *_args):
            return "[]"

    class _DummyUI:
        multi_json_text = _DummyText()

    updater = module.FactorPoolUpdater(_DummyUI())
    updater.last_good_snapshot = {
        "updated_at": "2026-03-25T00:00:00+00:00",
        "items": [
            {
                "strategy_id": 1,
                "strategy_key": "cached-long",
                "family": "EMA_Cross",
                "symbol": "BTCUSDT",
                "direction": "long",
                "interval": "4h",
                "stake_pct": 50.0,
                "sharpe": 2.4,
                "total_return_pct": 45.0,
                "max_drawdown_pct": 6.0,
            }
        ],
        "summary": {
            "selected_count": 1,
            "candidate_count": 4,
            "backtested_count": 4,
            "portfolio_metrics": {"sharpe": 2.4},
        },
    }
    updater.last_good_json = json.dumps(updater.last_good_snapshot["items"], ensure_ascii=False)
    monkeypatch.setattr(updater, "_collect_runtime_position_items", lambda: [])

    calls = []

    class _Resp:
        status_code = 200
        headers = {"content-type": "application/json"}
        text = '{"ok": true}'

        def json(self):
            return {"ok": True, "snapshot": {"strategy_count": 1}}

    def _fake_http_request(_session, method, url, timeout=None, verify=None, json=None, headers=None, **_kwargs):
        if method == "GET":
            return _Resp()
        assert method == "POST"
        calls.append({"url": url, "json": json, "headers": headers})
        return _Resp()

    monkeypatch.setattr(module, "http_request", _fake_http_request)

    runtime_kwargs = {
        "factor_pool_url": "https://example.com",
        "factor_pool_token": "runtime-token",
        "factor_pool_user": "",
        "factor_pool_pass": "",
    }
    assert updater._sync_cached_runtime_snapshot("global", runtime_kwargs, reason="startup") is True
    assert updater._sync_cached_runtime_snapshot("global", runtime_kwargs, reason="failure") is True
    assert len(calls) == 1
    assert any("略過重送" in msg for msg in logs)


def test_runtime_snapshot_html_405_retries_without_password_fallback(monkeypatch):
    module = _load_live_trader_module()
    logs = []
    module.log = logs.append

    class _DummyText:
        def get(self, *_args):
            return "[]"

    class _DummyUI:
        multi_json_text = _DummyText()

    updater = module.FactorPoolUpdater(_DummyUI())

    token_calls = {"count": 0}
    calls = []

    class _Resp:
        def __init__(self, status_code, *, text="", payload=None, content_type="text/html"):
            self.status_code = status_code
            self.text = text
            self._payload = payload or {}
            self.headers = {"content-type": content_type}

        def json(self):
            return self._payload

    def _fake_http_request(_session, method, url, timeout=None, verify=None, json=None, headers=None, **_kwargs):
        if method == "GET":
            return _Resp(200, payload={"ok": True}, content_type="application/json")
        assert method == "POST"
        calls.append({"url": url, "json": dict(json or {}), "headers": dict(headers or {})})
        if "Authorization" not in (headers or {}):
            raise AssertionError("password fallback should not be used for transient 405 html errors")
        token_calls["count"] += 1
        if token_calls["count"] < 3:
            return _Resp(
                405,
                text="<html><head><title>405 Not Allowed</title></head><body><center>nginx/1.25.5</center></body></html>",
            )
        return _Resp(200, payload={"ok": True, "snapshot": {"strategy_count": 1}}, content_type="application/json")

    monkeypatch.setattr(module, "http_request", _fake_http_request)
    monkeypatch.setattr(module.time, "sleep", lambda _s: None)

    ok = updater._post_runtime_snapshot(
        "global",
        {"scope": "global", "items": [], "summary": {}, "updated_at": "2026-03-25T00:00:00+00:00"},
        {
            "factor_pool_url": "https://example.com",
            "factor_pool_token": "runtime-token",
            "factor_pool_user": "user",
            "factor_pool_pass": "pass",
        },
    )

    assert ok is True
    assert token_calls["count"] == 3
    assert all("Authorization" in call["headers"] for call in calls)
    assert all("username" not in call["json"] for call in calls)
    assert any("HTTP 405" in msg for msg in logs)


def test_runtime_snapshot_token_401_falls_back_to_password(monkeypatch):
    module = _load_live_trader_module()
    logs = []
    module.log = logs.append

    class _DummyText:
        def get(self, *_args):
            return "[]"

    class _DummyUI:
        multi_json_text = _DummyText()

    updater = module.FactorPoolUpdater(_DummyUI())
    calls = []

    class _Resp:
        def __init__(self, status_code, *, payload=None, content_type="application/json"):
            self.status_code = status_code
            self._payload = payload or {}
            self.text = json.dumps(self._payload, ensure_ascii=False)
            self.headers = {"content-type": content_type}

        def json(self):
            return self._payload

    def _fake_http_request(_session, method, url, timeout=None, verify=None, json=None, headers=None, **_kwargs):
        if method == "GET":
            return _Resp(200, payload={"ok": True})
        assert method == "POST"
        calls.append({"json": dict(json or {}), "headers": dict(headers or {})})
        if "Authorization" in (headers or {}):
            return _Resp(401, payload={"ok": False, "detail": "unauthorized"})
        return _Resp(200, payload={"ok": True, "snapshot": {"strategy_count": 1}})

    monkeypatch.setattr(module, "http_request", _fake_http_request)

    ok = updater._post_runtime_snapshot(
        "global",
        {"scope": "global", "items": [], "summary": {}, "updated_at": "2026-03-25T00:00:00+00:00"},
        {
            "factor_pool_url": "https://example.com",
            "factor_pool_token": "runtime-token",
            "factor_pool_user": "user",
            "factor_pool_pass": "pass",
        },
    )

    assert ok is True
    assert len(calls) == 2
    assert "Authorization" in calls[0]["headers"]
    assert "Authorization" not in calls[1]["headers"]
    assert calls[1]["json"]["username"] == "user"
    assert any("改用帳密重試同步" in msg for msg in logs)


def test_factor_pool_updater_runtime_payload_includes_live_position_items():
    module = _load_live_trader_module()

    class _Result:
        multi_payload = [
            {
                "strategy_key": "alpha-long",
                "family": "TEMA_RSI",
                "symbol": "BTCUSDT",
                "direction": "long",
                "interval": "4h",
                "family_params": {"fast_len": 9, "slow_len": 30},
                "tp_pct": 1.2,
                "sl_pct": 0.8,
                "max_hold_bars": 36,
                "stake_pct": 40.0,
                "enabled": True,
            }
        ]
        portfolio_metrics = {"sharpe": 2.1}
        selected_count = 1
        candidate_count = 3
        backtested_count = 3
        report_paths = {}

    class _DummyContracts:
        def get_positions(self):
            return {
                "data": [
                    {
                        "positionId": "BTCUSDT:LONG",
                        "symbol": "BTCUSDT",
                        "positionAmt": 0.2,
                        "entryPrice": 100000.0,
                        "markPrice": 101250.0,
                        "margin": 4050.0,
                        "liquidationPrice": 80200.0,
                        "unrealizedPnl": 250.0,
                    }
                ]
            }

        def get_contract_size(self, _symbol):
            return 1.0

    class _DummyTrader:
        def __init__(self):
            self.c = _DummyContracts()
            self.positions = {
                "TEMA_RSI_alpha-long": {
                    "in_pos": "LONG",
                    "position_id": "BTCUSDT:LONG",
                    "entry_qty": 0.2,
                    "entry_avg": 100000.0,
                    "cfg": {
                        "strategy_key": "alpha-long",
                        "family": "TEMA_RSI",
                        "symbol": "BTCUSDT",
                        "direction": "long",
                        "interval": "4h",
                    },
                }
            }

        def _safe_get_mark_price(self, _symbol):
            return 101250.0

        def _safe_get_equity(self):
            return 10000.0

    class _DummyVar:
        def __init__(self, value):
            self._value = value

        def get(self):
            return self._value

    class _DummyText:
        def get(self, *_args):
            return "[]"

    class _DummyUI:
        global_stake_pct_var = _DummyVar(95.0)
        multi_json_text = _DummyText()
        active_trader = _DummyTrader()
        _ui_perf_state = {
            "last_backlog": 12,
            "last_drain_ms": 8.5,
            "animation_mode": "low",
            "ui_perf_degraded": True,
        }

    updater = module.FactorPoolUpdater(_DummyUI())
    payload = updater._runtime_sync_payload("global", _Result(), {})

    position_items = payload["summary"]["position_items"]
    assert len(position_items) == 1
    assert position_items[0]["strategy_key"] == "alpha-long"
    assert position_items[0]["symbol"] == "BTCUSDT"
    assert position_items[0]["direction"] == "long"
    assert position_items[0]["position_usdt"] > 0
    assert position_items[0]["unrealized_pnl_usdt"] > 0
    assert payload["summary"]["ui_perf"]["ui_animation_mode"] == "low"
    assert payload["summary"]["telegram_stats"]["telegram_sent"] >= 0


def test_live_trader_accepts_wrapped_multi_strategy_json():
    module = _load_live_trader_module()

    cfg, active_pairs = module.normalize_multi_strategy_entries(
        {
            "schema_version": 1,
            "strategies": [
                {
                    "strategy_key": "tema-long",
                    "family": "TEMA_Cross",
                    "symbol": "ETHUSDT",
                    "direction": "long",
                    "interval": "30m",
                    "family_params": {"fast_len": 12, "slow_len": 55},
                    "tp_pct": 1.2,
                    "sl_pct": 0.8,
                    "max_hold_bars": 36,
                    "stake_pct": 25.0,
                    "enabled": True,
                },
                {
                    "strategy_key": "tema-short",
                    "family": "TEMA_Cross",
                    "symbol": "BTCUSDT",
                    "direction": "short",
                    "interval": "1h",
                    "family_params": {"fast_len": 9, "slow_len": 44},
                    "tp_pct": 1.0,
                    "sl_pct": 0.7,
                    "max_hold_bars": 48,
                    "stake_pct": 20.0,
                    "enabled": True,
                },
            ],
        }
    )

    rows = list(cfg.values())
    assert [row["direction"] for row in rows] == ["long", "short"]
    assert rows[0]["strategy_key"] == "tema-long"
    assert rows[1]["interval"] == "1h"
    assert active_pairs == [("BTCUSDT", "1h"), ("ETHUSDT", "30m")]


def test_bitmart_client_place_order_accepts_scalar_order_id_payload():
    module = _load_live_trader_module()

    client = module.BitmartClient(
        api_key="k",
        secret="s",
        memo="m",
        trade_base="https://example.com",
        quote_base="https://example.com",
        dry_run=False,
    )

    client.get_contract_size = lambda _symbol: 1.0
    client._request = lambda method, path, params=None, signed=True: {"code": 1000, "data": "abc123"}

    result = client.place_order("INJUSDT", "BUY", "LONG", "MARKET", qty=9.3)

    assert result["code"] == "0"
    assert result["data"]["orderId"] == "abc123"


def test_trader_open_market_handles_scalar_order_id_response(monkeypatch):
    module = _load_live_trader_module()
    logs = []
    monkeypatch.setattr(module, "log", logs.append)

    trader = module.Trader.__new__(module.Trader)
    trader.strategies_cfg = {
        "TEMA_RSI_11951": {
            "symbol": "INJUSDT",
            "stake_pct": 13.5331,
        }
    }
    trader.max_retries = 1
    trader.max_retry_total_sec = 0.1
    trader.fee_bps = 2.0
    trader.slip_bps = 0.0
    trader._calc_qty_from_stake = lambda _sid: 9.3
    trader._safe_get_mark_price = lambda _symbol: 3.05
    trader._apply_cost_side = lambda _tag, px, _fee_bps, _slip_bps: px

    class _DummyClient:
        dry_run = True

        @staticmethod
        def _extract_order_id(payload):
            return "scalar-oid" if payload else ""

        @staticmethod
        def _preview_payload(payload, limit=320):
            return repr(payload)[:limit]

        def cancel_all_open_orders(self, _symbol):
            return {}

        def place_order(self, _symbol, _side_bm, _pos_side, _otype, qty=None):
            assert qty == 9.3
            return {"code": "0", "data": "scalar-oid"}

    trader.c = _DummyClient()

    position_id, px_ref, entry_avg, filled_qty, exec_delay = module.Trader.open_market(
        trader,
        "LONG",
        "TEMA_RSI_11951",
    )

    assert position_id == "INJUSDT:LONG"
    assert px_ref == 3.05
    assert entry_avg == 3.05
    assert filled_qty == 9.3
    assert exec_delay >= 0.0
    assert any("order_id=scalar-oid" in msg for msg in logs)
    assert not any("'str' object has no attribute 'get'" in msg for msg in logs)


def test_trader_startup_gate_blocks_new_entry_before_consuming_signal(monkeypatch):
    module = _load_live_trader_module()
    logs = []
    monkeypatch.setattr(module, "log", logs.append)

    class _Gate:
        @staticmethod
        def allow_new_entries():
            return False, "syncing:bootstrap_pending"

    trader = module.Trader.__new__(module.Trader)
    trader.entry_gate_controller = _Gate()
    trader._last_entry_gate_log_ts = 0.0
    trader._last_entry_gate_log_key = ""
    trader.arm_tp_sl_sid = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not arm tp/sl while gate is closed"))
    trader.open_market = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not attempt open while gate is closed"))

    pos_data = {"cfg": {"params": {}}, "last_attempted_bar_ts": None}
    active_signals = {"S1": (True, False, 123456789)}
    df = pd.DataFrame({"time": [1234567890000]})

    module.Trader._maybe_open_signal_entry(
        trader,
        "S1",
        pos_data,
        "INJUSDT",
        "1h",
        df,
        0,
        active_signals,
    )

    assert pos_data.get("last_attempted_bar_ts") is None
    assert any("bootstrap_pending" in msg for msg in logs)


def test_symbol_net_target_is_absolute_notional_qty():
    module = _load_live_trader_module()

    trader = module.Trader.__new__(module.Trader)
    trader.global_symbol = "BTCUSDT"
    trader.global_interval = "1h"
    trader.symbol_info = {"BTCUSDT": {"qty_step": 0.001, "min_qty": 0.001}}
    trader.system_leverage = 5.0
    trader.system_capital_usdt = 1000.0
    trader.state_lock = module.threading.RLock()
    trader.symbol_states = {}
    trader.positions = {
        "LONG_1": {
            "cfg": {"symbol": "BTCUSDT", "interval": "15m", "stake_pct": 20.0, "strategy_key": "long-1"},
            "desired_state": "long",
        },
        "SHORT_1": {
            "cfg": {"symbol": "BTCUSDT", "interval": "4h", "stake_pct": 10.0, "strategy_key": "short-1"},
            "desired_state": "short",
        },
    }

    state = module.Trader._recompute_symbol_target_locked(trader, "BTCUSDT", mark_price=50000.0, total_capital=1000.0)
    assert state["target_notional_usdt"] == pytest.approx(500.0)
    assert state["target_qty"] == pytest.approx(0.01)

    state_repeat = module.Trader._recompute_symbol_target_locked(trader, "BTCUSDT", mark_price=50000.0, total_capital=1000.0)
    assert state_repeat["target_notional_usdt"] == pytest.approx(500.0)
    assert state_repeat["target_qty"] == pytest.approx(0.01)


def test_apply_strategy_desired_state_dedupes_same_bar():
    module = _load_live_trader_module()

    trader = module.Trader.__new__(module.Trader)
    trader.state_lock = module.threading.RLock()
    trader.positions = {
        "S1": {
            "strategy_id": "S1",
            "strategy_key": "S1",
            "cfg": {"symbol": "BTCUSDT", "interval": "1h", "direction": "long", "stake_pct": 20.0},
            "symbol": "BTCUSDT",
            "interval": "1h",
            "configured_direction": "long",
            "desired_state": "flat",
            "last_closed_bar_ts": None,
            "last_transition_ts": None,
        }
    }
    dirty_calls = []
    trader._mark_symbol_dirty = lambda *args, **kwargs: dirty_calls.append((args, kwargs))
    trader._persist_symbol_executor_state = lambda: None

    changed_first = module.Trader._apply_strategy_desired_state(trader, "S1", "long", 123456)
    changed_dup = module.Trader._apply_strategy_desired_state(trader, "S1", "long", 123456)
    changed_next = module.Trader._apply_strategy_desired_state(trader, "S1", "flat", 123457)

    assert changed_first is True
    assert changed_dup is False
    assert changed_next is True
    assert len(dirty_calls) == 2
    assert dirty_calls[0][0][0] == "BTCUSDT"
    assert dirty_calls[0][0][3] == "long"
    assert dirty_calls[1][0][3] == "flat"


def test_symbol_net_reconcile_uses_delta_qty_and_flip_flow():
    module = _load_live_trader_module()

    trader = module.Trader.__new__(module.Trader)
    trader.symbol_info = {"BTCUSDT": {"qty_step": 0.001, "min_qty": 0.001}}

    opens = []
    closes = []
    actual_after_close = {"qty": 0.0}

    async def _open(symbol, position_side, qty):
        opens.append((symbol, position_side, qty))

    async def _close(symbol, position_side, qty):
        closes.append((symbol, position_side, qty))
        actual_after_close["qty"] = 0.0

    trader._submit_open_qty_async = _open
    trader._submit_reduce_qty_async = _close
    trader._refresh_symbol_actual_state = lambda _symbol: {"actual_qty": actual_after_close["qty"]}

    ok, err = asyncio.run(module.Trader._drive_symbol_qty_to_target_async(trader, "BTCUSDT", 0.01, 0.015))
    assert ok is True
    assert err == ""
    assert closes == []
    assert len(opens) == 1
    assert opens[0][0] == "BTCUSDT"
    assert opens[0][1] == "LONG"
    assert opens[0][2] == pytest.approx(0.005)

    opens.clear()
    closes.clear()
    actual_after_close["qty"] = 0.0

    ok, err = asyncio.run(module.Trader._drive_symbol_qty_to_target_async(trader, "BTCUSDT", 0.01, -0.005))
    assert ok is True
    assert err == ""
    assert len(closes) == 1
    assert closes[0][0] == "BTCUSDT"
    assert closes[0][1] == "LONG"
    assert closes[0][2] == pytest.approx(0.01)
    assert len(opens) == 1
    assert opens[0][0] == "BTCUSDT"
    assert opens[0][1] == "SHORT"
    assert opens[0][2] == pytest.approx(0.005)


def test_symbol_net_runtime_position_items_are_symbol_level():
    module = _load_live_trader_module()

    trader = module.Trader.__new__(module.Trader)
    trader.state_lock = module.threading.RLock()
    trader.execution_mode = "symbol_net_executor"
    trader.symbol_states = {
        "BTCUSDT": {
            "symbol": "BTCUSDT",
            "actual_qty": 0.01,
            "actual_entry_price": 70000.0,
            "actual_mark_price": 70500.0,
            "actual_notional_usdt": 705.0,
            "actual_margin_usdt": 141.0,
            "actual_margin_ratio_pct": 3.2,
            "actual_liquidation_price": None,
            "actual_unrealized_pnl_usdt": 5.0,
            "actual_unrealized_pnl_roe_pct": 3.546099290780142,
            "target_qty": 0.015,
            "target_notional_usdt": 1057.5,
            "intervals": ["15m", "4h"],
            "pending_dirty": False,
            "offboarding": False,
        }
    }

    items = module.Trader.collect_runtime_position_items(trader)
    assert len(items) == 1
    item = items[0]
    assert item["strategy_key"] == "BTCUSDT"
    assert item["family"] == "SYMBOL_NET"
    assert item["position_qty"] == pytest.approx(0.01)
    assert item["target_qty"] == pytest.approx(0.015)
    assert item["target_notional_usdt"] == pytest.approx(1057.5)
    assert item["interval"] == "15m,4h"
    assert item["executor_mode"] == "symbol_net_executor"


def test_runtime_position_items_preserve_real_exchange_fields_and_null_missing_values(monkeypatch):
    module = _load_live_trader_module()

    class _DummyText:
        def get(self, *_args):
            return "[]"

    class _DummyUI:
        multi_json_text = _DummyText()

    updater = module.FactorPoolUpdater(_DummyUI())

    class _DummyClient:
        @staticmethod
        def get_contract_size(_symbol):
            return 1.0

        @staticmethod
        def get_positions():
            return {
                "data": [
                    {
                        "positionId": "INJUSDT:LONG",
                        "symbol": "INJUSDT",
                        "positionSide": "LONG",
                        "entryPrice": "3.05",
                        "markPrice": "3.12",
                        "positionAmt": "9.3",
                        "positionValue": "29.02",
                        "margin": "5.80",
                        "unrealizedPnl": "0.66",
                        "marginRatePct": "",
                        "liquidationPrice": "",
                        "raw": {},
                    }
                ]
            }

    class _DummyTrader:
        def __init__(self):
            self.c = _DummyClient()
            self.positions = {
                "STRAT_1": {
                    "cfg": {
                        "strategy_id": 101,
                        "family": "TEMA_RSI",
                        "symbol": "INJUSDT",
                        "direction": "long",
                        "interval": "4h",
                        "stake_pct": 13.5,
                    },
                    "position_id": "INJUSDT:LONG",
                    "in_pos": "LONG",
                    "entry_avg": 9.99,
                    "entry_qty": 9.3,
                }
            }

        @staticmethod
        def _safe_get_mark_price(_symbol):
            return 3.12

    updater.ui.active_trader = _DummyTrader()

    items = updater._collect_runtime_position_items()

    assert len(items) == 1
    item = items[0]
    assert item["entry_price"] == pytest.approx(3.05)
    assert item["mark_price"] == pytest.approx(3.12)
    assert item["position_usdt"] == pytest.approx(29.02)
    assert item["margin_usdt"] == pytest.approx(5.80)
    assert item["liquidation_price"] is None
    assert item["margin_ratio_pct"] is None
    assert item["unrealized_pnl_usdt"] == pytest.approx(0.66)
    assert item["unrealized_pnl_roe_pct"] == pytest.approx((0.66 / 5.80) * 100.0)
    assert item["unrealized_pnl_pct"] == pytest.approx((0.66 / 5.80) * 100.0)


def test_leaderboard_stats_include_time_and_points_fallback(monkeypatch, tmp_path):
    db_path = tmp_path / "leaderboard-regression.sqlite3"
    monkeypatch.setenv("SHEEP_DB_URL", "")
    monkeypatch.setenv("SHEEP_DB_PATH", str(db_path))
    _reset_db_module()
    import sheep_platform_db as db

    db.init_db()
    db.ensure_cycle_rollover()
    cycle = db.get_active_cycle()
    user = db.get_user_by_username("sheep")
    if user is None:
        db.create_user("sheep", "test-hash", role="user")
        user = db.get_user_by_username("sheep")
    assert cycle is not None
    assert user is not None

    pool_id = db.create_factor_pool(
        cycle_id=int(cycle["id"]),
        name="Leaderboard Pool",
        symbol="ETH_USDT",
        timeframe_min=60,
        years=2,
        family="trend",
        grid_spec={"alpha": [1, 2]},
        risk_spec={"max_leverage": 2},
        num_partitions=4,
        seed=11,
        active=True,
    )[0]

    now = db._now_iso()
    conn = db._conn()
    try:
        conn.execute(
            """
            INSERT INTO mining_tasks (
                user_id, pool_id, cycle_id, partition_idx, num_partitions,
                status, progress_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(user["id"]),
                int(pool_id),
                int(cycle["id"]),
                0,
                4,
                "completed",
                json.dumps({"combos_done": 321, "elapsed_s": 7200}),
                now,
                now,
            ),
        )
        strategy_cur = conn.execute(
            """
            INSERT INTO strategies (
                submission_id, user_id, pool_id, params_json, status,
                allocation_pct, note, created_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                int(user["id"]),
                int(pool_id),
                json.dumps({"family": "TEMA_Cross"}),
                "active",
                10.0,
                "leaderboard-fallback-test",
                now,
                "2099-12-31T23:59:59+00:00",
            ),
        )
        conn.commit()
        strategy_id = int(strategy_cur.lastrowid)
    finally:
        conn.close()

    db.set_setting("capital_usdt", 100000.0)
    db.set_setting("payout_rate", 0.2)
    db.create_weekly_check(
        strategy_id=strategy_id,
        week_start_ts=now,
        week_end_ts=now,
        return_pct=12.5,
        max_drawdown_pct=3.0,
        trades=20,
        eligible=True,
    )

    stats = db.get_leaderboard_stats(period_hours=720)

    assert stats["time"], stats
    assert float(stats["time"][0]["total_seconds"]) >= 7200.0
    assert stats["points"], stats
    assert round(float(stats["points"][0]["total_usdt"]), 4) == 250.0
    assert stats["qualified_strategies"], stats
    assert int(stats["qualified_strategies"][0]["active_strategy_count"]) >= 1


def test_leaderboard_points_fallback_uses_all_time_weekly_checks(monkeypatch, tmp_path):
    db_path = tmp_path / "leaderboard-alltime-regression.sqlite3"
    monkeypatch.setenv("SHEEP_DB_URL", "")
    monkeypatch.setenv("SHEEP_DB_PATH", str(db_path))
    _reset_db_module()
    import sheep_platform_db as db

    db.init_db()
    db.ensure_cycle_rollover()
    cycle = db.get_active_cycle()
    user = db.get_user_by_username("sheep")
    if user is None:
        db.create_user("sheep", "test-hash", role="user")
        user = db.get_user_by_username("sheep")
    assert cycle is not None
    assert user is not None

    pool_id = db.create_factor_pool(
        cycle_id=int(cycle["id"]),
        name="Leaderboard All-Time Pool",
        symbol="BTC_USDT",
        timeframe_min=60,
        years=2,
        family="trend",
        grid_spec={"alpha": [1, 2]},
        risk_spec={"max_leverage": 2},
        num_partitions=4,
        seed=13,
        active=True,
    )[0]

    now = db._now_iso()
    conn = db._conn()
    try:
        strategy_cur = conn.execute(
            """
            INSERT INTO strategies (
                submission_id, user_id, pool_id, params_json, status,
                allocation_pct, note, created_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                int(user["id"]),
                int(pool_id),
                json.dumps({"family": "TEMA_Cross"}),
                "active",
                10.0,
                "leaderboard-all-time-points-test",
                now,
                "2099-12-31T23:59:59+00:00",
            ),
        )
        conn.commit()
        strategy_id = int(strategy_cur.lastrowid)
    finally:
        conn.close()

    db.set_setting("capital_usdt", 100000.0)
    db.set_setting("payout_rate", 0.2)
    db.create_weekly_check(
        strategy_id=strategy_id,
        week_start_ts=now,
        week_end_ts=now,
        return_pct=12.5,
        max_drawdown_pct=3.0,
        trades=20,
        eligible=True,
    )
    old_checked_at = (datetime.now(timezone.utc) - timedelta(days=45)).isoformat()
    conn = db._conn()
    try:
        conn.execute("UPDATE weekly_checks SET checked_at = ? WHERE strategy_id = ?", (old_checked_at, int(strategy_id)))
        conn.commit()
    finally:
        conn.close()

    stats = db.get_leaderboard_stats(period_hours=24)

    assert stats["points"], stats
    assert round(float(stats["points"][0]["total_usdt"]), 4) == 250.0


def test_leaderboard_task_fallback_scans_multiple_pages(monkeypatch, tmp_path):
    db_path = tmp_path / "leaderboard-multipage-regression.sqlite3"
    monkeypatch.setenv("SHEEP_DB_URL", "")
    monkeypatch.setenv("SHEEP_DB_PATH", str(db_path))
    monkeypatch.setenv("SHEEP_LEADERBOARD_TASK_SCAN_LIMIT", "500")
    monkeypatch.setenv("SHEEP_LEADERBOARD_TASK_SCAN_MAX_ROWS", "2000")
    _reset_db_module()
    import sheep_platform_db as db

    db.init_db()
    db.ensure_cycle_rollover()
    cycle = db.get_active_cycle()
    user_a = db.get_user_by_username("sheep")
    if user_a is None:
        db.create_user("sheep", "test-hash", role="user")
        user_a = db.get_user_by_username("sheep")
    user_b = db.get_user_by_username("beta")
    if user_b is None:
        db.create_user("beta", "test-hash", role="user", nickname="Beta Miner")
        user_b = db.get_user_by_username("beta")
    assert cycle is not None and user_a is not None and user_b is not None

    pool_id = db.create_factor_pool(
        cycle_id=int(cycle["id"]),
        name="Leaderboard MultiPage Pool",
        symbol="BTC_USDT",
        timeframe_min=60,
        years=2,
        family="trend",
        grid_spec={"alpha": [1, 2]},
        risk_spec={"max_leverage": 2},
        num_partitions=4,
        seed=17,
        active=True,
    )[0]

    now = db._now_iso()
    conn = db._conn()
    try:
        for _ in range(505):
            conn.execute(
                """
                INSERT INTO mining_tasks (
                    user_id, pool_id, cycle_id, partition_idx, num_partitions,
                    status, progress_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(user_a["id"]),
                    int(pool_id),
                    int(cycle["id"]),
                    0,
                    4,
                    "completed",
                    json.dumps({"combos_done": 10, "elapsed_s": 60}),
                    now,
                    now,
                ),
            )
        conn.execute(
            """
            INSERT INTO mining_tasks (
                user_id, pool_id, cycle_id, partition_idx, num_partitions,
                status, progress_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(user_b["id"]),
                int(pool_id),
                int(cycle["id"]),
                1,
                4,
                "completed",
                json.dumps({"combos_done": 77, "elapsed_s": 3600}),
                now,
                now,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    stats = db.get_leaderboard_stats(period_hours=720)

    combos_users = {str(item.get("username") or "") for item in stats["combos"]}
    time_users = {str(item.get("username") or "") for item in stats["time"]}
    assert "sheep" in combos_users
    assert "beta" in combos_users
    assert "sheep" in time_users
    assert "beta" in time_users


def test_leaderboard_invalidation_is_throttled(monkeypatch):
    _reset_api_module()
    import sheep_platform_api as api

    now = 1_000_000.0
    monkeypatch.setattr(api.time, "time", lambda: now)
    api._live_cache["leaderboard"] = {
        "period_hours::720": {"ts": now, "value": {"combos": [{"username": "sheep"}]}}
    }
    api._live_versions["leaderboard"] = "old-version"
    api._live_last_invalidated_at["leaderboard"] = now

    api._invalidate_live_state("leaderboard")

    assert api._live_versions["leaderboard"] == "old-version"
    assert "period_hours::720" in api._live_cache["leaderboard"]

    now += 60.0
    api._invalidate_live_state("leaderboard")

    assert api._live_versions["leaderboard"] != "old-version"
    assert api._live_cache["leaderboard"] == {}


def test_postgres_leaderboard_prefers_recent_aggregate(monkeypatch, tmp_path):
    db_path = tmp_path / "leaderboard-postgres-path.sqlite3"
    monkeypatch.setenv("SHEEP_DB_URL", "")
    monkeypatch.setenv("SHEEP_DB_PATH", str(db_path))
    _reset_db_module()
    import sheep_platform_db as db

    db.init_db()
    db.ensure_cycle_rollover()
    real_conn = db._conn()

    monkeypatch.setattr(db, "_db_kind", lambda: "postgres")
    monkeypatch.setattr(db, "_conn", lambda: real_conn)
    monkeypatch.setattr(
        db,
        "_leaderboard_postgres_recent_agg",
        lambda conn, cutoff_iso, window_end_iso: {
            "combos": [{"username": "miner-a", "total_done": 1234.0}],
            "time": [{"username": "miner-a", "total_seconds": 4321.0}],
        },
    )
    monkeypatch.setattr(
        db,
        "_leaderboard_python_fallback",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("python fallback should not be used")),
    )

    stats = db.get_leaderboard_stats(period_hours=720)

    assert stats["combos"]
    assert stats["time"]
    assert stats["combos"][0]["username"] == "miner-a"
    assert stats["time"][0]["username"] == "miner-a"


def test_postgres_leaderboard_falls_back_when_aggregate_fails(monkeypatch, tmp_path):
    db_path = tmp_path / "leaderboard-postgres-fallback.sqlite3"
    monkeypatch.setenv("SHEEP_DB_URL", "")
    monkeypatch.setenv("SHEEP_DB_PATH", str(db_path))
    _reset_db_module()
    import sheep_platform_db as db

    db.init_db()
    db.ensure_cycle_rollover()
    real_conn = db._conn()

    monkeypatch.setattr(db, "_db_kind", lambda: "postgres")
    monkeypatch.setattr(db, "_conn", lambda: real_conn)
    monkeypatch.setattr(
        db,
        "_leaderboard_postgres_recent_agg",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("aggregate_timeout")),
    )
    monkeypatch.setattr(
        db,
        "_leaderboard_python_fallback",
        lambda conn, cutoff_iso, window_end_iso: {
            "combos": [{"username": "miner-b", "total_done": 987.0}],
            "time": [{"username": "miner-b", "total_seconds": 654.0}],
        },
    )

    stats = db.get_leaderboard_stats(period_hours=720)

    assert stats["combos"][0]["username"] == "miner-b"
    assert stats["time"][0]["username"] == "miner-b"


def test_postgres_recent_aggregate_runs_split_queries(monkeypatch, tmp_path):
    db_path = tmp_path / "leaderboard-postgres-split.sqlite3"
    monkeypatch.setenv("SHEEP_DB_URL", "")
    monkeypatch.setenv("SHEEP_DB_PATH", str(db_path))
    _reset_db_module()
    import sheep_platform_db as db

    class _FakeCursor:
        def __init__(self, rows):
            self._rows = list(rows)

        def fetchall(self):
            return list(self._rows)

    class _FakeConn:
        def __init__(self):
            self.calls = []

        def execute(self, sql, params=None):
            self.calls.append((sql, params))
            if "recent_combo_tasks" in sql:
                return _FakeCursor(
                    [
                        {
                            "username": "combo-a",
                            "nickname": "Combo A",
                            "avatar_url": "",
                            "task_count": 2,
                            "total_done": 1234.0,
                        }
                    ]
                )
            if "aggregated_time" in sql:
                return _FakeCursor(
                    [
                        {
                            "username": "time-a",
                            "nickname": "Time A",
                            "avatar_url": "",
                            "total_seconds": 4321.0,
                        }
                    ]
                )
            raise AssertionError(f"unexpected SQL: {sql}")

    monkeypatch.setattr(db, "_default_avatar_url_from_conn", lambda conn: "https://example.com/default.png")
    monkeypatch.setattr(
        db,
        "_leaderboard_python_fallback",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("python fallback should not be used")),
    )

    stats = db._leaderboard_postgres_recent_agg(_FakeConn(), "2026-03-01T00:00:00+00:00", "2026-03-25T00:00:00+00:00")

    assert stats["combos"][0]["username"] == "combo-a"
    assert float(stats["combos"][0]["total_done"]) == pytest.approx(1234.0)
    assert stats["time"][0]["username"] == "time-a"
    assert float(stats["time"][0]["total_seconds"]) == pytest.approx(4321.0)


def test_postgres_recent_aggregate_falls_back_only_for_failed_section(monkeypatch, tmp_path):
    db_path = tmp_path / "leaderboard-postgres-partial-fallback.sqlite3"
    monkeypatch.setenv("SHEEP_DB_URL", "")
    monkeypatch.setenv("SHEEP_DB_PATH", str(db_path))
    _reset_db_module()
    import sheep_platform_db as db

    class _FakeCursor:
        def __init__(self, rows):
            self._rows = list(rows)

        def fetchall(self):
            return list(self._rows)

    class _FakeConn:
        def execute(self, sql, params=None):
            if "recent_combo_tasks" in sql:
                raise RuntimeError("statement timeout")
            if "aggregated_time" in sql:
                return _FakeCursor(
                    [
                        {
                            "username": "pg-time",
                            "nickname": "PG Time",
                            "avatar_url": "",
                            "total_seconds": 654.0,
                        }
                    ]
                )
            raise AssertionError(f"unexpected SQL: {sql}")

    fallback_calls = []

    def _fake_fallback(conn, cutoff_iso, window_end_iso):
        fallback_calls.append((cutoff_iso, window_end_iso))
        return {
            "combos": [{"username": "fallback-combo", "total_done": 321.0}],
            "time": [{"username": "fallback-time", "total_seconds": 111.0}],
        }

    monkeypatch.setattr(db, "_default_avatar_url_from_conn", lambda conn: "https://example.com/default.png")
    monkeypatch.setattr(db, "_leaderboard_python_fallback", _fake_fallback)

    stats = db._leaderboard_postgres_recent_agg(_FakeConn(), "2026-03-01T00:00:00+00:00", "2026-03-25T00:00:00+00:00")

    assert len(fallback_calls) == 1
    assert stats["combos"][0]["username"] == "fallback-combo"
    assert stats["time"][0]["username"] == "pg-time"


def test_leaderboard_python_fallback_prefers_larger_timestamp_elapsed(monkeypatch, tmp_path):
    db_path = tmp_path / "leaderboard-elapsed-regression.sqlite3"
    monkeypatch.setenv("SHEEP_DB_URL", "")
    monkeypatch.setenv("SHEEP_DB_PATH", str(db_path))
    monkeypatch.setenv("SHEEP_LEADERBOARD_TASK_SCAN_LIMIT", "100")
    monkeypatch.setenv("SHEEP_LEADERBOARD_TASK_SCAN_MAX_ROWS", "1000")
    _reset_db_module()
    import sheep_platform_db as db

    db.init_db()
    db.ensure_cycle_rollover()
    cycle = db.get_active_cycle()
    user = db.get_user_by_username("sheep")
    if user is None:
        db.create_user("sheep", "test-hash", role="user")
        user = db.get_user_by_username("sheep")
    assert cycle is not None and user is not None

    pool_id = db.create_factor_pool(
        cycle_id=int(cycle["id"]),
        name="Leaderboard Elapsed Pool",
        symbol="BTC_USDT",
        timeframe_min=60,
        years=2,
        family="trend",
        grid_spec={"alpha": [1, 2]},
        risk_spec={"max_leverage": 2},
        num_partitions=4,
        seed=19,
        active=True,
    )[0]

    now_dt = datetime.now(timezone.utc)
    created_at = (now_dt - timedelta(hours=8, minutes=12)).isoformat()
    updated_at = now_dt.isoformat()
    conn = db._conn()
    try:
        conn.execute(
            """
            INSERT INTO mining_tasks (
                user_id, pool_id, cycle_id, partition_idx, num_partitions,
                status, progress_json, created_at, updated_at, last_heartbeat
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(user["id"]),
                int(pool_id),
                int(cycle["id"]),
                0,
                4,
                "completed",
                json.dumps({"combos_done": 77, "elapsed_s": 60}),
                created_at,
                updated_at,
                updated_at,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    stats = db.get_leaderboard_stats(period_hours=720)

    assert stats["time"], stats
    assert float(stats["time"][0]["total_seconds"]) >= (8 * 3600), stats


def test_telegram_notifier_dedupes_and_sends_recovery(monkeypatch):
    module = _load_live_trader_module()

    sent_payloads = []

    class _FakeResp:
        status_code = 200

        @staticmethod
        def json():
            return {"ok": True}

    class _FakeSession:
        def __init__(self):
            self.headers = {}

        def post(self, url, json=None, timeout=None):
            sent_payloads.append({"url": url, "json": dict(json or {}), "timeout": timeout})
            return _FakeResp()

    monkeypatch.setattr(module.requests, "Session", lambda: _FakeSession())
    notifier = module.TelegramNotifier()
    notifier.configure(
        enabled=True,
        bot_token="test-bot-token",
        chat_id="6071244154",
        dedupe_sec=900,
        scope="critical_and_trade",
    )

    try:
        assert notifier.emit(
            event_type="runtime_sync_failed",
            severity="error",
            subsystem="runtime_sync_global",
            message="global runtime 快照更新失敗。",
            reason="HTTP 502",
            dedupe_key="runtime_sync:global",
        )
        assert notifier.emit(
            event_type="runtime_sync_failed",
            severity="error",
            subsystem="runtime_sync_global",
            message="global runtime 快照更新失敗。",
            reason="HTTP 502",
            dedupe_key="runtime_sync:global",
        ) is False
        assert notifier.flush(timeout=1.0) is True
        assert len(sent_payloads) == 1
        assert notifier.stats["telegram_suppressed_dedupe"] >= 1

        assert notifier.emit(
            event_type="runtime_sync_recovered",
            severity="info",
            subsystem="runtime_sync_global",
            message="global runtime 快照已更新。",
            dedupe_key="runtime_sync:global",
            recovery_of="runtime_sync:global",
        )
        assert notifier.flush(timeout=1.0) is True
        assert len(sent_payloads) == 2
    finally:
        notifier.shutdown()


def test_ui_perf_helpers_choose_lower_impact_modes():
    module = _load_live_trader_module()

    assert module.AnimatedUI._next_log_drain_delay_ms(0, 1.0) == 120
    assert module.AnimatedUI._next_log_drain_delay_ms(250, 5.0) == 35
    assert module.AnimatedUI._next_log_drain_delay_ms(600, 5.0) == 20
    assert module.AnimatedUI._determine_animation_mode("auto", backlog=0, trader_running=False, holy_grail_busy=False) == "normal"
    assert module.AnimatedUI._determine_animation_mode("auto", backlog=120, trader_running=True, holy_grail_busy=False) == "low"
    assert module.AnimatedUI._determine_animation_mode("auto", backlog=450, trader_running=False, holy_grail_busy=True) == "paused"
    assert module.AnimatedUI._determine_animation_mode("minimal", backlog=0, trader_running=False, holy_grail_busy=False) == "paused"
