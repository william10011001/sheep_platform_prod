import ast
import importlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import sheep_runtime_paths as paths
import sheep_holy_grail_runtime as holy_runtime_mod
from sheep_holy_grail_runtime import HolyGrailRuntime, run_holy_grail_build


def _load_live_trader_module():
    script_path = next((ROOT / "實盤程式").glob("*下單程式.py"))
    spec = importlib.util.spec_from_file_location("sheep_live_trader_runtime_tests", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_ohlcv(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def test_timeframe_labels_and_aliases():
    assert paths.timeframe_min_to_label(1) == "1m"
    assert paths.timeframe_min_to_label(5) == "5m"
    assert paths.timeframe_min_to_label(15) == "15m"
    assert paths.timeframe_min_to_label(30) == "30m"
    assert paths.timeframe_min_to_label(60) == "1h"
    assert paths.timeframe_min_to_label(120) == "2h"
    assert paths.timeframe_min_to_label(240) == "4h"
    assert paths.timeframe_min_to_label(1440) == "1d"
    assert paths.timeframe_candidate_labels(240) == ["4h", "240m"]
    assert paths.timeframe_candidate_labels(1440) == ["1d", "1440m"]


def test_import_backtest_panel_failure_is_explicit(monkeypatch, tmp_path):
    def _boom(_name):
        raise ImportError("boom")

    monkeypatch.setattr(importlib, "import_module", _boom)
    module, error = paths.import_backtest_panel(tmp_path)
    assert module is None
    assert "backtest_panel2" in error
    assert str(tmp_path / "app") in error


def test_import_backtest_runtime_failure_is_explicit(monkeypatch, tmp_path):
    def _boom(_name):
        raise ImportError("boom")

    monkeypatch.setattr(importlib, "import_module", _boom)
    module, error = paths.import_backtest_runtime(tmp_path)
    assert module is None
    assert "backtest_runtime_core" in error
    assert str(tmp_path / "app") in error


def test_backend_runtime_import_does_not_emit_streamlit_runtime_warning():
    code = (
        "import sys;"
        f"sys.path.insert(0, {str(APP_DIR)!r});"
        "import backtest_runtime_core as mod;"
        "print(bool(getattr(mod, 'run_backtest', None)))"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "True"
    assert "No runtime found" not in result.stderr
    assert "MemoryCacheStorageManager" not in result.stderr


def test_realtime_trader_import_does_not_emit_streamlit_runtime_warning():
    script_path = ROOT / "實盤程式" / "實盤因子池下單程式.py"
    code = (
        "import importlib.util, sys;"
        f"sys.path.insert(0, {str(APP_DIR)!r});"
        f"script_path = {str(script_path)!r};"
        "spec = importlib.util.spec_from_file_location('sheep_live_trader', script_path);"
        "mod = importlib.util.module_from_spec(spec);"
        "spec.loader.exec_module(mod);"
        "print(bool(getattr(mod, 'HolyGrailRuntime', None)))"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "True"
    assert "No runtime found" not in result.stderr
    assert "MemoryCacheStorageManager" not in result.stderr


def test_runtime_legacy_uses_postponed_annotations_for_py311_compat():
    runtime_legacy_path = APP_DIR / "sheep_realtime" / "runtime_legacy.py"
    module = ast.parse(runtime_legacy_path.read_text(encoding="utf-8-sig"))
    future_imports = [
        node
        for node in module.body
        if isinstance(node, ast.ImportFrom) and node.module == "__future__"
    ]
    assert any(alias.name == "annotations" for node in future_imports for alias in node.names)


def test_kline_loader_supports_exact_and_resample(monkeypatch, tmp_path):
    monkeypatch.setenv("SHEEP_DATA_DIR", str(tmp_path))

    _write_ohlcv(
        tmp_path / "UNITBTC_USDT_1h_3y.csv",
        [
            {"ts": "2026-01-01T00:00:00+00:00", "open": 100, "high": 102, "low": 99, "close": 101, "volume": 10},
            {"ts": "2026-01-01T01:00:00+00:00", "open": 101, "high": 103, "low": 100, "close": 102, "volume": 11},
        ],
    )
    _write_ohlcv(
        tmp_path / "UNITETH_USDT_1h_3y.csv",
        [
            {"ts": f"2026-01-01T0{hour}:00:00+00:00", "open": 200 + hour, "high": 201 + hour, "low": 199 + hour, "close": 200.5 + hour, "volume": 5 + hour}
            for hour in range(8)
        ],
    )

    runtime = HolyGrailRuntime(bt_module=object(), log=lambda _msg: None)
    exact = runtime.load_kline_data("UNITBTC_USDT", 60)
    assert exact is not None
    assert len(exact) == 2

    resampled = runtime.load_kline_data("UNITETH_USDT", 240)
    assert resampled is not None
    assert len(resampled) == 2
    assert float(resampled.iloc[0]["open"]) == 200
    assert float(resampled.iloc[0]["close"]) == 203.5

    missing = runtime.load_kline_data("UNITMISS_USDT", 240)
    assert missing is None
    assert any("missing kline" in msg.lower() for msg in runtime._warning_messages)


def test_live_trader_runtime_auto_syncs_missing_canonical_kline_csv(monkeypatch, tmp_path):
    monkeypatch.setenv("SHEEP_DATA_DIR", str(tmp_path))
    live_mod = _load_live_trader_module()
    logs = []
    live_mod.log = logs.append
    sync_calls = []

    class _BT:
        def ensure_bitmart_data(self, symbol, main_step_min, years=3, auto_sync=True, force_full=False, skip_1m=False, **_kwargs):
            sync_calls.append(
                {
                    "symbol": symbol,
                    "timeframe_min": int(main_step_min),
                    "years": int(years),
                    "auto_sync": bool(auto_sync),
                    "force_full": bool(force_full),
                    "skip_1m": bool(skip_1m),
                }
            )
            csv_path = tmp_path / "AUTOSYNC_SOL_USDT_4h_3y.csv"
            _write_ohlcv(
                csv_path,
                [
                    {"ts": "2026-01-01T00:00:00+00:00", "open": 100, "high": 101, "low": 99, "close": 100.5, "volume": 10},
                    {"ts": "2026-01-01T04:00:00+00:00", "open": 100.5, "high": 103, "low": 100, "close": 102.5, "volume": 11},
                ],
            )
            return str(csv_path), ""

        def load_and_validate_csv(self, path):
            df = pd.read_csv(path)
            df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
            return df.dropna(subset=["ts"]).reset_index(drop=True)

    live_mod.bt = _BT()

    runtime = live_mod._new_holy_grail_runtime()
    df = runtime.load_kline_data("AUTOSYNC_SOL_USDT", 240)

    assert df is not None
    assert len(df) == 2
    assert sync_calls == [
        {
            "symbol": "AUTOSYNC_SOL_USDT",
            "timeframe_min": 240,
            "years": 3,
            "auto_sync": True,
            "force_full": False,
            "skip_1m": True,
        }
    ]
    assert (tmp_path / "AUTOSYNC_SOL_USDT_4h_3y.csv").exists()
    assert any("auto-syncing canonical kline csv" in msg.lower() for msg in logs)


def test_live_trader_runtime_repairs_legacy_kline_csv_by_force_full_sync(monkeypatch, tmp_path):
    monkeypatch.setenv("SHEEP_DATA_DIR", str(tmp_path))
    legacy_path = tmp_path / "AUTOSYNC_NEAR_USDT_4h_3y.csv"
    pd.DataFrame(
        [
            {
                "time": 1704067200000,
                "open_price": 10.0,
                "high_price": 10.5,
                "low_price": 9.8,
                "close_price": 10.2,
                "volume": 123.0,
            }
        ]
    ).to_csv(legacy_path, index=False)

    live_mod = _load_live_trader_module()
    force_full_calls = []

    class _BT:
        def ensure_bitmart_data(self, symbol, main_step_min, years=3, auto_sync=True, force_full=False, skip_1m=False, **_kwargs):
            force_full_calls.append(bool(force_full))
            if force_full:
                _write_ohlcv(
                    legacy_path,
                    [
                        {"ts": "2026-01-01T00:00:00+00:00", "open": 10, "high": 10.2, "low": 9.9, "close": 10.1, "volume": 100},
                        {"ts": "2026-01-01T04:00:00+00:00", "open": 10.1, "high": 10.4, "low": 10.0, "close": 10.3, "volume": 90},
                    ],
                )
            return str(legacy_path), ""

        def load_and_validate_csv(self, path):
            df = pd.read_csv(path)
            need_cols = {"ts", "open", "high", "low", "close", "volume"}
            if not need_cols.issubset(df.columns):
                raise ValueError("legacy csv format")
            df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
            return df.dropna(subset=["ts"]).reset_index(drop=True)

    live_mod.bt = _BT()
    live_mod.log = lambda _msg: None

    runtime = live_mod._new_holy_grail_runtime()
    df = runtime.load_kline_data("AUTOSYNC_NEAR_USDT", 240)

    assert df is not None
    assert len(df) == 2
    assert force_full_calls == [False, True]


def test_live_trader_holy_grail_build_blocks_partial_publish_when_required_kline_unresolved(monkeypatch, tmp_path):
    monkeypatch.setenv("SHEEP_DATA_DIR", str(tmp_path))
    live_mod = _load_live_trader_module()
    backtest_calls = {"count": 0}

    class _BT:
        def ensure_bitmart_data(self, *args, **kwargs):
            raise RuntimeError("bitmart unavailable")

        def run_backtest(self, **kwargs):
            backtest_calls["count"] += 1
            return {}

    strategies = [
        {
            "strategy_id": 1,
            "symbol": "UNITMISSING_XAUT_USDT",
            "timeframe_min": 60,
            "pool_name": "xaut-pool",
            "params": {
                "family": "TEMA_RSI",
                "tp": 0.01,
                "sl": 0.02,
                "max_hold": 40,
                "family_params": {"fast_len": 9, "slow_len": 30},
            },
            "metrics": {"sharpe": 1.8},
        }
    ]

    monkeypatch.setattr(
        live_mod.AutoSyncHolyGrailRuntime,
        "fetch_factor_pool_data",
        lambda self: (strategies, "https://example.com/api", "runtime-token"),
    )
    monkeypatch.setattr(
        live_mod,
        "http_request",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("network disabled for test")),
    )

    result = live_mod.run_holy_grail_build(
        bt_module=_BT(),
        log=lambda _msg: None,
        factor_pool_url="https://example.com",
        factor_pool_token="runtime-token",
    )

    assert result.ok is False
    assert "missing compatible kline data" in result.message
    assert backtest_calls["count"] == 0
    assert result.diagnostics["kline_sync"]["unresolved_pairs"][0]["symbol"] == "UNITMISSING_XAUT_USDT"
    assert result.diagnostics["kline_sync"]["failed_count"] >= 1


def test_live_trader_runtime_falls_back_to_contract_klines_for_xag(monkeypatch, tmp_path):
    monkeypatch.setenv("SHEEP_DATA_DIR", str(tmp_path))
    live_mod = _load_live_trader_module()
    logs = []
    live_mod.log = logs.append
    spot_calls = {"count": 0}
    contract_only_symbol = "XAGTEST_USDT"

    class _BT:
        def ensure_bitmart_data(self, *args, **kwargs):
            spot_calls["count"] += 1
            raise RuntimeError("spot symbol invalid")

        def load_and_validate_csv(self, path):
            df = pd.read_csv(path)
            df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
            return df.dropna(subset=["ts"]).reset_index(drop=True)

    class _Resp:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "code": 1000,
                "message": "Ok",
                "data": [
                    {
                        "timestamp": 1704067200,
                        "open_price": "23.0",
                        "high_price": "23.5",
                        "low_price": "22.8",
                        "close_price": "23.2",
                        "volume": "111.0",
                    },
                    {
                        "timestamp": 1704070800,
                        "open_price": "23.2",
                        "high_price": "23.6",
                        "low_price": "23.1",
                        "close_price": "23.4",
                        "volume": "98.0",
                    },
                ],
            }

    def _fake_http_request(_session, method, url, timeout=None, verify=None, params=None, headers=None, **_kwargs):
        assert method == "GET"
        assert "contract/public/kline" in url
        assert params["symbol"] == "XAGTESTUSDT"
        return _Resp()

    monkeypatch.setattr(live_mod, "http_request", _fake_http_request)
    monkeypatch.setattr(live_mod.time, "sleep", lambda _s: None)
    monkeypatch.setattr(live_mod, "CONTRACT_ONLY_KLINE_SYMBOLS", {contract_only_symbol})
    live_mod.bt = _BT()

    runtime = live_mod._new_holy_grail_runtime()
    df = runtime.load_kline_data(contract_only_symbol, 60)

    assert df is not None
    assert len(df) == 2
    assert spot_calls["count"] == 0
    assert (tmp_path / "XAGTEST_USDT_1h_3y.csv").exists()
    assert any("contract fallback" in msg.lower() for msg in logs)


def test_live_trader_runtime_retries_transient_factor_pool_fetch(monkeypatch):
    live_mod = _load_live_trader_module()
    logs = []
    live_mod.log = logs.append
    attempts = {"count": 0}

    def _fake_super_fetch(self):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError(
                "(\"Connection broken: ConnectionResetError(10054, '遠端主機已強制關閉一個現存的連線。')\")"
            )
        return ([{"strategy_id": 1, "symbol": "BTC_USDT", "timeframe_min": 60}], "https://example.com/api", "tok")

    monkeypatch.setattr(live_mod.HolyGrailRuntime, "fetch_factor_pool_data", _fake_super_fetch)
    monkeypatch.setattr(live_mod.time, "sleep", lambda _s: None)

    runtime = live_mod._new_holy_grail_runtime()
    strategies, api_base, token = runtime.fetch_factor_pool_data()

    assert attempts["count"] == 3
    assert api_base == "https://example.com/api"
    assert token == "tok"
    assert strategies[0]["symbol"] == "BTC_USDT"
    assert any("transient network error" in msg.lower() for msg in logs)


def test_live_trader_runtime_reuses_recent_factor_pool_cache_on_transient_failure(monkeypatch):
    live_mod = _load_live_trader_module()
    logs = []
    live_mod.log = logs.append
    calls = {"count": 0}

    def _fake_fetch_pages(self, api_base, token):
        calls["count"] += 1
        if calls["count"] == 1:
            return [{"strategy_id": 1, "symbol": "BTC_USDT", "timeframe_min": 60}]
        raise RuntimeError("Response ended prematurely")

    monkeypatch.setattr(live_mod.HolyGrailRuntime, "_fetch_factor_pool_pages", _fake_fetch_pages)
    monkeypatch.setattr(live_mod.HolyGrailRuntime, "detect_api_base", lambda self, host: "https://example.com/api")
    monkeypatch.setattr(live_mod.time, "sleep", lambda _s: None)

    runtime1 = live_mod._new_holy_grail_runtime(
        factor_pool_url="https://example.com",
        factor_pool_token="static-token",
    )
    first = runtime1.fetch_factor_pool_data()

    runtime2 = live_mod._new_holy_grail_runtime(
        factor_pool_url="https://example.com",
        factor_pool_token="static-token",
    )
    second = runtime2.fetch_factor_pool_data()

    assert first[0][0]["strategy_id"] == 1
    assert second[0][0]["strategy_id"] == 1
    assert calls["count"] == 4
    assert runtime2._last_factor_pool_fetch_used_cached_payload is True
    assert any("reusing cached factor-pool payload" in msg.lower() for msg in logs)


def test_live_trader_build_portfolio_retries_transient_preflight(monkeypatch):
    live_mod = _load_live_trader_module()
    logs = []
    live_mod.log = logs.append
    attempts = {"count": 0}

    def _fake_prime(self):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("Response ended prematurely")
        return {
            "api_base": "https://example.com/api",
            "token_present": True,
            "required_pairs": [],
            "ready_pairs": [],
            "unresolved_pairs": [],
            "details": [],
            "existing_count": 0,
            "synced_count": 0,
            "resampled_count": 0,
            "failed_count": 0,
        }

    def _fake_base_build(self, **kwargs):
        return live_mod.HolyGrailResult(ok=True, message="ok")

    monkeypatch.setattr(live_mod.AutoSyncHolyGrailRuntime, "prime_required_klines", _fake_prime)
    monkeypatch.setattr(live_mod.HolyGrailRuntime, "build_portfolio", _fake_base_build)
    monkeypatch.setattr(live_mod.time, "sleep", lambda _s: None)

    runtime = live_mod._new_holy_grail_runtime()
    result = runtime.build_portfolio()

    assert result.ok is True
    assert attempts["count"] == 3
    assert result.diagnostics["preflight_retry_count"] == 2
    assert any("kline preflight hit a transient upstream error" in msg.lower() for msg in logs)


def test_fetch_factor_pool_data_supports_token_and_pagination(monkeypatch):
    calls = {"get_pages": [], "post_calls": 0}

    class _Resp:
        def __init__(self, status_code=200, body=None):
            self.status_code = status_code
            self._body = body or {}
            self.text = json.dumps(self._body, ensure_ascii=False)

        def json(self):
            return self._body

    def _fake_http_request(_session, method, url, timeout=None, verify=None, params=None, headers=None, json=None, **_kwargs):
        assert method in {"GET", "POST"}
        if params is None:
            return _Resp(200, {"ok": True})
        calls["get_pages"].append(int((params or {}).get("page") or 0))
        assert headers == {"Authorization": "Bearer static-token", "Content-Type": "application/json"}
        page = int((params or {}).get("page") or 1)
        if page == 1:
            return _Resp(
                200,
                {
                    "items": [{"strategy_id": 1}, {"strategy_id": 2}],
                    "total": 3,
                    "has_next": True,
                },
            )
        return _Resp(
            200,
            {
                "items": [{"strategy_id": 3}],
                "total": 3,
                "has_next": False,
            },
        )

    def _unexpected_http_request(_session, method, *args, **kwargs):
        if method != "POST":
            return _fake_http_request(_session, method, *args, **kwargs)
        calls["post_calls"] += 1
        raise AssertionError("token fetch should not need password login")

    monkeypatch.setattr(holy_runtime_mod, "http_request", _unexpected_http_request)

    runtime = HolyGrailRuntime(
        bt_module=object(),
        log=lambda _msg: None,
        factor_pool_url="https://example.com",
        factor_pool_token="static-token",
    )
    strategies, api_base, token = runtime.fetch_factor_pool_data()

    assert api_base == "https://example.com/api"
    assert token == "static-token"
    assert [int(item["strategy_id"]) for item in strategies] == [1, 2, 3]
    assert calls["get_pages"] == [1, 2]
    assert calls["post_calls"] == 0


def test_flatten_strategies_prefers_active_strategy_payload_and_direction():
    runtime = HolyGrailRuntime(bt_module=object(), log=lambda _msg: None)
    df = runtime.flatten_strategies_to_dataframe(
        [
            {
                "strategy_id": 99,
                "symbol": "BTC_USDT",
                "timeframe_min": 240,
                "pool_name": "primary",
                "score": 8.2,
                "direction": "short",
                "params": {
                    "family": "TEMA_RSI",
                    "tp": 0.011,
                    "sl": 0.022,
                    "max_hold": 55,
                    "family_params": {"fast_len": 9, "slow_len": 30},
                },
                "metrics": {"sharpe": 1.7, "trades": 42},
                "progress": {
                    "checkpoint_candidates": [
                        {
                            "score": 99.0,
                            "params": {"family": "WRONG_FAMILY", "family_params": {"fast_len": 1}},
                            "metrics": {"sharpe": 9.9},
                        }
                    ]
                },
            }
        ]
    )

    assert len(df) == 1
    row = df.iloc[0].to_dict()
    assert row["family"] == "TEMA_RSI"
    assert bool(row["param_reverse_mode"]) is True
    assert float(row["metric_sharpe"]) == 1.7
    assert int(row["param_max_hold"]) == 55
    assert int(row["param_fast_len"]) == 9


def test_tema_rsi_python_fallback_produces_trades():
    import backtest_runtime_core as runtime_core

    result = runtime_core._simulate_tema_rsi_py(
        np.array([99.0, 100.0, 100.5, 101.0, 102.0], dtype=np.float64),
        np.array([99.5, 102.5, 101.0, 102.0, 102.5], dtype=np.float64),
        np.array([98.5, 99.8, 100.0, 100.5, 101.5], dtype=np.float64),
        np.array([99.2, 101.8, 100.8, 101.5, 102.2], dtype=np.float64),
        np.array([True, False, False, False, False], dtype=bool),
        np.array([0.01, 0.01, 0.01, 0.01, 0.01], dtype=np.float64),
        np.array([0.2, 0.2, 0.2, 0.2, 0.2], dtype=np.float64),
        np.array([0.02, 0.02, 0.02, 0.02, 0.02], dtype=np.float64),
        np.array([0.05, 0.05, 0.05, 0.05, 0.05], dtype=np.float64),
        np.array([1.0, 1.0, 1.0, 1.0, 1.0], dtype=np.float64),
        np.array([2, 0, 0, 0, 0], dtype=np.int8),
        0.0,
        10,
        0.0,
    )

    perbar, equity, e_idx, x_idx, e_px, x_px, tr_ret, bars_held, reasons, entry_reasons = result
    assert len(e_idx) == 1
    assert int(e_idx[0]) == 1
    assert int(x_idx[0]) == 1
    assert float(tr_ret[0]) > 0.0
    assert int(reasons[0]) in {2, 3}
    assert int(entry_reasons[0]) == 2
    assert float(equity[-1]) > 1.0


def test_cached_runtime_snapshot_preserves_full_strategy_metrics(monkeypatch, tmp_path):
    live_mod = _load_live_trader_module()
    live_mod.STATE_FILE = str(tmp_path / "tema_rsi_state.json")
    captured = {}

    class _TextBox:
        def __init__(self, text: str):
            self._text = text

        def get(self, *_args):
            return self._text

    class _UI:
        def __init__(self, text: str):
            self.multi_json_text = _TextBox(text)

    updater = live_mod.FactorPoolUpdater(
        _UI(
            json.dumps(
                [
                    {
                        "strategy_id": 321,
                        "strategy_key": "runtime-321",
                        "family": "TEMA_RSI",
                        "symbol": "ETH_USDT",
                        "direction": "long",
                        "interval": "1d",
                        "family_params": {"fast_len": 9, "slow_len": 30},
                        "stake_pct": 45.35,
                        "sharpe": 4.19,
                        "total_return_pct": 57.39,
                        "max_drawdown_pct": 3.37,
                    }
                ],
                ensure_ascii=False,
            )
        )
    )
    monkeypatch.setattr(updater, "_collect_runtime_position_items", lambda: [])

    def _capture(scope, payload, runtime_kwargs):
        captured["scope"] = scope
        captured["payload"] = payload
        captured["runtime_kwargs"] = runtime_kwargs
        return True

    monkeypatch.setattr(updater, "_post_runtime_snapshot", _capture)

    updater._sync_cached_runtime_snapshot("global", {}, reason="startup")

    assert captured["scope"] == "global"
    item = captured["payload"]["items"][0]
    assert int(item["strategy_id"]) == 321
    assert item["strategy_key"] == "runtime-321"
    assert item["total_return_pct"] == pytest.approx(57.39)
    assert item["max_drawdown_pct"] == pytest.approx(3.37)
    assert item["sharpe"] == pytest.approx(4.19)


def test_cached_runtime_snapshot_skips_config_only_payload(monkeypatch, tmp_path):
    live_mod = _load_live_trader_module()
    live_mod.STATE_FILE = str(tmp_path / "tema_rsi_state.json")
    captured = {}

    class _TextBox:
        def __init__(self, text):
            self._text = text

        def get(self, *_args):
            return self._text

    class _UI:
        def __init__(self, text):
            self.multi_json_text = _TextBox(text)

    updater = live_mod.FactorPoolUpdater(
        _UI(
            json.dumps(
                [
                    {
                        "strategy_id": 321,
                        "family": "TEMA_RSI",
                        "symbol": "ETH_USDT",
                        "direction": "long",
                        "interval": "1d",
                        "family_params": {"fast_len": 9, "slow_len": 30},
                        "stake_pct": 45.35,
                    }
                ],
                ensure_ascii=False,
            )
        )
    )
    monkeypatch.setattr(updater, "_collect_runtime_position_items", lambda: [])

    def _capture(scope, payload, runtime_kwargs):
        captured["scope"] = scope
        captured["payload"] = payload

    monkeypatch.setattr(updater, "_post_runtime_snapshot", _capture)

    updater._sync_cached_runtime_snapshot("global", {}, reason="startup")

    assert captured == {}


class _DummyBT:
    def run_backtest(self, **kwargs):
        return {
            "trades_detail": [
                {"entry_ts": "2026-01-01T00:00:00+00:00", "exit_ts": "2026-01-02T00:00:00+00:00", "net_return": 0.01},
                {"entry_ts": "2026-01-03T00:00:00+00:00", "exit_ts": "2026-01-04T00:00:00+00:00", "net_return": 0.02},
            ],
            "sharpe": 1.5,
            "sortino": 2.0,
            "calmar": 1.2,
            "cagr_pct": 12.0,
            "total_return_pct": 5.0,
            "max_drawdown_pct": 3.0,
            "trades": 2,
            "win_rate_pct": 100.0,
            "payoff": 1.5,
            "profit_factor": 2.0,
            "avg_win_pct": 1.5,
            "avg_loss_pct": 0.0,
            "expectancy_pct": 1.5,
            "avg_hold_bars": 2,
            "time_in_market_pct": 10.0,
        }


class _SyntheticBT:
    def __init__(self, curve_map):
        self.curve_map = curve_map

    def run_backtest(self, **kwargs):
        params = kwargs.get("params") or {}
        family_params = dict(kwargs.get("family_params") or params.get("family_params") or {})
        curve_key = str(params.get("curve_key") or family_params.get("curve_key") or "A")
        sharpe = float(family_params.get("sharpe") or 1.0)
        cagr = float(family_params.get("cagr_pct") or (10.0 + sharpe))
        max_dd = float(family_params.get("max_drawdown_pct") or 5.0)
        return {
            "trades_detail": [
                {
                    "entry_ts": f"2026-01-0{idx + 1}T00:00:00+00:00",
                    "exit_ts": f"2026-01-0{idx + 2}T00:00:00+00:00",
                    "entry_price": 100 + idx,
                    "exit_price": 100 + idx + ret,
                    "net_return": float(ret),
                    "curve_key": curve_key,
                }
                for idx, ret in enumerate(self.curve_map[curve_key])
            ],
            "sharpe": sharpe,
            "sortino": sharpe + 0.5,
            "calmar": max(0.1, cagr / max(max_dd, 0.1)),
            "cagr_pct": cagr,
            "total_return_pct": float(sum(self.curve_map[curve_key]) * 100.0),
            "max_drawdown_pct": max_dd,
            "trades": len(self.curve_map[curve_key]),
            "win_rate_pct": 60.0,
            "payoff": 1.4,
            "profit_factor": 1.2,
            "avg_win_pct": 1.1,
            "avg_loss_pct": -0.7,
            "expectancy_pct": 0.3,
            "avg_hold_bars": 3,
            "time_in_market_pct": 12.0,
        }


def _equity_from_returns(returns):
    returns = list(returns)
    base_index = pd.date_range("2026-01-01", periods=len(returns) + 1, freq="D", tz="UTC")
    equity = [1.0]
    running = 1.0
    for ret in returns:
        running *= 1.0 + float(ret)
        equity.append(running)
    return pd.Series(equity, index=base_index)


def test_shared_runtime_builds_portfolio(monkeypatch, tmp_path):
    monkeypatch.setenv("SHEEP_RUNTIME_DIR", str(tmp_path / "runtime"))

    def _fake_fetch(self):
        return (
            [
                {
                    "strategy_id": 1,
                    "symbol": "UNITRUN_USDT",
                    "timeframe_min": 60,
                    "pool_name": "primary",
                    "progress": {
                        "checkpoint_candidates": [
                            {
                                "score": 9.5,
                                "direction": "long",
                                "params": {
                                    "family": "TEMA_Cross",
                                    "tp": 0.01,
                                    "sl": 0.02,
                                    "max_hold": 10,
                                    "family_params": {"curve_key": "LONG_A", "fast_len": 10, "slow_len": 30, "sharpe": 1.0, "cagr_pct": 12.0},
                                },
                                "metrics": {"sharpe": 1.0},
                            },
                            {
                                "score": 9.4,
                                "direction": "short",
                                "params": {
                                    "family": "TEMA_Cross",
                                    "tp": 0.01,
                                    "sl": 0.02,
                                    "max_hold": 10,
                                    "family_params": {"curve_key": "SHORT_A", "fast_len": 12, "slow_len": 34, "sharpe": 1.0, "cagr_pct": 11.0},
                                },
                                "metrics": {"sharpe": 1.0},
                            }
                        ]
                    },
                }
            ],
            "https://example.com/api",
            "static-token",
        )

    monkeypatch.setattr(HolyGrailRuntime, "fetch_factor_pool_data", _fake_fetch)
    monkeypatch.setattr(
        HolyGrailRuntime,
        "_fetch_cost_settings",
        lambda self, api_base, token, fallback_fee_side: {
            "fee_pct": 0.06,
            "slippage_pct": 0.02,
            "fee_side": 0.0006,
            "slippage": 0.0002,
            "source_settings_updated_at": "2026-03-24T00:00:00+00:00",
        },
    )
    monkeypatch.setattr(
        HolyGrailRuntime,
        "load_kline_data",
        lambda self, symbol, timeframe_min: pd.DataFrame(
            {
                "ts": pd.date_range("2026-01-01", periods=6, freq="D", tz="UTC"),
                "open": [100, 101, 102, 103, 104, 105],
                "high": [101, 102, 103, 104, 105, 106],
                "low": [99, 100, 101, 102, 103, 104],
                "close": [100.5, 101.5, 102.5, 103.5, 104.5, 105.5],
                "volume": [10, 11, 12, 13, 14, 15],
            }
        ),
    )
    monkeypatch.setattr(
        HolyGrailRuntime,
        "build_daily_equity_curve",
        staticmethod(
            lambda trades: {
                "LONG_A": _equity_from_returns([0.02, 0.0, -0.02, 0.0, 0.02, 0.0]),
                "SHORT_A": _equity_from_returns([0.0, 0.02, 0.0, -0.02, 0.0, 0.02]),
            }[trades[0]["curve_key"]]
        ),
    )

    result = run_holy_grail_build(
        bt_module=_SyntheticBT({"LONG_A": [0.01, 0.02, 0.0], "SHORT_A": [0.0, 0.015, 0.01]}),
        log=lambda _msg: None,
        base_stake_pct=50.0,
    )
    assert result.ok
    assert result.selected_count == 2
    assert {item["direction"] for item in result.multi_payload} == {"long", "short"}
    assert sum(float(item["stake_pct"]) for item in result.multi_payload) == pytest.approx(50.0)
    assert all(item["interval"] == "1h" for item in result.multi_payload)
    assert result.cost_basis["fee_pct"] == pytest.approx(0.06)
    assert result.cost_basis["slippage_pct"] == pytest.approx(0.02)
    assert Path(result.report_paths["summary_report"]).exists()


def test_duplicate_trade_signatures_are_deduplicated(monkeypatch, tmp_path):
    monkeypatch.setenv("SHEEP_RUNTIME_DIR", str(tmp_path / "runtime"))

    def _fake_fetch(self):
        return (
            [
                {
                    "strategy_id": 1,
                    "symbol": "BTC_USDT",
                    "timeframe_min": 60,
                    "pool_name": "pool",
                    "progress": {
                        "checkpoint_candidates": [
                            {
                                "score": 10,
                                "params": {
                                    "family": "WMA_Cross",
                                    "tp": 0.01,
                                    "sl": 0.02,
                                    "max_hold": 10,
                                    "family_params": {"curve_key": "dup", "sharpe": 2.2, "cagr_pct": 18.0},
                                },
                                "metrics": {"sharpe": 2.2},
                            },
                            {
                                "score": 9.8,
                                "params": {
                                    "family": "WMA_Cross",
                                    "tp": 0.01,
                                    "sl": 0.02,
                                    "max_hold": 10,
                                    "family_params": {"curve_key": "dup", "sharpe": 2.1, "cagr_pct": 17.0},
                                },
                                "metrics": {"sharpe": 2.1},
                            },
                            {
                                "score": 9.2,
                                "direction": "short",
                                "params": {
                                    "family": "TEMA_Cross",
                                    "tp": 0.01,
                                    "sl": 0.02,
                                    "max_hold": 10,
                                    "family_params": {"curve_key": "unique_short", "sharpe": 1.5, "cagr_pct": 11.0},
                                },
                                "metrics": {"sharpe": 1.5},
                            },
                        ]
                    },
                }
            ],
            "https://example.com/api",
            "static-token",
        )

    def _fake_kline(self, symbol, timeframe_min):
        return pd.DataFrame(
            {
                "ts": pd.date_range("2026-01-01", periods=6, freq="D", tz="UTC"),
                "open": [100, 101, 102, 103, 104, 105],
                "high": [101, 102, 103, 104, 105, 106],
                "low": [99, 100, 101, 102, 103, 104],
                "close": [100.5, 101.5, 102.5, 103.5, 104.5, 105.5],
                "volume": [10, 11, 12, 13, 14, 15],
            }
        )

    def _fake_equity(trades):
        curve_key = trades[0]["curve_key"]
        mapping = {
            "dup": _equity_from_returns([0.01, 0.02, 0.02, 0.015, 0.018, 0.017]),
            "unique_short": _equity_from_returns([0.0, 0.02, 0.0, -0.02, 0.0, 0.02]),
        }
        return mapping[curve_key]

    monkeypatch.setattr(HolyGrailRuntime, "fetch_factor_pool_data", _fake_fetch)
    monkeypatch.setattr(
        HolyGrailRuntime,
        "_fetch_cost_settings",
        lambda self, api_base, token, fallback_fee_side: {
            "fee_pct": 0.06,
            "slippage_pct": 0.02,
            "fee_side": 0.0006,
            "slippage": 0.0002,
            "source_settings_updated_at": "2026-03-24T00:00:00+00:00",
        },
    )
    monkeypatch.setattr(HolyGrailRuntime, "load_kline_data", _fake_kline)
    monkeypatch.setattr(HolyGrailRuntime, "build_daily_equity_curve", staticmethod(_fake_equity))

    result = run_holy_grail_build(
        bt_module=_SyntheticBT({"dup": [0.01, 0.02, 0.02], "unique_short": [0.0, 0.01, 0.005]}),
        log=lambda _msg: None,
    )
    assert result.ok
    assert result.selected_count == 2
    assert len([item for item in result.selected_portfolio if item.get("duplicate_rank") == 1]) == 2
    full_report = pd.read_csv(result.report_paths["final_report"])
    dup_rows = full_report[full_report["duplicate_group_size"] > 1]
    assert len(dup_rows) == 2
    assert dup_rows["duplicate_group_id"].nunique() == 1
    assert set(dup_rows["selection_status"]) == {"selected", "rejected_duplicate"}
    assert any("duplicate" in str(reason) for reason in dup_rows["selection_reject_reason"].fillna(""))
    assert "direction" in full_report.columns
    assert "max_pairwise_corr_to_selected" in full_report.columns
    assert "behavior_hash_type" in full_report.columns


def test_pairwise_hard_threshold_blocks_highly_correlated_pair(monkeypatch, tmp_path):
    monkeypatch.setenv("SHEEP_RUNTIME_DIR", str(tmp_path / "runtime"))

    def _fake_fetch(self):
        return (
            [
                {
                    "strategy_id": 1,
                    "symbol": "ETH_USDT",
                    "timeframe_min": 60,
                    "pool_name": "pool",
                    "progress": {
                        "checkpoint_candidates": [
                            {
                                "score": 10.0,
                                "direction": "long",
                                "params": {
                                    "family": "Alpha",
                                    "tp": 0.01,
                                    "sl": 0.02,
                                    "max_hold": 10,
                                    "family_params": {"curve_key": "A", "sharpe": 2.4},
                                },
                                "metrics": {"sharpe": 2.4},
                            },
                            {
                                "score": 9.8,
                                "direction": "short",
                                "params": {
                                    "family": "Beta",
                                    "tp": 0.01,
                                    "sl": 0.02,
                                    "max_hold": 10,
                                    "family_params": {"curve_key": "B", "sharpe": 1.8},
                                },
                                "metrics": {"sharpe": 1.8},
                            },
                            {
                                "score": 9.7,
                                "direction": "short",
                                "params": {
                                    "family": "Gamma",
                                    "tp": 0.01,
                                    "sl": 0.02,
                                    "max_hold": 10,
                                    "family_params": {"curve_key": "C", "sharpe": 1.7},
                                },
                                "metrics": {"sharpe": 1.7},
                            },
                        ]
                    },
                }
            ],
            "https://example.com/api",
            "static-token",
        )

    def _fake_kline(self, symbol, timeframe_min):
        return pd.DataFrame(
            {
                "ts": pd.date_range("2026-01-01", periods=8, freq="D", tz="UTC"),
                "open": [100] * 8,
                "high": [101] * 8,
                "low": [99] * 8,
                "close": [100.5] * 8,
                "volume": [10] * 8,
            }
        )

    def _fake_equity(trades):
        curve_key = trades[0]["curve_key"]
        mapping = {
            "A": _equity_from_returns([0.02, 0.0, -0.02, 0.0, 0.02, 0.0, -0.02, 0.0]),
            "B": _equity_from_returns([0.0, 0.02, 0.0, -0.02, 0.0, 0.02, 0.0, -0.02]),
            "C": _equity_from_returns([0.0, 0.021, 0.0, -0.021, 0.0, 0.022, 0.0, -0.022]),
        }
        return mapping[curve_key]

    monkeypatch.setattr(HolyGrailRuntime, "fetch_factor_pool_data", _fake_fetch)
    monkeypatch.setattr(
        HolyGrailRuntime,
        "_fetch_cost_settings",
        lambda self, api_base, token, fallback_fee_side: {
            "fee_pct": 0.06,
            "slippage_pct": 0.02,
            "fee_side": 0.0006,
            "slippage": 0.0002,
            "source_settings_updated_at": "2026-03-24T00:00:00+00:00",
        },
    )
    monkeypatch.setattr(HolyGrailRuntime, "load_kline_data", _fake_kline)
    monkeypatch.setattr(HolyGrailRuntime, "build_daily_equity_curve", staticmethod(_fake_equity))

    result = run_holy_grail_build(
        bt_module=_SyntheticBT({"A": [0.02, 0.02, 0.02], "B": [0.01, -0.01, 0.01], "C": [-0.01, 0.01, -0.01]}),
        log=lambda _msg: None,
    )
    assert result.ok
    families = [row["family"] for row in result.selected_portfolio]
    assert "Alpha" in families
    assert len([name for name in families if name in {"Beta", "Gamma"}]) == 1
    full_report = pd.read_csv(result.report_paths["final_report"])
    rejected = full_report[full_report["selection_status"] == "rejected_corr"]
    assert not rejected.empty
    assert rejected["max_pairwise_corr_to_selected"].abs().max() > 0.4


def test_candidate_pool_reserves_exploration_slots_for_metricless_templates():
    runtime = HolyGrailRuntime(bt_module=object(), log=lambda _msg: None)
    rows = []
    for idx in range(5):
        rows.append(
            {
                "strategy_id": idx + 1,
                "family": f"LONG_{idx}",
                "symbol": f"L_{idx}",
                "timeframe_min": 60,
                "direction": "long",
                "param_reverse_mode": False,
                "param_alpha": idx,
                "cand_score": 10.0 - idx,
                "metric_sharpe": 2.0 - (idx * 0.1),
                "metric_cagr_pct": 20.0 - idx,
                "metric_max_drawdown_pct": 5.0 + idx,
                "allocation_pct": 5.0,
                "created_at": f"2026-01-0{idx + 1}T00:00:00+00:00",
                "candidate_source": "scored_strategy",
                "has_metrics": True,
            }
        )
    for idx in range(3):
        rows.append(
            {
                "strategy_id": 100 + idx,
                "family": f"SHORT_{idx}",
                "symbol": f"S_{idx}",
                "timeframe_min": 60,
                "direction": "short",
                "param_reverse_mode": True,
                "param_alpha": idx,
                "cand_score": 9.0 - idx,
                "metric_sharpe": 1.8 - (idx * 0.1),
                "metric_cagr_pct": 15.0 - idx,
                "metric_max_drawdown_pct": 6.0 + idx,
                "allocation_pct": 5.0,
                "created_at": f"2026-02-0{idx + 1}T00:00:00+00:00",
                "candidate_source": "checkpoint_candidate",
                "has_metrics": True,
            }
        )
    for idx in range(2):
        rows.append(
            {
                "strategy_id": 200 + idx,
                "family": f"TEMPLATE_SHORT_{idx}",
                "symbol": f"TS_{idx}",
                "timeframe_min": 60,
                "direction": "short",
                "param_reverse_mode": True,
                "param_alpha": idx,
                "cand_score": 0.0,
                "metric_sharpe": np.nan,
                "metric_cagr_pct": np.nan,
                "metric_max_drawdown_pct": np.nan,
                "allocation_pct": 20.0 - idx,
                "created_at": f"2026-03-0{idx + 1}T00:00:00+00:00",
                "candidate_source": "template",
                "has_metrics": False,
            }
        )

    pool_df = runtime._candidate_pool(pd.DataFrame(rows), top_n=10, max_per_group=3)

    assert len(pool_df) == 10
    assert len(pool_df[pool_df["direction"] == "long"]) == 5
    assert len(pool_df[pool_df["direction"] == "short"]) == 5
    template_short = pool_df[(pool_df["direction"] == "short") & (pool_df["candidate_source"] == "template")]
    assert len(template_short) == 2
    assert set(template_short["direction_bucket"]) == {"exploration"}


def test_candidate_pool_preserves_cross_timeframe_mirrors():
    runtime = HolyGrailRuntime(bt_module=object(), log=lambda _msg: None)
    rows = []
    for timeframe_min, score in ((5, 10.0), (60, 9.8), (240, 9.6)):
        rows.append(
            {
                "strategy_id": timeframe_min,
                "family": "TEMA_RSI",
                "symbol": "BTC_USDT",
                "timeframe_min": timeframe_min,
                "direction": "long",
                "cand_score": score,
                "metric_sharpe": 2.0,
                "metric_cagr_pct": 25.0,
                "metric_max_drawdown_pct": 8.0,
                "allocation_pct": 10.0,
                "created_at": f"2026-03-0{min(timeframe_min, 9)}T00:00:00+00:00",
                "candidate_source": "template",
                "has_metrics": True,
                "param_fast_len": 9,
                "param_slow_len": 30,
            }
        )

    augmented_df, source_counts, augmented_counts, diagnostics = runtime._augment_directional_coverage(
        pd.DataFrame(rows),
        top_n=6,
    )
    pool_df = runtime._candidate_pool(augmented_df, top_n=6, max_per_group=3)

    assert source_counts["short"] == 0
    assert augmented_counts["short"] == 3
    assert int(diagnostics["mirror_diversified_source_count"]) >= 3
    short_pool = pool_df[pool_df["direction"] == "short"].copy()
    assert len(short_pool) == 3
    assert set(short_pool["timeframe_min"]) == {5, 60, 240}


def test_short_exploration_preselection_prioritizes_distinct_groups():
    runtime = HolyGrailRuntime(bt_module=object(), log=lambda _msg: None)
    rows = []

    for idx, timeframe_min in enumerate((60, 240, 1440), start=1):
        rows.append(
            {
                "strategy_id": 1000 + idx,
                "family": f"LongFamily{idx}",
                "symbol": f"LONG_{idx}_USDT",
                "timeframe_min": timeframe_min,
                "direction": "long",
                "cand_score": 10.0 - idx,
                "metric_sharpe": 2.5 - (idx * 0.1),
                "metric_cagr_pct": 30.0 - idx,
                "metric_max_drawdown_pct": 8.0 + idx,
                "allocation_pct": 10.0,
                "created_at": f"2026-03-0{idx}T00:00:00+00:00",
                "candidate_source": "template",
                "has_metrics": True,
                "param_fast_len": 9,
                "param_slow_len": 30,
            }
        )

    for rank, (family, symbol, timeframe_min) in enumerate(
        [
            ("MirrorAlpha", "BTC_USDT", 240),
            ("MirrorAlpha", "BTC_USDT", 240),
            ("MirrorAlpha", "BTC_USDT", 240),
            ("MirrorBeta", "ETH_USDT", 60),
            ("MirrorBeta", "ETH_USDT", 60),
            ("MirrorGamma", "INJ_USDT", 15),
        ],
        start=1,
    ):
        rows.append(
            {
                "strategy_id": 2000 + rank,
                "family": family,
                "symbol": symbol,
                "timeframe_min": timeframe_min,
                "direction": "short",
                "cand_score": 20.0 - rank,
                "allocation_pct": 5.0,
                "created_at": f"2026-03-{10 + rank:02d}T00:00:00+00:00",
                "candidate_source": "synthetic_mirror",
                "has_metrics": False,
                "param_fast_len": 9 + rank,
                "param_slow_len": 30 + rank,
            }
        )

    pool_df = runtime._candidate_pool(pd.DataFrame(rows), top_n=8, max_per_group=3)
    short_pool = pool_df[pool_df["direction"] == "short"].copy()

    assert len(short_pool) == 4
    distinct_groups = {
        (str(row["family"]), str(row["symbol"]), int(row["timeframe_min"]))
        for _, row in short_pool.iterrows()
    }
    assert len(distinct_groups) >= 3
    assert ("MirrorBeta", "ETH_USDT", 60) in distinct_groups
    assert ("MirrorGamma", "INJ_USDT", 15) in distinct_groups


def test_balanced_pairing_maximizes_pair_count_before_score():
    runtime = HolyGrailRuntime(bt_module=object(), log=lambda _msg: None)

    def _candidate(curve_key, *, sharpe, cagr, cand_score):
        return {
            "curve_key": curve_key,
            "perf": {
                "sharpe": sharpe,
                "cagr_pct": cagr,
                "max_drawdown_pct": 5.0,
            },
            "row_data": {
                "cand_score": cand_score,
            },
        }

    long_candidates = [
        _candidate("L1", sharpe=4.5, cagr=45.0, cand_score=11.0),
        _candidate("L2", sharpe=3.3, cagr=31.0, cand_score=8.5),
        _candidate("L3", sharpe=3.1, cagr=29.0, cand_score=8.0),
    ]
    short_candidates = [
        _candidate("S1", sharpe=4.0, cagr=41.0, cand_score=10.0),
        _candidate("S2", sharpe=1.2, cagr=14.0, cand_score=2.5),
    ]
    corr_matrix = pd.DataFrame(
        [
            [1.0, 0.95, 0.95, 0.10, 0.10],
            [0.95, 1.0, 0.10, 0.10, 0.10],
            [0.95, 0.10, 1.0, 0.10, 0.10],
            [0.10, 0.10, 0.10, 1.0, 0.10],
            [0.10, 0.10, 0.10, 0.10, 1.0],
        ],
        index=["L1", "L2", "L3", "S1", "S2"],
        columns=["L1", "L2", "L3", "S1", "S2"],
    )

    selected_pairs, feasible_edges = runtime._select_balanced_pairing(
        long_candidates=long_candidates,
        short_candidates=short_candidates,
        corr_matrix=corr_matrix,
        corr_threshold=0.80,
        max_pairs=2,
    )

    assert feasible_edges == 6
    assert len(selected_pairs) == 2
    selected_curve_keys = {(left["curve_key"], right["curve_key"]) for left, right in selected_pairs}
    assert ("L2", "S1") in selected_curve_keys
    assert ("L3", "S2") in selected_curve_keys


def test_balanced_selection_stops_at_paired_count_when_one_side_underfills(monkeypatch, tmp_path):
    monkeypatch.setenv("SHEEP_RUNTIME_DIR", str(tmp_path / "runtime"))

    def _fake_fetch(self):
        return (
            [
                {
                    "strategy_id": 1,
                    "symbol": "PAIR_USDT",
                    "timeframe_min": 60,
                    "pool_name": "pool",
                    "progress": {
                        "checkpoint_candidates": [
                            {
                                "score": 10.0,
                                "direction": "long",
                                "params": {
                                    "family": "LongAlpha",
                                    "tp": 0.01,
                                    "sl": 0.02,
                                    "max_hold": 10,
                                    "family_params": {"curve_key": "LONG_1", "sharpe": 2.3},
                                },
                                "metrics": {"sharpe": 2.3},
                            },
                            {
                                "score": 9.8,
                                "direction": "long",
                                "params": {
                                    "family": "LongBeta",
                                    "tp": 0.01,
                                    "sl": 0.02,
                                    "max_hold": 10,
                                    "family_params": {"curve_key": "LONG_2", "sharpe": 2.0},
                                },
                                "metrics": {"sharpe": 2.0},
                            },
                            {
                                "score": 9.7,
                                "direction": "short",
                                "params": {
                                    "family": "ShortAlpha",
                                    "tp": 0.01,
                                    "sl": 0.02,
                                    "max_hold": 10,
                                    "family_params": {"curve_key": "SHORT_1", "sharpe": 1.9},
                                },
                                "metrics": {"sharpe": 1.9},
                            },
                        ]
                    },
                }
            ],
            "https://example.com/api",
            "static-token",
        )

    monkeypatch.setattr(HolyGrailRuntime, "fetch_factor_pool_data", _fake_fetch)
    monkeypatch.setattr(HolyGrailRuntime, "load_kline_data", lambda self, symbol, timeframe_min: pd.DataFrame({"ts": pd.date_range("2026-01-01", periods=6, freq="D", tz="UTC"), "open": [1] * 6, "high": [1] * 6, "low": [1] * 6, "close": [1] * 6, "volume": [1] * 6}))
    monkeypatch.setattr(
        HolyGrailRuntime,
        "build_daily_equity_curve",
        staticmethod(
            lambda trades: {
                "LONG_1": _equity_from_returns([0.02, 0.0, -0.02, 0.0, 0.02, 0.0]),
                "LONG_2": _equity_from_returns([0.01, -0.01, 0.005, -0.005, 0.007, -0.007]),
                "SHORT_1": _equity_from_returns([0.0, 0.02, 0.0, -0.02, 0.0, 0.02]),
            }[trades[0]["curve_key"]]
        ),
    )

    result = run_holy_grail_build(
        bt_module=_SyntheticBT({"LONG_1": [0.01, 0.015], "LONG_2": [0.008, 0.012], "SHORT_1": [0.007, 0.011]}),
        log=lambda _msg: None,
    )

    assert result.ok
    assert result.selected_count == 2
    assert {row["direction"] for row in result.selected_portfolio} == {"long", "short"}
    full_report = pd.read_csv(result.report_paths["final_report"])
    rejected_balance = full_report[full_report["selection_status"] == "rejected_balance"]
    assert not rejected_balance.empty
    assert "LongBeta" in set(rejected_balance["family"])


def test_long_only_source_generates_mirrored_short_candidates(monkeypatch, tmp_path):
    monkeypatch.setenv("SHEEP_RUNTIME_DIR", str(tmp_path / "runtime"))

    def _fake_fetch(self):
        return (
            [
                {
                    "strategy_id": 1,
                    "symbol": "PAIR_USDT",
                    "timeframe_min": 60,
                    "pool_name": "pool",
                    "progress": {
                        "checkpoint_candidates": [
                            {
                                "score": 10.0,
                                "direction": "long",
                                "params": {
                                    "family": "LongAlpha",
                                    "tp": 0.01,
                                    "sl": 0.02,
                                    "max_hold": 10,
                                    "family_params": {"curve_key": "LONG_1", "sharpe": 2.5},
                                },
                                "metrics": {"sharpe": 2.5, "cagr_pct": 32.0, "max_drawdown_pct": 6.0},
                            },
                            {
                                "score": 9.7,
                                "direction": "long",
                                "params": {
                                    "family": "LongBeta",
                                    "tp": 0.01,
                                    "sl": 0.02,
                                    "max_hold": 10,
                                    "family_params": {"curve_key": "LONG_2", "sharpe": 2.1},
                                },
                                "metrics": {"sharpe": 2.1, "cagr_pct": 28.0, "max_drawdown_pct": 7.5},
                            },
                        ]
                    },
                }
            ],
            "https://example.com/api",
            "static-token",
        )

    monkeypatch.setattr(HolyGrailRuntime, "fetch_factor_pool_data", _fake_fetch)
    monkeypatch.setattr(
        HolyGrailRuntime,
        "load_kline_data",
        lambda self, symbol, timeframe_min: pd.DataFrame(
            {
                "ts": pd.date_range("2026-01-01", periods=6, freq="D", tz="UTC"),
                "open": [1] * 6,
                "high": [1] * 6,
                "low": [1] * 6,
                "close": [1] * 6,
                "volume": [1] * 6,
            }
        ),
    )
    monkeypatch.setattr(
        HolyGrailRuntime,
        "build_daily_equity_curve",
        staticmethod(
            lambda trades: {
                "LONG_1": _equity_from_returns([0.02, 0.015, -0.01, 0.01, 0.015, 0.0]),
                "LONG_2": _equity_from_returns([0.015, 0.01, -0.008, 0.006, 0.012, -0.003]),
                "LONG_1__mirror_short": _equity_from_returns([0.012, 0.0, 0.015, -0.006, 0.01, 0.008]),
                "LONG_2__mirror_short": _equity_from_returns([0.01, -0.004, 0.012, -0.002, 0.011, 0.004]),
            }[trades[0]["curve_key"]]
        ),
    )

    result = run_holy_grail_build(
        bt_module=_SyntheticBT(
            {
                "LONG_1": [0.01, 0.012, 0.009],
                "LONG_2": [0.008, 0.01, 0.007],
                "LONG_1__mirror_short": [0.011, 0.009, 0.01],
                "LONG_2__mirror_short": [0.01, 0.008, 0.009],
            }
        ),
        log=lambda _msg: None,
    )

    assert result.ok
    assert result.selected_count >= 2
    assert {row["direction"] for row in result.selected_portfolio} == {"long", "short"}
    assert any(row["candidate_source"] == "synthetic_mirror" for row in result.selected_portfolio)
    summary_df = pd.read_csv(result.report_paths["summary_report"])
    assert int(summary_df.iloc[0]["source_short_candidates"]) == 0
    assert int(summary_df.iloc[0]["augmented_short_candidates"]) > 0
    assert int(summary_df.iloc[0]["augmented_short_distinct_groups"]) > 0
    assert int(summary_df.iloc[0]["candidate_pool_unique_short_groups"]) > 0
    assert int(summary_df.iloc[0]["balanced_pair_count_max_possible"]) >= 1
