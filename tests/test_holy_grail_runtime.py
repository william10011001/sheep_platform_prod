import importlib
import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import sheep_runtime_paths as paths
import sheep_holy_grail_runtime as holy_runtime_mod
from sheep_holy_grail_runtime import HolyGrailRuntime, run_holy_grail_build


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

    missing = runtime.load_kline_data("SOL_USDT", 240)
    assert missing is None
    assert any("missing kline" in msg.lower() for msg in runtime._warning_messages)


def test_fetch_factor_pool_data_supports_token_and_pagination(monkeypatch):
    calls = {"get_pages": [], "post_calls": 0}

    class _Resp:
        def __init__(self, status_code=200, body=None):
            self.status_code = status_code
            self._body = body or {}
            self.text = json.dumps(self._body, ensure_ascii=False)

        def json(self):
            return self._body

    def _fake_get(url, params=None, headers=None, verify=None, timeout=None):
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

    def _unexpected_post(*args, **kwargs):
        calls["post_calls"] += 1
        raise AssertionError("token fetch should not need password login")

    monkeypatch.setattr(holy_runtime_mod.requests, "get", _fake_get)
    monkeypatch.setattr(holy_runtime_mod.requests, "post", _unexpected_post)

    runtime = HolyGrailRuntime(
        bt_module=object(),
        log=lambda _msg: None,
        factor_pool_url="https://example.com",
        factor_pool_token="static-token",
    )
    strategies, api_base = runtime.fetch_factor_pool_data()

    assert api_base == "https://example.com/api"
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
    monkeypatch.setenv("SHEEP_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("SHEEP_RUNTIME_DIR", str(tmp_path / "runtime"))

    _write_ohlcv(
        tmp_path / "data" / "UNITRUN_USDT_1h_3y.csv",
        [
            {"ts": "2026-01-01T00:00:00+00:00", "open": 100, "high": 101, "low": 99, "close": 100.5, "volume": 10},
            {"ts": "2026-01-01T01:00:00+00:00", "open": 100.5, "high": 102, "low": 100, "close": 101.5, "volume": 12},
            {"ts": "2026-01-01T02:00:00+00:00", "open": 101.5, "high": 103, "low": 101, "close": 102.5, "volume": 11},
        ],
    )

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
                                "params": {
                                    "family": "TEMA_Cross",
                                    "tp": 0.01,
                                    "sl": 0.02,
                                    "max_hold": 10,
                                    "family_params": {"fast_len": 10, "slow_len": 30},
                                },
                                "metrics": {"sharpe": 1.5},
                            }
                        ]
                    },
                }
            ],
            "https://example.com/api",
        )

    monkeypatch.setattr(HolyGrailRuntime, "fetch_factor_pool_data", _fake_fetch)

    result = run_holy_grail_build(bt_module=_DummyBT(), log=lambda _msg: None, base_stake_pct=50.0)
    assert result.ok
    assert result.selected_count == 1
    assert result.multi_payload[0]["strategy_id"] == 1
    assert result.multi_payload[0]["interval"] == "1h"
    assert result.multi_payload[0]["stake_pct"] == 50.0
    assert result.multi_payload[0]["direction"] == "long"
    assert result.multi_payload[0]["total_return_pct"] == 5.0
    assert result.multi_payload[0]["max_drawdown_pct"] == 3.0
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
                                "params": {
                                    "family": "TEMA_Cross",
                                    "tp": 0.01,
                                    "sl": 0.02,
                                    "max_hold": 10,
                                    "family_params": {"curve_key": "unique", "sharpe": 1.5, "cagr_pct": 11.0},
                                },
                                "metrics": {"sharpe": 1.5},
                            },
                        ]
                    },
                }
            ],
            "https://example.com/api",
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
            "unique": _equity_from_returns([0.01, -0.02, 0.01, -0.02, 0.01, -0.02]),
        }
        return mapping[curve_key]

    monkeypatch.setattr(HolyGrailRuntime, "fetch_factor_pool_data", _fake_fetch)
    monkeypatch.setattr(HolyGrailRuntime, "load_kline_data", _fake_kline)
    monkeypatch.setattr(HolyGrailRuntime, "build_daily_equity_curve", staticmethod(_fake_equity))

    result = run_holy_grail_build(bt_module=_SyntheticBT({"dup": [0.01, 0.02, 0.02], "unique": [0.0, 0.01, 0.005]}), log=lambda _msg: None)
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
            "B": _equity_from_returns([0.01, 0.02, 0.01, 0.02, -0.01, -0.02, -0.01, -0.02]),
            "C": _equity_from_returns([0.011, 0.021, 0.012, 0.022, -0.011, -0.021, -0.012, -0.022]),
        }
        return mapping[curve_key]

    monkeypatch.setattr(HolyGrailRuntime, "fetch_factor_pool_data", _fake_fetch)
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
