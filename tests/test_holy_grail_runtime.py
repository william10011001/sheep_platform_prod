import importlib
import subprocess
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import sheep_runtime_paths as paths
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
    assert result.multi_payload[0]["interval"] == "1h"
    assert result.multi_payload[0]["stake_pct"] == 50.0
    assert Path(result.report_paths["summary_report"]).exists()
