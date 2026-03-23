import importlib.util
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


import sheep_worker_client


def _reset_db_module():
    sys.modules.pop("sheep_platform_db", None)


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


def test_factor_pool_updater_accepts_token_only_and_syncs_cached_snapshot_on_failure(monkeypatch):
    module = _load_live_trader_module()
    logs = []
    module.log = logs.append
    module.bt = type("_BT", (), {"NUMBA_OK": False})()

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

    def _fake_post(url, json=None, headers=None, timeout=None, verify=None):
        sync_calls.append({"url": url, "json": json, "headers": headers})
        return _SyncResp()

    module.run_holy_grail_build = _fake_build
    monkeypatch.setattr(module.requests, "post", _fake_post)

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
    assert len(sync_calls) == 4
    assert all(call["headers"] == {"Authorization": "Bearer runtime-token"} for call in sync_calls)
    assert [call["json"]["source"] for call in sync_calls] == [
        "holy_grail_cached_in_progress",
        "holy_grail_cached_in_progress",
        "holy_grail_cached_failure",
        "holy_grail_cached_failure",
    ]
    assert all(call["json"]["items"][0]["direction"] == "short" for call in sync_calls)
    assert any("保留上一版有效的對沖組合" in msg for msg in logs)


def test_factor_pool_updater_refreshes_cached_runtime_snapshot_before_rebuild(monkeypatch):
    module = _load_live_trader_module()
    logs = []
    module.log = logs.append
    module.bt = type("_BT", (), {"NUMBA_OK": True})()

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

    def _fake_post(url, json=None, headers=None, timeout=None, verify=None):
        sync_calls.append({"url": url, "json": json, "headers": headers})
        return _SyncResp()

    module.run_holy_grail_build = _fake_build
    monkeypatch.setattr(module.requests, "post", _fake_post)

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

        def update_multi_json(self, _payload):
            return None

    updater = module.FactorPoolUpdater(_DummyUI())
    updater._build_holy_grail()

    assert len(sync_calls) == 4
    assert sync_calls[0]["json"]["source"] == "holy_grail_cached_in_progress"
    assert sync_calls[1]["json"]["source"] == "holy_grail_cached_in_progress"
    assert sync_calls[2]["json"]["source"] == "holy_grail_runtime"
    assert sync_calls[3]["json"]["source"] == "holy_grail_runtime"
    assert sync_calls[0]["json"]["items"][0]["direction"] == "short"
    assert sync_calls[2]["json"]["items"][0]["direction"] == "long"


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

    updater = module.FactorPoolUpdater(_DummyUI())
    payload = updater._runtime_sync_payload("global", _Result(), {})

    position_items = payload["summary"]["position_items"]
    assert len(position_items) == 1
    assert position_items[0]["strategy_key"] == "alpha-long"
    assert position_items[0]["symbol"] == "BTCUSDT"
    assert position_items[0]["direction"] == "long"
    assert position_items[0]["position_usdt"] > 0
    assert position_items[0]["unrealized_pnl_usdt"] > 0


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
