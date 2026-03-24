import importlib
import json
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


MODULES_TO_RESET = ("sheep_platform_api", "sheep_platform_db")
FIXED_SECRET_KEY = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="


def _reset_app_modules() -> None:
    for name in MODULES_TO_RESET:
        sys.modules.pop(name, None)


def _seed_runtime_state(db_module):
    db_module.ensure_cycle_rollover()
    cycle = db_module.get_active_cycle()
    user = db_module.get_user_by_username("sheep")
    pool_id = db_module.create_factor_pool(
        cycle_id=int(cycle["id"]),
        name="Primary Pool",
        symbol="BTC_USDT",
        timeframe_min=60,
        years=2,
        family="trend",
        grid_spec={"alpha": [1, 2]},
        risk_spec={"max_leverage": 2},
        num_partitions=8,
        seed=7,
        active=True,
    )[0]

    now = db_module._now_iso()
    conn = db_module._conn()
    try:
        conn.execute(
            """
            INSERT INTO strategies (
                submission_id, user_id, pool_id, params_json, status,
                allocation_pct, note, created_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, int(user["id"]), int(pool_id), json.dumps({"window": 20}), "active", 10.0, "seeded", now, now),
        )
        task_cur = conn.execute(
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
                8,
                "assigned",
                json.dumps({"oos_status": "queued"}),
                now,
                now,
            ),
        )
        conn.commit()
        task_id = int(task_cur.lastrowid)
    finally:
        conn.close()

    return {
        "cycle_id": int(cycle["id"]),
        "user_id": int(user["id"]),
        "pool_id": int(pool_id),
        "task_id": task_id,
    }


def _insert_task(db_module, *, user_id: int, pool_id: int, cycle_id: int, status: str, progress: dict) -> int:
    now = db_module._now_iso()
    conn = db_module._conn()
    try:
        cur = conn.execute(
            """
            INSERT INTO mining_tasks (
                user_id, pool_id, cycle_id, partition_idx, num_partitions,
                status, progress_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(user_id),
                int(pool_id),
                int(cycle_id),
                1,
                8,
                str(status),
                json.dumps(progress),
                now,
                now,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def _attach_review_pipeline(
    db_module,
    *,
    task_id: int,
    user_id: int,
    pool_id: int,
    with_submission: bool = True,
    with_active_strategy: bool = True,
) -> dict:
    now = db_module._now_iso()
    conn = db_module._conn()
    try:
        candidate_cur = conn.execute(
            """
            INSERT INTO candidates (
                task_id, user_id, pool_id, params_json, metrics_json, score, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(task_id),
                int(user_id),
                int(pool_id),
                json.dumps({"family": "TEMA_Cross", "fast_len": 12, "slow_len": 50}),
                json.dumps({"sharpe": 1.8, "trades": 88}),
                1.8,
                now,
            ),
        )
        candidate_id = int(candidate_cur.lastrowid)

        submission_id = None
        strategy_id = None
        if with_submission:
            submission_cur = conn.execute(
                """
                INSERT INTO submissions (
                    candidate_id, user_id, pool_id, status, audit_json, submitted_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    int(candidate_id),
                    int(user_id),
                    int(pool_id),
                    "approved",
                    json.dumps({"source": "test"}),
                    now,
                ),
            )
            submission_id = int(submission_cur.lastrowid)
            conn.execute("UPDATE candidates SET is_submitted = 1 WHERE id = ?", (candidate_id,))

            if with_active_strategy:
                strategy_cur = conn.execute(
                    """
                    INSERT INTO strategies (
                        submission_id, user_id, pool_id, params_json, status,
                        allocation_pct, note, created_at, expires_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(submission_id),
                        int(user_id),
                        int(pool_id),
                        json.dumps({"family": "TEMA_Cross"}),
                        "active",
                        5.0,
                        "review-pipeline-test",
                        now,
                        "2099-12-31T23:59:59+00:00",
                    ),
                )
                strategy_id = int(strategy_cur.lastrowid)

        conn.commit()
        return {
            "candidate_id": candidate_id,
            "submission_id": submission_id,
            "strategy_id": strategy_id,
        }
    finally:
        conn.close()


def _query_sys_events(db_module, event_type: str):
    conn = db_module._conn()
    try:
        rows = conn.execute(
            "SELECT event_type, message, detail_json FROM sys_monitor_events WHERE event_type = ? ORDER BY rowid ASC",
            (str(event_type),),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


@pytest.fixture()
def admin_client(tmp_path, monkeypatch):
    db_path = tmp_path / "admin-regression.sqlite3"
    monkeypatch.setenv("SHEEP_DB_URL", "")
    monkeypatch.setenv("SHEEP_DB_PATH", str(db_path))
    monkeypatch.setenv("SHEEP_COMPUTE_USER", "sheep")
    monkeypatch.setenv("SHEEP_COMPUTE_PASS", "@@Wm105020")
    monkeypatch.setenv("SHEEP_SECRET_KEY", FIXED_SECRET_KEY)
    monkeypatch.setenv("SHEEP_SKIP_SIGNATURE_CHECK", "true")

    _reset_app_modules()
    api_module = importlib.import_module("sheep_platform_api")
    db_module = sys.modules["sheep_platform_db"]

    seeded = _seed_runtime_state(db_module)
    client = TestClient(api_module.app)

    login = client.post(
        "/token",
        json={
            "username": "sheep",
            "password": "@@Wm105020",
            "name": "compute",
            "ttl_seconds": 3600,
        },
    )
    assert login.status_code == 200, login.text
    token = login.json()["token"]

    seeded.update(
        {
            "client": client,
            "db": db_module,
            "headers": {"Authorization": f"Bearer {token}"},
        }
    )
    web_token = db_module.create_api_token(seeded["user_id"], ttl_seconds=3600, name="web_session")["token"]
    seeded["web_headers"] = {"Authorization": f"Bearer {web_token}"}

    try:
        yield seeded
    finally:
        client.close()
        _reset_app_modules()


def test_threshold_alias_round_trip_persists_candidate_keep_top_n(admin_client):
    client = admin_client["client"]
    headers = admin_client["headers"]
    db_module = admin_client["db"]

    initial = client.get("/settings/thresholds", headers=headers)
    assert initial.status_code == 200
    initial_body = initial.json()
    assert initial_body["candidate_keep_top_n"] == 30
    assert initial_body["keep_top_n"] == 30

    update = client.post(
        "/admin/settings/thresholds",
        headers=headers,
        json={
            "min_trades": 11,
            "min_total_return_pct": 2.5,
            "max_drawdown_pct": 15.0,
            "min_sharpe": 1.2,
            "keep_top_n": 5,
        },
    )
    assert update.status_code == 200, update.text

    thresholds = client.get("/settings/thresholds", headers=headers).json()
    snapshot = client.get("/settings/snapshot", headers=headers).json()["thresholds"]
    assert thresholds["min_trades"] == 11
    assert thresholds["min_total_return_pct"] == 2.5
    assert thresholds["max_drawdown_pct"] == 15.0
    assert thresholds["min_sharpe"] == 1.2
    assert thresholds["candidate_keep_top_n"] == 5
    assert thresholds["keep_top_n"] == 5
    assert snapshot["candidate_keep_top_n"] == 5
    assert snapshot["keep_top_n"] == 5

    conn = db_module._conn()
    try:
        assert int(db_module.get_setting(conn, "candidate_keep_top_n", 0)) == 5
    finally:
        conn.close()

    alias_events = _query_sys_events(db_module, "DEPRECATED_ALIAS_USED")
    assert any("/admin/settings/thresholds" in row["message"] for row in alias_events)


def test_admin_routes_and_aliases_return_seeded_data(admin_client):
    client = admin_client["client"]
    headers = admin_client["headers"]
    db_module = admin_client["db"]
    pool_id = admin_client["pool_id"]
    cycle_id = admin_client["cycle_id"]

    pools = client.get("/admin/factor_pools", headers=headers)
    strategies = client.get("/admin/strategies", headers=headers)
    assert pools.status_code == 200, pools.text
    assert strategies.status_code == 200, strategies.text

    pools_body = pools.json()
    strategies_body = strategies.json()
    assert pools_body["cycle_id"] == cycle_id
    assert len(pools_body["pools"]) == 1
    assert strategies_body["total"] == 1

    alias_pools = client.get("/admin/pools", headers=headers)
    alias_strategies = client.get("/api/trading/strategies", headers=headers)
    assert alias_pools.status_code == 200
    assert alias_pools.json()["pools"][0]["id"] == pool_id
    assert alias_strategies.status_code == 200
    assert alias_strategies.json()["total"] == 1

    pool = pools_body["pools"][0]
    update = client.post(
        f"/admin/pools/{pool_id}/update",
        headers=headers,
        json={
            "name": "Primary Pool Updated",
            "symbol": pool["symbol"],
            "timeframe_min": pool["timeframe_min"],
            "years": pool["years"],
            "family": pool["family"],
            "grid_spec": pool["grid_spec"],
            "risk_spec": pool["risk_spec"],
            "num_partitions": pool["num_partitions"],
            "seed": pool["seed"],
            "active": bool(pool["active"]),
        },
    )
    assert update.status_code == 200, update.text

    refreshed = client.get("/admin/factor_pools", headers=headers).json()["pools"][0]
    assert refreshed["name"] == "Primary Pool Updated"

    alias_events = _query_sys_events(db_module, "DEPRECATED_ALIAS_USED")
    assert any("/admin/pools -> /admin/factor_pools" in row["message"] for row in alias_events)
    assert any("/api/trading/strategies -> /admin/strategies" in row["message"] for row in alias_events)
    assert any(f"/admin/pools/{pool_id}/update" in row["message"] for row in alias_events)


def test_admin_strategy_pagination_live_summary_and_error_export(admin_client):
    client = admin_client["client"]
    headers = admin_client["headers"]
    db_module = admin_client["db"]
    user_id = admin_client["user_id"]
    pool_id = admin_client["pool_id"]

    now = db_module._now_iso()
    conn = db_module._conn()
    try:
        conn.execute(
            """
            INSERT INTO strategies (
                submission_id, user_id, pool_id, params_json, status,
                allocation_pct, note, created_at, expires_at, direction, external_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                2,
                int(user_id),
                int(pool_id),
                json.dumps({"family": "TEMA_Cross", "interval": "30m"}),
                "active",
                15.0,
                "extra-active-short",
                now,
                "2099-12-31T23:59:59+00:00",
                "short",
                "extra-short-key",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    paged = client.get(
        "/admin/strategies?page=1&page_size=1&direction=short&q=extra-short-key",
        headers=headers,
    )
    assert paged.status_code == 200, paged.text
    body = paged.json()
    assert body["ok"] is True
    assert body["page"] == 1
    assert body["page_size"] == 1
    assert body["total"] >= 1
    assert len(body["items"]) == 1
    assert body["items"][0]["direction"] == "short"
    assert "in_runtime" in body["items"][0]
    assert "summary" in body

    db_module.log_sys_event("CAPTCHA_GEN_ERROR", user_id, "captcha generation failed", {"source": "test"})
    export_res = client.get("/admin/errors/export.txt", headers=headers)
    assert export_res.status_code == 200, export_res.text
    assert export_res.headers["content-type"].startswith("text/plain")
    export_text = export_res.text
    assert "timestamp | source | event_type | user | worker | message | detail_json" in export_text
    assert "CAPTCHA_GEN_ERROR" in export_text


def test_system_diagnostics_and_stop_route_reflect_runtime_state(admin_client):
    client = admin_client["client"]
    headers = admin_client["headers"]
    db_module = admin_client["db"]
    user_id = admin_client["user_id"]
    cycle_id = admin_client["cycle_id"]

    db_module.set_user_run_enabled(user_id, True)
    stop = client.post("/tasks/stop", headers=headers)
    assert stop.status_code == 200, stop.text
    assert db_module.get_user_run_enabled(user_id) is False

    diagnostics = client.get("/admin/system_diagnostics", headers=headers)
    assert diagnostics.status_code == 200, diagnostics.text
    body = diagnostics.json()
    assert body["db_kind"] == "sqlite"
    assert body["active_cycle"]["id"] == cycle_id
    assert body["active_pool_count"] == 1
    assert body["active_strategy_count"] == 1
    assert body["thresholds"]["candidate_keep_top_n"] == body["thresholds"]["keep_top_n"]
    assert body["threshold_updated_at"]["candidate_keep_top_n"]


def test_dashboard_start_route_workers_token_and_task_review_fields(admin_client):
    client = admin_client["client"]
    headers = admin_client["headers"]
    web_headers = admin_client["web_headers"]
    db_module = admin_client["db"]
    user_id = admin_client["user_id"]
    pool_id = admin_client["pool_id"]
    cycle_id = admin_client["cycle_id"]

    rejected_task_id = _insert_task(
        db_module,
        user_id=user_id,
        pool_id=pool_id,
        cycle_id=cycle_id,
        status="completed",
        progress={
            "best_any_score": -0.25,
            "best_any_passed": False,
            "review_status": "rejected",
            "review_reason": "最少交易數不足。",
            "review_failures": [
                {
                    "code": "min_trades",
                    "label": "最少交易數",
                    "actual": 30,
                    "threshold": 50,
                    "comparator": ">=",
                    "message": "最少交易數為 30，低於門檻 50。",
                }
            ],
            "oos_status": "rejected",
        },
    )
    _insert_task(
        db_module,
        user_id=user_id,
        pool_id=pool_id,
        cycle_id=cycle_id,
        status="completed",
        progress={
            "best_any_score": 1.75,
            "best_any_passed": True,
            "review_status": "auto_managed",
            "review_reason": "已通過審核並進入自動管理流程。",
            "oos_status": "auto_managed",
        },
    )
    _insert_task(
        db_module,
        user_id=user_id,
        pool_id=pool_id,
        cycle_id=cycle_id,
        status="running",
        progress={
            "best_any_score": 1.22,
            "best_any_passed": True,
        },
    )
    _insert_task(
        db_module,
        user_id=user_id,
        pool_id=pool_id,
        cycle_id=cycle_id,
        status="assigned",
        progress={
            "best_any_score": 1.09,
            "best_any_passed": True,
        },
    )

    dashboard = client.get("/dashboard", headers=headers)
    assert dashboard.status_code == 200, dashboard.text
    dash = dashboard.json()
    assert dash["personal_live_strategies_active"] == 1
    assert dash["strategies_active"] == 1
    assert dash["global_strategies_active"] == 1
    assert dash["personal_review_pipeline_count"] == 3
    assert dash["personal_runtime_portfolio_count"] == 0
    assert dash["global_runtime_portfolio_count"] == 0
    assert dash["personal_runtime_portfolio_items"] == []
    assert dash["global_runtime_portfolio_items"] == []
    assert "已達標並由系統持續追蹤" in dash["personal_review_pipeline_hint"]

    tasks = client.get("/tasks", headers=headers)
    assert tasks.status_code == 200, tasks.text
    task_rows = {int(t["id"]): t for t in tasks.json()["tasks"]}
    rejected = task_rows[rejected_task_id]
    assert rejected["review_status"] == "rejected"
    assert rejected["review_failures"][0]["code"] == "min_trades"
    assert rejected["best_any_passed"] is False

    conn = db_module._conn()
    try:
        conn.execute("DELETE FROM mining_tasks WHERE user_id = ?", (int(user_id),))
        conn.commit()
    finally:
        conn.close()

    db_module.set_user_run_enabled(user_id, False)
    start = client.post("/tasks/start", headers=headers)
    assert start.status_code == 200, start.text
    start_body = start.json()
    assert start_body["run_enabled"] is True
    assert start_body["active_cycle_id"] == cycle_id
    assert start_body["assigned_count"] >= 1
    assert start_body["task_count"] >= start_body["assigned_count"]

    old_worker = db_module.create_api_token(user_id, ttl_seconds=3600, name="worker")["token"]
    worker_issue = client.post(
        "/workers/token",
        headers=web_headers,
        json={"ttl_seconds": 7200, "rotate_existing": True},
    )
    assert worker_issue.status_code == 200, worker_issue.text
    worker_body = worker_issue.json()
    assert worker_body["token_kind"] == "worker"
    assert worker_body["token"] != old_worker
    assert worker_body["rotated_count"] >= 1
    assert db_module.verify_api_token(old_worker) is None
    fresh_worker = db_module.verify_api_token(worker_body["token"])
    assert fresh_worker is not None
    assert fresh_worker["token"]["name"] == "worker"

    db_module.set_user_run_enabled(user_id, False)
    flags_web = client.get("/flags", headers=web_headers)
    flags_worker = client.get("/flags", headers={"Authorization": f"Bearer {worker_body['token']}"})
    flags_compute = client.get("/flags", headers=headers)
    assert flags_web.status_code == 200
    assert flags_worker.status_code == 200
    assert flags_compute.status_code == 200
    assert flags_web.json()["token_kind"] == "web_session"
    assert flags_web.json()["assignment_mode"] == "personal_worker"
    assert flags_web.json()["reason"] == "legacy_web_session_token"
    assert flags_worker.json()["token_kind"] == "worker"
    assert flags_worker.json()["assignment_mode"] == "personal_worker"
    assert flags_worker.json()["reason"] == "run_disabled"
    assert flags_compute.json()["token_kind"] == "compute"
    assert flags_compute.json()["assignment_mode"] == "global_compute"


def test_runtime_portfolio_sync_updates_dashboard_personal_and_global(admin_client):
    client = admin_client["client"]
    headers = admin_client["headers"]
    db_module = admin_client["db"]
    user_id = admin_client["user_id"]
    cycle_id = admin_client["cycle_id"]

    payload_items = [
        {
            "strategy_key": "TEMA_001",
            "family": "TEMA_Cross",
            "symbol": "ETH_USDT",
            "direction": "long",
            "interval": "30m",
            "family_params": {"fast_len": 12, "slow_len": 55},
            "stake_pct": 35.5,
            "sharpe": 2.1,
            "total_return_pct": 18.5,
            "max_drawdown_pct": 4.2,
            "avg_pairwise_corr_to_selected": 0.15,
            "max_pairwise_corr_to_selected": 0.22,
        },
        {
            "strategy_key": "WMA_002",
            "family": "WMA_Cross",
            "symbol": "BTC_USDT",
            "direction": "short",
            "interval": "4h",
            "family_params": {"fast_len": 10, "slow_len": 40},
            "stake_pct": 20.0,
            "sharpe": 1.3,
            "total_return_pct": 9.4,
            "max_drawdown_pct": 2.6,
        },
    ]

    match_pool_id = db_module.create_factor_pool(
        cycle_id=int(cycle_id),
        name="ETH Runtime 30m",
        symbol="ETH_USDT",
        timeframe_min=30,
        years=2,
        family="TEMA_Cross",
        grid_spec={"alpha": [1]},
        risk_spec={"max_leverage": 2},
        num_partitions=4,
        seed=9,
        active=True,
    )[0]
    now = db_module._now_iso()
    conn = db_module._conn()
    try:
        candidate_cur = conn.execute(
            """
            INSERT INTO candidates (
                task_id, user_id, pool_id, direction, params_json, metrics_json, score, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                0,
                int(user_id),
                int(match_pool_id),
                "long",
                json.dumps({"family": "TEMA_Cross"}),
                json.dumps({"sharpe": 2.4}),
                2.4,
                now,
            ),
        )
        candidate_id = int(candidate_cur.lastrowid)
        submission_cur = conn.execute(
            """
            INSERT INTO submissions (candidate_id, user_id, pool_id, status, audit_json, submitted_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                candidate_id,
                int(user_id),
                int(match_pool_id),
                "approved",
                json.dumps({"source": "runtime-test"}),
                now,
            ),
        )
        submission_id = int(submission_cur.lastrowid)
        strategy_cur = conn.execute(
            """
            INSERT INTO strategies (
                submission_id, user_id, pool_id, external_key, direction, params_json, status,
                allocation_pct, note, created_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                submission_id,
                int(user_id),
                int(match_pool_id),
                "db-runtime-match",
                "long",
                json.dumps({"family": "TEMA_Cross"}),
                "active",
                10.0,
                "runtime-match",
                now,
                "2099-12-31T23:59:59+00:00",
            ),
        )
        strategy_id = int(strategy_cur.lastrowid)
        conn.commit()
    finally:
        conn.close()
    db_module.create_weekly_check(
        strategy_id=strategy_id,
        week_start_ts=now,
        week_end_ts=now,
        return_pct=16.2,
        max_drawdown_pct=3.7,
        trades=18,
        eligible=True,
    )

    personal = client.post(
        "/runtime/portfolio/sync",
        json={
            "scope": "personal",
            "username": "sheep",
            "password": "@@Wm105020",
            "summary": {"portfolio_metrics": {"sharpe": 1.9}},
            "items": payload_items,
        },
    )
    assert personal.status_code == 200, personal.text
    assert personal.json()["snapshot"]["strategy_count"] == 2

    issued = client.post(
        "/runtime-sync/token",
        headers=headers,
        json={"ttl_seconds": 7200, "rotate_existing": True},
    )
    assert issued.status_code == 200, issued.text
    issued_body = issued.json()
    assert issued_body["token_kind"] == "system_sync"

    global_sync = client.post(
        "/runtime/portfolio/sync",
        headers={"Authorization": f"Bearer {issued_body['token']}"},
        json={
            "scope": "global",
            "summary": {"portfolio_metrics": {"sharpe": 1.9}},
            "items": payload_items,
        },
    )
    assert global_sync.status_code == 200, global_sync.text

    dashboard = client.get("/dashboard", headers=headers)
    assert dashboard.status_code == 200, dashboard.text
    dash = dashboard.json()
    assert dash["personal_runtime_portfolio_count"] == 2
    assert dash["global_runtime_portfolio_count"] == 2
    assert dash["personal_runtime_portfolio_items"][0]["direction"] == "long"
    assert dash["global_runtime_portfolio_items"][1]["direction"] == "short"
    assert int(dash["global_runtime_portfolio_items"][0]["strategy_id"]) == int(strategy_id)
    assert dash["global_runtime_portfolio_items"][0]["total_return_pct"] == pytest.approx(18.5)
    assert dash["global_runtime_portfolio_items"][0]["max_drawdown_pct"] == pytest.approx(4.2)
    assert dash["global_runtime_portfolio_updated_at"]
    assert dash["runtime_sync"]["global"]["last_success_at"]
    assert dash["runtime_sync"]["personal"]["last_success_at"]
    assert dash["runtime_sync"]["global"]["count_mismatch"] is False
    assert dash["runtime_sync"]["global_active_strategy_mismatch"] is False


def test_runtime_portfolio_sync_preserves_raw_strategy_id_without_active_match(admin_client):
    client = admin_client["client"]
    headers = admin_client["headers"]

    issued = client.post(
        "/runtime-sync/token",
        headers=headers,
        json={"ttl_seconds": 7200, "rotate_existing": True},
    )
    assert issued.status_code == 200, issued.text
    token = issued.json()["token"]

    sync_resp = client.post(
        "/runtime/portfolio/sync",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "scope": "global",
            "summary": {"portfolio_metrics": {"sharpe": 2.2}},
            "items": [
                {
                    "strategy_id": 321,
                    "strategy_key": "runtime-321",
                    "family": "TEMA_RSI",
                    "symbol": "ETH_USDT",
                    "direction": "long",
                    "interval": "1d",
                    "family_params": {"fast_len": 9, "slow_len": 30},
                    "stake_pct": 45.35,
                    "sharpe": 4.39,
                    "total_return_pct": 57.47,
                    "max_drawdown_pct": 3.37,
                }
            ],
        },
    )
    assert sync_resp.status_code == 200, sync_resp.text

    dashboard = client.get("/dashboard", headers=headers)
    assert dashboard.status_code == 200, dashboard.text
    item = dashboard.json()["global_runtime_portfolio_items"][0]
    assert int(item["strategy_id"]) == 321
    assert item["total_return_pct"] == pytest.approx(57.47)
    assert item["max_drawdown_pct"] == pytest.approx(3.37)


def test_runtime_portfolio_strategy_id_lookup_recovers_owner_identity(admin_client):
    client = admin_client["client"]
    headers = admin_client["headers"]
    db_module = admin_client["db"]
    user_id = admin_client["user_id"]

    db_module.update_user_profile(user_id, nickname="Sheep Miner")
    conn = db_module._conn()
    try:
        row = conn.execute(
            "SELECT id FROM strategies WHERE user_id = ? AND status = 'active' ORDER BY id DESC LIMIT 1",
            (int(user_id),),
        ).fetchone()
        assert row is not None
        strategy_id = int(row["id"])
    finally:
        conn.close()

    issued = client.post(
        "/runtime-sync/token",
        headers=headers,
        json={"ttl_seconds": 7200, "rotate_existing": True},
    )
    assert issued.status_code == 200, issued.text
    token = issued.json()["token"]

    sync_resp = client.post(
        "/runtime/portfolio/sync",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "scope": "global",
            "summary": {"portfolio_metrics": {"sharpe": 1.1}},
            "items": [
                {
                    "strategy_id": strategy_id,
                    "strategy_key": "mismatch-runtime-key",
                    "family": "TEMA_RSI",
                    "symbol": "ETH_USDT",
                    "direction": "short",
                    "interval": "1d",
                    "family_params": {"fast_len": 9, "slow_len": 30},
                    "stake_pct": 15.0,
                    "sharpe": 1.1,
                    "total_return_pct": 12.34,
                    "max_drawdown_pct": 2.22,
                }
            ],
        },
    )
    assert sync_resp.status_code == 200, sync_resp.text

    dashboard = client.get("/dashboard", headers=headers)
    assert dashboard.status_code == 200, dashboard.text
    item = dashboard.json()["global_runtime_portfolio_items"][0]
    assert int(item["strategy_id"]) == strategy_id
    assert item["owner_username"] == "sheep"
    assert item["owner_nickname"] == "Sheep Miner"
    assert item["owner_avatar_url"]


def test_dashboard_prefers_previous_publishable_runtime_snapshot_and_backfills_metrics(admin_client):
    client = admin_client["client"]
    headers = admin_client["headers"]
    db_module = admin_client["db"]
    user_id = admin_client["user_id"]
    cycle_id = admin_client["cycle_id"]

    match_pool_id = db_module.create_factor_pool(
        cycle_id=int(cycle_id),
        name="Runtime Recovery Pool",
        symbol="ETH_USDT",
        timeframe_min=1440,
        years=2,
        family="TEMA_RSI",
        grid_spec={"alpha": [1, 2]},
        risk_spec={"max_leverage": 2},
        num_partitions=2,
        seed=23,
        active=True,
        direction="long",
        external_key="tema-rsi-eth-1d",
    )[0]

    now = db_module._now_iso()
    conn = db_module._conn()
    try:
        strategy_cur = conn.execute(
            """
            INSERT INTO strategies (
                submission_id, user_id, pool_id, external_key, direction, params_json, status,
                allocation_pct, note, created_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                7,
                int(user_id),
                int(match_pool_id),
                "",
                "long",
                json.dumps({"family": "TEMA_RSI", "interval": "1d", "symbol": "ETH_USDT"}),
                "active",
                25.0,
                "runtime-recovery-test",
                now,
                "2099-12-31T23:59:59+00:00",
            ),
        )
        strategy_id = int(strategy_cur.lastrowid)
        conn.commit()
    finally:
        conn.close()

    db_module.create_weekly_check(
        strategy_id=strategy_id,
        week_start_ts=now,
        week_end_ts=now,
        return_pct=57.39,
        max_drawdown_pct=3.37,
        trades=24,
        eligible=True,
    )

    issued = client.post(
        "/runtime-sync/token",
        headers=headers,
        json={"ttl_seconds": 7200, "rotate_existing": True},
    )
    assert issued.status_code == 200, issued.text
    token = issued.json()["token"]

    good_sync = client.post(
        "/runtime/portfolio/sync",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "scope": "global",
            "summary": {"portfolio_metrics": {"sharpe": 4.19}},
            "items": [
                {
                    "strategy_id": strategy_id,
                    "strategy_key": "tema-rsi-eth-1d",
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
        },
    )
    assert good_sync.status_code == 200, good_sync.text

    bad_cached_sync = client.post(
        "/runtime/portfolio/sync",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "scope": "global",
            "source": "holy_grail_cached_failure",
            "summary": {"portfolio_metrics": {"sharpe": 0.0}},
            "items": [
                {
                    "strategy_id": strategy_id,
                    "strategy_key": str(strategy_id),
                    "family": "TEMA_RSI",
                    "symbol": "ETH_USDT",
                    "direction": "long",
                    "interval": "1d",
                    "family_params": {"fast_len": 9, "slow_len": 30},
                    "stake_pct": 45.35,
                    "sharpe": 0.0,
                    "total_return_pct": 0.0,
                    "max_drawdown_pct": 0.0,
                }
            ],
        },
    )
    assert bad_cached_sync.status_code == 200, bad_cached_sync.text

    dashboard = client.get("/dashboard", headers=headers)
    assert dashboard.status_code == 200, dashboard.text
    item = dashboard.json()["global_runtime_portfolio_items"][0]
    assert int(item["strategy_id"]) == strategy_id
    assert item["external_key"] == "tema-rsi-eth-1d"
    assert item["strategy_key"] in {"", "tema-rsi-eth-1d"}
    assert item["total_return_pct"] == pytest.approx(57.39)
    assert item["max_drawdown_pct"] == pytest.approx(3.37)


def test_manifest_exposes_default_worker_download_url_and_honors_override(admin_client):
    client = admin_client["client"]
    db_module = admin_client["db"]

    manifest = client.get("/manifest")
    assert manifest.status_code == 200, manifest.text
    assert manifest.json()["worker_download_url"] == db_module.DEFAULT_WORKER_DOWNLOAD_URL

    db_module.set_setting("worker_download_url", "https://example.com/OpenNode.exe")

    updated_manifest = client.get("/manifest")
    assert updated_manifest.status_code == 200, updated_manifest.text
    assert updated_manifest.json()["worker_download_url"] == "https://example.com/OpenNode.exe"


def test_dashboard_exposes_review_ready_items_for_rating_panel(admin_client):
    client = admin_client["client"]
    headers = admin_client["headers"]
    db_module = admin_client["db"]
    user_id = admin_client["user_id"]
    pool_id = admin_client["pool_id"]
    cycle_id = admin_client["cycle_id"]

    task_id = _insert_task(
        db_module,
        user_id=user_id,
        pool_id=pool_id,
        cycle_id=cycle_id,
        status="completed",
        progress={
            "best_any_score": 1.92,
            "best_any_passed": True,
            "review_status": "auto_managed",
            "oos_status": "auto_managed",
        },
    )
    review = _attach_review_pipeline(
        db_module,
        task_id=task_id,
        user_id=user_id,
        pool_id=pool_id,
        with_submission=True,
        with_active_strategy=True,
    )

    dashboard = client.get("/dashboard", headers=headers)
    assert dashboard.status_code == 200, dashboard.text
    body = dashboard.json()

    assert body["personal_reviewed_strategy_count"] >= 1
    assert body["personal_review_ready_items"], body
    assert int(body["personal_review_ready_items"][0]["strategy_id"]) == int(review["strategy_id"])
    assert body["personal_review_ready_items"][0]["review_status"] == "auto_managed"
    assert float(body["personal_review_ready_items"][0]["best_any_score"]) >= 1.8


def test_admin_catalog_import_dry_run_apply_and_upsert(admin_client):
    client = admin_client["client"]
    headers = admin_client["headers"]

    payload = {
        "schema_version": 1,
        "factor_pools": [
            {
                "key": "import_pool_eth_long_30m",
                "name": "ETH Long 30m",
                "family": "TEMA_Cross",
                "symbol": "ETH_USDT",
                "direction": "long",
                "timeframe_min": 30,
                "years": 3,
                "grid_spec": {"fast_len": [12], "slow_len": [55]},
                "risk_spec": {"max_leverage": 3},
                "num_partitions": 6,
                "seed": 9,
                "active": True,
                "auto_expand": False,
            }
        ],
        "strategies": [
            {
                "key": "import_strategy_eth_long_30m",
                "name": "ETH Runtime Long",
                "family": "TEMA_Cross",
                "symbol": "ETH_USDT",
                "direction": "long",
                "interval": "30m",
                "family_params": {"fast_len": 12, "slow_len": 55},
                "tp_pct": 1.2,
                "sl_pct": 0.8,
                "max_hold_bars": 48,
                "stake_pct": 15.0,
                "status": "active",
                "enabled": True,
            }
        ],
    }

    dry_run = client.post("/admin/catalog/import?dry_run=true", headers=headers, json=payload)
    assert dry_run.status_code == 200, dry_run.text
    dry_body = dry_run.json()
    assert dry_body["ok"] is True
    assert dry_body["factor_pools"]["create"] == 1
    assert dry_body["strategies"]["create"] == 1

    apply_res = client.post("/admin/catalog/import?dry_run=false", headers=headers, json=payload)
    assert apply_res.status_code == 200, apply_res.text
    apply_body = apply_res.json()
    assert apply_body["ok"] is True

    pools = client.get("/admin/factor_pools", headers=headers).json()["pools"]
    imported_pool = next((p for p in pools if p.get("external_key") == "import_pool_eth_long_30m"), None)
    assert imported_pool is not None
    assert imported_pool["direction"] == "long"
    assert int(imported_pool["timeframe_min"]) == 30

    strategies = client.get("/admin/strategies", headers=headers).json()["strategies"]
    imported_strategy = next((s for s in strategies if s.get("external_key") == "import_strategy_eth_long_30m"), None)
    assert imported_strategy is not None
    assert imported_strategy["direction"] == "long"

    payload["strategies"][0]["stake_pct"] = 22.0
    second_apply = client.post("/admin/catalog/import?dry_run=false", headers=headers, json=payload)
    assert second_apply.status_code == 200, second_apply.text
    second_body = second_apply.json()
    assert second_body["strategies"]["update"] >= 1


def test_admin_catalog_import_clamps_large_seed(admin_client):
    client = admin_client["client"]
    headers = admin_client["headers"]

    payload = {
        "schema_version": 1,
        "factor_pools": [
            {
                "key": "import_pool_seed_clamp",
                "name": "Seed Clamp Pool",
                "family": "TEMA_Cross",
                "symbol": "ETH_USDT",
                "direction": "long",
                "timeframe_min": 60,
                "years": 3,
                "grid_spec": {"fast_len": [12], "slow_len": [55]},
                "risk_spec": {"tp_min": 1.0, "tp_max": 1.0, "tp_step": 1.0, "sl_min": 1.0, "sl_max": 1.0, "sl_step": 1.0, "max_hold_min": 24, "max_hold_max": 24, "max_hold_step": 24},
                "num_partitions": 4,
                "seed": 4294967295,
                "active": True,
                "auto_expand": False,
            }
        ],
        "strategies": [],
    }

    apply_res = client.post("/admin/catalog/import?dry_run=false", headers=headers, json=payload)
    assert apply_res.status_code == 200, apply_res.text
    body = apply_res.json()
    assert body["ok"] is True

    pools = client.get("/admin/factor_pools", headers=headers).json()["pools"]
    imported_pool = next((p for p in pools if p.get("external_key") == "import_pool_seed_clamp"), None)
    assert imported_pool is not None
    assert int(imported_pool["seed"]) == 2147483647


def test_generated_market_catalog_dry_run_succeeds(admin_client):
    client = admin_client["client"]
    headers = admin_client["headers"]
    catalog_path = ROOT / "catalogs" / "admin_batch_market_catalog_v1.json"

    payload = json.loads(catalog_path.read_text(encoding="utf-8"))
    res = client.post("/admin/catalog/import?dry_run=true", headers=headers, json=payload)
    assert res.status_code == 200, res.text

    body = res.json()
    assert body["ok"] is True
    assert body["factor_pools"]["create"] == len(payload["factor_pools"])
    assert body["strategies"]["create"] == len(payload["strategies"])
    assert body["factor_pools"]["errors"] == []
    assert body["strategies"]["errors"] == []


def test_generated_market_catalog_uses_postgres_safe_seed_range():
    catalog_path = ROOT / "catalogs" / "admin_batch_market_catalog_v1.json"
    payload = json.loads(catalog_path.read_text(encoding="utf-8"))
    seeds = [int(item.get("seed") or 0) for item in payload["factor_pools"]]
    assert seeds
    assert max(seeds) <= 2147483647
    assert min(seeds) >= 1


def test_admin_html_includes_batch_catalog_import_controls(admin_client):
    client = admin_client["client"]

    res = client.get("/")
    assert res.status_code == 200, res.text
    html = res.text
    assert "Batch JSON Import" in html
    assert "runCatalogImport" in html
    assert "loadCatalogFile" in html
    assert "/admin/factor_pools" in html
    assert "/admin/catalog/import" in html


def test_review_state_maintenance_repairs_legacy_tasks_and_is_idempotent(admin_client):
    client = admin_client["client"]
    headers = admin_client["headers"]
    db_module = admin_client["db"]
    user_id = admin_client["user_id"]
    pool_id = admin_client["pool_id"]
    cycle_id = admin_client["cycle_id"]

    explicit_rejected_id = _insert_task(
        db_module,
        user_id=user_id,
        pool_id=pool_id,
        cycle_id=cycle_id,
        status="completed",
        progress={
            "best_any_passed": False,
            "review_status": "rejected",
            "review_reason": "未通過門檻審核。",
            "review_failures": [{"code": "min_trades", "message": "最少交易數不足。"}],
            "oos_status": "rejected",
        },
    )
    legacy_auto_id = _insert_task(
        db_module,
        user_id=user_id,
        pool_id=pool_id,
        cycle_id=cycle_id,
        status="completed",
        progress={
            "best_any_score": 2.25,
        },
    )
    _attach_review_pipeline(
        db_module,
        task_id=legacy_auto_id,
        user_id=user_id,
        pool_id=pool_id,
        with_submission=True,
        with_active_strategy=True,
    )
    legacy_rejected_id = _insert_task(
        db_module,
        user_id=user_id,
        pool_id=pool_id,
        cycle_id=cycle_id,
        status="completed",
        progress={
            "best_any_score": 0.1,
            "best_any_passed": False,
            "review_failures": [{"code": "min_sharpe", "message": "夏普值不足。"}],
            "last_reject_reason": "夏普值不足。",
        },
    )

    rebuild = client.post("/admin/maintenance/rebuild-review-state", headers=headers)
    assert rebuild.status_code == 200, rebuild.text
    rebuild_body = rebuild.json()
    assert rebuild_body["ok"] is True
    assert rebuild_body["updated"] >= 2
    assert rebuild_body["explicit_preserved"] >= 1
    assert rebuild_body["auto_managed_repairs"] >= 1
    assert rebuild_body["rejected_repairs"] >= 1

    tasks = client.get("/tasks", headers=headers)
    assert tasks.status_code == 200, tasks.text
    task_rows = {int(t["id"]): t for t in tasks.json()["tasks"]}

    assert task_rows[explicit_rejected_id]["review_status"] == "rejected"
    assert task_rows[explicit_rejected_id]["oos_status"] == "rejected"

    assert task_rows[legacy_auto_id]["best_any_passed"] is True
    assert task_rows[legacy_auto_id]["review_status"] == "auto_managed"
    assert task_rows[legacy_auto_id]["oos_status"] == "auto_managed"

    assert task_rows[legacy_rejected_id]["best_any_passed"] is False
    assert task_rows[legacy_rejected_id]["review_status"] == "rejected"
    assert task_rows[legacy_rejected_id]["oos_status"] == "rejected"

    legacy = client.post(f"/tasks/{legacy_auto_id}/submit_oos", headers=headers)
    assert legacy.status_code == 200, legacy.text
    legacy_body = legacy.json()
    assert legacy_body["review_status"] == "auto_managed"
    assert legacy_body["oos_status"] == "auto_managed"

    rebuild_again = client.post("/admin/maintenance/rebuild-review-state", headers=headers)
    assert rebuild_again.status_code == 200, rebuild_again.text
    assert rebuild_again.json()["updated"] == 0

    cli = subprocess.run(
        [sys.executable, str(APP_DIR / "sheep_review_maintenance.py"), "--dry-run", "--limit", "10"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert cli.returncode == 0, cli.stdout + cli.stderr
    cli_body = json.loads(cli.stdout)
    assert cli_body["scanned"] >= 1


def test_legacy_submit_oos_is_read_only_and_unknown_routes_return_404(admin_client):
    client = admin_client["client"]
    headers = admin_client["headers"]
    db_module = admin_client["db"]
    task_id = admin_client["task_id"]

    before = db_module.get_task(task_id)
    assert before is not None

    legacy = client.post(f"/tasks/{task_id}/submit_oos", headers=headers)
    assert legacy.status_code == 200, legacy.text
    legacy_body = legacy.json()
    assert legacy_body["deprecated"] is True
    assert legacy_body["auto_managed"] is True
    assert legacy_body["oos_status"] == "queued"

    after = db_module.get_task(task_id)
    assert after["status"] == before["status"]
    assert after["progress_json"] == before["progress_json"]

    missing = client.get("/totally/missing", headers=headers)
    assert missing.status_code == 404
    assert missing.json() == {
        "ok": False,
        "error": "route_not_found",
        "path": "/totally/missing",
        "method": "GET",
    }

    alias_events = _query_sys_events(db_module, "DEPRECATED_ALIAS_USED")
    assert any(f"/tasks/{task_id}/submit_oos" in row["message"] for row in alias_events)
    unknown_route_events = _query_sys_events(db_module, "UNKNOWN_ROUTE")
    assert any("/totally/missing" in row["message"] for row in unknown_route_events)
