"""Initial schema (PostgreSQL)

Revision ID: 0001_initial
Revises: 
Create Date: 2026-02-19

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


DDL = r"""
CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS users (
                id BIGSERIAL PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('user','admin')),
                wallet_address_enc BYTEA,
                created_at TEXT NOT NULL,
                last_login_at TEXT,
                disabled INTEGER NOT NULL DEFAULT 0,
                login_fail_count INTEGER NOT NULL DEFAULT 0,
                lock_until TEXT,
                run_enabled INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS cycles (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                start_ts TEXT NOT NULL,
                end_ts TEXT NOT NULL,
                created_at TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                seed INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS factor_pools (
                id BIGSERIAL PRIMARY KEY,
                cycle_id BIGINT NOT NULL REFERENCES cycles(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timeframe_min INTEGER NOT NULL,
                years INTEGER NOT NULL,
                family TEXT NOT NULL,
                grid_spec_json TEXT NOT NULL,
                risk_spec_json TEXT NOT NULL,
                num_partitions INTEGER NOT NULL,
                seed INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id BIGSERIAL PRIMARY KEY,
                cycle_id BIGINT NOT NULL REFERENCES cycles(id) ON DELETE CASCADE,
                pool_id BIGINT NOT NULL REFERENCES factor_pools(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                partition_idx INTEGER NOT NULL,
                partition_total INTEGER NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('assigned','running','completed','expired','revoked')),
                assigned_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                last_heartbeat TEXT,
                lease_id TEXT,
                lease_worker_id TEXT,
                lease_expires_at TEXT,
                attempt INTEGER NOT NULL DEFAULT 0,
                estimated_combos BIGINT NOT NULL DEFAULT 0,
                priority INTEGER NOT NULL DEFAULT 0,
                progress_json TEXT NOT NULL,
                UNIQUE(cycle_id, pool_id, partition_idx)
            );

            CREATE TABLE IF NOT EXISTS candidates (
                id BIGSERIAL PRIMARY KEY,
                task_id BIGINT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                pool_id BIGINT NOT NULL REFERENCES factor_pools(id) ON DELETE CASCADE,
                created_at TEXT NOT NULL,
                params_enc BYTEA NOT NULL,
                metrics_json TEXT NOT NULL,
                score DOUBLE PRECISION NOT NULL,
                is_submitted INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS submissions (
                id BIGSERIAL PRIMARY KEY,
                candidate_id BIGINT NOT NULL UNIQUE REFERENCES candidates(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                pool_id BIGINT NOT NULL REFERENCES factor_pools(id) ON DELETE CASCADE,
                submitted_at TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('pending','approved','rejected')),
                audit_json TEXT,
                approved_at TEXT,
                approved_by BIGINT REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS strategies (
                id BIGSERIAL PRIMARY KEY,
                submission_id BIGINT NOT NULL UNIQUE REFERENCES submissions(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                pool_id BIGINT NOT NULL REFERENCES factor_pools(id) ON DELETE CASCADE,
                cycle_id BIGINT NOT NULL REFERENCES cycles(id) ON DELETE CASCADE,
                status TEXT NOT NULL CHECK(status IN ('active','disqualified','expired','paused')),
                activated_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                allocation_pct DOUBLE PRECISION NOT NULL,
                note TEXT
            );

            CREATE TABLE IF NOT EXISTS weekly_checks (
                id BIGSERIAL PRIMARY KEY,
                strategy_id BIGINT NOT NULL REFERENCES strategies(id) ON DELETE CASCADE,
                week_start_ts TEXT NOT NULL,
                week_end_ts TEXT NOT NULL,
                computed_at TEXT NOT NULL,
                return_pct DOUBLE PRECISION NOT NULL,
                max_drawdown_pct DOUBLE PRECISION NOT NULL,
                trades INTEGER NOT NULL,
                eligible INTEGER NOT NULL,
                UNIQUE(strategy_id, week_start_ts)
            );

            CREATE TABLE IF NOT EXISTS payouts (
                id BIGSERIAL PRIMARY KEY,
                strategy_id BIGINT NOT NULL REFERENCES strategies(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                week_start_ts TEXT NOT NULL,
                amount_usdt DOUBLE PRECISION NOT NULL,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('unpaid','paid','void')),
                paid_at TEXT,
                txid TEXT
            );

            CREATE TABLE IF NOT EXISTS audit_log (
                id BIGSERIAL PRIMARY KEY,
                ts TEXT NOT NULL,
                actor_user_id BIGINT REFERENCES users(id),
                action TEXT NOT NULL,
                detail_json TEXT NOT NULL
            );


            CREATE TABLE IF NOT EXISTS api_tokens (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                issued_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                revoked_at TEXT,
                last_seen_at TEXT,
                last_ip TEXT,
                last_user_agent TEXT
            );

            CREATE TABLE IF NOT EXISTS workers (
                id BIGSERIAL PRIMARY KEY,
                worker_id TEXT NOT NULL UNIQUE,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                name TEXT,
                version TEXT NOT NULL,
                protocol INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                last_heartbeat_at TEXT NOT NULL,
                last_task_id BIGINT,
                avg_cps DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                meta_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS worker_events (
                id BIGSERIAL PRIMARY KEY,
                ts TEXT NOT NULL,
                user_id BIGINT,
                worker_id TEXT,
                event TEXT NOT NULL,
                detail_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS api_request_log (
                id BIGSERIAL PRIMARY KEY,
                ts TEXT NOT NULL,
                user_id BIGINT,
                worker_id TEXT,
                token_id BIGINT,
                method TEXT NOT NULL,
                path TEXT NOT NULL,
                status_code INTEGER NOT NULL,
                duration_ms DOUBLE PRECISION NOT NULL,
                detail_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_api_tokens_user ON api_tokens(user_id, revoked_at, expires_at);
            CREATE INDEX IF NOT EXISTS idx_workers_user ON workers(user_id, last_seen_at);
            CREATE INDEX IF NOT EXISTS idx_worker_events_ts ON worker_events(ts);
            CREATE INDEX IF NOT EXISTS idx_api_request_log_ts ON api_request_log(ts);
            CREATE INDEX IF NOT EXISTS idx_api_request_log_path_ts ON api_request_log(path, ts);
CREATE INDEX IF NOT EXISTS idx_tasks_user_cycle_status ON tasks(user_id, cycle_id, status);
            CREATE INDEX IF NOT EXISTS idx_tasks_cycle_pool_status ON tasks(cycle_id, pool_id, status);
            CREATE INDEX IF NOT EXISTS idx_tasks_cycle_status_lease ON tasks(cycle_id, status, lease_expires_at);
                CREATE INDEX IF NOT EXISTS idx_tasks_cycle_status_lease ON tasks(cycle_id, status, lease_expires_at);
            CREATE INDEX IF NOT EXISTS idx_candidates_task_score ON candidates(task_id, score);
            CREATE INDEX IF NOT EXISTS idx_submissions_status ON submissions(status, submitted_at);
            CREATE INDEX IF NOT EXISTS idx_strategies_status ON strategies(status, expires_at);
            CREATE INDEX IF NOT EXISTS idx_payouts_status ON payouts(status, created_at);
            CREATE INDEX IF NOT EXISTS idx_audit_log_ts ON audit_log(ts);
"""


def upgrade() -> None:
    # Execute DDL statement-by-statement (works across drivers).
    stmts = [s.strip() for s in DDL.split(";") if s.strip()]
    for s in stmts:
        op.execute(s + ";")


def downgrade() -> None:
    # Drop in reverse dependency order.
    # CASCADE removes dependent objects (indexes, constraints).
    op.execute("DROP TABLE IF EXISTS api_request_log CASCADE;")
    op.execute("DROP TABLE IF EXISTS worker_events CASCADE;")
    op.execute("DROP TABLE IF EXISTS workers CASCADE;")
    op.execute("DROP TABLE IF EXISTS api_tokens CASCADE;")
    op.execute("DROP TABLE IF EXISTS audit_log CASCADE;")
    op.execute("DROP TABLE IF EXISTS payouts CASCADE;")
    op.execute("DROP TABLE IF EXISTS weekly_checks CASCADE;")
    op.execute("DROP TABLE IF EXISTS strategies CASCADE;")
    op.execute("DROP TABLE IF EXISTS submissions CASCADE;")
    op.execute("DROP TABLE IF EXISTS candidates CASCADE;")
    op.execute("DROP TABLE IF EXISTS tasks CASCADE;")
    op.execute("DROP TABLE IF EXISTS factor_pools CASCADE;")
    op.execute("DROP TABLE IF EXISTS cycles CASCADE;")
    op.execute("DROP TABLE IF EXISTS users CASCADE;")
    op.execute("DROP TABLE IF EXISTS settings CASCADE;")
