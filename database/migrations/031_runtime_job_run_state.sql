CREATE SCHEMA IF NOT EXISTS runtime;

CREATE TABLE IF NOT EXISTS runtime.job_run_state (
    run_id TEXT PRIMARY KEY,

    latest_event_id TEXT,
    latest_event_type TEXT,

    state TEXT,
    status TEXT,

    executor_run_id TEXT,
    executor_type TEXT,
    executor_id TEXT,
    executor_state TEXT,

    job_id TEXT,
    job TEXT,
    job_name TEXT,
    pipeline TEXT,
    pipeline_id TEXT,
    job_code TEXT,
    runner_id TEXT,

    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    duration_seconds DOUBLE PRECISION,

    records_read BIGINT,
    records_written BIGINT,
    records_inserted BIGINT,
    records_updated BIGINT,
    records_deleted BIGINT,

    target_path TEXT,
    runtime_source TEXT,
    status_reason TEXT,

    first_observed_at TIMESTAMPTZ,
    last_observed_at TIMESTAMPTZ,
    last_ts_epoch BIGINT,

    payload JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_job_run_state_job_id
    ON runtime.job_run_state (job_id);

CREATE INDEX IF NOT EXISTS ix_job_run_state_job_name
    ON runtime.job_run_state (job_name);

CREATE INDEX IF NOT EXISTS ix_job_run_state_pipeline_id
    ON runtime.job_run_state (pipeline_id);

CREATE INDEX IF NOT EXISTS ix_job_run_state_executor_run_id
    ON runtime.job_run_state (executor_run_id);

CREATE INDEX IF NOT EXISTS ix_job_run_state_state
    ON runtime.job_run_state (state);

CREATE INDEX IF NOT EXISTS ix_job_run_state_last_observed_at
    ON runtime.job_run_state (last_observed_at DESC);


-- Get-Content .\database\migrations\031_runtime_job_run_state.sql -Raw | docker exec -i postgres-warehouse `  sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'