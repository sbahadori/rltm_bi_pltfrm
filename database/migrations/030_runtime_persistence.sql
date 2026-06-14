CREATE SCHEMA IF NOT EXISTS runtime;

CREATE TABLE IF NOT EXISTS runtime.job_run_event (
    event_id TEXT PRIMARY KEY,

    event_type TEXT NOT NULL,

    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ts_epoch BIGINT NOT NULL,

    run_id TEXT,
    executor_run_id TEXT,

    job_id TEXT,
    job TEXT,
    job_name TEXT,
    pipeline TEXT,
    pipeline_id TEXT,
    job_code TEXT,
    runner_id TEXT,

    state TEXT,
    status TEXT,

    executor_type TEXT,
    executor_id TEXT,
    executor_state TEXT,

    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    duration_seconds DOUBLE PRECISION,

    records_read BIGINT,
    records_written BIGINT,
    records_inserted BIGINT,
    records_updated BIGINT,
    records_deleted BIGINT,

    runtime_source TEXT,
    status_reason TEXT,

    payload JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_job_run_event_job_id
    ON runtime.job_run_event (job_id);

CREATE INDEX IF NOT EXISTS ix_job_run_event_job_name
    ON runtime.job_run_event (job_name);

CREATE INDEX IF NOT EXISTS ix_job_run_event_pipeline_id
    ON runtime.job_run_event (pipeline_id);

CREATE INDEX IF NOT EXISTS ix_job_run_event_run_id
    ON runtime.job_run_event (run_id);

CREATE INDEX IF NOT EXISTS ix_job_run_event_executor_run_id
    ON runtime.job_run_event (executor_run_id);

CREATE INDEX IF NOT EXISTS ix_job_run_event_observed_at
    ON runtime.job_run_event (observed_at DESC);

CREATE INDEX IF NOT EXISTS ix_job_run_event_state
    ON runtime.job_run_event (state);