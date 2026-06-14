CREATE TABLE IF NOT EXISTS runtime.job_run (
    run_id TEXT PRIMARY KEY,

    job_id BIGINT REFERENCES meta.job(job_id),
    job_key TEXT,
    job_code TEXT,

    job_name TEXT,
    pipeline_name TEXT,
    base_job_name TEXT,

    source_id TEXT,
    table_id TEXT,
    entity_name TEXT,

    layer TEXT,
    runner TEXT,

    airflow_dag_id TEXT,
    airflow_dag_run_id TEXT,
    airflow_task_id TEXT,
    airflow_try_number INTEGER,

    status TEXT NOT NULL DEFAULT 'unknown',
    status_reason TEXT,
    error_message TEXT,

    effective_start_date TIMESTAMPTZ,
    effective_end_date TIMESTAMPTZ,

    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    duration_seconds DOUBLE PRECISION,

    records_read BIGINT,
    records_written BIGINT,

    source_path TEXT,
    target_path TEXT,

    payload JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ck_job_run_run_id_guid CHECK (
        run_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    )
);

CREATE INDEX IF NOT EXISTS ix_job_run_job_id
    ON runtime.job_run (job_id);

CREATE INDEX IF NOT EXISTS ix_job_run_job_key
    ON runtime.job_run (job_key);

CREATE INDEX IF NOT EXISTS ix_job_run_job_code
    ON runtime.job_run (job_code);

CREATE INDEX IF NOT EXISTS ix_job_run_pipeline_name
    ON runtime.job_run (pipeline_name);

CREATE INDEX IF NOT EXISTS ix_job_run_status
    ON runtime.job_run (status);

CREATE INDEX IF NOT EXISTS ix_job_run_started_at
    ON runtime.job_run (started_at DESC);

CREATE INDEX IF NOT EXISTS ix_job_run_airflow
    ON runtime.job_run (airflow_dag_id, airflow_dag_run_id, airflow_task_id);


CREATE TABLE IF NOT EXISTS runtime.job_event (
    event_id BIGSERIAL PRIMARY KEY,

    run_id TEXT,
    job_id BIGINT REFERENCES meta.job(job_id),
    job_key TEXT,

    event_type TEXT NOT NULL,
    event_message TEXT,
    event_payload JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ck_job_event_run_id_guid CHECK (
        run_id IS NULL
        OR run_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    )
);

CREATE INDEX IF NOT EXISTS ix_job_event_run_id
    ON runtime.job_event (run_id);

CREATE INDEX IF NOT EXISTS ix_job_event_job_id
    ON runtime.job_event (job_id);

CREATE INDEX IF NOT EXISTS ix_job_event_job_key
    ON runtime.job_event (job_key);

CREATE INDEX IF NOT EXISTS ix_job_event_type
    ON runtime.job_event (event_type);

CREATE INDEX IF NOT EXISTS ix_job_event_created_at
    ON runtime.job_event (created_at DESC);


CREATE TABLE IF NOT EXISTS runtime.watermark_state (
    watermark_id BIGSERIAL PRIMARY KEY,

    job_id BIGINT REFERENCES meta.job(job_id),
    job_key TEXT,

    source_id TEXT,
    table_id TEXT,

    watermark_column TEXT,

    last_successful_value TEXT,
    current_value TEXT,

    last_run_id TEXT,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ux_watermark_state UNIQUE (
        job_key,
        source_id,
        table_id,
        watermark_column
    ),

    CONSTRAINT ck_watermark_state_last_run_id_guid CHECK (
        last_run_id IS NULL
        OR last_run_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    )
);

CREATE INDEX IF NOT EXISTS ix_watermark_job_id
    ON runtime.watermark_state (job_id);

CREATE INDEX IF NOT EXISTS ix_watermark_source_table
    ON runtime.watermark_state (source_id, table_id);

CREATE INDEX IF NOT EXISTS ix_watermark_updated_at
    ON runtime.watermark_state (updated_at DESC);
