CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- DEV RESET:
-- This drops runtime observability/control tables only.
-- meta.* tables are preserved.
DROP TABLE IF EXISTS lineage.dataset_lineage CASCADE;
DROP TABLE IF EXISTS dq.quality_result CASCADE;
DROP TABLE IF EXISTS dq.quality_rule CASCADE;
DROP TABLE IF EXISTS runtime.watermark_state CASCADE;
DROP TABLE IF EXISTS runtime.job_event CASCADE;
DROP TABLE IF EXISTS runtime.job_run CASCADE;
DROP TABLE IF EXISTS meta.job CASCADE;


CREATE SCHEMA IF NOT EXISTS runtime;
CREATE SCHEMA IF NOT EXISTS dq;
CREATE SCHEMA IF NOT EXISTS lineage;


CREATE TABLE meta.job (
    job_id BIGSERIAL PRIMARY KEY,

    job_code TEXT NOT NULL UNIQUE,
    job_name TEXT NOT NULL,
    display_name TEXT,

    pipeline_name TEXT NOT NULL,
    base_job_name TEXT,

    job_type TEXT NOT NULL,
    runner TEXT NOT NULL,
    layer TEXT,

    source_id TEXT,
    table_id TEXT,
    entity_name TEXT,

    manifest_ref TEXT,
    target_path TEXT,

    active_flag BOOLEAN NOT NULL DEFAULT TRUE,

    effective_start_date TIMESTAMP NULL,
    effective_end_date TIMESTAMP NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE runtime.job_run (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    job_id BIGINT NULL REFERENCES meta.job(job_id),
    job_key TEXT,
    job_code TEXT NOT NULL,

    job_name TEXT NOT NULL,
    pipeline_name TEXT NOT NULL,
    base_job_name TEXT,

    source_id TEXT,
    table_id TEXT,
    entity_name TEXT,
    layer TEXT,
    runner TEXT,

    airflow_dag_id TEXT,
    airflow_dag_run_id TEXT,
    airflow_task_id TEXT,
    airflow_try_number INT,

    status TEXT NOT NULL,
    status_reason TEXT,
    error_message TEXT,

    effective_start_date TIMESTAMP NULL,
    effective_end_date TIMESTAMP NULL,

    started_at TIMESTAMP NULL,
    ended_at TIMESTAMP NULL,
    duration_seconds NUMERIC NULL,

    records_read BIGINT NULL,
    records_written BIGINT NULL,

    source_path TEXT NULL,
    target_path TEXT NULL,

    payload JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE INDEX idx_job_run_job_id_created_at
ON runtime.job_run(job_id, created_at DESC);

CREATE INDEX idx_job_run_status
ON runtime.job_run(status);

CREATE INDEX idx_job_run_effective_dates
ON runtime.job_run(effective_start_date, effective_end_date);


CREATE TABLE runtime.job_event (
    event_id           BIGSERIAL PRIMARY KEY,

    run_id             UUID REFERENCES runtime.job_run(run_id),
    job_id             BIGINT REFERENCES meta.job(job_id),
    job_key            TEXT,

    event_type         TEXT NOT NULL,
    event_message      TEXT,
    event_payload      JSONB,

    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_job_event_run_id
ON runtime.job_event(run_id);

CREATE INDEX idx_job_event_job_id_created_at
ON runtime.job_event(job_id, created_at DESC);


CREATE TABLE runtime.watermark_state (
    watermark_id BIGSERIAL PRIMARY KEY,

    job_id BIGINT NULL REFERENCES meta.job(job_id),
    job_key TEXT NOT NULL,

    source_id TEXT NOT NULL,
    table_id TEXT NOT NULL,
    watermark_column TEXT NOT NULL,

    last_successful_value TEXT,
    current_value TEXT,

    last_run_id UUID REFERENCES runtime.job_run(run_id),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(job_key, source_id, table_id, watermark_column)
);

CREATE INDEX idx_watermark_state_job_id
ON runtime.watermark_state(job_id);


CREATE TABLE dq.quality_rule (
    rule_id            BIGSERIAL PRIMARY KEY,

    job_id             BIGINT REFERENCES meta.job(job_id),
    job_key            TEXT,
    dataset_key        TEXT,

    rule_name          TEXT NOT NULL,
    rule_type          TEXT NOT NULL,
    rule_config        JSONB NOT NULL DEFAULT '{}'::jsonb,
    severity           TEXT NOT NULL DEFAULT 'error',

    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE dq.quality_result (
    result_id          BIGSERIAL PRIMARY KEY,

    rule_id            BIGINT REFERENCES dq.quality_rule(rule_id),
    run_id             UUID REFERENCES runtime.job_run(run_id),

    job_id             BIGINT REFERENCES meta.job(job_id),
    job_key            TEXT,
    dataset_key        TEXT,

    status             TEXT NOT NULL,
    observed_value     TEXT,
    expected_value     TEXT,
    details            JSONB,

    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_quality_result_run_id
ON dq.quality_result(run_id);

CREATE INDEX idx_quality_result_job_id_created_at
ON dq.quality_result(job_id, created_at DESC);


CREATE TABLE lineage.dataset_lineage (
    lineage_id             BIGSERIAL PRIMARY KEY,

    run_id                 UUID REFERENCES runtime.job_run(run_id),

    job_id                 BIGINT REFERENCES meta.job(job_id),
    job_key                TEXT,

    source_dataset_key     TEXT,
    target_dataset_key     TEXT,

    transformation_type    TEXT,
    transformation_ref     TEXT,
    details                JSONB,

    created_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dataset_lineage_run_id
ON lineage.dataset_lineage(run_id);

CREATE INDEX idx_dataset_lineage_job_id_created_at
ON lineage.dataset_lineage(job_id, created_at DESC);

-- Get-Content .\metadata\migrations\002_reset_runtime_control_plane.sql | docker compose --project-directory . -f compose/compose.phase1.yaml exec -T postgres-warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'