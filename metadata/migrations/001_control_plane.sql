CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS runtime;
CREATE SCHEMA IF NOT EXISTS dq;
CREATE SCHEMA IF NOT EXISTS lineage;

CREATE TABLE IF NOT EXISTS meta.pipeline (
    pipeline_id        BIGSERIAL PRIMARY KEY,
    pipeline_name      TEXT NOT NULL UNIQUE,
    domain             TEXT,
    description        TEXT,
    owner              TEXT DEFAULT 'data_platform',
    schedule_cron      TEXT,
    airflow_dag_id     TEXT,
    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    raw_config         JSONB,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS meta.source_system (
    source_system_id   BIGSERIAL PRIMARY KEY,
    source_name        TEXT NOT NULL UNIQUE,
    source_type        TEXT NOT NULL,
    connection_ref     TEXT,
    owner              TEXT,
    environment        TEXT DEFAULT 'dev',
    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS meta.dataset (
    dataset_id         BIGSERIAL PRIMARY KEY,
    dataset_key        TEXT NOT NULL UNIQUE,
    dataset_name       TEXT NOT NULL,
    source_system_id   BIGINT REFERENCES meta.source_system(source_system_id),
    source_object      TEXT,
    layer              TEXT NOT NULL,
    target_path        TEXT,
    target_format      TEXT DEFAULT 'delta',
    primary_keys       JSONB,
    schema_definition  JSONB,
    contract           JSONB,
    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS meta.job (
    job_id             BIGSERIAL PRIMARY KEY,
    job_key            TEXT NOT NULL UNIQUE,
    pipeline_id        BIGINT NOT NULL REFERENCES meta.pipeline(pipeline_id),
    pipeline_name      TEXT NOT NULL,
    job_name           TEXT NOT NULL,
    job_type           TEXT NOT NULL,
    source_type        TEXT,
    runner_type        TEXT NOT NULL DEFAULT 'spark_submit',
    runner_path        TEXT,
    layer_from         TEXT,
    layer_to           TEXT,
    config             JSONB NOT NULL DEFAULT '{}'::jsonb,
    runtime_policy     JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(pipeline_id, job_name)
);

CREATE TABLE IF NOT EXISTS meta.job_dependency (
    dependency_id      BIGSERIAL PRIMARY KEY,
    job_id             BIGINT NOT NULL REFERENCES meta.job(job_id),
    depends_on_job_id  BIGINT NOT NULL REFERENCES meta.job(job_id),
    dependency_type    TEXT NOT NULL DEFAULT 'success',
    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(job_id, depends_on_job_id)
);

CREATE TABLE IF NOT EXISTS runtime.job_run (
    run_id             TEXT PRIMARY KEY,
    job_key            TEXT NOT NULL REFERENCES meta.job(job_key),
    pipeline_name      TEXT NOT NULL,
    job_name           TEXT NOT NULL,
    airflow_dag_id     TEXT,
    airflow_run_id     TEXT,
    status             TEXT NOT NULL,
    started_at         TIMESTAMP,
    ended_at           TIMESTAMP,
    duration_seconds   NUMERIC,
    records_read       BIGINT,
    records_written    BIGINT,
    error_message      TEXT,
    run_config         JSONB,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_job_run_job_key_created_at
ON runtime.job_run(job_key, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_job_run_status
ON runtime.job_run(status);

CREATE TABLE IF NOT EXISTS runtime.job_event (
    event_id           BIGSERIAL PRIMARY KEY,
    run_id             TEXT REFERENCES runtime.job_run(run_id),
    job_key            TEXT REFERENCES meta.job(job_key),
    event_type         TEXT NOT NULL,
    event_message      TEXT,
    event_payload      JSONB,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);



CREATE TABLE IF NOT EXISTS runtime.watermark_state (
    watermark_id              BIGSERIAL PRIMARY KEY,
    job_key                   TEXT NOT NULL REFERENCES meta.job(job_key),
    source_id                 TEXT,
    table_id                  TEXT,
    watermark_column          TEXT NOT NULL,
    last_successful_value     TEXT,
    current_value             TEXT,
    last_run_id               TEXT REFERENCES runtime.job_run(run_id),
    updated_at                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(job_key, source_id, table_id, watermark_column)
);

CREATE TABLE IF NOT EXISTS dq.quality_rule (
    rule_id            BIGSERIAL PRIMARY KEY,
    job_key            TEXT REFERENCES meta.job(job_key),
    dataset_key        TEXT,
    rule_name          TEXT NOT NULL,
    rule_type          TEXT NOT NULL,
    rule_config        JSONB NOT NULL DEFAULT '{}'::jsonb,
    severity           TEXT NOT NULL DEFAULT 'error',
    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dq.quality_result (
    result_id          BIGSERIAL PRIMARY KEY,
    rule_id            BIGINT REFERENCES dq.quality_rule(rule_id),
    run_id             TEXT REFERENCES runtime.job_run(run_id),
    job_key            TEXT REFERENCES meta.job(job_key),
    dataset_key        TEXT,
    status             TEXT NOT NULL,
    observed_value     TEXT,
    expected_value     TEXT,
    details            JSONB,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS lineage.dataset_lineage (
    lineage_id             BIGSERIAL PRIMARY KEY,
    run_id                 TEXT REFERENCES runtime.job_run(run_id),
    job_key                TEXT REFERENCES meta.job(job_key),
    source_dataset_key     TEXT,
    target_dataset_key     TEXT,
    transformation_type    TEXT,
    transformation_ref     TEXT,
    details                JSONB,
    created_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);