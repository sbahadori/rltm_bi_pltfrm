CREATE TABLE IF NOT EXISTS meta.source_system (
    source_system_id TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_type TEXT,
    connection_ref TEXT,
    owner TEXT,
    environment TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_source_system_name_env
    ON meta.source_system (source_name, COALESCE(environment, ''));


CREATE TABLE IF NOT EXISTS meta.pipeline (
    pipeline_id BIGSERIAL PRIMARY KEY,
    pipeline_name TEXT NOT NULL UNIQUE,
    domain TEXT,
    description TEXT,
    owner TEXT,
    schedule_cron TEXT,
    airflow_dag_id TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    raw_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_pipeline_airflow_dag_id
    ON meta.pipeline (airflow_dag_id);

CREATE INDEX IF NOT EXISTS ix_pipeline_active
    ON meta.pipeline (is_active);


CREATE TABLE IF NOT EXISTS meta.job (
    job_id BIGSERIAL PRIMARY KEY,

    job_code TEXT NOT NULL,
    job_name TEXT NOT NULL,
    display_name TEXT,

    pipeline_name TEXT NOT NULL,
    base_job_name TEXT,

    job_type TEXT,
    runner TEXT,
    layer TEXT,

    source_id TEXT,
    table_id TEXT,
    entity_name TEXT,

    manifest_ref TEXT,
    target_path TEXT,

    active_flag BOOLEAN NOT NULL DEFAULT TRUE,
    effective_start_date TIMESTAMPTZ,
    effective_end_date TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    job_key TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,

    source_type TEXT,

    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    runtime_policy JSONB NOT NULL DEFAULT '{}'::jsonb,

    CONSTRAINT ux_meta_job_code UNIQUE (job_code),
    CONSTRAINT ux_meta_job_key UNIQUE (job_key)
);

CREATE INDEX IF NOT EXISTS ix_job_pipeline_name
    ON meta.job (pipeline_name);

CREATE INDEX IF NOT EXISTS ix_job_source_table
    ON meta.job (source_id, table_id);

CREATE INDEX IF NOT EXISTS ix_job_active
    ON meta.job (is_active, active_flag);

CREATE INDEX IF NOT EXISTS ix_job_layer
    ON meta.job (layer);

CREATE INDEX IF NOT EXISTS ix_job_runner
    ON meta.job (runner);


CREATE TABLE IF NOT EXISTS meta.job_dependency (
    dependency_id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES meta.job(job_id) ON DELETE CASCADE,
    depends_on_job_id BIGINT NOT NULL REFERENCES meta.job(job_id) ON DELETE CASCADE,
    dependency_type TEXT NOT NULL DEFAULT 'success',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ux_job_dependency UNIQUE (job_id, depends_on_job_id, dependency_type)
);

CREATE INDEX IF NOT EXISTS ix_job_dependency_job_id
    ON meta.job_dependency (job_id);

CREATE INDEX IF NOT EXISTS ix_job_dependency_depends_on
    ON meta.job_dependency (depends_on_job_id);


CREATE TABLE IF NOT EXISTS meta.dataset (
    dataset_id BIGSERIAL PRIMARY KEY,
    dataset_key TEXT NOT NULL UNIQUE,
    dataset_name TEXT NOT NULL,

    source_system_id TEXT REFERENCES meta.source_system(source_system_id),
    source_object TEXT,

    layer TEXT,
    target_path TEXT,
    target_format TEXT,

    primary_keys JSONB NOT NULL DEFAULT '[]'::jsonb,
    schema_definition JSONB NOT NULL DEFAULT '{}'::jsonb,
    contract JSONB NOT NULL DEFAULT '{}'::jsonb,

    is_active BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_dataset_source_system
    ON meta.dataset (source_system_id);

CREATE INDEX IF NOT EXISTS ix_dataset_layer
    ON meta.dataset (layer);

CREATE INDEX IF NOT EXISTS ix_dataset_target_path
    ON meta.dataset (target_path);