/*      Manifest / config registered into PostgreSQL
                ↓
        Airflow DAG generator reads active pipelines/jobs from PostgreSQL
                ↓
        Airflow creates DAG dynamically
                ↓
        Before execution, job_run is created in runtime.job_run
                ↓
        Runner receives job_id and run_id
                ↓
        Runner reads parameters from PostgreSQL
                ↓
        Runner executes ingestion/transformation
                ↓
        Runner updates runtime.job_run
                ↓
        Runner updates watermark_state
                ↓
        Runner writes DQ results
                ↓
        Runner writes lineage
                ↓
        Dashboard reads PostgreSQL runtime/control tables
*/



-- The `meta` schema is used to store metadata about the data processing pipelines, including information about data sources, transformations, and outputs. This schema can be used to track the lineage of data as it flows through the system, as well as to store information about data quality and runtime metrics.
CREATE SCHEMA IF NOT EXISTS meta;
-- The `runtime` schema is used to store runtime information about the data processing pipelines, including information about job execution, performance metrics, and error logs.
CREATE SCHEMA IF NOT EXISTS runtime;
-- The `dq` schema is used to store data quality information, including validation rules, compliance checks, and data profiling results.
CREATE SCHEMA IF NOT EXISTS dq;
-- The `lineage` schema is used to store information about the relationships between different data entities, including the flow of data through the system and the dependencies between various components.
CREATE SCHEMA IF NOT EXISTS lineage;


-- The `meta.pipeline` table stores information about the data processing pipelines, including their names, descriptions, owners, and schedules. Each pipeline can have multiple jobs associated with it, which are defined in the `meta.job` table.
CREATE TABLE IF NOT EXISTS meta.pipeline (
    pipeline_id        BIGSERIAL PRIMARY KEY,
    pipeline_name      TEXT NOT NULL UNIQUE,
    domain             TEXT,
    description        TEXT,
    owner              TEXT,
    schedule_cron      TEXT,
    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- The `meta.source_system` table stores information about the various data sources that are used in the pipelines, including their names, types, connection details, and ownership. This allows for better management and tracking of data sources across the system.
CREATE TABLE IF NOT EXISTS meta.source_system (
    source_system_id   BIGSERIAL PRIMARY KEY,
    source_name        TEXT NOT NULL UNIQUE,
    source_type        TEXT NOT NULL, -- postgres, mysql, sqlserver, api, file, kafka
    connection_ref     TEXT,
    owner              TEXT,
    environment        TEXT DEFAULT 'dev',
    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- The `meta.dataset` table stores information about the datasets that are used in the pipelines, including their names, source systems, and target layers. This allows for better management and tracking of datasets across the system.
CREATE TABLE IF NOT EXISTS meta.dataset (
    dataset_id         BIGSERIAL PRIMARY KEY,
    dataset_name       TEXT NOT NULL,
    source_system_id   BIGINT REFERENCES meta.source_system(source_system_id),
    source_object      TEXT NOT NULL,
    target_layer       TEXT NOT NULL, -- bronze, silver, gold
    target_table       TEXT NOT NULL,
    primary_keys       JSONB,
    schema_definition  JSONB,
    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_system_id, dataset_name, target_layer)
);

-- The `meta.job` table stores information about the individual jobs that make up the pipelines, including their names, types, source and target datasets, runner types, and configurations. Each job can have dependencies on other jobs, which are defined in the `meta.job_dependency` table.
CREATE TABLE IF NOT EXISTS meta.job (
    job_id             BIGSERIAL PRIMARY KEY,
    pipeline_id        BIGINT REFERENCES meta.pipeline(pipeline_id),
    job_name           TEXT NOT NULL,
    job_type           TEXT NOT NULL, -- ingest, transform, dq, publish
    source_dataset_id  BIGINT REFERENCES meta.dataset(dataset_id),
    target_dataset_id  BIGINT REFERENCES meta.dataset(dataset_id),
    runner_type        TEXT NOT NULL, -- spark, python, dbt, sql, api
    runner_path        TEXT,
    config             JSONB,
    layer_from         TEXT,
    layer_to           TEXT,
    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(pipeline_id, job_name)
);

-- The `meta.job_dependency` table stores information about the dependencies between jobs, including the types of dependencies and their active status. This allows for better management and tracking of job dependencies across the system.
CREATE TABLE IF NOT EXISTS meta.job_dependency (
    dependency_id      BIGSERIAL PRIMARY KEY,
    job_id             BIGINT NOT NULL REFERENCES meta.job(job_id),
    depends_on_job_id  BIGINT NOT NULL REFERENCES meta.job(job_id),
    dependency_type    TEXT NOT NULL DEFAULT 'success', -- success, data_ready, dq_passed
    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(job_id, depends_on_job_id)
);


-- The `runtime.job_run` table stores information about the execution of individual jobs, including their status, start and end times, performance metrics, and any error messages. This allows for better monitoring and troubleshooting of job executions across the system.
CREATE TABLE IF NOT EXISTS runtime.job_run (
    run_id             BIGSERIAL PRIMARY KEY,
    job_id             BIGINT NOT NULL REFERENCES meta.job(job_id),
    pipeline_id        BIGINT REFERENCES meta.pipeline(pipeline_id),
    airflow_dag_id     TEXT,
    airflow_run_id     TEXT,
    status             TEXT NOT NULL, -- queued, running, success, failed, skipped
    started_at         TIMESTAMP,
    ended_at           TIMESTAMP,
    duration_seconds   NUMERIC,
    records_read       BIGINT DEFAULT 0,
    records_written    BIGINT DEFAULT 0,
    error_message      TEXT,
    run_config         JSONB,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- The `runtime.job_event` table stores information about the various events that occur during the execution of jobs, including their types, messages, and payloads. This allows for better tracking and analysis of job events across the system.
CREATE TABLE IF NOT EXISTS runtime.job_event (
    event_id           BIGSERIAL PRIMARY KEY,
    run_id             BIGINT REFERENCES runtime.job_run(run_id),
    job_id             BIGINT REFERENCES meta.job(job_id),
    event_type         TEXT NOT NULL, -- started, completed, failed, dq_failed, watermark_updated
    event_message      TEXT,
    event_payload      JSONB,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- The `runtime.watermark_state` table stores information about the watermark state for each job and dataset, including the last successful value, current value, and the last run that updated the watermark. This allows for better tracking and management of watermarks across the system.
CREATE TABLE IF NOT EXISTS runtime.watermark_state (
    watermark_id           BIGSERIAL PRIMARY KEY,
    job_id                 BIGINT NOT NULL REFERENCES meta.job(job_id),
    dataset_id             BIGINT REFERENCES meta.dataset(dataset_id),
    watermark_column       TEXT NOT NULL,
    last_successful_value  TEXT,
    current_value          TEXT,
    last_run_id            BIGINT REFERENCES runtime.job_run(run_id),
    updated_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(job_id, watermark_column)
);


-- The `dq.quality_rule` table stores information about the data quality rules that are defined for each dataset, including their names, types, configurations, severity levels, and active status. This allows for better management and tracking of data quality rules across the system.
CREATE TABLE IF NOT EXISTS dq.quality_rule (
    rule_id            BIGSERIAL PRIMARY KEY,
    dataset_id         BIGINT REFERENCES meta.dataset(dataset_id),
    rule_name          TEXT NOT NULL,
    rule_type          TEXT NOT NULL, -- not_null, unique, accepted_values, row_count, freshness, schema_check
    rule_config        JSONB NOT NULL,
    severity           TEXT NOT NULL DEFAULT 'error', -- warning, error, critical
    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- The `dq.quality_result` table stores information about the outcomes of data quality rule evaluations, including their statuses, observed and expected values, and details. This allows for better analysis and reporting of data quality issues across the system.
CREATE TABLE IF NOT EXISTS dq.quality_result (
    result_id          BIGSERIAL PRIMARY KEY,
    rule_id            BIGINT REFERENCES dq.quality_rule(rule_id),
    run_id             BIGINT REFERENCES runtime.job_run(run_id),
    dataset_id         BIGINT REFERENCES meta.dataset(dataset_id),
    status             TEXT NOT NULL, -- passed, failed, warning
    observed_value     TEXT,
    expected_value     TEXT,
    details            JSONB,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- The `lineage.dataset_lineage` table stores information about the lineage of datasets as they flow through the system, including their source and target datasets, transformation types and references, and the job runs that are responsible for the transformations. This allows for better tracking and analysis of data lineage across the system.
CREATE TABLE IF NOT EXISTS lineage.dataset_lineage (
    lineage_id             BIGSERIAL PRIMARY KEY,
    run_id                 BIGINT REFERENCES runtime.job_run(run_id),
    source_dataset_id      BIGINT REFERENCES meta.dataset(dataset_id),
    target_dataset_id      BIGINT REFERENCES meta.dataset(dataset_id),
    transformation_type    TEXT,
    transformation_ref     TEXT,
    created_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);