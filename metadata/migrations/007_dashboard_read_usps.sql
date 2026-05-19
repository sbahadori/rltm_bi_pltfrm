CREATE SCHEMA IF NOT EXISTS ctl;

-- =========================================================
-- Dashboard read USP functions
-- These functions remove raw SELECT statements from apps/dashboard/app.py.
-- =========================================================

CREATE OR REPLACE FUNCTION ctl.usp_get_latest_runtime_job_run(
    p_job_code TEXT DEFAULT NULL,
    p_source_id TEXT DEFAULT NULL,
    p_table_id TEXT DEFAULT NULL,
    p_pipeline_name TEXT DEFAULT NULL,
    p_job_name TEXT DEFAULT NULL
)
RETURNS TABLE (
    run_id TEXT,
    job_id BIGINT,
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
    status TEXT,
    status_reason TEXT,
    error_message TEXT,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    duration_seconds DOUBLE PRECISION,
    records_read BIGINT,
    records_written BIGINT,
    source_path TEXT,
    target_path TEXT,
    created_at TIMESTAMP
)
LANGUAGE sql
AS $$
    SELECT
        r.run_id::text,
        r.job_id,
        r.job_code,
        r.job_name,
        r.pipeline_name,
        r.base_job_name,
        r.source_id,
        r.table_id,
        r.entity_name,
        r.layer,
        r.runner,
        r.airflow_dag_id,
        r.airflow_dag_run_id,
        r.airflow_task_id,
        r.airflow_try_number,
        r.status,
        r.status_reason,
        r.error_message,
        r.effective_start_date,
        r.effective_end_date,
        r.started_at,
        r.ended_at,
        r.duration_seconds,
        r.records_read,
        r.records_written,
        r.source_path,
        r.target_path,
        r.created_at
    FROM runtime.job_run r
    WHERE
        (p_job_code IS NOT NULL AND r.job_code = p_job_code)
        OR (
            p_source_id IS NOT NULL
            AND p_table_id IS NOT NULL
            AND r.source_id = p_source_id
            AND r.table_id = p_table_id
        )
        OR (
            p_pipeline_name IS NOT NULL
            AND p_job_name IS NOT NULL
            AND r.pipeline_name = p_pipeline_name
            AND r.job_name = p_job_name
        )
    ORDER BY r.created_at DESC
    LIMIT 1;
$$;


CREATE OR REPLACE FUNCTION ctl.usp_list_control_jobs()
RETURNS TABLE (
    job_id BIGINT,
    job_key TEXT,
    job_code TEXT,
    pipeline_name TEXT,
    job_name TEXT,
    base_job_name TEXT,
    job_type TEXT,
    runner TEXT,
    source_id TEXT,
    table_id TEXT,
    entity_name TEXT,
    target_path TEXT,
    is_active BOOLEAN,
    updated_at TIMESTAMP
)
LANGUAGE sql
AS $$
    SELECT
        j.job_id,
        j.job_key,
        j.job_code,
        j.pipeline_name,
        j.job_name,
        j.base_job_name,
        j.job_type,
        j.runner,
        j.source_id,
        j.table_id,
        j.entity_name,
        j.target_path,
        j.is_active,
        j.updated_at
    FROM meta.job j
    ORDER BY j.pipeline_name, j.job_name;
$$;


CREATE OR REPLACE FUNCTION ctl.usp_list_runtime_job_runs(
    p_limit INTEGER DEFAULT 50
)
RETURNS TABLE (
    run_id TEXT,
    job_id BIGINT,
    job_key TEXT,
    job_code TEXT,
    pipeline_name TEXT,
    job_name TEXT,
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
    status TEXT,
    status_reason TEXT,
    error_message TEXT,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    duration_seconds DOUBLE PRECISION,
    records_read BIGINT,
    records_written BIGINT,
    source_path TEXT,
    target_path TEXT,
    created_at TIMESTAMP
)
LANGUAGE sql
AS $$
    SELECT
        r.run_id::text,
        r.job_id,
        NULL::text AS job_key,
        r.job_code,
        r.pipeline_name,
        r.job_name,
        r.base_job_name,
        r.source_id,
        r.table_id,
        r.entity_name,
        r.layer,
        r.runner,
        r.airflow_dag_id,
        r.airflow_dag_run_id,
        r.airflow_task_id,
        r.airflow_try_number,
        r.status,
        r.status_reason,
        r.error_message,
        r.effective_start_date,
        r.effective_end_date,
        r.started_at,
        r.ended_at,
        r.duration_seconds,
        r.records_read,
        r.records_written,
        r.source_path,
        r.target_path,
        r.created_at
    FROM runtime.job_run r
    ORDER BY r.created_at DESC
    LIMIT p_limit;
$$;


CREATE OR REPLACE FUNCTION ctl.usp_list_runtime_watermarks()
RETURNS TABLE (
    watermark_id BIGINT,
    job_id BIGINT,
    job_key TEXT,
    source_id TEXT,
    table_id TEXT,
    watermark_column TEXT,
    last_successful_value TEXT,
    current_value TEXT,
    last_run_id TEXT,
    updated_at TIMESTAMP
)
LANGUAGE sql
AS $$
    SELECT
        w.watermark_id,
        w.job_id,
        w.job_key,
        w.source_id,
        w.table_id,
        w.watermark_column,
        w.last_successful_value,
        w.current_value,
        w.last_run_id::text,
        w.updated_at
    FROM runtime.watermark_state w
    ORDER BY w.updated_at DESC;
$$;


CREATE OR REPLACE FUNCTION ctl.usp_list_quality_results(
    p_limit INTEGER DEFAULT 100
)
RETURNS TABLE (
    run_id TEXT,
    job_id BIGINT,
    result_id BIGINT,
    job_key TEXT,
    dataset_key TEXT,
    status TEXT,
    observed_value TEXT,
    expected_value TEXT,
    details JSONB,
    created_at TIMESTAMP
)
LANGUAGE sql
AS $$
    SELECT
        q.run_id::text,
        q.job_id,
        q.result_id,
        q.job_key,
        q.dataset_key,
        q.status,
        q.observed_value,
        q.expected_value,
        q.details,
        q.created_at
    FROM dq.quality_result q
    ORDER BY q.created_at DESC
    LIMIT p_limit;
$$;


CREATE OR REPLACE FUNCTION ctl.usp_list_dataset_lineage(
    p_limit INTEGER DEFAULT 100
)
RETURNS TABLE (
    lineage_id BIGINT,
    run_id TEXT,
    job_id BIGINT,
    job_key TEXT,
    source_dataset_key TEXT,
    target_dataset_key TEXT,
    transformation_type TEXT,
    transformation_ref TEXT,
    details JSONB,
    created_at TIMESTAMP
)
LANGUAGE sql
AS $$
    SELECT
        l.lineage_id,
        l.run_id::text,
        l.job_id,
        l.job_key,
        l.source_dataset_key,
        l.target_dataset_key,
        l.transformation_type,
        l.transformation_ref,
        l.details,
        l.created_at
    FROM lineage.dataset_lineage l
    ORDER BY l.created_at DESC
    LIMIT p_limit;
$$;
