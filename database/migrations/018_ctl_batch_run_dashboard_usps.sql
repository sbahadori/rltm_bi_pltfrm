-- -----------------------------------------------------------------------------
-- 018_ctl_batch_run_dashboard_usps.sql
-- Purpose:
--   Dashboard-friendly batch run history from runtime.job_run.
--   This is better than showing Airflow dag_run_id as the main run id.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION ctl.usp_list_batch_runs_for_job(
    p_job_code TEXT DEFAULT NULL,
    p_source_id TEXT DEFAULT NULL,
    p_table_id TEXT DEFAULT NULL,
    p_pipeline_name TEXT DEFAULT NULL,
    p_job_name TEXT DEFAULT NULL,
    p_limit INTEGER DEFAULT 10
)
RETURNS TABLE (
    run_id TEXT,
    display_run_id TEXT,
    job_id BIGINT,
    job_key TEXT,
    job_code TEXT,
    job_name TEXT,
    pipeline_name TEXT,
    airflow_dag_run_id TEXT,
    airflow_task_id TEXT,
    airflow_try_number INTEGER,
    state TEXT,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    duration_seconds DOUBLE PRECISION,
    records_read BIGINT,
    records_written BIGINT,
    records_inserted BIGINT,
    records_updated BIGINT,
    records_deleted BIGINT,
    source_path TEXT,
    target_path TEXT,
    status_reason TEXT,
    error_message TEXT,
    payload JSONB,
    created_at TIMESTAMPTZ
)
LANGUAGE sql
AS $$
    SELECT
        r.run_id,
        CASE
            WHEN length(r.run_id) > 18 THEN left(r.run_id, 18)
            ELSE r.run_id
        END AS display_run_id,
        r.job_id,
        r.job_key,
        r.job_code,
        r.job_name,
        r.pipeline_name,
        r.airflow_dag_run_id,
        r.airflow_task_id,
        r.airflow_try_number,
        r.status AS state,
        r.started_at,
        r.ended_at,
        r.duration_seconds,
        r.records_read,
        r.records_written,

        CASE
            WHEN COALESCE(r.payload ->> 'records_inserted', '') ~ '^[0-9]+$'
                THEN (r.payload ->> 'records_inserted')::BIGINT
            WHEN COALESCE(r.payload ->> 'inserted_rows', '') ~ '^[0-9]+$'
                THEN (r.payload ->> 'inserted_rows')::BIGINT
            ELSE NULL
        END AS records_inserted,

        CASE
            WHEN COALESCE(r.payload ->> 'records_updated', '') ~ '^[0-9]+$'
                THEN (r.payload ->> 'records_updated')::BIGINT
            WHEN COALESCE(r.payload ->> 'updated_rows', '') ~ '^[0-9]+$'
                THEN (r.payload ->> 'updated_rows')::BIGINT
            ELSE NULL
        END AS records_updated,

        CASE
            WHEN COALESCE(r.payload ->> 'records_deleted', '') ~ '^[0-9]+$'
                THEN (r.payload ->> 'records_deleted')::BIGINT
            WHEN COALESCE(r.payload ->> 'deleted_rows', '') ~ '^[0-9]+$'
                THEN (r.payload ->> 'deleted_rows')::BIGINT
            ELSE NULL
        END AS records_deleted,

        r.source_path,
        r.target_path,
        r.status_reason,
        r.error_message,
        r.payload,
        r.created_at
    FROM runtime.job_run r
    WHERE
        (p_job_code IS NULL OR r.job_code = p_job_code OR r.job_key = p_job_code)
        AND (p_source_id IS NULL OR r.source_id = p_source_id)
        AND (p_table_id IS NULL OR r.table_id = p_table_id)
        AND (p_pipeline_name IS NULL OR r.pipeline_name = p_pipeline_name)
        AND (p_job_name IS NULL OR r.job_name = p_job_name OR r.base_job_name = p_job_name)
    ORDER BY COALESCE(r.started_at, r.created_at) DESC
    LIMIT COALESCE(p_limit, 10);
$$;


-- Get-Content .\database\migrations\018_ctl_batch_run_dashboard_usps.sql -Raw | docker exec -i postgres-warehouse `  sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
