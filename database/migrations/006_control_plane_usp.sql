CREATE SCHEMA IF NOT EXISTS ctl;

-- =========================================================
-- 1) Job identity / metadata
-- =========================================================

CREATE OR REPLACE FUNCTION ctl.usp_get_active_job_identity(
    p_job_id BIGINT DEFAULT NULL,
    p_job_key TEXT DEFAULT NULL,
    p_job_code TEXT DEFAULT NULL
)
RETURNS TABLE (
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
    target_path TEXT
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
        j.source_id,
        j.table_id,
        j.entity_name,
        j.layer,
        j.runner,
        j.target_path
    FROM meta.job j
    WHERE COALESCE(j.is_active, TRUE) = TRUE
      AND (
            (p_job_id IS NOT NULL AND j.job_id = p_job_id)
         OR (p_job_key IS NOT NULL AND j.job_key = p_job_key)
         OR (p_job_code IS NOT NULL AND j.job_code = p_job_code)
      )
    ORDER BY
        CASE
            WHEN p_job_id IS NOT NULL AND j.job_id = p_job_id THEN 1
            WHEN p_job_code IS NOT NULL AND j.job_code = p_job_code THEN 2
            WHEN p_job_key IS NOT NULL AND j.job_key = p_job_key THEN 3
            ELSE 9
        END
    LIMIT 1;
$$;


CREATE OR REPLACE FUNCTION ctl.usp_get_active_job_metadata(
    p_job_id BIGINT DEFAULT NULL,
    p_job_key TEXT DEFAULT NULL,
    p_job_code TEXT DEFAULT NULL
)
RETURNS TABLE (
    job_id BIGINT,
    job_key TEXT,
    job_code TEXT,
    pipeline_name TEXT,
    job_name TEXT,
    base_job_name TEXT,
    job_type TEXT,
    source_type TEXT,
    runner TEXT,
    layer TEXT,
    source_id TEXT,
    table_id TEXT,
    entity_name TEXT,
    manifest_ref TEXT,
    target_path TEXT,
    config JSONB,
    runtime_policy JSONB,
    is_active BOOLEAN
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
        j.source_type,
        j.runner,
        j.layer,
        j.source_id,
        j.table_id,
        j.entity_name,
        j.manifest_ref,
        j.target_path,
        j.config,
        j.runtime_policy,
        j.is_active
    FROM meta.job j
    WHERE COALESCE(j.is_active, TRUE) = TRUE
      AND (
            (p_job_id IS NOT NULL AND j.job_id = p_job_id)
         OR (p_job_key IS NOT NULL AND j.job_key = p_job_key)
         OR (p_job_code IS NOT NULL AND j.job_code = p_job_code)
      )
    ORDER BY
        CASE
            WHEN p_job_id IS NOT NULL AND j.job_id = p_job_id THEN 1
            WHEN p_job_code IS NOT NULL AND j.job_code = p_job_code THEN 2
            WHEN p_job_key IS NOT NULL AND j.job_key = p_job_key THEN 3
            ELSE 9
        END
    LIMIT 1;
$$;


-- =========================================================
-- 2) Pipeline specs for Airflow DAG generation
-- =========================================================

CREATE OR REPLACE FUNCTION ctl.usp_list_active_pipeline_specs()
RETURNS TABLE (
    pipeline_name TEXT,
    raw_config JSONB
)
LANGUAGE sql
AS $$
    SELECT
        p.pipeline_name,
        p.raw_config
    FROM meta.pipeline p
    WHERE COALESCE(p.is_active, TRUE) = TRUE
    ORDER BY p.pipeline_name;
$$;


-- =========================================================
-- 3) Watermark
-- =========================================================

CREATE OR REPLACE FUNCTION ctl.usp_get_watermark_state(
    p_job_key TEXT,
    p_source_id TEXT,
    p_table_id TEXT,
    p_watermark_column TEXT
)
RETURNS TABLE (
    last_successful_value TEXT
)
LANGUAGE sql
AS $$
    SELECT
        w.last_successful_value
    FROM runtime.watermark_state w
    WHERE w.job_key = p_job_key
      AND w.source_id = p_source_id
      AND w.table_id = p_table_id
      AND w.watermark_column = p_watermark_column
    ORDER BY w.updated_at DESC
    LIMIT 1;
$$;


CREATE OR REPLACE PROCEDURE ctl.usp_upsert_watermark_state(
    p_job_id BIGINT,
    p_job_key TEXT,
    p_source_id TEXT,
    p_table_id TEXT,
    p_watermark_column TEXT,
    p_value TEXT,
    p_run_id UUID DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO runtime.watermark_state (
        job_id,
        job_key,
        source_id,
        table_id,
        watermark_column,
        last_successful_value,
        current_value,
        last_run_id,
        updated_at
    )
    VALUES (
        p_job_id,
        p_job_key,
        p_source_id,
        p_table_id,
        p_watermark_column,
        p_value,
        p_value,
        p_run_id,
        CURRENT_TIMESTAMP
    )
    ON CONFLICT (job_key, source_id, table_id, watermark_column)
    DO UPDATE SET
        job_id = EXCLUDED.job_id,
        last_successful_value = EXCLUDED.last_successful_value,
        current_value = EXCLUDED.current_value,
        last_run_id = EXCLUDED.last_run_id,
        updated_at = CURRENT_TIMESTAMP;
END;
$$;


-- =========================================================
-- 4) Runtime job run / event
-- =========================================================

CREATE OR REPLACE PROCEDURE ctl.usp_upsert_job_run(
    p_run_id UUID,
    p_job_id BIGINT,
    p_job_code TEXT,
    p_job_name TEXT,
    p_pipeline_name TEXT,
    p_base_job_name TEXT,
    p_source_id TEXT,
    p_table_id TEXT,
    p_entity_name TEXT,
    p_layer TEXT,
    p_runner TEXT,
    p_airflow_dag_id TEXT,
    p_airflow_dag_run_id TEXT,
    p_airflow_task_id TEXT,
    p_airflow_try_number INTEGER,
    p_status TEXT,
    p_status_reason TEXT,
    p_error_message TEXT,
    p_effective_start_date TIMESTAMP,
    p_effective_end_date TIMESTAMP,
    p_started_at TIMESTAMP,
    p_ended_at TIMESTAMP,
    p_duration_seconds DOUBLE PRECISION,
    p_records_read BIGINT,
    p_records_written BIGINT,
    p_source_path TEXT,
    p_target_path TEXT,
    p_payload JSONB
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO runtime.job_run (
        run_id,
        job_id,
        job_code,
        job_name,
        pipeline_name,
        base_job_name,
        source_id,
        table_id,
        entity_name,
        layer,
        runner,
        airflow_dag_id,
        airflow_dag_run_id,
        airflow_task_id,
        airflow_try_number,
        status,
        status_reason,
        error_message,
        effective_start_date,
        effective_end_date,
        started_at,
        ended_at,
        duration_seconds,
        records_read,
        records_written,
        source_path,
        target_path,
        payload
    )
    VALUES (
        p_run_id,
        p_job_id,
        p_job_code,
        p_job_name,
        p_pipeline_name,
        p_base_job_name,
        p_source_id,
        p_table_id,
        p_entity_name,
        p_layer,
        p_runner,
        p_airflow_dag_id,
        p_airflow_dag_run_id,
        p_airflow_task_id,
        p_airflow_try_number,
        p_status,
        p_status_reason,
        p_error_message,
        p_effective_start_date,
        p_effective_end_date,
        p_started_at,
        p_ended_at,
        p_duration_seconds,
        p_records_read,
        p_records_written,
        p_source_path,
        p_target_path,
        p_payload
    )
    ON CONFLICT (run_id)
    DO UPDATE SET
        status = EXCLUDED.status,
        ended_at = COALESCE(EXCLUDED.ended_at, runtime.job_run.ended_at),
        duration_seconds = COALESCE(EXCLUDED.duration_seconds, runtime.job_run.duration_seconds),
        records_read = COALESCE(EXCLUDED.records_read, runtime.job_run.records_read),
        records_written = COALESCE(EXCLUDED.records_written, runtime.job_run.records_written),
        error_message = COALESCE(EXCLUDED.error_message, runtime.job_run.error_message),
        status_reason = COALESCE(EXCLUDED.status_reason, runtime.job_run.status_reason),
        effective_start_date = COALESCE(EXCLUDED.effective_start_date, runtime.job_run.effective_start_date),
        effective_end_date = COALESCE(EXCLUDED.effective_end_date, runtime.job_run.effective_end_date),
        source_path = COALESCE(EXCLUDED.source_path, runtime.job_run.source_path),
        target_path = COALESCE(EXCLUDED.target_path, runtime.job_run.target_path),
        payload = EXCLUDED.payload;
END;
$$;


CREATE OR REPLACE PROCEDURE ctl.usp_insert_job_event(
    p_run_id UUID,
    p_job_id BIGINT,
    p_job_key TEXT,
    p_event_type TEXT,
    p_event_message TEXT,
    p_event_payload JSONB
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO runtime.job_event (
        run_id,
        job_id,
        job_key,
        event_type,
        event_message,
        event_payload
    )
    VALUES (
        p_run_id,
        p_job_id,
        p_job_key,
        p_event_type,
        p_event_message,
        p_event_payload
    );
END;
$$;


-- =========================================================
-- 5) Data quality
-- =========================================================

CREATE OR REPLACE PROCEDURE ctl.usp_insert_quality_result(
    p_run_id UUID,
    p_job_id BIGINT,
    p_job_key TEXT,
    p_dataset_key TEXT,
    p_status TEXT,
    p_observed_value TEXT,
    p_expected_value TEXT,
    p_details JSONB
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dq.quality_result (
        run_id,
        job_id,
        job_key,
        dataset_key,
        status,
        observed_value,
        expected_value,
        details
    )
    VALUES (
        p_run_id,
        p_job_id,
        p_job_key,
        p_dataset_key,
        p_status,
        p_observed_value,
        p_expected_value,
        p_details
    );
END;
$$;


-- =========================================================
-- 6) Lineage
-- =========================================================

CREATE OR REPLACE PROCEDURE ctl.usp_insert_dataset_lineage(
    p_run_id UUID,
    p_job_id BIGINT,
    p_job_key TEXT,
    p_source_dataset_key TEXT,
    p_target_dataset_key TEXT,
    p_transformation_type TEXT,
    p_transformation_ref TEXT,
    p_details JSONB
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO lineage.dataset_lineage (
        run_id,
        job_id,
        job_key,
        source_dataset_key,
        target_dataset_key,
        transformation_type,
        transformation_ref,
        details
    )
    VALUES (
        p_run_id,
        p_job_id,
        p_job_key,
        p_source_dataset_key,
        p_target_dataset_key,
        p_transformation_type,
        p_transformation_ref,
        p_details
    );
END;
$$;


-- =========================================================
-- 7) Catalog registration
-- =========================================================

CREATE OR REPLACE PROCEDURE ctl.usp_upsert_pipeline(
    p_pipeline_name TEXT,
    p_domain TEXT,
    p_description TEXT,
    p_owner TEXT,
    p_schedule_cron TEXT,
    p_airflow_dag_id TEXT,
    p_is_active BOOLEAN,
    p_raw_config JSONB
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO meta.pipeline (
        pipeline_name,
        domain,
        description,
        owner,
        schedule_cron,
        airflow_dag_id,
        is_active,
        raw_config,
        updated_at
    )
    VALUES (
        p_pipeline_name,
        p_domain,
        p_description,
        p_owner,
        p_schedule_cron,
        p_airflow_dag_id,
        p_is_active,
        p_raw_config,
        CURRENT_TIMESTAMP
    )
    ON CONFLICT (pipeline_name)
    DO UPDATE SET
        domain = EXCLUDED.domain,
        description = EXCLUDED.description,
        owner = EXCLUDED.owner,
        schedule_cron = EXCLUDED.schedule_cron,
        airflow_dag_id = EXCLUDED.airflow_dag_id,
        is_active = EXCLUDED.is_active,
        raw_config = EXCLUDED.raw_config,
        updated_at = CURRENT_TIMESTAMP;
END;
$$;


CREATE OR REPLACE PROCEDURE ctl.usp_upsert_job(
    p_job_key TEXT,
    p_job_code TEXT,
    p_pipeline_name TEXT,
    p_job_name TEXT,
    p_base_job_name TEXT,
    p_job_type TEXT,
    p_source_type TEXT,
    p_runner TEXT,
    p_layer TEXT,
    p_source_id TEXT,
    p_table_id TEXT,
    p_entity_name TEXT,
    p_manifest_ref TEXT,
    p_target_path TEXT,
    p_config JSONB,
    p_runtime_policy JSONB,
    p_is_active BOOLEAN
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO meta.job (
        job_key,
        job_code,
        pipeline_name,
        job_name,
        base_job_name,
        job_type,
        source_type,
        runner,
        layer,
        source_id,
        table_id,
        entity_name,
        manifest_ref,
        target_path,
        config,
        runtime_policy,
        is_active,
        updated_at
    )
    VALUES (
        p_job_key,
        p_job_code,
        p_pipeline_name,
        p_job_name,
        p_base_job_name,
        p_job_type,
        p_source_type,
        p_runner,
        p_layer,
        p_source_id,
        p_table_id,
        p_entity_name,
        p_manifest_ref,
        p_target_path,
        p_config,
        p_runtime_policy,
        p_is_active,
        CURRENT_TIMESTAMP
    )
    ON CONFLICT (job_key)
    DO UPDATE SET
        job_code = EXCLUDED.job_code,
        pipeline_name = EXCLUDED.pipeline_name,
        job_name = EXCLUDED.job_name,
        base_job_name = EXCLUDED.base_job_name,
        job_type = EXCLUDED.job_type,
        source_type = EXCLUDED.source_type,
        runner = EXCLUDED.runner,
        layer = EXCLUDED.layer,
        source_id = EXCLUDED.source_id,
        table_id = EXCLUDED.table_id,
        entity_name = EXCLUDED.entity_name,
        manifest_ref = EXCLUDED.manifest_ref,
        target_path = EXCLUDED.target_path,
        config = EXCLUDED.config,
        runtime_policy = EXCLUDED.runtime_policy,
        is_active = EXCLUDED.is_active,
        updated_at = CURRENT_TIMESTAMP;
END;
$$;



-- Get-Content .\database\migrations\006_control_plane_usp.sql | docker compose --project-directory . -f compose/compose.phase1.yaml exec -T postgres-warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'