CREATE OR REPLACE FUNCTION ctl.usp_get_active_job_metadata(
    p_job_id BIGINT DEFAULT NULL,
    p_job_key TEXT DEFAULT NULL,
    p_job_code TEXT DEFAULT NULL
)
RETURNS TABLE (
    job_id BIGINT,
    job_code TEXT,
    job_name TEXT,
    display_name TEXT,
    pipeline_name TEXT,
    base_job_name TEXT,
    job_type TEXT,
    runner TEXT,
    layer TEXT,
    source_id TEXT,
    table_id TEXT,
    entity_name TEXT,
    manifest_ref TEXT,
    target_path TEXT,
    active_flag BOOLEAN,
    effective_start_date TIMESTAMPTZ,
    effective_end_date TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    job_key TEXT,
    is_active BOOLEAN,
    source_type TEXT,
    config JSONB,
    runtime_policy JSONB
)
LANGUAGE sql
AS $$
    SELECT
        j.job_id,
        j.job_code,
        j.job_name,
        j.display_name,
        j.pipeline_name,
        j.base_job_name,
        j.job_type,
        j.runner,
        j.layer,
        j.source_id,
        j.table_id,
        j.entity_name,
        j.manifest_ref,
        j.target_path,
        j.active_flag,
        j.effective_start_date,
        j.effective_end_date,
        j.created_at,
        j.updated_at,
        j.job_key,
        j.is_active,
        j.source_type,
        j.config,
        j.runtime_policy
    FROM meta.job j
    WHERE
        (p_job_id IS NULL OR j.job_id = p_job_id)
        AND (p_job_key IS NULL OR j.job_key = p_job_key)
        AND (p_job_code IS NULL OR j.job_code = p_job_code)
        AND j.is_active IS TRUE
        AND j.active_flag IS TRUE
        AND (j.effective_start_date IS NULL OR j.effective_start_date <= now())
        AND (j.effective_end_date IS NULL OR j.effective_end_date > now())
    ORDER BY j.updated_at DESC, j.job_id DESC
    LIMIT 1;
$$;


CREATE OR REPLACE FUNCTION ctl.usp_get_active_job_identity(
    p_job_id BIGINT DEFAULT NULL,
    p_job_key TEXT DEFAULT NULL,
    p_job_code TEXT DEFAULT NULL
)
RETURNS TABLE (
    job_id BIGINT,
    job_code TEXT,
    job_key TEXT,
    job_name TEXT,
    pipeline_name TEXT,
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
        j.job_code,
        j.job_key,
        j.job_name,
        j.pipeline_name,
        j.base_job_name,
        j.source_id,
        j.table_id,
        j.entity_name,
        j.layer,
        j.runner,
        j.target_path
    FROM meta.job j
    WHERE
        (p_job_id IS NULL OR j.job_id = p_job_id)
        AND (p_job_key IS NULL OR j.job_key = p_job_key)
        AND (p_job_code IS NULL OR j.job_code = p_job_code)
        AND j.is_active IS TRUE
        AND j.active_flag IS TRUE
        AND (j.effective_start_date IS NULL OR j.effective_start_date <= now())
        AND (j.effective_end_date IS NULL OR j.effective_end_date > now())
    ORDER BY j.updated_at DESC, j.job_id DESC
    LIMIT 1;
$$;


CREATE OR REPLACE PROCEDURE ctl.usp_upsert_job_run(
    p_run_id TEXT,
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
    p_effective_start_date TIMESTAMPTZ,
    p_effective_end_date TIMESTAMPTZ,
    p_started_at TIMESTAMPTZ,
    p_ended_at TIMESTAMPTZ,
    p_duration_seconds DOUBLE PRECISION,
    p_records_read BIGINT,
    p_records_written BIGINT,
    p_source_path TEXT,
    p_target_path TEXT,
    p_payload TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_payload JSONB;
    v_job_key TEXT;
BEGIN
    v_payload := COALESCE(NULLIF(p_payload, '')::jsonb, '{}'::jsonb);

    SELECT j.job_key
    INTO v_job_key
    FROM meta.job j
    WHERE j.job_id = p_job_id;

    v_job_key := COALESCE(v_job_key, p_job_code);

    INSERT INTO runtime.job_run (
        run_id,
        job_id,
        job_key,
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
        payload,
        created_at
    )
    VALUES (
        p_run_id,
        p_job_id,
        v_job_key,
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
        v_payload,
        now()
    )
    ON CONFLICT (run_id)
    DO UPDATE SET
        job_id = COALESCE(EXCLUDED.job_id, runtime.job_run.job_id),
        job_key = COALESCE(EXCLUDED.job_key, runtime.job_run.job_key),
        job_code = COALESCE(EXCLUDED.job_code, runtime.job_run.job_code),
        job_name = COALESCE(EXCLUDED.job_name, runtime.job_run.job_name),
        pipeline_name = COALESCE(EXCLUDED.pipeline_name, runtime.job_run.pipeline_name),
        base_job_name = COALESCE(EXCLUDED.base_job_name, runtime.job_run.base_job_name),
        source_id = COALESCE(EXCLUDED.source_id, runtime.job_run.source_id),
        table_id = COALESCE(EXCLUDED.table_id, runtime.job_run.table_id),
        entity_name = COALESCE(EXCLUDED.entity_name, runtime.job_run.entity_name),
        layer = COALESCE(EXCLUDED.layer, runtime.job_run.layer),
        runner = COALESCE(EXCLUDED.runner, runtime.job_run.runner),
        airflow_dag_id = COALESCE(EXCLUDED.airflow_dag_id, runtime.job_run.airflow_dag_id),
        airflow_dag_run_id = COALESCE(EXCLUDED.airflow_dag_run_id, runtime.job_run.airflow_dag_run_id),
        airflow_task_id = COALESCE(EXCLUDED.airflow_task_id, runtime.job_run.airflow_task_id),
        airflow_try_number = COALESCE(EXCLUDED.airflow_try_number, runtime.job_run.airflow_try_number),
        status = COALESCE(EXCLUDED.status, runtime.job_run.status),
        status_reason = COALESCE(EXCLUDED.status_reason, runtime.job_run.status_reason),
        error_message = COALESCE(EXCLUDED.error_message, runtime.job_run.error_message),
        effective_start_date = COALESCE(EXCLUDED.effective_start_date, runtime.job_run.effective_start_date),
        effective_end_date = COALESCE(EXCLUDED.effective_end_date, runtime.job_run.effective_end_date),
        started_at = COALESCE(runtime.job_run.started_at, EXCLUDED.started_at),
        ended_at = COALESCE(EXCLUDED.ended_at, runtime.job_run.ended_at),
        duration_seconds = COALESCE(EXCLUDED.duration_seconds, runtime.job_run.duration_seconds),
        records_read = COALESCE(EXCLUDED.records_read, runtime.job_run.records_read),
        records_written = COALESCE(EXCLUDED.records_written, runtime.job_run.records_written),
        source_path = COALESCE(EXCLUDED.source_path, runtime.job_run.source_path),
        target_path = COALESCE(EXCLUDED.target_path, runtime.job_run.target_path),
        payload = EXCLUDED.payload;
END;
$$;


CREATE OR REPLACE PROCEDURE ctl.usp_insert_job_event(
    p_run_id TEXT,
    p_job_id BIGINT,
    p_job_key TEXT,
    p_event_type TEXT,
    p_event_message TEXT,
    p_event_payload TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_payload JSONB;
BEGIN
    v_payload := COALESCE(NULLIF(p_event_payload, '')::jsonb, '{}'::jsonb);

    INSERT INTO runtime.job_event (
        run_id,
        job_id,
        job_key,
        event_type,
        event_message,
        event_payload,
        created_at
    )
    VALUES (
        p_run_id,
        p_job_id,
        p_job_key,
        p_event_type,
        p_event_message,
        v_payload,
        now()
    );
END;
$$;


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
    status TEXT,
    status_reason TEXT,
    error_message TEXT,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    duration_seconds DOUBLE PRECISION,
    records_read BIGINT,
    records_written BIGINT,
    source_path TEXT,
    target_path TEXT,
    created_at TIMESTAMPTZ
)
LANGUAGE sql
AS $$
    SELECT
        r.run_id,
        r.job_id,
        r.job_key,
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
        (p_job_code IS NULL OR r.job_code = p_job_code OR r.job_key = p_job_code)
        AND (p_source_id IS NULL OR r.source_id = p_source_id)
        AND (p_table_id IS NULL OR r.table_id = p_table_id)
        AND (p_pipeline_name IS NULL OR r.pipeline_name = p_pipeline_name)
        AND (p_job_name IS NULL OR r.job_name = p_job_name OR r.base_job_name = p_job_name)
    ORDER BY COALESCE(r.started_at, r.created_at) DESC
    LIMIT 1;
$$;


CREATE OR REPLACE FUNCTION ctl.usp_list_control_jobs()
RETURNS TABLE (
    job_id BIGINT,
    job_code TEXT,
    job_name TEXT,
    display_name TEXT,
    pipeline_name TEXT,
    base_job_name TEXT,
    job_type TEXT,
    runner TEXT,
    layer TEXT,
    source_id TEXT,
    table_id TEXT,
    entity_name TEXT,
    manifest_ref TEXT,
    target_path TEXT,
    active_flag BOOLEAN,
    effective_start_date TIMESTAMPTZ,
    effective_end_date TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    job_key TEXT,
    is_active BOOLEAN,
    source_type TEXT,
    config JSONB,
    runtime_policy JSONB
)
LANGUAGE sql
AS $$
    SELECT
        j.job_id,
        j.job_code,
        j.job_name,
        j.display_name,
        j.pipeline_name,
        j.base_job_name,
        j.job_type,
        j.runner,
        j.layer,
        j.source_id,
        j.table_id,
        j.entity_name,
        j.manifest_ref,
        j.target_path,
        j.active_flag,
        j.effective_start_date,
        j.effective_end_date,
        j.created_at,
        j.updated_at,
        j.job_key,
        j.is_active,
        j.source_type,
        j.config,
        j.runtime_policy
    FROM meta.job j
    ORDER BY j.pipeline_name, j.job_name, j.job_id;
$$;


CREATE OR REPLACE FUNCTION ctl.usp_list_runtime_job_runs(
    p_limit INTEGER DEFAULT 50
)
RETURNS TABLE (
    run_id TEXT,
    job_id BIGINT,
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
    status TEXT,
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
    payload JSONB,
    created_at TIMESTAMPTZ
)
LANGUAGE sql
AS $$
    SELECT
        r.run_id,
        r.job_id,
        r.job_key,
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
        r.payload,
        r.created_at
    FROM runtime.job_run r
    ORDER BY COALESCE(r.started_at, r.created_at) DESC
    LIMIT COALESCE(p_limit, 50);
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
    updated_at TIMESTAMPTZ
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
        w.last_run_id,
        w.updated_at
    FROM runtime.watermark_state w
    ORDER BY w.updated_at DESC;
$$;


CREATE OR REPLACE FUNCTION ctl.usp_list_quality_results(
    p_limit INTEGER DEFAULT 100
)
RETURNS TABLE (
    result_id BIGINT,
    rule_id BIGINT,
    run_id TEXT,
    job_id BIGINT,
    job_key TEXT,
    dataset_key TEXT,
    status TEXT,
    observed_value TEXT,
    expected_value TEXT,
    details JSONB,
    created_at TIMESTAMPTZ
)
LANGUAGE sql
AS $$
    SELECT
        q.result_id,
        q.rule_id,
        q.run_id,
        q.job_id,
        q.job_key,
        q.dataset_key,
        q.status,
        q.observed_value,
        q.expected_value,
        q.details,
        q.created_at
    FROM dq.quality_result q
    ORDER BY q.created_at DESC
    LIMIT COALESCE(p_limit, 100);
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
    created_at TIMESTAMPTZ
)
LANGUAGE sql
AS $$
    SELECT
        l.lineage_id,
        l.run_id,
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
    LIMIT COALESCE(p_limit, 100);
$$;