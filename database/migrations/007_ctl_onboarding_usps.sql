-- -----------------------------------------------------------------------------
-- 007_ctl_onboarding_usps.sql
-- Purpose:
--   Onboard pipelines, jobs, datasets, source systems, and dependencies
--   into the PostgreSQL control plane.
--
-- Required by:
--   Airflow metadata-driven DAG loading:
--     SELECT * FROM ctl.usp_list_active_pipeline_specs()
--
--   Batch runners:
--     ctl.usp_get_active_job_metadata(...)
--
-- Important:
--   - meta.pipeline.raw_config stores the full pipeline JSON object.
--   - meta.job.config stores the executable job spec.
--   - meta.job.job_code and meta.job.job_key must match CONTROL_JOB_CODE/KEY.
-- -----------------------------------------------------------------------------

DROP FUNCTION IF EXISTS ctl.usp_list_active_pipeline_specs();

CREATE OR REPLACE FUNCTION ctl.usp_list_active_pipeline_specs()
RETURNS TABLE (
    pipeline_id BIGINT,
    pipeline_name TEXT,
    domain TEXT,
    description TEXT,
    owner TEXT,
    schedule_cron TEXT,
    airflow_dag_id TEXT,
    raw_config JSONB,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
)
LANGUAGE sql
AS $$
    SELECT
        p.pipeline_id,
        p.pipeline_name,
        p.domain,
        p.description,
        p.owner,
        p.schedule_cron,
        p.airflow_dag_id,
        p.raw_config,
        p.created_at,
        p.updated_at
    FROM meta.pipeline p
    WHERE p.is_active IS TRUE
      AND COALESCE((p.raw_config ->> 'enabled')::BOOLEAN, TRUE) IS TRUE
    ORDER BY p.pipeline_name;
$$;


CREATE OR REPLACE PROCEDURE ctl.usp_onboard_pipeline(
    p_pipeline_name TEXT,
    p_domain TEXT,
    p_description TEXT,
    p_owner TEXT,
    p_schedule_cron TEXT,
    p_airflow_dag_id TEXT,
    p_raw_config TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_raw_config JSONB;
BEGIN
    v_raw_config := COALESCE(NULLIF(p_raw_config, '')::jsonb, '{}'::jsonb);

    IF p_pipeline_name IS NULL OR trim(p_pipeline_name) = '' THEN
        RAISE EXCEPTION 'p_pipeline_name is required';
    END IF;

    INSERT INTO meta.pipeline (
        pipeline_name,
        domain,
        description,
        owner,
        schedule_cron,
        airflow_dag_id,
        is_active,
        raw_config,
        created_at,
        updated_at
    )
    VALUES (
        p_pipeline_name,
        p_domain,
        p_description,
        p_owner,
        p_schedule_cron,
        COALESCE(p_airflow_dag_id, p_pipeline_name),
        TRUE,
        v_raw_config,
        now(),
        now()
    )
    ON CONFLICT (pipeline_name)
    DO UPDATE SET
        domain = COALESCE(EXCLUDED.domain, meta.pipeline.domain),
        description = COALESCE(EXCLUDED.description, meta.pipeline.description),
        owner = COALESCE(EXCLUDED.owner, meta.pipeline.owner),
        schedule_cron = EXCLUDED.schedule_cron,
        airflow_dag_id = COALESCE(EXCLUDED.airflow_dag_id, meta.pipeline.airflow_dag_id),
        is_active = TRUE,
        raw_config = EXCLUDED.raw_config,
        updated_at = now();
END;
$$;


CREATE OR REPLACE PROCEDURE ctl.usp_onboard_source_system(
    p_source_system_id TEXT,
    p_source_name TEXT,
    p_source_type TEXT,
    p_connection_ref TEXT,
    p_owner TEXT,
    p_environment TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_source_system_id IS NULL OR trim(p_source_system_id) = '' THEN
        RAISE EXCEPTION 'p_source_system_id is required';
    END IF;

    INSERT INTO meta.source_system (
        source_system_id,
        source_name,
        source_type,
        connection_ref,
        owner,
        environment,
        is_active,
        created_at
    )
    VALUES (
        p_source_system_id,
        COALESCE(NULLIF(p_source_name, ''), p_source_system_id),
        p_source_type,
        p_connection_ref,
        p_owner,
        p_environment,
        TRUE,
        now()
    )
    ON CONFLICT (source_system_id)
    DO UPDATE SET
        source_name = COALESCE(EXCLUDED.source_name, meta.source_system.source_name),
        source_type = COALESCE(EXCLUDED.source_type, meta.source_system.source_type),
        connection_ref = COALESCE(EXCLUDED.connection_ref, meta.source_system.connection_ref),
        owner = COALESCE(EXCLUDED.owner, meta.source_system.owner),
        environment = COALESCE(EXCLUDED.environment, meta.source_system.environment),
        is_active = TRUE;
END;
$$;


CREATE OR REPLACE PROCEDURE ctl.usp_onboard_job(
    p_job_code TEXT,
    p_job_name TEXT,
    p_display_name TEXT,
    p_pipeline_name TEXT,
    p_base_job_name TEXT,
    p_job_type TEXT,
    p_runner TEXT,
    p_layer TEXT,
    p_source_id TEXT,
    p_table_id TEXT,
    p_entity_name TEXT,
    p_manifest_ref TEXT,
    p_target_path TEXT,
    p_source_type TEXT,
    p_config TEXT,
    p_runtime_policy TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_config JSONB;
    v_runtime_policy JSONB;
BEGIN
    v_config := COALESCE(NULLIF(p_config, '')::jsonb, '{}'::jsonb);
    v_runtime_policy := COALESCE(NULLIF(p_runtime_policy, '')::jsonb, '{}'::jsonb);

    IF p_job_code IS NULL OR trim(p_job_code) = '' THEN
        RAISE EXCEPTION 'p_job_code is required';
    END IF;

    IF p_job_name IS NULL OR trim(p_job_name) = '' THEN
        RAISE EXCEPTION 'p_job_name is required for job_code=%', p_job_code;
    END IF;

    IF p_pipeline_name IS NULL OR trim(p_pipeline_name) = '' THEN
        RAISE EXCEPTION 'p_pipeline_name is required for job_code=%', p_job_code;
    END IF;

    INSERT INTO meta.job (
        job_code,
        job_name,
        display_name,
        pipeline_name,
        base_job_name,
        job_type,
        runner,
        layer,
        source_id,
        table_id,
        entity_name,
        manifest_ref,
        target_path,
        active_flag,
        effective_start_date,
        effective_end_date,
        created_at,
        updated_at,
        job_key,
        is_active,
        source_type,
        config,
        runtime_policy
    )
    VALUES (
        p_job_code,
        p_job_name,
        COALESCE(NULLIF(p_display_name, ''), p_job_name),
        p_pipeline_name,
        COALESCE(NULLIF(p_base_job_name, ''), p_job_name),
        p_job_type,
        COALESCE(NULLIF(p_runner, ''), p_job_type),
        p_layer,
        p_source_id,
        p_table_id,
        p_entity_name,
        p_manifest_ref,
        p_target_path,
        TRUE,
        NULL,
        NULL,
        now(),
        now(),
        p_job_code,
        TRUE,
        p_source_type,
        v_config,
        v_runtime_policy
    )
    ON CONFLICT (job_code)
    DO UPDATE SET
        job_name = EXCLUDED.job_name,
        display_name = EXCLUDED.display_name,
        pipeline_name = EXCLUDED.pipeline_name,
        base_job_name = EXCLUDED.base_job_name,
        job_type = EXCLUDED.job_type,
        runner = EXCLUDED.runner,
        layer = EXCLUDED.layer,
        source_id = EXCLUDED.source_id,
        table_id = EXCLUDED.table_id,
        entity_name = EXCLUDED.entity_name,
        manifest_ref = EXCLUDED.manifest_ref,
        target_path = EXCLUDED.target_path,
        active_flag = TRUE,
        effective_start_date = NULL,
        effective_end_date = NULL,
        updated_at = now(),
        job_key = EXCLUDED.job_key,
        is_active = TRUE,
        source_type = EXCLUDED.source_type,
        config = EXCLUDED.config,
        runtime_policy = EXCLUDED.runtime_policy;
END;
$$;


CREATE OR REPLACE PROCEDURE ctl.usp_onboard_dataset(
    p_dataset_key TEXT,
    p_dataset_name TEXT,
    p_source_system_id TEXT,
    p_source_object TEXT,
    p_layer TEXT,
    p_target_path TEXT,
    p_target_format TEXT,
    p_primary_keys TEXT,
    p_schema_definition TEXT,
    p_contract TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_primary_keys JSONB;
    v_schema_definition JSONB;
    v_contract JSONB;
BEGIN
    v_primary_keys := COALESCE(NULLIF(p_primary_keys, '')::jsonb, '[]'::jsonb);
    v_schema_definition := COALESCE(NULLIF(p_schema_definition, '')::jsonb, '{}'::jsonb);
    v_contract := COALESCE(NULLIF(p_contract, '')::jsonb, '{}'::jsonb);

    IF p_dataset_key IS NULL OR trim(p_dataset_key) = '' THEN
        RAISE EXCEPTION 'p_dataset_key is required';
    END IF;

    INSERT INTO meta.dataset (
        dataset_key,
        dataset_name,
        source_system_id,
        source_object,
        layer,
        target_path,
        target_format,
        primary_keys,
        schema_definition,
        contract,
        is_active,
        created_at,
        updated_at
    )
    VALUES (
        p_dataset_key,
        COALESCE(NULLIF(p_dataset_name, ''), p_dataset_key),
        p_source_system_id,
        p_source_object,
        p_layer,
        p_target_path,
        COALESCE(NULLIF(p_target_format, ''), 'delta'),
        v_primary_keys,
        v_schema_definition,
        v_contract,
        TRUE,
        now(),
        now()
    )
    ON CONFLICT (dataset_key)
    DO UPDATE SET
        dataset_name = EXCLUDED.dataset_name,
        source_system_id = EXCLUDED.source_system_id,
        source_object = EXCLUDED.source_object,
        layer = EXCLUDED.layer,
        target_path = EXCLUDED.target_path,
        target_format = EXCLUDED.target_format,
        primary_keys = EXCLUDED.primary_keys,
        schema_definition = EXCLUDED.schema_definition,
        contract = EXCLUDED.contract,
        is_active = TRUE,
        updated_at = now();
END;
$$;


CREATE OR REPLACE PROCEDURE ctl.usp_onboard_job_dependency(
    p_job_code TEXT,
    p_depends_on_job_code TEXT,
    p_dependency_type TEXT DEFAULT 'success'
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_job_id BIGINT;
    v_depends_on_job_id BIGINT;
BEGIN
    IF p_job_code IS NULL OR trim(p_job_code) = '' THEN
        RAISE EXCEPTION 'p_job_code is required';
    END IF;

    IF p_depends_on_job_code IS NULL OR trim(p_depends_on_job_code) = '' THEN
        RAISE EXCEPTION 'p_depends_on_job_code is required';
    END IF;

    SELECT job_id
    INTO v_job_id
    FROM meta.job
    WHERE job_code = p_job_code
      AND is_active IS TRUE
      AND active_flag IS TRUE
    ORDER BY updated_at DESC, job_id DESC
    LIMIT 1;

    SELECT job_id
    INTO v_depends_on_job_id
    FROM meta.job
    WHERE job_code = p_depends_on_job_code
      AND is_active IS TRUE
      AND active_flag IS TRUE
    ORDER BY updated_at DESC, job_id DESC
    LIMIT 1;

    IF v_job_id IS NULL THEN
        RAISE EXCEPTION 'Job not found for dependency onboarding: %', p_job_code;
    END IF;

    IF v_depends_on_job_id IS NULL THEN
        RAISE EXCEPTION 'Depends-on job not found for dependency onboarding: %', p_depends_on_job_code;
    END IF;

    INSERT INTO meta.job_dependency (
        job_id,
        depends_on_job_id,
        dependency_type,
        is_active,
        created_at
    )
    VALUES (
        v_job_id,
        v_depends_on_job_id,
        COALESCE(NULLIF(p_dependency_type, ''), 'success'),
        TRUE,
        now()
    )
    ON CONFLICT (job_id, depends_on_job_id, dependency_type)
    DO UPDATE SET
        is_active = TRUE;
END;
$$;
