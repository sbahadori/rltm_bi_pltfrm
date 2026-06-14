-- -----------------------------------------------------------------------------
-- 008_ctl_watermark_usps.sql
-- Purpose:
--   Watermark state read/write functions for incremental batch jobs.
--
-- Table:
--   runtime.watermark_state
--
-- Expected table columns:
--   watermark_id, job_id, job_key, source_id, table_id, watermark_column,
--   last_successful_value, current_value, last_run_id, updated_at
-- -----------------------------------------------------------------------------

DROP FUNCTION IF EXISTS ctl.usp_get_watermark_state(TEXT, TEXT, TEXT, TEXT);

CREATE OR REPLACE FUNCTION ctl.usp_get_watermark_state(
    p_job_key TEXT,
    p_source_id TEXT,
    p_table_id TEXT,
    p_watermark_column TEXT
)
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
    WHERE w.job_key = p_job_key
      AND COALESCE(w.source_id, '') = COALESCE(p_source_id, '')
      AND COALESCE(w.table_id, '') = COALESCE(p_table_id, '')
      AND w.watermark_column = p_watermark_column
    ORDER BY w.updated_at DESC
    LIMIT 1;
$$;


DROP PROCEDURE IF EXISTS ctl.usp_upsert_watermark_state(
    BIGINT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT
);

DROP PROCEDURE IF EXISTS ctl.usp_upsert_watermark_state(
    BIGINT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT
);

CREATE OR REPLACE PROCEDURE ctl.usp_upsert_watermark_state(
    p_job_id BIGINT,
    p_job_key TEXT,
    p_source_id TEXT,
    p_table_id TEXT,
    p_watermark_column TEXT,
    p_last_successful_value TEXT,
    p_current_value TEXT,
    p_last_run_id TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_job_key IS NULL OR trim(p_job_key) = '' THEN
        RAISE EXCEPTION 'p_job_key is required';
    END IF;

    IF p_watermark_column IS NULL OR trim(p_watermark_column) = '' THEN
        RAISE EXCEPTION 'p_watermark_column is required';
    END IF;

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
        p_last_successful_value,
        p_current_value,
        p_last_run_id,
        now()
    )
    ON CONFLICT (
        job_key,
        source_id,
        table_id,
        watermark_column
    )
    DO UPDATE SET
        job_id = COALESCE(EXCLUDED.job_id, runtime.watermark_state.job_id),
        last_successful_value = COALESCE(
            EXCLUDED.last_successful_value,
            runtime.watermark_state.last_successful_value
        ),
        current_value = COALESCE(
            EXCLUDED.current_value,
            runtime.watermark_state.current_value
        ),
        last_run_id = COALESCE(
            EXCLUDED.last_run_id,
            runtime.watermark_state.last_run_id
        ),
        updated_at = now();
END;
$$;


CREATE OR REPLACE PROCEDURE ctl.usp_mark_watermark_success(
    p_job_key TEXT,
    p_source_id TEXT,
    p_table_id TEXT,
    p_watermark_column TEXT,
    p_successful_value TEXT,
    p_last_run_id TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_job_id BIGINT;
BEGIN
    SELECT j.job_id
    INTO v_job_id
    FROM meta.job j
    WHERE j.job_key = p_job_key
       OR j.job_code = p_job_key
    ORDER BY j.updated_at DESC, j.job_id DESC
    LIMIT 1;

    CALL ctl.usp_upsert_watermark_state(
        v_job_id,
        p_job_key,
        p_source_id,
        p_table_id,
        p_watermark_column,
        p_successful_value,
        p_successful_value,
        p_last_run_id
    );
END;
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

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ux_watermark_state'
    ) THEN
        ALTER TABLE runtime.watermark_state
        ADD CONSTRAINT ux_watermark_state
        UNIQUE (
            job_key,
            source_id,
            table_id,
            watermark_column
        );
    END IF;
END;
$$;
