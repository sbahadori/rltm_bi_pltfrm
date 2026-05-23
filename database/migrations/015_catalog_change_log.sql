-- =============================================================
-- 015_catalog_change_log.sql
-- Audit trail for catalog edits applied through dashboard/API.
-- Source of truth remains configs/batch/pipeline_catalog.json.
-- =============================================================

CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS ctl;

CREATE TABLE IF NOT EXISTS meta.catalog_change_log (
    catalog_change_id BIGSERIAL PRIMARY KEY,
    username          TEXT,
    action_type       TEXT NOT NULL,
    pipeline_name     TEXT,
    job_name          TEXT,
    result_status     TEXT NOT NULL,
    request_payload   JSONB NOT NULL DEFAULT '{}'::jsonb,
    backup_path       TEXT,
    error_message     TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_catalog_change_log_created_at
    ON meta.catalog_change_log (created_at DESC);

CREATE INDEX IF NOT EXISTS ix_catalog_change_log_pipeline_job
    ON meta.catalog_change_log (pipeline_name, job_name);

CREATE OR REPLACE PROCEDURE ctl.usp_insert_catalog_change_log(
    p_username        TEXT,
    p_action_type     TEXT,
    p_pipeline_name   TEXT,
    p_job_name        TEXT,
    p_result_status   TEXT,
    p_request_payload TEXT,
    p_backup_path     TEXT,
    p_error_message   TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_payload JSONB;
BEGIN
    v_payload := COALESCE(NULLIF(p_request_payload, '')::jsonb, '{}'::jsonb);

    INSERT INTO meta.catalog_change_log (
        username,
        action_type,
        pipeline_name,
        job_name,
        result_status,
        request_payload,
        backup_path,
        error_message
    )
    VALUES (
        p_username,
        p_action_type,
        p_pipeline_name,
        p_job_name,
        p_result_status,
        v_payload,
        p_backup_path,
        p_error_message
    );
END;
$$;

CREATE OR REPLACE FUNCTION ctl.usp_list_catalog_change_logs(
    p_limit INTEGER DEFAULT 100
)
RETURNS TABLE (
    catalog_change_id BIGINT,
    username          TEXT,
    action_type       TEXT,
    pipeline_name     TEXT,
    job_name          TEXT,
    result_status     TEXT,
    request_payload   JSONB,
    backup_path       TEXT,
    error_message     TEXT,
    created_at        TIMESTAMPTZ
)
LANGUAGE sql
AS $$
    SELECT
        catalog_change_id,
        username,
        action_type,
        pipeline_name,
        job_name,
        result_status,
        request_payload,
        backup_path,
        error_message,
        created_at
    FROM meta.catalog_change_log
    ORDER BY created_at DESC
    LIMIT p_limit;
$$;

-- Get-Content .\database\migrations\015_catalog_change_log.sql -Raw | docker exec -i postgres-warehouse `  sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
