-- -----------------------------------------------------------------------------
-- 013_auth_and_actions.sql
--
-- Dashboard action audit log.
--
-- Dashboard user/auth schema is owned by:
--   017_dashboard_auth_schema.sql
--
-- Keeping auth out of this migration avoids the older BIGINT user_id contract
-- conflicting with the newer TEXT/opaque user_id contract.
-- -----------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS runtime;
CREATE SCHEMA IF NOT EXISTS ctl;

CREATE TABLE IF NOT EXISTS runtime.action_log (
    action_id BIGSERIAL PRIMARY KEY,
    user_id TEXT,
    username TEXT NOT NULL,
    action_type TEXT NOT NULL,
    target_type TEXT,
    target_id TEXT,
    request_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_status TEXT,
    result_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT,
    duration_ms INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE runtime.action_log
    DROP CONSTRAINT IF EXISTS action_log_user_id_fkey;

ALTER TABLE runtime.action_log
    ALTER COLUMN user_id TYPE TEXT USING user_id::TEXT;

ALTER TABLE runtime.action_log
    ADD COLUMN IF NOT EXISTS request_payload JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE runtime.action_log
    ADD COLUMN IF NOT EXISTS result_payload JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS ix_action_log_user_id
    ON runtime.action_log (user_id);

CREATE INDEX IF NOT EXISTS ix_action_log_action_type
    ON runtime.action_log (action_type);

CREATE INDEX IF NOT EXISTS ix_action_log_created_at
    ON runtime.action_log (created_at DESC);

CREATE INDEX IF NOT EXISTS ix_action_log_target
    ON runtime.action_log (target_type, target_id);

DROP PROCEDURE IF EXISTS ctl.usp_insert_action_log(
    TEXT,
    BIGINT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    INTEGER
);

DROP PROCEDURE IF EXISTS ctl.usp_insert_action_log(
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    INTEGER
);

CREATE OR REPLACE PROCEDURE ctl.usp_insert_action_log(
    p_username TEXT,
    p_user_id TEXT,
    p_action_type TEXT,
    p_target_type TEXT,
    p_target_id TEXT,
    p_request_payload TEXT,
    p_result_status TEXT,
    p_result_payload TEXT,
    p_error_message TEXT,
    p_duration_ms INTEGER
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_req JSONB;
    v_res JSONB;
BEGIN
    v_req := COALESCE(NULLIF(p_request_payload, '')::jsonb, '{}'::jsonb);
    v_res := COALESCE(NULLIF(p_result_payload,  '')::jsonb, '{}'::jsonb);

    INSERT INTO runtime.action_log (
        username,
        user_id,
        action_type,
        target_type,
        target_id,
        request_payload,
        result_status,
        result_payload,
        error_message,
        duration_ms
    )
    VALUES (
        p_username,
        p_user_id,
        p_action_type,
        p_target_type,
        p_target_id,
        v_req,
        p_result_status,
        v_res,
        p_error_message,
        p_duration_ms
    );
END;
$$;

CREATE OR REPLACE FUNCTION ctl.usp_list_action_logs(p_limit INTEGER DEFAULT 100)
RETURNS TABLE (
    action_id BIGINT,
    username TEXT,
    action_type TEXT,
    target_type TEXT,
    target_id TEXT,
    result_status TEXT,
    error_message TEXT,
    duration_ms INTEGER,
    created_at TIMESTAMPTZ
)
LANGUAGE sql
AS $$
    SELECT
        action_id,
        username,
        action_type,
        target_type,
        target_id,
        result_status,
        error_message,
        duration_ms,
        created_at
    FROM runtime.action_log
    ORDER BY created_at DESC
    LIMIT COALESCE(p_limit, 100);
$$;
