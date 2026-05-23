-- =============================================================
-- 013_auth_and_actions.sql
-- Dashboard user auth + action audit log
-- =============================================================

-- ─────────────────────────────────────────────────────────────
-- 1. Dashboard users
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS meta.dashboard_user (
    user_id     BIGSERIAL PRIMARY KEY,
    username    TEXT NOT NULL UNIQUE,
    email       TEXT,
    password_hash TEXT NOT NULL,
    role        TEXT NOT NULL DEFAULT 'viewer'
                CHECK (role IN ('admin', 'operator', 'viewer')),
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_login_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_dashboard_user_username
    ON meta.dashboard_user (username);

CREATE INDEX IF NOT EXISTS ix_dashboard_user_role
    ON meta.dashboard_user (role);

-- ─────────────────────────────────────────────────────────────
-- 2. Action audit log — every execution action is recorded
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS runtime.action_log (
    action_id       BIGSERIAL PRIMARY KEY,
    user_id         BIGINT REFERENCES meta.dashboard_user(user_id),
    username        TEXT NOT NULL,
    action_type     TEXT NOT NULL,   -- dag_trigger, dag_pause, stream_restart, onboard, ...
    target_type     TEXT,            -- dag, stream, pipeline
    target_id       TEXT,            -- dag_id, unit_name, pipeline_name
    request_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_status   TEXT,            -- success, failed, pending
    result_payload  JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message   TEXT,
    duration_ms     INTEGER,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_action_log_user_id
    ON runtime.action_log (user_id);

CREATE INDEX IF NOT EXISTS ix_action_log_action_type
    ON runtime.action_log (action_type);

CREATE INDEX IF NOT EXISTS ix_action_log_created_at
    ON runtime.action_log (created_at DESC);

CREATE INDEX IF NOT EXISTS ix_action_log_target
    ON runtime.action_log (target_type, target_id);

-- ─────────────────────────────────────────────────────────────
-- 3. USPs
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE ctl.usp_upsert_dashboard_user(
    p_username      TEXT,
    p_email         TEXT,
    p_password_hash TEXT,
    p_role          TEXT DEFAULT 'viewer'
)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO meta.dashboard_user (username, email, password_hash, role)
    VALUES (p_username, p_email, p_password_hash, p_role)
    ON CONFLICT (username)
    DO UPDATE SET
        email         = COALESCE(EXCLUDED.email, meta.dashboard_user.email),
        password_hash = EXCLUDED.password_hash,
        role          = EXCLUDED.role,
        updated_at    = now();
END;
$$;


CREATE OR REPLACE FUNCTION ctl.usp_get_dashboard_user(p_username TEXT)
RETURNS TABLE (
    user_id      BIGINT,
    username     TEXT,
    email        TEXT,
    password_hash TEXT,
    role         TEXT,
    is_active    BOOLEAN,
    last_login_at TIMESTAMPTZ
)
LANGUAGE sql AS $$
    SELECT user_id, username, email, password_hash, role, is_active, last_login_at
    FROM meta.dashboard_user
    WHERE username = p_username AND is_active IS TRUE
    LIMIT 1;
$$;


CREATE OR REPLACE PROCEDURE ctl.usp_update_last_login(p_username TEXT)
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE meta.dashboard_user SET last_login_at = now() WHERE username = p_username;
END;
$$;


CREATE OR REPLACE PROCEDURE ctl.usp_insert_action_log(
    p_username        TEXT,
    p_user_id         BIGINT,
    p_action_type     TEXT,
    p_target_type     TEXT,
    p_target_id       TEXT,
    p_request_payload TEXT,
    p_result_status   TEXT,
    p_result_payload  TEXT,
    p_error_message   TEXT,
    p_duration_ms     INTEGER
)
LANGUAGE plpgsql AS $$
DECLARE
    v_req  JSONB;
    v_res  JSONB;
BEGIN
    v_req := COALESCE(NULLIF(p_request_payload, '')::jsonb, '{}'::jsonb);
    v_res := COALESCE(NULLIF(p_result_payload,  '')::jsonb, '{}'::jsonb);

    INSERT INTO runtime.action_log (
        username, user_id, action_type, target_type, target_id,
        request_payload, result_status, result_payload, error_message, duration_ms
    ) VALUES (
        p_username, p_user_id, p_action_type, p_target_type, p_target_id,
        v_req, p_result_status, v_res, p_error_message, p_duration_ms
    );
END;
$$;


CREATE OR REPLACE FUNCTION ctl.usp_list_action_logs(p_limit INTEGER DEFAULT 100)
RETURNS TABLE (
    action_id     BIGINT,
    username      TEXT,
    action_type   TEXT,
    target_type   TEXT,
    target_id     TEXT,
    result_status TEXT,
    error_message TEXT,
    duration_ms   INTEGER,
    created_at    TIMESTAMPTZ
)
LANGUAGE sql AS $$
    SELECT action_id, username, action_type, target_type, target_id,
           result_status, error_message, duration_ms, created_at
    FROM runtime.action_log
    ORDER BY created_at DESC
    LIMIT COALESCE(p_limit, 100);
$$;


-- ─────────────────────────────────────────────────────────────
-- 4. Seed default admin user  (password: admin — CHANGE IN PROD)
--    bcrypt hash for "admin"
-- ─────────────────────────────────────────────────────────────
INSERT INTO meta.dashboard_user (username, email, password_hash, role)
VALUES (
    'admin',
    'admin@localhost',
    '$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW',
    'admin'
)
ON CONFLICT (username) DO NOTHING;


-- Get-Content .\database\migrations\013_auth_and_actions.sql -Raw | docker exec -i postgres-warehouse `  sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'