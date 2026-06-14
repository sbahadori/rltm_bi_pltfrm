CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS ctl;

CREATE TABLE IF NOT EXISTS meta.dashboard_user (
    user_id TEXT PRIMARY KEY DEFAULT md5(random()::text || clock_timestamp()::text),
    username TEXT NOT NULL,
    email TEXT,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'viewer',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_login_at TIMESTAMPTZ
);

ALTER TABLE meta.dashboard_user
    ADD COLUMN IF NOT EXISTS user_id TEXT DEFAULT md5(random()::text || clock_timestamp()::text);

ALTER TABLE meta.dashboard_user
    ADD COLUMN IF NOT EXISTS username TEXT;

ALTER TABLE meta.dashboard_user
    ADD COLUMN IF NOT EXISTS email TEXT;

ALTER TABLE meta.dashboard_user
    ADD COLUMN IF NOT EXISTS password_hash TEXT;

ALTER TABLE meta.dashboard_user
    ADD COLUMN IF NOT EXISTS role TEXT DEFAULT 'viewer';

ALTER TABLE meta.dashboard_user
    ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;

ALTER TABLE meta.dashboard_user
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now();

ALTER TABLE meta.dashboard_user
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();

ALTER TABLE meta.dashboard_user
    ADD COLUMN IF NOT EXISTS last_login_at TIMESTAMPTZ;

CREATE UNIQUE INDEX IF NOT EXISTS ux_dashboard_user_username
    ON meta.dashboard_user(username);

DROP FUNCTION IF EXISTS ctl.usp_get_dashboard_user(TEXT);

CREATE OR REPLACE FUNCTION ctl.usp_get_dashboard_user(p_username TEXT)
RETURNS TABLE (
    user_id TEXT,
    username TEXT,
    email TEXT,
    password_hash TEXT,
    role TEXT,
    is_active BOOLEAN,
    last_login_at TIMESTAMPTZ
)
LANGUAGE sql
AS $$
    SELECT
        u.user_id::TEXT,
        u.username,
        u.email,
        u.password_hash,
        COALESCE(u.role, 'viewer') AS role,
        COALESCE(u.is_active, TRUE) AS is_active,
        u.last_login_at
    FROM meta.dashboard_user u
    WHERE u.username = p_username
      AND COALESCE(u.is_active, TRUE) = TRUE
    LIMIT 1;
$$;

DROP PROCEDURE IF EXISTS ctl.usp_update_last_login(TEXT);

CREATE OR REPLACE PROCEDURE ctl.usp_update_last_login(p_username TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE meta.dashboard_user
    SET last_login_at = now(),
        updated_at = now()
    WHERE username = p_username;
END;
$$;

DROP PROCEDURE IF EXISTS ctl.usp_upsert_dashboard_user(TEXT, TEXT, TEXT, TEXT);

CREATE OR REPLACE PROCEDURE ctl.usp_upsert_dashboard_user(
    p_username TEXT,
    p_email TEXT,
    p_password_hash TEXT,
    p_role TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO meta.dashboard_user (
        username,
        email,
        password_hash,
        role,
        is_active,
        created_at,
        updated_at
    )
    VALUES (
        p_username,
        p_email,
        p_password_hash,
        COALESCE(NULLIF(p_role, ''), 'viewer'),
        TRUE,
        now(),
        now()
    )
    ON CONFLICT (username)
    DO UPDATE SET
        email = EXCLUDED.email,
        password_hash = EXCLUDED.password_hash,
        role = EXCLUDED.role,
        is_active = TRUE,
        updated_at = now();
END;
$$;
