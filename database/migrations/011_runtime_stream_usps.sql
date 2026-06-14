DROP PROCEDURE IF EXISTS ctl.usp_upsert_stream_unit_current(
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    INTEGER,
    INTEGER,
    INTEGER,
    INTEGER,
    TEXT,
    TIMESTAMPTZ,
    INTEGER,
    BIGINT,
    BIGINT,
    BIGINT,
    BIGINT,
    BIGINT,
    BIGINT,
    BIGINT,
    BIGINT,
    BOOLEAN,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT
);

CREATE OR REPLACE PROCEDURE ctl.usp_upsert_stream_unit_current(
    p_unit_name TEXT,
    p_stream_name TEXT,
    p_layer TEXT,
    p_run_id TEXT,

    p_computed_status TEXT,
    p_status_reason TEXT,

    p_pid INTEGER,
    p_returncode INTEGER,
    p_retries INTEGER,
    p_max_retries INTEGER,

    p_heartbeat_status TEXT,
    p_heartbeat_ts TIMESTAMPTZ,
    p_heartbeat_age_seconds INTEGER,

    p_last_batch_id BIGINT,
    p_last_input_rows BIGINT,
    p_last_batch_rows BIGINT,

    p_last_valid_rows BIGINT,
    p_last_invalid_rows BIGINT,

    p_last_written_rows BIGINT,
    p_last_written_valid_rows BIGINT,
    p_last_written_invalid_rows BIGINT,

    p_last_write_ok BOOLEAN,
    p_last_message TEXT,
    p_last_error TEXT,

    p_target_path TEXT,
    p_checkpoint_path TEXT,

    p_payload TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_payload JSONB;
BEGIN
    v_payload := COALESCE(NULLIF(p_payload, '')::jsonb, '{}'::jsonb);

    INSERT INTO runtime.stream_unit_current (
        unit_name,
        stream_name,
        layer,
        run_id,
        computed_status,
        status_reason,
        pid,
        returncode,
        retries,
        max_retries,
        heartbeat_status,
        heartbeat_ts,
        heartbeat_age_seconds,
        last_batch_id,
        last_input_rows,
        last_batch_rows,
        last_valid_rows,
        last_invalid_rows,
        last_written_rows,
        last_written_valid_rows,
        last_written_invalid_rows,
        last_write_ok,
        last_message,
        last_error,
        target_path,
        checkpoint_path,
        payload,
        updated_at
    )
    VALUES (
        p_unit_name,
        p_stream_name,
        p_layer,
        p_run_id,
        p_computed_status,
        p_status_reason,
        p_pid,
        p_returncode,
        p_retries,
        p_max_retries,
        p_heartbeat_status,
        p_heartbeat_ts,
        p_heartbeat_age_seconds,
        p_last_batch_id,
        p_last_input_rows,
        p_last_batch_rows,
        p_last_valid_rows,
        p_last_invalid_rows,
        p_last_written_rows,
        p_last_written_valid_rows,
        p_last_written_invalid_rows,
        p_last_write_ok,
        p_last_message,
        p_last_error,
        p_target_path,
        p_checkpoint_path,
        v_payload,
        now()
    )
    ON CONFLICT (unit_name)
    DO UPDATE SET
        stream_name = EXCLUDED.stream_name,
        layer = EXCLUDED.layer,
        run_id = COALESCE(EXCLUDED.run_id, runtime.stream_unit_current.run_id),
        computed_status = COALESCE(EXCLUDED.computed_status, runtime.stream_unit_current.computed_status),
        status_reason = COALESCE(EXCLUDED.status_reason, runtime.stream_unit_current.status_reason),
        pid = COALESCE(EXCLUDED.pid, runtime.stream_unit_current.pid),
        returncode = COALESCE(EXCLUDED.returncode, runtime.stream_unit_current.returncode),
        retries = COALESCE(EXCLUDED.retries, runtime.stream_unit_current.retries),
        max_retries = COALESCE(EXCLUDED.max_retries, runtime.stream_unit_current.max_retries),
        heartbeat_status = COALESCE(EXCLUDED.heartbeat_status, runtime.stream_unit_current.heartbeat_status),
        heartbeat_ts = COALESCE(EXCLUDED.heartbeat_ts, runtime.stream_unit_current.heartbeat_ts),
        heartbeat_age_seconds = COALESCE(EXCLUDED.heartbeat_age_seconds, runtime.stream_unit_current.heartbeat_age_seconds),
        last_batch_id = COALESCE(EXCLUDED.last_batch_id, runtime.stream_unit_current.last_batch_id),
        last_input_rows = COALESCE(EXCLUDED.last_input_rows, runtime.stream_unit_current.last_input_rows),
        last_batch_rows = COALESCE(EXCLUDED.last_batch_rows, runtime.stream_unit_current.last_batch_rows),
        last_valid_rows = COALESCE(EXCLUDED.last_valid_rows, runtime.stream_unit_current.last_valid_rows),
        last_invalid_rows = COALESCE(EXCLUDED.last_invalid_rows, runtime.stream_unit_current.last_invalid_rows),
        last_written_rows = COALESCE(EXCLUDED.last_written_rows, runtime.stream_unit_current.last_written_rows),
        last_written_valid_rows = COALESCE(EXCLUDED.last_written_valid_rows, runtime.stream_unit_current.last_written_valid_rows),
        last_written_invalid_rows = COALESCE(EXCLUDED.last_written_invalid_rows, runtime.stream_unit_current.last_written_invalid_rows),
        last_write_ok = COALESCE(EXCLUDED.last_write_ok, runtime.stream_unit_current.last_write_ok),
        last_message = COALESCE(EXCLUDED.last_message, runtime.stream_unit_current.last_message),
        last_error =
            CASE
                WHEN EXCLUDED.last_write_ok IS TRUE THEN NULL
                WHEN EXCLUDED.last_error IS NOT NULL THEN EXCLUDED.last_error
                ELSE runtime.stream_unit_current.last_error
            END,
        target_path = COALESCE(EXCLUDED.target_path, runtime.stream_unit_current.target_path),
        checkpoint_path = COALESCE(EXCLUDED.checkpoint_path, runtime.stream_unit_current.checkpoint_path),
        payload = EXCLUDED.payload,
        updated_at = now();
END;
$$;


DROP PROCEDURE IF EXISTS ctl.usp_insert_stream_batch_metric(
    TEXT,
    TEXT,
    TEXT,
    BIGINT,
    BIGINT,
    BIGINT,
    BIGINT,
    BIGINT,
    BIGINT,
    BIGINT,
    BIGINT,
    BOOLEAN,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TIMESTAMPTZ,
    TEXT
);

CREATE OR REPLACE PROCEDURE ctl.usp_insert_stream_batch_metric(
    p_unit_name TEXT,
    p_stream_name TEXT,
    p_layer TEXT,
    p_run_id TEXT,

    p_batch_id BIGINT,

    p_input_rows BIGINT,
    p_batch_rows BIGINT,

    p_valid_rows BIGINT,
    p_invalid_rows BIGINT,

    p_written_rows BIGINT,
    p_written_valid_rows BIGINT,
    p_written_invalid_rows BIGINT,

    p_write_ok BOOLEAN,
    p_message TEXT,
    p_error_message TEXT,

    p_target_path TEXT,
    p_checkpoint_path TEXT,

    p_batch_ts TIMESTAMPTZ,
    p_payload TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_payload JSONB;
BEGIN
    v_payload := COALESCE(NULLIF(p_payload, '')::jsonb, '{}'::jsonb);

    INSERT INTO runtime.stream_batch_metric (
        unit_name,
        stream_name,
        layer,
        run_id,
        batch_id,
        input_rows,
        batch_rows,
        valid_rows,
        invalid_rows,
        written_rows,
        written_valid_rows,
        written_invalid_rows,
        write_ok,
        message,
        error_message,
        target_path,
        checkpoint_path,
        batch_ts,
        payload,
        created_at,
        updated_at
    )
    VALUES (
        p_unit_name,
        p_stream_name,
        p_layer,
        p_run_id,
        p_batch_id,
        p_input_rows,
        p_batch_rows,
        p_valid_rows,
        p_invalid_rows,
        p_written_rows,
        p_written_valid_rows,
        p_written_invalid_rows,
        COALESCE(p_write_ok, FALSE),
        p_message,
        p_error_message,
        p_target_path,
        p_checkpoint_path,
        COALESCE(p_batch_ts, now()),
        v_payload,
        now(),
        now()
    )
    ON CONFLICT (unit_name, run_id, batch_id)
    DO UPDATE SET
        stream_name = EXCLUDED.stream_name,
        layer = EXCLUDED.layer,
        run_id = COALESCE(EXCLUDED.run_id, runtime.stream_batch_metric.run_id),
        input_rows = COALESCE(EXCLUDED.input_rows, runtime.stream_batch_metric.input_rows),
        batch_rows = COALESCE(EXCLUDED.batch_rows, runtime.stream_batch_metric.batch_rows),
        valid_rows = COALESCE(EXCLUDED.valid_rows, runtime.stream_batch_metric.valid_rows),
        invalid_rows = COALESCE(EXCLUDED.invalid_rows, runtime.stream_batch_metric.invalid_rows),
        written_rows = COALESCE(EXCLUDED.written_rows, runtime.stream_batch_metric.written_rows),
        written_valid_rows = COALESCE(EXCLUDED.written_valid_rows, runtime.stream_batch_metric.written_valid_rows),
        written_invalid_rows = COALESCE(EXCLUDED.written_invalid_rows, runtime.stream_batch_metric.written_invalid_rows),
        write_ok = EXCLUDED.write_ok,
        message = COALESCE(EXCLUDED.message, runtime.stream_batch_metric.message),
        error_message =
            CASE
                WHEN EXCLUDED.write_ok IS TRUE THEN NULL
                WHEN EXCLUDED.error_message IS NOT NULL THEN EXCLUDED.error_message
                ELSE runtime.stream_batch_metric.error_message
            END,
        target_path = COALESCE(EXCLUDED.target_path, runtime.stream_batch_metric.target_path),
        checkpoint_path = COALESCE(EXCLUDED.checkpoint_path, runtime.stream_batch_metric.checkpoint_path),
        batch_ts = COALESCE(EXCLUDED.batch_ts, runtime.stream_batch_metric.batch_ts),
        payload = EXCLUDED.payload,
        updated_at = now();
END;
$$;


DROP FUNCTION IF EXISTS ctl.usp_get_stream_unit_current(TEXT);

CREATE OR REPLACE FUNCTION ctl.usp_get_stream_unit_current(
    p_unit_name TEXT
)
RETURNS TABLE (
    unit_name TEXT,
    stream_name TEXT,
    layer TEXT,
    run_id TEXT,
    computed_status TEXT,
    status_reason TEXT,
    pid INTEGER,
    returncode INTEGER,
    retries INTEGER,
    max_retries INTEGER,
    heartbeat_status TEXT,
    heartbeat_ts TIMESTAMPTZ,
    heartbeat_age_seconds INTEGER,
    last_batch_id BIGINT,
    last_input_rows BIGINT,
    last_batch_rows BIGINT,
    last_valid_rows BIGINT,
    last_invalid_rows BIGINT,
    last_written_rows BIGINT,
    last_written_valid_rows BIGINT,
    last_written_invalid_rows BIGINT,
    last_write_ok BOOLEAN,
    last_message TEXT,
    last_error TEXT,
    target_path TEXT,
    checkpoint_path TEXT,
    updated_at TIMESTAMPTZ
)
LANGUAGE sql
AS $$
    SELECT
        c.unit_name,
        c.stream_name,
        c.layer,
        c.run_id,
        c.computed_status,
        c.status_reason,
        c.pid,
        c.returncode,
        c.retries,
        c.max_retries,
        c.heartbeat_status,
        c.heartbeat_ts,
        COALESCE(
            EXTRACT(EPOCH FROM (now() - c.heartbeat_ts))::INTEGER,
            c.heartbeat_age_seconds
        ) AS heartbeat_age_seconds,
        c.last_batch_id,
        c.last_input_rows,
        c.last_batch_rows,
        c.last_valid_rows,
        c.last_invalid_rows,
        c.last_written_rows,
        c.last_written_valid_rows,
        c.last_written_invalid_rows,
        c.last_write_ok,
        c.last_message,
        c.last_error,
        c.target_path,
        c.checkpoint_path,
        c.updated_at
    FROM runtime.stream_unit_current c
    WHERE c.unit_name = p_unit_name;
$$;


DROP FUNCTION IF EXISTS ctl.usp_list_stream_batch_metrics(TEXT, INTEGER);

CREATE OR REPLACE FUNCTION ctl.usp_list_stream_batch_metrics(
    p_unit_name TEXT DEFAULT NULL,
    p_limit INTEGER DEFAULT 50
)
RETURNS TABLE (
    unit_name TEXT,
    stream_name TEXT,
    layer TEXT,
    run_id TEXT,
    batch_id BIGINT,
    input_rows BIGINT,
    batch_rows BIGINT,
    valid_rows BIGINT,
    invalid_rows BIGINT,
    written_rows BIGINT,
    written_valid_rows BIGINT,
    written_invalid_rows BIGINT,
    write_ok BOOLEAN,
    message TEXT,
    error_message TEXT,
    target_path TEXT,
    checkpoint_path TEXT,
    batch_ts TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
)
LANGUAGE sql
AS $$
    SELECT
        m.unit_name,
        m.stream_name,
        m.layer,
        m.run_id,
        m.batch_id,
        m.input_rows,
        m.batch_rows,
        m.valid_rows,
        m.invalid_rows,
        m.written_rows,
        m.written_valid_rows,
        m.written_invalid_rows,
        m.write_ok,
        m.message,
        m.error_message,
        m.target_path,
        m.checkpoint_path,
        m.batch_ts,
        m.updated_at
    FROM runtime.stream_batch_metric m
    WHERE p_unit_name IS NULL OR m.unit_name = p_unit_name
    ORDER BY m.batch_ts DESC, m.batch_id DESC
    LIMIT COALESCE(p_limit, 50);
$$;
