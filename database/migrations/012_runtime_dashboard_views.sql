CREATE OR REPLACE VIEW runtime.v_stream_unit_current AS
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
    ) AS computed_heartbeat_age_seconds,
    CASE
        WHEN c.heartbeat_ts IS NULL THEN NULL
        WHEN now() - c.heartbeat_ts > INTERVAL '120 seconds' THEN TRUE
        ELSE FALSE
    END AS heartbeat_is_stale,
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
    CASE
        WHEN c.last_write_ok IS FALSE THEN 'write_failed'
        WHEN c.heartbeat_ts IS NOT NULL
             AND now() - c.heartbeat_ts > INTERVAL '120 seconds' THEN 'stale'
        ELSE COALESCE(c.computed_status, 'unknown')
    END AS dashboard_status,
    c.target_path,
    c.checkpoint_path,
    c.updated_at
FROM runtime.stream_unit_current c;


CREATE OR REPLACE VIEW runtime.v_stream_dashboard_summary AS
SELECT
    u.unit_name,
    u.stream_name,
    u.layer,
    u.run_id,
    u.dashboard_status,
    u.computed_status,
    u.status_reason,
    u.pid,
    u.returncode,
    u.retries,
    u.max_retries,
    u.heartbeat_status,
    u.heartbeat_ts,
    u.computed_heartbeat_age_seconds AS heartbeat_age_seconds,
    u.heartbeat_is_stale,
    u.last_batch_id,
    u.last_input_rows,
    u.last_batch_rows,
    u.last_valid_rows,
    u.last_invalid_rows,
    u.last_written_rows,
    u.last_written_valid_rows,
    u.last_written_invalid_rows,
    u.last_write_ok,
    u.last_message,
    u.last_error,
    latest.batch_id AS latest_metric_batch_id,
    latest.run_id AS latest_metric_run_id,
    latest.input_rows AS latest_metric_input_rows,
    latest.batch_rows AS latest_metric_batch_rows,
    latest.valid_rows AS latest_metric_valid_rows,
    latest.invalid_rows AS latest_metric_invalid_rows,
    latest.written_rows AS latest_metric_written_rows,
    latest.written_valid_rows AS latest_metric_written_valid_rows,
    latest.written_invalid_rows AS latest_metric_written_invalid_rows,
    latest.write_ok AS latest_metric_write_ok,
    latest.message AS latest_metric_message,
    latest.error_message AS latest_metric_error_message,
    latest.batch_ts AS latest_metric_batch_ts,
    u.target_path,
    u.checkpoint_path,
    u.updated_at
FROM runtime.v_stream_unit_current u
LEFT JOIN LATERAL (
    SELECT
        m.batch_id,
        m.run_id,
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
        m.batch_ts
    FROM runtime.stream_batch_metric m
    WHERE m.unit_name = u.unit_name
    ORDER BY m.batch_id DESC, m.batch_ts DESC
    LIMIT 1
) latest ON TRUE;


CREATE OR REPLACE VIEW runtime.v_job_run_latest AS
SELECT DISTINCT ON (r.job_code)
    r.*
FROM runtime.job_run r
ORDER BY r.job_code, COALESCE(r.started_at, r.created_at) DESC;


CREATE OR REPLACE VIEW runtime.v_job_run_dashboard AS
SELECT
    j.job_id,
    j.job_code,
    j.job_key,
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
    j.target_path AS defined_target_path,
    r.run_id,
    r.status AS latest_status,
    r.status_reason,
    r.error_message,
    r.started_at,
    r.ended_at,
    r.duration_seconds,
    r.records_read,
    r.records_written,
    r.target_path AS latest_target_path,
    r.created_at AS latest_run_created_at
FROM meta.job j
LEFT JOIN runtime.v_job_run_latest r
    ON r.job_code = j.job_code
WHERE j.is_active IS TRUE
  AND j.active_flag IS TRUE;
