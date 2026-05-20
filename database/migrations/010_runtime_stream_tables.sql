CREATE TABLE IF NOT EXISTS runtime.stream_unit_current (
    unit_name TEXT PRIMARY KEY,
    stream_name TEXT NOT NULL,
    layer TEXT NOT NULL,

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

    payload JSONB NOT NULL DEFAULT '{}'::jsonb,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_stream_unit_current_stream_name
    ON runtime.stream_unit_current (stream_name);

CREATE INDEX IF NOT EXISTS ix_stream_unit_current_layer
    ON runtime.stream_unit_current (layer);

CREATE INDEX IF NOT EXISTS ix_stream_unit_current_status
    ON runtime.stream_unit_current (computed_status);

CREATE INDEX IF NOT EXISTS ix_stream_unit_current_updated_at
    ON runtime.stream_unit_current (updated_at DESC);


CREATE TABLE IF NOT EXISTS runtime.stream_batch_metric (
    id BIGSERIAL PRIMARY KEY,

    unit_name TEXT NOT NULL,
    stream_name TEXT NOT NULL,
    layer TEXT NOT NULL,

    batch_id BIGINT NOT NULL,

    input_rows BIGINT,
    batch_rows BIGINT,

    valid_rows BIGINT,
    invalid_rows BIGINT,

    written_rows BIGINT,
    written_valid_rows BIGINT,
    written_invalid_rows BIGINT,

    write_ok BOOLEAN NOT NULL,
    message TEXT,
    error_message TEXT,

    target_path TEXT,
    checkpoint_path TEXT,

    batch_ts TIMESTAMPTZ NOT NULL DEFAULT now(),

    payload JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_stream_batch_metric_unit_batch UNIQUE (unit_name, batch_id)
);

CREATE INDEX IF NOT EXISTS ix_stream_batch_metric_unit_batch_desc
    ON runtime.stream_batch_metric (unit_name, batch_id DESC);

CREATE INDEX IF NOT EXISTS ix_stream_batch_metric_stream_layer
    ON runtime.stream_batch_metric (stream_name, layer);

CREATE INDEX IF NOT EXISTS ix_stream_batch_metric_write_ok
    ON runtime.stream_batch_metric (write_ok);

CREATE INDEX IF NOT EXISTS ix_stream_batch_metric_batch_ts
    ON runtime.stream_batch_metric (batch_ts DESC);