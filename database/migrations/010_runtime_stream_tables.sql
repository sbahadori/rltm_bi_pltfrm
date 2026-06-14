CREATE TABLE IF NOT EXISTS runtime.stream_unit_current (
    unit_name TEXT PRIMARY KEY,
    stream_name TEXT NOT NULL,
    layer TEXT NOT NULL,
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

    payload JSONB NOT NULL DEFAULT '{}'::jsonb,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ck_stream_unit_current_run_id_guid CHECK (
        run_id IS NULL
        OR run_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    )
);

ALTER TABLE runtime.stream_unit_current
    ADD COLUMN IF NOT EXISTS run_id TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ck_stream_unit_current_run_id_guid'
          AND conrelid = 'runtime.stream_unit_current'::regclass
    ) THEN
        ALTER TABLE runtime.stream_unit_current
            ADD CONSTRAINT ck_stream_unit_current_run_id_guid
            CHECK (
                run_id IS NULL
                OR run_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            )
            NOT VALID;
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS ix_stream_unit_current_stream_name
    ON runtime.stream_unit_current (stream_name);

CREATE INDEX IF NOT EXISTS ix_stream_unit_current_layer
    ON runtime.stream_unit_current (layer);

CREATE INDEX IF NOT EXISTS ix_stream_unit_current_run_id
    ON runtime.stream_unit_current (run_id);

CREATE INDEX IF NOT EXISTS ix_stream_unit_current_status
    ON runtime.stream_unit_current (computed_status);

CREATE INDEX IF NOT EXISTS ix_stream_unit_current_updated_at
    ON runtime.stream_unit_current (updated_at DESC);


CREATE TABLE IF NOT EXISTS runtime.stream_batch_metric (
    id BIGSERIAL PRIMARY KEY,

    unit_name TEXT NOT NULL,
    stream_name TEXT NOT NULL,
    layer TEXT NOT NULL,
    run_id TEXT,

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

    CONSTRAINT uq_stream_batch_metric_unit_run_batch UNIQUE (unit_name, run_id, batch_id),
    CONSTRAINT ck_stream_batch_metric_run_id_guid CHECK (
        run_id IS NULL
        OR run_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    )
);

ALTER TABLE runtime.stream_batch_metric
    ADD COLUMN IF NOT EXISTS run_id TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ck_stream_batch_metric_run_id_guid'
          AND conrelid = 'runtime.stream_batch_metric'::regclass
    ) THEN
        ALTER TABLE runtime.stream_batch_metric
            ADD CONSTRAINT ck_stream_batch_metric_run_id_guid
            CHECK (
                run_id IS NULL
                OR run_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            )
            NOT VALID;
    END IF;
END;
$$;

ALTER TABLE runtime.stream_batch_metric
    DROP CONSTRAINT IF EXISTS uq_stream_batch_metric_unit_batch;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_stream_batch_metric_unit_run_batch'
          AND conrelid = 'runtime.stream_batch_metric'::regclass
    ) THEN
        ALTER TABLE runtime.stream_batch_metric
            ADD CONSTRAINT uq_stream_batch_metric_unit_run_batch
            UNIQUE (unit_name, run_id, batch_id);
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS ix_stream_batch_metric_unit_batch_desc
    ON runtime.stream_batch_metric (unit_name, batch_id DESC);

CREATE INDEX IF NOT EXISTS ix_stream_batch_metric_stream_layer
    ON runtime.stream_batch_metric (stream_name, layer);

CREATE INDEX IF NOT EXISTS ix_stream_batch_metric_run_id
    ON runtime.stream_batch_metric (run_id);

CREATE INDEX IF NOT EXISTS ix_stream_batch_metric_write_ok
    ON runtime.stream_batch_metric (write_ok);

CREATE INDEX IF NOT EXISTS ix_stream_batch_metric_batch_ts
    ON runtime.stream_batch_metric (batch_ts DESC);
