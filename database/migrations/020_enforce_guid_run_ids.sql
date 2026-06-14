-- -----------------------------------------------------------------------------
-- 020_enforce_guid_run_ids.sql
-- Purpose:
--   Enforce GUID-only runtime run identifiers on existing databases.
--
-- Notes:
--   - Columns stay TEXT for compatibility with existing procedure signatures.
--   - CHECK constraints use NOT VALID so historical non-GUID rows do not block
--     deployment, while all new/updated rows must satisfy the contract.
-- -----------------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ck_job_run_run_id_guid'
          AND conrelid = 'runtime.job_run'::regclass
    ) THEN
        ALTER TABLE runtime.job_run
            ADD CONSTRAINT ck_job_run_run_id_guid
            CHECK (
                run_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            )
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ck_job_event_run_id_guid'
          AND conrelid = 'runtime.job_event'::regclass
    ) THEN
        ALTER TABLE runtime.job_event
            ADD CONSTRAINT ck_job_event_run_id_guid
            CHECK (
                run_id IS NULL
                OR run_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            )
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ck_watermark_state_last_run_id_guid'
          AND conrelid = 'runtime.watermark_state'::regclass
    ) THEN
        ALTER TABLE runtime.watermark_state
            ADD CONSTRAINT ck_watermark_state_last_run_id_guid
            CHECK (
                last_run_id IS NULL
                OR last_run_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            )
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ck_quality_result_run_id_guid'
          AND conrelid = 'dq.quality_result'::regclass
    ) THEN
        ALTER TABLE dq.quality_result
            ADD CONSTRAINT ck_quality_result_run_id_guid
            CHECK (
                run_id IS NULL
                OR run_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            )
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ck_dataset_lineage_run_id_guid'
          AND conrelid = 'lineage.dataset_lineage'::regclass
    ) THEN
        ALTER TABLE lineage.dataset_lineage
            ADD CONSTRAINT ck_dataset_lineage_run_id_guid
            CHECK (
                run_id IS NULL
                OR run_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            )
            NOT VALID;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'runtime'
          AND table_name = 'stream_unit_current'
          AND column_name = 'run_id'
    )
       AND NOT EXISTS (
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

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'runtime'
          AND table_name = 'stream_batch_metric'
          AND column_name = 'run_id'
    )
       AND NOT EXISTS (
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
