-- -----------------------------------------------------------------------------
-- 019_ctl_lineage_usps.sql
-- Purpose:
--   Stored procedure for writing dataset lineage events.
--
-- Table:
--   lineage.dataset_lineage
--
-- Python caller:
--   shared.control.lineage_store.write_dataset_lineage(...)
-- -----------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS ctl.usp_insert_dataset_lineage(
    TEXT,
    BIGINT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT
);

CREATE OR REPLACE PROCEDURE ctl.usp_insert_dataset_lineage(
    p_run_id TEXT,
    p_job_id BIGINT,
    p_job_key TEXT,
    p_source_dataset_key TEXT,
    p_target_dataset_key TEXT,
    p_transformation_type TEXT,
    p_transformation_ref TEXT,
    p_details TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_details JSONB;
BEGIN
    v_details := COALESCE(NULLIF(p_details, '')::jsonb, '{}'::jsonb);

    INSERT INTO lineage.dataset_lineage (
        run_id,
        job_id,
        job_key,
        source_dataset_key,
        target_dataset_key,
        transformation_type,
        transformation_ref,
        details,
        created_at
    )
    VALUES (
        p_run_id,
        p_job_id,
        p_job_key,
        p_source_dataset_key,
        p_target_dataset_key,
        p_transformation_type,
        p_transformation_ref,
        v_details,
        now()
    );
END;
$$;
