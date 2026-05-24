-- -----------------------------------------------------------------------------
-- 008_ctl_quality_usps.sql
-- Purpose:
--   Stored procedure for writing data-quality results.
--
-- Table:
--   dq.quality_result
--
-- Python caller:
--   shared.runtime.quality_store.write_quality_result(...)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE ctl.usp_insert_quality_result(
    p_run_id TEXT,
    p_job_id BIGINT,
    p_job_key TEXT,
    p_dataset_key TEXT,
    p_status TEXT,
    p_observed_value TEXT,
    p_expected_value TEXT,
    p_details TEXT,
    p_rule_id BIGINT DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_details JSONB;
BEGIN
    v_details := COALESCE(NULLIF(p_details, '')::jsonb, '{}'::jsonb);

    INSERT INTO dq.quality_result (
        rule_id,
        run_id,
        job_id,
        job_key,
        dataset_key,
        status,
        observed_value,
        expected_value,
        details,
        created_at
    )
    VALUES (
        p_rule_id,
        p_run_id,
        p_job_id,
        p_job_key,
        p_dataset_key,
        p_status,
        p_observed_value,
        p_expected_value,
        v_details,
        now()
    );
END;
$$;