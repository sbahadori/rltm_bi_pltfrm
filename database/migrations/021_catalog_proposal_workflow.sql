-- =============================================================
-- 021_catalog_proposal_workflow.sql
-- Draft/proposal queue for dashboard catalog edits.
--
-- Architecture decision:
--   UI Job Builder is a proposal layer only. It stores proposed catalog
--   changes for approval in meta.catalog_proposal. Approved proposals are
--   published to configs/batch/pipeline_catalog.json, then materialized by
--   onboarding into meta.pipeline/meta.job.
--
--   meta.catalog_proposal is not an executable registry and is not runtime
--   state. Runners must never read from it.
-- =============================================================

CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS ctl;

CREATE TABLE IF NOT EXISTS meta.catalog_proposal (
    proposal_id BIGSERIAL PRIMARY KEY,
    proposal_type TEXT NOT NULL
        CONSTRAINT ck_catalog_proposal_type
        CHECK (proposal_type IN ('job', 'pipeline')),
    proposal_state TEXT NOT NULL DEFAULT 'pending_approval'
        CONSTRAINT ck_catalog_proposal_state
        CHECK (proposal_state IN ('pending_approval', 'approved', 'rejected', 'stale', 'failed')),
    publish_state TEXT NOT NULL DEFAULT 'proposal_only',
    materialization_state TEXT NOT NULL DEFAULT 'not_requested',
    action_type TEXT NOT NULL,
    pipeline_name TEXT NOT NULL,
    job_name TEXT,
    requested_by TEXT,
    reviewed_by TEXT,
    reviewer_note TEXT,
    request_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    proposed_catalog JSONB NOT NULL DEFAULT '{}'::jsonb,
    validation_result JSONB NOT NULL DEFAULT '{}'::jsonb,
    base_catalog_hash TEXT,
    proposal_hash TEXT,
    backup_path TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    reviewed_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ
);

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS proposal_type TEXT;

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS proposal_state TEXT NOT NULL DEFAULT 'pending_approval';

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS publish_state TEXT NOT NULL DEFAULT 'proposal_only';

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS materialization_state TEXT NOT NULL DEFAULT 'not_requested';

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS action_type TEXT;

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS pipeline_name TEXT;

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS job_name TEXT;

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS requested_by TEXT;

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS reviewed_by TEXT;

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS reviewer_note TEXT;

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS request_payload JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS proposed_catalog JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS validation_result JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS base_catalog_hash TEXT;

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS proposal_hash TEXT;

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS backup_path TEXT;

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS error_message TEXT;

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ;

ALTER TABLE meta.catalog_proposal
    ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ck_catalog_proposal_type'
          AND conrelid = 'meta.catalog_proposal'::regclass
    ) THEN
        ALTER TABLE meta.catalog_proposal
            ADD CONSTRAINT ck_catalog_proposal_type
            CHECK (proposal_type IN ('job', 'pipeline'))
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ck_catalog_proposal_state'
          AND conrelid = 'meta.catalog_proposal'::regclass
    ) THEN
        ALTER TABLE meta.catalog_proposal
            ADD CONSTRAINT ck_catalog_proposal_state
            CHECK (proposal_state IN ('pending_approval', 'approved', 'rejected', 'stale', 'failed'))
            NOT VALID;
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS ix_catalog_proposal_state_created
    ON meta.catalog_proposal (proposal_state, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_catalog_proposal_pipeline_job
    ON meta.catalog_proposal (pipeline_name, job_name, created_at DESC);


CREATE OR REPLACE FUNCTION ctl.usp_create_catalog_proposal(
    p_requested_by TEXT,
    p_proposal_type TEXT,
    p_pipeline_name TEXT,
    p_job_name TEXT,
    p_action_type TEXT,
    p_request_payload TEXT,
    p_proposed_catalog TEXT,
    p_validation_result TEXT,
    p_base_catalog_hash TEXT,
    p_proposal_hash TEXT,
    p_materialization_state TEXT DEFAULT 'not_requested'
)
RETURNS TABLE (
    proposal_id BIGINT,
    proposal_type TEXT,
    proposal_state TEXT,
    publish_state TEXT,
    materialization_state TEXT,
    action_type TEXT,
    pipeline_name TEXT,
    job_name TEXT,
    requested_by TEXT,
    reviewed_by TEXT,
    reviewer_note TEXT,
    request_payload JSONB,
    proposed_catalog JSONB,
    validation_result JSONB,
    base_catalog_hash TEXT,
    proposal_hash TEXT,
    backup_path TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    reviewed_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_request_payload JSONB;
    v_proposed_catalog JSONB;
    v_validation_result JSONB;
BEGIN
    v_request_payload := COALESCE(NULLIF(p_request_payload, '')::jsonb, '{}'::jsonb);
    v_proposed_catalog := COALESCE(NULLIF(p_proposed_catalog, '')::jsonb, '{}'::jsonb);
    v_validation_result := COALESCE(NULLIF(p_validation_result, '')::jsonb, '{}'::jsonb);

    IF p_proposal_type NOT IN ('job', 'pipeline') THEN
        RAISE EXCEPTION 'Invalid proposal type: %', p_proposal_type;
    END IF;

    IF p_pipeline_name IS NULL OR trim(p_pipeline_name) = '' THEN
        RAISE EXCEPTION 'p_pipeline_name is required';
    END IF;

    RETURN QUERY
    INSERT INTO meta.catalog_proposal (
        requested_by,
        proposal_type,
        proposal_state,
        publish_state,
        materialization_state,
        action_type,
        pipeline_name,
        job_name,
        request_payload,
        proposed_catalog,
        validation_result,
        base_catalog_hash,
        proposal_hash,
        created_at,
        updated_at
    )
    VALUES (
        p_requested_by,
        p_proposal_type,
        'pending_approval',
        'proposal_only',
        COALESCE(NULLIF(p_materialization_state, ''), 'not_requested'),
        p_action_type,
        p_pipeline_name,
        p_job_name,
        v_request_payload,
        v_proposed_catalog,
        v_validation_result,
        p_base_catalog_hash,
        p_proposal_hash,
        now(),
        now()
    )
    RETURNING
        meta.catalog_proposal.proposal_id,
        meta.catalog_proposal.proposal_type,
        meta.catalog_proposal.proposal_state,
        meta.catalog_proposal.publish_state,
        meta.catalog_proposal.materialization_state,
        meta.catalog_proposal.action_type,
        meta.catalog_proposal.pipeline_name,
        meta.catalog_proposal.job_name,
        meta.catalog_proposal.requested_by,
        meta.catalog_proposal.reviewed_by,
        meta.catalog_proposal.reviewer_note,
        meta.catalog_proposal.request_payload,
        meta.catalog_proposal.proposed_catalog,
        meta.catalog_proposal.validation_result,
        meta.catalog_proposal.base_catalog_hash,
        meta.catalog_proposal.proposal_hash,
        meta.catalog_proposal.backup_path,
        meta.catalog_proposal.error_message,
        meta.catalog_proposal.created_at,
        meta.catalog_proposal.updated_at,
        meta.catalog_proposal.reviewed_at,
        meta.catalog_proposal.published_at;
END;
$$;


CREATE OR REPLACE FUNCTION ctl.usp_get_catalog_proposal(
    p_proposal_id BIGINT
)
RETURNS SETOF meta.catalog_proposal
LANGUAGE sql
AS $$
    SELECT *
    FROM meta.catalog_proposal
    WHERE proposal_id = p_proposal_id;
$$;


CREATE OR REPLACE FUNCTION ctl.usp_list_catalog_proposals(
    p_proposal_state TEXT DEFAULT 'pending_approval',
    p_limit INTEGER DEFAULT 100
)
RETURNS SETOF meta.catalog_proposal
LANGUAGE sql
AS $$
    SELECT *
    FROM meta.catalog_proposal
    WHERE p_proposal_state IS NULL
       OR p_proposal_state = ''
       OR proposal_state = p_proposal_state
    ORDER BY created_at DESC
    LIMIT COALESCE(p_limit, 100);
$$;


CREATE OR REPLACE FUNCTION ctl.usp_set_catalog_proposal_state(
    p_proposal_id BIGINT,
    p_proposal_state TEXT,
    p_reviewed_by TEXT,
    p_reviewer_note TEXT,
    p_publish_state TEXT,
    p_materialization_state TEXT,
    p_backup_path TEXT,
    p_error_message TEXT
)
RETURNS SETOF meta.catalog_proposal
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_proposal_state NOT IN ('pending_approval', 'approved', 'rejected', 'stale', 'failed') THEN
        RAISE EXCEPTION 'Invalid proposal state: %', p_proposal_state;
    END IF;

    RETURN QUERY
    UPDATE meta.catalog_proposal
    SET
        proposal_state = p_proposal_state,
        reviewed_by = COALESCE(NULLIF(p_reviewed_by, ''), reviewed_by),
        reviewer_note = COALESCE(p_reviewer_note, reviewer_note),
        publish_state = COALESCE(NULLIF(p_publish_state, ''), publish_state),
        materialization_state = COALESCE(NULLIF(p_materialization_state, ''), materialization_state),
        backup_path = COALESCE(NULLIF(p_backup_path, ''), backup_path),
        error_message = p_error_message,
        reviewed_at = CASE
            WHEN p_proposal_state IN ('approved', 'rejected', 'stale', 'failed') THEN now()
            ELSE reviewed_at
        END,
        published_at = CASE
            WHEN COALESCE(NULLIF(p_publish_state, ''), publish_state) = 'catalog_published' THEN now()
            ELSE published_at
        END,
        updated_at = now()
    WHERE proposal_id = p_proposal_id
    RETURNING meta.catalog_proposal.*;
END;
$$;
