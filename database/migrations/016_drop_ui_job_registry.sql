-- =============================================================
-- 016_drop_ui_job_registry.sql
--
-- Non-destructive archive of the old UI dynamic job registry.
--
-- Architecture decision:
--   Executable jobs are no longer defined through UI registry tables.
--
--   Source of truth:
--     configs/batch/pipeline_catalog.json
--       -> onboarding
--       -> meta.pipeline / meta.job
--
--   Runtime source of truth:
--     runtime/control DB tables and ctl runtime views/functions.
--
--   The old UI registry is archived, not dropped, to preserve any
--   development/debug data that may exist locally.
-- =============================================================

CREATE SCHEMA IF NOT EXISTS archive;

-- Move legacy UI registry tables to archive schema.
DO $$
BEGIN
    IF to_regclass('meta.ui_job_dependency') IS NOT NULL
       AND to_regclass('archive.deprecated_ui_job_dependency') IS NULL THEN
        ALTER TABLE meta.ui_job_dependency SET SCHEMA archive;
        ALTER TABLE archive.ui_job_dependency RENAME TO deprecated_ui_job_dependency;
    END IF;

    IF to_regclass('meta.ui_job_definition') IS NOT NULL
       AND to_regclass('archive.deprecated_ui_job_definition') IS NULL THEN
        ALTER TABLE meta.ui_job_definition SET SCHEMA archive;
        ALTER TABLE archive.ui_job_definition RENAME TO deprecated_ui_job_definition;
    END IF;

    IF to_regclass('meta.ui_pipeline_definition') IS NOT NULL
       AND to_regclass('archive.deprecated_ui_pipeline_definition') IS NULL THEN
        ALTER TABLE meta.ui_pipeline_definition SET SCHEMA archive;
        ALTER TABLE archive.ui_pipeline_definition RENAME TO deprecated_ui_pipeline_definition;
    END IF;

    IF to_regclass('meta.ui_job_template') IS NOT NULL
       AND to_regclass('archive.deprecated_ui_job_template') IS NULL THEN
        ALTER TABLE meta.ui_job_template SET SCHEMA archive;
        ALTER TABLE archive.ui_job_template RENAME TO deprecated_ui_job_template;
    END IF;
END $$;

-- Drop old UI-registry procedures/functions.
-- These are no longer part of the active control-plane contract.

DROP PROCEDURE IF EXISTS ctl.usp_upsert_ui_pipeline(
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    BOOLEAN,
    TEXT,
    TEXT,
    TEXT
);

DROP PROCEDURE IF EXISTS ctl.usp_upsert_ui_job(
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    TEXT,
    BOOLEAN,
    TEXT,
    TEXT,
    TEXT,
    TEXT
);

DROP PROCEDURE IF EXISTS ctl.usp_delete_ui_job(TEXT);

DROP PROCEDURE IF EXISTS ctl.usp_delete_ui_pipeline(TEXT);

DROP FUNCTION IF EXISTS ctl.usp_list_ui_pipelines(BOOLEAN);

DROP FUNCTION IF EXISTS ctl.usp_list_ui_jobs(TEXT, BOOLEAN);

DROP FUNCTION IF EXISTS ctl.usp_get_ui_job(TEXT);

DROP FUNCTION IF EXISTS ctl.usp_list_ui_job_templates();
