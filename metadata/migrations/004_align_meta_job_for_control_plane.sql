CREATE SCHEMA IF NOT EXISTS meta;

ALTER TABLE meta.job
ADD COLUMN IF NOT EXISTS job_key TEXT;

ALTER TABLE meta.job
ADD COLUMN IF NOT EXISTS job_code TEXT;

ALTER TABLE meta.job
ADD COLUMN IF NOT EXISTS base_job_name TEXT;

ALTER TABLE meta.job
ADD COLUMN IF NOT EXISTS source_type TEXT;

ALTER TABLE meta.job
ADD COLUMN IF NOT EXISTS runner TEXT;

ALTER TABLE meta.job
ADD COLUMN IF NOT EXISTS layer TEXT;

ALTER TABLE meta.job
ADD COLUMN IF NOT EXISTS source_id TEXT;

ALTER TABLE meta.job
ADD COLUMN IF NOT EXISTS table_id TEXT;

ALTER TABLE meta.job
ADD COLUMN IF NOT EXISTS entity_name TEXT;

ALTER TABLE meta.job
ADD COLUMN IF NOT EXISTS manifest_ref TEXT;

ALTER TABLE meta.job
ADD COLUMN IF NOT EXISTS target_path TEXT;

ALTER TABLE meta.job
ADD COLUMN IF NOT EXISTS config JSONB DEFAULT '{}'::jsonb;

ALTER TABLE meta.job
ADD COLUMN IF NOT EXISTS runtime_policy JSONB DEFAULT '{}'::jsonb;

ALTER TABLE meta.job
ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;

UPDATE meta.job
SET job_key = COALESCE(job_key, pipeline_name || '::' || job_name);

UPDATE meta.job
SET job_code = COALESCE(job_code, job_key);

CREATE UNIQUE INDEX IF NOT EXISTS ux_meta_job_job_key
ON meta.job(job_key);

CREATE UNIQUE INDEX IF NOT EXISTS ux_meta_job_job_code
ON meta.job(job_code);

CREATE INDEX IF NOT EXISTS ix_meta_job_pipeline_active
ON meta.job(pipeline_name, is_active);

CREATE INDEX IF NOT EXISTS ix_meta_job_source_table
ON meta.job(source_id, table_id);


-- Get-Content .\metadata\migrations\004_align_meta_job_for_control_plane.sql | docker compose --project-directory . -f compose/compose.phase1.yaml exec -T postgres-warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
