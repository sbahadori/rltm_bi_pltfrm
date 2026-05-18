-- metadata/migrations/003_add_job_key_to_meta_job.sql

ALTER TABLE meta.job
ADD COLUMN IF NOT EXISTS job_key TEXT;

ALTER TABLE meta.job
ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;


UPDATE meta.job
SET job_key = pipeline_name || '::' || job_name
WHERE job_key IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_meta_job_job_key
ON meta.job(job_key);
CREATE INDEX IF NOT EXISTS ix_meta_job_is_active
ON meta.job(is_active);


-- Get-Content .\metadata\migrations\003_add_job_key_to_meta_job.sql | docker compose --project-directory . -f compose/compose.phase1.yaml exec -T postgres-warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'