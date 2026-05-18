CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- فقط برای محیط dev. اگر داده مهم داری، قبلش backup بگیر.
TRUNCATE TABLE lineage.dataset_lineage RESTART IDENTITY CASCADE;
TRUNCATE TABLE dq.quality_result RESTART IDENTITY CASCADE;
TRUNCATE TABLE runtime.watermark_state RESTART IDENTITY CASCADE;
TRUNCATE TABLE runtime.job_event RESTART IDENTITY CASCADE;
TRUNCATE TABLE runtime.job_run RESTART IDENTITY CASCADE;

ALTER TABLE runtime.job_event
DROP CONSTRAINT IF EXISTS job_event_run_id_fkey;

ALTER TABLE runtime.watermark_state
DROP CONSTRAINT IF EXISTS watermark_state_last_run_id_fkey;

ALTER TABLE dq.quality_result
DROP CONSTRAINT IF EXISTS quality_result_run_id_fkey;

ALTER TABLE lineage.dataset_lineage
DROP CONSTRAINT IF EXISTS dataset_lineage_run_id_fkey;

ALTER TABLE runtime.job_run
DROP CONSTRAINT IF EXISTS job_run_pkey;

ALTER TABLE runtime.job_run
DROP CONSTRAINT IF EXISTS job_run_job_key_fkey;

ALTER TABLE runtime.job_run
DROP COLUMN IF EXISTS run_id;

ALTER TABLE runtime.job_run
DROP COLUMN IF EXISTS job_key;

ALTER TABLE runtime.job_run
ADD COLUMN run_id UUID PRIMARY KEY DEFAULT gen_random_uuid();

ALTER TABLE runtime.job_run
ADD COLUMN job_id BIGINT REFERENCES meta.job(job_id);

ALTER TABLE runtime.job_run
ADD COLUMN job_key TEXT;

ALTER TABLE runtime.job_run
ADD COLUMN effective_start_date DATE;

ALTER TABLE runtime.job_run
ADD COLUMN effective_end_date DATE;

CREATE INDEX IF NOT EXISTS idx_job_run_job_id_created_at
ON runtime.job_run(job_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_job_run_effective_dates
ON runtime.job_run(effective_start_date, effective_end_date);

ALTER TABLE runtime.job_event
ALTER COLUMN run_id TYPE UUID USING NULL;

ALTER TABLE runtime.job_event
ADD COLUMN IF NOT EXISTS job_id BIGINT REFERENCES meta.job(job_id);

ALTER TABLE runtime.job_event
ADD CONSTRAINT job_event_run_id_fkey
FOREIGN KEY (run_id) REFERENCES runtime.job_run(run_id);

ALTER TABLE runtime.watermark_state
ALTER COLUMN last_run_id TYPE UUID USING NULL;

ALTER TABLE runtime.watermark_state
ADD CONSTRAINT watermark_state_last_run_id_fkey
FOREIGN KEY (last_run_id) REFERENCES runtime.job_run(run_id);

ALTER TABLE dq.quality_result
ALTER COLUMN run_id TYPE UUID USING NULL;

ALTER TABLE dq.quality_result
ADD COLUMN IF NOT EXISTS job_id BIGINT REFERENCES meta.job(job_id);

ALTER TABLE dq.quality_result
ADD CONSTRAINT quality_result_run_id_fkey
FOREIGN KEY (run_id) REFERENCES runtime.job_run(run_id);

ALTER TABLE lineage.dataset_lineage
ALTER COLUMN run_id TYPE UUID USING NULL;

ALTER TABLE lineage.dataset_lineage
ADD COLUMN IF NOT EXISTS job_id BIGINT REFERENCES meta.job(job_id);

ALTER TABLE lineage.dataset_lineage
ADD CONSTRAINT dataset_lineage_run_id_fkey
FOREIGN KEY (run_id) REFERENCES runtime.job_run(run_id);

-- for execution: 
-- Get-Content .\metadata\migrations\002_refactor_job_run_identity.sql | docker compose --project-directory . -f compose/compose.phase1.yaml exec -T postgres-warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'