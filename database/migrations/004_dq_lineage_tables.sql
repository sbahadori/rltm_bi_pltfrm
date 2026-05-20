CREATE TABLE IF NOT EXISTS dq.quality_rule (
    rule_id BIGSERIAL PRIMARY KEY,

    job_id BIGINT REFERENCES meta.job(job_id),
    job_key TEXT,

    dataset_key TEXT,

    rule_name TEXT NOT NULL,
    rule_type TEXT NOT NULL,

    rule_config JSONB NOT NULL DEFAULT '{}'::jsonb,

    severity TEXT NOT NULL DEFAULT 'medium',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_quality_rule_job_id
    ON dq.quality_rule (job_id);

CREATE INDEX IF NOT EXISTS ix_quality_rule_job_key
    ON dq.quality_rule (job_key);

CREATE INDEX IF NOT EXISTS ix_quality_rule_dataset_key
    ON dq.quality_rule (dataset_key);

CREATE INDEX IF NOT EXISTS ix_quality_rule_active
    ON dq.quality_rule (is_active);


CREATE TABLE IF NOT EXISTS dq.quality_result (
    result_id BIGSERIAL PRIMARY KEY,

    rule_id BIGINT REFERENCES dq.quality_rule(rule_id),
    run_id TEXT,
    job_id BIGINT REFERENCES meta.job(job_id),
    job_key TEXT,

    dataset_key TEXT,

    status TEXT NOT NULL,
    observed_value TEXT,
    expected_value TEXT,

    details JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_quality_result_rule_id
    ON dq.quality_result (rule_id);

CREATE INDEX IF NOT EXISTS ix_quality_result_run_id
    ON dq.quality_result (run_id);

CREATE INDEX IF NOT EXISTS ix_quality_result_job_id
    ON dq.quality_result (job_id);

CREATE INDEX IF NOT EXISTS ix_quality_result_job_key
    ON dq.quality_result (job_key);

CREATE INDEX IF NOT EXISTS ix_quality_result_dataset_key
    ON dq.quality_result (dataset_key);

CREATE INDEX IF NOT EXISTS ix_quality_result_status
    ON dq.quality_result (status);

CREATE INDEX IF NOT EXISTS ix_quality_result_created_at
    ON dq.quality_result (created_at DESC);


CREATE TABLE IF NOT EXISTS lineage.dataset_lineage (
    lineage_id BIGSERIAL PRIMARY KEY,

    run_id TEXT,
    job_id BIGINT REFERENCES meta.job(job_id),
    job_key TEXT,

    source_dataset_key TEXT,
    target_dataset_key TEXT,

    transformation_type TEXT,
    transformation_ref TEXT,

    details JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_dataset_lineage_run_id
    ON lineage.dataset_lineage (run_id);

CREATE INDEX IF NOT EXISTS ix_dataset_lineage_job_id
    ON lineage.dataset_lineage (job_id);

CREATE INDEX IF NOT EXISTS ix_dataset_lineage_job_key
    ON lineage.dataset_lineage (job_key);

CREATE INDEX IF NOT EXISTS ix_dataset_lineage_source_target
    ON lineage.dataset_lineage (source_dataset_key, target_dataset_key);

CREATE INDEX IF NOT EXISTS ix_dataset_lineage_created_at
    ON lineage.dataset_lineage (created_at DESC);