-- =============================================================
-- 014_dynamic_job_registry.sql
-- جداول برای تعریف dynamic jobs از UI
-- بدون نیاز به تغییر pipeline_catalog.json یا stream_registry.json
-- =============================================================

-- ─────────────────────────────────────────────────────────────
-- 1. Job definitions — هر job که از UI تعریف می‌شود اینجا ذخیره می‌شود
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS meta.ui_job_definition (
    job_def_id      BIGSERIAL PRIMARY KEY,
    job_def_key     TEXT NOT NULL UNIQUE,   -- unique slug: pipeline.layer.name
    job_name        TEXT NOT NULL,
    display_name    TEXT,
    pipeline_name   TEXT NOT NULL,
    layer           TEXT NOT NULL CHECK (layer IN ('bronze','silver','gold','stream')),
    job_type        TEXT NOT NULL,
    description     TEXT,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    -- spec ساختار کامل JSON جاب (مثل pipeline_catalog.json)
    spec            JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- runtime policy
    runtime_policy  JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- tags
    tags            JSONB NOT NULL DEFAULT '[]'::jsonb,
    -- creator
    created_by      TEXT,
    updated_by      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_ui_job_def_pipeline
    ON meta.ui_job_definition (pipeline_name);
CREATE INDEX IF NOT EXISTS ix_ui_job_def_layer
    ON meta.ui_job_definition (layer);
CREATE INDEX IF NOT EXISTS ix_ui_job_def_enabled
    ON meta.ui_job_definition (enabled);
CREATE INDEX IF NOT EXISTS ix_ui_job_def_job_type
    ON meta.ui_job_definition (job_type);

-- ─────────────────────────────────────────────────────────────
-- 2. Pipeline definitions — پایپلاین‌های dynamic
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS meta.ui_pipeline_definition (
    pipeline_def_id  BIGSERIAL PRIMARY KEY,
    pipeline_name    TEXT NOT NULL UNIQUE,
    display_name     TEXT,
    description      TEXT,
    pipeline_type    TEXT NOT NULL DEFAULT 'batch' CHECK (pipeline_type IN ('batch','stream')),
    schedule         TEXT,               -- cron یا null
    enabled          BOOLEAN NOT NULL DEFAULT TRUE,
    tags             JSONB NOT NULL DEFAULT '[]'::jsonb,
    dag_config       JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by       TEXT,
    updated_by       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_ui_pipeline_def_type
    ON meta.ui_pipeline_definition (pipeline_type);
CREATE INDEX IF NOT EXISTS ix_ui_pipeline_def_enabled
    ON meta.ui_pipeline_definition (enabled);

-- ─────────────────────────────────────────────────────────────
-- 3. Job dependency — وابستگی‌های dynamic jobs
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS meta.ui_job_dependency (
    dep_id          BIGSERIAL PRIMARY KEY,
    job_def_key     TEXT NOT NULL REFERENCES meta.ui_job_definition(job_def_key) ON DELETE CASCADE,
    depends_on_key  TEXT NOT NULL REFERENCES meta.ui_job_definition(job_def_key) ON DELETE CASCADE,
    dep_type        TEXT NOT NULL DEFAULT 'success',
    CONSTRAINT uq_ui_job_dep UNIQUE (job_def_key, depends_on_key)
);

-- ─────────────────────────────────────────────────────────────
-- 4. Job templates — قالب‌های آماده برای انواع رایج جاب
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS meta.ui_job_template (
    template_id     BIGSERIAL PRIMARY KEY,
    template_key    TEXT NOT NULL UNIQUE,
    template_name   TEXT NOT NULL,
    description     TEXT,
    job_type        TEXT NOT NULL,
    layer           TEXT NOT NULL,
    -- ساختار پیش‌فرض spec که UI به عنوان starting point استفاده می‌کند
    default_spec    JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- schema که UI برای نمایش فرم استفاده می‌کند
    form_schema     JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ─────────────────────────────────────────────────────────────
-- 5. USPs
-- ─────────────────────────────────────────────────────────────

-- upsert pipeline definition
CREATE OR REPLACE PROCEDURE ctl.usp_upsert_ui_pipeline(
    p_pipeline_name  TEXT,
    p_display_name   TEXT,
    p_description    TEXT,
    p_pipeline_type  TEXT,
    p_schedule       TEXT,
    p_enabled        BOOLEAN,
    p_tags           TEXT,
    p_dag_config     TEXT,
    p_created_by     TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_tags      JSONB;
    v_dag_cfg   JSONB;
BEGIN
    v_tags    := COALESCE(NULLIF(p_tags,'')::jsonb,    '[]'::jsonb);
    v_dag_cfg := COALESCE(NULLIF(p_dag_config,'')::jsonb, '{}'::jsonb);

    INSERT INTO meta.ui_pipeline_definition
        (pipeline_name, display_name, description, pipeline_type,
         schedule, enabled, tags, dag_config, created_by, updated_by, updated_at)
    VALUES
        (p_pipeline_name, p_display_name, p_description, p_pipeline_type,
         p_schedule, p_enabled, v_tags, v_dag_cfg, p_created_by, p_created_by, now())
    ON CONFLICT (pipeline_name) DO UPDATE SET
        display_name   = EXCLUDED.display_name,
        description    = EXCLUDED.description,
        pipeline_type  = EXCLUDED.pipeline_type,
        schedule       = EXCLUDED.schedule,
        enabled        = EXCLUDED.enabled,
        tags           = EXCLUDED.tags,
        dag_config     = EXCLUDED.dag_config,
        updated_by     = EXCLUDED.updated_by,
        updated_at     = now();
END;
$$;

-- upsert job definition
CREATE OR REPLACE PROCEDURE ctl.usp_upsert_ui_job(
    p_job_def_key    TEXT,
    p_job_name       TEXT,
    p_display_name   TEXT,
    p_pipeline_name  TEXT,
    p_layer          TEXT,
    p_job_type       TEXT,
    p_description    TEXT,
    p_enabled        BOOLEAN,
    p_spec           TEXT,
    p_runtime_policy TEXT,
    p_tags           TEXT,
    p_created_by     TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_spec   JSONB;
    v_policy JSONB;
    v_tags   JSONB;
BEGIN
    v_spec   := COALESCE(NULLIF(p_spec,'')::jsonb,           '{}'::jsonb);
    v_policy := COALESCE(NULLIF(p_runtime_policy,'')::jsonb, '{}'::jsonb);
    v_tags   := COALESCE(NULLIF(p_tags,'')::jsonb,           '[]'::jsonb);

    INSERT INTO meta.ui_job_definition
        (job_def_key, job_name, display_name, pipeline_name, layer, job_type,
         description, enabled, spec, runtime_policy, tags, created_by, updated_by, updated_at)
    VALUES
        (p_job_def_key, p_job_name, p_display_name, p_pipeline_name, p_layer, p_job_type,
         p_description, p_enabled, v_spec, v_policy, v_tags, p_created_by, p_created_by, now())
    ON CONFLICT (job_def_key) DO UPDATE SET
        job_name       = EXCLUDED.job_name,
        display_name   = EXCLUDED.display_name,
        pipeline_name  = EXCLUDED.pipeline_name,
        layer          = EXCLUDED.layer,
        job_type       = EXCLUDED.job_type,
        description    = EXCLUDED.description,
        enabled        = EXCLUDED.enabled,
        spec           = EXCLUDED.spec,
        runtime_policy = EXCLUDED.runtime_policy,
        tags           = EXCLUDED.tags,
        updated_by     = EXCLUDED.updated_by,
        updated_at     = now();
END;
$$;

-- delete job
CREATE OR REPLACE PROCEDURE ctl.usp_delete_ui_job(p_job_def_key TEXT)
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE meta.ui_job_definition
    SET enabled = FALSE, updated_at = now()
    WHERE job_def_key = p_job_def_key;
END;
$$;

-- delete pipeline
CREATE OR REPLACE PROCEDURE ctl.usp_delete_ui_pipeline(p_pipeline_name TEXT)
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE meta.ui_pipeline_definition
    SET enabled = FALSE, updated_at = now()
    WHERE pipeline_name = p_pipeline_name;
END;
$$;

-- list pipelines
CREATE OR REPLACE FUNCTION ctl.usp_list_ui_pipelines(p_include_disabled BOOLEAN DEFAULT FALSE)
RETURNS TABLE (
    pipeline_def_id  BIGINT,
    pipeline_name    TEXT,
    display_name     TEXT,
    description      TEXT,
    pipeline_type    TEXT,
    schedule         TEXT,
    enabled          BOOLEAN,
    tags             JSONB,
    dag_config       JSONB,
    created_by       TEXT,
    created_at       TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ
)
LANGUAGE sql AS $$
    SELECT pipeline_def_id, pipeline_name, display_name, description,
           pipeline_type, schedule, enabled, tags, dag_config,
           created_by, created_at, updated_at
    FROM meta.ui_pipeline_definition
    WHERE (p_include_disabled OR enabled = TRUE)
    ORDER BY pipeline_name;
$$;

-- list jobs
CREATE OR REPLACE FUNCTION ctl.usp_list_ui_jobs(
    p_pipeline_name TEXT DEFAULT NULL,
    p_include_disabled BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (
    job_def_id      BIGINT,
    job_def_key     TEXT,
    job_name        TEXT,
    display_name    TEXT,
    pipeline_name   TEXT,
    layer           TEXT,
    job_type        TEXT,
    description     TEXT,
    enabled         BOOLEAN,
    spec            JSONB,
    runtime_policy  JSONB,
    tags            JSONB,
    created_by      TEXT,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ
)
LANGUAGE sql AS $$
    SELECT job_def_id, job_def_key, job_name, display_name, pipeline_name,
           layer, job_type, description, enabled, spec, runtime_policy,
           tags, created_by, created_at, updated_at
    FROM meta.ui_job_definition
    WHERE (p_pipeline_name IS NULL OR pipeline_name = p_pipeline_name)
      AND (p_include_disabled OR enabled = TRUE)
    ORDER BY pipeline_name, layer, job_name;
$$;

-- get single job
CREATE OR REPLACE FUNCTION ctl.usp_get_ui_job(p_job_def_key TEXT)
RETURNS TABLE (
    job_def_id      BIGINT,
    job_def_key     TEXT,
    job_name        TEXT,
    display_name    TEXT,
    pipeline_name   TEXT,
    layer           TEXT,
    job_type        TEXT,
    description     TEXT,
    enabled         BOOLEAN,
    spec            JSONB,
    runtime_policy  JSONB,
    tags            JSONB,
    created_by      TEXT,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ
)
LANGUAGE sql AS $$
    SELECT job_def_id, job_def_key, job_name, display_name, pipeline_name,
           layer, job_type, description, enabled, spec, runtime_policy,
           tags, created_by, created_at, updated_at
    FROM meta.ui_job_definition
    WHERE job_def_key = p_job_def_key
    LIMIT 1;
$$;

-- list templates
CREATE OR REPLACE FUNCTION ctl.usp_list_ui_job_templates()
RETURNS TABLE (
    template_id   BIGINT,
    template_key  TEXT,
    template_name TEXT,
    description   TEXT,
    job_type      TEXT,
    layer         TEXT,
    default_spec  JSONB,
    form_schema   JSONB
)
LANGUAGE sql AS $$
    SELECT template_id, template_key, template_name, description,
           job_type, layer, default_spec, form_schema
    FROM meta.ui_job_template
    WHERE is_active = TRUE
    ORDER BY layer, job_type;
$$;

-- ─────────────────────────────────────────────────────────────
-- 6. Seed built-in job templates
-- ─────────────────────────────────────────────────────────────

INSERT INTO meta.ui_job_template
    (template_key, template_name, description, job_type, layer, default_spec, form_schema)
VALUES
(
  'api_to_bronze',
  'API → Bronze',
  'Fetch from REST API and write to Delta bronze layer',
  'generic_api_to_bronze',
  'bronze',
  '{
    "load_type": "event",
    "strategy": "append_event",
    "source": {"base_url": "", "method": "GET", "timeout_seconds": 20},
    "auth": {"type": "none", "secret_env": ""},
    "request": {"query_params": {}, "headers": {}, "body": null},
    "response": {"format": "json", "root_path": "$", "record_mode": "single_object"},
    "validation": {"required_paths": [], "rules": []},
    "schema": [],
    "mapping": {},
    "bronze_write": {"target_path": "", "mode": "append", "partition_by": ["ingest_year","ingest_month","ingest_day","ingest_hour"]},
    "runtime_policy": {"max_retries": 3, "backoff_seconds": 10},
    "spark": {"master": null, "packages": [], "conf": {}}
  }'::jsonb,
  '{
    "sections": [
      {"key":"source","label":"Source","fields":[
        {"key":"source.base_url","label":"Base URL","type":"text","required":true,"placeholder":"https://api.example.com/v1/data"},
        {"key":"source.method","label":"HTTP Method","type":"select","options":["GET","POST"],"default":"GET"},
        {"key":"source.timeout_seconds","label":"Timeout (s)","type":"number","default":20}
      ]},
      {"key":"auth","label":"Authentication","fields":[
        {"key":"auth.type","label":"Auth Type","type":"select","options":["none","query_param","bearer","header"],"default":"none"},
        {"key":"auth.secret_env","label":"Secret Env Var","type":"text","placeholder":"MY_API_KEY"},
        {"key":"auth.param_name","label":"Param Name (if query_param)","type":"text","placeholder":"api_key"}
      ]},
      {"key":"write","label":"Bronze Write","fields":[
        {"key":"bronze_write.target_path","label":"Target Path (S3)","type":"text","required":true,"placeholder":"s3a://lakehouse/bronze/my_dataset"},
        {"key":"bronze_write.mode","label":"Write Mode","type":"select","options":["append","overwrite"],"default":"append"}
      ]},
      {"key":"runtime","label":"Runtime Policy","fields":[
        {"key":"runtime_policy.max_retries","label":"Max Retries","type":"number","default":3},
        {"key":"runtime_policy.backoff_seconds","label":"Backoff (s)","type":"number","default":10}
      ]}
    ]
  }'::jsonb
),
(
  'bronze_to_silver',
  'Bronze → Silver',
  'Clean and transform bronze Delta to silver layer',
  'generic_bronze_to_silver',
  'silver',
  '{
    "source": {"path": "", "format": "delta"},
    "target": {"path": "", "format": "delta", "mode": "merge", "merge_keys": [], "partition_by": ["processing_date"]},
    "select_map": {},
    "filters": [],
    "derived_fields": {"processing_date": {"kind": "sql", "expr": "to_date(ingestion_ts)"}},
    "quality_rules": [],
    "dedupe": {"key_columns": [], "order_by": ["ingestion_ts asc"]},
    "spark": {"master": null, "packages": [], "conf": {}}
  }'::jsonb,
  '{
    "sections": [
      {"key":"source","label":"Source","fields":[
        {"key":"source.path","label":"Bronze Path","type":"text","required":true,"placeholder":"s3a://lakehouse/bronze/my_dataset"},
        {"key":"source.format","label":"Format","type":"select","options":["delta","parquet"],"default":"delta"}
      ]},
      {"key":"target","label":"Target","fields":[
        {"key":"target.path","label":"Silver Path","type":"text","required":true,"placeholder":"s3a://lakehouse/silver/my_dataset_clean"},
        {"key":"target.mode","label":"Write Mode","type":"select","options":["merge","append","overwrite"],"default":"merge"},
        {"key":"target.merge_keys","label":"Merge Keys (comma-sep)","type":"text","placeholder":"event_id"}
      ]},
      {"key":"dedupe","label":"Deduplication","fields":[
        {"key":"dedupe.key_columns","label":"Dedup Keys (comma-sep)","type":"text","placeholder":"event_id"},
        {"key":"dedupe.order_by","label":"Order By","type":"text","placeholder":"ingestion_ts asc"}
      ]}
    ]
  }'::jsonb
),
(
  'silver_to_gold',
  'Silver → Gold',
  'Aggregate and curate silver to gold layer',
  'generic_silver_to_gold',
  'gold',
  '{
    "source": {"path": "", "format": "delta"},
    "target": {"path": "", "format": "delta", "mode": "merge", "merge_keys": [], "partition_by": []},
    "select_map": {},
    "filters": [],
    "derived_fields": {},
    "quality_rules": [],
    "dedupe": {"key_columns": [], "order_by": []},
    "spark": {"master": null, "packages": [], "conf": {}}
  }'::jsonb,
  '{
    "sections": [
      {"key":"source","label":"Source","fields":[
        {"key":"source.path","label":"Silver Path","type":"text","required":true}
      ]},
      {"key":"target","label":"Target","fields":[
        {"key":"target.path","label":"Gold Path","type":"text","required":true},
        {"key":"target.mode","label":"Write Mode","type":"select","options":["merge","append","overwrite"],"default":"merge"},
        {"key":"target.merge_keys","label":"Merge Keys (comma-sep)","type":"text"}
      ]}
    ]
  }'::jsonb
),
(
  'jdbc_to_bronze',
  'JDBC → Bronze',
  'Ingest from SQL database using manifest-driven approach',
  'generic_jdbc_manifest_to_bronze',
  'bronze',
  '{
    "manifest_ref": "",
    "execution_strategy": "one_task_per_table",
    "runtime_policy": {"max_retries": 1, "backoff_seconds": 30}
  }'::jsonb,
  '{
    "sections": [
      {"key":"source","label":"JDBC Source","fields":[
        {"key":"manifest_ref","label":"Manifest File Path","type":"text","required":true,"placeholder":"configs/sources/jdbc/my_source_manifest.json"},
        {"key":"execution_strategy","label":"Execution Strategy","type":"select","options":["one_task_per_table","one_task_per_manifest"],"default":"one_task_per_table"}
      ]},
      {"key":"runtime","label":"Runtime Policy","fields":[
        {"key":"runtime_policy.max_retries","label":"Max Retries","type":"number","default":1}
      ]}
    ]
  }'::jsonb
)
ON CONFLICT (template_key) DO NOTHING;


-- Get-Content .\database\migrations\014_dynamic_job_registry.sql -Raw | docker exec -i postgres-warehouse `  sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
