CREATE SCHEMA IF NOT EXISTS ctl;
CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS runtime;
CREATE SCHEMA IF NOT EXISTS dq;
CREATE SCHEMA IF NOT EXISTS lineage;

COMMENT ON SCHEMA ctl IS 'Control-plane stored procedures/functions.';
COMMENT ON SCHEMA meta IS 'Metadata catalog for pipelines, jobs, datasets, and sources.';
COMMENT ON SCHEMA runtime IS 'Runtime state, execution history, watermarks, and stream metrics.';
COMMENT ON SCHEMA dq IS 'Data quality rules and results.';
COMMENT ON SCHEMA lineage IS 'Dataset lineage records.';