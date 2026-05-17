# How to Add a New Batch API Pipeline

This guide explains how to add a new API-driven batch pipeline to the `new_architecture_v2` platform.

The API batch architecture is catalog-driven:

```text
configs/batch/pipeline_catalog.json
        ↓
airflow/builder/batch_catalog_builder.py
        ↓
batch/runners/generic_api_to_bronze.py
        ↓
batch/runners/generic_bronze_to_silver.py
        ↓
batch/runners/generic_silver_to_gold.py
        ↓
Delta Lake on MinIO
```

---

## 1. When to Use an API Batch Pipeline

Use this pattern when data is collected from a REST API on demand or on a schedule.

Examples:

```text
weather API
currency API
gold/commodity price API
public open data API
third-party business API
```

Do not use this for Kafka/event streaming. Use the streaming registry instead.

Do not use this for SQL Server/PostgreSQL/MySQL tables. Use JDBC database ingestion instead.

---

## 2. Main File to Edit

Open:

```text
configs/batch/pipeline_catalog.json
```

Add a new object inside the top-level `pipelines` array.

---

## 3. Minimal Pipeline Structure

A complete API pipeline normally has three jobs:

```text
API → Bronze
Bronze → Silver
Silver → Gold
```

Example skeleton:

```json
{
  "name": "currency_rate_pipeline",
  "enabled": true,
  "description": "API-driven currency rate Bronze/Silver/Gold batch pipeline",
  "domain": "currency_rate",
  "source_type": "api",
  "dag": {
    "schedule": null,
    "catchup": false,
    "max_active_runs": 1,
    "start_date": "2026-01-01T00:00:00+00:00",
    "tags": [
      "currency",
      "batch",
      "api",
      "spark",
      "catalog_driven"
    ],
    "default_args": {
      "owner": "admin",
      "retries": 0,
      "retry_delay_minutes": 1
    }
  },
  "jobs": []
}
```

---

## 4. Add the API-to-Bronze Job

Add this job inside the `jobs` array:

```json
{
  "name": "currency_rate_ingest",
  "enabled": true,
  "job_type": "generic_api_to_bronze",
  "source_type": "api",
  "dependencies": [],
  "execution_timeout_minutes": 10,
  "retries": 0,
  "retry_delay_minutes": 1,
  "tags": [
    "currency",
    "api",
    "bronze"
  ],
  "spec": {
    "source": {
      "base_url": "https://api.example.com/v1/latest",
      "method": "GET",
      "timeout_seconds": 20
    },
    "auth": {
      "type": "query_param",
      "param_name": "api_key",
      "secret_env": "CURRENCY_API_KEY"
    },
    "request": {
      "query_params": {
        "base": "USD",
        "symbols": "EUR,GBP,CAD"
      },
      "headers": {},
      "body": null
    },
    "response": {
      "format": "json",
      "root_path": "$",
      "record_mode": "single_object"
    },
    "validation": {
      "required_paths": [
        "$.timestamp",
        "$.rates"
      ],
      "rules": []
    },
    "schema": [
      "event_id string not null",
      "source_name string not null",
      "source_event_ts timestamp null",
      "ingestion_ts timestamp not null",
      "payload_json string not null",
      "api_status string not null"
    ],
    "mapping": {
      "source_name": {
        "source": "constant",
        "value": "currency_api"
      },
      "source_event_ts": {
        "source": "json",
        "path": "$.timestamp",
        "cast": "unix_ts"
      },
      "ingestion_ts": {
        "source": "runtime",
        "value": "current_utc"
      },
      "payload_json": {
        "source": "runtime",
        "value": "raw_payload_json"
      },
      "api_status": {
        "source": "constant",
        "value": "success"
      }
    },
    "bronze_write": {
      "target_path": "s3a://lakehouse/bronze/currency_rate_events",
      "mode": "append",
      "partition_by": [
        "ingest_year",
        "ingest_month",
        "ingest_day",
        "ingest_hour"
      ],
      "include_raw_payload": true
    },
    "runtime_policy": {
      "max_retries": 3,
      "backoff_seconds": 10
    },
    "spark": {
      "master": null,
      "packages": [],
      "conf": {}
    }
  }
}
```

---

## 5. Authentication Options

### No Authentication

```json
"auth": {
  "type": "none",
  "secret_env": "NO_AUTH"
}
```

### Query Parameter Authentication

```json
"auth": {
  "type": "query_param",
  "param_name": "api_key",
  "secret_env": "CURRENCY_API_KEY"
}
```

Add the secret to `.env`:

```env
CURRENCY_API_KEY=replace-with-real-api-key
```

The Airflow builder forwards prefixed secrets for known prefixes. If you add a new secret prefix, make sure it is forwarded in:

```text
airflow/builder/batch_catalog_builder.py
```

Specifically, update `forward_prefixed_env([...])` if needed.

---

## 6. Add Bronze-to-Silver Job

Add a Silver job that depends on the ingest job:

```json
{
  "name": "currency_rate_silver",
  "enabled": true,
  "job_type": "generic_bronze_to_silver",
  "source_type": "api",
  "dependencies": [
    "currency_rate_ingest"
  ],
  "execution_timeout_minutes": 10,
  "retries": 0,
  "retry_delay_minutes": 1,
  "tags": [
    "currency",
    "silver"
  ],
  "spec": {
    "source": {
      "path": "s3a://lakehouse/bronze/currency_rate_events",
      "format": "delta"
    },
    "target": {
      "path": "s3a://lakehouse/silver/currency_rate_clean",
      "format": "delta",
      "mode": "merge",
      "merge_keys": [
        "event_id"
      ],
      "partition_by": [
        "processing_date"
      ]
    },
    "select_map": {
      "event_id": "event_id",
      "source_name": "source_name",
      "source_event_ts": "source_event_ts",
      "ingestion_ts": "ingestion_ts",
      "payload_json": "payload_json",
      "api_status": "api_status"
    },
    "filters": [
      {
        "type": "equals",
        "field": "api_status",
        "value": "success"
      }
    ],
    "derived_fields": {
      "processing_date": {
        "kind": "sql",
        "expr": "to_date(ingestion_ts)"
      }
    },
    "quality_rules": [
      {
        "type": "not_null",
        "field": "event_id"
      },
      {
        "type": "not_null",
        "field": "ingestion_ts"
      },
      {
        "type": "not_null",
        "field": "payload_json"
      }
    ],
    "dedupe": {
      "key_columns": [
        "event_id"
      ],
      "order_by": [
        "ingestion_ts asc"
      ]
    },
    "spark": {
      "master": null,
      "packages": [],
      "conf": {}
    }
  }
}
```

---

## 7. Add Silver-to-Gold Job

Add a Gold job that depends on the Silver job:

```json
{
  "name": "currency_rate_gold",
  "enabled": true,
  "job_type": "generic_silver_to_gold",
  "source_type": "api",
  "dependencies": [
    "currency_rate_silver"
  ],
  "execution_timeout_minutes": 10,
  "retries": 0,
  "retry_delay_minutes": 1,
  "tags": [
    "currency",
    "gold"
  ],
  "spec": {
    "source": {
      "path": "s3a://lakehouse/silver/currency_rate_clean",
      "format": "delta"
    },
    "target": {
      "path": "s3a://lakehouse/gold/currency_rate_curated",
      "format": "delta",
      "mode": "merge",
      "merge_keys": [
        "event_id"
      ],
      "partition_by": [
        "event_date"
      ]
    },
    "select_map": {
      "event_id": "event_id",
      "source_name": "source_name",
      "event_ts": "source_event_ts",
      "payload_json": "payload_json"
    },
    "filters": [],
    "derived_fields": {
      "event_date": {
        "kind": "sql",
        "expr": "to_date(event_ts)"
      },
      "dataset_name": {
        "kind": "sql",
        "expr": "'currency_rate'"
      }
    },
    "quality_rules": [
      {
        "type": "not_null",
        "field": "event_id"
      }
    ],
    "dedupe": {
      "key_columns": [
        "event_id"
      ],
      "order_by": [
        "event_ts asc"
      ]
    },
    "spark": {
      "master": null,
      "packages": [],
      "conf": {}
    }
  }
}
```

---

## 8. DAG Dependencies

Use the `dependencies` array to define job order.

Example:

```text
currency_rate_ingest
        ↓
currency_rate_silver
        ↓
currency_rate_gold
```

The catalog builder converts these dependencies to Airflow task dependencies.

---

## 9. Add Required Secrets

Update `.env`:

```env
CURRENCY_API_KEY=replace-with-real-api-key
```

Also update `.env.example` with a placeholder:

```env
CURRENCY_API_KEY=replace-with-real-api-key
```

If the secret prefix is not forwarded, update:

```text
airflow/builder/batch_catalog_builder.py
```

Example:

```python
forward_prefixed_env([
    "ECOM_",
    "METALPRICE_",
    "OPENWEATHER_",
    "CURRENCY_"
])
```

---

## 10. Validate Airflow DAGs

Restart Airflow components:

```powershell
docker compose `
  -f compose/compose.phase1.yaml `
  -f compose/compose.phase2.yaml `
  -f compose/compose.simulator.yaml `
  up -d --force-recreate airflow-scheduler airflow-api-server airflow-dag-processor
```

Check DAG import:

```powershell
docker logs airflow-scheduler --tail 300
```

---

## 11. Run the Pipeline

Open Airflow UI and trigger the new DAG.

Expected task order:

```text
currency_rate_ingest
currency_rate_silver
currency_rate_gold
```

---

## 12. Check Delta Outputs

Expected paths:

```text
s3a://lakehouse/bronze/currency_rate_events
s3a://lakehouse/silver/currency_rate_clean
s3a://lakehouse/gold/currency_rate_curated
```

Use Spark/Jupyter to inspect:

```python
df = spark.read.format("delta").load("s3a://lakehouse/bronze/currency_rate_events")
df.show(truncate=False)
```

---

## 13. Common Problems

### Missing API key

Symptoms:

```text
Required secret env is not set
```

Fix:

```text
Add the API key to .env and make sure the prefix is forwarded by batch_catalog_builder.py.
```

### DAG does not appear

Fix:

```text
Check JSON syntax in pipeline_catalog.json.
Restart airflow-scheduler and airflow-dag-processor.
Check scheduler logs.
```

### Bronze works but Silver fails

Fix:

```text
Check select_map fields.
Check payload_json format.
Check quality_rules.
Check dedupe keys.
```

---

## 14. Checklist

```text
[ ] Added new pipeline object to configs/batch/pipeline_catalog.json
[ ] Used unique pipeline name
[ ] Used unique job names
[ ] Added API secret to .env
[ ] Added API secret placeholder to .env.example
[ ] Updated env prefix forwarding if needed
[ ] Configured API-to-Bronze job
[ ] Configured Bronze-to-Silver job
[ ] Configured Silver-to-Gold job
[ ] Set dependencies correctly
[ ] Restarted Airflow components
[ ] Triggered DAG successfully
[ ] Verified Bronze Delta output
[ ] Verified Silver Delta output
[ ] Verified Gold Delta output
```
