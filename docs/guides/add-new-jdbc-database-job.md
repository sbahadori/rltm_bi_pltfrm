# How to Add a New JDBC Database Job

This guide explains how to add a new SQL Server, PostgreSQL, or MySQL database ingestion job to the `new_architecture_v2` platform.

The JDBC database ingestion architecture is manifest-driven:

```text
configs/batch/pipeline_catalog.json
        ↓
configs/sources/jdbc/<source>_manifest.json
        ↓
configs/connections/jdbc_connections.json
        ↓
configs/sql/jdbc/<source>/*.sql
        ↓
batch/runners/generic_jdbc_manifest_to_bronze.py
        ↓
Delta Bronze on MinIO
```

---

## 1. When to Use a JDBC Database Job

Use this pattern when the source is a relational database:

```text
SQL Server
PostgreSQL
MySQL
```

Use this for:

```text
operational tables
ecommerce tables
orders/products/customers
lookup/reference tables
transaction tables
```

Do not use this for Kafka/event streams. Use the streaming registry.

Do not use this for REST APIs. Use an API batch pipeline.

---

## 2. Step 1 — Add or Reuse a JDBC Connection

Open:

```text
configs/connections/jdbc_connections.json
```

The platform already supports connection definitions like:

```text
ecommerce_sqlserver
ecommerce_postgres
ecommerce_mysql
```

Example SQL Server connection:

```json
{
  "type": "sqlserver",
  "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
  "jdbc_url_template": "jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=false;trustServerCertificate=true",
  "default_port": 1433,
  "env": {
    "host": "ECOM_SQLSERVER_HOST",
    "port": "ECOM_SQLSERVER_PORT",
    "database": "ECOM_SQLSERVER_DB",
    "user": "ECOM_SQLSERVER_USER",
    "password": "ECOM_SQLSERVER_PASSWORD"
  },
  "spark_packages": [],
  "default_options": {
    "fetchsize": "5000",
    "queryTimeout": "0"
  }
}
```

---

## 3. Step 2 — Add Credentials to `.env`

Example for SQL Server:

```env
ECOM_SQLSERVER_HOST=host.docker.internal
ECOM_SQLSERVER_PORT=1433
ECOM_SQLSERVER_DB=replace-with-database-name
ECOM_SQLSERVER_USER=replace-with-user
ECOM_SQLSERVER_PASSWORD=replace-with-password
```

Also add placeholders to:

```text
.env.example
```

Important:

```text
Do not commit real credentials.
```

---

## 4. Step 3 — Create a Source Manifest

Create a new manifest under:

```text
configs/sources/jdbc/
```

Example:

```text
configs/sources/jdbc/ecommerce_sqlserver_manifest.json
```

Recommended structure:

```json
{
  "version": 1,
  "source_id": "ecommerce_sqlserver",
  "source_type": "jdbc",
  "connection_ref": "ecommerce_sqlserver",
  "enabled": true,
  "defaults": {
    "load_type": "full",
    "strategy": "overwrite",
    "count_rows": false,
    "num_partitions": 1,
    "fetch_size": 10000,
    "bronze_format": "delta",
    "bronze_mode": "overwrite",
    "target_path_template": "s3a://lakehouse/bronze/{source_id}/{table_id}",
    "state_path_template": "s3a://lakehouse/_state/jdbc/{source_id}/{table_id}",
    "watermark": null,
    "cdc": null,
    "sql": {},
    "partition_by": [
      "ingest_year",
      "ingest_month",
      "ingest_day"
    ],
    "add_ingestion_metadata": true,
    "add_row_hash": false
  },
  "tables": []
}
```

---

## 5. Step 4 — Add Tables to the Manifest

Each table must define:

```text
table_id
enabled
load_type
strategy
source_table
primary_key
sql.extract_ref
columns
```

---

## 6. Full Overwrite Table

Use this for small lookup/reference tables.

```json
{
  "table_id": "brands",
  "enabled": true,
  "load_type": "full",
  "strategy": "overwrite",
  "bronze_mode": "overwrite",
  "source_table": "dbo.brands",
  "primary_key": [
    "post_id"
  ],
  "sql": {
    "extract_ref": "configs/sql/jdbc/ecommerce_sqlserver/brands_full.sql"
  },
  "columns": [
    "post_id",
    "post_title",
    "created_at",
    "updated_at"
  ]
}
```

SQL file:

```text
configs/sql/jdbc/ecommerce_sqlserver/brands_full.sql
```

```sql
SELECT {columns}
FROM {source_table}
```

Behavior:

```text
Read full source table
Overwrite Bronze Delta table
Do not use state
```

---

## 7. Incremental Numeric Watermark Table

Use this when the table has a numeric watermark column.

```json
{
  "table_id": "products",
  "enabled": true,
  "load_type": "incremental",
  "strategy": "numeric_watermark",
  "bronze_mode": "append",
  "source_table": "dbo.products",
  "primary_key": [
    "id"
  ],
  "watermark": {
    "column": "updated_at",
    "type": "long",
    "initial_value": 0,
    "upper_bound_mode": "none"
  },
  "sql": {
    "extract_ref": "configs/sql/jdbc/ecommerce_sqlserver/products_incremental_numeric.sql"
  },
  "columns": [
    "id",
    "title",
    "updated_at",
    "created_at"
  ]
}
```

SQL file:

```text
configs/sql/jdbc/ecommerce_sqlserver/products_incremental_numeric.sql
```

```sql
SELECT {columns}
FROM {source_table}
WHERE {incremental_column} > {lower_bound}
```

Behavior:

```text
Read last state from Delta
Read rows greater than lower_bound
Append to Bronze
Update state after successful write
```

---

## 8. Incremental Timestamp Table

Use this when the table has a reliable timestamp column.

```json
{
  "table_id": "customers",
  "enabled": true,
  "load_type": "incremental",
  "strategy": "timestamp",
  "bronze_mode": "append",
  "source_table": "dbo.customers",
  "primary_key": [
    "customer_id"
  ],
  "watermark": {
    "column": "updated_at",
    "type": "timestamp",
    "initial_value": "1970-01-01T00:00:00"
  },
  "sql": {
    "extract_ref": "configs/sql/jdbc/ecommerce_sqlserver/customers_incremental_timestamp.sql"
  },
  "columns": [
    "customer_id",
    "email",
    "updated_at"
  ]
}
```

SQL file:

```sql
SELECT {columns}
FROM {source_table}
WHERE {incremental_column} > {lower_bound}
```

Generated SQL Server example:

```sql
WHERE [updated_at] > '1970-01-01T00:00:00'
```

---

## 9. Incremental Sequence Table

Use this for insert-only tables with increasing IDs.

```json
{
  "table_id": "orders",
  "enabled": true,
  "load_type": "incremental",
  "strategy": "sequence",
  "bronze_mode": "append",
  "source_table": "dbo.orders",
  "primary_key": [
    "id"
  ],
  "watermark": {
    "column": "id",
    "type": "long",
    "initial_value": 0,
    "upper_bound_mode": "max"
  },
  "sql": {
    "extract_ref": "configs/sql/jdbc/ecommerce_sqlserver/orders_incremental_sequence.sql",
    "max_bound_ref": "configs/sql/jdbc/ecommerce_sqlserver/orders_max_sequence.sql"
  },
  "columns": [
    "id",
    "customer_id",
    "order_total",
    "created_at"
  ]
}
```

Extraction SQL:

```sql
SELECT {columns}
FROM {source_table}
WHERE {incremental_column} > {lower_bound}
  AND {incremental_column} <= {upper_bound}
```

Max-bound SQL:

```sql
SELECT MAX({incremental_column}) AS max_value
FROM {source_table}
```

Behavior:

```text
Read last sequence value
Read current max sequence from source
Extract bounded range
Append to Bronze
Update state to upper_bound
```

Warning:

```text
Do not use sequence strategy if old rows can be updated and those updates must be captured.
```

---

## 10. Full Append Snapshot

Use this when:

```text
No reliable timestamp exists
No reliable sequence exists
The table is not too large
You want audit snapshots
```

Example:

```json
{
  "table_id": "customer_profile",
  "enabled": true,
  "load_type": "full",
  "strategy": "append_snapshot",
  "bronze_mode": "append",
  "source_table": "dbo.customer_profile",
  "primary_key": [
    "customer_id"
  ],
  "sql": {
    "extract_ref": "configs/sql/jdbc/ecommerce_sqlserver/customer_profile_full.sql"
  }
}
```

Silver should then choose the latest snapshot as the current state.

---

## 11. Step 5 — Add SQL Files

SQL templates are stored under:

```text
configs/sql/jdbc/<source_id>/
```

Examples:

```text
configs/sql/jdbc/ecommerce_sqlserver/brands_full.sql
configs/sql/jdbc/ecommerce_sqlserver/products_incremental_numeric.sql
configs/sql/jdbc/ecommerce_sqlserver/orders_incremental_sequence.sql
configs/sql/jdbc/ecommerce_sqlserver/orders_max_sequence.sql
```

Supported placeholders:

| Placeholder | Meaning |
|---|---|
| `{columns}` | Selected columns, quoted for the database |
| `{source_table}` | Source table, quoted for the database |
| `{incremental_column}` | Quoted watermark/incremental column |
| `{watermark_column}` | Alias for quoted watermark column |
| `{lower_bound}` | Last persisted state |
| `{upper_bound}` | Upper bound for bounded strategies |
| `{load_type}` | Current load type |
| `{strategy}` | Current strategy |

---

## 12. Step 6 — Register the Pipeline in the Batch Catalog

Open:

```text
configs/batch/pipeline_catalog.json
```

Add a pipeline object:

```json
{
  "name": "ecommerce_sqlserver_bronze_pipeline",
  "enabled": true,
  "description": "Ecommerce SQL Server source-manifest-driven Bronze ingestion",
  "domain": "ecommerce",
  "source_type": "jdbc",
  "dag": {
    "schedule": null,
    "catchup": false,
    "max_active_runs": 1,
    "start_date": "2026-01-01T00:00:00+00:00",
    "tags": [
      "ecommerce",
      "sqlserver",
      "jdbc",
      "batch",
      "bronze",
      "manifest_driven"
    ],
    "default_args": {
      "owner": "admin",
      "retries": 1,
      "retry_delay_minutes": 2
    }
  },
  "jobs": [
    {
      "name": "ecommerce_sqlserver_to_bronze",
      "enabled": true,
      "job_type": "generic_jdbc_manifest_to_bronze",
      "source_type": "jdbc",
      "dependencies": [],
      "execution_timeout_minutes": 90,
      "retries": 1,
      "retry_delay_minutes": 2,
      "tags": [
        "ecommerce",
        "sqlserver",
        "bronze"
      ],
      "manifest_ref": "configs/sources/jdbc/ecommerce_sqlserver_manifest.json",
      "execution_strategy": "one_task_per_table"
    }
  ]
}
```

Execution strategies:

| execution_strategy | Meaning |
|---|---|
| `one_task_per_manifest` | One Airflow task ingests all enabled tables |
| `one_task_per_table` | One Airflow task per enabled table |

Recommended:

```text
Use one_task_per_table for better observability and failure isolation.
```

---

## 13. Step 7 — Restart Airflow

```powershell
docker compose `
  -f compose/compose.phase1.yaml `
  -f compose/compose.phase2.yaml `
  -f compose/compose.simulator.yaml `
  up -d --force-recreate airflow-scheduler airflow-api-server airflow-dag-processor
```

---

## 14. Step 8 — Run the DAG

Open Airflow and trigger the DAG.

If `execution_strategy = one_task_per_table`, expected tasks:

```text
ecommerce_sqlserver_to_bronze__brands
ecommerce_sqlserver_to_bronze__products
...
```

---

## 15. State Management

Incremental state is stored in Delta:

```text
s3a://lakehouse/_state/jdbc/{source_id}/{table_id}
```

State key format:

```text
{load_type}::{strategy}::{column}
```

Examples:

```text
incremental::numeric_watermark::updated_at
incremental::sequence::id
incremental::timestamp::updated_at
```

State is updated only after a successful Bronze write.

---

## 16. Resetting State

If you change strategy/type/initial value and need a clean run, delete the table state prefix from MinIO:

```text
_state/jdbc/<source_id>/<table_id>
```

Example:

```text
_state/jdbc/ecommerce_sqlserver/products
```

Or temporarily change:

```json
"state_path_template": "s3a://lakehouse/_state/jdbc_v2/{source_id}/{table_id}"
```

---

## 17. Common Errors

### Missing database environment values

Error:

```text
Missing environment values for connection_ref
```

Fix:

```text
Check .env variables.
Check configs/connections/jdbc_connections.json env mapping.
Restart Airflow.
```

### Invalid numeric SQL bound value

Error:

```text
Invalid numeric SQL bound value='1970-01-01T00:00:00' for value_type='long'
```

Fix:

```text
The persisted state contains an old timestamp string.
Delete old state or change state_path_template.
```

### Incorrect syntax near T00

Cause:

```text
A timestamp string was used as a numeric watermark.
```

Fix:

```text
Use watermark.type=timestamp for timestamp columns.
Use watermark.type=long and initial_value=0 for numeric columns.
Reset old state.
```

### Delta schema mismatch

Fix:

```text
The writer already uses overwriteSchema for overwrite and mergeSchema for append.
If old data is incompatible, delete the old Bronze prefix for local development.
```

---

## 18. Checklist

```text
[ ] Added or reused connection_ref in configs/connections/jdbc_connections.json
[ ] Added required credentials to .env
[ ] Added placeholders to .env.example
[ ] Created JDBC source manifest under configs/sources/jdbc/
[ ] Added table entries with load_type and strategy
[ ] Added SQL files under configs/sql/jdbc/<source_id>/
[ ] Registered pipeline in configs/batch/pipeline_catalog.json
[ ] Chose execution_strategy
[ ] Restarted Airflow components
[ ] Triggered DAG
[ ] Verified Bronze Delta output
[ ] Verified state path for incremental tables
```
