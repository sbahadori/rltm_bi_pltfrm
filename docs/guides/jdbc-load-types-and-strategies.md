# JDBC Load Types and Strategies Guide

This document explains how JDBC-based batch ingestion works in this project and how to configure each source table using `load_type` and `strategy`.

The current architecture is:

```text
Manifest-driven Python/Spark engine
+ SQL templates for table-level extraction
+ Delta/Spark for Bronze/Silver/Gold processing
```

---

## 1. Core Concept

Each table must define two concepts:

```json
{
  "load_type": "...",
  "strategy": "..."
}
```

### `load_type`

Defines the general loading family.

Supported values:

| load_type | Meaning |
|---|---|
| `full` | Read the entire source table |
| `incremental` | Read only new or changed records using a watermark/sequence |
| `cdc` | Read database change events from CDC/log-based mechanisms |

### `strategy`

Defines the exact implementation method for the selected `load_type`.

Supported strategies:

| load_type | strategy | Meaning |
|---|---|---|
| `full` | `overwrite` | Replace the Bronze table on every run |
| `full` | `append_snapshot` | Append a full snapshot on every run |
| `incremental` | `timestamp` | Use a timestamp column such as `updated_at` |
| `incremental` | `numeric_watermark` | Use a numeric watermark column |
| `incremental` | `sequence` | Use an increasing sequence/id column |
| `incremental` | `rowversion` | Use SQL Server rowversion/change-version style logic |
| `cdc` | `sqlserver_cdc` | Future SQL Server CDC support |
| `cdc` | `sqlserver_change_tracking` | Future SQL Server Change Tracking support |
| `cdc` | `postgres_logical_replication` | Future PostgreSQL logical replication support |
| `cdc` | `mysql_binlog` | Future MySQL binlog support |
| `cdc` | `debezium` | Future Debezium-based CDC support |

---

## 2. Decision Rule

Use this rule when choosing a load strategy:

```text
1. Small lookup/reference table?
   -> full + overwrite

2. Need full audit history of snapshots?
   -> full + append_snapshot

3. Has reliable updated_at / modified_at timestamp?
   -> incremental + timestamp

4. Has numeric updated_at / numeric watermark?
   -> incremental + numeric_watermark

5. Has increasing id / transaction_id / sequence?
   -> incremental + sequence

6. Has SQL Server rowversion/change version?
   -> incremental + rowversion

7. Need inserts, updates, and deletes accurately?
   -> cdc strategy, when implemented

8. No timestamp, no sequence, no CDC?
   -> full overwrite or full append_snapshot
```

Important:

> Incremental loading without a reliable timestamp, sequence, rowversion, or CDC is unsafe. It may silently miss updates or deletes.

---

## 3. Manifest Contract

Each JDBC source manifest is stored under:

```text
configs/sources/jdbc/
```

Example:

```text
configs/sources/jdbc/ecommerce_sqlserver_manifest.json
```

Recommended table-level contract:

```json
{
  "table_id": "products",
  "enabled": true,
  "load_type": "incremental",
  "strategy": "numeric_watermark",
  "bronze_mode": "append",
  "source_table": "dbo.products",
  "primary_key": ["id"],
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

---

## 4. Full Load

Use full load for small or reference tables.

### 4.1 Full + Overwrite

Use this when the source table is small and the latest state is enough.

```json
{
  "table_id": "brands",
  "enabled": true,
  "load_type": "full",
  "strategy": "overwrite",
  "bronze_mode": "overwrite",
  "source_table": "dbo.brands",
  "primary_key": ["post_id"],
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
Overwrite Bronze table
Overwrite schema if needed
No state/watermark is used
```

---

### 4.2 Full + Append Snapshot

Use this when you want to keep every full snapshot for audit/history.

```json
{
  "table_id": "customer_profile",
  "enabled": true,
  "load_type": "full",
  "strategy": "append_snapshot",
  "bronze_mode": "append",
  "source_table": "dbo.customer_profile",
  "primary_key": ["customer_id"],
  "sql": {
    "extract_ref": "configs/sql/jdbc/ecommerce_sqlserver/customer_profile_full.sql"
  }
}
```

Behavior:

```text
Read full source table
Append the full snapshot to Bronze
Silver should select the latest snapshot as the current state
```

Use this when:

```text
The table has updates but no timestamp
The table has no reliable sequence
You still want snapshot history
```

---

## 5. Incremental Load

Incremental load requires a reliable column that can detect new or changed records.

---

### 5.1 Incremental + Timestamp

Use this when the source table has a real timestamp column.

Example:

```json
{
  "table_id": "products",
  "enabled": true,
  "load_type": "incremental",
  "strategy": "timestamp",
  "bronze_mode": "append",
  "source_table": "dbo.products",
  "primary_key": ["id"],
  "watermark": {
    "column": "UpdatedDateTime",
    "type": "timestamp",
    "initial_value": "1970-01-01T00:00:00"
  },
  "sql": {
    "extract_ref": "configs/sql/jdbc/ecommerce_sqlserver/products_incremental_timestamp.sql"
  }
}
```

SQL file:

```sql
SELECT {columns}
FROM {source_table}
WHERE {watermark_column} > {lower_bound}
```

Generated example for SQL Server:

```sql
SELECT [id], [title], [UpdatedDateTime]
FROM [dbo].[products]
WHERE [UpdatedDateTime] > '1970-01-01T00:00:00'
```

Behavior:

```text
Read last watermark from Delta state
Extract records greater than last watermark
Append records to Bronze
Update watermark after successful Bronze write
```

Warning:

```text
Timestamp-based incremental may miss records if multiple rows share the same timestamp.
For critical tables, use lookback windows or a tie-breaker column in Silver.
```

---

### 5.2 Incremental + Numeric Watermark

Use this when the watermark column is numeric.

Example:

```json
{
  "table_id": "products",
  "enabled": true,
  "load_type": "incremental",
  "strategy": "numeric_watermark",
  "bronze_mode": "append",
  "source_table": "dbo.products",
  "primary_key": ["id"],
  "watermark": {
    "column": "updated_at",
    "type": "long",
    "initial_value": 0,
    "upper_bound_mode": "none"
  },
  "sql": {
    "extract_ref": "configs/sql/jdbc/ecommerce_sqlserver/products_incremental_numeric.sql"
  }
}
```

SQL file:

```sql
SELECT {columns}
FROM {source_table}
WHERE {watermark_column} > {lower_bound}
```

Generated example:

```sql
SELECT [id], [title], [updated_at]
FROM [dbo].[products]
WHERE [updated_at] > 0
```

Use this when:

```text
updated_at is stored as integer/long
watermark is a numeric change marker
the column reliably increases when data changes
```

Do not use this when:

```text
The numeric column is not monotonic
The value can be reset or reused
The column does not change on updates
```

---

### 5.3 Incremental + Sequence

Use this when the table is insert-only and has an increasing ID or transaction sequence.

Example:

```json
{
  "table_id": "orders",
  "enabled": true,
  "load_type": "incremental",
  "strategy": "sequence",
  "bronze_mode": "append",
  "source_table": "dbo.orders",
  "primary_key": ["id"],
  "watermark": {
    "column": "id",
    "type": "long",
    "initial_value": 0,
    "upper_bound_mode": "max"
  },
  "sql": {
    "extract_ref": "configs/sql/jdbc/ecommerce_sqlserver/orders_incremental_sequence.sql",
    "max_bound_ref": "configs/sql/jdbc/ecommerce_sqlserver/orders_max_sequence.sql"
  }
}
```

SQL extraction file:

```sql
SELECT {columns}
FROM {source_table}
WHERE {incremental_column} > {lower_bound}
  AND {incremental_column} <= {upper_bound}
```

SQL max-bound file:

```sql
SELECT MAX({incremental_column}) AS max_value
FROM {source_table}
```

Behavior:

```text
Read last sequence value from Delta state
Read current MAX(sequence_column) from source
Extract bounded range: lower_bound < column <= upper_bound
Append to Bronze
Update state to upper_bound after successful write
```

Use this when:

```text
The table is insert-only
The sequence column is strictly increasing
Old records are not updated
Deletes are not important
```

Do not use this when:

```text
Old rows can be updated
Deletes must be captured
IDs are not monotonic
```

---

### 5.4 Incremental + Rowversion

Use this when SQL Server rowversion or equivalent change version is available.

Example:

```json
{
  "table_id": "products",
  "enabled": true,
  "load_type": "incremental",
  "strategy": "rowversion",
  "bronze_mode": "append",
  "source_table": "dbo.products",
  "primary_key": ["id"],
  "watermark": {
    "column": "row_version_bigint",
    "type": "long",
    "initial_value": 0,
    "upper_bound_mode": "max"
  },
  "sql": {
    "extract_ref": "configs/sql/jdbc/ecommerce_sqlserver/products_incremental_rowversion.sql",
    "max_bound_ref": "configs/sql/jdbc/ecommerce_sqlserver/products_max_rowversion.sql"
  }
}
```

SQL extraction file:

```sql
SELECT {columns},
       CONVERT(BIGINT, row_version) AS row_version_bigint
FROM {source_table}
WHERE CONVERT(BIGINT, row_version) > {lower_bound}
  AND CONVERT(BIGINT, row_version) <= {upper_bound}
```

SQL max-bound file:

```sql
SELECT MAX(CONVERT(BIGINT, row_version)) AS max_value
FROM {source_table}
```

Use this when:

```text
SQL Server rowversion exists
Update detection is required
Delete detection is not required or handled separately
```

---

## 6. CDC Load

CDC is not fully implemented yet in the JDBC runner.

Reserved structure:

```json
{
  "table_id": "orders",
  "enabled": true,
  "load_type": "cdc",
  "strategy": "sqlserver_cdc",
  "source_table": "dbo.orders",
  "primary_key": ["id"],
  "cdc": {
    "capture_instance": "dbo_orders",
    "initial_lsn": null
  }
}
```

Possible future strategies:

```text
sqlserver_cdc
sqlserver_change_tracking
postgres_logical_replication
mysql_binlog
debezium
```

Use CDC when:

```text
Inserts, updates, and deletes must be captured correctly
Source system supports CDC/log-based replication
Near-real-time or reliable change capture is required
```

---

## 7. SQL Template Placeholders

SQL templates can use the following placeholders:

| Placeholder | Meaning |
|---|---|
| `{columns}` | Quoted selected columns |
| `{source_table}` | Quoted source table |
| `{raw_source_table}` | Raw source table name |
| `{watermark_column}` | Quoted watermark column |
| `{raw_watermark_column}` | Raw watermark column name |
| `{incremental_column}` | Alias for quoted watermark/incremental column |
| `{raw_incremental_column}` | Alias for raw watermark/incremental column |
| `{lower_bound}` | Last persisted state value |
| `{upper_bound}` | Current upper bound for bounded strategies |
| `{load_type}` | Current load type |
| `{strategy}` | Current strategy |

Example:

```sql
SELECT {columns}
FROM {source_table}
WHERE {incremental_column} > {lower_bound}
```

---

## 8. State Management

Incremental state is stored as Delta under:

```text
s3a://lakehouse/_state/jdbc/{source_id}/{table_id}
```

The state key format is:

```text
{load_type}::{strategy}::{column}
```

Example:

```text
incremental::numeric_watermark::updated_at
incremental::sequence::id
incremental::rowversion::row_version_bigint
```

This prevents state conflicts when a table changes from one strategy to another.

State is updated only after a successful Bronze write.

---

## 9. Bronze Layer Rules

Bronze is the raw ingestion layer.

Recommended rules:

```text
1. Keep source columns as close to raw as possible.
2. Add ingestion metadata columns.
3. Do not perform business transformations in Bronze.
4. Use Silver for deduplication, cleansing, and current-state logic.
```

Current Bronze metadata columns:

| Column | Meaning |
|---|---|
| `_source_id` | Source identifier |
| `_table_id` | Table identifier |
| `_source_table` | Original source table |
| `_batch_run_id` | Batch run identifier |
| `_load_type` | Load type |
| `_strategy` | Strategy |
| `_ingestion_ts` | Ingestion timestamp |
| `ingest_year` | Partition year |
| `ingest_month` | Partition month |
| `ingest_day` | Partition day |
| `_row_hash` | Optional row hash |

---

## 10. Silver Layer Recommendation

Bronze should not be treated as clean current-state data.

Recommended Silver logic:

```text
1. Read Bronze table.
2. Deduplicate by primary_key.
3. Select latest record by _ingestion_ts or watermark column.
4. Apply type casting and cleaning.
5. Write Silver Delta table.
```

For `full + overwrite`:

```text
Silver can be overwritten from latest Bronze.
```

For `full + append_snapshot`:

```text
Silver should select the latest snapshot.
```

For `incremental`:

```text
Silver should merge/upsert using primary_key.
```

For CDC:

```text
Silver should apply insert/update/delete operations.
```

---

## 11. Common Configuration Patterns

### Small Lookup Table

```json
{
  "load_type": "full",
  "strategy": "overwrite",
  "bronze_mode": "overwrite"
}
```

### Full Snapshot History

```json
{
  "load_type": "full",
  "strategy": "append_snapshot",
  "bronze_mode": "append"
}
```

### Timestamp Incremental

```json
{
  "load_type": "incremental",
  "strategy": "timestamp",
  "watermark": {
    "column": "updated_at",
    "type": "timestamp",
    "initial_value": "1970-01-01T00:00:00"
  }
}
```

### Numeric Incremental

```json
{
  "load_type": "incremental",
  "strategy": "numeric_watermark",
  "watermark": {
    "column": "updated_at",
    "type": "long",
    "initial_value": 0
  }
}
```

### Sequence-Based Incremental

```json
{
  "load_type": "incremental",
  "strategy": "sequence",
  "watermark": {
    "column": "id",
    "type": "long",
    "initial_value": 0,
    "upper_bound_mode": "max"
  }
}
```

---

## 12. Troubleshooting

### Error: Invalid numeric SQL bound value

Example:

```text
Invalid numeric SQL bound value='1970-01-01T00:00:00' for value_type='long'
```

Cause:

```text
The persisted state contains a timestamp string, but the current watermark type is numeric.
```

Fix:

```text
1. Check manifest watermark.type and initial_value.
2. Delete old state path from MinIO.
3. Or change state_path_template temporarily.
```

State path example:

```text
_state/jdbc/ecommerce_sqlserver/products
```

---

### Error: Incorrect syntax near 'T00:'

Cause:

```text
A timestamp string was inserted into SQL without quotes,
usually because watermark.type was configured as numeric.
```

Fix:

```text
If the column is timestamp:
  watermark.type = timestamp

If the column is numeric:
  initial_value = 0
  and clear old state
```

---

### Error: Schema mismatch when writing Delta

Cause:

```text
Bronze schema changed after metadata columns were added.
```

Fix:

```text
For full overwrite:
  overwriteSchema=true

For append:
  mergeSchema=true
```

This is already handled by the writer.

---

## 13. Design Warnings

### Do not use sequence strategy for mutable tables

If old records can be updated, sequence-based ingestion will not detect those updates.

### Do not use timestamp strategy with unreliable timestamps

If timestamps are null, non-monotonic, or not updated on changes, data will be missed.

### Do not treat Bronze as final data

Bronze is raw ingestion history. Use Silver for current-state and deduplication.

### Do not update state before writing Bronze

State must be updated only after successful Bronze write.

---

## 14. Recommended Next Step

After this ingestion layer, implement a generic Bronze-to-Silver engine:

```text
Manifest-driven Bronze -> Silver
+ primary_key-based deduplication
+ latest-record selection
+ optional Delta MERGE
+ data quality checks
```

Recommended Silver table contract:

```json
{
  "table_id": "products",
  "source_bronze_path": "s3a://lakehouse/bronze/ecommerce_sqlserver/products",
  "target_silver_path": "s3a://lakehouse/silver/ecommerce/products",
  "primary_key": ["id"],
  "dedupe_order_by": ["_ingestion_ts"],
  "write_mode": "merge"
}
```

---

## 15. Summary

Use this final model:

```text
load_type = full | incremental | cdc
strategy  = overwrite | append_snapshot | timestamp | numeric_watermark | sequence | rowversion | ...
```

The engine should remain generic. Table-specific extraction logic belongs in SQL files. Operational control belongs in Python. Distributed read/write belongs to Spark and Delta.
