# How to Add a New Streaming Pipeline

This guide explains how to add a new streaming pipeline to the `new_architecture_v2` platform.

The streaming architecture is registry-driven:

```text
configs/streaming/stream_registry.json
        ↓
streaming/runners/stream_supervisor.py
        ↓
streaming/engines/generic_kafka_to_bronze.py
        ↓
streaming/engines/generic_bronze_to_silver.py
        ↓
Delta Lake on MinIO
```

The main configuration file is:

```text
configs/streaming/stream_registry.json
```

---

## 1. When to Use a Streaming Pipeline

Use a streaming pipeline when data arrives continuously or near-real-time, for example:

```text
clickstream events
web/app user actions
IoT events
POS events
backend event logs
Kafka topics
```

For REST APIs that are called periodically, use a batch API job instead.

For SQL Server/PostgreSQL/MySQL tables, use a JDBC database batch job instead.

---

## 2. Required Components

A streaming pipeline usually has two layers:

```text
Kafka → Bronze
Bronze → Silver
```

### Bronze

Bronze stores raw Kafka messages plus minimal metadata.

### Silver

Silver parses, validates, casts, filters, and cleans Bronze records.

---

## 3. Add a New Stream Entry

Open:

```text
configs/streaming/stream_registry.json
```

Add a new object inside the `streams` array.

Example:

```json
{
  "name": "orders_user_events",
  "enabled": true,
  "source": {
    "type": "kafka",
    "bootstrap_servers": "kafka:19092",
    "topic": "orders_events",
    "starting_offsets": "latest",
    "fail_on_data_loss": false,
    "startup_wait_seconds": 60,
    "metadata_retry_interval_seconds": 5
  },
  "bronze": {
    "engine": "generic_kafka_to_bronze",
    "app_name": "bronze_orders_user_events",
    "path": "s3a://lakehouse/bronze_delta/orders_events",
    "checkpoint_dir": "/tmp/checkpoints/bronze_orders_user_events",
    "heartbeat_file": "/tmp/health/bronze_orders_user_events.json",
    "trigger_interval": "15 seconds",
    "partition_by": [
      "event_date"
    ],
    "derived_fields": {
      "event_type": {
        "kind": "json",
        "path": "$.event_type"
      },
      "user_id": {
        "kind": "json",
        "path": "$.user_id"
      },
      "event_ts": {
        "kind": "json_timestamp",
        "path": "$.event_ts"
      },
      "event_date": {
        "kind": "sql",
        "expr": "coalesce(to_date(event_ts), to_date(kafka_timestamp))"
      }
    }
  },
  "silver": {
    "engine": "generic_bronze_to_silver",
    "app_name": "silver_orders_user_events",
    "bronze_path": "s3a://lakehouse/bronze_delta/orders_events",
    "path": "s3a://lakehouse/silver_delta/orders_events_clean",
    "quarantine_path": "s3a://lakehouse/silver_delta/orders_events_quarantine",
    "checkpoint_dir": "/tmp/checkpoints/silver_orders_user_events",
    "heartbeat_file": "/tmp/health/silver_orders_user_events.json",
    "trigger_interval": "30 seconds",
    "field_map": {
      "event_id": {
        "path": "$.event_id",
        "cast": "string"
      },
      "event_ts": {
        "path": "$.event_ts",
        "cast": "timestamp"
      },
      "event_type": {
        "path": "$.event_type",
        "cast": "string"
      },
      "user_id": {
        "path": "$.user_id",
        "cast": "string"
      },
      "session_id": {
        "path": "$.session_id",
        "cast": "string"
      },
      "order_id": {
        "path": "$.properties.order_id",
        "cast": "string"
      },
      "total_amount": {
        "path": "$.properties.total_amount",
        "cast": "double"
      }
    },
    "required_fields": [
      "event_type",
      "event_date"
    ],
    "identity_keys": [
      "event_id",
      "user_id",
      "session_id"
    ],
    "computed_fields": {
      "event_date": {
        "kind": "sql",
        "expr": "coalesce(to_date(event_ts), to_date(kafka_timestamp))"
      },
      "canonical_user_key": {
        "kind": "sql",
        "expr": "user_id"
      }
    },
    "transform_plugin": null
  },
  "restart_policy": {
    "max_retries": 10,
    "backoff_seconds": 10
  }
}
```

---

## 4. Field Reference

### Top-Level Fields

| Field | Required | Description |
|---|---:|---|
| `name` | Yes | Unique stream name |
| `enabled` | Yes | Enables/disables the stream |
| `source` | Yes | Kafka source configuration |
| `bronze` | Yes | Raw Kafka-to-Delta configuration |
| `silver` | Yes | Bronze-to-Silver cleaning configuration |
| `restart_policy` | Recommended | Supervisor retry policy |

---

## 5. Source Configuration

Example:

```json
"source": {
  "type": "kafka",
  "bootstrap_servers": "kafka:19092",
  "topic": "orders_events",
  "starting_offsets": "latest",
  "fail_on_data_loss": false,
  "startup_wait_seconds": 60,
  "metadata_retry_interval_seconds": 5
}
```

Recommended values:

| Field | Suggested Value |
|---|---|
| `bootstrap_servers` | `kafka:19092` inside Docker |
| `starting_offsets` | `latest` for normal runtime |
| `starting_offsets` | `earliest` only for replay/testing |
| `fail_on_data_loss` | `false` for local/dev |

---

## 6. Bronze Configuration

Bronze should keep data close to the Kafka source.

Example:

```json
"bronze": {
  "engine": "generic_kafka_to_bronze",
  "app_name": "bronze_orders_user_events",
  "path": "s3a://lakehouse/bronze_delta/orders_events",
  "checkpoint_dir": "/tmp/checkpoints/bronze_orders_user_events",
  "heartbeat_file": "/tmp/health/bronze_orders_user_events.json",
  "trigger_interval": "15 seconds",
  "partition_by": ["event_date"],
  "derived_fields": {
    "event_type": {
      "kind": "json",
      "path": "$.event_type"
    },
    "event_date": {
      "kind": "sql",
      "expr": "coalesce(to_date(event_ts), to_date(kafka_timestamp))"
    }
  }
}
```

Rules:

```text
1. Use a unique app_name.
2. Use a unique Delta path.
3. Use a unique checkpoint_dir.
4. Use a unique heartbeat_file.
5. Do not put business transformations in Bronze.
```

---

## 7. Silver Configuration

Silver should parse, cast, validate, and clean Bronze events.

Example:

```json
"silver": {
  "engine": "generic_bronze_to_silver",
  "app_name": "silver_orders_user_events",
  "bronze_path": "s3a://lakehouse/bronze_delta/orders_events",
  "path": "s3a://lakehouse/silver_delta/orders_events_clean",
  "quarantine_path": "s3a://lakehouse/silver_delta/orders_events_quarantine",
  "checkpoint_dir": "/tmp/checkpoints/silver_orders_user_events",
  "heartbeat_file": "/tmp/health/silver_orders_user_events.json",
  "trigger_interval": "30 seconds",
  "field_map": {
    "event_id": {
      "path": "$.event_id",
      "cast": "string"
    }
  },
  "required_fields": [
    "event_type",
    "event_date"
  ],
  "identity_keys": [
    "event_id",
    "user_id",
    "session_id"
  ],
  "computed_fields": {
    "event_date": {
      "kind": "sql",
      "expr": "coalesce(to_date(event_ts), to_date(kafka_timestamp))"
    }
  },
  "transform_plugin": null
}
```

Rules:

```text
1. Put parsing/casting in field_map.
2. Put mandatory fields in required_fields.
3. Put derived SQL expressions in computed_fields.
4. Send invalid records to quarantine_path.
```

---

## 8. Create Kafka Topic

If the topic is new, update the Kafka topic initialization logic or create it manually.

Manual example inside the Kafka container:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --create \
  --if-not-exists \
  --topic orders_events \
  --partitions 3 \
  --replication-factor 1
```

For local Docker Compose, replication factor should usually be `1`.

---

## 9. Restart Streaming Supervisor

After changing `stream_registry.json`, restart the stream supervisor:

```powershell
docker compose `
  -f compose/compose.phase1.yaml `
  -f compose/compose.phase2.yaml `
  -f compose/compose.simulator.yaml `
  up -d --force-recreate spark-stream-supervisor
```

---

## 10. Check Runtime Health

Check supervisor logs:

```powershell
docker logs spark-stream-supervisor --tail 200
```

Check heartbeat files:

```text
spark_health/
```

The dashboard should read stream status from the runtime health/status files.

---

## 11. Common Problems

### Topic does not exist

Symptoms:

```text
Stream starts but no data is read
Kafka metadata warnings
```

Fix:

```text
Create the Kafka topic or update the init topic service.
```

### Checkpoint conflict

Symptoms:

```text
Spark Structured Streaming fails after config changes
```

Fix:

```text
Use a new checkpoint_dir for a new stream or incompatible schema change.
```

### Schema parsing error

Symptoms:

```text
Records go to quarantine or fields are null
```

Fix:

```text
Check JSON paths in field_map and derived_fields.
```

---

## 12. Checklist

Before committing a new stream:

```text
[ ] Added new entry to configs/streaming/stream_registry.json
[ ] Used unique stream name
[ ] Used unique app_name values
[ ] Used unique checkpoint_dir values
[ ] Used unique heartbeat_file values
[ ] Used unique Bronze path
[ ] Used unique Silver path
[ ] Kafka topic exists
[ ] field_map paths are correct
[ ] required_fields are minimal and meaningful
[ ] Stream supervisor restarts successfully
[ ] Dashboard shows stream health
```
