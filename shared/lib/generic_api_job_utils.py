from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import dayofmonth, hour, month, year
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("source_name", StringType(), False),
    StructField("symbol", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("source_event_ts", TimestampType(), True),
    StructField("ingestion_ts", TimestampType(), False),
    StructField("payload_json", StringType(), False),
    StructField("api_status", StringType(), False),
    StructField("price_usd", DoubleType(), True),
])


def build_runtime_context(job: dict[str, Any]) -> dict[str, Any]:
    spec = job["spec"]
    return {
        "job_name": job["name"],
        "source": spec["source"],
        "auth": spec["auth"],
        "request": spec["request"],
        "response": spec["response"],
        "validation": spec["validation"],
        "mapping": spec["mapping"],
        "bronze_write": spec["bronze_write"],
        "runtime_policy": spec["runtime_policy"],
    }


def resolve_secret(auth_spec: dict[str, Any]) -> str:
    env_name = auth_spec["secret_env"]
    value = os.getenv(env_name)
    if not value:
        raise ValueError(f"Required secret env '{env_name}' is not set")
    return value


def build_request_parts(runtime_ctx: dict[str, Any]) -> tuple[str, dict[str, Any], dict[str, str], Any, int]:
    source = runtime_ctx["source"]
    auth = runtime_ctx["auth"]
    request_spec = runtime_ctx["request"]

    url = source["base_url"]
    method = source.get("method", "GET").upper()
    timeout = int(source.get("timeout_seconds", 20))

    params = dict(request_spec.get("query_params", {}) or {})
    headers = dict(request_spec.get("headers", {}) or {})
    body = request_spec.get("body")

    secret_value = resolve_secret(auth)
    auth_type = auth["type"]

    if auth_type == "query_param":
        params[auth["param_name"]] = secret_value
    elif auth_type == "header_bearer":
        header_name = auth.get("header_name", "Authorization")
        headers[header_name] = f"Bearer {secret_value}"
    elif auth_type == "header_value":
        header_name = auth["header_name"]
        headers[header_name] = secret_value
    else:
        raise ValueError(f"Unsupported auth type: {auth_type}")

    return method, params, headers, body, timeout


def execute_api_request(runtime_ctx: dict[str, Any]) -> dict[str, Any]:
    source = runtime_ctx["source"]
    method, params, headers, body, timeout = build_request_parts(runtime_ctx)

    response = requests.request(
        method=method,
        url=source["base_url"],
        params=params,
        headers=headers,
        json=body,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def get_json_path(payload: dict[str, Any], path: str) -> Any:
    if path == "$":
        return payload

    if not path.startswith("$."):
        raise ValueError(f"Unsupported JSON path: {path}")

    current: Any = payload
    for part in path[2:].split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def validate_payload(payload: dict[str, Any], validation_spec: dict[str, Any]) -> None:
    for path in validation_spec.get("required_paths", []):
        value = get_json_path(payload, path)
        if value is None:
            raise ValueError(f"Missing required payload path: {path}")

    for rule in validation_spec.get("rules", []):
        rule_type = rule["type"]
        if rule_type == "equals":
            actual = get_json_path(payload, rule["path"])
            expected = rule["value"]
            if actual != expected:
                raise ValueError(
                    f"Validation failed for path '{rule['path']}': expected {expected}, got {actual}"
                )
        else:
            raise ValueError(f"Unsupported validation rule type: {rule_type}")


def map_field_value(payload: dict[str, Any], field_spec: dict[str, Any]) -> Any:
    kind = field_spec["kind"]
    path = field_spec["path"]
    raw_value = get_json_path(payload, path)

    if raw_value is None:
        return None

    if kind == "float":
        return float(raw_value)

    if kind == "unix_ts":
        return datetime.fromtimestamp(int(raw_value), tz=timezone.utc)

    if kind == "string":
        return str(raw_value)

    raise ValueError(f"Unsupported mapping kind: {kind}")


def map_payload_to_row(payload: dict[str, Any], runtime_ctx: dict[str, Any]) -> dict[str, Any]:
    mapping = runtime_ctx["mapping"]
    constants = mapping.get("constants", {})
    fields = mapping.get("fields", {})

    row: dict[str, Any] = {
        "event_id": build_event_id(payload, constants),
        "source_name": constants.get("source_name", "unknown"),
        "symbol": constants.get("symbol"),
        "currency": constants.get("currency"),
        "source_event_ts": None,
        "ingestion_ts": datetime.now(timezone.utc),
        "payload_json": json.dumps(payload, ensure_ascii=False),
        "api_status": "success",
        "price_usd": None,
    }

    for target_field, field_spec in fields.items():
        row[target_field] = map_field_value(payload, field_spec)

    return row


def build_event_id(payload: dict[str, Any], constants: dict[str, Any]) -> str:
    source_name = constants.get("source_name", "unknown")
    symbol = constants.get("symbol", "")
    ts_value = get_json_path(payload, "$.timestamp")
    price_value = get_json_path(payload, "$.rates.USD")
    return f"{source_name}|{symbol}|{ts_value}|{price_value}"


def build_bronze_dataframe(spark: SparkSession, row: dict[str, Any]) -> DataFrame:
    df = spark.createDataFrame([row], schema=BRONZE_SCHEMA)
    return (
        df.withColumn("ingest_year", year("ingestion_ts"))
          .withColumn("ingest_month", month("ingestion_ts"))
          .withColumn("ingest_day", dayofmonth("ingestion_ts"))
          .withColumn("ingest_hour", hour("ingestion_ts"))
    )


def write_bronze_dataframe(df: DataFrame, bronze_write_spec: dict[str, Any]) -> None:
    target_path = bronze_write_spec["target_path"]
    mode = bronze_write_spec.get("mode", "append")
    partition_by = bronze_write_spec.get("partition_by", [])

    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(target_path)