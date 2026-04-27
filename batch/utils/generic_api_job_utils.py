from __future__ import annotations

import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, dayofmonth, hour, month, year
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


SUPPORTED_TYPES = {
    "string",
    "timestamp",
    "double",
    "float",
    "int",
    "integer",
    "long",
    "bigint",
    "boolean",
    "date",
}

TYPE_ALIASES = {
    "integer": "int",
    "bigint": "long",
}

SPARK_TYPE_MAP = {
    "string": StringType(),
    "timestamp": TimestampType(),
    "double": DoubleType(),
    "float": FloatType(),
    "int": IntegerType(),
    "long": LongType(),
    "boolean": BooleanType(),
    "date": DateType(),
}


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def parse_schema(schema: Any) -> list[dict[str, Any]]:
    if isinstance(schema, list):
        raw_fields = [str(x).strip().rstrip(",") for x in schema if str(x).strip()]
    elif isinstance(schema, str):
        raw_fields = [x.strip() for x in schema.replace("\n", " ").split(",") if x.strip()]
    else:
        raise ValueError("schema must be either a string or a list of strings")

    fields: list[dict[str, Any]] = []
    seen: set[str] = set()

    for raw in raw_fields:
        parts = raw.split()

        if len(parts) == 3:
            name, dtype, null_token = parts

            if null_token.lower() != "null":
                raise ValueError(f"Invalid nullable syntax: {raw}")

            nullable = True

        elif len(parts) == 4:
            name, dtype, not_token, null_token = parts

            if not_token.lower() != "not" or null_token.lower() != "null":
                raise ValueError(f"Invalid not-null syntax: {raw}")

            nullable = False

        else:
            raise ValueError(
                f"Invalid schema field: {raw}. "
                "Expected '<name> <type> null' or '<name> <type> not null'."
            )

        dtype = dtype.lower()

        if dtype not in SUPPORTED_TYPES:
            raise ValueError(
                f"Unsupported type '{dtype}' in field '{name}'. "
                f"Supported types: {sorted(SUPPORTED_TYPES)}"
            )

        dtype = TYPE_ALIASES.get(dtype, dtype)

        if name in seen:
            raise ValueError(f"Duplicate field in schema: {name}")

        seen.add(name)

        fields.append(
            {
                "name": name,
                "type": dtype,
                "nullable": nullable,
            }
        )

    return fields


def to_spark_schema(schema: Any) -> StructType:
    fields = parse_schema(schema)

    return StructType(
        [
            StructField(
                f["name"],
                SPARK_TYPE_MAP[f["type"]],
                f["nullable"],
            )
            for f in fields
        ]
    )


def build_runtime_context(job: dict[str, Any]) -> dict[str, Any]:
    spec = job["spec"]

    return {
        "job_name": job["name"],
        "source": spec["source"],
        "auth": spec.get("auth", {"type": "none"}),
        "request": spec.get("request", {}),
        "response": spec.get(
            "response",
            {
                "format": "json",
                "root_path": "$",
                "record_mode": "single_object",
            },
        ),
        "validation": spec.get("validation", {}),
        "schema": spec["schema"],
        "mapping": spec["mapping"],
        "bronze_write": spec["bronze_write"],
        "runtime_policy": spec.get("runtime_policy", {}),
    }


def resolve_secret(auth_spec: dict[str, Any]) -> str | None:
    auth_type = auth_spec.get("type", "none")

    if auth_type == "none":
        return None

    env_name = auth_spec.get("secret_env")

    if not env_name:
        raise ValueError("auth.secret_env is required when auth.type is not none")

    value = os.getenv(env_name)

    if not value:
        raise ValueError(f"Required secret env '{env_name}' is not set")

    return value


def build_request_parts(
    runtime_ctx: dict[str, Any],
) -> tuple[str, str, dict[str, Any], dict[str, str], Any, int]:
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
    auth_type = auth.get("type", "none")

    if auth_type == "none":
        pass

    elif auth_type == "query_param":
        param_name = auth.get("param_name")
        if not param_name:
            raise ValueError("auth.param_name is required for query_param auth")
        params[param_name] = secret_value

    elif auth_type == "bearer":
        headers["Authorization"] = f"Bearer {secret_value}"

    elif auth_type == "header_bearer":
        header_name = auth.get("header_name", "Authorization")
        headers[header_name] = f"Bearer {secret_value}"

    elif auth_type == "header":
        header_name = auth.get("header_name")
        if not header_name:
            raise ValueError("auth.header_name is required for header auth")
        headers[header_name] = secret_value

    elif auth_type == "header_value":
        header_name = auth.get("header_name")
        if not header_name:
            raise ValueError("auth.header_name is required for header_value auth")
        headers[header_name] = secret_value

    else:
        raise ValueError(f"Unsupported auth type: {auth_type}")

    return method, url, params, headers, body, timeout


def execute_api_request(runtime_ctx: dict[str, Any]) -> dict[str, Any]:
    method, url, params, headers, body, timeout = build_request_parts(runtime_ctx)

    runtime_policy = runtime_ctx.get("runtime_policy", {})
    max_retries = int(runtime_policy.get("max_retries", 0))
    backoff_seconds = int(runtime_policy.get("backoff_seconds", 5))

    last_error: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            response = requests.request(
                method=method,
                url=url,
                params=params,
                headers=headers,
                json=body if isinstance(body, dict) else None,
                data=body if isinstance(body, str) else None,
                timeout=timeout,
            )

            response.raise_for_status()

            response_format = runtime_ctx.get("response", {}).get("format", "json")

            if response_format != "json":
                raise ValueError(f"Unsupported response format: {response_format}")

            payload = response.json()

            if not isinstance(payload, dict):
                raise ValueError("API response payload must be a JSON object")

            return payload

        except Exception as exc:
            last_error = exc

            if attempt < max_retries:
                time.sleep(backoff_seconds)

    raise RuntimeError(f"API call failed after {max_retries + 1} attempts: {last_error}")


def get_json_path(payload: Any, path: str) -> Any:
    if path == "$":
        return payload

    if not path.startswith("$."):
        raise ValueError(f"Only JSONPath starting with '$.' is supported: {path}")

    current = payload

    for token in path[2:].split("."):
        if current is None:
            return None

        match = re.match(
            r"^(?P<key>[A-Za-z_][A-Za-z0-9_-]*)(\[(?P<idx>\d+)\])?$",
            token,
        )

        if not match:
            raise ValueError(f"Unsupported JSONPath token: {token}")

        key = match.group("key")
        idx = match.group("idx")

        if not isinstance(current, dict):
            return None

        current = current.get(key)

        if idx is not None:
            if not isinstance(current, list):
                return None

            i = int(idx)

            if i >= len(current):
                return None

            current = current[i]

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
                    f"Validation failed for path '{rule['path']}': "
                    f"expected {expected!r}, got {actual!r}"
                )

        else:
            raise ValueError(f"Unsupported validation rule type: {rule_type}")


def cast_value(value: Any, cast_type: str | None) -> Any:
    if value is None:
        return None

    if not cast_type:
        return value

    cast_type = cast_type.lower()

    if cast_type in {"string", "str"}:
        return str(value)

    if cast_type in {"double", "float"}:
        return float(value)

    if cast_type in {"int", "integer"}:
        return int(value)

    if cast_type in {"long", "bigint"}:
        return int(value)

    if cast_type in {"boolean", "bool"}:
        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            return value.strip().lower() in {"true", "1", "yes", "y"}

        return bool(value)

    if cast_type == "unix_ts":
        return datetime.fromtimestamp(float(value), tz=timezone.utc).replace(tzinfo=None)

    if cast_type == "timestamp":
        if isinstance(value, datetime):
            if value.tzinfo:
                return value.astimezone(timezone.utc).replace(tzinfo=None)
            return value

        text = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(text).astimezone(timezone.utc).replace(tzinfo=None)

    raise ValueError(f"Unsupported cast type: {cast_type}")


def resolve_mapping_value(
    payload: dict[str, Any],
    mapping_rule: dict[str, Any],
    raw_payload_json: str,
    runtime_values: dict[str, Any],
) -> Any:
    source = mapping_rule.get("source")
    cast_type = mapping_rule.get("cast")

    if source == "constant":
        return cast_value(mapping_rule.get("value"), cast_type)

    if source == "json":
        path = mapping_rule.get("path")

        if not path:
            raise ValueError(f"JSON mapping requires path: {mapping_rule}")

        return cast_value(get_json_path(payload, path), cast_type)

    if source == "runtime":
        value_name = mapping_rule.get("value")

        if value_name == "current_utc":
            return runtime_values["current_utc"]

        if value_name == "raw_payload_json":
            return raw_payload_json

        if value_name in runtime_values:
            return runtime_values[value_name]

        raise ValueError(f"Unsupported runtime value: {value_name}")

    raise ValueError(f"Unsupported mapping source: {source}")


def build_event_id(row: dict[str, Any], payload: dict[str, Any]) -> str:
    identity = {
        "source_name": row.get("source_name"),
        "source_event_ts": row.get("source_event_ts"),
        "payload": payload,
    }

    raw = canonical_json(identity)

    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def map_payload_to_row(payload: dict[str, Any], runtime_ctx: dict[str, Any]) -> dict[str, Any]:
    schema = runtime_ctx["schema"]
    mapping = runtime_ctx["mapping"]

    if not isinstance(mapping, dict):
        raise ValueError("mapping must be an object/dict")

    schema_fields = parse_schema(schema)
    schema_field_names = {f["name"] for f in schema_fields}

    unknown_mapping_fields = set(mapping.keys()) - schema_field_names

    if unknown_mapping_fields:
        raise ValueError(
            f"Mapping contains fields not defined in schema: {sorted(unknown_mapping_fields)}"
        )

    raw_payload_json = canonical_json(payload)

    runtime_values = {
        "current_utc": utc_now_naive(),
    }

    row: dict[str, Any] = {}

    for field in schema_fields:
        field_name = field["name"]

        if field_name in mapping:
            row[field_name] = resolve_mapping_value(
                payload=payload,
                mapping_rule=mapping[field_name],
                raw_payload_json=raw_payload_json,
                runtime_values=runtime_values,
            )
        else:
            row[field_name] = None

    if "event_id" in schema_field_names and not row.get("event_id"):
        row["event_id"] = build_event_id(row=row, payload=payload)

    missing_required = [
        f["name"]
        for f in schema_fields
        if not f["nullable"] and row.get(f["name"]) is None
    ]

    if missing_required:
        raise ValueError(f"Required fields are null after mapping: {missing_required}")

    return row


def build_bronze_dataframe(
    spark: SparkSession,
    row: dict[str, Any],
    schema: Any | None = None,
) -> DataFrame:
    if schema is not None:
        spark_schema = to_spark_schema(schema)
        df = spark.createDataFrame([row], schema=spark_schema)
    else:
        df = spark.createDataFrame([row])

    return df


def add_ingest_partitions(df: DataFrame, partition_by: list[str]) -> DataFrame:
    if not partition_by:
        return df

    if "ingestion_ts" not in df.columns:
        return df

    result = df

    if "ingest_year" in partition_by and "ingest_year" not in result.columns:
        result = result.withColumn("ingest_year", year(col("ingestion_ts")))

    if "ingest_month" in partition_by and "ingest_month" not in result.columns:
        result = result.withColumn("ingest_month", month(col("ingestion_ts")))

    if "ingest_day" in partition_by and "ingest_day" not in result.columns:
        result = result.withColumn("ingest_day", dayofmonth(col("ingestion_ts")))

    if "ingest_hour" in partition_by and "ingest_hour" not in result.columns:
        result = result.withColumn("ingest_hour", hour(col("ingestion_ts")))

    return result


def write_bronze_dataframe(df: DataFrame, bronze_write_spec: dict[str, Any]) -> None:
    target_path = bronze_write_spec["target_path"]
    mode = bronze_write_spec.get("mode", "append")
    partition_by = bronze_write_spec.get("partition_by", [])

    df = add_ingest_partitions(df, partition_by)

    writer = df.write.format("delta").mode(mode)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(target_path)