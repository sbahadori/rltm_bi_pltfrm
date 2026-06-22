from __future__ import annotations

import argparse
import base64
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    dayofmonth,
    expr,
    hour,
    lit,
    month,
    struct,
    to_json,
    year,
)
from pyspark.sql.types import StringType, TimestampType

# -----------------------------------------------------------------------------
# Bootstrap repo path
# -----------------------------------------------------------------------------

REPO_ROOT = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from shared.control.job_spec_store import (  # noqa: E402
    load_current_job_metadata,
    load_current_job_spec,
)
from shared.core.spark import create_spark  # noqa: E402
from shared.runtime.control_run_context import (  # noqa: E402
    build_runtime_context,
    control_run,
)


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog-path", required=True)
    parser.add_argument("--pipeline-name", required=True)
    parser.add_argument("--job-name", required=True)
    return parser.parse_args()


# -----------------------------------------------------------------------------
# Spark
# -----------------------------------------------------------------------------

def build_spark() -> SparkSession:
    return create_spark("generic_api_to_bronze")


# -----------------------------------------------------------------------------
# Spec loading
# -----------------------------------------------------------------------------

def load_job_spec(catalog_path: str, pipeline_name: str, job_name: str) -> dict[str, Any]:
    """
    Runtime source: PostgreSQL meta.job.config through CONTROL_JOB_CODE / CONTROL_JOB_KEY.

    Catalog JSON is design-time input only. Runtime runners must execute the
    onboarding-materialized projection in meta.job.config.
    """
    try:
        return load_current_job_spec()
    except Exception as exc:
        raise RuntimeError(
            "Could not load runtime job spec from meta.job.config. "
            f"pipeline={pipeline_name}, job={job_name}, catalog_path={catalog_path}. "
            "Publish the Catalog and run control-plane onboarding before executing "
            "this job."
        ) from exc


def load_job_metadata_safe() -> dict[str, Any]:
    try:
        return load_current_job_metadata()
    except Exception as exc:
        print(
            f"[WARN] Could not load job metadata from meta.job. error={exc}",
            flush=True,
        )
        return {}


# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def deep_get(payload: Any, path: str | None, default: Any = None) -> Any:
    """
    Supports dotted paths such as:
      data
      result.items
      payload.records
    """
    if not path:
        return payload

    current = payload

    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list):
            try:
                current = current[int(part)]
            except Exception:
                return default
        else:
            return default

        if current is None:
            return default

    return current


def extract_payload_by_path(payload: Any, path: str | None, default: Any = None) -> Any:
    if not path or path == "$":
        return payload

    normalized = path.strip()

    if normalized.startswith("$."):
        normalized = normalized[2:]

    if normalized == "$":
        return payload

    return deep_get(payload, normalized, default=default)


def normalize_records(data: Any) -> list[dict[str, Any]]:
    """
    Converts API response into a list of dictionaries.

    Supported:
      - list[dict]
      - dict
      - scalar payloads wrapped as {"value": scalar}
    """
    if data is None:
        return []

    if isinstance(data, list):
        records: list[dict[str, Any]] = []

        for item in data:
            if isinstance(item, dict):
                records.append(item)
            else:
                records.append({"value": item})

        return records

    if isinstance(data, dict):
        return [data]

    return [{"value": data}]


def records_from_response(
    *,
    request_payload: Any,
    response_spec: dict[str, Any],
    source_spec: dict[str, Any],
) -> tuple[Any, list[dict[str, Any]]]:
    record_mode = str(response_spec.get("record_mode", "")).lower().strip()

    records_path = (
        response_spec.get("records_path")
        or response_spec.get("data_path")
        or response_spec.get("root_path")
        or source_spec.get("records_path")
    )

    records_payload = extract_payload_by_path(
        request_payload,
        records_path,
        default=None,
    )

    if record_mode == "single_object":
        if records_payload is None:
            return records_payload, []
        if isinstance(records_payload, dict):
            return records_payload, [records_payload]
        return records_payload, [{"value": records_payload}]

    if record_mode in {"array", "list", "records", "records_array"}:
        if records_payload is None:
            return records_payload, []
        if not isinstance(records_payload, list):
            raise ValueError(
                f"record_mode={record_mode} expects a list at path={records_path}, "
                f"but got {type(records_payload).__name__}"
            )
        return records_payload, normalize_records(records_payload)

    return records_payload, normalize_records(records_payload)


def render_template(value: Any) -> Any:
    """
    Minimal env-template support:
      "${OPENWEATHER_API_KEY}"
      "https://api.example.com?q=${CITY}"
    """
    if not isinstance(value, str):
        return value

    result = value

    for key, env_value in os.environ.items():
        result = result.replace("${" + key + "}", env_value)

    return result


def render_dict(data: dict[str, Any] | None) -> dict[str, Any]:
    if not data:
        return {}

    return {
        str(k): render_template(v)
        for k, v in data.items()
        if v is not None
    }


def sanitize_record(record: dict[str, Any]) -> dict[str, Any]:
    """
    Spark can infer many primitive JSON values, but nested/irregular structures
    can cause unstable schemas. For Bronze, keep nested structures as JSON strings.
    """
    clean: dict[str, Any] = {}

    for key, value in record.items():
        safe_key = str(key)

        if isinstance(value, (dict, list)):
            clean[safe_key] = json.dumps(value, ensure_ascii=False, default=str)
        elif value is None:
            clean[safe_key] = None
        else:
            clean[safe_key] = value

    return clean


def ensure_non_empty_schema(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if records:
        return records

    return [
        {
            "_empty_response": True,
            "_ingested_at": utc_now_iso(),
        }
    ]


# -----------------------------------------------------------------------------
# API request handling
# -----------------------------------------------------------------------------

def build_url(source: dict[str, Any]) -> str:
    base_url = str(render_template(source.get("base_url", ""))).rstrip("/")
    endpoint = str(render_template(source.get("endpoint", ""))).lstrip("/")

    if base_url and endpoint:
        url = f"{base_url}/{endpoint}"
    elif base_url:
        url = base_url
    else:
        raise ValueError("API source.base_url is required")

    query_params = dict(render_dict(source.get("params") or source.get("query_params")))

    auth = source.get("auth") or {}
    auth_type = str(auth.get("type", "")).lower().strip()

    if auth_type == "query_param":
        secret_env = auth.get("secret_env") or auth.get("env_var")
        param_name = auth.get("param_name") or auth.get("name") or "api_key"

        secret_value = ""
        if secret_env:
            secret_value = os.getenv(str(secret_env), "")

        secret_value = secret_value or str(auth.get("value", ""))

        if not secret_value:
            raise ValueError(
                f"Query-param auth value is empty. "
                f"auth.secret_env={secret_env}, auth.param_name={param_name}"
            )

        query_params[str(param_name)] = secret_value

    if query_params:
        encoded = urllib.parse.urlencode(query_params, doseq=True)
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}{encoded}"

    return url


def build_headers(source: dict[str, Any]) -> dict[str, str]:
    headers = {
        str(k): str(v)
        for k, v in render_dict(source.get("headers")).items()
    }

    auth = source.get("auth") or {}
    auth_type = str(auth.get("type", "")).lower().strip()

    if auth_type == "api_key":
        env_var = auth.get("env_var")
        key_value = os.getenv(str(env_var), "") if env_var else str(auth.get("value", ""))

        if not key_value:
            raise ValueError(f"API key is empty. auth.env_var={env_var}")

        location = str(auth.get("location", "header")).lower()
        name = str(auth.get("name", "Authorization"))

        if location == "header":
            prefix = auth.get("prefix")
            headers[name] = f"{prefix} {key_value}" if prefix else key_value

        elif location == "query":
            raise ValueError(
                "auth.location='query' is not automatically injected. "
                "Use auth.type='query_param' or put the API key in source.params."
            )

        else:
            raise ValueError(f"Unsupported api_key auth.location: {location}")

    elif auth_type == "basic":
        username = render_template(auth.get("username", ""))
        password = render_template(auth.get("password", ""))

        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")
        headers["Authorization"] = f"Basic {token}"

    elif auth_type == "query_param":
        # Handled in build_url().
        pass

    elif auth_type == "bearer":
        secret_env = auth.get("secret_env") or auth.get("env_var")
        token = os.getenv(str(secret_env), "") if secret_env else str(auth.get("value", ""))

        if not token:
            raise ValueError(f"Bearer token is empty. auth.secret_env={secret_env}")

        headers["Authorization"] = f"Bearer {token}"

    elif auth_type == "header":
        secret_env = auth.get("secret_env") or auth.get("env_var")
        header_name = auth.get("header_name") or auth.get("name")

        if not header_name:
            raise ValueError("auth.header_name or auth.name is required for header auth")

        header_value = os.getenv(str(secret_env), "") if secret_env else str(auth.get("value", ""))

        if not header_value:
            raise ValueError(f"Header auth value is empty. auth.secret_env={secret_env}")

        headers[str(header_name)] = header_value

    elif auth_type in ("", "none"):
        pass

    else:
        raise ValueError(f"Unsupported auth.type: {auth_type}")

    headers.setdefault("Accept", "application/json")

    return headers


def execute_api_request(source: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
    method = str(source.get("method", "GET")).upper()
    timeout = int(source.get("timeout_seconds", 30))

    url = build_url(source)
    headers = build_headers(source)

    body_spec = source.get("body")
    data = None

    if body_spec is not None:
        body_payload = render_template(json.dumps(body_spec, default=str))
        data = body_payload.encode("utf-8")
        headers.setdefault("Content-Type", "application/json")

    request = urllib.request.Request(
        url=url,
        data=data,
        headers=headers,
        method=method,
    )

    started = time.time()

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw_body = response.read().decode("utf-8")
            elapsed = round(time.time() - started, 3)

            parsed = json.loads(raw_body) if raw_body else None

            meta = {
                "url": url,
                "method": method,
                "status_code": response.status,
                "elapsed_seconds": elapsed,
                "response_bytes": len(raw_body.encode("utf-8")),
            }

            return parsed, meta

    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"API HTTP error: status={exc.code}, url={url}, body={body[:1000]}"
        ) from exc

    except urllib.error.URLError as exc:
        host = urllib.parse.urlparse(url).hostname or ""
        if isinstance(exc.reason, socket.gaierror):
            raise RuntimeError(
                "API DNS resolution failed from the runner container. "
                f"host={host}, url={url}, error={exc.reason}. "
                "Check Docker/container DNS and outbound internet access."
            ) from exc

        raise RuntimeError(f"API URL error: url={url}, error={exc}") from exc


# -----------------------------------------------------------------------------
# DataFrame building
# -----------------------------------------------------------------------------

def records_to_dataframe(
    spark: SparkSession,
    records: list[dict[str, Any]],
    *,
    request_meta: dict[str, Any],
    source_name: str,
    raw_payload: Any | None = None,
    keep_raw_payload: bool = False,
) -> DataFrame:
    clean_records = [sanitize_record(r) for r in ensure_non_empty_schema(records)]

    raw_df = spark.createDataFrame(clean_records)

    payload_columns = raw_df.columns

    if payload_columns:
        payload_json_col = to_json(
            struct(*[col(c).alias(c) for c in payload_columns])
        )
    else:
        payload_json_col = lit("{}")

    status_code = request_meta.get("status_code")
    api_status = str(status_code) if status_code is not None else "unknown"

    df = (
        raw_df
        .withColumn("event_id", expr("uuid()"))
        .withColumn("source_name", lit(source_name))
        .withColumn("source_event_ts", lit(None).cast(TimestampType()))
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("payload_json", payload_json_col.cast(StringType()))
        .withColumn("api_status", lit(api_status))
        .withColumn("api_url", lit(str(request_meta.get("url", ""))))
        .withColumn("api_method", lit(str(request_meta.get("method", ""))))
        .withColumn("api_elapsed_seconds", lit(float(request_meta.get("elapsed_seconds", 0.0))))
        .withColumn("api_response_bytes", lit(int(request_meta.get("response_bytes", 0))))
        .withColumn("ingest_year", year(col("ingestion_ts")))
        .withColumn("ingest_month", month(col("ingestion_ts")))
        .withColumn("ingest_day", dayofmonth(col("ingestion_ts")))
        .withColumn("ingest_hour", hour(col("ingestion_ts")))
    )

    if keep_raw_payload:
        raw_payload_text = json.dumps(raw_payload, ensure_ascii=False, default=str)
        df = df.withColumn("raw_response_json", lit(raw_payload_text).cast(StringType()))

    selected_cols = [
        "event_id",
        "source_name",
        "source_event_ts",
        "ingestion_ts",
        "payload_json",
        "api_status",
        "api_url",
        "api_method",
        "api_elapsed_seconds",
        "api_response_bytes",
    ]

    if keep_raw_payload:
        selected_cols.append("raw_response_json")

    selected_cols.extend(
        [
            "ingest_year",
            "ingest_month",
            "ingest_day",
            "ingest_hour",
        ]
    )

    return df.select(*selected_cols)


def write_bronze(df: DataFrame, write_spec: dict[str, Any]) -> None:
    target_path = write_spec.get("target_path") or write_spec.get("path")

    if not target_path:
        raise ValueError("bronze_write.target_path or target.path is required")

    output_format = write_spec.get("format", "delta")
    mode = write_spec.get("mode", "append")
    partition_by = write_spec.get("partition_by", [])

    missing_partitions = [
        c for c in partition_by
        if c not in df.columns
    ]

    if missing_partitions:
        raise ValueError(
            f"Partition columns are missing from API Bronze DataFrame: "
            f"{missing_partitions}. Available columns: {df.columns}"
        )

    writer = df.write.format(output_format).mode(mode)

    if output_format.lower() == "delta":
        writer = writer.option("mergeSchema", "true")

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(target_path)


# -----------------------------------------------------------------------------
# Spec normalization
# -----------------------------------------------------------------------------

def get_source_spec(spec: dict[str, Any]) -> dict[str, Any]:
    source = spec.get("source") or spec.get("api") or {}

    if not isinstance(source, dict):
        raise ValueError("spec.source must be a JSON object")

    return source


def build_effective_source_spec(spec: dict[str, Any]) -> dict[str, Any]:
    """
    Merge catalog-level source/request/auth into the shape expected by
    execute_api_request().

    Catalog contract:
      spec.source  -> base_url, method, timeout_seconds
      spec.request -> query_params, headers, body
      spec.auth    -> auth config

    Runner contract:
      source -> base_url, method, timeout_seconds, query_params, headers, body, auth
    """
    source = dict(get_source_spec(spec))

    request = spec.get("request") or {}
    if not isinstance(request, dict):
        raise ValueError("spec.request must be a JSON object when provided")

    auth = spec.get("auth") or {}
    if auth and not isinstance(auth, dict):
        raise ValueError("spec.auth must be a JSON object when provided")

    if "query_params" not in source and request.get("query_params") is not None:
        source["query_params"] = request.get("query_params") or {}

    if "params" not in source and request.get("params") is not None:
        source["params"] = request.get("params") or {}

    if "headers" not in source and request.get("headers") is not None:
        source["headers"] = request.get("headers") or {}

    if "body" not in source and "body" in request:
        source["body"] = request.get("body")

    if "auth" not in source and auth:
        source["auth"] = auth

    return source


def get_response_spec(spec: dict[str, Any]) -> dict[str, Any]:
    response = spec.get("response") or {}

    if not isinstance(response, dict):
        raise ValueError("spec.response must be a JSON object")

    return response


def get_write_spec(spec: dict[str, Any]) -> dict[str, Any]:
    write_spec = (
        spec.get("bronze_write")
        or spec.get("target")
        or spec.get("write")
        or {}
    )

    if not isinstance(write_spec, dict):
        raise ValueError("spec.bronze_write/target/write must be a JSON object")

    return write_spec


def get_source_name(spec: dict[str, Any], metadata: dict[str, Any]) -> str:
    source = get_source_spec(spec)

    return (
        source.get("name")
        or metadata.get("entity_name")
        or metadata.get("job_code")
        or "api_source"
    )


def get_target_path_from_spec(spec: dict[str, Any], metadata: dict[str, Any]) -> str:
    write_spec = get_write_spec(spec)

    return (
        write_spec.get("target_path")
        or write_spec.get("path")
        or metadata.get("target_path")
        or ""
    )


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    metadata = load_job_metadata_safe()

    context = build_runtime_context(
        default_pipeline_name=args.pipeline_name,
        default_job_name=args.job_name,
        default_job_code=f"bronze.{args.pipeline_name}.{args.job_name}",
        default_base_job_name=args.job_name,
        default_layer="bronze",
        default_runner="generic_api_to_bronze",
        default_target_path=metadata.get("target_path"),
        default_entity_name=metadata.get("entity_name"),
    )

    spark: SparkSession | None = None

    with control_run(context=context) as run_ctx:
        try:
            spark = build_spark()

            spec = load_job_spec(
                args.catalog_path,
                args.pipeline_name,
                args.job_name,
            )

            source = build_effective_source_spec(spec)
            response = get_response_spec(spec)
            write_spec = get_write_spec(spec)

            target_path = get_target_path_from_spec(spec, metadata)
            if target_path:
                write_spec = {**write_spec, "target_path": target_path}
                run_ctx["target_path"] = target_path

            request_payload, request_meta = execute_api_request(source)

            _records_payload, records = records_from_response(
                request_payload=request_payload,
                response_spec=response,
                source_spec=source,
            )

            print(
                f"[API_BRONZE_EXTRACT] "
                f"job_code={context.get('job_code')} "
                f"url={request_meta.get('url')} "
                f"status={request_meta.get('status_code')} "
                f"record_mode={response.get('record_mode')} "
                f"root_path={response.get('root_path')} "
                f"payload_type={type(request_payload).__name__} "
                f"records={len(records)}",
                flush=True,
            )

            keep_raw_payload = bool(
                response.get("keep_raw_payload", False)
                or write_spec.get("include_raw_payload", False)
            )

            source_name = get_source_name(spec, metadata)

            if not records:
                run_ctx["records_read"] = 0
                run_ctx["records_written"] = 0
                run_ctx["records_inserted"] = 0
                run_ctx["records_updated"] = 0
                run_ctx["records_deleted"] = 0
                run_ctx["target_path"] = write_spec.get("target_path") or write_spec.get("path")

                runtime_policy = (
                    context.get("runtime_policy")
                    or spec.get("runtime_policy")
                    or {}
                )

                fail_on_empty = runtime_policy.get("fail_on_empty", True)

                if isinstance(fail_on_empty, str):
                    fail_on_empty = fail_on_empty.strip().lower() in {"1", "true", "yes", "y"}

                msg = (
                    f"[API_BRONZE_EMPTY] "
                    f"job_code={context.get('job_code')} "
                    f"record_mode={response.get('record_mode')} "
                    f"root_path={response.get('root_path')} "
                    f"target={run_ctx.get('target_path')}. "
                    f"No records were produced from the API response."
                )

                print(msg, flush=True)

                if fail_on_empty:
                    raise RuntimeError(msg)

                return

            df = records_to_dataframe(
                spark,
                records,
                request_meta=request_meta,
                source_name=source_name,
                raw_payload=request_payload,
                keep_raw_payload=keep_raw_payload,
            )

            records_read = len(records)
            records_written = int(df.count())

            write_bronze(df, write_spec)

            run_ctx["records_read"] = records_read
            run_ctx["records_written"] = records_written
            run_ctx["records_inserted"] = records_written
            run_ctx["records_updated"] = 0
            run_ctx["records_deleted"] = 0
            run_ctx["target_path"] = write_spec.get("target_path") or write_spec.get("path")

            print(
                f"[API_BRONZE_WRITE_OK] "
                f"job_code={context.get('job_code')} "
                f"read={records_read} "
                f"written={records_written} "
                f"inserted={records_written} "
                f"updated=0 deleted=0 "
                f"target={run_ctx.get('target_path')}",
                flush=True,
            )

        finally:
            if spark is not None:
                spark.stop()


if __name__ == "__main__":
    main()
