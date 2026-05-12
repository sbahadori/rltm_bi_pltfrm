from __future__ import annotations

import argparse
import atexit
import importlib
import json
import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, expr, get_json_object, lit, to_timestamp, when
from shared.core.spark import create_spark

def _bootstrap_repo_path() -> Path:
    repo_root = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    return repo_root


REPO_ROOT = _bootstrap_repo_path()

from streaming.specs.stream_spec_utils import (  # noqa: E402
    get_stream_spec,
    validate_stream_spec,
)


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger("generic_bronze_to_silver")

_stop_event = threading.Event()
_query: Optional[object] = None
_spark: Optional[SparkSession] = None
_runtime: dict = {}
_last_batch_state: dict = {}
_last_error_state: dict = {}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", required=True)
    parser.add_argument("--stream-name", required=True)
    return parser.parse_args()

def _utc_now_fields(prefix: str = "last_batch") -> dict:
    now_epoch = int(time.time())
    return {
        f"{prefix}_ts_epoch": now_epoch,
        f"{prefix}_ts_iso": time.strftime(
            "%Y-%m-%dT%H:%M:%SZ",
            time.gmtime(now_epoch),
        ),
    }

def write_heartbeat(status: str = "running", extra: Optional[dict] = None) -> None:
    p = Path(_runtime["heartbeat_file"])
    p.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "app": _runtime["app_name"],
        "stream_name": _runtime["stream_name"],
        "status": status,
        "ts_epoch": int(time.time()),
        "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "bronze_path": _runtime["bronze_path"],
        "silver_path": _runtime["silver_path"],
        "quarantine_path": _runtime["quarantine_path"],
        "checkpoint_path": _runtime["checkpoint_path"],
        "query_started": bool(_query is not None),
    }

    # Preserve last batch/error state so heartbeat_loop does not overwrite useful debug info.
    payload.update(_last_batch_state)
    payload.update(_last_error_state)

    if extra:
        payload.update(extra)

    p.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def heartbeat_loop() -> None:
    while not _stop_event.is_set():
        try:
            write_heartbeat("running")
        except Exception as exc:
            logger.warning("Failed to write heartbeat: %s", exc)
        _stop_event.wait(20)


def stop_runtime() -> None:
    global _query, _spark

    _stop_event.set()

    try:
        if _query is not None and _query.isActive:
            _query.stop()
    except Exception as exc:
        logger.warning("Failed to stop query cleanly: %s", exc)

    try:
        if _spark is not None:
            _spark.stop()
    except Exception as exc:
        logger.warning("Failed to stop Spark cleanly: %s", exc)


def shutdown_handler(*_args) -> None:
    logger.info("Shutdown requested")

    try:
        write_heartbeat("stopping")
    except Exception:
        pass

    stop_runtime()


atexit.register(shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)


def build_spark_session(app_name: str) -> SparkSession:
    return create_spark(app_name, log_level="WARN")


def s3_path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    j_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = j_path.getFileSystem(hadoop_conf)
    return fs.exists(j_path)


def normalize_field_rule(rule) -> dict:
    if isinstance(rule, str):
        return {
            "path": rule,
            "cast": None,
        }

    if isinstance(rule, dict):
        if "path" not in rule:
            raise ValueError(f"field_map rule object must contain 'path': {rule}")
        return {
            "path": rule["path"],
            "cast": rule.get("cast"),
        }

    raise ValueError(f"Unsupported field_map rule: {rule}")


def apply_cast(column_expr, cast_type: str | None):
    if not cast_type:
        return column_expr

    cast_type = cast_type.lower()

    if cast_type in {"string", "str"}:
        return column_expr.cast("string")

    if cast_type in {"int", "integer"}:
        return column_expr.cast("int")

    if cast_type in {"long", "bigint"}:
        return column_expr.cast("long")

    if cast_type in {"double", "float"}:
        return column_expr.cast("double")

    if cast_type in {"boolean", "bool"}:
        return column_expr.cast("boolean")

    if cast_type == "timestamp":
        return to_timestamp(column_expr)

    if cast_type == "date":
        return expr(f"to_date({column_expr._jc.toString()})")

    raise ValueError(f"Unsupported cast type in field_map: {cast_type}")


def apply_field_map(df: DataFrame, field_map: dict) -> DataFrame:
    out = df

    for target_col, raw_rule in field_map.items():
        rule = normalize_field_rule(raw_rule)

        json_path = rule["path"]
        cast_type = rule["cast"]

        base_col = get_json_object(col("raw_json"), json_path)

        if target_col == "event_ts" and cast_type is None:
            mapped_col = to_timestamp(base_col)
        else:
            mapped_col = apply_cast(base_col, cast_type)

        out = out.withColumn(target_col, mapped_col)

    return out

def apply_computed_fields(df: DataFrame, computed_fields: dict) -> DataFrame:
    out = df

    for field_name, spec in computed_fields.items():
        kind = spec.get("kind", "sql")

        if kind != "sql":
            raise ValueError(
                f"Unsupported computed field kind for '{field_name}': {kind}"
            )

        out = out.withColumn(field_name, expr(spec["expr"]))

    return out


def maybe_apply_plugin(df: DataFrame) -> DataFrame:
    plugin_path = _runtime.get("transform_plugin")

    if not plugin_path:
        return df

    if ":" not in plugin_path:
        raise ValueError(
            f"Invalid transform_plugin '{plugin_path}'. "
            "Expected format: 'module:function'"
        )

    module_name, func_name = plugin_path.split(":", 1)

    logger.info("[silver] applying transform_plugin=%s", plugin_path)

    module = importlib.import_module(module_name)
    fn = getattr(module, func_name)

    return fn(df)


def validate_columns_exist(df: DataFrame, columns: list[str], context: str) -> None:
    missing = [c for c in columns if c not in df.columns]

    if missing:
        raise ValueError(
            f"Missing columns during {context}: {missing}. "
            f"Available columns: {df.columns}"
        )


def build_silver_transform(bronze_df: DataFrame) -> DataFrame:
    validate_columns_exist(
        bronze_df,
        [
            "raw_json",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "bronze_ingest_ts",
        ],
        context="silver transform input validation",
    )

    parsed = apply_field_map(bronze_df, _runtime["field_map"])
    parsed = apply_computed_fields(parsed, _runtime["computed_fields"])

    base_cols = [
        "raw_json",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "bronze_ingest_ts",
    ]

    selected = (
        base_cols
        + list(_runtime["field_map"].keys())
        + list(_runtime["computed_fields"].keys())
    )

    seen = set()
    ordered = []

    for c in selected:
        if c not in seen:
            seen.add(c)
            ordered.append(c)

    validate_columns_exist(
        parsed,
        ordered,
        context="silver selected columns validation",
    )

    out = parsed.select(*ordered)
    return maybe_apply_plugin(out)


def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    required_fields = _runtime["required_fields"]
    identity_keys = _runtime["identity_keys"]

    validate_columns_exist(
        df,
        required_fields + identity_keys,
        context="silver quality validation",
    )

    required_condition = lit(True)

    for field_name in required_fields:
        required_condition = required_condition & col(field_name).isNotNull()

    if identity_keys:
        identity_condition = lit(False)
        for field_name in identity_keys:
            identity_condition = identity_condition | col(field_name).isNotNull()
    else:
        identity_condition = lit(True)

    valid_condition = required_condition & identity_condition

    valid_df = df.filter(valid_condition)

    reason_expr = lit("unknown_quality_failure")

    for field_name in reversed(required_fields):
        reason_expr = when(
            col(field_name).isNull(),
            lit(f"missing_{field_name}"),
        ).otherwise(reason_expr)

    if identity_keys:
        identity_missing_condition = lit(True)

        for field_name in identity_keys:
            identity_missing_condition = (
                identity_missing_condition & col(field_name).isNull()
            )

        reason_expr = when(
            identity_missing_condition,
            lit("missing_identity_keys"),
        ).otherwise(reason_expr)

    invalid_df = (
        df.filter(~valid_condition)
        .withColumn("quarantine_reason", reason_expr)
    )

    return valid_df, invalid_df


def write_delta_if_not_empty(
    df: DataFrame,
    path: str,
    partition_by: list[str] | None = None,
) -> int:
    row_count = df.count()

    if row_count <= 0:
        return 0

    writer = (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
    )

    if partition_by:
        validate_columns_exist(
            df,
            partition_by,
            context=f"partition validation for {path}",
        )
        writer = writer.partitionBy(*partition_by)

    writer.save(path)
    return row_count


def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    global _last_batch_state, _last_error_state

    try:
        if batch_df.isEmpty():
            _last_batch_state = {
            "last_batch_id": batch_id,
            **_utc_now_fields("last_batch"),
            "last_input_rows": input_count,
            "last_batch_rows": total_count,
            "last_valid_rows": valid_count,
            "last_invalid_rows": invalid_count,
            "last_written_valid_rows": written_valid,
            "last_written_invalid_rows": written_invalid,
            "last_write_ok": True,
            "last_message": "write_ok",
            }
            write_heartbeat("running", _last_batch_state)
            logger.info("[silver] batch_id=%s empty batch", batch_id)
            return

        input_count = batch_df.count()

        logger.info("[silver] batch_id=%s input_count=%s", batch_id, input_count)

        silver_df = build_silver_transform(batch_df)
        valid_df, invalid_df = split_valid_invalid(silver_df)

        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        total_count = valid_count + invalid_count

        logger.info(
            "[silver] batch_id=%s total=%s valid=%s invalid=%s",
            batch_id,
            total_count,
            valid_count,
            invalid_count,
        )

        written_valid = 0
        written_invalid = 0

        if valid_count > 0:
            logger.info(
                "[silver] writing valid rows to %s",
                _runtime["silver_path"],
            )
            written_valid = write_delta_if_not_empty(
                valid_df,
                _runtime["silver_path"],
                partition_by=["event_date"],
            )
        else:
            logger.warning(
                "[silver] batch_id=%s no valid rows; clean silver path will not be written",
                batch_id,
            )

        if invalid_count > 0:
            logger.info(
                "[silver] writing invalid rows to %s",
                _runtime["quarantine_path"],
            )
            written_invalid = write_delta_if_not_empty(
                invalid_df,
                _runtime["quarantine_path"],
                partition_by=None,
            )

        _last_error_state = {}
        _last_batch_state = {
            "last_batch_id": batch_id,
            **_utc_now_fields("last_batch"),
            "last_input_rows": input_count,
            "last_batch_rows": total_count,
            "last_valid_rows": valid_count,
            "last_invalid_rows": invalid_count,
            "last_written_valid_rows": written_valid,
            "last_written_invalid_rows": written_invalid,
            "last_write_ok": True,
            "last_message": "write_ok",
        }


        write_heartbeat("running", _last_batch_state)

    except Exception as exc:
        logger.exception("[silver] batch_id=%s write failed: %s", batch_id, exc)

        _last_error_state = {
            "last_batch_id": batch_id,
            **_utc_now_fields("last_error"),
            "last_write_ok": False,
            "last_error": str(exc),
        }

        write_heartbeat("error", _last_error_state)
        raise


def main() -> None:
    global _query, _spark, _runtime

    args = parse_args()
    spec = get_stream_spec(args.registry, args.stream_name)
    validate_stream_spec(spec)

    silver = spec["silver"]

    _runtime = {
        "stream_name": spec["name"],
        "app_name": silver["app_name"],
        "bronze_path": silver["bronze_path"],
        "silver_path": silver["path"],
        "quarantine_path": silver["quarantine_path"],
        "checkpoint_path": silver["checkpoint_dir"],
        "heartbeat_file": silver["heartbeat_file"],
        "trigger_interval": silver.get("trigger_interval", "30 seconds"),
        "field_map": silver.get("field_map", {}),
        "computed_fields": silver.get("computed_fields", {}),
        "required_fields": silver.get("required_fields", []),
        "identity_keys": silver.get("identity_keys", []),
        "transform_plugin": silver.get("transform_plugin"),
    }

    heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()

    try:
        write_heartbeat("starting")

        logger.info(
            "[silver] starting app=%s bronze_path=%s silver_path=%s quarantine_path=%s checkpoint=%s",
            _runtime["app_name"],
            _runtime["bronze_path"],
            _runtime["silver_path"],
            _runtime["quarantine_path"],
            _runtime["checkpoint_path"],
        )

        _spark = build_spark_session(_runtime["app_name"])

        if not s3_path_exists(_spark, _runtime["bronze_path"]):
            raise FileNotFoundError(
                f"Bronze path does not exist: {_runtime['bronze_path']}. "
                "Start bronze stream first and send at least one event."
            )

        bronze_stream_df = (
            _spark.readStream
            .format("delta")
            .load(_runtime["bronze_path"])
        )

        _query = (
            bronze_stream_df.writeStream
            .queryName(_runtime["app_name"])
            .outputMode("append")
            .option("checkpointLocation", _runtime["checkpoint_path"])
            .trigger(processingTime=_runtime["trigger_interval"])
            .foreachBatch(write_batch)
            .start()
        )

        write_heartbeat("running", {"query_started": True})

        logger.info("[silver] query started")
        _query.awaitTermination()

    except Exception as exc:
        logger.exception("Generic silver stream failed: %s", exc)

        try:
            write_heartbeat(
                "error",
                {
                    "last_error": str(exc),
                    "query_started": bool(_query is not None),
                },
            )
        except Exception:
            pass

        raise

    finally:
        _stop_event.set()
        heartbeat_thread.join(timeout=2)
        stop_runtime()

        try:
            # Do not hide last_error/last_batch because write_heartbeat preserves states.
            write_heartbeat("stopped")
        except Exception:
            pass


if __name__ == "__main__":
    main()