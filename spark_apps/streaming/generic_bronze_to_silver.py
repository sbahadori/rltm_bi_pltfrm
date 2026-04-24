from __future__ import annotations

import atexit
import argparse
import importlib
import json
import logging
import os
import signal
import threading
import time
import sys
from pathlib import Path
from typing import Optional

def _bootstrap_repo_path() -> Path:
    repo_root = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    return repo_root

REPO_ROOT = _bootstrap_repo_path()

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, expr, get_json_object, lit, to_timestamp, when
from shared.lib.stream_spec_utils import get_stream_spec, validate_stream_spec


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


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", required=True)
    parser.add_argument("--stream-name", required=True)
    return parser.parse_args()


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
    }
    if extra:
        payload.update(extra)

    p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


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
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minio"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def s3_path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    j_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = j_path.getFileSystem(hadoop_conf)
    return fs.exists(j_path)


def apply_field_map(df: DataFrame, field_map: dict[str, str]) -> DataFrame:
    out = df
    for target_col, json_path in field_map.items():
        if target_col == "event_ts":
            out = out.withColumn(target_col, to_timestamp(get_json_object(col("raw_json"), json_path)))
        else:
            out = out.withColumn(target_col, get_json_object(col("raw_json"), json_path))
    return out


def apply_computed_fields(df: DataFrame, computed_fields: dict) -> DataFrame:
    out = df
    for field_name, spec in computed_fields.items():
        out = out.withColumn(field_name, expr(spec["expr"]))
    return out


def maybe_apply_plugin(df: DataFrame) -> DataFrame:
    plugin_path = _runtime.get("transform_plugin")
    if not plugin_path:
        return df

    module_name, func_name = plugin_path.split(":")
    module = importlib.import_module(module_name)
    fn = getattr(module, func_name)
    return fn(df)


def build_silver_transform(bronze_df: DataFrame) -> DataFrame:
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
    selected = base_cols + list(_runtime["field_map"].keys()) + list(_runtime["computed_fields"].keys())

    seen = set()
    ordered = []
    for c in selected:
        if c not in seen:
            seen.add(c)
            ordered.append(c)

    out = parsed.select(*ordered)
    return maybe_apply_plugin(out)


def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    required_fields = _runtime["required_fields"]
    identity_keys = _runtime["identity_keys"]

    valid_condition = None
    for field_name in required_fields:
        cond = col(field_name).isNotNull()
        valid_condition = cond if valid_condition is None else (valid_condition & cond)

    id_condition = None
    for field_name in identity_keys:
        cond = col(field_name).isNotNull()
        id_condition = cond if id_condition is None else (id_condition | cond)

    valid_condition = valid_condition & id_condition

    valid_df = df.filter(valid_condition)

    invalid_df = (
        df.filter(~valid_condition)
        .withColumn(
            "quarantine_reason",
            when(col("event_type").isNull(), lit("missing_event_type"))
            .when(col("event_date").isNull(), lit("missing_event_date"))
            .otherwise(lit("missing_identity_keys"))
        )
    )

    return valid_df, invalid_df


def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    try:
        if batch_df.isEmpty():
            write_heartbeat("running", {"last_batch_id": batch_id, "last_batch_rows": 0})
            return

        silver_df = build_silver_transform(batch_df)
        valid_df, invalid_df = split_valid_invalid(silver_df)

        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        total_count = valid_count + invalid_count

        if valid_count > 0:
            (
                valid_df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .partitionBy("event_date")
                .save(_runtime["silver_path"])
            )

        if invalid_count > 0:
            (
                invalid_df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(_runtime["quarantine_path"])
            )

        write_heartbeat(
            "running",
            {
                "last_batch_id": batch_id,
                "last_batch_rows": total_count,
                "last_valid_rows": valid_count,
                "last_invalid_rows": invalid_count,
                "last_write_ok": True,
            },
        )

    except Exception as exc:
        logger.exception("[silver] batch_id=%s write failed: %s", batch_id, exc)
        write_heartbeat("error", {"last_batch_id": batch_id, "last_error": str(exc)})
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
        _spark = build_spark_session(_runtime["app_name"])

        if not s3_path_exists(_spark, _runtime["bronze_path"]):
            raise FileNotFoundError(f"Bronze path does not exist: {_runtime['bronze_path']}")

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
        _query.awaitTermination()

    except Exception as exc:
        logger.exception("Generic silver stream failed: %s", exc)
        try:
            write_heartbeat("error", {"last_error": str(exc), "query_started": bool(_query is not None)})
        except Exception:
            pass
        raise

    finally:
        _stop_event.set()
        heartbeat_thread.join(timeout=2)
        stop_runtime()
        try:
            write_heartbeat("stopped")
        except Exception:
            pass


if __name__ == "__main__":
    main()