from __future__ import annotations

import atexit
import json
import logging
import os
import signal
import threading
import time
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    get_json_object,
    lit,
    to_date,
    to_timestamp,
    when,
)


APP_NAME = os.getenv("APP_NAME", "bronze_to_silver_events")

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minio")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://lakehouse/bronze_delta/user_events")
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://lakehouse/silver_delta/user_events_clean")
QUARANTINE_PATH = os.getenv("QUARANTINE_PATH", "s3a://lakehouse/silver_delta/user_events_quarantine")
CHECKPOINT_PATH = os.getenv(
    "CHECKPOINT_PATH",
    "/tmp/checkpoints/bronze_to_silver_events",
)

TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")

HEARTBEAT_FILE = os.getenv("HEARTBEAT_FILE", "/tmp/health/silver_events_heartbeat.txt")
HEARTBEAT_INTERVAL_SEC = int(os.getenv("HEARTBEAT_INTERVAL_SEC", "20"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(APP_NAME)

_stop_event = threading.Event()
_query: Optional[object] = None
_spark: Optional[SparkSession] = None


def write_heartbeat(status: str = "running", extra: Optional[dict] = None) -> None:
    p = Path(HEARTBEAT_FILE)
    p.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "app": APP_NAME,
        "status": status,
        "ts_epoch": int(time.time()),
        "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "bronze_path": BRONZE_PATH,
        "silver_path": SILVER_PATH,
        "quarantine_path": QUARANTINE_PATH,
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
        _stop_event.wait(HEARTBEAT_INTERVAL_SEC)


def stop_runtime() -> None:
    global _query, _spark

    _stop_event.set()

    try:
        if _query is not None and _query.isActive:
            _query.stop()
    except Exception as exc:
        logger.warning("Failed to stop streaming query cleanly: %s", exc)

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


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
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

def build_silver_transform(bronze_df: DataFrame) -> DataFrame:
    parsed = (
        bronze_df
        .withColumn("event_id", get_json_object(col("raw_json"), "$.event_id"))
        .withColumn("event_ts", to_timestamp(get_json_object(col("raw_json"), "$.event_ts")))
        .withColumn("event_type", get_json_object(col("raw_json"), "$.event_type"))
        .withColumn("source", get_json_object(col("raw_json"), "$.source"))
        .withColumn("user_id", get_json_object(col("raw_json"), "$.user_id"))
        .withColumn("anonymous_id", get_json_object(col("raw_json"), "$.anonymous_id"))
        .withColumn("session_id", get_json_object(col("raw_json"), "$.session_id"))
        .withColumn("page_url", get_json_object(col("raw_json"), "$.page_url"))
        .withColumn("item_id", get_json_object(col("raw_json"), "$.properties.item_id"))
        .withColumn("item_title", get_json_object(col("raw_json"), "$.properties.item_title"))
        .withColumn("backend_event_name", get_json_object(col("raw_json"), "$.properties.backend_event_name"))
        .withColumn("message", get_json_object(col("raw_json"), "$.properties.message"))
        .withColumn(
            "event_date",
            coalesce(to_date(col("event_ts")), to_date(col("kafka_timestamp"))),
        )
        .withColumn(
            "canonical_user_key",
            coalesce(col("user_id"), col("anonymous_id")),
        )
    )

    silver_df = parsed.select(
        "raw_json",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "bronze_ingest_ts",
        "event_id",
        "event_ts",
        "event_type",
        "source",
        "user_id",
        "anonymous_id",
        "session_id",
        "page_url",
        "event_date",
        "canonical_user_key",
        "item_id",
        "item_title",
        "backend_event_name",
        "message",
    )

    return silver_df


def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid_condition = (
        col("event_type").isNotNull()
        & col("event_date").isNotNull()
        & (
            col("event_id").isNotNull()
            | col("session_id").isNotNull()
            | col("anonymous_id").isNotNull()
            | col("user_id").isNotNull()
        )
    )

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
            logger.info("[silver] batch_id=%s empty", batch_id)
            write_heartbeat("running", {"last_batch_id": batch_id, "last_batch_rows": 0})
            return

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

        if valid_count > 0:
            (
                valid_df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .partitionBy("event_date")
                .save(SILVER_PATH)
            )
            logger.info("[silver] batch_id=%s wrote valid rows to %s", batch_id, SILVER_PATH)

        if invalid_count > 0:
            (
                invalid_df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(QUARANTINE_PATH)
            )
            logger.info("[silver] batch_id=%s wrote invalid rows to %s", batch_id, QUARANTINE_PATH)

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
        write_heartbeat(
            "error",
            {
                "last_batch_id": batch_id,
                "last_error": str(exc),
            },
        )
        raise


def main() -> None:
    global _query, _spark

    heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()

    try:
        write_heartbeat("starting")

        logger.info(
            "Starting silver stream bootstrap: bronze=%s silver=%s quarantine=%s checkpoint=%s",
            BRONZE_PATH,
            SILVER_PATH,
            QUARANTINE_PATH,
            CHECKPOINT_PATH,
        )

        _spark = build_spark_session()

        if not s3_path_exists(_spark, BRONZE_PATH):
            msg = f"Bronze path does not exist: {BRONZE_PATH}"
            logger.error(msg)
            write_heartbeat("error", {"last_error": msg, "query_started": False})
            raise FileNotFoundError(msg)

        bronze_stream_df = (
            _spark.readStream
            .format("delta")
            .load(BRONZE_PATH)
        )

        _query = (
            bronze_stream_df.writeStream
            .queryName(APP_NAME)
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_PATH)
            .trigger(processingTime=TRIGGER_INTERVAL)
            .foreachBatch(write_batch)
            .start()
        )

        logger.info(
            "Silver streaming query started successfully: bronze=%s silver=%s quarantine=%s checkpoint=%s",
            BRONZE_PATH,
            SILVER_PATH,
            QUARANTINE_PATH,
            CHECKPOINT_PATH,
        )

        write_heartbeat("running", {"query_started": True})
        _query.awaitTermination()

    except Exception as exc:
        logger.exception("Silver streaming app failed during startup/runtime: %s", exc)
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
            write_heartbeat("stopped")
        except Exception:
            pass


if __name__ == "__main__":
    main()