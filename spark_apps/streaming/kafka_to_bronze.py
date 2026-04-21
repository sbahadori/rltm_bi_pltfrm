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
from pyspark.sql.functions import col, current_timestamp, expr, get_json_object


APP_NAME = os.getenv("APP_NAME", "kafka_to_bronze_user_events_delta")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
EVENT_TOPIC = os.getenv("EVENT_TOPIC", "user_events")

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minio")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://lakehouse/bronze_delta/user_events")
CHECKPOINT_PATH = os.getenv(
    "CHECKPOINT_PATH",
    "/tmp/checkpoints/kafka_to_bronze_user_events_delta",
)

TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "15 seconds")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")
FAIL_ON_DATA_LOSS = os.getenv("FAIL_ON_DATA_LOSS", "false").lower()

HEARTBEAT_FILE = os.getenv("HEARTBEAT_FILE", "/tmp/health/bronze_heartbeat.txt")
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
        "topic": EVENT_TOPIC,
        "bronze_path": BRONZE_PATH,
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


def build_bronze_df(spark: SparkSession) -> DataFrame:
    raw_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", EVENT_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", FAIL_ON_DATA_LOSS)
        .load()
    )

    return (
        raw_kafka
        .selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "CAST(value AS STRING) AS raw_json",
            "topic AS kafka_topic",
            "partition AS kafka_partition",
            "offset AS kafka_offset",
            "timestamp AS kafka_timestamp",
            "timestampType AS kafka_timestamp_type",
        )
        .withColumn("bronze_ingest_ts", current_timestamp())
        .withColumn("event_type", get_json_object(col("raw_json"), "$.event_type"))
        .withColumn("anonymous_id", get_json_object(col("raw_json"), "$.anonymous_id"))
        .withColumn("session_id", get_json_object(col("raw_json"), "$.session_id"))
        .withColumn("event_ts", expr("to_timestamp(get_json_object(raw_json, '$.event_ts'))"))
        .withColumn("event_date", expr("coalesce(to_date(event_ts), to_date(kafka_timestamp))"))
    )


def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    try:
        if batch_df.isEmpty():
            logger.info("[bronze] batch_id=%s empty", batch_id)
            write_heartbeat("running", {"last_batch_id": batch_id, "last_batch_rows": 0})
            return

        row_count = batch_df.count()
        logger.info("[bronze] batch_id=%s row_count=%s writing to %s", batch_id, row_count, BRONZE_PATH)

        (
            batch_df.write
            .format("delta")
            .mode("append")
            .partitionBy("event_date")
            .save(BRONZE_PATH)
        )

        write_heartbeat(
            "running",
            {
                "last_batch_id": batch_id,
                "last_batch_rows": row_count,
                "last_write_ok": True,
            },
        )
        logger.info("[bronze] batch_id=%s write complete", batch_id)

    except Exception as exc:
        logger.exception("[bronze] batch_id=%s write failed: %s", batch_id, exc)
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
            "Starting stream bootstrap: topic=%s, bootstrap=%s, bronze=%s, checkpoint=%s",
            EVENT_TOPIC,
            KAFKA_BOOTSTRAP_SERVERS,
            BRONZE_PATH,
            CHECKPOINT_PATH,
        )

        _spark = build_spark_session()
        bronze_df = build_bronze_df(_spark)

        _query = (
            bronze_df.writeStream
            .queryName(APP_NAME)
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_PATH)
            .trigger(processingTime=TRIGGER_INTERVAL)
            .foreachBatch(write_batch)
            .start()
        )

        logger.info(
            "Streaming query started successfully: topic=%s, bronze=%s, checkpoint=%s",
            EVENT_TOPIC,
            BRONZE_PATH,
            CHECKPOINT_PATH,
        )

        write_heartbeat("running", {"query_started": True})
        _query.awaitTermination()

    except Exception as exc:
        logger.exception("Bronze streaming app failed during startup/runtime: %s", exc)
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