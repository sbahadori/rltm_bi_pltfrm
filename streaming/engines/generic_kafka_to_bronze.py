from __future__ import annotations

import atexit
import argparse
import json
import logging
import os
import signal
import threading
import time
import sys
from pathlib import Path
from typing import Optional
from pyspark.sql.utils import StreamingQueryException
from shared.core.spark import create_spark

from shared.runtime.stream_runtime_db import (
    insert_stream_batch_metric,
    upsert_stream_unit_current_from_heartbeat,
)

_last_db_current_ts_epoch: int = 0

def _bootstrap_repo_path() -> Path:
    repo_root = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    return repo_root

REPO_ROOT = _bootstrap_repo_path()

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, get_json_object
from streaming.specs.stream_spec_utils import get_stream_spec, validate_stream_spec

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("generic_kafka_to_bronze")

_stop_event = threading.Event()
_query: Optional[object] = None
_spark: Optional[SparkSession] = None
_runtime: dict = {}
_last_batch_state: dict = {}
_last_error_state: dict = {}

def wait_for_kafka_topic(spark: SparkSession) -> None:
    wait_seconds = int(_runtime.get("startup_wait_seconds", 60))
    retry_interval = int(_runtime.get("metadata_retry_interval_seconds", 5))
    deadline = time.time() + wait_seconds
    last_error = None

    while time.time() < deadline:
        try:
            df = (
                spark.read
                .format("kafka")
                .option("kafka.bootstrap.servers", _runtime["bootstrap_servers"])
                .option("subscribe", _runtime["topic"])
                .option("startingOffsets", "earliest")
                .load()
            )
            df.limit(0).collect()
            write_heartbeat("running", {"topic_ready": True})
            return
        except Exception as exc:
            last_error = str(exc)
            write_heartbeat(
                "waiting_for_topic",
                {
                    "topic_ready": False,
                    "last_error": last_error,
                },
            )
            time.sleep(retry_interval)

    raise RuntimeError(
        f"Kafka topic '{_runtime['topic']}' not ready after {wait_seconds}s. Last error: {last_error}"
    )

def parse_args():
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
        "run_id": _runtime.get("run_id"),
        "status": status,
        "ts_epoch": int(time.time()),
        "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "topic": _runtime["topic"],
        "bronze_path": _runtime["bronze_path"],
        "checkpoint_path": _runtime["checkpoint_path"],
        "query_started": bool(_query is not None),
    }

    # Preserve useful stream progress across heartbeat_loop updates.
    payload.update(_last_batch_state)
    payload.update(_last_error_state)

    if extra:
        payload.update(extra)

    p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
   
    global _last_db_current_ts_epoch

    now_epoch = int(payload["ts_epoch"])
    db_interval = int(_runtime.get("db_current_interval_seconds", 30))

    force_db_write = status in {
        "starting",
        "waiting_for_topic",
        "error",
        "stopping",
        "stopped",
    }

    if force_db_write or now_epoch - _last_db_current_ts_epoch >= db_interval:
        _last_db_current_ts_epoch = now_epoch
        upsert_stream_unit_current_from_heartbeat(
            unit_name=_runtime["unit_name"],
            stream_name=_runtime["stream_name"],
            layer=_runtime["layer"],
            heartbeat=payload,
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



def apply_derived_fields(df: DataFrame, derived_fields: dict) -> DataFrame:
    out = df
    for field_name, spec in derived_fields.items():
        kind = spec["kind"]
        if kind == "json":
            out = out.withColumn(field_name, get_json_object(col("raw_json"), spec["path"]))
        elif kind == "json_timestamp":
            out = out.withColumn(field_name, expr(f"to_timestamp(get_json_object(raw_json, '{spec['path']}'))"))
        elif kind == "sql":
            out = out.withColumn(field_name, expr(spec["expr"]))
        else:
            raise ValueError(f"Unsupported derived field kind: {kind}")
    return out


def build_bronze_df(spark: SparkSession) -> DataFrame:
    raw_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", _runtime["bootstrap_servers"])
        .option("subscribe", _runtime["topic"])
        .option("startingOffsets", _runtime["starting_offsets"])
        .option("failOnDataLoss", _runtime["fail_on_data_loss"])
        .load()
    )

    base = (
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
    )

    return apply_derived_fields(base, _runtime["derived_fields"])

def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    global _last_batch_state, _last_error_state

    try:
        row_count = batch_df.count()

        if row_count == 0:
            _last_error_state = {}
            _last_batch_state = {
                "last_batch_id": batch_id,
                **_utc_now_fields("last_batch"),
                "last_input_rows": 0,
                "last_batch_rows": 0,
                "last_written_rows": 0,
                "last_write_ok": True,
                "last_message": "empty_batch",
            }

            insert_stream_batch_metric(
                unit_name=_runtime["unit_name"],
                stream_name=_runtime["stream_name"],
                layer=_runtime["layer"],
                batch_id=batch_id,
                input_rows=0,
                batch_rows=0,
                written_rows=0,
                write_ok=True,
                message="empty_batch",
                target_path=_runtime["bronze_path"],
                checkpoint_path=_runtime["checkpoint_path"],
                batch_ts_epoch=_last_batch_state.get("last_batch_ts_epoch"),
                payload=_last_batch_state,
            )

            write_heartbeat("running", _last_batch_state)
            return

        writer = batch_df.write.format("delta").mode("append")
        partition_cols = _runtime["partition_by"]

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.save(_runtime["bronze_path"])

        _last_error_state = {}
        _last_batch_state = {
            "last_batch_id": batch_id,
            **_utc_now_fields("last_batch"),
            "last_input_rows": row_count,
            "last_batch_rows": row_count,
            "last_written_rows": row_count,
            "last_write_ok": True,
            "last_message": "write_ok",
        }

        insert_stream_batch_metric(
            unit_name=_runtime["unit_name"],
            stream_name=_runtime["stream_name"],
            layer=_runtime["layer"],
            batch_id=batch_id,
            input_rows=row_count,
            batch_rows=row_count,
            written_rows=row_count,
            write_ok=True,
            message="write_ok",
            target_path=_runtime["bronze_path"],
            checkpoint_path=_runtime["checkpoint_path"],
            batch_ts_epoch=_last_batch_state.get("last_batch_ts_epoch"),
            payload=_last_batch_state,
        )

        write_heartbeat("running", _last_batch_state)

    except Exception as exc:
        logger.exception("[bronze] batch_id=%s write failed: %s", batch_id, exc)

        _last_error_state = {
            "last_batch_id": batch_id,
            **_utc_now_fields("last_error"),
            "last_write_ok": False,
            "last_error": str(exc),
        }

        insert_stream_batch_metric(
            unit_name=_runtime["unit_name"],
            stream_name=_runtime["stream_name"],
            layer=_runtime["layer"],
            batch_id=batch_id,
            write_ok=False,
            message="write_failed",
            error_message=str(exc),
            target_path=_runtime["bronze_path"],
            checkpoint_path=_runtime["checkpoint_path"],
            batch_ts_epoch=_last_error_state.get("last_error_ts_epoch"),
            payload=_last_error_state,
        )
        
        write_heartbeat("error", _last_error_state)
        raise

def main() -> None:
    global _query, _spark, _runtime

    args = parse_args()
    spec = get_stream_spec(args.registry, args.stream_name)
    validate_stream_spec(spec)

    bronze = spec["bronze"]
    source = spec["source"]

    _runtime = {
        "stream_name": spec["name"],
        "app_name": bronze["app_name"],
        "bootstrap_servers": source["bootstrap_servers"],
        "topic": source["topic"],
        "starting_offsets": source.get("starting_offsets", "latest"),
        "fail_on_data_loss": str(source.get("fail_on_data_loss", False)).lower(),
        "bronze_path": bronze["path"],
        "checkpoint_path": bronze["checkpoint_dir"],
        "heartbeat_file": bronze["heartbeat_file"],
        "trigger_interval": bronze.get("trigger_interval", "15 seconds"),
        "partition_by": bronze.get("partition_by", []),
        "derived_fields": bronze.get("derived_fields", {}),
        "startup_wait_seconds": source.get("startup_wait_seconds", 60),
        "metadata_retry_interval_seconds": source.get("metadata_retry_interval_seconds", 5),
        "layer": "bronze",
        "unit_name": f"{spec['name']}_bronze",
        "run_id": os.getenv("STREAM_RUN_ID"),
        "db_current_interval_seconds": int(os.getenv("STREAM_DB_CURRENT_INTERVAL_SECONDS", "30")),
    }

    heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()

    try:
        write_heartbeat("starting")
        _spark = build_spark_session(_runtime["app_name"])
        wait_for_kafka_topic(_spark)
        bronze_df = build_bronze_df(_spark)

        _query = (
            bronze_df.writeStream
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
        logger.exception("Generic bronze stream failed: %s", exc)
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
