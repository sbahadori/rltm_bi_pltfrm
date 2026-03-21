import argparse
import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import dayofmonth, hour, month, year
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

try:
    from dateutil import parser as dtparser
except Exception:
    dtparser = None


LOG = logging.getLogger("ingest_to_bronze")


BRONZE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("source_name", StringType(), False),
        StructField("source_event_ts", TimestampType(), True),
        StructField("ingestion_ts", TimestampType(), False),
        StructField("price_usd", DoubleType(), True),
        StructField("currency", StringType(), False),
        StructField("payload_json", StringType(), False),
        StructField("api_status", StringType(), False),
    ]
)


def build_spark(app_name: str) -> SparkSession:
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minio"))
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minio123"))
    region = os.getenv("AWS_REGION", "us-east-1")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https://")).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint.region", region)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Poll gold-api.com and append raw price events to Bronze Delta."
    )
    parser.add_argument("--api-url", default="https://api.gold-api.com/price/XAU")
    parser.add_argument("--symbol", default="XAU")
    parser.add_argument("--currency", default="USD")
    parser.add_argument("--source-name", default="gold_api_com")
    parser.add_argument("--bronze-path", default="s3a://lakehouse/bronze/gold_price_events")
    parser.add_argument("--poll-seconds", type=int, default=60)
    parser.add_argument("--request-timeout", type=int, default=15)
    parser.add_argument("--run-once", action="store_true")
    return parser.parse_args()


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_event_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
    elif isinstance(value, str):
        txt = value.strip()
        if not txt:
            return None

        if txt.isdigit():
            dt = datetime.fromtimestamp(float(txt), tz=timezone.utc)
        else:
            if dtparser is not None:
                dt = dtparser.parse(txt)
            else:
                dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
    else:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def fetch_gold_api_payload(api_url: str, timeout: int) -> Dict[str, Any]:
    response = requests.get(
        api_url,
        headers={"Accept": "application/json"},
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()

    if not isinstance(payload, dict):
        raise ValueError("gold-api.com response is not a JSON object")

    return payload


def build_event_row(
    payload: Dict[str, Any],
    symbol: str,
    currency: str,
    source_name: str,
) -> Dict[str, Any]:
    ingestion_ts = datetime.now(timezone.utc)

    provider_symbol = payload.get("symbol") or symbol
    source_event_ts = parse_event_ts(payload.get("updatedAt"))
    price_usd = safe_float(payload.get("price"))

    payload_json = json.dumps(
        {
            "provider": source_name,
            "raw_payload": payload,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )

    event_id_seed = "|".join(
        [
            str(provider_symbol),
            str(source_name),
            str(currency),
            str(source_event_ts.isoformat() if source_event_ts else payload.get("updatedAt")),
            str(price_usd),
            payload_json,
        ]
    )
    event_id = hashlib.sha256(event_id_seed.encode("utf-8")).hexdigest()

    return {
        "event_id": event_id,
        "symbol": provider_symbol,
        "source_name": source_name,
        "source_event_ts": source_event_ts,
        "ingestion_ts": ingestion_ts,
        "price_usd": price_usd,
        "currency": currency,
        "payload_json": payload_json,
        "api_status": "OK",
    }


def build_error_row(args: argparse.Namespace, err: Exception) -> Dict[str, Any]:
    ingestion_ts = datetime.now(timezone.utc)

    payload_json = json.dumps(
        {
            "provider": args.source_name,
            "api_url": args.api_url,
            "symbol": args.symbol,
            "currency": args.currency,
            "error": str(err),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )

    event_id = hashlib.sha256(
        f"{args.source_name}|{args.symbol}|{args.currency}|{ingestion_ts.isoformat()}|{payload_json}".encode("utf-8")
    ).hexdigest()

    return {
        "event_id": event_id,
        "symbol": args.symbol,
        "source_name": args.source_name,
        "source_event_ts": None,
        "ingestion_ts": ingestion_ts,
        "price_usd": None,
        "currency": args.currency,
        "payload_json": payload_json,
        "api_status": "HTTP_ERROR",
    }


def write_bronze(spark: SparkSession, bronze_path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)

    out_df = (
        df.withColumn("ingest_year", year("ingestion_ts"))
          .withColumn("ingest_month", month("ingestion_ts"))
          .withColumn("ingest_day", dayofmonth("ingestion_ts"))
          .withColumn("ingest_hour", hour("ingestion_ts"))
    )

    (
        out_df.write.format("delta")
        .mode("append")
        .partitionBy("ingest_year", "ingest_month", "ingest_day", "ingest_hour")
        .save(bronze_path)
    )


def run_once(spark: SparkSession, args: argparse.Namespace) -> int:
    try:
        payload = fetch_gold_api_payload(args.api_url, args.request_timeout)
        row = build_event_row(
            payload=payload,
            symbol=args.symbol,
            currency=args.currency,
            source_name=args.source_name,
        )
        write_bronze(spark, args.bronze_path, [row])

        LOG.info(
            "Wrote 1 Bronze row to %s | source=%s | symbol=%s | price_usd=%s",
            args.bronze_path,
            row["source_name"],
            row["symbol"],
            row["price_usd"],
        )
        return 0

    except Exception as exc:
        LOG.exception("Ingestion failed: %s", exc)

        try:
            error_row = build_error_row(args, exc)
            write_bronze(spark, args.bronze_path, [error_row])
        except Exception:
            LOG.exception("Failed to write error row to Bronze")

        return 1


def run_loop(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        stream=sys.stdout,
    )

    spark = build_spark("ingest_gold_api_to_bronze")

    try:
        while True:
            exit_code = run_once(spark, args)

            if args.run_once:
                return exit_code

            time.sleep(args.poll_seconds)

    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(run_loop(parse_args()))