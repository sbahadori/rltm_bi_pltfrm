import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import dayofmonth, hour, month, year
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
)

SOURCE_NAME = "gold_api_com"
SYMBOL = "XAU"
CURRENCY = "USD"

SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("source_name", StringType(), False),
    StructField("source_event_ts", TimestampType(), True),
    StructField("ingestion_ts", TimestampType(), False),
    StructField("price_usd", DoubleType(), True),
    StructField("currency", StringType(), False),
    StructField("payload_json", StringType(), False),
    StructField("api_status", StringType(), False),
])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-path", default="s3a://lakehouse/bronze/gold_price_events")
    parser.add_argument("--api-url", default="https://api.gold-api.com/price/XAU")
    parser.add_argument("--poll-seconds", type=int, default=60)
    parser.add_argument("--request-timeout", type=int, default=15)
    parser.add_argument("--run-once", action="store_true")
    return parser.parse_args()


def build_spark():
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minio"))
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minio123"))
    region = os.getenv("AWS_REGION", "us-east-1")

    spark = (
        SparkSession.builder
        .appName("ingest_gold_api_to_bronze")
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
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def parse_ts(value):
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def fetch_row(api_url, timeout):
    r = requests.get(api_url, headers={"Accept": "application/json"}, timeout=timeout)
    r.raise_for_status()
    payload = r.json()

    if not isinstance(payload, dict):
        raise ValueError("API response must be a JSON object")

    source_event_ts = parse_ts(payload.get("updatedAt"))
    ingestion_ts = datetime.now(timezone.utc)
    price_usd = float(payload["price"]) if payload.get("price") is not None else None
    symbol = payload.get("symbol") or SYMBOL

    payload_json = json.dumps(
        {"provider": SOURCE_NAME, "raw_payload": payload},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )

    event_id = hashlib.sha256(
        f"{symbol}|{SOURCE_NAME}|{CURRENCY}|{source_event_ts}|{price_usd}|{payload_json}".encode("utf-8")
    ).hexdigest()

    return {
        "event_id": event_id,
        "symbol": symbol,
        "source_name": SOURCE_NAME,
        "source_event_ts": source_event_ts,
        "ingestion_ts": ingestion_ts,
        "price_usd": price_usd,
        "currency": CURRENCY,
        "payload_json": payload_json,
        "api_status": "OK",
    }


def write_row(spark, bronze_path, row):
    df = spark.createDataFrame([row], schema=SCHEMA)
    df = (
        df.withColumn("ingest_year", year("ingestion_ts"))
          .withColumn("ingest_month", month("ingestion_ts"))
          .withColumn("ingest_day", dayofmonth("ingestion_ts"))
          .withColumn("ingest_hour", hour("ingestion_ts"))
    )

    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("ingest_year", "ingest_month", "ingest_day", "ingest_hour")
        .save(bronze_path)
    )


def main():
    args = parse_args()
    spark = build_spark()

    try:
        while True:
            row = fetch_row(args.api_url, args.request_timeout)
            write_row(spark, args.bronze_path, row)
            print(f"Wrote 1 row to {args.bronze_path} | symbol={row['symbol']} | price_usd={row['price_usd']}")

            if args.run_once:
                break

            time.sleep(args.poll_seconds)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()