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


def fetch_row(api_url: str, request_timeout: int, max_retries: int = 3):
    last_exc = None

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(api_url, timeout=request_timeout)

            if r.status_code == 429:
                wait_sec = 5 * attempt
                print(f"Rate limited by API (429). Retry {attempt}/{max_retries} after {wait_sec}s")
                time.sleep(wait_sec)
                last_exc = RuntimeError("429 Too Many Requests")
                continue

            r.raise_for_status()
            return r.json()

        except requests.RequestException as exc:
            last_exc = exc
            wait_sec = 5 * attempt
            print(f"Request failed on attempt {attempt}/{max_retries}: {exc}")
            if attempt < max_retries:
                time.sleep(wait_sec)

    print(f"No data fetched after {max_retries} attempts. Last error: {last_exc}")
    return None

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

            if row is None:
                print("Skipping bronze ingest because source API is temporarily unavailable or rate-limited.")
                return

            write_row(spark, args.bronze_path, row)
            print(f"Wrote 1 row to {args.bronze_path} | symbol={row['symbol']} | price_usd={row['price_usd']}")

            if args.run_once:
                break

            time.sleep(args.poll_seconds)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()