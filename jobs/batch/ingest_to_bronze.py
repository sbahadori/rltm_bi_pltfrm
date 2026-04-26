import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pyspark import SparkContext
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import dayofmonth, hour, month, year
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType
)

SOURCE_NAME = "metalpriceapi"
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
    parser.add_argument("--api-base-url", default="https://api.metalpriceapi.com/v1/latest")
    parser.add_argument("--request-timeout", type=int, default=20)
    parser.add_argument("--run-once", action="store_true")
    return parser.parse_args()


def build_spark():
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "minio")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")
    region = os.getenv("AWS_REGION", "us-east-1")

    # پاک کردن session/context قبلی اگر stop شده یا stale مانده باشد
    try:
        active_session = SparkSession.getActiveSession()
        if active_session is not None:
            try:
                active_session.stop()
            except Exception:
                pass
    except Exception:
        pass

    SparkSession._instantiatedSession = None
    SparkContext._active_spark_context = None

    spark = (
        SparkSession.builder
        .appName("gold_price_ingest_to_bronze")
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


def build_api_url(api_base_url: str) -> str:
    api_key = os.getenv("METALPRICE_API_KEY")
    if not api_key:
        raise ValueError("METALPRICE_API_KEY is not set")

    return f"{api_base_url}?api_key={api_key}&base=XAU&currencies=USD"


def fetch_payload(api_url: str, timeout: int) -> dict | None:
    try:
        r = requests.get(api_url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"API request failed: {e}")
        return None


def validate_payload(payload: dict) -> None:
    if not isinstance(payload, dict):
        raise ValueError("API payload is not a JSON object")

    if not payload.get("success", False):
        raise ValueError(f"API returned error payload: {json.dumps(payload, ensure_ascii=False)}")

    if "timestamp" not in payload:
        raise ValueError(f"API payload has no 'timestamp'. Payload keys: {list(payload.keys())}")

    if "rates" not in payload:
        raise ValueError(f"API payload has no 'rates'. Payload keys: {list(payload.keys())}")

    if "USD" not in payload["rates"]:
        raise ValueError(f"API payload has no 'rates[\"USD\"]'. Payload keys: {list(payload['rates'].keys())}")


def extract_price_usd(payload: dict) -> float:
    return float(payload["rates"]["USD"])


def build_row(payload: dict) -> dict:
    ts_value = payload["timestamp"]
    source_event_ts = datetime.fromtimestamp(int(ts_value), tz=timezone.utc)
    ingestion_ts = datetime.now(timezone.utc)
    price_usd = extract_price_usd(payload)

    raw_key = f"{SOURCE_NAME}|{SYMBOL}|{ts_value}|{price_usd}"
    event_id = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    return {
        "event_id": event_id,
        "symbol": SYMBOL,
        "source_name": SOURCE_NAME,
        "source_event_ts": source_event_ts,
        "ingestion_ts": ingestion_ts,
        "price_usd": price_usd,
        "currency": CURRENCY,
        "payload_json": json.dumps(payload, ensure_ascii=False),
        "api_status": "success",
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
    spark = None


    try:
        spark = build_spark()
        api_url = build_api_url(args.api_base_url)

        payload = fetch_payload(api_url, args.request_timeout)
        if payload is None:
            print("No data fetched.")
            return

        validate_payload(payload)
        row = build_row(payload)
        write_row(spark, args.bronze_path, row)
        print(f"Wrote Bronze row to {args.bronze_path} | price_usd={row['price_usd']}")
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()