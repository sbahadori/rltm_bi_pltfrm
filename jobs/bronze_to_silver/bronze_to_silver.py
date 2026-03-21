import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Optional

from pyspark.sql import Row, SparkSession, Window
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    lit,
    row_number,
    to_date,
    to_utc_timestamp,
    when,
)
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

try:
    from delta import configure_spark_with_delta_pip
    from delta.tables import DeltaTable
except Exception as exc:  # pragma: no cover
    raise RuntimeError(
        "Delta Lake is required. Install delta-spark and make sure Spark is started with Delta extensions."
    ) from exc

LOG = logging.getLogger("bronze_to_silver")
JOB_NAME = "bronze_to_silver_gold_ticks"

SILVER_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("symbol", StringType(), True),
        StructField("source_name", StringType(), True),
        StructField("event_ts_utc", TimestampType(), True),
        StructField("ingestion_ts", TimestampType(), True),
        StructField("price_usd", DoubleType(), True),
        StructField("bid_usd", DoubleType(), True),
        StructField("ask_usd", DoubleType(), True),
        StructField("mid_price_usd", DoubleType(), True),
        StructField("spread_usd", DoubleType(), True),
        StructField("quality_flag", StringType(), True),
        StructField("processing_date", DateType(), True),
    ]
)

REJECT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("symbol", StringType(), True),
        StructField("source_name", StringType(), True),
        StructField("event_ts_utc", TimestampType(), True),
        StructField("ingestion_ts", TimestampType(), True),
        StructField("price_usd", DoubleType(), True),
        StructField("bid_usd", DoubleType(), True),
        StructField("ask_usd", DoubleType(), True),
        StructField("mid_price_usd", DoubleType(), True),
        StructField("spread_usd", DoubleType(), True),
        StructField("rejection_reason", StringType(), True),
        StructField("processing_date", DateType(), True),
    ]
)

WATERMARK_SCHEMA = StructType(
    [
        StructField("job_name", StringType(), False),
        StructField("last_ingestion_ts", TimestampType(), True),
        StructField("updated_ts", TimestampType(), True),
    ]
)


def build_spark(app_name: str) -> SparkSession:
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minio"))
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minio123"))
    region = os.getenv("AWS_REGION", "us-east-1")

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https://")).lower())
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint.region", region)
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incrementally transform Bronze gold ticks into Silver.")
    parser.add_argument("--bronze-path", default="s3a://lakehouse/bronze/gold_price_events")
    parser.add_argument("--silver-path", default="s3a://lakehouse/silver/gold_price_ticks_clean")
    parser.add_argument("--rejects-path", default="s3a://lakehouse/silver/gold_price_ticks_rejects")
    parser.add_argument("--watermark-path", default="s3a://lakehouse/meta/pipeline_watermarks")
    parser.add_argument("--source-timezone", default="UTC")
    parser.add_argument("--stale-threshold-minutes", type=int, default=60)
    parser.add_argument("--future-grace-seconds", type=int, default=30)
    return parser.parse_args()


def ensure_delta_table(spark: SparkSession, path: str, schema: StructType, partition_cols=None) -> None:
    if DeltaTable.isDeltaTable(spark, path):
        return
    empty_df = spark.createDataFrame([], schema)
    writer = empty_df.write.format("delta").mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)
    LOG.info("Created empty Delta table at %s", path)


def get_watermark(spark: SparkSession, watermark_path: str) -> Optional[datetime]:
    ensure_delta_table(spark, watermark_path, WATERMARK_SCHEMA)
    df = spark.read.format("delta").load(watermark_path).filter(col("job_name") == JOB_NAME)
    rows = df.orderBy(col("updated_ts").desc()).limit(1).collect()
    if not rows or rows[0]["last_ingestion_ts"] is None:
        return None
    return rows[0]["last_ingestion_ts"]


def update_watermark(spark: SparkSession, watermark_path: str, last_ingestion_ts) -> None:
    if last_ingestion_ts is None:
        return
    ensure_delta_table(spark, watermark_path, WATERMARK_SCHEMA)

    updates = spark.createDataFrame(
        [
            Row(
                job_name=JOB_NAME,
                last_ingestion_ts=last_ingestion_ts,
                updated_ts=None,
            )
        ],
        schema=WATERMARK_SCHEMA,
    ).withColumn("updated_ts", current_timestamp())

    target = DeltaTable.forPath(spark, watermark_path)
    (
        target.alias("t")
        .merge(updates.alias("s"), "t.job_name = s.job_name")
        .whenMatchedUpdate(
            set={
                "last_ingestion_ts": col("s.last_ingestion_ts"),
                "updated_ts": col("s.updated_ts"),
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def load_incremental_bronze(spark: SparkSession, bronze_path: str, watermark: Optional[datetime]):
    df = spark.read.format("delta").load(bronze_path)
    if watermark is not None:
        return df.filter(col("ingestion_ts") > lit(watermark))
    return df


def standardize(bronze_df, source_timezone: str, stale_threshold_minutes: int, future_grace_seconds: int):
    event_ts_col = col("source_event_ts") if source_timezone.upper() == "UTC" else to_utc_timestamp(col("source_event_ts"), source_timezone)

    standardized = (
        bronze_df.filter(col("api_status") == "OK")
        .withColumn("event_ts_utc", event_ts_col)
        .withColumn("mid_price_usd", when(col("bid_usd").isNotNull() & col("ask_usd").isNotNull(), (col("bid_usd") + col("ask_usd")) / lit(2.0)))
        .withColumn("spread_usd", when(col("bid_usd").isNotNull() & col("ask_usd").isNotNull(), col("ask_usd") - col("bid_usd")))
        .withColumn("price_usd", when(col("price_usd").isNull(), col("mid_price_usd")).otherwise(col("price_usd")))
    )

    with_quality = standardized.withColumn(
        "quality_flag",
        when(col("event_ts_utc").isNull(), lit("NULL_EVENT_TS"))
        .when(col("price_usd").isNull(), lit("NULL_PRICE"))
        .when(col("price_usd") <= 0, lit("NEGATIVE_PRICE"))
        .when(col("bid_usd").isNotNull() & col("ask_usd").isNotNull() & (col("ask_usd") < col("bid_usd")), lit("CROSSED_MARKET"))
        .when(col("event_ts_utc") < expr(f"ingestion_ts - INTERVAL {stale_threshold_minutes} MINUTES"), lit("STALE_TIMESTAMP"))
        .when(col("event_ts_utc") > expr(f"ingestion_ts + INTERVAL {future_grace_seconds} SECONDS"), lit("FUTURE_TIMESTAMP"))
        .otherwise(lit("OK")),
    )

    w = Window.partitionBy("event_id").orderBy(col("ingestion_ts").asc())
    return (
        with_quality.withColumn("rn", row_number().over(w))
        .withColumn("processing_date", to_date(col("ingestion_ts")))
        .withColumn(
            "rejection_reason",
            when(col("rn") > lit(1), lit("DUPLICATE_EVENT")).otherwise(col("quality_flag")),
        )
    )


def split_silver_and_rejects(standardized_df):
    silver_df = (
        standardized_df.filter((col("rn") == 1) & (col("quality_flag") == "OK"))
        .select(
            "event_id",
            "symbol",
            "source_name",
            "event_ts_utc",
            "ingestion_ts",
            "price_usd",
            "bid_usd",
            "ask_usd",
            "mid_price_usd",
            "spread_usd",
            "quality_flag",
            "processing_date",
        )
    )

    reject_df = (
        standardized_df.filter((col("rn") > 1) | (col("quality_flag") != "OK"))
        .select(
            "event_id",
            "symbol",
            "source_name",
            "event_ts_utc",
            "ingestion_ts",
            "price_usd",
            "bid_usd",
            "ask_usd",
            "mid_price_usd",
            "spread_usd",
            "rejection_reason",
            "processing_date",
        )
    )

    return silver_df, reject_df


def merge_to_silver(spark: SparkSession, silver_path: str, silver_df) -> None:
    ensure_delta_table(spark, silver_path, SILVER_SCHEMA, partition_cols=["processing_date"])
    target = DeltaTable.forPath(spark, silver_path)
    (
        target.alias("t")
        .merge(silver_df.alias("s"), "t.event_id = s.event_id")
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_to_rejects(spark: SparkSession, rejects_path: str, reject_df) -> None:
    ensure_delta_table(spark, rejects_path, REJECT_SCHEMA, partition_cols=["processing_date"])
    target = DeltaTable.forPath(spark, rejects_path)
    (
        target.alias("t")
        .merge(
            reject_df.alias("s"),
            "t.event_id = s.event_id AND t.ingestion_ts = s.ingestion_ts AND t.rejection_reason = s.rejection_reason",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        stream=sys.stdout,
    )
    args = parse_args()
    spark = build_spark("bronze_to_silver_gold_ticks")

    try:
        watermark = get_watermark(spark, args.watermark_path)
        bronze_df = load_incremental_bronze(spark, args.bronze_path, watermark)

        if bronze_df.rdd.isEmpty():
            LOG.info("No new Bronze rows found.")
            return 0

        standardized_df = standardize(
            bronze_df=bronze_df,
            source_timezone=args.source_timezone,
            stale_threshold_minutes=args.stale_threshold_minutes,
            future_grace_seconds=args.future_grace_seconds,
        )
        silver_df, reject_df = split_silver_and_rejects(standardized_df)

        if not silver_df.rdd.isEmpty():
            merge_to_silver(spark, args.silver_path, silver_df)

        if not reject_df.rdd.isEmpty():
            merge_to_rejects(spark, args.rejects_path, reject_df)

        max_ts = bronze_df.agg({"ingestion_ts": "max"}).collect()[0][0]
        update_watermark(spark, args.watermark_path, max_ts)

        LOG.info(
            "Silver inserted=%s, rejects inserted/merged=%s, watermark=%s",
            silver_df.count(),
            reject_df.count(),
            max_ts,
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
