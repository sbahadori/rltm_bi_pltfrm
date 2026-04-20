import argparse
import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, max as spark_max, min as spark_min
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


SCHEMA = StructType([
    StructField("event_date", DateType(), False),
    StructField("event_type", StringType(), False),
    StructField("source", StringType(), False),
    StructField("events_count", LongType(), False),
    StructField("users_count", LongType(), False),
    StructField("sessions_count", LongType(), False),
    StructField("first_event_ts", TimestampType(), True),
    StructField("last_event_ts", TimestampType(), True),
])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-path", default="s3a://lakehouse/silver_delta/user_events_clean")
    parser.add_argument("--target-path", default="s3a://lakehouse/gold_delta/event_daily_metrics")
    return parser.parse_args()


def build_spark():
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minio"))
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minio123"))
    region = os.getenv("AWS_REGION", "us-east-1")

    spark = (
        SparkSession.builder
        .appName("clickstream_event_daily_metrics")
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


def ensure_table(spark, path):
    if DeltaTable.isDeltaTable(spark, path):
        return

    (
        spark.createDataFrame([], SCHEMA)
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("event_date")
        .save(path)
    )


def build_metrics(df):
    return (
        df.filter(col("event_date").isNotNull())
        .filter(col("event_type").isNotNull())
        .filter(col("source").isNotNull())
        .groupBy("event_date", "event_type", "source")
        .agg(
            count("*").alias("events_count"),
            countDistinct("canonical_user_key").alias("users_count"),
            countDistinct("session_id").alias("sessions_count"),
            spark_min("event_ts").alias("first_event_ts"),
            spark_max("event_ts").alias("last_event_ts"),
        )
    )


def merge_metrics(spark, path, metrics_df):
    ensure_table(spark, path)
    target = DeltaTable.forPath(spark, path)

    (
        target.alias("t")
        .merge(
            metrics_df.alias("s"),
            """
            t.event_date = s.event_date
            AND t.event_type = s.event_type
            AND t.source = s.source
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def main():
    args = parse_args()
    spark = build_spark()

    try:
        silver_df = spark.read.format("delta").load(args.source_path)
        metrics_df = build_metrics(silver_df)

        if metrics_df.rdd.isEmpty():
            print("No rows to write to event_daily_metrics.")
            return

        merge_metrics(spark, args.target_path, metrics_df)
        print(f"Wrote clickstream event daily metrics to {args.target_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()