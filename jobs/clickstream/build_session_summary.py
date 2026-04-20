import argparse
import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    unix_timestamp,
    when,
)
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
    StructField("canonical_user_key", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("session_start_ts", TimestampType(), True),
    StructField("session_end_ts", TimestampType(), True),
    StructField("session_duration_sec", LongType(), True),
    StructField("event_count", LongType(), False),
    StructField("distinct_event_types", LongType(), False),
    StructField("page_view_count", LongType(), False),
    StructField("product_view_count", LongType(), False),
    StructField("add_to_cart_count", LongType(), False),
    StructField("backend_log_count", LongType(), False),
])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-path", default="s3a://lakehouse/silver_delta/user_events_clean")
    parser.add_argument("--target-path", default="s3a://lakehouse/gold_delta/session_summary")
    return parser.parse_args()


def build_spark():
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minio"))
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minio123"))
    region = os.getenv("AWS_REGION", "us-east-1")

    spark = (
        SparkSession.builder
        .appName("clickstream_session_summary")
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


def build_summary(df):
    base = (
        df.filter(col("event_date").isNotNull())
        .filter(col("canonical_user_key").isNotNull())
        .filter(col("session_id").isNotNull())
        .filter(col("event_ts").isNotNull())
    )

    summary = (
        base.groupBy("event_date", "canonical_user_key", "session_id")
        .agg(
            spark_min("event_ts").alias("session_start_ts"),
            spark_max("event_ts").alias("session_end_ts"),
            count("*").alias("event_count"),
            countDistinct("event_type").alias("distinct_event_types"),
            spark_sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
            spark_sum(when(col("event_type") == "product_view", 1).otherwise(0)).alias("product_view_count"),
            spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
            spark_sum(when(col("event_type") == "backend_log", 1).otherwise(0)).alias("backend_log_count"),
        )
        .withColumn(
            "session_duration_sec",
            unix_timestamp("session_end_ts") - unix_timestamp("session_start_ts")
        )
    )

    return summary.select(
        "event_date",
        "canonical_user_key",
        "session_id",
        "session_start_ts",
        "session_end_ts",
        "session_duration_sec",
        "event_count",
        "distinct_event_types",
        "page_view_count",
        "product_view_count",
        "add_to_cart_count",
        "backend_log_count",
    )


def merge_summary(spark, path, summary_df):
    ensure_table(spark, path)
    target = DeltaTable.forPath(spark, path)

    (
        target.alias("t")
        .merge(
            summary_df.alias("s"),
            """
            t.event_date = s.event_date
            AND t.canonical_user_key = s.canonical_user_key
            AND t.session_id = s.session_id
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
        summary_df = build_summary(silver_df)

        if summary_df.rdd.isEmpty():
            print("No rows to write to session_summary.")
            return

        merge_summary(spark, args.target_path, summary_df)
        print(f"Wrote clickstream session summary to {args.target_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()