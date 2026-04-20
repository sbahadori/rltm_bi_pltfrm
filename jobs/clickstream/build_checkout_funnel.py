import argparse
import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    lit,
    max as spark_max,
    sum as spark_sum,
    when,
)
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StructField,
    StructType,
)


SCHEMA = StructType([
    StructField("event_date", DateType(), False),
    StructField("sessions_any", LongType(), False),
    StructField("sessions_page_view", LongType(), False),
    StructField("sessions_product_view", LongType(), False),
    StructField("sessions_add_to_cart", LongType(), False),
    StructField("sessions_backend_log", LongType(), False),
    StructField("sessions_checkout", LongType(), False),
    StructField("sessions_purchase", LongType(), False),
    StructField("product_to_cart_rate", DoubleType(), True),
    StructField("cart_to_checkout_rate", DoubleType(), True),
    StructField("checkout_to_purchase_rate", DoubleType(), True),
])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-path", default="s3a://lakehouse/silver_delta/user_events_clean")
    parser.add_argument("--target-path", default="s3a://lakehouse/gold_delta/checkout_funnel")
    return parser.parse_args()


def build_spark():
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minio"))
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minio123"))
    region = os.getenv("AWS_REGION", "us-east-1")

    spark = (
        SparkSession.builder
        .appName("clickstream_checkout_funnel")
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


def build_funnel(df):
    session_flags = (
        df.filter(col("event_date").isNotNull())
        .filter(col("canonical_user_key").isNotNull())
        .filter(col("session_id").isNotNull())
        .groupBy("event_date", "canonical_user_key", "session_id")
        .agg(
            spark_max(when(col("event_type") == "page_view", lit(1)).otherwise(lit(0))).alias("did_page_view"),
            spark_max(when(col("event_type") == "product_view", lit(1)).otherwise(lit(0))).alias("did_product_view"),
            spark_max(when(col("event_type") == "add_to_cart", lit(1)).otherwise(lit(0))).alias("did_add_to_cart"),
            spark_max(when(col("event_type") == "backend_log", lit(1)).otherwise(lit(0))).alias("did_backend_log"),
            spark_max(when(col("event_type") == "checkout", lit(1)).otherwise(lit(0))).alias("did_checkout"),
            spark_max(when(col("event_type") == "purchase", lit(1)).otherwise(lit(0))).alias("did_purchase"),
        )
    )

    daily = (
        session_flags.groupBy("event_date")
        .agg(
            count("*").alias("sessions_any"),
            spark_sum("did_page_view").alias("sessions_page_view"),
            spark_sum("did_product_view").alias("sessions_product_view"),
            spark_sum("did_add_to_cart").alias("sessions_add_to_cart"),
            spark_sum("did_backend_log").alias("sessions_backend_log"),
            spark_sum("did_checkout").alias("sessions_checkout"),
            spark_sum("did_purchase").alias("sessions_purchase"),
        )
        .withColumn(
            "product_to_cart_rate",
            when(col("sessions_product_view") == 0, None)
            .otherwise(col("sessions_add_to_cart") / col("sessions_product_view"))
        )
        .withColumn(
            "cart_to_checkout_rate",
            when(col("sessions_add_to_cart") == 0, None)
            .otherwise(col("sessions_checkout") / col("sessions_add_to_cart"))
        )
        .withColumn(
            "checkout_to_purchase_rate",
            when(col("sessions_checkout") == 0, None)
            .otherwise(col("sessions_purchase") / col("sessions_checkout"))
        )
    )

    return daily.select(
        "event_date",
        "sessions_any",
        "sessions_page_view",
        "sessions_product_view",
        "sessions_add_to_cart",
        "sessions_backend_log",
        "sessions_checkout",
        "sessions_purchase",
        "product_to_cart_rate",
        "cart_to_checkout_rate",
        "checkout_to_purchase_rate",
    )


def merge_funnel(spark, path, funnel_df):
    ensure_table(spark, path)
    target = DeltaTable.forPath(spark, path)

    (
        target.alias("t")
        .merge(funnel_df.alias("s"), "t.event_date = s.event_date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def main():
    args = parse_args()
    spark = build_spark()

    try:
        silver_df = spark.read.format("delta").load(args.source_path)
        funnel_df = build_funnel(silver_df)

        if funnel_df.rdd.isEmpty():
            print("No rows to write to checkout_funnel.")
            return

        merge_funnel(spark, args.target_path, funnel_df)
        print(f"Wrote clickstream checkout funnel to {args.target_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()