import argparse
import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lit, row_number, to_date, when
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType, DateType
)

SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("source_name", StringType(), False),
    StructField("event_ts_utc", TimestampType(), True),
    StructField("ingestion_ts", TimestampType(), False),
    StructField("price_usd", DoubleType(), True),
    StructField("currency", StringType(), False),
    StructField("quality_flag", StringType(), False),
    StructField("processing_date", DateType(), False),
])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-path", default="s3a://lakehouse/bronze/gold_price_events")
    parser.add_argument("--silver-path", default="s3a://lakehouse/silver/gold_price_ticks_clean")
    return parser.parse_args()


def build_spark():
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "minio")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")
    region = os.getenv("AWS_REGION", "us-east-1")

    spark = (
        SparkSession.builder
        .appName("bronze_to_silver_gold_ticks")
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
        .partitionBy("processing_date")
        .save(path)
    )


def build_silver(bronze_df):
    df = (
        bronze_df
        .filter(col("api_status") == "success")
        .select(
            "event_id",
            "symbol",
            "source_name",
            col("source_event_ts").alias("event_ts_utc"),
            "ingestion_ts",
            "price_usd",
            "currency",
        )
        .withColumn(
            "quality_flag",
            when(col("event_ts_utc").isNull(), lit("NULL_EVENT_TS"))
            .when(col("price_usd").isNull(), lit("NULL_PRICE"))
            .when(col("price_usd") <= 0, lit("NON_POSITIVE_PRICE"))
            .otherwise(lit("OK"))
        )
        .withColumn("processing_date", to_date("ingestion_ts"))
        .filter(col("quality_flag") == "OK")
    )

    w = Window.partitionBy("event_id").orderBy(col("ingestion_ts").asc())

    return (
        df.withColumn("rn", row_number().over(w))
          .filter(col("rn") == 1)
          .drop("rn")
    )


def merge_silver(spark, path, df):
    ensure_table(spark, path)

    target = DeltaTable.forPath(spark, path)
    (
        target.alias("t")
        .merge(df.alias("s"), "t.event_id = s.event_id")
        .whenNotMatchedInsertAll()
        .execute()
    )


def main():
    args = parse_args()
    spark = build_spark()

    try:
        bronze_df = spark.read.format("delta").load(args.bronze_path)
        silver_df = build_silver(bronze_df)

        if silver_df.rdd.isEmpty():
            print("No new valid rows for Silver.")
            return

        merge_silver(spark, args.silver_path, silver_df)
        print(f"Wrote Silver rows to {args.silver_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()