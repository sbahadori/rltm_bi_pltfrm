import argparse
import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import abs, avg, col, lag, lit, stddev_samp, when
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType, BooleanType
)

SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("source_name", StringType(), False),
    StructField("currency", StringType(), False),
    StructField("metric_ts", TimestampType(), False),
    StructField("price_usd", DoubleType(), True),
    StructField("return_1m", DoubleType(), True),
    StructField("return_5m", DoubleType(), True),
    StructField("ma_5", DoubleType(), True),
    StructField("ma_15", DoubleType(), True),
    StructField("ma_60", DoubleType(), True),
    StructField("volatility_15", DoubleType(), True),
    StructField("volatility_60", DoubleType(), True),
    StructField("zscore_60", DoubleType(), True),
    StructField("is_anomaly", BooleanType(), False),
])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gold-path", default="s3a://lakehouse/gold/gold_price_bars_1m")
    parser.add_argument("--metrics-path", default="s3a://lakehouse/gold/gold_price_metrics")
    return parser.parse_args()


def build_spark():
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "minio")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")
    region = os.getenv("AWS_REGION", "us-east-1")

    spark = (
        SparkSession.builder
        .appName("gold_bars_to_metrics")
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

    spark.createDataFrame([], SCHEMA).write.format("delta").mode("overwrite").save(path)


def build_metrics(gold_df):
    base = (
        gold_df
        .select(
            "symbol",
            "source_name",
            "currency",
            col("bar_start_ts").alias("metric_ts"),
            col("close_price").alias("price_usd"),
        )
    )

    order_w = Window.partitionBy("symbol", "source_name", "currency").orderBy("metric_ts")
    w_5 = order_w.rowsBetween(-4, 0)
    w_15 = order_w.rowsBetween(-14, 0)
    w_60 = order_w.rowsBetween(-59, 0)

    return (
        base
        .withColumn("prev_close_1", lag("price_usd", 1).over(order_w))
        .withColumn("prev_close_5", lag("price_usd", 5).over(order_w))
        .withColumn(
            "return_1m",
            when(col("prev_close_1").isNull(), None)
            .otherwise((col("price_usd") - col("prev_close_1")) / col("prev_close_1"))
        )
        .withColumn(
            "return_5m",
            when(col("prev_close_5").isNull(), None)
            .otherwise((col("price_usd") - col("prev_close_5")) / col("prev_close_5"))
        )
        .withColumn("ma_5", avg("price_usd").over(w_5))
        .withColumn("ma_15", avg("price_usd").over(w_15))
        .withColumn("ma_60", avg("price_usd").over(w_60))
        .withColumn("volatility_15", stddev_samp("return_1m").over(w_15))
        .withColumn("volatility_60", stddev_samp("return_1m").over(w_60))
        .withColumn("price_std_60", stddev_samp("price_usd").over(w_60))
        .withColumn(
            "zscore_60",
            when(col("price_std_60").isNull() | (col("price_std_60") == 0), None)
            .otherwise((col("price_usd") - col("ma_60")) / col("price_std_60"))
        )
        .withColumn(
            "is_anomaly",
            when(abs(col("zscore_60")) >= lit(3.0), lit(True)).otherwise(lit(False))
        )
        .drop("prev_close_1", "prev_close_5", "price_std_60")
    )


def merge_metrics(spark, path, metrics_df):
    ensure_table(spark, path)

    target = DeltaTable.forPath(spark, path)
    (
        target.alias("t")
        .merge(
            metrics_df.alias("s"),
            """
            t.symbol = s.symbol
            AND t.source_name = s.source_name
            AND t.currency = s.currency
            AND t.metric_ts = s.metric_ts
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
        gold_df = spark.read.format("delta").load(args.gold_path)
        metrics_df = build_metrics(gold_df)

        if metrics_df.rdd.isEmpty():
            print("No rows to write to metrics.")
            return

        merge_metrics(spark, args.metrics_path, metrics_df)
        print(f"Wrote metrics to {args.metrics_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()