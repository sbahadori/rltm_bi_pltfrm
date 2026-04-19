import argparse
import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_trunc,
    expr,
    lit,
    max as spark_max,
    min as spark_min,
    row_number,
    to_date,
)
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("source_name", StringType(), False),
    StructField("currency", StringType(), False),
    StructField("bar_start_ts", TimestampType(), False),
    StructField("bar_end_ts", TimestampType(), False),
    StructField("trade_date", DateType(), False),
    StructField("open_price", DoubleType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True),
    StructField("close_price", DoubleType(), True),
    StructField("avg_price", DoubleType(), True),
    StructField("tick_count", LongType(), False),
])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-path", default="s3a://lakehouse/silver/gold_price_ticks_clean")
    parser.add_argument("--gold-path", default="s3a://lakehouse/gold/gold_price_bars_1m")
    return parser.parse_args()


def build_spark():
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minio"))
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minio123"))
    region = os.getenv("AWS_REGION", "us-east-1")

    spark = (
        SparkSession.builder
        .appName("silver_to_gold_bars_1m")
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
        .partitionBy("trade_date")
        .save(path)
    )


def build_gold_bars(silver_df):
    base = (
        silver_df
        .select(
            "event_id",
            "symbol",
            "source_name",
            "currency",
            "event_ts_utc",
            "ingestion_ts",
            "price_usd",
        )
        .filter(col("event_ts_utc").isNotNull())
        .filter(col("price_usd").isNotNull())
        .withColumn("bar_start_ts", date_trunc("minute", col("event_ts_utc")))
        .withColumn("bar_end_ts", expr("bar_start_ts + INTERVAL 1 MINUTE"))
        .withColumn("trade_date", to_date("bar_start_ts"))
    )

    w_open = Window.partitionBy(
        "symbol", "source_name", "currency", "bar_start_ts"
    ).orderBy(
        col("event_ts_utc").asc(),
        col("ingestion_ts").asc(),
        col("event_id").asc(),
    )

    w_close = Window.partitionBy(
        "symbol", "source_name", "currency", "bar_start_ts"
    ).orderBy(
        col("event_ts_utc").desc(),
        col("ingestion_ts").desc(),
        col("event_id").desc(),
    )

    open_df = (
        base.withColumn("rn", row_number().over(w_open))
        .filter(col("rn") == 1)
        .select(
            "symbol",
            "source_name",
            "currency",
            "bar_start_ts",
            col("price_usd").alias("open_price"),
        )
    )

    close_df = (
        base.withColumn("rn", row_number().over(w_close))
        .filter(col("rn") == 1)
        .select(
            "symbol",
            "source_name",
            "currency",
            "bar_start_ts",
            col("price_usd").alias("close_price"),
        )
    )

    agg_df = (
        base.groupBy(
            "symbol",
            "source_name",
            "currency",
            "bar_start_ts",
            "bar_end_ts",
            "trade_date",
        )
        .agg(
            spark_max("price_usd").alias("high_price"),
            spark_min("price_usd").alias("low_price"),
            avg("price_usd").alias("avg_price"),
            count(lit(1)).alias("tick_count"),
        )
    )

    gold_df = (
        agg_df
        .join(
            open_df,
            on=["symbol", "source_name", "currency", "bar_start_ts"],
            how="inner",
        )
        .join(
            close_df,
            on=["symbol", "source_name", "currency", "bar_start_ts"],
            how="inner",
        )
        .select(
            "symbol",
            "source_name",
            "currency",
            "bar_start_ts",
            "bar_end_ts",
            "trade_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "avg_price",
            "tick_count",
        )
    )

    return gold_df


def merge_gold(spark, path, gold_df):
    ensure_table(spark, path)

    target = DeltaTable.forPath(spark, path)
    (
        target.alias("t")
        .merge(
            gold_df.alias("s"),
            """
            t.symbol = s.symbol
            AND t.source_name = s.source_name
            AND t.currency = s.currency
            AND t.bar_start_ts = s.bar_start_ts
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
        silver_df = spark.read.format("delta").load(args.silver_path)
        gold_df = build_gold_bars(silver_df)

        if gold_df.rdd.isEmpty():
            print("No rows to write to Gold.")
            return

        merge_gold(spark, args.gold_path, gold_df)
        print(f"Wrote Gold bars to {args.gold_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()