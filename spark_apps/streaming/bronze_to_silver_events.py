import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, StructType
)
from pyspark.sql.functions import (
    col, current_timestamp, coalesce, from_json, to_date, lit
)

BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://lakehouse/bronze_delta/user_events")
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://lakehouse/silver_delta/user_events_clean")
QUARANTINE_PATH = os.getenv("QUARANTINE_PATH", "s3a://lakehouse/silver_delta/user_events_quarantine")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/tmp/checkpoints/bronze_to_silver_events")

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minio")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")

TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "15 seconds")

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("source", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("anonymous_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("properties", StructType([
        StructField("item_id", StringType(), True),
        StructField("item_title", StringType(), True),
        StructField("backend_event_name", StringType(), True),
        StructField("message", StringType(), True),
    ]), True)
])

spark = (
    SparkSession.builder
    .appName("bronze_to_silver_events")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

bronze_df = (
    spark.readStream
    .format("delta")
    .load(BRONZE_PATH)
)

parsed = (
    bronze_df
    .withColumn("parsed", from_json(col("raw_json"), event_schema))
    .select(
        col("raw_json"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
        col("bronze_ingest_ts"),
        col("parsed.*")
    )
    .withColumn("event_ts", col("event_ts").cast(TimestampType()))
    .withColumn("event_date", to_date(col("event_ts")))
    .withColumn("canonical_user_key", coalesce(col("user_id"), col("anonymous_id")))
    .withColumn("item_id", col("properties.item_id"))
    .withColumn("item_title", col("properties.item_title"))
    .withColumn("backend_event_name", col("properties.backend_event_name"))
)

valid = parsed.filter(
    col("event_id").isNotNull() &
    col("event_type").isNotNull() &
    col("event_ts").isNotNull() &
    col("session_id").isNotNull() &
    col("canonical_user_key").isNotNull() &
    col("source").isin("frontend", "backend")
)

invalid = (
    parsed.filter(
        col("event_id").isNull() |
        col("event_type").isNull() |
        col("event_ts").isNull() |
        col("session_id").isNull() |
        col("canonical_user_key").isNull() |
        (~col("source").isin("frontend", "backend"))
    )
    .withColumn("quarantine_reason", lit("schema_or_required_field_validation_failed"))
    .withColumn("quarantine_ts", current_timestamp())
)

def write_batch(batch_df: DataFrame, batch_id: int):
    valid_batch = batch_df.filter(
        col("event_id").isNotNull() &
        col("event_type").isNotNull() &
        col("event_ts").isNotNull() &
        col("session_id").isNotNull() &
        col("canonical_user_key").isNotNull() &
        col("source").isin("frontend", "backend")
    ).dropDuplicates(["event_id"])

    invalid_batch = batch_df.filter(
        col("event_id").isNull() |
        col("event_type").isNull() |
        col("event_ts").isNull() |
        col("session_id").isNull() |
        col("canonical_user_key").isNull() |
        (~col("source").isin("frontend", "backend"))
    ).withColumn("quarantine_reason", lit("schema_or_required_field_validation_failed")) \
     .withColumn("quarantine_ts", current_timestamp())

    if not valid_batch.isEmpty():
        (
            valid_batch.write
            .format("delta")
            .mode("append")
            .partitionBy("event_date")
            .save(SILVER_PATH)
        )

    if not invalid_batch.isEmpty():
        (
            invalid_batch.write
            .format("delta")
            .mode("append")
            .save(QUARANTINE_PATH)
        )

query = (
    parsed.writeStream
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .foreachBatch(write_batch)
    .start()
)

query.awaitTermination()