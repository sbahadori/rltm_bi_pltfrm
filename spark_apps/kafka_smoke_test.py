import os
from pyspark.sql import SparkSession

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
EVENT_TOPIC = os.getenv("EVENT_TOPIC", "user_events")

spark = (
    SparkSession.builder
    .appName("kafka_smoke_test")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", EVENT_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS raw_json",
        "topic",
        "partition",
        "offset",
        "timestamp"
    )
)

query = (
    df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()