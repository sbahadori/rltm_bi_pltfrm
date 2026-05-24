from __future__ import annotations

from pyspark.sql import SparkSession

from shared.core.env import s3_env


def create_spark(app_name: str, log_level: str = "ERROR") -> SparkSession:
    s3 = s3_env()

    endpoint = s3["endpoint"]
    ssl_enabled = "true" if endpoint.startswith("https://") else "false"

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", s3["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", ssl_enabled)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.endpoint.region", s3["region"])
        .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(log_level)
    return spark