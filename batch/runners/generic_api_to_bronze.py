from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def _bootstrap_repo_path() -> Path:
    repo_root = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    return repo_root


REPO_ROOT = _bootstrap_repo_path()

from batch.specs.batch_catalog_utils import get_job_by_name  # noqa: E402
from batch.utils.generic_api_job_utils import (  # noqa: E402
    build_bronze_dataframe,
    build_runtime_context,
    execute_api_request,
    map_payload_to_row,
    validate_payload,
    write_bronze_dataframe,
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog-path", required=True)
    parser.add_argument("--pipeline-name", required=True)
    parser.add_argument("--job-name", required=True)
    return parser.parse_args()


def build_spark() -> SparkSession:
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "minio")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")
    region = os.getenv("AWS_REGION", "us-east-1")

    spark = (
        SparkSession.builder
        .appName("generic_api_to_bronze")
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


def main():
    args = parse_args()
    spark = None

    try:
        job = get_job_by_name(args.catalog_path, args.pipeline_name, args.job_name)
        if job["job_type"] != "generic_api_to_bronze":
            raise ValueError(
                f"Job '{args.job_name}' is not generic_api_to_bronze; got '{job['job_type']}'"
            )

        runtime_ctx = build_runtime_context(job)
        payload = execute_api_request(runtime_ctx)
        validate_payload(payload, runtime_ctx["validation"])
        row = map_payload_to_row(payload, runtime_ctx)
        spark = build_spark()
        df = build_bronze_dataframe(spark, row)
        write_bronze_dataframe(df, runtime_ctx["bronze_write"])

        print(
            f"Wrote Bronze rows for job '{args.job_name}' to "
            f"{runtime_ctx['bronze_write']['target_path']}"
        )
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()