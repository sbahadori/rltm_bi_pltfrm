from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any
import time
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, expr, lit, row_number
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
    
from shared.runtime.job_run_registry import (
    append_job_event,
    exception_to_text,
    new_run_id,
)

def _bootstrap_repo_path() -> Path:
    repo_root = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    return repo_root


REPO_ROOT = _bootstrap_repo_path()

from batch.specs.batch_catalog_utils import get_job_by_name  # noqa: E402

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
        .appName("generic_silver_to_gold")
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


def load_job_spec(catalog_path: str, pipeline_name: str, job_name: str) -> dict[str, Any]:
    job = get_job_by_name(catalog_path, pipeline_name, job_name)
    if job["job_type"] != "generic_silver_to_gold":
        raise ValueError(
            f"Job '{job_name}' is not generic_silver_to_gold; got '{job['job_type']}'"
        )
    return job["spec"]


def apply_select_map(df: DataFrame, select_map: dict[str, str]) -> DataFrame:
    cols = [col(source_name).alias(target_name) for target_name, source_name in select_map.items()]
    return df.select(*cols)


def apply_filters(df: DataFrame, filters_spec: list[dict[str, Any]]) -> DataFrame:
    result = df
    for rule in filters_spec or []:
        rule_type = rule["type"]
        field = rule["field"]

        if rule_type == "equals":
            result = result.filter(col(field) == lit(rule["value"]))
        elif rule_type == "not_equals":
            result = result.filter(col(field) != lit(rule["value"]))
        elif rule_type == "greater_than":
            result = result.filter(col(field) > lit(rule["value"]))
        elif rule_type == "greater_or_equal":
            result = result.filter(col(field) >= lit(rule["value"]))
        elif rule_type == "less_than":
            result = result.filter(col(field) < lit(rule["value"]))
        elif rule_type == "less_or_equal":
            result = result.filter(col(field) <= lit(rule["value"]))
        elif rule_type == "is_not_null":
            result = result.filter(col(field).isNotNull())
        else:
            raise ValueError(f"Unsupported filter type: {rule_type}")

    return result


def apply_derived_fields(df: DataFrame, derived_fields: dict[str, dict[str, Any]]) -> DataFrame:
    result = df
    for field_name, spec in (derived_fields or {}).items():
        kind = spec["kind"]

        if kind == "sql":
            result = result.withColumn(field_name, expr(spec["expr"]))
        else:
            raise ValueError(f"Unsupported derived field kind: {kind}")

    return result


def apply_quality_rules(df: DataFrame, quality_rules: list[dict[str, Any]]) -> DataFrame:
    result = df
    for rule in quality_rules or []:
        rule_type = rule["type"]
        field = rule["field"]

        if rule_type == "not_null":
            result = result.filter(col(field).isNotNull())
        elif rule_type == "greater_than":
            result = result.filter(col(field) > lit(rule["value"]))
        elif rule_type == "greater_or_equal":
            result = result.filter(col(field) >= lit(rule["value"]))
        elif rule_type == "equals":
            result = result.filter(col(field) == lit(rule["value"]))
        else:
            raise ValueError(f"Unsupported quality rule type: {rule_type}")

    return result


def apply_dedupe(df: DataFrame, dedupe_spec: dict[str, Any]) -> DataFrame:
    if not dedupe_spec:
        return df

    key_columns = dedupe_spec["key_columns"]
    order_by = dedupe_spec.get("order_by", [])

    if not order_by:
        return df.dropDuplicates(key_columns)

    order_exprs = [expr(item) for item in order_by]
    w = Window.partitionBy(*key_columns).orderBy(*order_exprs)

    return (
        df.withColumn("_rn", row_number().over(w))
          .filter(col("_rn") == 1)
          .drop("_rn")
    )


def ensure_table(
    spark: SparkSession,
    path: str,
    df: DataFrame,
    partition_by: list[str],
) -> None:
    if DeltaTable.isDeltaTable(spark, path):
        return

    writer = (
        df.limit(0)
        .write
        .format("delta")
        .mode("overwrite")
    )

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(path)
    
def merge_to_target(
    spark: SparkSession,
    target_path: str,
    df: DataFrame,
    merge_keys: list[str],
    partition_by: list[str],
) -> None:
    ensure_table(
        spark=spark,
        path=target_path,
        df=df,
        partition_by=partition_by,
    )

    target = DeltaTable.forPath(spark, target_path)

    condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])

    (
        target.alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

def main():
    args = parse_args()
    run_id = new_run_id(f"{args.pipeline_name}__{args.job_name}")
    started = time.time()

    append_job_event(
        event_type="batch_started",
        run_id=run_id,
        type="batch",
        pipeline=args.pipeline_name,
        job=args.job_name,
        job_id=f"{args.pipeline_name}__{args.job_name}",
        status="running",
    )

    spark = build_spark()

    try:
        spec = load_job_spec(args.catalog_path, args.pipeline_name, args.job_name)

        source_path = spec["source"]["path"]
        target_path = spec["target"]["path"]
        merge_keys = spec["target"].get("merge_keys", [])

        bronze_df = spark.read.format(spec["source"].get("format", "delta")).load(source_path)

        silver_df = (
            bronze_df.transform(lambda df: apply_filters(df, spec.get("filters", [])))
                     .transform(lambda df: apply_select_map(df, spec["select_map"]))
                     .transform(lambda df: apply_derived_fields(df, spec.get("derived_fields", {})))
                     .transform(lambda df: apply_quality_rules(df, spec.get("quality_rules", [])))
                     .transform(lambda df: apply_dedupe(df, spec.get("dedupe", {})))
        )

        if silver_df.rdd.isEmpty():
            print("No rows to write to Silver.")
            return

        target_mode = spec["target"].get("mode", "merge")
        target_format = spec["target"].get("format", "delta")
        partition_by = spec["target"].get("partition_by", [])

        if target_mode == "merge":
            if not merge_keys:
                raise ValueError("Target mode 'merge' requires target.merge_keys")

            merge_to_target(
                spark=spark,
                target_path=target_path,
                df=silver_df,
                merge_keys=merge_keys,
                partition_by=partition_by,
            )

        else:
            writer = (
                silver_df.write
                .format(target_format)
                .mode(target_mode)
            )

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            writer.save(target_path)

        append_job_event(
            event_type="batch_succeeded",
            run_id=run_id,
            type="batch",
            pipeline=args.pipeline_name,
            job=args.job_name,
            job_id=f"{args.pipeline_name}__{args.job_name}",
            status="success",
            started_at_epoch=int(started),
            ended_at_epoch=int(time.time()),
            duration_seconds=round(time.time() - started, 3),
        )

        print(f"Wrote Gold rows to {target_path}")

    except Exception as exc:
        append_job_event(
            event_type="batch_failed",
            run_id=run_id,
            type="batch",
            pipeline=args.pipeline_name,
            job=args.job_name,
            job_id=f"{args.pipeline_name}__{args.job_name}",
            status="failed",
            started_at_epoch=int(started),
            ended_at_epoch=int(time.time()),
            duration_seconds=round(time.time() - started, 3),
            error=str(exc),
            traceback=exception_to_text(exc),
        )
        raise
  
    finally:
        spark.stop()

if __name__ == "__main__":
    main()