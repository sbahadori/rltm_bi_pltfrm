import argparse
import os
import sys
from pathlib import Path
from typing import Any
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, expr, lit, row_number

def _safe_int(value: Any) -> int:
    if value in (None, ""):
        return 0

    try:
        return int(value)
    except Exception:
        return 0


def _latest_delta_operation_metrics(
    spark: SparkSession,
    target_path: str,
) -> dict[str, Any]:
    try:
        delta_table = DeltaTable.forPath(spark, target_path)
        rows = delta_table.history(1).select("operation", "operationMetrics").collect()

        if not rows:
            return {}

        row = rows[0]
        metrics = row["operationMetrics"] or {}

        return {
            "operation": row["operation"],
            "operation_metrics": dict(metrics),
        }

    except Exception:
        return {}
    
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
) -> dict[str, Any]:
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

    latest = _latest_delta_operation_metrics(
        spark=spark,
        target_path=target_path,
    )

    raw = latest.get("operation_metrics") or {}

    inserted = _safe_int(raw.get("numTargetRowsInserted"))
    updated = _safe_int(raw.get("numTargetRowsUpdated"))
    deleted = _safe_int(raw.get("numTargetRowsDeleted"))

    output_rows = (
        _safe_int(raw.get("numOutputRows"))
        or inserted + updated + deleted
    )

    return {
        "records_written": output_rows,
        "records_inserted": inserted,
        "records_updated": updated,
        "records_deleted": deleted,
        "delta_operation": latest.get("operation"),
        "delta_operation_metrics": raw,
    }


def validate_partition_columns(df: DataFrame, partition_by: list[str]) -> None:
    missing = [column for column in partition_by if column not in df.columns]

    if missing:
        raise ValueError(
            f"Target partition columns are missing from Gold DataFrame: {missing}. "
            f"Available columns: {df.columns}"
        )