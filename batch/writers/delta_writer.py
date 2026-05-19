import argparse
import os
import sys
from pathlib import Path
from typing import Any
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, expr, lit, row_number


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



def validate_partition_columns(df: DataFrame, partition_by: list[str]) -> None:
    missing = [column for column in partition_by if column not in df.columns]

    if missing:
        raise ValueError(
            f"Target partition columns are missing from Gold DataFrame: {missing}. "
            f"Available columns: {df.columns}"
        )