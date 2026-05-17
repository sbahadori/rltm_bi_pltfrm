from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_state_path(table_cfg: dict[str, Any], source_id: str) -> str:
    template = table_cfg.get(
        "state_path_template",
        "s3a://lakehouse/_state/jdbc/{source_id}/{table_id}",
    )
    return template.format(
        source_id=source_id,
        table_id=table_cfg["table_id"],
    )


def read_state(
    spark: SparkSession,
    *,
    state_path: str,
    source_id: str,
    table_id: str,
    state_key: str,
    default_value: Any,
) -> str:
    try:
        df = spark.read.format("delta").load(state_path)
    except Exception:
        return str(default_value)

    rows = (
        df.filter(
            (F.col("source_id") == source_id)
            & (F.col("table_id") == table_id)
            & (F.col("state_key") == state_key)
        )
        .orderBy(F.col("updated_at").desc())
        .limit(1)
        .collect()
    )

    if not rows:
        return str(default_value)

    return str(rows[0]["state_value"])


def write_state(
    spark: SparkSession,
    *,
    state_path: str,
    source_id: str,
    table_id: str,
    state_key: str,
    state_value: Any,
    batch_run_id: str,
) -> None:
    df = (
        spark.createDataFrame(
            [
                {
                    "source_id": source_id,
                    "table_id": table_id,
                    "state_key": state_key,
                    "state_value": str(state_value),
                    "batch_run_id": batch_run_id,
                }
            ]
        )
        .withColumn("updated_at", F.current_timestamp())
    )

    df.write.format("delta").mode("append").save(state_path)