from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_api_state_path(runtime_ctx: dict[str, Any]) -> str:
    template = runtime_ctx.get(
        "state_path_template",
        "s3a://lakehouse/_state/api/{pipeline_name}/{job_name}",
    )

    return template.format(
        pipeline_name=runtime_ctx["pipeline_name"],
        job_name=runtime_ctx["job_name"],
    )


def build_api_state_key(runtime_ctx: dict[str, Any]) -> str:
    load_type = runtime_ctx.get("load_type", "event")
    strategy = runtime_ctx.get("strategy", "append_event")
    state_cfg = runtime_ctx.get("state") or {}

    column = (
        state_cfg.get("name")
        or state_cfg.get("param_name")
        or state_cfg.get("response_path")
        or "default"
    )

    return f"{load_type}::{strategy}::{column}"


def read_api_state(
    spark: SparkSession,
    *,
    state_path: str,
    pipeline_name: str,
    job_name: str,
    state_key: str,
    default_value: Any,
) -> str:
    try:
        df = spark.read.format("delta").load(state_path)
    except Exception:
        return str(default_value)

    rows = (
        df.filter(
            (F.col("pipeline_name") == pipeline_name)
            & (F.col("job_name") == job_name)
            & (F.col("state_key") == state_key)
        )
        .orderBy(F.col("updated_at").desc())
        .limit(1)
        .collect()
    )

    if not rows:
        return str(default_value)

    return str(rows[0]["state_value"])


def write_api_state(
    spark: SparkSession,
    *,
    state_path: str,
    pipeline_name: str,
    job_name: str,
    state_key: str,
    state_value: Any,
    batch_run_id: str,
) -> None:
    df = (
        spark.createDataFrame(
            [
                {
                    "pipeline_name": pipeline_name,
                    "job_name": job_name,
                    "state_key": state_key,
                    "state_value": str(state_value),
                    "batch_run_id": batch_run_id,
                }
            ]
        )
        .withColumn("updated_at", F.current_timestamp())
    )

    df.write.format("delta").mode("append").save(state_path)