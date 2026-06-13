from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F

# -----------------------------------------------------------------------------
# Bootstrap repo path
# -----------------------------------------------------------------------------

REPO_ROOT = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from batch.specs.batch_catalog_utils import get_job_by_name  # noqa: E402
from batch.transforms.rules import (  # noqa: E402
    apply_dedupe,
    apply_derived_fields,
    apply_filters,
    apply_quality_rules,
    apply_select_map,
)
from batch.writers.delta_writer import (  # noqa: E402
    merge_to_target,
    validate_partition_columns,
)
from shared.control.job_spec_store import load_current_job_spec  # noqa: E402
from shared.core.spark import create_spark  # noqa: E402
from shared.runtime.control_run_context import (  # noqa: E402
    build_runtime_context,
    control_run,
)


# -----------------------------------------------------------------------------
# CLI / Spark / Spec
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog-path", required=True)
    parser.add_argument("--pipeline-name", required=True)
    parser.add_argument("--job-name", required=True)
    return parser.parse_args()


def build_spark() -> SparkSession:
    return create_spark("generic_bronze_to_silver")


def load_job_spec(catalog_path: str, pipeline_name: str, job_name: str) -> dict[str, Any]:
    """
    Primary source: PostgreSQL meta.job.config through CONTROL_JOB_CODE / CONTROL_JOB_KEY.
    Fallback: design-time JSON catalog for local/dev compatibility.
    """
    try:
        return load_current_job_spec()
    except Exception as exc:
        print(
            f"[WARN] Could not load job spec from meta.job.config; "
            f"falling back to JSON catalog. error={exc}",
            flush=True,
        )

    job = get_job_by_name(catalog_path, pipeline_name, job_name)

    if job["job_type"] != "generic_bronze_to_silver":
        raise ValueError(
            f"Job '{job_name}' is not generic_bronze_to_silver; got '{job['job_type']}'"
        )

    return job["spec"]


# -----------------------------------------------------------------------------
# Metrics / Incremental helpers
# -----------------------------------------------------------------------------

def _to_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default

    try:
        return int(value)
    except Exception:
        return default


def _metric_int(metrics: dict[str, Any], keys: list[str], default: int = 0) -> int:
    for key in keys:
        if key in metrics and metrics.get(key) is not None:
            return _to_int(metrics.get(key), default=default)
    return default


def _is_delta_table(spark: SparkSession, path: str) -> bool:
    if not path:
        return False

    try:
        return bool(DeltaTable.isDeltaTable(spark, path))
    except Exception:
        try:
            spark.read.format("delta").load(path).limit(1).count()
            return True
        except Exception:
            return False


def _target_max_watermark(
    spark: SparkSession,
    target_path: str,
    watermark_column: str,
) -> Any:
    if not target_path or not _is_delta_table(spark, target_path):
        return None

    target_df = spark.read.format("delta").load(target_path)

    if watermark_column not in target_df.columns:
        print(
            f"[INCREMENTAL_TARGET_NO_WATERMARK_COLUMN] "
            f"target={target_path} watermark_column={watermark_column}",
            flush=True,
        )
        return None

    row = target_df.agg(F.max(F.col(watermark_column)).alias("watermark")).first()

    if not row:
        return None

    return row["watermark"]


def _apply_incremental_filter(
    *,
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    incremental_spec: dict[str, Any],
) -> tuple[DataFrame, Any]:
    """
    Incremental strategy for Bronze -> Silver cleanup jobs.

    Current supported strategy:
      target_max_watermark:
        Read max(watermark_column) from the target Silver Delta table and filter
        the primary Bronze input with source.watermark_column > last target watermark.

    Required catalog shape:
      "incremental": {
        "enabled": true,
        "strategy": "target_max_watermark",
        "watermark_column": "ingestion_ts",
        "lookback_minutes": 0,
        "source_alias": "bronze"
      }
    """
    if not incremental_spec or not incremental_spec.get("enabled", False):
        return source_df, None

    strategy = str(incremental_spec.get("strategy", "target_max_watermark"))
    watermark_column = str(incremental_spec.get("watermark_column", "ingestion_ts"))
    lookback_minutes = int(incremental_spec.get("lookback_minutes", 0) or 0)

    if strategy != "target_max_watermark":
        raise ValueError(f"Unsupported incremental strategy: {strategy}")

    if watermark_column not in source_df.columns:
        raise ValueError(
            f"Incremental watermark column `{watermark_column}` not found in source dataframe. "
            f"Available columns: {source_df.columns}"
        )

    last_watermark = _target_max_watermark(
        spark=spark,
        target_path=target_path,
        watermark_column=watermark_column,
    )

    if last_watermark is None:
        print(
            f"[INCREMENTAL_FULL_FIRST_RUN] "
            f"target={target_path} watermark_column={watermark_column}",
            flush=True,
        )
        return source_df, None

    effective_watermark = last_watermark

    if lookback_minutes > 0:
        effective_watermark = last_watermark - timedelta(minutes=lookback_minutes)

    print(
        f"[INCREMENTAL_FILTER] "
        f"target={target_path} "
        f"watermark_column={watermark_column} "
        f"last_watermark={last_watermark} "
        f"effective_watermark={effective_watermark}",
        flush=True,
    )

    filtered_df = source_df.where(
        F.col(watermark_column) > F.lit(effective_watermark).cast("timestamp")
    )

    return filtered_df, last_watermark


# -----------------------------------------------------------------------------
# SQL/View contract
# -----------------------------------------------------------------------------

def _safe_view_name(value: str) -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", value or ""):
        raise ValueError(
            f"Invalid Spark SQL view alias: {value!r}. "
            "Use letters, digits, and underscores; first character must be a letter or underscore."
        )
    return value


def _sql_query_from_spec(spec: dict[str, Any]) -> str | None:
    sql_spec = spec.get("sql") or spec.get("query")

    if not sql_spec:
        return None

    if isinstance(sql_spec, str):
        return sql_spec

    if isinstance(sql_spec, dict):
        return sql_spec.get("query") or sql_spec.get("statement")

    raise ValueError("spec.sql/spec.query must be a string or an object with query/statement")


def _view_list_from_spec(spec: dict[str, Any], *, default_alias: str) -> list[dict[str, Any]]:
    """
    Official contract:
      spec.views = [
        {"alias": "bronze", "path": "s3a://...", "format": "delta"}
      ]

    Backward-compatible aliases:
      spec.sources
      spec.source
    """
    if isinstance(spec.get("views"), list) and spec["views"]:
        return spec["views"]

    if isinstance(spec.get("sources"), list) and spec["sources"]:
        return spec["sources"]

    if isinstance(spec.get("source"), dict):
        source = dict(spec["source"])
        source.setdefault("alias", source.get("name") or default_alias)
        return [source]

    raise ValueError("Silver job requires spec.source, spec.sources, or spec.views")


def load_silver_input_dataframe(
    *,
    spark: SparkSession,
    spec: dict[str, Any],
    target_path: str,
) -> tuple[DataFrame, int, Any]:
    """
    Silver layer purpose:
      source-specific cleanup, parsing, schema normalization, validation, and dedupe.

    Supported transformation contracts:
      1. SQL-first mode:
         spec.views + spec.sql.query
         Each view is registered as a Spark temp view using view.alias.
         SQL output becomes the Silver dataframe before post-rules.

      2. Legacy/rule mode:
         spec.source + filters/select_map/derived_fields/quality_rules/dedupe
    """
    views = _view_list_from_spec(spec, default_alias="bronze")
    sql_query = _sql_query_from_spec(spec)
    incremental_spec = spec.get("incremental", {}) or {}

    primary_alias = (
        incremental_spec.get("source_alias")
        or spec.get("primary_view_alias")
        or spec.get("primary_source_alias")
        or views[0].get("alias")
        or views[0].get("name")
        or "bronze"
    )

    view_frames: dict[str, DataFrame] = {}
    primary_count = 0
    last_watermark = None

    for index, view in enumerate(views):
        alias = _safe_view_name(str(view.get("alias") or view.get("name") or f"view_{index}"))
        source_path = view["path"]
        source_format = view.get("format", "delta")

        df = spark.read.format(source_format).load(source_path)

        if alias == primary_alias:
            df, last_watermark = _apply_incremental_filter(
                spark=spark,
                source_df=df,
                target_path=target_path,
                incremental_spec=incremental_spec,
            )
            primary_count = int(df.count())

        view_frames[alias] = df
        df.createOrReplaceTempView(alias)

    if not view_frames:
        raise ValueError("No Silver input view was loaded")

    if primary_count == 0 and incremental_spec.get("enabled", False):
        first_alias = next(iter(view_frames))
        return view_frames[first_alias], 0, last_watermark

    if sql_query:
        silver_df = spark.sql(sql_query)
        input_count = primary_count if incremental_spec.get("enabled", False) else sum(
            int(df.count()) for df in view_frames.values()
        )
        return silver_df, input_count, last_watermark

    # Legacy/rule mode uses the first loaded source dataframe.
    first_alias = next(iter(view_frames))
    source_df = view_frames[first_alias]
    input_count = primary_count if incremental_spec.get("enabled", False) else int(source_df.count())

    return source_df, input_count, last_watermark


def build_silver_dataframe(input_df: DataFrame, spec: dict[str, Any]) -> DataFrame:
    """
    Apply post-SQL or legacy rule-based cleanup.

    SQL can fully define the Silver shape. These rules are kept as optional
    post-processing hooks for additional filters, derived fields, quality checks,
    and dedupe.
    """
    return (
        input_df
        .transform(lambda df: apply_filters(df, spec.get("filters", [])))
        .transform(lambda df: apply_select_map(df, spec.get("select_map", {})))
        .transform(lambda df: apply_derived_fields(df, spec.get("derived_fields", {})))
        .transform(lambda df: apply_quality_rules(df, spec.get("quality_rules", [])))
        .transform(lambda df: apply_dedupe(df, spec.get("dedupe", {})))
    )


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    context = build_runtime_context(
        default_pipeline_name=args.pipeline_name,
        default_job_name=args.job_name,
        default_job_code=f"silver.{args.pipeline_name}.{args.job_name}",
        default_base_job_name=args.job_name,
        default_layer="silver",
        default_runner="generic_bronze_to_silver",
    )

    spark: SparkSession | None = None

    with control_run(context=context) as run_ctx:
        try:
            spark = build_spark()

            spec = load_job_spec(
                args.catalog_path,
                args.pipeline_name,
                args.job_name,
            )

            target_spec = spec.get("target") or {}
            target_path = target_spec["path"]
            target_format = target_spec.get("format", "delta")
            target_mode = target_spec.get("mode", "merge")
            merge_keys = target_spec.get("merge_keys", [])
            partition_by = target_spec.get("partition_by", [])

            run_ctx["target_path"] = target_path

            silver_input_df, input_count, last_watermark = load_silver_input_dataframe(
                spark=spark,
                spec=spec,
                target_path=target_path,
            )

            if input_count == 0:
                run_ctx["records_read"] = 0
                run_ctx["records_written"] = 0
                run_ctx["records_inserted"] = 0
                run_ctx["records_updated"] = 0
                run_ctx["records_deleted"] = 0
                run_ctx["target_path"] = target_path

                print(
                    f"[SILVER_SKIP_NO_NEW_ROWS] "
                    f"target={target_path} "
                    f"last_watermark={last_watermark}",
                    flush=True,
                )
                return

            silver_df = build_silver_dataframe(silver_input_df, spec)
            output_count = int(silver_df.count())

            if output_count == 0:
                run_ctx["records_read"] = input_count
                run_ctx["records_written"] = 0
                run_ctx["records_inserted"] = 0
                run_ctx["records_updated"] = 0
                run_ctx["records_deleted"] = 0
                run_ctx["target_path"] = target_path

                print(
                    f"[SILVER_SKIP_EMPTY_AFTER_CLEANUP] "
                    f"read={input_count} "
                    f"target={target_path}",
                    flush=True,
                )
                return

            validate_partition_columns(silver_df, partition_by)

            run_ctx["records_read"] = input_count

            if target_mode == "merge":
                if not merge_keys:
                    raise ValueError("Target mode 'merge' requires target.merge_keys")

                merge_metrics = merge_to_target(
                    spark=spark,
                    target_path=target_path,
                    df=silver_df,
                    merge_keys=merge_keys,
                    partition_by=partition_by,
                )

                records_written = _metric_int(
                    merge_metrics,
                    ["records_written", "numOutputRows", "numTargetRowsInserted"],
                    default=output_count,
                )
                records_inserted = _metric_int(
                    merge_metrics,
                    ["records_inserted", "numTargetRowsInserted"],
                    default=records_written,
                )
                records_updated = _metric_int(
                    merge_metrics,
                    ["records_updated", "numTargetRowsUpdated"],
                    default=0,
                )
                records_deleted = _metric_int(
                    merge_metrics,
                    ["records_deleted", "numTargetRowsDeleted"],
                    default=0,
                )

                run_ctx["records_written"] = records_written
                run_ctx["records_inserted"] = records_inserted
                run_ctx["records_updated"] = records_updated
                run_ctx["records_deleted"] = records_deleted
                run_ctx["delta_operation_metrics"] = merge_metrics.get("delta_operation_metrics", {})

            else:
                writer = silver_df.write.format(target_format).mode(target_mode)

                if target_format.lower() == "delta":
                    writer = writer.option("mergeSchema", "true")

                if partition_by:
                    writer = writer.partitionBy(*partition_by)

                writer.save(target_path)

                run_ctx["records_written"] = output_count
                run_ctx["records_inserted"] = output_count
                run_ctx["records_updated"] = 0
                run_ctx["records_deleted"] = 0

            run_ctx["target_path"] = target_path

            print(
                f"[SILVER_WRITE_OK] "
                f"job_code={context.get('job_code')} "
                f"read={run_ctx.get('records_read')} "
                f"written={run_ctx.get('records_written')} "
                f"inserted={run_ctx.get('records_inserted')} "
                f"updated={run_ctx.get('records_updated')} "
                f"deleted={run_ctx.get('records_deleted')} "
                f"target={target_path}",
                flush=True,
            )

        finally:
            if spark is not None:
                spark.stop()


if __name__ == "__main__":
    main()
