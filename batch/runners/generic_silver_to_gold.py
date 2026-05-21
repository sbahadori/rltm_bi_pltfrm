from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, expr, lit, row_number

from batch.transforms.rules import (
    apply_select_map,
    apply_filters,
    apply_derived_fields,
    apply_quality_rules,
    apply_dedupe,
)

from batch.writers.delta_writer import (
    merge_to_target,
    validate_partition_columns,
)

# -----------------------------------------------------------------------------
# Bootstrap repo path
# -----------------------------------------------------------------------------

REPO_ROOT = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from shared.core.spark import create_spark  # noqa: E402
from batch.specs.batch_catalog_utils import get_job_by_name  # noqa: E402
from shared.control.job_spec_store import load_current_job_spec  # noqa: E402
from shared.runtime.control_run_context import build_runtime_context, control_run  # noqa: E402


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog-path", required=True)
    parser.add_argument("--pipeline-name", required=True)
    parser.add_argument("--job-name", required=True)
    return parser.parse_args()


# -----------------------------------------------------------------------------
# Spark
# -----------------------------------------------------------------------------

def build_spark() -> SparkSession:
    return create_spark("generic_silver_to_gold")


# -----------------------------------------------------------------------------
# Spec loading
# -----------------------------------------------------------------------------

def load_job_spec(
    catalog_path: str,
    pipeline_name: str,
    job_name: str,
) -> dict[str, Any]:
    """
    Primary source:
      PostgreSQL meta.job.config through CONTROL_JOB_CODE / CONTROL_JOB_KEY.

    Optional fallback:
      JSON catalog only when ALLOW_JSON_SPEC_FALLBACK=true.
    """
    try:
        return load_current_job_spec()
    except Exception as exc:
        allow_fallback = os.getenv("ALLOW_JSON_SPEC_FALLBACK", "false").lower() in {
            "1",
            "true",
            "yes",
        }

        if not allow_fallback:
            raise RuntimeError(
                "Failed to load Silver-to-Gold job spec from meta.job.config. "
                "JSON fallback is disabled. "
                f"pipeline_name={pipeline_name}, job_name={job_name}, error={exc}"
            ) from exc

        print(
            f"[WARN] Could not load job spec from meta.job.config; "
            f"falling back to JSON catalog. error={exc}",
            flush=True,
        )

        job = get_job_by_name(catalog_path, pipeline_name, job_name)

        if job["job_type"] != "generic_silver_to_gold":
            raise ValueError(
                f"Job '{job_name}' is not generic_silver_to_gold; "
                f"got '{job['job_type']}'"
            )

        return job["spec"]


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    context = build_runtime_context(
        default_pipeline_name=args.pipeline_name,
        default_job_name=args.job_name,
        default_job_code=f"gold.{args.pipeline_name}.{args.job_name}",
        default_base_job_name=args.job_name,
        default_layer="gold",
        default_runner="generic_silver_to_gold",
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

            source_path = spec["source"]["path"]
            source_format = spec["source"].get("format", "delta")

            target_path = spec["target"]["path"]
            target_format = spec["target"].get("format", "delta")
            target_mode = spec["target"].get("mode", "merge")
            merge_keys = spec["target"].get("merge_keys", [])
            partition_by = spec["target"].get("partition_by", [])

            run_ctx["target_path"] = target_path

            silver_input_df = spark.read.format(source_format).load(source_path)

            gold_df = (
                silver_input_df
                .transform(lambda df: apply_filters(df, spec.get("filters", [])))
                .transform(lambda df: apply_select_map(df, spec.get("select_map", {})))
                .transform(lambda df: apply_derived_fields(df, spec.get("derived_fields", {})))
                .transform(lambda df: apply_quality_rules(df, spec.get("quality_rules", [])))
                .transform(lambda df: apply_dedupe(df, spec.get("dedupe", {})))
            )

            if gold_df.rdd.isEmpty():
                run_ctx["records_read"] = 0
                run_ctx["records_written"] = 0
                run_ctx["records_inserted"] = 0
                run_ctx["records_updated"] = 0
                run_ctx["records_deleted"] = 0
                print("[GOLD_SKIP_EMPTY] No rows to write to Gold.", flush=True)
                return

            input_count = silver_input_df.count()
            output_count = gold_df.count()

            run_ctx["records_read"] = input_count

            validate_partition_columns(gold_df, partition_by)

            if target_mode == "merge":
                if not merge_keys:
                    raise ValueError("Target mode 'merge' requires target.merge_keys")

                merge_to_target(
                    spark=spark,
                    target_path=target_path,
                    df=gold_df,
                    merge_keys=merge_keys,
                    partition_by=partition_by,
                )
            else:
                writer = gold_df.write.format(target_format).mode(target_mode)

                if partition_by:
                    writer = writer.partitionBy(*partition_by)

                writer.save(target_path)

            run_ctx["records_read"] = output_count
            run_ctx["records_written"] = output_count
            run_ctx["target_path"] = target_path

            print(
                f"[GOLD_WRITE_OK] "
                f"job_code={context.get('job_code')} "
                f"records={output_count} "
                f"target={target_path}",
                flush=True,
            )

        finally:
            if spark is not None:
                spark.stop()


if __name__ == "__main__":
    main()