from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any
import time
from pyspark.sql import  SparkSession


import sys
from pathlib import Path
from shared.core.spark import create_spark

REPO_ROOT = Path(__file__).resolve().parents[2]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
    


def _bootstrap_repo_path() -> Path:
    repo_root = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    return repo_root


REPO_ROOT = _bootstrap_repo_path()

from batch.specs.batch_catalog_utils import get_job_by_name  # noqa: E402

from shared.runtime.control_run_context import build_runtime_context, control_run

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

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog-path", required=True)
    parser.add_argument("--pipeline-name", required=True)
    parser.add_argument("--job-name", required=True)
    return parser.parse_args()


def build_spark() -> SparkSession:
    return create_spark("generic_bronze_to_silver")

from shared.control.job_spec_store import load_current_job_spec

def load_job_spec(catalog_path: str, pipeline_name: str, job_name: str) -> dict[str, Any]:
    try:
        return load_current_job_spec()
    except Exception:
        job = get_job_by_name(catalog_path, pipeline_name, job_name)
        if job["job_type"] != "generic_bronze_to_silver":
            raise ValueError(
                f"Job '{job_name}' is not generic_bronze_to_silver; got '{job['job_type']}'"
            )
        return job["spec"]
    

def main():
    args = parse_args()

    context = build_runtime_context(
        default_pipeline_name=args.pipeline_name,
        default_job_name=args.job_name,
        default_job_code=f"silver.{args.pipeline_name}.{args.job_name}",
        default_base_job_name=args.job_name,
        default_layer="silver",
        default_runner="generic_bronze_to_silver",
    )

    spark = None

    with control_run(context=context) as run_ctx:
        try:
            spark = build_spark()

            spec = load_job_spec(
                args.catalog_path,
                args.pipeline_name,
                args.job_name,
            )

            source_path = spec["source"]["path"]
            target_path = spec["target"]["path"]
            merge_keys = spec["target"].get("merge_keys", [])

            run_ctx["target_path"] = target_path

            bronze_df = spark.read.format(
                spec["source"].get("format", "delta")
            ).load(source_path)

            silver_df = (
                bronze_df.transform(lambda df: apply_filters(df, spec.get("filters", [])))
                .transform(lambda df: apply_select_map(df, spec["select_map"]))
                .transform(lambda df: apply_derived_fields(df, spec.get("derived_fields", {})))
                .transform(lambda df: apply_quality_rules(df, spec.get("quality_rules", [])))
                .transform(lambda df: apply_dedupe(df, spec.get("dedupe", {})))
            )

            if silver_df.rdd.isEmpty():
                run_ctx["records_written"] = 0
                print("No rows to write to Silver.")
                return

            output_count = silver_df.count()

            if spec["target"].get("mode", "merge") == "merge":
                if not merge_keys:
                    raise ValueError("Target mode 'merge' requires target.merge_keys")

                partition_by = spec["target"].get("partition_by", [])

                merge_to_target(
                    spark=spark,
                    target_path=target_path,
                    df=silver_df,
                    merge_keys=merge_keys,
                    partition_by=partition_by,
                )
            else:
                writer = silver_df.write.format(
                    spec["target"].get("format", "delta")
                ).mode(
                    spec["target"].get("mode", "append")
                )

                partition_by = spec["target"].get("partition_by", [])

                if partition_by:
                    writer = writer.partitionBy(*partition_by)

                writer.save(target_path)

            run_ctx["records_written"] = output_count
            run_ctx["target_path"] = target_path

            print(f"Wrote Silver rows to {target_path}")

        finally:
            if spark is not None:
                spark.stop()


if __name__ == "__main__":
    main()