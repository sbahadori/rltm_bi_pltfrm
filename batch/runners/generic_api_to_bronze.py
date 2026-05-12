from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
import time

import sys
from pathlib import Path
from shared.core.spark import create_spark

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
    return create_spark("generic_api_to_bronze")


def verify_bronze_written(spark: SparkSession, target_path: str) -> None:
    if not DeltaTable.isDeltaTable(spark, target_path):
        raise RuntimeError(f"Bronze Delta table was not created: {target_path}")

    count = spark.read.format("delta").load(target_path).count()

    if count < 1:
        raise RuntimeError(f"Bronze Delta table is empty: {target_path}")

    print(f"[BRONZE_WRITE_OK] path={target_path}, rows={count}", flush=True)


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
    spark = None
    records_written = 0
    target_path = None

    try:
        print(
            f"[START] pipeline={args.pipeline_name}, job={args.job_name}",
            flush=True,
        )

        job = get_job_by_name(
            args.catalog_path,
            args.pipeline_name,
            args.job_name,
        )

        if job["job_type"] != "generic_api_to_bronze":
            raise ValueError(
                f"Job '{args.job_name}' is not generic_api_to_bronze; "
                f"got '{job['job_type']}'"
            )

        runtime_ctx = build_runtime_context(job)
        print("[STEP] runtime context built", flush=True)

        payload = execute_api_request(runtime_ctx)
        print("[STEP] API request executed", flush=True)

        validate_payload(payload, runtime_ctx["validation"])
        print("[STEP] payload validated", flush=True)

        row = map_payload_to_row(payload, runtime_ctx)
        print(
            f"[STEP] mapped row columns={list(row.keys())}",
            flush=True,
        )

        spark = build_spark()

        df = build_bronze_dataframe(
            spark=spark,
            row=row,
            schema=runtime_ctx["schema"],
        )
        records_written = df.count()

        print(f"[STEP] dataframe created rows={records_written}", flush=True)

        write_bronze_dataframe(df, runtime_ctx["bronze_write"])

        target_path = runtime_ctx["bronze_write"]["target_path"]

        print(f"[STEP] write finished target={target_path}", flush=True)

        verify_bronze_written(spark, target_path)

        ended = time.time()

        append_job_event(
            event_type="batch_succeeded",
            run_id=run_id,
            type="batch",
            pipeline=args.pipeline_name,
            job=args.job_name,
            job_id=f"{args.pipeline_name}__{args.job_name}",
            status="success",
            started_at_epoch=int(started),
            ended_at_epoch=int(ended),
            duration_seconds=round(ended - started, 3),
            target_path=target_path,
            records_written=records_written,
        )

        print(
            f"[SUCCESS] Wrote Bronze rows for job '{args.job_name}' to {target_path}",
            flush=True,
        )

    except Exception as exc:
        ended = time.time()

        append_job_event(
            event_type="batch_failed",
            run_id=run_id,
            type="batch",
            pipeline=args.pipeline_name,
            job=args.job_name,
            job_id=f"{args.pipeline_name}__{args.job_name}",
            status="failed",
            started_at_epoch=int(started),
            ended_at_epoch=int(ended),
            duration_seconds=round(ended - started, 3),
            error=str(exc),
            traceback=exception_to_text(exc),
        )

        raise

    finally:
        if spark is not None:
            spark.stop()

if __name__ == "__main__":
    main()