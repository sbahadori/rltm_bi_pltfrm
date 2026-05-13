from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from shared.core.spark import create_spark

REPO_ROOT = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from batch.specs.batch_catalog_utils import get_job_by_name  # noqa: E402
from batch.utils.api_load_state import (  # noqa: E402
    build_api_state_key,
    get_api_state_path,
    read_api_state,
    write_api_state,
)
from batch.utils.generic_api_job_utils import (  # noqa: E402
    apply_api_state_to_request,
    build_bronze_dataframe,
    build_runtime_context,
    execute_api_request,
    extract_state_value_from_payload,
    map_payload_to_row,
    validate_payload,
    write_bronze_dataframe,
)
from shared.runtime.job_run_registry import (  # noqa: E402
    append_job_event,
    exception_to_text,
    new_run_id,
)


VALID_API_LOAD_TYPES = {"event", "full", "incremental"}
VALID_API_STRATEGIES_BY_LOAD_TYPE = {
    "event": {"append_event"},
    "full": {"overwrite", "append_snapshot"},
    "incremental": {"timestamp", "numeric_watermark", "cursor"},
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog-path", required=True)
    parser.add_argument("--pipeline-name", required=True)
    parser.add_argument("--job-name", required=True)
    return parser.parse_args()


def build_spark() -> SparkSession:
    return create_spark("generic_api_to_bronze")


def validate_load_contract(runtime_ctx: dict[str, Any]) -> None:
    load_type = runtime_ctx.get("load_type", "event")
    strategy = runtime_ctx.get("strategy", "append_event")

    if load_type not in VALID_API_LOAD_TYPES:
        raise ValueError(
            f"Unsupported API load_type='{load_type}'. "
            f"Valid values: {sorted(VALID_API_LOAD_TYPES)}"
        )

    valid_strategies = VALID_API_STRATEGIES_BY_LOAD_TYPE[load_type]
    if strategy not in valid_strategies:
        raise ValueError(
            f"Unsupported API strategy='{strategy}' for load_type='{load_type}'. "
            f"Valid strategies: {sorted(valid_strategies)}"
        )


def apply_bronze_mode_from_load_strategy(runtime_ctx: dict[str, Any]) -> None:
    load_type = runtime_ctx.get("load_type", "event")
    strategy = runtime_ctx.get("strategy", "append_event")
    bronze_write = runtime_ctx["bronze_write"]

    if load_type == "event":
        bronze_write["mode"] = bronze_write.get("mode", "append")

    elif load_type == "full" and strategy == "overwrite":
        bronze_write["mode"] = bronze_write.get("mode", "overwrite")

    elif load_type == "full" and strategy == "append_snapshot":
        bronze_write["mode"] = bronze_write.get("mode", "append")

    elif load_type == "incremental":
        bronze_write["mode"] = bronze_write.get("mode", "append")


def verify_bronze_written(spark: SparkSession, target_path: str) -> None:
    if not DeltaTable.isDeltaTable(spark, target_path):
        raise RuntimeError(f"Bronze Delta table was not created: {target_path}")

    print(f"[BRONZE_WRITE_OK] path={target_path}", flush=True)


def prepare_incremental_api_context(
    spark: SparkSession,
    runtime_ctx: dict[str, Any],
) -> tuple[dict[str, Any], str | None, str | None]:
    load_type = runtime_ctx.get("load_type", "event")

    if load_type != "incremental":
        return runtime_ctx, None, None

    state_cfg = runtime_ctx.get("state") or {}

    if not state_cfg:
        raise ValueError("API incremental load requires spec.state configuration")

    state_key = build_api_state_key(runtime_ctx)
    state_path = get_api_state_path(runtime_ctx)

    lower_bound = read_api_state(
        spark,
        state_path=state_path,
        pipeline_name=runtime_ctx["pipeline_name"],
        job_name=runtime_ctx["job_name"],
        state_key=state_key,
        default_value=state_cfg.get("initial_value", ""),
    )

    print(
        f"[API_INCREMENTAL_STATE] pipeline={runtime_ctx['pipeline_name']} "
        f"job={runtime_ctx['job_name']} "
        f"load_type={runtime_ctx.get('load_type')} "
        f"strategy={runtime_ctx.get('strategy')} "
        f"state_key={state_key} lower_bound={lower_bound}",
        flush=True,
    )

    runtime_ctx = apply_api_state_to_request(
        runtime_ctx,
        lower_bound=lower_bound,
    )

    return runtime_ctx, state_path, state_key


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

        runtime_ctx = build_runtime_context(
            job,
            pipeline_name=args.pipeline_name,
        )

        validate_load_contract(runtime_ctx)
        apply_bronze_mode_from_load_strategy(runtime_ctx)

        print(
            f"[API_LOAD_CONTRACT] load_type={runtime_ctx.get('load_type')} "
            f"strategy={runtime_ctx.get('strategy')}",
            flush=True,
        )

        spark = build_spark()

        runtime_ctx, state_path, state_key = prepare_incremental_api_context(
            spark=spark,
            runtime_ctx=runtime_ctx,
        )

        payload = execute_api_request(runtime_ctx)
        print("[STEP] API request executed", flush=True)

        validate_payload(payload, runtime_ctx["validation"])
        print("[STEP] payload validated", flush=True)

        row = map_payload_to_row(payload, runtime_ctx)
        print(
            f"[STEP] mapped row columns={list(row.keys())}",
            flush=True,
        )

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

        if runtime_ctx.get("load_type") == "incremental":
            next_state_value = extract_state_value_from_payload(payload, runtime_ctx)

            if next_state_value is None:
                raise ValueError(
                    "API incremental load could not extract next state value. "
                    "Configure spec.state.response_path or spec.state.update_value."
                )

            write_api_state(
                spark,
                state_path=state_path,
                pipeline_name=runtime_ctx["pipeline_name"],
                job_name=runtime_ctx["job_name"],
                state_key=state_key,
                state_value=next_state_value,
                batch_run_id=run_id,
            )

            print(
                f"[API_STATE_UPDATED] state_key={state_key} value={next_state_value}",
                flush=True,
            )

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