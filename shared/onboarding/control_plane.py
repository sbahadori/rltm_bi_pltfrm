from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from shared.control.postgres import get_conn


REPO_ROOT = Path(
    os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")
).resolve()

def parse_selections_json(value: str | None) -> list[dict[str, Any]]:
    if not value:
        return []

    value = value.strip()

    if not value:
        return []

    selections = json.loads(value)

    if isinstance(selections, dict):
        selections = [selections]

    if not isinstance(selections, list):
        raise ValueError("--selections-json must be a JSON array or object")

    normalized: list[dict[str, Any]] = []

    for i, item in enumerate(selections):
        if not isinstance(item, dict):
            raise ValueError(f"Selection at index {i} must be a JSON object")

        pipeline_name = item.get("pipeline_name")
        job_name = item.get("job_name")
        table_id = item.get("table_id")
        include_dependencies = item.get("include_dependencies", True)

        if not pipeline_name:
            raise ValueError(f"selection[{i}].pipeline_name is required")

        if table_id and not job_name:
            raise ValueError(
                f"selection[{i}].job_name is required when table_id is provided"
            )

        normalized.append(
            {
                "pipeline_name": pipeline_name,
                "job_name": job_name,
                "table_id": table_id,
                "include_dependencies": bool(include_dependencies),
            }
        )

    return normalized

def _validate_proc_name(proc_name: str) -> str:
    if not proc_name.startswith("usp_"):
        raise ValueError(f"Procedure name must start with usp_: {proc_name}")

    if not all(ch.isalnum() or ch == "_" for ch in proc_name):
        raise ValueError(f"Invalid procedure name: {proc_name}")

    return proc_name


def dumps(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=False, default=str)


def load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)

    if not p.is_absolute():
        p = REPO_ROOT / p

    with p.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def call_proc(
    cur,
    proc_name: str,
    params: tuple[Any, ...],
    *,
    dry_run: bool = False,
) -> None:
    proc_name = _validate_proc_name(proc_name)

    if dry_run:
        print(f"[DRY_RUN] CALL ctl.{proc_name} params={params}", flush=True)
        return

    placeholders = ", ".join(["%s"] * len(params))
    cur.execute(f"CALL ctl.{proc_name}({placeholders})", params)


def infer_layer(job: dict[str, Any]) -> str:
    job_type = str(job.get("job_type", "")).lower()
    job_name = str(job.get("name", "")).lower()

    if job_type == "generic_api_to_bronze" or "bronze" in job_type or "bronze" in job_name:
        return "bronze"

    if "silver" in job_type or "silver" in job_name:
        return "silver"

    if "gold" in job_type or "gold" in job_name:
        return "gold"

    return "batch"


def target_path_from_spec(spec: dict[str, Any]) -> str:
    return (
        spec.get("bronze_write", {}).get("target_path")
        or spec.get("silver_write", {}).get("target_path")
        or spec.get("gold_write", {}).get("target_path")
        or spec.get("target", {}).get("target_path")
        or spec.get("target", {}).get("path")
        or spec.get("write", {}).get("target_path")
        or spec.get("write", {}).get("path")
        or ""
    )


def source_path_from_spec(spec: dict[str, Any]) -> str:
    return (
        spec.get("source", {}).get("path")
        or spec.get("source", {}).get("base_url")
        or spec.get("api", {}).get("base_url")
        or ""
    )


def select_pipelines(
    catalog: dict[str, Any],
    pipeline_name: str | None,
) -> list[dict[str, Any]]:
    pipelines = [
        p for p in catalog.get("pipelines", [])
        if p.get("enabled", True)
    ]

    if not pipeline_name:
        return pipelines

    selected = [
        p for p in pipelines
        if p.get("name") == pipeline_name
    ]

    if not selected:
        raise ValueError(f"Pipeline not found or disabled: {pipeline_name}")

    return selected


def select_jobs(
    pipeline: dict[str, Any],
    job_name: str | None,
    *,
    include_dependencies: bool,
) -> list[dict[str, Any]]:
    enabled_jobs = [
        j for j in pipeline.get("jobs", [])
        if j.get("enabled", True)
    ]

    if not job_name:
        return enabled_jobs

    jobs_by_name = {
        j["name"]: j
        for j in enabled_jobs
    }

    # Allows generated manifest task names, e.g. jdbc_ingest__customers
    base_job_name = job_name.split("__", 1)[0] if "__" in job_name else job_name

    if job_name in jobs_by_name:
        root_name = job_name
    elif base_job_name in jobs_by_name:
        root_name = base_job_name
    else:
        raise ValueError(
            f"Job not found or disabled: pipeline={pipeline.get('name')} job={job_name}"
        )

    selected: dict[str, dict[str, Any]] = {}

    def add_job(name: str) -> None:
        if name in selected:
            return

        job = jobs_by_name.get(name)

        if not job:
            raise ValueError(
                f"Dependency job not found in pipeline={pipeline.get('name')}: {name}"
            )

        selected[name] = job

        if include_dependencies:
            for dep_name in job.get("dependencies", []) or []:
                add_job(dep_name)

    add_job(root_name)

    return [
        j for j in enabled_jobs
        if j["name"] in selected
    ]


def regular_job_code(pipeline_name: str, job: dict[str, Any]) -> str:
    layer = infer_layer(job)
    return f"{layer}.{pipeline_name}.{job['name']}"


def load_manifest(manifest_ref: str) -> dict[str, Any]:
    return load_json(manifest_ref)


def enabled_manifest_tables(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        t for t in manifest.get("tables", [])
        if t.get("enabled", True)
    ]


def manifest_target_path(manifest: dict[str, Any], table: dict[str, Any]) -> str:
    source_id = manifest["source_id"]
    table_id = table["table_id"]

    return (
        table.get("target_path")
        or (manifest.get("defaults") or {})
        .get("target_path_template", "")
        .format(source_id=source_id, table_id=table_id)
    )


def resolve_manifest_table_id_filter(
    *,
    base_job_name: str,
    requested_job_name: str | None,
    requested_table_id: str | None,
) -> str | None:
    if requested_table_id:
        return requested_table_id

    if requested_job_name and requested_job_name.startswith(f"{base_job_name}__"):
        return requested_job_name.split("__", 1)[1]

    return None


def onboard_pipeline(
    cur,
    *,
    pipeline: dict[str, Any],
    dry_run: bool,
) -> None:
    dag = pipeline.get("dag", {}) or {}

    call_proc(
        cur,
        "usp_onboard_pipeline",
        (
            pipeline["name"],
            pipeline.get("domain"),
            pipeline.get("description"),
            (dag.get("default_args") or {}).get("owner"),
            dag.get("schedule"),
            pipeline["name"],
            dumps(pipeline),
        ),
        dry_run=dry_run,
    )


def onboard_regular_job(
    cur,
    *,
    pipeline: dict[str, Any],
    job: dict[str, Any],
    dry_run: bool,
) -> str:
    pipeline_name = pipeline["name"]
    spec = job.get("spec", {}) or {}
    layer = infer_layer(job)
    job_code = regular_job_code(pipeline_name, job)
    target_path = target_path_from_spec(spec)
    runtime_policy = spec.get("runtime_policy") or job.get("runtime_policy") or {}

    call_proc(
        cur,
        "usp_onboard_job",
        (
            job_code,
            job["name"],
            job.get("display_name") or job["name"],
            pipeline_name,
            job.get("base_job_name") or job["name"],
            job.get("job_type"),
            job.get("runner") or job.get("job_type"),
            layer,
            job.get("source_id"),
            job.get("table_id"),
            job.get("entity_name") or f"{pipeline_name}.{job['name']}",
            job.get("manifest_ref"),
            target_path,
            job.get("source_type") or pipeline.get("source_type"),
            dumps(spec),
            dumps(runtime_policy),
        ),
        dry_run=dry_run,
    )

    if target_path:
        dataset_key = f"{pipeline_name}.{layer}.{job['name']}"

        call_proc(
            cur,
            "usp_onboard_dataset",
            (
                dataset_key,
                job.get("display_name") or job["name"],
                None,
                source_path_from_spec(spec),
                layer,
                target_path,
                spec.get("target", {}).get("format") or "delta",
                dumps(spec.get("target", {}).get("merge_keys") or []),
                dumps(spec.get("schema") or {}),
                dumps(spec.get("contract") or {}),
            ),
            dry_run=dry_run,
        )

    return job_code


def onboard_manifest_jobs(
    cur,
    *,
    pipeline: dict[str, Any],
    job: dict[str, Any],
    requested_job_name: str | None,
    requested_table_id: str | None,
    dry_run: bool,
) -> dict[str, list[str]]:
    """
    Handles generic_jdbc_manifest_to_bronze.

    Current Airflow builder uses:
      one_task_per_manifest:
        CONTROL_JOB_CODE=bronze.{source_id}.manifest

      one_task_per_table:
        CONTROL_JOB_CODE=bronze.{source_id}.{table_id}

    This must match the builder contract.
    """

    pipeline_name = pipeline["name"]
    manifest_ref = job["manifest_ref"]
    manifest = load_manifest(manifest_ref)

    source_id = manifest["source_id"]
    strategy = job.get("execution_strategy", "one_task_per_manifest")

    call_proc(
        cur,
        "usp_onboard_source_system",
        (
            source_id,
            manifest.get("source_name") or source_id,
            manifest.get("source_type") or "jdbc",
            manifest.get("connection_ref"),
            manifest.get("owner"),
            manifest.get("environment"),
        ),
        dry_run=dry_run,
    )

    aliases: dict[str, list[str]] = {}

    if strategy == "one_task_per_manifest":
        job_code = f"bronze.{source_id}.manifest"

        call_proc(
            cur,
            "usp_onboard_job",
            (
                job_code,
                job["name"],
                job.get("display_name") or job["name"],
                pipeline_name,
                job["name"],
                "generic_jdbc_manifest_to_bronze",
                "generic_jdbc_manifest_to_bronze",
                "bronze",
                source_id,
                "manifest",
                f"{source_id}.manifest",
                manifest_ref,
                "",
                "jdbc",
                dumps({"manifest_ref": manifest_ref, "source_id": source_id}),
                dumps(job.get("runtime_policy") or {}),
            ),
            dry_run=dry_run,
        )

        aliases[job["name"]] = [job_code]
        return aliases

    if strategy != "one_task_per_table":
        raise ValueError(
            f"Unsupported generic_jdbc_manifest_to_bronze execution_strategy={strategy}"
        )

    table_id_filter = resolve_manifest_table_id_filter(
        base_job_name=job["name"],
        requested_job_name=requested_job_name,
        requested_table_id=requested_table_id,
    )

    tables = enabled_manifest_tables(manifest)

    if table_id_filter:
        tables = [
            t for t in tables
            if t.get("table_id") == table_id_filter
        ]

        if not tables:
            raise ValueError(
                f"table_id not found or disabled in manifest={manifest_ref}: {table_id_filter}"
            )

    generated_codes: list[str] = []

    for table in tables:
        table_id = table["table_id"]
        task_name = f"{job['name']}__{table_id}"
        job_code = f"bronze.{source_id}.{table_id}"
        entity_name = f"{source_id}.{table_id}"
        target_path = manifest_target_path(manifest, table)

        effective_config = {
            "manifest_ref": manifest_ref,
            "source_id": source_id,
            "table_id": table_id,
            "table": table,
            "manifest_defaults": manifest.get("defaults") or {},
        }

        call_proc(
            cur,
            "usp_onboard_job",
            (
                job_code,
                task_name,
                table.get("display_name") or task_name,
                pipeline_name,
                job["name"],
                "generic_jdbc_manifest_to_bronze",
                "generic_jdbc_manifest_to_bronze",
                "bronze",
                source_id,
                table_id,
                entity_name,
                manifest_ref,
                target_path,
                "jdbc",
                dumps(effective_config),
                dumps(job.get("runtime_policy") or {}),
            ),
            dry_run=dry_run,
        )

        call_proc(
            cur,
            "usp_onboard_dataset",
            (
                f"{source_id}.bronze.{table_id}",
                table.get("display_name") or entity_name,
                source_id,
                table.get("source_table") or table.get("table_name") or table_id,
                "bronze",
                target_path,
                "delta",
                dumps(table.get("primary_keys") or []),
                dumps(table.get("schema") or {}),
                dumps(table.get("contract") or {}),
            ),
            dry_run=dry_run,
        )

        generated_codes.append(job_code)
        aliases[task_name] = [job_code]

    aliases[job["name"]] = generated_codes
    return aliases


def onboard_dependencies(
    cur,
    *,
    jobs: list[dict[str, Any]],
    job_codes_by_alias: dict[str, list[str]],
    dry_run: bool,
) -> None:
    for job in jobs:
        current_codes = job_codes_by_alias.get(job["name"], [])

        if not current_codes:
            continue

        for dep_name in job.get("dependencies", []) or []:
            dependency_codes = job_codes_by_alias.get(dep_name, [])

            if not dependency_codes:
                print(
                    f"[WARN] Dependency '{dep_name}' not found for job '{job['name']}'. Skipping.",
                    flush=True,
                )
                continue

            for current_code in current_codes:
                for dependency_code in dependency_codes:
                    call_proc(
                        cur,
                        "usp_onboard_job_dependency",
                        (
                            current_code,
                            dependency_code,
                            "success",
                        ),
                        dry_run=dry_run,
                    )


def onboard_catalog(
    *,
    catalog_path: str,
    pipeline_name: str | None = None,
    job_name: str | None = None,
    table_id: str | None = None,
    include_dependencies: bool = True,
    dry_run: bool = False,
) -> None:
    catalog = load_json(catalog_path)
    pipelines = select_pipelines(catalog, pipeline_name)

    with get_conn() as conn:
        with conn.cursor() as cur:
            for pipeline in pipelines:
                selected_jobs = select_jobs(
                    pipeline,
                    job_name,
                    include_dependencies=include_dependencies,
                )

                print(
                    f"[ONBOARD] pipeline={pipeline['name']} "
                    f"jobs={[j['name'] for j in selected_jobs]} "
                    f"dry_run={dry_run}",
                    flush=True,
                )

                onboard_pipeline(cur, pipeline=pipeline, dry_run=dry_run)

                job_codes_by_alias: dict[str, list[str]] = {}

                for job in selected_jobs:
                    job_type = job.get("job_type")

                    if job_type == "generic_jdbc_manifest_to_bronze":
                        aliases = onboard_manifest_jobs(
                            cur,
                            pipeline=pipeline,
                            job=job,
                            requested_job_name=job_name,
                            requested_table_id=table_id,
                            dry_run=dry_run,
                        )
                        job_codes_by_alias.update(aliases)
                    else:
                        if table_id:
                            raise ValueError(
                                "--table-id is only valid for generic_jdbc_manifest_to_bronze jobs"
                            )

                        code = onboard_regular_job(
                            cur,
                            pipeline=pipeline,
                            job=job,
                            dry_run=dry_run,
                        )
                        job_codes_by_alias[job["name"]] = [code]

                if include_dependencies:
                    onboard_dependencies(
                        cur,
                        jobs=selected_jobs,
                        job_codes_by_alias=job_codes_by_alias,
                        dry_run=dry_run,
                    )

            if dry_run:
                conn.rollback()
                print("[ONBOARD] dry_run completed; transaction rolled back", flush=True)
            else:
                print("[ONBOARD] committed successfully", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--catalog",
        default="configs/batch/pipeline_catalog.json",
        help="Path to pipeline catalog JSON.",
    )

    parser.add_argument(
        "--pipeline-name",
        default=None,
        help="Onboard only this pipeline.",
    )

    parser.add_argument(
        "--job-name",
        default=None,
        help=(
            "Onboard only this job. "
            "For generated manifest tasks, use base_job__table_id."
        ),
    )

    parser.add_argument(
        "--table-id",
        default=None,
        help="For manifest one_task_per_table jobs, onboard only this table_id.",
    )

    parser.add_argument(
        "--skip-dependencies",
        action="store_true",
        help="Do not onboard upstream dependencies.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without committing DB changes.",
    )

    parser.add_argument(
        "--selections-json",
        default=None,
        help=(
            "JSON array of onboarding selections. Example: "
            """[{"pipeline_name":"p1","job_name":"j1"},{"pipeline_name":"p2","job_name":"j2"}]"""
        ),
    )

    args = parser.parse_args()

    if args.job_name and not args.pipeline_name:
        parser.error("--pipeline-name is required when --job-name is used")

    if args.table_id and not args.job_name:
        parser.error("--job-name is required when --table-id is used")

    return args


def main() -> None:
    args = parse_args()

    selections = parse_selections_json(args.selections_json)

    if selections:
        if args.pipeline_name or args.job_name or args.table_id:
            raise ValueError(
                "Use either --selections-json OR --pipeline-name/--job-name/--table-id, not both."
            )

        for selection in selections:
            print(
                "[ONBOARD_SELECTION] "
                f"pipeline_name={selection.get('pipeline_name')} "
                f"job_name={selection.get('job_name')} "
                f"table_id={selection.get('table_id')} "
                f"include_dependencies={selection.get('include_dependencies')} "
                f"dry_run={args.dry_run}",
                flush=True,
            )

            onboard_catalog(
                catalog_path=args.catalog,
                pipeline_name=selection.get("pipeline_name"),
                job_name=selection.get("job_name"),
                table_id=selection.get("table_id"),
                include_dependencies=selection.get("include_dependencies", True),
                dry_run=args.dry_run,
            )

        return

    onboard_catalog(
        catalog_path=args.catalog,
        pipeline_name=args.pipeline_name,
        job_name=args.job_name,
        table_id=args.table_id,
        include_dependencies=not args.skip_dependencies,
        dry_run=args.dry_run,
    )

if __name__ == "__main__":
    main()