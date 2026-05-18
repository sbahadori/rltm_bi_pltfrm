from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(os.getenv("PIPELINE_REPO_ROOT", ".")).resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from shared.control.postgres import get_conn  # noqa: E402


def read_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as f:
        return json.load(f)


def resolve_repo_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (REPO_ROOT / path).resolve()


def load_manifest(manifest_ref: str) -> dict[str, Any]:
    manifest_path = resolve_repo_path(manifest_ref)
    with manifest_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def target_path_for_table(manifest: dict[str, Any], table: dict[str, Any]) -> str | None:
    if table.get("target_path"):
        return table["target_path"]

    template = (manifest.get("defaults") or {}).get("target_path_template")
    if not template:
        return None

    return template.format(
        source_id=manifest.get("source_id", ""),
        table_id=table.get("table_id", ""),
    )


def upsert_pipeline(cur, pipeline: dict[str, Any]) -> None:
    dag = pipeline.get("dag", {}) or {}

    cur.execute(
        """
        INSERT INTO meta.pipeline (
            pipeline_name,
            domain,
            description,
            owner,
            schedule_cron,
            airflow_dag_id,
            is_active,
            raw_config,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, CURRENT_TIMESTAMP)
        ON CONFLICT (pipeline_name)
        DO UPDATE SET
            domain = EXCLUDED.domain,
            description = EXCLUDED.description,
            owner = EXCLUDED.owner,
            schedule_cron = EXCLUDED.schedule_cron,
            airflow_dag_id = EXCLUDED.airflow_dag_id,
            is_active = EXCLUDED.is_active,
            raw_config = EXCLUDED.raw_config,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            pipeline["name"],
            pipeline.get("domain"),
            pipeline.get("description"),
            (dag.get("default_args") or {}).get("owner", "data_platform"),
            dag.get("schedule"),
            pipeline["name"],
            bool(pipeline.get("enabled", True)),
            json.dumps(pipeline),
        ),
    )


def upsert_meta_job(
    cur,
    *,
    job_key: str,
    job_code: str,
    pipeline_name: str,
    job_name: str,
    base_job_name: str | None,
    job_type: str,
    source_type: str | None,
    runner: str,
    layer: str | None,
    source_id: str | None,
    table_id: str | None,
    entity_name: str | None,
    manifest_ref: str | None,
    target_path: str | None,
    config: dict[str, Any],
    runtime_policy: dict[str, Any],
    is_active: bool,
) -> None:
    cur.execute(
        """
        INSERT INTO meta.job (
            job_key,
            job_code,
            pipeline_name,
            job_name,
            base_job_name,
            job_type,
            source_type,
            runner,
            layer,
            source_id,
            table_id,
            entity_name,
            manifest_ref,
            target_path,
            config,
            runtime_policy,
            is_active,
            updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s::jsonb,
            %s::jsonb,
            %s,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (job_key)
        DO UPDATE SET
            job_code = EXCLUDED.job_code,
            pipeline_name = EXCLUDED.pipeline_name,
            job_name = EXCLUDED.job_name,
            base_job_name = EXCLUDED.base_job_name,
            job_type = EXCLUDED.job_type,
            source_type = EXCLUDED.source_type,
            runner = EXCLUDED.runner,
            layer = EXCLUDED.layer,
            source_id = EXCLUDED.source_id,
            table_id = EXCLUDED.table_id,
            entity_name = EXCLUDED.entity_name,
            manifest_ref = EXCLUDED.manifest_ref,
            target_path = EXCLUDED.target_path,
            config = EXCLUDED.config,
            runtime_policy = EXCLUDED.runtime_policy,
            is_active = EXCLUDED.is_active,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            job_key,
            job_code,
            pipeline_name,
            job_name,
            base_job_name,
            job_type,
            source_type,
            runner,
            layer,
            source_id,
            table_id,
            entity_name,
            manifest_ref,
            target_path,
            json.dumps(config),
            json.dumps(runtime_policy),
            is_active,
        ),
    )


def register_pipeline_jobs(cur, pipeline: dict[str, Any]) -> None:
    pipeline_name = pipeline["name"]

    for job in pipeline.get("jobs", []):
        if not job.get("enabled", True):
            continue

        job_name = job["name"]
        job_type = job["job_type"]
        source_type = job.get("source_type")
        execution_strategy = job.get("execution_strategy")
        runtime_policy = {
            "execution_timeout_minutes": job.get("execution_timeout_minutes", 30),
            "retries": job.get("retries", 0),
            "retry_delay_minutes": job.get("retry_delay_minutes", 1),
        }

        # Table-level registration for JDBC manifest jobs.
        if (
            job_type == "generic_jdbc_manifest_to_bronze"
            and execution_strategy == "one_task_per_table"
        ):
            manifest_ref = job["manifest_ref"]
            manifest = load_manifest(manifest_ref)
            source_id = manifest["source_id"]

            for table in manifest.get("tables", []):
                if not table.get("enabled", True):
                    continue

                table_id = table["table_id"]
                table_job_name = f"{job_name}__{table_id}"
                job_code = f"bronze.{source_id}.{table_id}"
                job_key = job_code
                entity_name = f"{source_id}.{table_id}"
                target_path = target_path_for_table(manifest, table)

                config = {
                    "manifest_ref": manifest_ref,
                    "table_id": table_id,
                    "execution_strategy": "one_task_per_table",
                    "base_job_name": job_name,
                    "tags": job.get("tags", []),
                    "dependencies": job.get("dependencies", []),
                    "source_table": table.get("source_table"),
                    "target_path": target_path,
                }

                upsert_meta_job(
                    cur,
                    job_key=job_key,
                    job_code=job_code,
                    pipeline_name=pipeline_name,
                    job_name=table_job_name,
                    base_job_name=job_name,
                    job_type=job_type,
                    source_type=source_type or "jdbc",
                    runner=job_type,
                    layer="bronze",
                    source_id=source_id,
                    table_id=table_id,
                    entity_name=entity_name,
                    manifest_ref=manifest_ref,
                    target_path=target_path,
                    config=config,
                    runtime_policy=runtime_policy,
                    is_active=True,
                )

            continue

        # Non-manifest batch jobs.
        layer = None
        if job_type.endswith("_to_bronze") or job_type == "generic_api_to_bronze":
            layer = "bronze"
        elif "silver" in job_type or job_name.endswith("_silver"):
            layer = "silver"
        elif "gold" in job_type or job_name.endswith("_gold"):
            layer = "gold"

        job_code = f"{layer or 'batch'}.{pipeline_name}.{job_name}"
        job_key = job_code
        spec = job.get("spec", {}) or {}

        target_path = (
            spec.get("bronze_write", {}).get("target_path")
            or spec.get("silver_write", {}).get("target_path")
            or spec.get("gold_write", {}).get("target_path")
            or spec.get("target", {}).get("path")
        )

        upsert_meta_job(
            cur,
            job_key=job_key,
            job_code=job_code,
            pipeline_name=pipeline_name,
            job_name=job_name,
            base_job_name=job_name,
            job_type=job_type,
            source_type=source_type,
            runner=job_type,
            layer=layer,
            source_id=None,
            table_id=None,
            entity_name=f"{pipeline_name}.{job_name}",
            manifest_ref=None,
            target_path=target_path,
            config=spec,
            runtime_policy=runtime_policy,
            is_active=True,
        )


def register_catalog(catalog_path: str) -> None:
    catalog = read_json(catalog_path)

    with get_conn() as conn:
        with conn.cursor() as cur:
            for pipeline in catalog.get("pipelines", []):
                if not pipeline.get("enabled", True):
                    continue

                upsert_pipeline(cur, pipeline)
                register_pipeline_jobs(cur, pipeline)

    print(f"[OK] Registered catalog into metadata DB: {catalog_path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--catalog-path",
        default="configs/batch/pipeline_catalog.json",
    )
    args = parser.parse_args()
    register_catalog(args.catalog_path)


if __name__ == "__main__":
    main()