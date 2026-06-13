from __future__ import annotations

"""
Catalog and configuration loader.

Single responsibility:
- Read environment-backed paths.
- Load batch catalog and stream registry.
- Convert static catalog/registry definitions into dashboard pipeline/job payloads.

No Control DB calls, no Airflow calls, no runtime-state resolution belongs here.
"""

import json
import os
from pathlib import Path
from typing import Any

try:
    from apps.dashboard.runtime_models import now_iso
except ImportError:  # pragma: no cover - package import fallback
    from .runtime_models import now_iso

try:
    from apps.dashboard.settings import get_settings
except ImportError:  # pragma: no cover
    from .settings import get_settings

try:
    from apps.dashboard.domain_contracts import canonical_job, canonical_pipeline
except ImportError:  # pragma: no cover
    from .domain_contracts import canonical_job, canonical_pipeline

settings = get_settings()

PIPELINE_REPO_ROOT = settings.pipeline_repo_root
BATCH_CATALOG_PATH = settings.batch_catalog_path
STREAM_REGISTRY_PATH = settings.stream_registry_path

STREAM_STATUS_FILE = settings.stream_status_file
STREAM_LOG_DIR = settings.stream_log_dir
AIRFLOW_LOG_DIR = settings.airflow_log_dir
JOB_RUN_REGISTRY_FILE = settings.job_run_registry_file

AIRFLOW_API_BASE = settings.airflow_api_base
AIRFLOW_USER = settings.airflow_user
AIRFLOW_PASSWORD = settings.airflow_password

STREAM_CONTROL_DIR = settings.stream_control_dir
STREAM_STATUS_STALE_SECONDS = settings.stream_status_stale_seconds
STREAM_HEARTBEAT_STALE_SECONDS = settings.stream_heartbeat_stale_seconds

def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8-sig") as file:
        return json.load(file)


def path_state(path: Path) -> dict[str, Any]:
    exists = path.exists()
    return {
        "path": str(path),
        "exists": exists,
        "is_file": path.is_file() if exists else False,
        "readable": os.access(path, os.R_OK) if exists else False,
    }


def resolve_repo_path(path_ref: str) -> Path:
    path = Path(path_ref)
    return path if path.is_absolute() else (PIPELINE_REPO_ROOT / path).resolve()


def load_manifest(ref: str) -> dict[str, Any]:
    path = resolve_repo_path(ref)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def enabled_manifest_tables(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    return [table for table in manifest.get("tables", []) if table.get("enabled", True)]


def manifest_target(manifest: dict[str, Any], table: dict[str, Any]) -> str:
    if table.get("target_path"):
        return table["target_path"]

    template = (manifest.get("defaults") or {}).get("target_path_template", "")
    try:
        return template.format(
            source_id=manifest.get("source_id", ""),
            table_id=table.get("table_id", ""),
        )
    except Exception:
        return ""


def target_from_spec(spec: dict[str, Any]) -> str:
    return (
        spec.get("bronze_write", {}).get("target_path")
        or spec.get("target", {}).get("path")
        or spec.get("silver_write", {}).get("target_path")
        or spec.get("gold_write", {}).get("target_path")
        or ""
    )


def pipeline_job_names(pipeline: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for job in pipeline.get("jobs", []):
        if not job.get("enabled", True):
            continue

        job_type = job.get("job_type", "")
        execution_strategy = job.get("execution_strategy", "")

        if job_type == "generic_jdbc_manifest_to_bronze" and execution_strategy == "one_task_per_table":
            manifest = load_manifest(job.get("manifest_ref", ""))
            for table in enabled_manifest_tables(manifest):
                table_id = table.get("table_id")
                if table_id:
                    names.append(f"{job['name']}__{table_id}")
        else:
            names.append(job["name"])

    return names


def build_batch_jobs(catalog: dict[str, Any]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []

    for pipeline in catalog.get("pipelines", []):
        if not pipeline.get("enabled", True):
            continue

        pipeline_name = pipeline.get("name", "")
        dag = pipeline.get("dag", {}) or {}
        schedule = dag.get("schedule")
        dag_tags = dag.get("tags", [])

        for job in pipeline.get("jobs", []):
            if not job.get("enabled", True):
                continue

            job_name = job.get("name", "")
            job_type = job.get("job_type", "")
            execution_strategy = job.get("execution_strategy", "")

            if job_type == "generic_jdbc_manifest_to_bronze" and execution_strategy == "one_task_per_table":
                manifest = load_manifest(job.get("manifest_ref", ""))
                source_id = manifest.get("source_id", "")

                for table in enabled_manifest_tables(manifest):
                    table_id = table.get("table_id", "")
                    if not table_id:
                        continue

                    task_name = f"{job_name}__{table_id}"
                    jobs.append(
                        {
                            "id": f"{pipeline_name}__{task_name}",
                            "name": task_name,
                            "pipeline": pipeline_name,
                            "type": "batch",
                            "job_type": job_type,
                            "runner": job_type,
                            "job_code": f"bronze.{source_id}.{table_id}",
                            "source_id": source_id,
                            "table_id": table_id,
                            "layer": "bronze",
                            "target_path": manifest_target(manifest, table),
                            "schedule": schedule,
                            "tags": job.get("tags", []) or dag_tags,
                        }
                    )
                continue

            spec = job.get("spec", {}) or {}
            jobs.append(
                {
                    "id": f"{pipeline_name}__{job_name}",
                    "name": job_name,
                    "pipeline": pipeline_name,
                    "type": "batch",
                    "job_type": job_type,
                    "runner": job_type,
                    "target_path": target_from_spec(spec),
                    "schedule": schedule,
                    "tags": job.get("tags", []) or dag_tags,
                }
            )
    return [canonical_job(job) for job in jobs]


def build_stream_jobs(registry: dict[str, Any]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []

    for stream in registry.get("streams", []):
        if not stream.get("enabled", True):
            continue

        name = stream.get("name", "")
        source = stream.get("source", {}) or {}
        bronze = stream.get("bronze", {}) or {}
        silver = stream.get("silver", {}) or {}

        if bronze:
            jobs.append(
                {
                    "id": f"{name}__bronze",
                    "name": bronze.get("app_name", f"bronze_{name}"),
                    "pipeline": name,
                    "type": "stream",
                    "job_type": bronze.get("engine", "generic_kafka_to_bronze"),
                    "target_path": bronze.get("path", ""),
                    "checkpoint_path": bronze.get("checkpoint_dir", ""),
                    "heartbeat_file": bronze.get("heartbeat_file", ""),
                    "source_url": f"{source.get('bootstrap_servers','')} / {source.get('topic','')}",
                }
            )

        if silver:
            jobs.append(
                {
                    "id": f"{name}__silver",
                    "name": silver.get("app_name", f"silver_{name}"),
                    "pipeline": name,
                    "type": "stream",
                    "job_type": silver.get("engine", "generic_bronze_to_silver"),
                    "target_path": silver.get("path", ""),
                    "quarantine_path": silver.get("quarantine_path", ""),
                    "checkpoint_path": silver.get("checkpoint_dir", ""),
                    "heartbeat_file": silver.get("heartbeat_file", ""),
                }
            )
    return [canonical_job(job) for job in jobs]


def build_pipelines(catalog: dict[str, Any], registry: dict[str, Any]) -> list[dict[str, Any]]:
    pipelines: list[dict[str, Any]] = []

    for pipeline in catalog.get("pipelines", []):
        if not pipeline.get("enabled", True):
            continue

        dag = pipeline.get("dag", {}) or {}
        pipelines.append(
            {
                "name": pipeline.get("name", ""),
                "type": "batch",
                "description": pipeline.get("description", ""),
                "schedule": dag.get("schedule"),
                "tags": dag.get("tags", []),
                "jobs": pipeline_job_names(pipeline),
            }
        )

    for stream in registry.get("streams", []):
        if not stream.get("enabled", True):
            continue

        name = stream.get("name", "")
        jobs: list[str] = []
        if stream.get("bronze"):
            jobs.append(stream["bronze"].get("app_name", f"bronze_{name}"))
        if stream.get("silver"):
            jobs.append(stream["silver"].get("app_name", f"silver_{name}"))

        pipelines.append(
            {
                "name": name,
                "type": "stream",
                "description": f"Streaming: {stream.get('source', {}).get('topic', '')}",
                "schedule": "always-on",
                "tags": ["stream", "kafka"],
                "jobs": jobs,
            }
        )
    return [canonical_pipeline(pipeline) for pipeline in pipelines]


def config_bundle() -> dict[str, Any]:
    catalog = load_json(BATCH_CATALOG_PATH) or {"pipelines": []}
    registry = load_json(STREAM_REGISTRY_PATH) or {"streams": []}
    return {
        "catalog": catalog,
        "registry": registry,
        "pipelines": build_pipelines(catalog, registry),
        "jobs": build_batch_jobs(catalog) + build_stream_jobs(registry),
    }


def config_payload() -> dict[str, Any]:
    bundle = config_bundle()
    return {
        "pipelines": bundle["pipelines"],
        "jobs": bundle["jobs"],
        "meta": {"loaded_at": now_iso()},
    }


def job_from_config(job_id: str) -> dict[str, Any] | None:
    for job in config_payload().get("jobs", []):
        if job.get("id") == job_id or job.get("name") == job_id:
            return job
    return None
