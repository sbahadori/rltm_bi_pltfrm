import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI(title="BI Dashboard Config API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

PIPELINE_REPO_ROOT = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm"))
BATCH_CATALOG_PATH = PIPELINE_REPO_ROOT / "configs" / "batch" / "pipeline_catalog.json"
STREAM_REGISTRY_PATH = PIPELINE_REPO_ROOT / "configs" / "streaming" / "stream_registry.json"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _target_path_from_spec(spec: dict[str, Any]) -> str:
    return (
        spec.get("bronze_write", {}).get("target_path")
        or spec.get("target", {}).get("path")
        or spec.get("silver_write", {}).get("target_path")
        or spec.get("gold_write", {}).get("target_path")
        or ""
    )


def _build_batch_jobs(catalog: dict[str, Any]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []

    for pipeline in catalog.get("pipelines", []):
        if not pipeline.get("enabled", True):
            continue

        pipeline_name = pipeline.get("name", "")
        schedule = pipeline.get("dag", {}).get("schedule")

        for job in pipeline.get("jobs", []):
            if not job.get("enabled", True):
                continue

            spec = job.get("spec", {}) or {}
            source = spec.get("source", {}) or {}

            jobs.append(
                {
                    "id": f"{pipeline_name}__{job.get('name', '')}",
                    "name": job.get("name", ""),
                    "pipeline": pipeline_name,
                    "type": "batch",
                    "job_type": job.get("job_type", ""),
                    "source_url": source.get("base_url", ""),
                    "dependencies": job.get("dependencies", []),
                    "timeout_minutes": job.get("execution_timeout_minutes", 30),
                    "tags": job.get("tags", []),
                    "target_path": _target_path_from_spec(spec),
                    "schedule": schedule,
                }
            )

    return jobs


def _build_stream_jobs(registry: dict[str, Any]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []

    for stream in registry.get("streams", []):
        if not stream.get("enabled", True):
            continue

        name = stream.get("name", "")
        source = stream.get("source", {}) or {}
        bronze = stream.get("bronze", {}) or {}
        silver = stream.get("silver", {}) or {}

        if bronze:
            bronze_app_name = bronze.get("app_name", f"{name}_bronze")
            jobs.append(
                {
                    "id": f"{name}__bronze",
                    "name": bronze_app_name,
                    "pipeline": name,
                    "type": "stream",
                    "job_type": bronze.get("engine", "generic_kafka_to_bronze"),
                    "source_url": f"{source.get('bootstrap_servers', '')} / {source.get('topic', '')}",
                    "dependencies": [],
                    "trigger": bronze.get("trigger_interval", "15 seconds"),
                    "target_path": bronze.get("path", ""),
                    "heartbeat_file": bronze.get("heartbeat_file", ""),
                }
            )

        if silver:
            silver_app_name = silver.get("app_name", f"{name}_silver")
            bronze_app_name = bronze.get("app_name", f"{name}_bronze") if bronze else f"{name}_bronze"
            jobs.append(
                {
                    "id": f"{name}__silver",
                    "name": silver_app_name,
                    "pipeline": name,
                    "type": "stream",
                    "job_type": silver.get("engine", "generic_bronze_to_silver"),
                    "source_url": silver.get("bronze_path", ""),
                    "dependencies": [bronze_app_name] if bronze else [],
                    "trigger": silver.get("trigger_interval", "30 seconds"),
                    "target_path": silver.get("path", ""),
                    "quarantine_path": silver.get("quarantine_path", ""),
                    "heartbeat_file": silver.get("heartbeat_file", ""),
                }
            )

    return jobs


def _build_pipelines(catalog: dict[str, Any], registry: dict[str, Any]) -> list[dict[str, Any]]:
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
                "jobs": [j.get("name", "") for j in pipeline.get("jobs", []) if j.get("enabled", True)],
            }
        )

    for stream in registry.get("streams", []):
        if not stream.get("enabled", True):
            continue

        jobs: list[str] = []
        if stream.get("bronze"):
            jobs.append(stream["bronze"].get("app_name", f"{stream.get('name', '')}_bronze"))
        if stream.get("silver"):
            jobs.append(stream["silver"].get("app_name", f"{stream.get('name', '')}_silver"))

        source = stream.get("source", {}) or {}
        pipelines.append(
            {
                "name": stream.get("name", ""),
                "type": "stream",
                "description": f"Streaming pipeline: {source.get('topic', '')}",
                "schedule": "always-on",
                "tags": ["stream", "kafka"],
                "jobs": jobs,
            }
        )

    return pipelines


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/config")
async def get_config() -> JSONResponse:
    catalog = _load_json(BATCH_CATALOG_PATH) or {"pipelines": []}
    registry = _load_json(STREAM_REGISTRY_PATH) or {"streams": []}

    return JSONResponse(
        {
            "pipelines": _build_pipelines(catalog, registry),
            "jobs": _build_batch_jobs(catalog) + _build_stream_jobs(registry),
            "meta": {
                "batch_catalog_path": str(BATCH_CATALOG_PATH),
                "stream_registry_path": str(STREAM_REGISTRY_PATH),
                "loaded_at": now_iso(),
            },
        }
    )
