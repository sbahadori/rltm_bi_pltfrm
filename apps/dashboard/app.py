import os
import json
from datetime import datetime, timezone
from pathlib import Path

from apps.web import app
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

dash_app = FastAPI(title="BI Dashboard Config API")

dash_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

PIPELINE_REPO_ROOT = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm"))
BATCH_CATALOG_PATH = PIPELINE_REPO_ROOT / "configs" / "batch" / "pipeline_catalog.json"
STREAM_REGISTRY_PATH = PIPELINE_REPO_ROOT / "configs" / "streaming" / "stream_registry.json"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path):
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _build_batch_jobs(catalog: dict) -> list[dict]:
    jobs = []
    for pipeline in catalog.get("pipelines", []):
        if not pipeline.get("enabled", True):
            continue
        pipeline_name = pipeline["name"]
        for job in pipeline.get("jobs", []):
            if not job.get("enabled", True):
                continue
            spec = job.get("spec", {})
            source = spec.get("source", {})
            jobs.append({
                "id": f"{pipeline_name}__{job['name']}",
                "name": job["name"],
                "pipeline": pipeline_name,
                "type": "batch",
                "job_type": job.get("job_type", ""),
                "source_url": source.get("base_url", ""),
                "dependencies": job.get("dependencies", []),
                "timeout_minutes": job.get("execution_timeout_minutes", 30),
                "tags": job.get("tags", []),
                "target_path": (
                    spec.get("bronze_write", {}).get("target_path")
                    or spec.get("target", {}).get("path", "")
                ),
                "schedule": pipeline.get("dag", {}).get("schedule"),
            })
    return jobs


def _build_stream_jobs(registry: dict) -> list[dict]:
    jobs = []
    for stream in registry.get("streams", []):
        if not stream.get("enabled", True):
            continue
        name = stream["name"]
        source = stream.get("source", {})
        bronze = stream.get("bronze", {})
        silver = stream.get("silver", {})

        if bronze:
            jobs.append({
                "id": f"{name}__bronze",
                "name": bronze.get("app_name", f"{name}_bronze"),
                "pipeline": name,
                "type": "stream",
                "job_type": bronze.get("engine", "generic_kafka_to_bronze"),
                "source_url": f"{source.get('bootstrap_servers', '')} / {source.get('topic', '')}",
                "dependencies": [],
                "trigger": bronze.get("trigger_interval", "15 seconds"),
                "target_path": bronze.get("path", ""),
                "heartbeat_file": bronze.get("heartbeat_file", ""),
            })

        if silver:
            jobs.append({
                "id": f"{name}__silver",
                "name": silver.get("app_name", f"{name}_silver"),
                "pipeline": name,
                "type": "stream",
                "job_type": silver.get("engine", "generic_bronze_to_silver"),
                "source_url": silver.get("bronze_path", ""),
                "dependencies": [bronze.get("app_name", f"{name}_bronze")] if bronze else [],
                "trigger": silver.get("trigger_interval", "30 seconds"),
                "target_path": silver.get("path", ""),
                "quarantine_path": silver.get("quarantine_path", ""),
                "heartbeat_file": silver.get("heartbeat_file", ""),
            })
    return jobs


def _build_pipelines(catalog: dict, registry: dict) -> list[dict]:
    pipelines = []

    for p in catalog.get("pipelines", []):
        if not p.get("enabled", True):
            continue
        dag = p.get("dag", {})
        pipelines.append({
            "name": p["name"],
            "type": "batch",
            "description": p.get("description", ""),
            "schedule": dag.get("schedule"),
            "tags": dag.get("tags", []),
            "jobs": [j["name"] for j in p.get("jobs", []) if j.get("enabled", True)],
        })

    for s in registry.get("streams", []):
        if not s.get("enabled", True):
            continue
        jobs = []
        if s.get("bronze"):
            jobs.append(s["bronze"].get("app_name", f"{s['name']}_bronze"))
        if s.get("silver"):
            jobs.append(s["silver"].get("app_name", f"{s['name']}_silver"))
        pipelines.append({
            "name": s["name"],
            "type": "stream",
            "description": f"Streaming pipeline: {s['source'].get('topic', '')}",
            "schedule": "always-on",
            "tags": ["stream", "kafka"],
            "jobs": jobs,
        })

    return pipelines


@app.get("/health")
async def health():
    return {"status": "ok"}


@dash_app.get("/api/config")
async def get_config():
    catalog = _load_json(BATCH_CATALOG_PATH) or {"pipelines": []}
    registry = _load_json(STREAM_REGISTRY_PATH) or {"streams": []}

    return JSONResponse({
        "pipelines": _build_pipelines(catalog, registry),
        "jobs": _build_batch_jobs(catalog) + _build_stream_jobs(registry),
        "meta": {
            "batch_catalog_path": str(BATCH_CATALOG_PATH),
            "stream_registry_path": str(STREAM_REGISTRY_PATH),
            "loaded_at": now_iso(),
        },
    })