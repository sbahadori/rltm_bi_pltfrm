import os
import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI , HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
import time



app = FastAPI(title="BI Dashboard Config API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    allow_headers=["*"],
)

PIPELINE_REPO_ROOT = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm"))
BATCH_CATALOG_PATH = PIPELINE_REPO_ROOT / "configs" / "batch" / "pipeline_catalog.json"
STREAM_REGISTRY_PATH = PIPELINE_REPO_ROOT / "configs" / "streaming" / "stream_registry.json"

STREAM_STATUS_FILE = Path(
    os.getenv(
        "STREAM_STATUS_FILE",
        "/runtime/spark_health/stream_supervisor_status.json"
    )
)

AIRFLOW_LOG_DIR = Path(
    os.getenv(
        "AIRFLOW_LOG_DIR",
        "/runtime/airflow_logs"
    )
)


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

def _read_json_file(path: Path) -> dict:
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)



def _tail_file(path: Path, lines: int = 300) -> str:
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Log file not found: {path}")

    content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(content[-lines:])



@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/config")
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

@app.get("/api/runtime/streams/status")
async def get_stream_status():
    data = _read_json_file(STREAM_STATUS_FILE)

    now = int(time.time())

    for unit_name, unit in data.get("units", {}).items():
        pid = unit.get("pid")
        returncode = unit.get("returncode")
        heartbeat = unit.get("heartbeat") or {}

        if pid and returncode is None:
            computed_status = "running"
        elif returncode == 0:
            computed_status = "stopped"
        elif returncode is not None:
            computed_status = "failed"
        else:
            computed_status = "unknown"

        unit["computed_status"] = computed_status

        if "ts_epoch" in heartbeat:
            unit["heartbeat_age_seconds"] = now - int(heartbeat["ts_epoch"])

    return data


STREAM_LOG_DIR = Path(
    os.getenv(
        "STREAM_LOG_DIR",
        "/runtime/spark_health/logs"
    )
)


@app.get("/api/runtime/streams/logs/{unit_name}")
async def get_stream_log(
    unit_name: str,
    lines: int = Query(default=300, ge=10, le=2000)
):
    log_path = STREAM_LOG_DIR / f"{unit_name}.log"
    return PlainTextResponse(_tail_text(log_path, lines))

def _find_latest_airflow_log(dag_id: str, task_id: str) -> Path:
    dag_dir = AIRFLOW_LOG_DIR / f"dag_id={dag_id}"

    if not dag_dir.exists():
        raise HTTPException(
            status_code=404,
            detail=f"No Airflow log directory found for dag_id={dag_id}"
        )

    candidates = list(dag_dir.glob(f"**/task_id={task_id}/**/*.log"))

    if not candidates:
        raise HTTPException(
            status_code=404,
            detail=f"No log found for dag_id={dag_id}, task_id={task_id}"
        )

    return max(candidates, key=lambda p: p.stat().st_mtime)


@app.get("/api/runtime/logs/{job_id}")
async def get_runtime_logs(
    job_id: str,
    kind: str = Query(..., pattern="^(batch|stream)$"),
    pipeline: str | None = None,
    task: str | None = None,
    lines: int = Query(default=300, ge=20, le=2000),
):
    if kind == "stream":
        log_path = STREAM_LOG_DIR / f"{job_id}.log"
        return PlainTextResponse(_tail_file(log_path, lines))

    if kind == "batch":
        if not pipeline or not task:
            raise HTTPException(
                status_code=400,
                detail="pipeline and task are required for batch logs"
            )
        log_path = _find_latest_airflow_log(pipeline, task)
        return PlainTextResponse(_tail_file(log_path, lines))

    raise HTTPException(status_code=400, detail="Unsupported job kind")