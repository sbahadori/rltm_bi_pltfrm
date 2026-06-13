from __future__ import annotations

"""Runtime log API and shared log-path utilities."""

from pathlib import Path

from fastapi import APIRouter, Query
from fastapi.responses import PlainTextResponse

try:
    from apps.dashboard.config_loader import AIRFLOW_LOG_DIR, STREAM_LOG_DIR, job_from_config
    from apps.dashboard.stream_runtime import resolve_stream_log_name
except ImportError:  # pragma: no cover
    from .config_loader import AIRFLOW_LOG_DIR, STREAM_LOG_DIR, job_from_config
    from .stream_runtime import resolve_stream_log_name

router = APIRouter()


def tail_file(path: Path, lines: int = 300) -> str:
    if not path.exists():
        raise FileNotFoundError(str(path))
    return "\n".join(path.read_text(encoding="utf-8", errors="replace").splitlines()[-lines:])


def find_airflow_log(dag_id: str, task_id: str, dag_run_id: str | None = None) -> Path:
    candidates: list[Path] = []
    dag_dir = AIRFLOW_LOG_DIR / f"dag_id={dag_id}"

    if dag_run_id:
        candidates.extend((dag_dir / f"run_id={dag_run_id}").glob(f"task_id={task_id}/**/*.log"))

    if not candidates and dag_dir.exists():
        candidates.extend(dag_dir.glob(f"**/task_id={task_id}/**/*.log"))

    if not candidates and AIRFLOW_LOG_DIR.exists():
        candidates.extend(AIRFLOW_LOG_DIR.glob(f"**/dag_id={dag_id}/**/task_id={task_id}/**/*.log"))

    candidates = [path for path in candidates if path.is_file()]
    if not candidates:
        raise FileNotFoundError(f"No Airflow log: dag={dag_id} task={task_id}")

    return max(candidates, key=lambda path: path.stat().st_mtime)


@router.get("/api/runtime/logs/{job_id}")
async def get_logs(
    job_id: str,
    kind: str = Query(default="batch", pattern="^(batch|stream)$"),
    pipeline: str | None = None,
    task: str | None = None,
    lines: int = Query(default=500, ge=10, le=5000),
) -> PlainTextResponse:
    job = job_from_config(job_id)
    effective_kind = kind or (job.get("type") if job else None)

    if effective_kind == "stream":
        stream_name = resolve_stream_log_name(
            (job.get("runtime_unit_name") if job else None)
            or (job.get("name") if job else None)
            or job_id
        )
        log_path = STREAM_LOG_DIR / f"{stream_name}.log"
        if not log_path.exists():
            available = sorted(path.name for path in STREAM_LOG_DIR.glob("*.log")) if STREAM_LOG_DIR.exists() else []
            return PlainTextResponse(f"Log not found: {log_path}\nAvailable: {available}", status_code=404)
        return PlainTextResponse(tail_file(log_path, lines))

    dag_id = pipeline or (job.get("pipeline") if job else None)
    task_id = task or (job.get("name") if job else None)

    if not dag_id or not task_id:
        return PlainTextResponse("Missing pipeline/task", status_code=400)

    try:
        return PlainTextResponse(tail_file(find_airflow_log(dag_id, task_id), lines))
    except FileNotFoundError as exc:
        return PlainTextResponse(str(exc), status_code=404)
