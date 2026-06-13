from __future__ import annotations

from typing import Any

try:
    from apps.dashboard.airflow_client import trigger_dag
    from apps.dashboard.domain_contracts import canonical_job, job_id_of, job_name_of, job_mode_of, pipeline_id_of
except ImportError:  # pragma: no cover
    from .airflow_client import trigger_dag
    from .domain_contracts import canonical_job, job_id_of, job_name_of, job_mode_of, pipeline_id_of


def executor_id_for_job(job: dict[str, Any]) -> str | None:
    job = canonical_job(job)

    return (
        job.get("metadata_airflow_dag_id")
        or job.get("airflow_dag_id")
        or job.get("dag_id")
        or job.get("pipeline_airflow_dag_id")
        or pipeline_id_of(job)
    )


def execute_job(
    job: dict[str, Any],
    *,
    conf: dict[str, Any] | None = None,
    logical_date: str | None = None,
) -> dict[str, Any]:
    job = canonical_job(job)

    mode = job_mode_of(job)
    if mode != "batch":
        raise ValueError(f"Job execution is only implemented for batch jobs. job_mode={mode}")

    dag_id = executor_id_for_job(job)
    if not dag_id:
        raise ValueError(f"No executor DAG could be resolved for job_id={job_id_of(job)}")

    executor_conf = {
        **(conf or {}),
        "job_id": job_id_of(job),
        "job_name": job_name_of(job),
        "pipeline_id": pipeline_id_of(job),
        "runner_id": job.get("runner_id"),
        "job_code": job.get("job_code"),
    }

    result = trigger_dag(dag_id, conf=executor_conf, logical_date=logical_date)

    return {
        "ok": True,
        "job_id": job_id_of(job),
        "job_name": job_name_of(job),
        "pipeline_id": pipeline_id_of(job),
        "runner_id": job.get("runner_id"),
        "executor_type": "airflow",
        "executor_id": dag_id,
        "executor_result": result,
    }