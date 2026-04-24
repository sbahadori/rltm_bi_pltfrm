# وظایفش:

# load registry
# validate registry
# get pipelines
# get jobs
# validate unique job names
# validate dependencies
# باید این قابلیت‌ها را داشته باشد
# load_job_registry(path)
# get_enabled_pipelines(path)
# validate_job_registry(data)
# get_pipeline_spec(path, pipeline_name)

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def get_repo_root() -> Path:
    return Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()


def resolve_repo_path(path_str: str | Path) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (get_repo_root() / path).resolve()


def load_batch_catalog(catalog_path: str | Path) -> dict[str, Any]:
    path = resolve_repo_path(catalog_path)
    if not path.exists():
        raise FileNotFoundError(f"Batch catalog not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    validate_batch_catalog(data)
    return data


def validate_batch_catalog(data: dict[str, Any]) -> None:
    pipelines = data.get("pipelines")
    if not isinstance(pipelines, list):
        raise ValueError("Batch catalog must contain a top-level 'pipelines' list")

    pipeline_names: set[str] = set()

    for pipeline in pipelines:
        validate_pipeline_spec(pipeline)

        name = pipeline["name"]
        if name in pipeline_names:
            raise ValueError(f"Duplicate pipeline name found: {name}")
        pipeline_names.add(name)


def validate_pipeline_spec(pipeline: dict[str, Any]) -> None:
    required_top = ["name", "enabled", "dag", "jobs"]
    missing_top = [k for k in required_top if k not in pipeline]
    if missing_top:
        raise ValueError(
            f"Pipeline '{pipeline.get('name', '?')}' missing required keys: {missing_top}"
        )

    if not isinstance(pipeline["jobs"], list) or not pipeline["jobs"]:
        raise ValueError(f"Pipeline '{pipeline['name']}' must define a non-empty jobs list")

    dag = pipeline["dag"]
    if not isinstance(dag, dict):
        raise ValueError(f"Pipeline '{pipeline['name']}' dag section must be an object")

    required_dag = ["catchup", "max_active_runs", "start_date", "tags", "default_args"]
    missing_dag = [k for k in required_dag if k not in dag]
    if missing_dag:
        raise ValueError(
            f"Pipeline '{pipeline['name']}' dag section missing keys: {missing_dag}"
        )

    job_names: set[str] = set()
    for job in pipeline["jobs"]:
        validate_job_spec(pipeline["name"], job)
        if job["name"] in job_names:
            raise ValueError(
                f"Duplicate job name '{job['name']}' in pipeline '{pipeline['name']}'"
            )
        job_names.add(job["name"])

    for job in pipeline["jobs"]:
        for dep in job.get("dependencies", []):
            if dep not in job_names:
                raise ValueError(
                    f"Job '{job['name']}' in pipeline '{pipeline['name']}' depends on unknown job '{dep}'"
                )


def validate_job_spec(pipeline_name: str, job: dict[str, Any]) -> None:
    required = [
        "name",
        "enabled",
        "job_type",
        "entrypoint",
        "args",
        "dependencies",
        "execution_timeout_minutes",
    ]
    missing = [k for k in required if k not in job]
    if missing:
        raise ValueError(
            f"Job in pipeline '{pipeline_name}' missing required keys: {missing}"
        )

    if job["job_type"] != "spark_batch":
        raise ValueError(
            f"Unsupported job_type '{job['job_type']}' in pipeline '{pipeline_name}'. "
            f"Current step supports only 'spark_batch'."
        )

    if not isinstance(job["args"], dict):
        raise ValueError(
            f"Job '{job['name']}' in pipeline '{pipeline_name}' must have args as an object"
        )

    if not isinstance(job["dependencies"], list):
        raise ValueError(
            f"Job '{job['name']}' in pipeline '{pipeline_name}' must have dependencies as a list"
        )


def get_enabled_pipelines(catalog_path: str | Path) -> list[dict[str, Any]]:
    data = load_batch_catalog(catalog_path)
    return [p for p in data["pipelines"] if p.get("enabled", True)]


def get_pipeline_by_name(catalog_path: str | Path, pipeline_name: str) -> dict[str, Any]:
    pipelines = get_enabled_pipelines(catalog_path)
    for pipeline in pipelines:
        if pipeline["name"] == pipeline_name:
            return pipeline
    raise ValueError(f"Enabled pipeline '{pipeline_name}' not found in batch catalog")