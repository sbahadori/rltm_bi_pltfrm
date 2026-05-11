from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

from airflow.providers.standard.operators.bash import BashOperator
from batch.utils.jdbc_connection_registry import get_spark_packages_for_manifest
from batch.utils.jdbc_manifest_loader import get_enabled_tables, load_jdbc_manifest

def _merge_packages(existing: list[str] | None, extra: list[str] | None) -> list[str]:
    merged: list[str] = []

    for package in list(existing or []) + list(extra or []):
        if package not in merged:
            merged.append(package)

    return merged

def build_airflow_tasks_from_job(
    job: dict[str, Any],
    pipeline_spec: dict[str, Any],
    catalog_path: str,
    dag,
    common_env: dict[str, str] | None = None,
) -> list[BashOperator]:
    if job["job_type"] != "generic_jdbc_manifest_to_bronze":
        return [
            build_airflow_task_from_job(
                job,
                pipeline_spec,
                catalog_path,
                dag,
                common_env=common_env,
            )
        ]

    env = build_common_env()
    if common_env:
        env.update(common_env)

    manifest_ref = job["manifest_ref"]
    execution_strategy = job.get("execution_strategy", "one_task_per_manifest")

    jdbc_packages = get_spark_packages_for_manifest(manifest_ref)

    base_spark_cfg = dict(job.get("spark", {}))
    base_spark_cfg["packages"] = _merge_packages(
        base_spark_cfg.get("packages", []),
        jdbc_packages,
    )

    tasks: list[BashOperator] = []

    if execution_strategy == "one_task_per_manifest":
        spark_job_config = {
            "entrypoint": "batch/runners/generic_jdbc_manifest_to_bronze.py",
            "args": {
                "manifest_ref": manifest_ref,
            },
            "spark": base_spark_cfg,
        }

        cmd = build_spark_submit_command(spark_job_config)

        tasks.append(
            BashOperator(
                task_id=job["name"],
                bash_command=cmd,
                env=env,
                append_env=True,
                execution_timeout=timedelta(minutes=job.get("execution_timeout_minutes", 30)),
                retries=job.get("retries", 0),
                retry_delay=timedelta(minutes=job.get("retry_delay_minutes", 1)),
                dag=dag,
            )
        )

        return tasks

    if execution_strategy == "one_task_per_table":
        manifest = load_jdbc_manifest(manifest_ref)

        for table in get_enabled_tables(manifest):
            table_id = table["table_id"]

            spark_job_config = {
                "entrypoint": "batch/runners/generic_jdbc_manifest_to_bronze.py",
                "args": {
                    "manifest_ref": manifest_ref,
                    "table_id": table_id,
                },
                "spark": base_spark_cfg,
            }

            cmd = build_spark_submit_command(spark_job_config)

            tasks.append(
                BashOperator(
                    task_id=f"{job['name']}__{table_id}",
                    bash_command=cmd,
                    env=env,
                    append_env=True,
                    execution_timeout=timedelta(minutes=job.get("execution_timeout_minutes", 30)),
                    retries=job.get("retries", 0),
                    retry_delay=timedelta(minutes=job.get("retry_delay_minutes", 1)),
                    dag=dag,
                )
            )

        return tasks

    raise ValueError(
        f"Unsupported execution_strategy for job '{job['name']}': {execution_strategy}"
    )

def _bootstrap_repo_path() -> Path:
    repo_root = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    return repo_root


REPO_ROOT = _bootstrap_repo_path()

from batch.specs.batch_catalog_utils import get_enabled_pipelines  # noqa: E402
from shared.spark.spark_submit_utils import build_spark_submit_command, get_repo_root  # noqa: E402


def build_common_env() -> dict[str, str]:
    return {
        "PIPELINE_REPO_ROOT": str(get_repo_root()),
        "S3_ENDPOINT": os.getenv("S3_ENDPOINT", "http://minio:9000"),
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minio"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"),
        "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
        "SPARK_SUBMIT": os.getenv("SPARK_SUBMIT", "/home/airflow/.local/bin/spark-submit"),
        "SPARK_MASTER_URL": os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"),
        "PATH": f"/home/airflow/.local/bin:{os.getenv('PATH', '')}",
    }


def _build_runner_job_spec(job: dict[str, Any], pipeline_spec: dict[str, Any], catalog_path: str) -> dict[str, Any]:
    job_type = job["job_type"]
    spec = job["spec"]

    if job_type == "spark_batch":
        return {
            "entrypoint": spec["entrypoint"],
            "args": spec.get("args", {}),
            "spark": spec.get("spark", {}),
        }

    if job_type == "generic_api_to_bronze":
        return {
            "entrypoint": "batch/runners/generic_api_to_bronze.py",
            "args": {
                "catalog_path": catalog_path,
                "pipeline_name": pipeline_spec["name"],
                "job_name": job["name"],
            },
            "spark": spec.get("spark", {}),
        }

    if job_type == "generic_bronze_to_silver":
        return {
            "entrypoint": "batch/runners/generic_bronze_to_silver.py",
            "args": {
                "catalog_path": catalog_path,
                "pipeline_name": pipeline_spec["name"],
                "job_name": job["name"],
            },
            "spark": spec.get("spark", {}),
        }

    if job_type == "generic_silver_to_gold":
        return {
            "entrypoint": "batch/runners/generic_silver_to_gold.py",
            "args": {
                "catalog_path": catalog_path,
                "pipeline_name": pipeline_spec["name"],
                "job_name": job["name"],
            },
            "spark": spec.get("spark", {}),
        }
    
    if job_type == "generic_jdbc_to_bronze":
        return {
            "entrypoint": "batch/runners/generic_jdbc_to_bronze.py",
            "args": {
                "catalog_path": catalog_path,
                "pipeline_name": pipeline_spec["name"],
                "job_name": job["name"],
            },
            "spark": spec.get("spark", {}),
        }

    raise ValueError(f"Unsupported job_type: {job_type}")


def build_airflow_task_from_job(
    job: dict[str, Any],
    pipeline_spec: dict[str, Any],
    catalog_path: str,
    dag,
    common_env: dict[str, str] | None = None,
) -> BashOperator:
    env = build_common_env()
    if common_env:
        env.update(common_env)

    spark_job_config = _build_runner_job_spec(job, pipeline_spec, catalog_path)
    cmd = build_spark_submit_command(spark_job_config)

    return BashOperator(
        task_id=job["name"],
        bash_command=cmd,
        env=env,
        append_env=True,
        execution_timeout=timedelta(minutes=job.get("execution_timeout_minutes", 30)),
        retries=job.get("retries", 0),
        retry_delay=timedelta(minutes=job.get("retry_delay_minutes", 1)),
        dag=dag,
    )


def build_tasks_from_pipeline_spec(
    *,
    dag,
    pipeline_spec: dict[str, Any],
    catalog_path: str,
    common_env: dict[str, str] | None = None,
) -> dict[str, BashOperator]:
    jobs = [job for job in pipeline_spec["jobs"] if job.get("enabled", True)]
    enabled_job_names = {job["name"] for job in jobs}

    for job in jobs:
        for dep in job.get("dependencies", []):
            if dep not in enabled_job_names:
                raise ValueError(
                    f"Enabled job '{job['name']}' depends on disabled or missing job '{dep}' "
                    f"in pipeline '{pipeline_spec['name']}'"
                )

    tasks_by_job_name: dict[str, list[BashOperator]] = {}

    for job in jobs:
        tasks_by_job_name[job["name"]] = build_airflow_tasks_from_job(
            job,
            pipeline_spec,
            catalog_path,
            dag,
            common_env=common_env,
        )

    for job in jobs:
        current_tasks = tasks_by_job_name[job["name"]]

        for dep in job.get("dependencies", []):
            upstream_tasks = tasks_by_job_name[dep]

            for upstream in upstream_tasks:
                for downstream in current_tasks:
                    upstream >> downstream

    flat_tasks: dict[str, BashOperator] = {}

    for job_name, task_list in tasks_by_job_name.items():
        for task in task_list:
            flat_tasks[task.task_id] = task

    return flat_tasks

def load_enabled_pipeline_specs(catalog_path: str) -> list[dict[str, Any]]:
    return get_enabled_pipelines(catalog_path)