from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

from airflow.providers.standard.operators.bash import BashOperator
from batch.utils.jdbc_manifest_loader import get_enabled_tables, load_jdbc_manifest

AIRFLOW_EFFECTIVE_START_TEMPLATE = (
    "{{ data_interval_start.isoformat() "
    "if data_interval_start is defined and data_interval_start else '' }}"
)
AIRFLOW_EFFECTIVE_END_TEMPLATE = (
    "{{ data_interval_end.isoformat() "
    "if data_interval_end is defined and data_interval_end else '' }}"
)


def _target_path_from_job(job: dict) -> str:
    spec = job.get("spec") or {}

    return (
        (spec.get("bronze_write") or {}).get("target_path")
        or (spec.get("target") or {}).get("path")
        or (spec.get("silver_write") or {}).get("target_path")
        or (spec.get("gold_write") or {}).get("target_path")
        or job.get("target_path")
        or ""
    )


def infer_layer(job: dict) -> str:
    """
    Infer the OUTPUT layer of a job.
    This value is used for CONTROL_LAYER and CONTROL_JOB_CODE.
    """

    explicit = str(job.get("layer") or "").strip().lower()

    if explicit in {"bronze", "silver", "gold", "stream"}:
        return explicit

    job_type = str(job.get("job_type") or "").strip().lower()

    by_job_type = {
        "generic_api_to_bronze": "bronze",
        "generic_jdbc_manifest_to_bronze": "bronze",
        "generic_bronze_to_silver": "silver",
        "generic_silver_to_gold": "gold",
        "generic_delta_to_gold": "gold",
    }

    if job_type in by_job_type:
        return by_job_type[job_type]

    target_path = _target_path_from_job(job).lower()

    if "/gold/" in target_path:
        return "gold"
    if "/silver/" in target_path:
        return "silver"
    if "/bronze/" in target_path:
        return "bronze"

    return "batch"

def build_control_env_for_regular_job(
    *,
    env: dict[str, str],
    pipeline_spec: dict,
    job: dict,
) -> dict[str, str]:
    layer = infer_layer(job)
    pipeline_name = pipeline_spec["name"]
    job_name = job["name"]
    job_type = job["job_type"]

    job_code = f"{layer}.{pipeline_name}.{job_name}"

    target_path = (
        job.get("spec", {}).get("bronze_write", {}).get("target_path")
        or job.get("spec", {}).get("silver_write", {}).get("target_path")
        or job.get("spec", {}).get("gold_write", {}).get("target_path")
        or job.get("spec", {}).get("target", {}).get("path")
        or ""
    )

    env_job = dict(env)
    env_job.update(
        {
            "CONTROL_JOB_KEY": job_code,
            "CONTROL_JOB_CODE": job_code,
            "CONTROL_PIPELINE_NAME": pipeline_name,
            "CONTROL_JOB_NAME": job_name,
            "CONTROL_BASE_JOB_NAME": job_name,
            "CONTROL_ENTITY_NAME": f"{pipeline_name}.{job_name}",
            "CONTROL_LAYER": layer,
            "CONTROL_RUNNER": job_type,
            "CONTROL_TARGET_PATH": target_path,

            "AIRFLOW_DAG_ID": "{{ dag.dag_id }}",
            "AIRFLOW_DAG_RUN_ID": "{{ run_id }}",
            "AIRFLOW_TASK_ID": "{{ task.task_id }}",
            "AIRFLOW_TRY_NUMBER": "{{ ti.try_number }}",
            "EFFECTIVE_START_DATE": AIRFLOW_EFFECTIVE_START_TEMPLATE,
            "EFFECTIVE_END_DATE": AIRFLOW_EFFECTIVE_END_TEMPLATE,
        }
    )

    return env_job


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

    base_env = build_common_env()
    if common_env:
        base_env.update(common_env)

    manifest_ref = job["manifest_ref"]
    execution_strategy = job.get("execution_strategy", "one_task_per_manifest")

    base_spark_cfg = dict(job.get("spark", {}))
    base_spark_cfg["packages"] = []

    tasks: list[BashOperator] = []

    if execution_strategy == "one_task_per_manifest":
        manifest = load_jdbc_manifest(manifest_ref)
        source_id = manifest["source_id"]

        job_code = f"bronze.{source_id}.manifest"
        task_name = job["name"]

        env_manifest = dict(base_env)
        env_manifest.update(
            {
                "CONTROL_JOB_KEY": job_code,
                "CONTROL_JOB_CODE": job_code,
                "CONTROL_PIPELINE_NAME": pipeline_spec["name"],
                "CONTROL_JOB_NAME": task_name,
                "CONTROL_BASE_JOB_NAME": job["name"],
                "CONTROL_SOURCE_ID": source_id,
                "CONTROL_TABLE_ID": "manifest",
                "CONTROL_ENTITY_NAME": f"{source_id}.manifest",
                "CONTROL_LAYER": "bronze",
                "CONTROL_RUNNER": "generic_jdbc_manifest_to_bronze",
                "CONTROL_TARGET_PATH": "",
                "AIRFLOW_DAG_ID": "{{ dag.dag_id }}",
                "AIRFLOW_DAG_RUN_ID": "{{ run_id }}",
                "AIRFLOW_TASK_ID": "{{ task.task_id }}",
                "AIRFLOW_TRY_NUMBER": "{{ ti.try_number }}",
                "EFFECTIVE_START_DATE": AIRFLOW_EFFECTIVE_START_TEMPLATE,
                "EFFECTIVE_END_DATE": AIRFLOW_EFFECTIVE_END_TEMPLATE,
            }
        )

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
                env=env_manifest,
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
        source_id = manifest["source_id"]

        for table in get_enabled_tables(manifest):
            table_id = table["table_id"]
            task_name = f"{job['name']}__{table_id}"
            job_code = f"bronze.{source_id}.{table_id}"
            entity_name = f"{source_id}.{table_id}"
            target_path = (
                table.get("target_path")
                or (manifest.get("defaults") or {})
                .get("target_path_template", "")
                .format(source_id=source_id, table_id=table_id)
            )

            env_table = dict(base_env)
            env_table.update(
                {
                    "CONTROL_JOB_KEY": job_code,
                    "CONTROL_JOB_CODE": job_code,
                    "CONTROL_PIPELINE_NAME": pipeline_spec["name"],
                    "CONTROL_JOB_NAME": task_name,
                    "CONTROL_BASE_JOB_NAME": job["name"],
                    "CONTROL_SOURCE_ID": source_id,
                    "CONTROL_TABLE_ID": table_id,
                    "CONTROL_ENTITY_NAME": entity_name,
                    "CONTROL_LAYER": "bronze",
                    "CONTROL_RUNNER": "generic_jdbc_manifest_to_bronze",
                    "CONTROL_TARGET_PATH": target_path,
                    "AIRFLOW_DAG_ID": "{{ dag.dag_id }}",
                    "AIRFLOW_DAG_RUN_ID": "{{ run_id }}",
                    "AIRFLOW_TASK_ID": "{{ task.task_id }}",
                    "AIRFLOW_TRY_NUMBER": "{{ ti.try_number }}",
                    "EFFECTIVE_START_DATE": AIRFLOW_EFFECTIVE_START_TEMPLATE,
                    "EFFECTIVE_END_DATE": AIRFLOW_EFFECTIVE_END_TEMPLATE,
                }
            )

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
                    task_id=task_name,
                    bash_command=cmd,
                    env=env_table,
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


def forward_prefixed_env(prefixes: list[str]) -> dict[str, str]:
    forwarded = {}
    for key, value in os.environ.items():
        if any(key.startswith(prefix) for prefix in prefixes):
            forwarded[key] = value
    return forwarded

def build_common_env() -> dict[str, str]:
    required = [
        "S3_ENDPOINT",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
    ]

    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    repo_root = get_repo_root()

    env = {
        "PIPELINE_REPO_ROOT": str(repo_root),
        "PYTHONPATH": str(repo_root),
        "S3_ENDPOINT": os.environ["S3_ENDPOINT"],
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
        "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
        "SPARK_SUBMIT": os.getenv("SPARK_SUBMIT", "/home/airflow/.local/bin/spark-submit"),
        "SPARK_MASTER_URL": os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"),
        "PATH": f"/home/airflow/.local/bin:{os.getenv('PATH', '')}",
        "CONTROL_DB_ENABLED": os.getenv("CONTROL_DB_ENABLED", "true"),
        "CONTROL_DB_HOST": os.getenv("CONTROL_DB_HOST", "postgres-warehouse"),
        "CONTROL_DB_PORT": os.getenv("CONTROL_DB_PORT", "5432"),
        "CONTROL_DB_NAME": os.getenv("CONTROL_DB_NAME", os.getenv("POSTGRES_DB", "warehouse")),
        "CONTROL_DB_USER": os.getenv("CONTROL_DB_USER", os.getenv("POSTGRES_USER", "warehouse")),
        "CONTROL_DB_PASSWORD": os.getenv("CONTROL_DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "warehouse")),
        "CONTROL_DB_SSLMODE": os.getenv("CONTROL_DB_SSLMODE", "disable"),
    }

    env.update(
        forward_prefixed_env([
            "ECOM_",
            "METALPRICE_",
            "OPENWEATHER_",
        ])
    )

    return env


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

    env = build_control_env_for_regular_job(
        env=env,
        pipeline_spec=pipeline_spec,
        job=job,
    )

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
