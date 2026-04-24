# وظیفه این فایل
# registry را بخواند
# برای هر job یک Airflow task بسازد
# dependencyها را وصل کند
# command مناسب را با reusable runner بسازد


from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator


def _bootstrap_repo_path() -> Path:
    repo_root = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    return repo_root


REPO_ROOT = _bootstrap_repo_path()

from shared.lib.batch_catalog_utils import get_enabled_pipelines  # noqa: E402
from shared.lib.spark_submit_utils import build_spark_submit_command, get_repo_root  # noqa: E402


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


def build_airflow_task_from_job(job: dict, dag, common_env: dict[str, str] | None = None) -> BashOperator:
    env = build_common_env()
    if common_env:
        env.update(common_env)

    legacy_like_config = {
        "name": job["name"],
        "domain": job.get("domain", "batch_api"),
        "job_type": job["job_type"],
        "entrypoint": job["entrypoint"],
        "args": job.get("args", {}),
        "dependencies": job.get("dependencies", []),
        "schedule": "catalog",
        "execution_timeout_minutes": job.get("execution_timeout_minutes", 30),
        "spark_master": job.get("spark_master"),
        "packages": job.get("packages", []),
        "spark_conf": job.get("spark_conf", {}),
    }

    cmd = build_spark_submit_command(legacy_like_config)

    return BashOperator(
        task_id=job["name"],
        bash_command=cmd,
        env=env,
        append_env=True,
        execution_timeout=timedelta(minutes=job.get("execution_timeout_minutes", 30)),
        dag=dag,
    )


def build_tasks_from_pipeline_spec(
    *,
    dag,
    pipeline_spec: dict,
    common_env: dict[str, str] | None = None,
) -> dict[str, BashOperator]:
    jobs = [job for job in pipeline_spec["jobs"] if job.get("enabled", True)]

    tasks: dict[str, BashOperator] = {}
    for job in jobs:
        tasks[job["name"]] = build_airflow_task_from_job(job, dag, common_env=common_env)

    for job in jobs:
        for dep in job.get("dependencies", []):
            tasks[dep] >> tasks[job["name"]]

    return tasks


def load_enabled_pipeline_specs(catalog_path: str) -> list[dict]:
    return get_enabled_pipelines(catalog_path)