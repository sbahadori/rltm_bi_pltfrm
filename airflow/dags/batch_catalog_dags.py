## Dynamic Airflow DAG factory for catalog-driven batch pipelines.

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import DAG

REPO_ROOT = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()
AIRFLOW_APP_ROOT = REPO_ROOT / "airflow"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

if str(AIRFLOW_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_APP_ROOT))

from lib.batch_catalog_builder import build_tasks_from_pipeline_spec, load_enabled_pipeline_specs  # noqa: E402

CATALOG_PATH = "configs/orchestration/batch_pipeline_catalog.json"


def _parse_start_date(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


for pipeline_spec in load_enabled_pipeline_specs(CATALOG_PATH):
    try:
        dag_cfg = pipeline_spec["dag"]
        default_args_cfg = dag_cfg.get("default_args", {})

        dag = DAG(
            dag_id=pipeline_spec["name"],
            description=pipeline_spec.get("description", f"Catalog-driven DAG for {pipeline_spec['name']}"),
            start_date=_parse_start_date(dag_cfg["start_date"]),
            schedule=dag_cfg.get("schedule"),
            catchup=dag_cfg.get("catchup", False),
            max_active_runs=dag_cfg.get("max_active_runs", 1),
            default_args={
                "owner": default_args_cfg.get("owner", "admin"),
                "retries": default_args_cfg.get("retries", 0),
                "retry_delay": timedelta(minutes=default_args_cfg.get("retry_delay_minutes", 1)),
            },
            tags=dag_cfg.get("tags", ["catalog_driven", "airflow", "batch"]),
        )

        with dag:
            build_tasks_from_pipeline_spec(dag=dag, pipeline_spec=pipeline_spec)

        globals()[pipeline_spec["name"]] = dag

    except Exception as exc:
        print(f"[batch_catalog_dags] Failed to build DAG for pipeline '{pipeline_spec.get('name', '?')}': {exc}")