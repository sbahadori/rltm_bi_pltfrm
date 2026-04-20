import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import DAG

REPO_ROOT = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()
AIRFLOW_APP_ROOT = REPO_ROOT / "airflow"

if str(AIRFLOW_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_APP_ROOT))

from lib.pipeline_runner import build_tasks_from_configs  # noqa: E402


GOLD_PRICE_CONFIGS = [
    "configs/pipelines/gold_price_ingest.yaml",
    "configs/pipelines/gold_price_silver.yaml",
    "configs/pipelines/gold_price_bars_1m.yaml",
    "configs/pipelines/gold_price_metrics.yaml",
]


with DAG(
    dag_id="gold_price_pipeline",
    description="Config-driven orchestration for the gold-price Bronze/Silver/Gold pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "admin",
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["gold_price", "spark", "config_driven", "airflow"],
) as dag:
    build_tasks_from_configs(
        dag=dag,
        config_paths=GOLD_PRICE_CONFIGS,
    )