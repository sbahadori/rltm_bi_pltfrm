from __future__ import annotations

from datetime import datetime, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


DEFAULT_CATALOG_PATH = "configs/batch/pipeline_catalog.json"

SELECTIONS_JSON='{{ dag_run.conf.get("selections", []) | tojson }}'

with DAG(
    dag_id="control_plane_onboarding",
    description="Manual DAG to onboard catalog pipelines/jobs into the PostgreSQL control plane.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["control-plane", "onboarding", "manual", "metadata"],
) as dag:

    precheck_control_db = BashOperator(
        task_id="precheck_control_db",
        bash_command=r"""
set -euo pipefail

python - <<'PY'
import os
import psycopg2

cfg = {
    "host": os.getenv("CONTROL_DB_HOST", "postgres-warehouse"),
    "port": int(os.getenv("CONTROL_DB_PORT", "5432")),
    "dbname": os.getenv("CONTROL_DB_NAME", os.getenv("POSTGRES_DB", "warehouse")),
    "user": os.getenv("CONTROL_DB_USER", os.getenv("POSTGRES_USER", "warehouse")),
    "password": os.getenv("CONTROL_DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "warehouse")),
    "sslmode": os.getenv("CONTROL_DB_SSLMODE", "disable"),
}

required = [
    "usp_onboard_pipeline",
    "usp_onboard_source_system",
    "usp_onboard_job",
    "usp_onboard_dataset",
    "usp_onboard_job_dependency",
    "usp_list_active_pipeline_specs",
    "usp_get_active_job_metadata",
]

with psycopg2.connect(**cfg) as conn:
    with conn.cursor() as cur:
        for routine_name in required:
            cur.execute(
                '''
                SELECT COUNT(*)
                FROM information_schema.routines
                WHERE routine_schema = 'ctl'
                  AND routine_name = %s
                ''',
                (routine_name,),
            )
            count = cur.fetchone()[0]
            if count == 0:
                raise RuntimeError(f"Missing required ctl routine: {routine_name}")

print("[ONBOARD_PRECHECK_OK] control DB routines exist")
PY
""",
        append_env=True,
    )

    onboard_control_plane = BashOperator(
        task_id="onboard_control_plane",
        bash_command=r"""
set -euo pipefail

CATALOG_PATH="{{ dag_run.conf.get('catalog_path', 'configs/batch/pipeline_catalog.json') }}"
PIPELINE_NAME="{{ dag_run.conf.get('pipeline_name', '') }}"
JOB_NAME="{{ dag_run.conf.get('job_name', '') }}"
TABLE_ID="{{ dag_run.conf.get('table_id', '') }}"
SELECTIONS_JSON='{{ dag_run.conf.get("selections", []) | tojson }}'
INCLUDE_DEPENDENCIES="{{ dag_run.conf.get('include_dependencies', true) | string | lower }}"
DRY_RUN="{{ dag_run.conf.get('dry_run', false) | string | lower }}"

CMD=(python -m shared.onboarding.control_plane --catalog "$CATALOG_PATH")

if [ "$SELECTIONS_JSON" != "[]" ]; then
  CMD+=(--selections-json "$SELECTIONS_JSON")
else
  if [ -n "$PIPELINE_NAME" ]; then
    CMD+=(--pipeline-name "$PIPELINE_NAME")
  fi

  if [ -n "$JOB_NAME" ]; then
    CMD+=(--job-name "$JOB_NAME")
  fi

  if [ -n "$TABLE_ID" ]; then
    CMD+=(--table-id "$TABLE_ID")
  fi

  if [ "$INCLUDE_DEPENDENCIES" != "true" ]; then
    CMD+=(--skip-dependencies)
  fi
fi

if [ "$DRY_RUN" = "true" ]; then
  CMD+=(--dry-run)
fi

echo "[ONBOARD_CMD] ${CMD[*]}"
"${CMD[@]}"
""",
    append_env=True,
)
    
    validate_onboarding = BashOperator(
        task_id="validate_onboarding",
        bash_command=r"""

set -euo pipefail

python - <<'PY'
import os
import psycopg2

cfg = {
    "host": os.getenv("CONTROL_DB_HOST", "postgres-warehouse"),
    "port": int(os.getenv("CONTROL_DB_PORT", "5432")),
    "dbname": os.getenv("CONTROL_DB_NAME", os.getenv("POSTGRES_DB", "warehouse")),
    "user": os.getenv("CONTROL_DB_USER", os.getenv("POSTGRES_USER", "warehouse")),
    "password": os.getenv("CONTROL_DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "warehouse")),
    "sslmode": os.getenv("CONTROL_DB_SSLMODE", "disable"),
}

with psycopg2.connect(**cfg) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM meta.pipeline WHERE is_active IS TRUE")
        active_pipelines = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM meta.job WHERE is_active IS TRUE AND active_flag IS TRUE")
        active_jobs = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM ctl.usp_list_active_pipeline_specs()")
        active_specs = cur.fetchone()[0]

print(
    "[ONBOARD_VALIDATE] "
    f"active_pipelines={active_pipelines} "
    f"active_jobs={active_jobs} "
    f"active_specs={active_specs}"
)

if active_pipelines <= 0:
    raise RuntimeError("No active pipelines onboarded")

if active_jobs <= 0:
    raise RuntimeError("No active jobs onboarded")

if active_specs <= 0:
    raise RuntimeError("ctl.usp_list_active_pipeline_specs returned no specs")
PY
""",
        append_env=True,
    )

    print_next_steps = BashOperator(
        task_id="print_next_steps",
        bash_command=r"""
echo "[ONBOARD_SUCCESS]"
echo "Control plane metadata is ready."
echo ""
echo "Next checks:"
echo "SELECT pipeline_name, airflow_dag_id, is_active FROM meta.pipeline;"
echo "SELECT job_code, job_name, pipeline_name, layer, runner FROM meta.job ORDER BY pipeline_name, job_code;"
echo ""
echo "Restart/reparse Airflow DAGs if needed:"
echo "docker restart airflow-dag-processor airflow-scheduler"
""",
        append_env=True,
    )

    (
        precheck_control_db
        >> onboard_control_plane
        >> validate_onboarding
        >> print_next_steps
    )