from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from batch.specs.io_policy import normalize_read_policy, normalize_write_policy

SUPPORTED_JOB_TYPES = {
    "spark_batch",
    "generic_api_to_bronze",
    "generic_bronze_to_silver",
    "generic_silver_to_gold",
    "generic_jdbc_manifest_to_bronze",
    
}

def _views_from_spec(spec: dict[str, Any]) -> list[dict[str, Any]]:
    views = spec.get("views") or spec.get("sources") or []
    if isinstance(views, list):
        return [v for v in views if isinstance(v, dict)]
    return []


def _has_sql_query(spec: dict[str, Any]) -> bool:
    sql = spec.get("sql") or {}
    return isinstance(sql, dict) and bool(str(sql.get("query") or "").strip())


def _sync_legacy_io_sections(spec: dict[str, Any], *, layer: str, job_type: str) -> None:
    read_policy = normalize_read_policy(spec=spec, layer=layer, job_type=job_type)
    write_policy = normalize_write_policy(spec=spec, layer=layer, job_type=job_type)

    spec["read_policy"] = read_policy
    spec["write_policy"] = write_policy

    if "target" not in spec:
        spec["target"] = {
            "path": write_policy["target_path"],
            "format": write_policy.get("format", "delta"),
            "mode": write_policy.get("mode", "merge"),
            "merge_keys": write_policy.get("merge_keys", []),
            "partition_by": write_policy.get("partition_by", []),
        }

    if "source" not in spec:
        views = _views_from_spec(spec)
        if views:
            spec["source"] = {
                "path": views[0]["path"],
                "format": views[0].get("format", "delta"),
            }

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

def validate_generic_jdbc_manifest_to_bronze_spec(
    pipeline_name: str,
    job: dict[str, Any],
) -> None:
    if "manifest_ref" not in job:
        raise ValueError(
            f"generic_jdbc_manifest_to_bronze job '{job['name']}' "
            f"in pipeline '{pipeline_name}' missing manifest_ref"
        )

    strategy = job.get("execution_strategy", "one_task_per_manifest")
    if strategy not in {"one_task_per_manifest", "one_task_per_table"}:
        raise ValueError(
            f"generic_jdbc_manifest_to_bronze job '{job['name']}' "
            f"has unsupported execution_strategy: {strategy}"
        )

    manifest_path = resolve_repo_path(job["manifest_ref"])
    if not manifest_path.exists():
        raise ValueError(
            f"generic_jdbc_manifest_to_bronze job '{job['name']}' "
            f"manifest_ref does not exist: {manifest_path}"
        )
    

def validate_job_spec(pipeline_name: str, job: dict[str, Any]) -> None:
    required = [
        "name",
        "enabled",
        "job_type",
        "dependencies",
        "execution_timeout_minutes",
    ]

    missing = [k for k in required if k not in job]
    if missing:
        raise ValueError(
            f"Job in pipeline '{pipeline_name}' missing required keys: {missing}"
        )

    if not isinstance(job["dependencies"], list):
        raise ValueError(
            f"Job '{job['name']}' in pipeline '{pipeline_name}' must have dependencies as a list"
        )

    job_type = job["job_type"]

    if job_type not in SUPPORTED_JOB_TYPES:
        raise ValueError(
            f"Unsupported job_type '{job_type}' in pipeline '{pipeline_name}'. "
            f"Supported values: {sorted(SUPPORTED_JOB_TYPES)}"
        )

    if job_type == "generic_jdbc_manifest_to_bronze":
        validate_generic_jdbc_manifest_to_bronze_spec(pipeline_name, job)
        return

    if "spec" not in job:
        raise ValueError(
            f"Job '{job['name']}' in pipeline '{pipeline_name}' must define spec"
        )

    if not isinstance(job["spec"], dict):
        raise ValueError(
            f"Job '{job['name']}' in pipeline '{pipeline_name}' must have spec as an object"
        )

    if job_type == "spark_batch":
        validate_spark_batch_spec(pipeline_name, job)
    elif job_type == "generic_api_to_bronze":
        validate_generic_api_to_bronze_spec(pipeline_name, job)
    elif job_type == "generic_bronze_to_silver":
        validate_generic_bronze_to_silver_spec(pipeline_name, job)
    elif job_type == "generic_silver_to_gold":
        validate_generic_silver_to_gold_spec(pipeline_name, job)


def validate_spark_batch_spec(pipeline_name: str, job: dict[str, Any]) -> None:
    spec = job["spec"]
    required = ["entrypoint", "args", "spark"]
    missing = [k for k in required if k not in spec]
    if missing:
        raise ValueError(
            f"spark_batch job '{job['name']}' in pipeline '{pipeline_name}' missing spec keys: {missing}"
        )

    if not isinstance(spec["args"], dict):
        raise ValueError(
            f"spark_batch job '{job['name']}' in pipeline '{pipeline_name}' must have spec.args as an object"
        )

    if not isinstance(spec["spark"], dict):
        raise ValueError(
            f"spark_batch job '{job['name']}' in pipeline '{pipeline_name}' must have spec.spark as an object"
        )

    entrypoint_path = resolve_repo_path(spec["entrypoint"])
    if not entrypoint_path.exists():
        raise ValueError(
            f"spark_batch job '{job['name']}' in pipeline '{pipeline_name}' entrypoint does not exist: {entrypoint_path}"
        )


def validate_generic_api_to_bronze_spec(pipeline_name: str, job: dict[str, Any]) -> None:
    spec = job["spec"]
    required = [
        "source",
        "auth",
        "request",
        "response",
        "validation",
        "mapping",
        "bronze_write",
        "runtime_policy",
        "spark",
    ]
    missing = [k for k in required if k not in spec]
    if missing:
        raise ValueError(
            f"generic_api_to_bronze job '{job['name']}' in pipeline '{pipeline_name}' missing spec keys: {missing}"
        )

    _require_object_sections(
        pipeline_name,
        job["name"],
        spec,
        required,
        job_type="generic_api_to_bronze",
    )

    source_required = ["base_url", "method", "timeout_seconds"]
    missing_source = [k for k in source_required if k not in spec["source"]]
    if missing_source:
        raise ValueError(
            f"generic_api_to_bronze job '{job['name']}' in pipeline '{pipeline_name}' "
            f"missing source keys: {missing_source}"
        )

    auth_required = ["type", "secret_env"]
    missing_auth = [k for k in auth_required if k not in spec["auth"]]
    if missing_auth:
        raise ValueError(
            f"generic_api_to_bronze job '{job['name']}' in pipeline '{pipeline_name}' "
            f"missing auth keys: {missing_auth}"
        )

    if spec["auth"]["type"] == "query_param" and "param_name" not in spec["auth"]:
        raise ValueError(
            f"generic_api_to_bronze job '{job['name']}' in pipeline '{pipeline_name}' "
            f"requires auth.param_name for query_param auth"
        )

    if spec["response"].get("format") != "json":
        raise ValueError(
            f"generic_api_to_bronze job '{job['name']}' in pipeline '{pipeline_name}' currently supports only JSON responses"
        )

    if "target_path" not in spec["bronze_write"]:
        raise ValueError(
            f"generic_api_to_bronze job '{job['name']}' in pipeline '{pipeline_name}' missing bronze_write.target_path"
        )


def validate_generic_bronze_to_silver_spec(pipeline_name: str, job: dict[str, Any]) -> None:
    spec = job["spec"]

    spec.setdefault("filters", [])
    spec.setdefault("quality_rules", [])
    spec.setdefault("dedupe", {"key_columns": [], "order_by": []})
    spec.setdefault("spark", {"master": None, "packages": [], "conf": {}})
    spec.setdefault("select_map", {})
    spec.setdefault("derived_fields", {})

    _sync_legacy_io_sections(
        spec,
        layer="silver",
        job_type="generic_bronze_to_silver",
    )

    views = _views_from_spec(spec)

    if views:
        for idx, view in enumerate(views):
            if not view.get("alias"):
                raise ValueError(
                    f"generic_bronze_to_silver job '{job['name']}' in pipeline '{pipeline_name}' "
                    f"missing views[{idx}].alias"
                )
            if not view.get("path"):
                raise ValueError(
                    f"generic_bronze_to_silver job '{job['name']}' in pipeline '{pipeline_name}' "
                    f"missing views[{idx}].path"
                )

        if not _has_sql_query(spec):
            raise ValueError(
                f"generic_bronze_to_silver job '{job['name']}' in pipeline '{pipeline_name}' "
                "must define spec.sql.query when spec.views is used"
            )

    elif not spec.get("source", {}).get("path"):
        raise ValueError(
            f"generic_bronze_to_silver job '{job['name']}' in pipeline '{pipeline_name}' "
            "requires either spec.source.path or spec.views[].path"
        )
    

def validate_generic_silver_to_gold_spec(pipeline_name: str, job: dict[str, Any]) -> None:
    spec = job["spec"]

    spec.setdefault("filters", [])
    spec.setdefault("quality_rules", [])
    spec.setdefault("dedupe", {"key_columns": [], "order_by": []})
    spec.setdefault("spark", {"master": None, "packages": [], "conf": {}})
    spec.setdefault("select_map", {})
    spec.setdefault("derived_fields", {})

    _sync_legacy_io_sections(
        spec,
        layer="gold",
        job_type="generic_silver_to_gold",
    )

    views = _views_from_spec(spec)

    if views:
        for idx, view in enumerate(views):
            if not view.get("alias"):
                raise ValueError(
                    f"generic_silver_to_gold job '{job['name']}' in pipeline '{pipeline_name}' "
                    f"missing views[{idx}].alias"
                )
            if not view.get("path"):
                raise ValueError(
                    f"generic_silver_to_gold job '{job['name']}' in pipeline '{pipeline_name}' "
                    f"missing views[{idx}].path"
                )

        if not _has_sql_query(spec):
            raise ValueError(
                f"generic_silver_to_gold job '{job['name']}' in pipeline '{pipeline_name}' "
                "must define spec.sql.query when spec.views is used"
            )

    elif not spec.get("source", {}).get("path"):
        raise ValueError(
            f"generic_silver_to_gold job '{job['name']}' in pipeline '{pipeline_name}' "
            "requires either spec.source.path or spec.views[].path"
        )

def _require_object_sections(
    pipeline_name: str,
    job_name: str,
    spec: dict[str, Any],
    section_names: list[str],
    *,
    job_type: str,
) -> None:
    for section_name in section_names:
        if not isinstance(spec[section_name], dict):
            raise ValueError(
                f"{job_type} job '{job_name}' in pipeline '{pipeline_name}' "
                f"must have spec.{section_name} as an object"
            )


def _require_list_sections(
    pipeline_name: str,
    job_name: str,
    spec: dict[str, Any],
    section_names: list[str],
    *,
    job_type: str,
) -> None:
    for section_name in section_names:
        if not isinstance(spec[section_name], list):
            raise ValueError(
                f"{job_type} job '{job_name}' in pipeline '{pipeline_name}' "
                f"must have spec.{section_name} as a list"
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


def get_job_by_name(catalog_path: str | Path, pipeline_name: str, job_name: str) -> dict[str, Any]:
    pipeline = get_pipeline_by_name(catalog_path, pipeline_name)
    for job in pipeline["jobs"]:
        if job["name"] == job_name and job.get("enabled", True):
            return job
    raise ValueError(
        f"Enabled job '{job_name}' not found in pipeline '{pipeline_name}'"
    )