from __future__ import annotations

import json
from typing import Any

from shared.control.postgres import fetch_all


def _parse_raw_config(raw_config: Any) -> dict[str, Any] | None:
    if not raw_config:
        return None

    if isinstance(raw_config, dict):
        return raw_config

    if isinstance(raw_config, str):
        return json.loads(raw_config)

    return None


def load_enabled_pipeline_specs_from_control_db() -> list[dict[str, Any]]:
    """
    Load pipeline design definitions from PostgreSQL Control Plane.

    Important:
    - meta.pipeline.raw_config is the source for DAG generation.
    - meta.job is for runtime/control-plane observability.
    """
    rows = fetch_all(
        """
        SELECT
            pipeline_name,
            raw_config
        FROM meta.pipeline
        WHERE is_active = TRUE
        ORDER BY pipeline_name
        """
    )

    specs_by_name: dict[str, dict[str, Any]] = {}

    for row in rows:
        spec = _parse_raw_config(row.get("raw_config"))

        if not spec:
            continue

        if not spec.get("enabled", True):
            continue

        pipeline_name = spec.get("name") or row.get("pipeline_name")

        if not pipeline_name:
            continue

        spec["name"] = pipeline_name

        # Avoid duplicated DAG registration.
        specs_by_name[pipeline_name] = spec

    return list(specs_by_name.values())
