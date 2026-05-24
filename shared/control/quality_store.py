from __future__ import annotations

import json
import os
from typing import Any

from shared.control.postgres import call_usp_void, control_db_enabled


def _to_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def write_quality_result(
    *,
    run_id: str,
    job_key: str,
    dataset_key: str,
    status: str,
    observed_value: Any = None,
    expected_value: Any = None,
    details: dict[str, Any] | None = None,
    job_id: int | None = None,
    rule_id: int | None = None,
    rule_name: str | None = None,
    rule_type: str | None = None,
) -> None:
    """
    Write one data-quality result into the control DB.

    Design:
      - rule_name and rule_type are not passed as standalone DB columns.
      - They are stored inside details JSON.
      - DB write is non-critical by default.
      - If CONTROL_DB_STRICT=true, DB write failures are raised.

    Expected USP:
      ctl.usp_insert_quality_result(
          p_run_id,
          p_job_id,
          p_job_key,
          p_dataset_key,
          p_status,
          p_observed_value,
          p_expected_value,
          p_details,
          p_rule_id DEFAULT NULL
      )
    """

    if not control_db_enabled():
        return

    enriched_details = dict(details or {})

    if rule_name:
        enriched_details["rule_name"] = rule_name

    if rule_type:
        enriched_details["rule_type"] = rule_type

    try:
        call_usp_void(
            "usp_insert_quality_result",
            (
                run_id,
                job_id,
                job_key,
                dataset_key,
                status,
                _to_text(observed_value),
                _to_text(expected_value),
                json.dumps(enriched_details, ensure_ascii=False, default=str),
                rule_id,
            ),
        )

    except Exception as exc:
        print(
            f"[WARN] Failed to write quality result to control DB: {exc}",
            flush=True,
        )

        if os.getenv("CONTROL_DB_STRICT", "false").lower() in {"1", "true", "yes"}:
            raise