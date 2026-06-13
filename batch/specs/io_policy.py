from __future__ import annotations

from typing import Any


VALID_READ_MODES = {"full", "snapshot", "append", "incremental", "cdc"}
VALID_WRITE_MODES = {"append", "overwrite", "merge", "ignore", "errorifexists"}

VALID_READ_STRATEGIES_BY_MODE = {
    "full": {"full_scan", "overwrite", "append_snapshot", "full_refresh"},
    "snapshot": {"api_request", "snapshot_request"},
    "append": {"append_event", "append_snapshot"},
    "incremental": {
        "target_max_watermark",
        "source_watermark",
        "timestamp",
        "numeric_watermark",
        "sequence",
        "rowversion",
    },
    "cdc": {
        "sqlserver_cdc",
        "sqlserver_change_tracking",
        "postgres_logical_replication",
        "mysql_binlog",
        "debezium",
    },
}


def normalize_read_policy(
    *,
    spec: dict[str, Any],
    layer: str,
    job_type: str | None = None,
) -> dict[str, Any]:
    """
    Unified read/load contract for Bronze, Silver, and Gold.

    Preferred:
        spec.read_policy

    Backward compatibility:
        spec.incremental
        spec.load_type / spec.strategy
        JDBC table config load_type / strategy / watermark
    """
    if isinstance(spec.get("read_policy"), dict):
        policy = dict(spec["read_policy"])
    else:
        policy = {}

    layer = str(layer or "").lower()
    job_type = str(job_type or "").lower()

    incremental = spec.get("incremental") or {}
    if not policy and incremental.get("enabled"):
        policy = {
            "mode": "incremental",
            "strategy": incremental.get("strategy", "target_max_watermark"),
            "source_alias": incremental.get("source_alias"),
            "watermark_column": incremental.get("watermark_column"),
            "lookback_minutes": incremental.get("lookback_minutes", 0),
        }

    if not policy and spec.get("load_type"):
        legacy_load_type = str(spec.get("load_type") or "").strip().lower()
        legacy_strategy = str(spec.get("strategy") or "").strip().lower()
        job_type_lc = str(job_type or "").strip().lower()

        # Legacy API event ingestion:
        # old: load_type=event, strategy=append_event
        # new read contract: snapshot/api_request
        # write behavior remains controlled by write_policy / bronze_write.
        if legacy_load_type == "event" or (
            "api_to_bronze" in job_type_lc and legacy_strategy == "append_event"
        ):
            policy = {
                "mode": "snapshot",
                "strategy": "api_request",
            }
        else:
            policy = {
                "mode": legacy_load_type,
                "strategy": legacy_strategy or default_read_strategy(
                    layer=layer,
                    job_type=job_type,
                ),
            }

    if not policy:
        policy = {
            "mode": default_read_mode(layer=layer, job_type=job_type),
            "strategy": default_read_strategy(layer=layer, job_type=job_type),
        }

    policy.setdefault("mode", default_read_mode(layer=layer, job_type=job_type))
    policy.setdefault("strategy", default_read_strategy(layer=layer, job_type=job_type))

    validate_read_policy(policy)
    return policy


def normalize_write_policy(
    *,
    spec: dict[str, Any],
    layer: str,
    job_type: str | None = None,
) -> dict[str, Any]:
    """
    Unified write contract for Bronze, Silver, and Gold.

    Preferred:
        spec.write_policy

    Backward compatibility:
        spec.bronze_write
        spec.target
        spec.silver_write
        spec.gold_write
    """
    if isinstance(spec.get("write_policy"), dict):
        policy = dict(spec["write_policy"])
    else:
        policy = {}

    target = spec.get("target") or {}
    bronze_write = spec.get("bronze_write") or {}
    silver_write = spec.get("silver_write") or {}
    gold_write = spec.get("gold_write") or {}

    legacy = bronze_write or target or silver_write or gold_write

    if not policy:
        policy = {
            "mode": legacy.get("mode") or default_write_mode(layer=layer, job_type=job_type),
            "target_path": (
                legacy.get("target_path")
                or legacy.get("path")
                or spec.get("target_path")
            ),
            "format": legacy.get("format", "delta"),
            "merge_keys": legacy.get("merge_keys", []),
            "partition_by": legacy.get("partition_by", []),
        }

    policy.setdefault("mode", default_write_mode(layer=layer, job_type=job_type))
    policy.setdefault("format", "delta")
    policy.setdefault("merge_keys", [])
    policy.setdefault("partition_by", [])

    if not policy.get("target_path"):
        policy["target_path"] = (
            target.get("path")
            or bronze_write.get("target_path")
            or silver_write.get("target_path")
            or gold_write.get("target_path")
            or spec.get("target_path")
        )

    validate_write_policy(policy)
    return policy


def normalize_jdbc_table_read_policy(table_cfg: dict[str, Any]) -> dict[str, Any]:
    """
    Adapter for existing JDBC manifest config.
    Does not break current JDBC behavior.
    """
    load_type = table_cfg.get("load_type", "full")
    strategy = table_cfg.get("strategy", "overwrite")
    watermark = table_cfg.get("watermark") or {}

    policy = {
        "mode": load_type,
        "strategy": strategy,
    }

    if load_type == "incremental":
        policy.update(
            {
                "watermark_column": watermark.get("column"),
                "watermark_type": watermark.get("type"),
                "initial_value": watermark.get("initial_value", 0),
                "state_backend": "control_db",
            }
        )

    validate_read_policy(policy)
    return policy


def normalize_jdbc_table_write_policy(table_cfg: dict[str, Any]) -> dict[str, Any]:
    policy = {
        "mode": table_cfg.get("bronze_mode") or table_cfg.get("write_mode") or "append",
        "target_path": table_cfg.get("target_path"),
        "format": table_cfg.get("bronze_format", table_cfg.get("format", "delta")),
        "partition_by": table_cfg.get("partition_by", []),
        "merge_keys": table_cfg.get("merge_keys", []),
    }

    validate_write_policy(policy)
    return policy


def validate_read_policy(policy: dict[str, Any]) -> None:
    mode = policy.get("mode")
    strategy = policy.get("strategy")

    if mode not in VALID_READ_MODES:
        raise ValueError(f"Invalid read_policy.mode='{mode}'. Valid values: {sorted(VALID_READ_MODES)}")

    valid_strategies = VALID_READ_STRATEGIES_BY_MODE.get(mode, set())
    if strategy and strategy not in valid_strategies:
        raise ValueError(
            f"Invalid read_policy.strategy='{strategy}' for mode='{mode}'. "
            f"Valid values: {sorted(valid_strategies)}"
        )

    if mode == "incremental":
        if not policy.get("watermark_column"):
            raise ValueError("read_policy.watermark_column is required when mode='incremental'.")


def validate_write_policy(policy: dict[str, Any]) -> None:
    mode = policy.get("mode")

    if mode not in VALID_WRITE_MODES:
        raise ValueError(f"Invalid write_policy.mode='{mode}'. Valid values: {sorted(VALID_WRITE_MODES)}")

    if not policy.get("target_path"):
        raise ValueError("write_policy.target_path is required.")

    if mode == "merge" and not policy.get("merge_keys"):
        raise ValueError("write_policy.merge_keys is required when mode='merge'.")


def default_read_mode(*, layer: str, job_type: str | None = None) -> str:
    job_type = str(job_type or "").lower()
    layer = str(layer or "").lower()

    if "api_to_bronze" in job_type:
        return "snapshot"

    if "jdbc" in job_type:
        return "full"

    if layer == "silver":
        return "full"

    if layer == "gold":
        return "full"

    return "full"

def default_read_strategy(*, layer: str, job_type: str | None = None) -> str:
    job_type = str(job_type or "").lower()
    layer = str(layer or "").lower()

    if "api_to_bronze" in job_type:
        return "api_request"

    if "jdbc" in job_type:
        return "overwrite"

    if layer == "silver":
        return "full_scan"

    if layer == "gold":
        return "full_refresh"

    return "full_scan"


def default_write_mode(*, layer: str, job_type: str | None = None) -> str:
    layer = str(layer or "").lower()

    if layer == "bronze":
        return "append"

    if layer in {"silver", "gold"}:
        return "merge"

    return "append"


def legacy_target_from_write_policy(write_policy: dict[str, Any]) -> dict[str, Any]:
    return {
        "path": write_policy["target_path"],
        "format": write_policy.get("format", "delta"),
        "mode": write_policy.get("mode", "merge"),
        "merge_keys": write_policy.get("merge_keys", []),
        "partition_by": write_policy.get("partition_by", []),
    }


def legacy_bronze_write_from_write_policy(write_policy: dict[str, Any]) -> dict[str, Any]:
    return {
        "target_path": write_policy["target_path"],
        "format": write_policy.get("format", "delta"),
        "mode": write_policy.get("mode", "append"),
        "partition_by": write_policy.get("partition_by", []),
    }