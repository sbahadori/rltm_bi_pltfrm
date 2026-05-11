from __future__ import annotations

import json
import os
import socket
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def registry_path() -> Path:
    return Path(
        os.getenv(
            "JOB_RUN_REGISTRY_FILE",
            "/workspace/rltm_bi_pltfrm/runtime/job_runs/job_runs.jsonl",
        )
    )


def new_run_id(prefix: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}__{ts}__{uuid.uuid4().hex[:8]}"


def append_job_event(**event: Any) -> dict[str, Any]:
    path = registry_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    payload: dict[str, Any] = {
        "event_id": uuid.uuid4().hex,
        "ts": utc_now_iso(),
        "ts_epoch": int(time.time()),
        "host": socket.gethostname(),
        **event,
    }

    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")

    return payload


def exception_to_text(exc: BaseException) -> str:
    return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))