import os
import json
import asyncio
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Collector")

allowed_origins = os.getenv(
    "ALLOWED_ORIGINS",
    "http://localhost:8080,http://127.0.0.1:8080,http://localhost:8081,http://127.0.0.1:8081,http://localhost:8091,http://127.0.0.1:8091"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in allowed_origins if o.strip()],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

STORAGE_FILE = Path(os.getenv("STORAGE_FILE", "/data/events.jsonl"))
WRITE_LOCK = asyncio.Lock()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/collect")
async def collect(request: Request) -> dict[str, Any]:
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload must be a JSON object")

    if "event_type" not in payload:
        raise HTTPException(status_code=400, detail="event_type is required")

    enriched = {
        **payload,
        "collector_received_at": now_iso(),
        "collector_client_ip": request.client.host if request.client else None,
        "collector_user_agent": request.headers.get("user-agent"),
    }

    STORAGE_FILE.parent.mkdir(parents=True, exist_ok=True)

    async with WRITE_LOCK:
        with STORAGE_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(enriched, ensure_ascii=False) + "\n")

    return {
        "ok": True,
        "event_type": enriched.get("event_type"),
    }


@app.get("/events")
async def events(limit: int = 20) -> dict[str, Any]:
    if not STORAGE_FILE.exists():
        return {"count": 0, "items": [], "bad_lines": 0}

    items = []
    bad_lines = 0

    with STORAGE_FILE.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except json.JSONDecodeError:
                bad_lines += 1
                continue

    return {
        "count": min(limit, len(items)),
        "items": items[-limit:],
        "bad_lines": bad_lines,
    }