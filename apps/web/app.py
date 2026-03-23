import os
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

app = FastAPI(title="Web Demo")
templates = Jinja2Templates(directory="templates")

COLLECTOR_URL = os.getenv("COLLECTOR_URL", "http://collector:8000/collect")
PUBLIC_COLLECTOR_URL = os.getenv("PUBLIC_COLLECTOR_URL", "http://localhost:8000/collect")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def get_or_create_cookie(request: Request, cookie_name: str, prefix: str) -> tuple[str, bool]:
    current = request.cookies.get(cookie_name)
    if current:
        return current, False
    return new_id(prefix), True


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    anonymous_id, anon_new = get_or_create_cookie(request, "anonymous_id", "anon")
    session_id, sess_new = get_or_create_cookie(request, "session_id", "sess")

    response = templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "anonymous_id": anonymous_id,
            "session_id": session_id,
            "public_collector_url": PUBLIC_COLLECTOR_URL,
        },
    )

    if anon_new:
        response.set_cookie(
            key="anonymous_id",
            value=anonymous_id,
            max_age=60 * 60 * 24 * 180,
            httponly=False,
            samesite="lax",
        )

    if sess_new:
        response.set_cookie(
            key="session_id",
            value=session_id,
            max_age=60 * 60 * 8,
            httponly=False,
            samesite="lax",
        )

    return response


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/backend-action")
async def backend_action(request: Request):
    anonymous_id, _ = get_or_create_cookie(request, "anonymous_id", "anon")
    session_id, _ = get_or_create_cookie(request, "session_id", "sess")

    body = await request.json()

    backend_event: dict[str, Any] = {
        "event_id": new_id("evt"),
        "event_ts": now_iso(),
        "event_type": "backend_log",
        "source": "backend",
        "anonymous_id": anonymous_id,
        "session_id": session_id,
        "page_url": str(request.headers.get("referer", "/")),
        "cookies": {
            "anonymous_id": anonymous_id,
            "session_id": session_id,
        },
        "properties": {
            "backend_event_name": body.get("backend_event_name", "unknown"),
            "item_id": body.get("item_id"),
            "item_title": body.get("item_title"),
            "message": body.get("message", "structured backend event"),
        },
    }

    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.post(COLLECTOR_URL, json=backend_event)
        resp.raise_for_status()

    return JSONResponse({"ok": True, "forwarded": backend_event})