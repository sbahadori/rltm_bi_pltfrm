import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware


logger = logging.getLogger("collector")
logging.basicConfig(level=logging.INFO)


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user_events")
STORAGE_FILE_RAW = os.getenv("STORAGE_FILE", "/data/events.jsonl")
STORAGE_FILE = Path(STORAGE_FILE_RAW) if STORAGE_FILE_RAW else None
ENABLE_FILE_FALLBACK = os.getenv("ENABLE_FILE_FALLBACK", "true").lower() == "true"

allowed_origins = [
    x.strip()
    for x in os.getenv(
        "ALLOWED_ORIGINS",
        "http://localhost:8080,http://127.0.0.1:8080,"
        "http://localhost:8081,http://127.0.0.1:8081,"
        "http://localhost:8091,http://127.0.0.1:8091",
    ).split(",")
    if x.strip()
]

producer: AIOKafkaProducer | None = None
producer_ready = False
write_lock = asyncio.Lock()
producer_lock = asyncio.Lock()
reconnect_task: asyncio.Task | None = None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def serialize_json(value: dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


async def write_event_to_file(event: dict[str, Any]) -> None:
    if STORAGE_FILE is None:
        return

    STORAGE_FILE.parent.mkdir(parents=True, exist_ok=True)

    async with write_lock:
        with STORAGE_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")


async def connect_producer() -> bool:
    global producer, producer_ready

    async with producer_lock:
        if producer_ready and producer is not None:
            return True

        try:
            new_producer = AIOKafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=serialize_json,
                compression_type="gzip",
            )
            await new_producer.start()

            producer = new_producer
            producer_ready = True

            logger.info("Kafka producer connected to %s", BOOTSTRAP_SERVERS)
            return True

        except Exception:
            logger.exception(
                "Failed to connect Kafka producer to %s",
                BOOTSTRAP_SERVERS,
            )
            producer = None
            producer_ready = False
            return False


async def reconnect_loop() -> None:
    while True:
        if not producer_ready:
            await connect_producer()

        await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, producer_ready, reconnect_task

    await connect_producer()
    reconnect_task = asyncio.create_task(reconnect_loop())

    try:
        yield

    finally:
        if reconnect_task is not None:
            reconnect_task.cancel()

            try:
                await reconnect_task
            except asyncio.CancelledError:
                pass

        if producer is not None:
            await producer.stop()

        producer = None
        producer_ready = False
        reconnect_task = None

        logger.info("Collector shutdown complete")


app = FastAPI(
    title="Kafka-ready Collector",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "kafka_bootstrap_servers": BOOTSTRAP_SERVERS,
        "kafka_topic": KAFKA_TOPIC,
        "producer_ready": producer_ready,
        "file_fallback_enabled": ENABLE_FILE_FALLBACK,
        "storage_file": str(STORAGE_FILE) if STORAGE_FILE else None,
    }


@app.post("/collect")
async def collect(request: Request) -> dict[str, Any]:
    global producer, producer_ready

    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload must be a JSON object")

    event_type = payload.get("event_type")

    if not event_type:
        raise HTTPException(status_code=400, detail="event_type is required")

    enriched = {
        **payload,
        "collector_received_at": now_iso(),
        "collector_client_ip": request.client.host if request.client else None,
        "collector_user_agent": request.headers.get("user-agent"),
    }

    event_key_value = (
        enriched.get("user_id")
        or enriched.get("anonymous_id")
        or enriched.get("session_id")
        or enriched.get("event_type")
    )
    event_key = str(event_key_value).encode("utf-8")

    kafka_written = False
    kafka_error = None

    if producer_ready and producer is not None:
        try:
            await producer.send_and_wait(
                KAFKA_TOPIC,
                value=enriched,
                key=event_key,
            )
            kafka_written = True

        except Exception as exc:
            kafka_error = str(exc)
            producer_ready = False
            producer = None

            logger.warning(
                "Kafka write failed, marking producer not ready: %s",
                exc,
            )

    if ENABLE_FILE_FALLBACK:
        await write_event_to_file(enriched)

    if not kafka_written and not ENABLE_FILE_FALLBACK:
        raise HTTPException(
            status_code=503,
            detail="Kafka unavailable and file fallback disabled",
        )

    return {
        "ok": True,
        "event_type": event_type,
        "kafka_topic": KAFKA_TOPIC,
        "kafka_written": kafka_written,
        "file_written": ENABLE_FILE_FALLBACK,
        "kafka_error": kafka_error,
    }


@app.get("/events")
async def events(limit: int = 20) -> dict[str, Any]:
    if STORAGE_FILE is None or not STORAGE_FILE.exists():
        return {
            "count": 0,
            "items": [],
            "bad_lines": 0,
        }

    items: list[dict[str, Any]] = []
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

    return {
        "count": min(limit, len(items)),
        "items": items[-limit:],
        "bad_lines": bad_lines,
    }