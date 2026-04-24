import asyncio
import logging
import time

import requests
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

from config import SCHEMA_REGISTRY_URL
from generator import run_generator
from kafka_producer import MovieEventProducer
from models import MovieEventRequest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def _wait_for_schema_registry(url: str, timeout: int = 120) -> None:
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(f"{url}/subjects", timeout=5)
            if r.status_code == 200:
                return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(3)
    raise TimeoutError(f"Schema Registry not available at {url}")


_producer: MovieEventProducer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _producer
    _wait_for_schema_registry(SCHEMA_REGISTRY_URL)
    _producer = MovieEventProducer()
    task = asyncio.create_task(run_generator(_producer.publish))
    yield
    task.cancel()
    _producer.flush()


app = FastAPI(title="Movie Event Producer", lifespan=lifespan)


@app.post("/events", status_code=201)
async def publish_event(event: MovieEventRequest):
    record = {
        "event_id": event.event_id,
        "user_id": event.user_id,
        "movie_id": event.movie_id,
        "event_type": event.event_type.value,
        "timestamp": event.timestamp,
        "device_type": event.device_type.value,
        "session_id": event.session_id,
        "progress_seconds": event.progress_seconds,
    }
    try:
        _producer.publish(record)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"event_id": event.event_id}


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
