import asyncio
import logging
import random
import uuid
from datetime import datetime, timezone

from config import GENERATOR_INTERVAL_SEC, GENERATOR_USERS

logger = logging.getLogger(__name__)

MOVIE_IDS = [f"movie_{i:03d}" for i in range(1, 21)]
DEVICE_TYPES = ["MOBILE", "DESKTOP", "TV", "TABLET"]
SEARCH_QUERIES = ["action", "comedy", "drama", "sci-fi", "thriller"]


def _now_micros() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1_000_000)


def _make_event(
    event_type: str,
    user_id: str,
    movie_id: str,
    session_id: str,
    device_type: str,
    progress_seconds: int | None = None,
) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "movie_id": movie_id,
        "event_type": event_type,
        "timestamp": _now_micros(),
        "device_type": device_type,
        "session_id": session_id,
        "progress_seconds": progress_seconds,
    }


async def _simulate_user_session(user_id: str, publish_fn) -> None:
    movie_id = random.choice(MOVIE_IDS)
    session_id = str(uuid.uuid4())
    device_type = random.choice(DEVICE_TYPES)
    progress = 0

    publish_fn(_make_event("VIEW_STARTED", user_id, movie_id, session_id, device_type, progress))
    await asyncio.sleep(GENERATOR_INTERVAL_SEC)

    num_pauses = random.randint(0, 3)
    for _ in range(num_pauses):
        progress += random.randint(30, 120)
        publish_fn(_make_event("VIEW_PAUSED", user_id, movie_id, session_id, device_type, progress))
        await asyncio.sleep(GENERATOR_INTERVAL_SEC)

        progress += random.randint(1, 10)
        publish_fn(_make_event("VIEW_RESUMED", user_id, movie_id, session_id, device_type, progress))
        await asyncio.sleep(GENERATOR_INTERVAL_SEC)

    progress += random.randint(60, 300)
    publish_fn(_make_event("VIEW_FINISHED", user_id, movie_id, session_id, device_type, progress))
    await asyncio.sleep(GENERATOR_INTERVAL_SEC)

    if random.random() < 0.4:
        publish_fn(_make_event("LIKED", user_id, movie_id, session_id, device_type, None))
        await asyncio.sleep(GENERATOR_INTERVAL_SEC)

    if random.random() < 0.3:
        publish_fn(_make_event("SEARCHED", user_id, movie_id, session_id, device_type, None))
        await asyncio.sleep(GENERATOR_INTERVAL_SEC)


async def run_generator(publish_fn) -> None:
    user_ids = [f"user_{i:04d}" for i in range(1, GENERATOR_USERS + 1)]
    logger.info("Generator started with %d virtual users", GENERATOR_USERS)
    while True:
        user_id = random.choice(user_ids)
        asyncio.create_task(_simulate_user_session(user_id, publish_fn))
        await asyncio.sleep(GENERATOR_INTERVAL_SEC)
