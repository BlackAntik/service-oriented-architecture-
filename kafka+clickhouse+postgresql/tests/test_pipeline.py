import time
import uuid
from datetime import datetime, timezone

import pytest
import requests

PRODUCER_URL = "http://producer:8000"
CLICKHOUSE_URL = "http://clickhouse:8123"


def _now_micros() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1_000_000)


def _wait_for_service(url: str, path: str = "/", timeout: int = 60) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{url}{path}", timeout=5)
            if r.status_code == 200:
                return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(2)
    raise TimeoutError(f"Service {url} not ready after {timeout}s")


def _clickhouse_query(sql: str) -> str:
    r = requests.post(f"{CLICKHOUSE_URL}/", params={"query": sql}, timeout=10)
    r.raise_for_status()
    return r.text.strip()


def _poll_clickhouse(event_id: str, timeout: int = 60) -> dict | None:
    deadline = time.time() + timeout
    sql = (
        f"SELECT event_id, user_id, movie_id, event_type, device_type, "
        f"session_id, progress_seconds "
        f"FROM default.movie_events "
        f"WHERE event_id = '{event_id}' "
        f"FORMAT JSONEachRow"
    )
    while time.time() < deadline:
        result = _clickhouse_query(sql)
        if result:
            import json
            return json.loads(result)
        time.sleep(2)
    return None


@pytest.fixture(scope="session", autouse=True)
def wait_for_services():
    _wait_for_service(PRODUCER_URL, "/healthz")
    _wait_for_service(CLICKHOUSE_URL, "/ping")


@pytest.mark.parametrize("event_type,progress_seconds", [
    ("VIEW_STARTED", 0),
    ("VIEW_PAUSED", 120),
    ("VIEW_RESUMED", 125),
    ("VIEW_FINISHED", 3600),
    ("LIKED", None),
    ("SEARCHED", None),
])
def test_event_reaches_clickhouse(event_type, progress_seconds):
    event_id = str(uuid.uuid4())
    user_id = f"test_user_{uuid.uuid4().hex[:8]}"
    movie_id = "test_movie_001"
    session_id = str(uuid.uuid4())

    payload = {
        "event_id": event_id,
        "user_id": user_id,
        "movie_id": movie_id,
        "event_type": event_type,
        "timestamp": _now_micros(),
        "device_type": "DESKTOP",
        "session_id": session_id,
        "progress_seconds": progress_seconds,
    }

    response = requests.post(f"{PRODUCER_URL}/events", json=payload, timeout=10)
    assert response.status_code == 201
    assert response.json()["event_id"] == event_id

    row = _poll_clickhouse(event_id, timeout=60)
    assert row is not None, f"Event {event_id} not found in ClickHouse after 60s"

    assert row["event_id"] == event_id
    assert row["user_id"] == user_id
    assert row["movie_id"] == movie_id
    assert row["event_type"] == event_type
    assert row["device_type"] == "DESKTOP"
    assert row["session_id"] == session_id
    if progress_seconds is not None:
        assert row["progress_seconds"] == progress_seconds
    else:
        assert row["progress_seconds"] is None


def test_invalid_event_type_rejected():
    payload = {
        "user_id": "test_user",
        "movie_id": "movie_001",
        "event_type": "INVALID_TYPE",
        "timestamp": _now_micros(),
        "device_type": "MOBILE",
        "session_id": str(uuid.uuid4()),
    }
    response = requests.post(f"{PRODUCER_URL}/events", json=payload, timeout=10)
    assert response.status_code == 422


def test_invalid_device_type_rejected():
    payload = {
        "user_id": "test_user",
        "movie_id": "movie_001",
        "event_type": "VIEW_STARTED",
        "timestamp": _now_micros(),
        "device_type": "SMARTWATCH",
        "session_id": str(uuid.uuid4()),
    }
    response = requests.post(f"{PRODUCER_URL}/events", json=payload, timeout=10)
    assert response.status_code == 422


def test_session_event_order_preserved():
    session_id = str(uuid.uuid4())
    user_id = f"test_user_{uuid.uuid4().hex[:8]}"
    movie_id = "test_movie_order"

    events = [
        ("VIEW_STARTED", 0),
        ("VIEW_PAUSED", 100),
        ("VIEW_RESUMED", 105),
        ("VIEW_FINISHED", 3600),
    ]

    published_ids = []
    for event_type, progress in events:
        event_id = str(uuid.uuid4())
        payload = {
            "event_id": event_id,
            "user_id": user_id,
            "movie_id": movie_id,
            "event_type": event_type,
            "timestamp": _now_micros(),
            "device_type": "TV",
            "session_id": session_id,
            "progress_seconds": progress,
        }
        r = requests.post(f"{PRODUCER_URL}/events", json=payload, timeout=10)
        assert r.status_code == 201
        published_ids.append(event_id)
        time.sleep(0.05)

    for event_id in published_ids:
        row = _poll_clickhouse(event_id, timeout=60)
        assert row is not None, f"Event {event_id} not found in ClickHouse"
        assert row["session_id"] == session_id
        assert row["user_id"] == user_id

    sql = (
        f"SELECT event_type, progress_seconds "
        f"FROM default.movie_events "
        f"WHERE session_id = '{session_id}' "
        f"ORDER BY event_time "
        f"FORMAT JSONEachRow"
    )
    result = _clickhouse_query(sql)
    import json
    rows = [json.loads(line) for line in result.splitlines() if line.strip()]
    assert len(rows) == 4
    assert rows[0]["event_type"] == "VIEW_STARTED"
    assert rows[-1]["event_type"] == "VIEW_FINISHED"
    progress_values = [r["progress_seconds"] for r in rows]
    assert progress_values == sorted(progress_values)
