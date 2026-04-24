import io
import json
import logging
import time
from typing import Any

import fastavro
import requests
from confluent_kafka import Producer, KafkaException

from config import KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, TOPIC_NAME, SCHEMA_FILE

logger = logging.getLogger(__name__)


def _load_schema(schema_file: str) -> dict:
    with open(schema_file, "r") as f:
        return json.load(f)


def _fetch_schema_id(subject: str) -> int:
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()["id"]


def _serialize_avro(record: dict, parsed_schema: Any, schema_id: int) -> bytes:
    buf = io.BytesIO()
    buf.write(b"\x00")
    buf.write(schema_id.to_bytes(4, byteorder="big"))
    fastavro.schemaless_writer(buf, parsed_schema, record)
    return buf.getvalue()


class MovieEventProducer:
    def __init__(self) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "acks": "all",
                "retries": 5,
                "retry.backoff.ms": 500,
                "enable.idempotence": True,
                "linger.ms": 10,
                "compression.type": "snappy",
            }
        )
        raw_schema = _load_schema(SCHEMA_FILE)
        self._parsed_schema = fastavro.parse_schema(raw_schema)
        self._schema_id = _fetch_schema_id(f"{TOPIC_NAME}-value")

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        if err:
            logger.error("Delivery failed: %s", err)
        else:
            logger.info(
                "Published event_id=%s partition=%d offset=%d",
                msg.key().decode() if msg.key() else None,
                msg.partition(),
                msg.offset(),
            )

    def publish(self, event: dict, max_attempts: int = 5) -> None:
        payload = _serialize_avro(event, self._parsed_schema, self._schema_id)
        key = event["user_id"].encode()

        for attempt in range(1, max_attempts + 1):
            try:
                self._producer.produce(
                    topic=TOPIC_NAME,
                    key=key,
                    value=payload,
                    on_delivery=self._delivery_callback,
                )
                self._producer.poll(0)
                logger.info(
                    "Sent event_id=%s event_type=%s timestamp=%s",
                    event["event_id"],
                    event["event_type"],
                    event["timestamp"],
                )
                return
            except KafkaException as exc:
                wait = 0.5 * (2 ** (attempt - 1))
                logger.warning("Attempt %d failed: %s, retrying in %.1fs", attempt, exc, wait)
                time.sleep(wait)

        raise RuntimeError(f"Failed to publish event {event['event_id']} after {max_attempts} attempts")

    def flush(self) -> None:
        self._producer.flush()
