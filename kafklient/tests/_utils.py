"""
Comprehensive tests for kafklient library.
Requires a running Kafka broker at localhost:9092.

Run with: python test.py
"""

import asyncio
import json
import uuid
from typing import Callable, Sequence

from kafklient import (
    AdminClient,
    Consumer,
    ConsumerConfig,
    NewTopic,
    Producer,
    ProducerConfig,
    create_consumer,
)

from ._config import KAFKA_BOOTSTRAP


def loads_json(value: bytes | None) -> dict[str, object]:
    if not value:
        return {}
    return json.loads(value.decode("utf-8"))


def as_str(value: object, default: str = "") -> str:
    if isinstance(value, str):
        return value
    if value is None:
        return default
    return str(value)


def as_int(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def as_bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1"}:
            return True
        if lowered in {"false", "0"}:
            return False
    return default


def get_topic_and_group_id(func: Callable[..., object], *, suffix: str = "") -> tuple[str, str]:
    if suffix:
        suffix = f"-{suffix}"
    return f"{func.__name__}{suffix}", f"{func.__name__}{suffix}-{uuid.uuid4().hex[:8]}"


def make_consumer_config(group_id: str) -> ConsumerConfig:
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    }


def make_producer_config() -> ProducerConfig:
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
    }


async def ensure_topic_exists(topic: str) -> None:
    """Ensure a topic exists before using it."""
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})

    def _create() -> None:
        futures = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
        for _, future in futures.items():
            try:
                future.result(timeout=10.0)
            except Exception:
                # Topic might already exist, that's fine
                pass

    await asyncio.to_thread(_create)


async def produce_messages(
    topic: str,
    messages: Sequence[tuple[bytes | None, bytes]],  # (key, value)
    headers: list[tuple[str, str | bytes]] | None = None,
) -> None:
    """Produce messages to a topic for testing."""
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    def _produce() -> None:
        for key, value in messages:
            producer.produce(topic, value=value, key=key, headers=headers)
        producer.flush(timeout=10.0)

    await asyncio.to_thread(_produce)


def make_ready_consumer(group_id: str, topics: list[str]) -> Consumer:
    """Create a consumer that's ready to receive messages (already subscribed and stabilized)."""
    consumer = create_consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "session.timeout.ms": 6000,
        "heartbeat.interval.ms": 1000,
    })
    consumer.subscribe(topics)
    # Poll until assigned and stabilized (Windows needs ~3s)
    for _ in range(50):
        consumer.poll(0.1)
        if consumer.assignment():
            break
    return consumer
