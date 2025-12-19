"""
Comprehensive tests for kafklient library.
Requires a running Kafka broker at localhost:9092.

Run with: python test.py
"""

import asyncio
import json
import uuid
from dataclasses import dataclass
from typing import Callable, Coroutine, Sequence

from kafklient import ConsumerConfig, KafkaRPCServer, Message, Producer, ProducerConfig

from ._config import KAFKA_BOOTSTRAP, TEST_TIMEOUT


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
    return {"bootstrap.servers": KAFKA_BOOTSTRAP}


async def produce_messages(
    topic: str,
    messages: Sequence[tuple[bytes | None, bytes]],  # (key, value)
    headers: list[tuple[str, str | bytes | None]] | None = None,
) -> None:
    """Produce messages to a topic for testing."""
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    def _produce() -> None:
        for key, value in messages:
            producer.produce(topic, value=value, key=key, headers=headers)
        producer.flush(timeout=10.0)

    await asyncio.to_thread(_produce)


# Request types for RPC servers
@dataclass
class EchoRequest:
    """Raw bytes echo request"""

    data: bytes


@dataclass
class JsonEchoRequest:
    """JSON echo request"""

    action: str
    value: int


def parse_echo_request(msg: Message) -> EchoRequest:
    """Parse raw bytes request"""
    return EchoRequest(data=msg.value() or b"")


def parse_json_echo_request(msg: Message) -> JsonEchoRequest:
    """Parse JSON request"""
    data = loads_json(msg.value())
    return JsonEchoRequest(
        action=as_str(data.get("action")),
        value=as_int(data.get("value")),
    )


def create_echo_rpc_server(
    server_group: str, request_topic: str, *, server_ready: asyncio.Event, server_stop: asyncio.Event
) -> Callable[[], Coroutine[None, None, None]]:
    """Create an echo RPC server using KafkaRPCServer."""

    async def echo_server() -> None:
        server = KafkaRPCServer(
            consumer_config=make_consumer_config(server_group),
            producer_config=make_producer_config(),
            parsers=[{"topics": [request_topic], "type": EchoRequest, "parser": parse_echo_request}],
            auto_create_topics=True,
        )

        @server.handler(EchoRequest)
        async def echo(request: EchoRequest, message: Message) -> bytes:  # pyright: ignore[reportUnusedFunction]
            return request.data

        await server.start()
        server_ready.set()
        await asyncio.wait_for(server_stop.wait(), timeout=TEST_TIMEOUT)
        await server.stop()

    return echo_server


def create_json_echo_server(
    server_group: str, request_topic: str, *, server_ready: asyncio.Event, server_stop: asyncio.Event
) -> Callable[[], Coroutine[None, None, None]]:
    """Create a JSON echo RPC server using KafkaRPCServer."""

    async def json_server() -> None:
        server = KafkaRPCServer(
            consumer_config=make_consumer_config(server_group),
            producer_config=make_producer_config(),
            parsers=[{"topics": [request_topic], "type": JsonEchoRequest, "parser": parse_json_echo_request}],
            auto_create_topics=True,
        )

        @server.handler(JsonEchoRequest)
        async def handle_json_echo(request: JsonEchoRequest, message: Message) -> bytes:  # pyright: ignore[reportUnusedFunction]
            return json.dumps({
                "status": "ok",
                "echo": {
                    "action": request.action,
                    "value": request.value,
                },
                "server": "json-server",
            }).encode()

        await server.start()
        server_ready.set()
        await asyncio.wait_for(server_stop.wait(), timeout=TEST_TIMEOUT)
        await server.stop()

    return json_server
