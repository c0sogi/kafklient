import asyncio
import json
import logging
import os
from argparse import ArgumentParser
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional, cast
from uuid import uuid4

import anyio
from anyio.lowlevel import checkpoint
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from mcp.server.stdio import stdio_server
from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCMessage

from kafklient.clients.listener import KafkaListener
from kafklient.types.backend import Message as KafkaMessage
from kafklient.types.config import ConsumerConfig, ProducerConfig
from kafklient.types.parser import Parser

logger = logging.getLogger(__name__)
REPLY_TOPIC_HEADER_KEY = "x-reply-topic"
SESSION_ID_HEADER_KEY = "x-session-id"


def _extract_header_bytes(record: KafkaMessage, header_key: str) -> bytes | None:
    try:
        headers = record.headers() or []
    except Exception:
        headers = []
    for k, v in headers:
        if k.lower() != header_key.lower():
            continue
        if v is None:
            return None
        if isinstance(v, bytes):
            return v
        return str(v).encode("utf-8")
    return None


def _parse_value(raw: str) -> object:
    lowered = raw.strip().lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "none"}:
        return None

    # Try int/float
    try:
        if "." in raw:
            return float(raw)
        return int(raw)
    except ValueError:
        pass

    # Try JSON (dict/list/strings/numbers)
    if raw and raw[0] in {"{", "[", '"'}:
        try:
            return json.loads(raw)
        except Exception:
            pass

    return raw


def _parse_kv_items(items: list[str]) -> dict[str, object]:
    out: dict[str, object] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Invalid config item {item!r}. Expected KEY=VALUE.")
        k, v = item.split("=", 1)
        k = k.strip()
        if not k:
            raise ValueError(f"Invalid config item {item!r}. Key cannot be empty.")
        out[k] = _parse_value(v.strip())
    return out


@asynccontextmanager
async def kafka_client_transport(
    bootstrap_servers: str,
    consumer_topic: str,
    producer_topic: str,
    *,
    consumer_group_id: str | None = None,
    consumer_config: ConsumerConfig = {"auto.offset.reset": "latest"},
    producer_config: ProducerConfig = {},
    auto_create_topics: bool = True,
    assignment_timeout_s: float = 5.0,
    session_id: bytes | None = None,
) -> AsyncIterator[tuple[MemoryObjectReceiveStream[SessionMessage], MemoryObjectSendStream[SessionMessage]]]:
    """
    Client transport: behaves in the opposite direction of the server.
    - Writes to Request Topic
    - Reads from Response Topic
    """
    read_stream_writer, read_stream = anyio.create_memory_object_stream[SessionMessage](0)
    write_stream, write_stream_reader = anyio.create_memory_object_stream[SessionMessage](0)

    listener = KafkaListener(
        parsers=[Parser[KafkaMessage](topics=[consumer_topic])],
        consumer_config=consumer_config
        | {
            "bootstrap.servers": bootstrap_servers,
            "group.id": consumer_group_id or f"mcp-client-{uuid4().hex}",
        },
        producer_config=producer_config | {"bootstrap.servers": bootstrap_servers},
        auto_create_topics=auto_create_topics,
        assignment_timeout_s=assignment_timeout_s,
    )

    # Best-effort topic creation:
    # - The response topic (consumer_topic) should exist before subscribing for stability
    #   (consider brokers with auto-create disabled).
    # - The request topic (producer_topic) may need to exist before producing.
    if auto_create_topics:
        await listener.create_topics(consumer_topic, producer_topic)

    # IMPORTANT NOTE:
    # Ensure the response consumer is fully started/assigned *before* we allow any
    # stdio->Kafka writes to happen (otherwise a fast server response can be missed
    # because this library seeks to end on assignment).
    stream = await listener.subscribe(KafkaMessage)

    # 2. Kafka(Response) -> Client (Reader)
    async def kafka_reader():
        try:
            async with read_stream_writer:
                async for record in stream:
                    if session_id is not None:
                        sid = _extract_header_bytes(record, SESSION_ID_HEADER_KEY)
                        # In isolation mode, drop messages that do not belong to "this session".
                        if sid != session_id:
                            continue
                    msg = JSONRPCMessage.model_validate_json(record.value() or b"")
                    await read_stream_writer.send(SessionMessage(msg))
        except anyio.ClosedResourceError:
            await checkpoint()
        finally:
            await listener.stop()

    # 3. Client -> Kafka(Request) (Writer)
    async def kafka_writer():
        try:
            async with write_stream_reader:
                async for session_message in write_stream_reader:
                    json_str: str = session_message.message.model_dump_json(by_alias=True, exclude_none=True)
                    # Attach reply-topic so the server knows which response topic to use for this client/session.
                    headers: list[tuple[str, str | bytes | None]] = [
                        (REPLY_TOPIC_HEADER_KEY, consumer_topic.encode("utf-8"))
                    ]
                    if session_id is not None:
                        headers.append((SESSION_ID_HEADER_KEY, session_id))
                    await listener.produce(
                        producer_topic,
                        json_str.encode("utf-8"),
                        headers=headers,
                    )
        except anyio.ClosedResourceError:
            await checkpoint()
        finally:
            await listener.stop()

    async with anyio.create_task_group() as tg:
        tg.start_soon(kafka_reader)
        tg.start_soon(kafka_writer)
        yield read_stream, write_stream


async def run_client_async(
    bootstrap_servers: str = "localhost:9092",
    *,
    consumer_topic: str = "mcp-responses",
    producer_topic: str = "mcp-requests",
    consumer_group_id: Optional[str] = None,
    consumer_config: ConsumerConfig = {"auto.offset.reset": "latest"},
    producer_config: ProducerConfig = {},
    isolate_session: bool = True,
    auto_create_topics: bool = True,
    assignment_timeout_s: float = 5.0,
) -> None:
    # In session isolation mode, responses are filtered by per-bridge session_id.
    # This provides logical isolation even when using a shared response topic (e.g. mcp-responses).
    session_id: bytes | None = uuid4().hex.encode("utf-8") if isolate_session else None
    logger.debug(f"Session ID: {session_id.decode('utf-8') if session_id else '<none>'}")

    async with stdio_server() as (stdio_read, stdio_write):
        async with kafka_client_transport(
            bootstrap_servers=bootstrap_servers,
            consumer_topic=consumer_topic,
            producer_topic=producer_topic,
            consumer_group_id=consumer_group_id,
            consumer_config=consumer_config,
            producer_config=producer_config,
            auto_create_topics=auto_create_topics,
            assignment_timeout_s=assignment_timeout_s,
            session_id=session_id,
        ) as (kafka_read, kafka_write):
            # NOTE:
            # The stdio client expects the spawned process to exit when stdin is closed.
            # If we keep the bridge alive after stdin EOF,
            # it may continue writing to stdout (Kafka responses/notifications)
            # and trigger BrokenResourceError in the client's stdout reader during shutdown.
            # Therefore, we cancel the task group as soon as either direction completes.
            async with anyio.create_task_group() as tg:

                async def forward_stdio_to_kafka() -> None:
                    try:
                        async with stdio_read, kafka_write:
                            async for message in stdio_read:
                                if isinstance(message, Exception):
                                    logger.warning(f"Received exception from stdio: {message}", exc_info=message)
                                    continue
                                await kafka_write.send(message)
                    finally:
                        tg.cancel_scope.cancel()

                async def forward_kafka_to_stdio() -> None:
                    try:
                        async with kafka_read, stdio_write:
                            async for message in kafka_read:
                                await stdio_write.send(message)
                    finally:
                        tg.cancel_scope.cancel()

                tg.start_soon(forward_stdio_to_kafka)
                tg.start_soon(forward_kafka_to_stdio)


def main(argv: list[str] | None = None) -> None:
    """
    kafklient-mcp-client

    A bridge process between stdio-based MCP clients (e.g. LangChain) and Kafka-based MCP servers.
    """
    parser = ArgumentParser(prog="kafklient-mcp-client")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKLIENT_MCP_BOOTSTRAP", "localhost:9092"),
        help="Kafka bootstrap servers (default: %(default)s)",
    )
    parser.add_argument(
        "--consumer-topic",
        default=None,
        help=(
            "Kafka topic to read responses/notifications from. "
            "If omitted, uses $KAFKLIENT_MCP_CONSUMER_TOPIC or 'mcp-responses'. "
            "When session isolation is enabled, messages are filtered by the 'x-session-id' header."
        ),
    )
    parser.add_argument(
        "--producer-topic",
        default=os.getenv("KAFKLIENT_MCP_PRODUCER_TOPIC", "mcp-requests"),
        help="Kafka topic to write requests to (default: %(default)s)",
    )
    parser.add_argument(
        "--consumer-group-id",
        default=os.getenv("KAFKLIENT_MCP_CONSUMER_GROUP_ID"),
        help="Kafka consumer group id for the response consumer (default: auto-generated)",
    )
    default_isolate_session = os.getenv("KAFKLIENT_MCP_ISOLATE_SESSION", "true").strip().lower() not in {
        "0",
        "false",
        "no",
    }
    isolate_group = parser.add_mutually_exclusive_group()
    isolate_group.add_argument(
        "--isolate-session",
        dest="isolate_session",
        action="store_true",
        default=default_isolate_session,
        help=(
            "Enable session isolation (default: true). "
            "When enabled and --consumer-topic is not provided, an instance-unique response topic is used."
        ),
    )
    isolate_group.add_argument(
        "--no-isolate-session",
        dest="isolate_session",
        action="store_false",
        help="Disable session isolation (forces using the provided/default response topic as-is).",
    )
    parser.add_argument(
        "--consumer-config",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help=(
            "Extra consumer config entries (repeatable). "
            "Example: --consumer-config auto.offset.reset=latest "
            "--consumer-config enable.auto.commit=false"
        ),
    )
    parser.add_argument(
        "--producer-config",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Extra producer config entries (repeatable). Example: --producer-config linger.ms=5",
    )
    parser.add_argument(
        "--consumer-config-json",
        default=None,
        help="Extra consumer config as a JSON object string (merged after defaults).",
    )
    parser.add_argument(
        "--producer-config-json",
        default=None,
        help="Extra producer config as a JSON object string (merged after defaults).",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("KAFKLIENT_MCP_LOG_LEVEL", "INFO"),
        help="Logging level (default: %(default)s)",
    )

    ns = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, str(ns.log_level).upper(), logging.INFO))

    consumer_config: ConsumerConfig = {"auto.offset.reset": "latest"}
    producer_config: ProducerConfig = {}

    if ns.consumer_config_json:
        loaded = json.loads(ns.consumer_config_json)
        if not isinstance(loaded, dict):
            raise SystemExit("--consumer-config-json must be a JSON object")
        consumer_config = consumer_config | cast(ConsumerConfig, loaded)

    if ns.producer_config_json:
        loaded = json.loads(ns.producer_config_json)
        if not isinstance(loaded, dict):
            raise SystemExit("--producer-config-json must be a JSON object")
        producer_config = producer_config | cast(ProducerConfig, loaded)

    if ns.consumer_config:
        consumer_config = consumer_config | cast(ConsumerConfig, _parse_kv_items(list(ns.consumer_config)))

    if ns.producer_config:
        producer_config = producer_config | cast(ProducerConfig, _parse_kv_items(list(ns.producer_config)))

    asyncio.run(
        run_client_async(
            bootstrap_servers=str(ns.bootstrap_servers),
            consumer_topic=str(
                ns.consumer_topic
                if ns.consumer_topic is not None
                else os.getenv("KAFKLIENT_MCP_CONSUMER_TOPIC", "mcp-responses")
            ),
            producer_topic=str(ns.producer_topic),
            consumer_group_id=(str(ns.consumer_group_id) if ns.consumer_group_id else None),
            consumer_config=consumer_config,
            producer_config=producer_config,
            isolate_session=bool(ns.isolate_session),
        )
    )


if __name__ == "__main__":
    main()
