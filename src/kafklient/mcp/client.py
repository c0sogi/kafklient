import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional
from uuid import uuid4

import anyio
from anyio.lowlevel import checkpoint
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from mcp.server.stdio import stdio_server
from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCMessage

from kafklient.clients.listener import KafkaListener
from kafklient.mcp import _config
from kafklient.mcp._utils import extract_header_bytes
from kafklient.types.backend import Message as KafkaMessage
from kafklient.types.config import ConsumerConfig, ProducerConfig
from kafklient.types.parser import Parser

logger = logging.getLogger(__name__)


@asynccontextmanager
async def kafka_client_transport(
    bootstrap_servers: str,
    consumer_topic: str,
    producer_topic: str,
    *,
    consumer_group_id: str | None = None,
    consumer_config: ConsumerConfig = _config.DEFAULT_MCP_CONSUMER_CONFIG,
    producer_config: ProducerConfig = _config.DEFAULT_MCP_PRODUCER_CONFIG,
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
                        sid = extract_header_bytes(record, _config.MCP_SESSION_ID_HEADER_KEY)
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
                        (_config.MCP_REPLY_TOPIC_HEADER_KEY, consumer_topic.encode("utf-8"))
                    ]
                    if session_id is not None:
                        headers.append((_config.MCP_SESSION_ID_HEADER_KEY, session_id))
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
    bootstrap_servers: str = "127.0.0.1:9092",
    *,
    consumer_topic: str = "mcp-responses",
    producer_topic: str = "mcp-requests",
    consumer_group_id: Optional[str] = None,
    consumer_config: ConsumerConfig = _config.DEFAULT_MCP_CONSUMER_CONFIG,
    producer_config: ProducerConfig = _config.DEFAULT_MCP_PRODUCER_CONFIG,
    isolate_session: bool = True,
    auto_create_topics: bool = True,
    assignment_timeout_s: float = 5.0,
) -> None:
    # In session isolation mode, responses are filtered by per-bridge session_id.
    # This provides logical isolation even when using a shared response topic (e.g. mcp-responses).
    session_id: bytes | None = uuid4().hex.encode("utf-8") if isolate_session else None
    logger.debug(f"Session ID: {session_id.decode('utf-8', errors='replace') if session_id else '<none>'}")

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
            shutdown_requested = anyio.Event()

            # NOTE:
            # The stdio client expects the spawned process to exit when stdin is closed.
            # If we keep the bridge alive after stdin EOF,
            # it may continue writing to stdout (Kafka responses/notifications)
            # and trigger BrokenResourceError in the client's stdout reader during shutdown.
            # Therefore, we cancel the task group as soon as either direction completes.
            async with anyio.create_task_group() as tg:

                def _extract_method(msg: SessionMessage) -> str | None:
                    # We avoid depending on the exact JSONRPCMessage variant classes
                    # (request/notification/response) and just inspect the dumped payload.
                    method = getattr(msg.message.root, "method", None)
                    return method if isinstance(method, str) else None

                async def forward_stdio_to_kafka() -> None:
                    try:
                        async with stdio_read, kafka_write:
                            async for message in stdio_read:
                                if isinstance(message, Exception):
                                    logger.warning(f"Received exception from stdio: {message}", exc_info=message)
                                    continue
                                method = _extract_method(message)
                                if method in {"shutdown", "exit"}:
                                    # Inspector's Disconnect typically triggers shutdown/exit.
                                    # If we continue emitting stdout after the browser side is gone,
                                    # the inspector proxy can crash ("Not connected").
                                    shutdown_requested.set()
                                    await kafka_write.send(message)
                                    return
                                await kafka_write.send(message)
                    finally:
                        tg.cancel_scope.cancel()

                async def forward_kafka_to_stdio() -> None:
                    try:
                        async with kafka_read, stdio_write:
                            async for message in kafka_read:
                                if shutdown_requested.is_set():
                                    return
                                try:
                                    await stdio_write.send(message)
                                except (anyio.ClosedResourceError, anyio.BrokenResourceError, BrokenPipeError):
                                    return
                    finally:
                        tg.cancel_scope.cancel()

                tg.start_soon(forward_stdio_to_kafka)
                tg.start_soon(forward_kafka_to_stdio)
