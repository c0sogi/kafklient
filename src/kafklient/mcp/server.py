import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator
from uuid import uuid4

import anyio
from anyio import EndOfStream
from anyio.lowlevel import checkpoint
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from fastmcp import FastMCP
from fastmcp.server.tasks.capabilities import get_task_capabilities
from fastmcp.utilities.cli import log_server_banner
from fastmcp.utilities.logging import temporary_log_level
from mcp.server.lowlevel.server import NotificationOptions
from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCMessage

from kafklient.clients.listener import KafkaListener
from kafklient.mcp._utils import extract_header_bytes, extract_session_id
from kafklient.types.backend import Message as KafkaMessage
from kafklient.types.config import ConsumerConfig, ProducerConfig
from kafklient.types.parser import Parser

logger = logging.getLogger(__name__)
REPLY_TOPIC_HEADER_KEY = "x-reply-topic"
SESSION_ID_HEADER_KEY = "x-session-id"


@dataclass
class _McpKafkaSession:
    session_key: str
    target_topic: str
    session_id: bytes | None
    read_stream_writer: MemoryObjectSendStream[SessionMessage | Exception]
    read_stream: MemoryObjectReceiveStream[SessionMessage | Exception]
    write_stream: MemoryObjectSendStream[SessionMessage]
    write_stream_reader: MemoryObjectReceiveStream[SessionMessage]


@asynccontextmanager
async def kafka_server_transport(
    bootstrap_servers: str,
    consumer_topic: str,
    producer_topic: str,
    *,
    consumer_group_id: str | None = None,
    ready_event: asyncio.Event | None = None,
    auto_create_topics: bool = True,
    assignment_timeout_s: float = 5.0,
    consumer_config: ConsumerConfig = {"auto.offset.reset": "latest"},
    producer_config: ProducerConfig = {},
) -> AsyncIterator[tuple[MemoryObjectReceiveStream[SessionMessage], MemoryObjectSendStream[SessionMessage]]]:
    read_stream_writer, read_stream = anyio.create_memory_object_stream[SessionMessage](0)
    write_stream, write_stream_reader = anyio.create_memory_object_stream[SessionMessage](0)

    listener = KafkaListener(
        parsers=[Parser[JSONRPCMessage](topics=[consumer_topic])],
        consumer_config=consumer_config
        | {
            "bootstrap.servers": bootstrap_servers,
            "group.id": consumer_group_id or f"mcp-server-{uuid4().hex}",
        },
        producer_config=producer_config | {"bootstrap.servers": bootstrap_servers},
        auto_create_topics=auto_create_topics,
        assignment_timeout_s=assignment_timeout_s,
    )

    # Ensure topics exist up-front (consumer subscription + producer output)
    if auto_create_topics:
        await listener.create_topics(consumer_topic, producer_topic)

    async def kafka_reader():
        try:
            async with read_stream_writer:
                stream = await listener.subscribe(JSONRPCMessage)
                if ready_event is not None:
                    ready_event.set()
                async for msg in stream:
                    await read_stream_writer.send(SessionMessage(msg))
        except anyio.ClosedResourceError:
            await checkpoint()
        finally:
            await listener.stop()

    async def kafka_writer():
        try:
            async with write_stream_reader:
                async for session_message in write_stream_reader:
                    json_str = session_message.message.model_dump_json(by_alias=True, exclude_none=True)
                    await listener.produce(producer_topic, json_str.encode("utf-8"))
        except anyio.ClosedResourceError:
            await checkpoint()
        finally:
            await listener.stop()

    async with anyio.create_task_group() as tg:
        tg.start_soon(kafka_reader)
        tg.start_soon(kafka_writer)
        yield read_stream, write_stream


async def run_server_async(
    mcp: FastMCP,
    *,
    bootstrap_servers: str = "localhost:9092",
    consumer_topic: str = "mcp-requests",
    producer_topic: str = "mcp-responses",
    consumer_group_id: str | None = None,
    ready_event: asyncio.Event | None = None,
    auto_create_topics: bool = True,
    assignment_timeout_s: float = 5.0,
    consumer_config: ConsumerConfig = {"auto.offset.reset": "latest"},
    producer_config: ProducerConfig = {},
    show_banner: bool = True,
    log_level: str | None = None,
    multi_session: bool = True,
) -> None:
    """Run the server using stdio transport.

    Args:
        show_banner: Whether to display the server banner
        log_level: Log level for the server
    """
    # Display server banner
    if show_banner:
        log_server_banner(
            server=mcp,
            transport="stdio",
        )

    with temporary_log_level(log_level):
        mcp_server = mcp._mcp_server  # pyright: ignore[reportPrivateUsage]
        async with mcp._lifespan_manager():  # pyright: ignore[reportPrivateUsage]
            # ---------------------------
            # Single-session (legacy) mode
            # ---------------------------
            if not multi_session:
                async with kafka_server_transport(
                    bootstrap_servers=bootstrap_servers,
                    consumer_topic=consumer_topic,
                    producer_topic=producer_topic,
                    consumer_group_id=consumer_group_id,
                    ready_event=ready_event,
                    auto_create_topics=auto_create_topics,
                    assignment_timeout_s=assignment_timeout_s,
                    consumer_config=consumer_config,
                    producer_config=producer_config,
                ) as (read_stream, write_stream):
                    logger.info(f"Starting MCP server {mcp.name!r} with transport 'stdio' over Kafka")

                    experimental_capabilities = get_task_capabilities()
                    await mcp_server.run(
                        read_stream,
                        write_stream,
                        mcp_server.create_initialization_options(
                            notification_options=NotificationOptions(tools_changed=True),
                            experimental_capabilities=experimental_capabilities,
                        ),
                    )
                return

            # ---------------------------
            # Multi-session (session isolation) mode
            # ---------------------------
            # Core idea:
            # - The client attaches its "reply topic" via the x-reply-topic header on requests.
            # - The server creates and maintains an independent MCP ServerSession per reply-topic (session key).
            # - Each session's write_stream produces only to that reply-topic to avoid mixing responses/notifications.

            experimental_capabilities = get_task_capabilities()
            init_opts = mcp_server.create_initialization_options(
                notification_options=NotificationOptions(tools_changed=True),
                experimental_capabilities=experimental_capabilities,
            )

            listener = KafkaListener(
                parsers=[Parser[KafkaMessage](topics=[consumer_topic])],
                consumer_config=consumer_config
                | {
                    "bootstrap.servers": bootstrap_servers,
                    "group.id": consumer_group_id or f"mcp-server-{uuid4().hex}",
                },
                producer_config=producer_config | {"bootstrap.servers": bootstrap_servers},
                auto_create_topics=auto_create_topics,
                assignment_timeout_s=assignment_timeout_s,
            )

            # Ensure base topics exist up-front
            if auto_create_topics:
                await listener.create_topics(consumer_topic, producer_topic)

            # Ensure subscription is ready before we accept requests
            stream = await listener.subscribe(KafkaMessage)
            if ready_event is not None:
                ready_event.set()

            sessions: dict[str, _McpKafkaSession] = {}
            created_topics: set[str] = set()
            cancelled_exc = anyio.get_cancelled_exc_class()

            async def close_session(*, session_key: str, session: _McpKafkaSession) -> None:
                """Best-effort cleanup for a session so future sends don't block forever."""
                sessions.pop(session_key, None)
                for s in (
                    session.read_stream_writer,
                    session.read_stream,
                    session.write_stream,
                    session.write_stream_reader,
                ):
                    try:
                        await s.aclose()
                    except Exception:
                        pass

            async def ensure_session(
                *, session_key: str, target_topic: str, session_id: bytes | None, tg: Any
            ) -> _McpKafkaSession:
                existing = sessions.get(session_key)
                if existing is not None:
                    return existing

                if auto_create_topics and target_topic not in created_topics:
                    await listener.create_topics(target_topic)
                    created_topics.add(target_topic)

                read_stream_writer, read_stream = anyio.create_memory_object_stream[SessionMessage | Exception](0)
                write_stream, write_stream_reader = anyio.create_memory_object_stream[SessionMessage](0)

                session = _McpKafkaSession(
                    session_key=session_key,
                    target_topic=target_topic,
                    session_id=session_id,
                    read_stream_writer=read_stream_writer,
                    read_stream=read_stream,
                    write_stream=write_stream,
                    write_stream_reader=write_stream_reader,
                )
                sessions[session_key] = session
                logger.info(
                    "Created MCP session (session_key=%r, target_topic=%r, session_id=%r)",
                    session_key,
                    target_topic,
                    session_id.decode("utf-8", errors="replace") if session_id else None,
                )

                async def run_mcp_session() -> None:
                    try:
                        await mcp_server.run(read_stream, write_stream, init_opts)
                    except cancelled_exc:
                        # Treat per-session cancellation as a normal shutdown path.
                        await checkpoint()
                    except BaseException:
                        logger.exception("MCP session crashed (session_key=%r)", session_key)
                    finally:
                        # If the MCP session exits (client disconnected, shutdown, crash),
                        # ensure the streams are closed. Otherwise, a later send() to a
                        # zero-buffer memory stream can block forever and stall the server.
                        await close_session(session_key=session_key, session=session)
                        logger.info("Closed MCP session (session_key=%r)", session_key)

                async def pump_session_to_kafka() -> None:
                    try:
                        async with write_stream_reader:
                            async for session_message in write_stream_reader:
                                json_str = session_message.message.model_dump_json(by_alias=True, exclude_none=True)
                                headers: list[tuple[str, str | bytes]] | None = (
                                    [(SESSION_ID_HEADER_KEY, session.session_id)]
                                    if session.session_id is not None
                                    else None
                                )
                                await listener.produce(session.target_topic, json_str.encode("utf-8"), headers=headers)
                    except anyio.ClosedResourceError:
                        await checkpoint()
                    except EndOfStream:
                        await checkpoint()
                    except cancelled_exc:
                        await checkpoint()
                    except BaseException:
                        logger.exception("pump_session_to_kafka crashed (session_key=%r)", session_key)

                tg.start_soon(run_mcp_session)
                tg.start_soon(pump_session_to_kafka)
                return session

            logger.info(f"Starting MCP server {mcp.name!r} with transport 'stdio' over Kafka (multi_session=True)")

            try:
                async with anyio.create_task_group() as tg:
                    async for record in stream:
                        try:
                            msg = JSONRPCMessage.model_validate_json(record.value() or b"")
                            if reply_topic_bytes := extract_header_bytes(record, REPLY_TOPIC_HEADER_KEY):
                                reply_topic: str = reply_topic_bytes.decode("utf-8", errors="replace")
                            else:
                                reply_topic = producer_topic

                            session_id: bytes | None = extract_session_id(record)
                            # NOTE:
                            # If session_id (e.g. a bridge instance UUID) and reply_topic (string) share the same
                            # namespace, collisions are possible. For example, if isolate_session=False and a client
                            # uses a reply_topic string that happens to match another client's session_id string,
                            # the sessions could be merged. We prevent this by separating the key namespaces.
                            if session_id is not None:
                                session_key: str = f"sid:{session_id.decode('utf-8', errors='replace')}"
                            else:
                                session_key = f"topic:{reply_topic}"

                            # If reply_topic differs from producer_topic, it means "dedicated reply topic" (client opted in).
                            # If it's the same, we use the shared reply topic but rely on session-id headers to avoid mixing.
                            target_topic: str = reply_topic if reply_topic != producer_topic else producer_topic
                            session = await ensure_session(
                                session_key=session_key,
                                target_topic=target_topic,
                                session_id=session_id,
                                tg=tg,
                            )
                            try:
                                await session.read_stream_writer.send(SessionMessage(msg))
                            except anyio.ClosedResourceError:
                                # Sending into a closed session (e.g. bridge already exited) raises ClosedResourceError.
                                # Clean up the session so this does not take down the whole server, and retry once.
                                logger.info(
                                    f"Session stream closed (session_key={session_key!r}); dropping session and retrying"
                                )
                                await close_session(session_key=session_key, session=session)

                                # Retry once (in case messages keep coming for the same key)
                                try:
                                    session = await ensure_session(
                                        session_key=session_key,
                                        target_topic=target_topic,
                                        session_id=session_id,
                                        tg=tg,
                                    )
                                    await session.read_stream_writer.send(SessionMessage(msg))
                                except anyio.ClosedResourceError:
                                    # If it's still closed right after recreation, drop this message.
                                    logger.warning(
                                        f"Session stream closed again (session_key={session_key!r}); dropping message"
                                    )
                        except Exception:
                            logger.exception("Error processing message")
            finally:
                try:
                    await listener.stop()
                except Exception:
                    pass


def run_server(
    mcp: FastMCP,
    *,
    bootstrap_servers: str = "localhost:9092",
    consumer_topic: str = "mcp-requests",
    producer_topic: str = "mcp-responses",
    consumer_group_id: str | None = None,
    ready_event: asyncio.Event | None = None,
    auto_create_topics: bool = True,
    assignment_timeout_s: float = 5.0,
    consumer_config: ConsumerConfig = {"auto.offset.reset": "latest"},
    producer_config: ProducerConfig = {},
    show_banner: bool = True,
    log_level: str | None = None,
    multi_session: bool = True,
) -> None:
    return asyncio.run(
        run_server_async(
            mcp=mcp,
            bootstrap_servers=bootstrap_servers,
            consumer_topic=consumer_topic,
            producer_topic=producer_topic,
            consumer_group_id=consumer_group_id,
            ready_event=ready_event,
            auto_create_topics=auto_create_topics,
            assignment_timeout_s=assignment_timeout_s,
            consumer_config=consumer_config,
            producer_config=producer_config,
            show_banner=show_banner,
            log_level=log_level,
            multi_session=multi_session,
        )
    )
