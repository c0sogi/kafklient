import logging
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import AsyncIterator

import anyio
import mcp.types as types
from anyio.lowlevel import checkpoint
from mcp.client.session import (
    ClientSession,
    ElicitationFnT,
    ListRootsFnT,
    LoggingFnT,
    MessageHandlerFnT,
    SamplingFnT,
)
from mcp.shared.message import SessionMessage

from kafklient.mcp.client import kafka_client_transport
from kafklient.mcp.server import Server

logger = logging.getLogger(__name__)


@asynccontextmanager
async def kafka_client_session(
    bootstrap_servers: str,
    *,
    consumer_topic: str = "mcp-responses",
    producer_topic: str = "mcp-requests",
    consumer_group_id: str | None = None,
    auto_create_topics: bool = True,
    assignment_timeout_s: float = 5.0,
    read_timeout_seconds: timedelta | None = None,
    initialize: bool = True,
    sampling_callback: SamplingFnT | None = None,
    elicitation_callback: ElicitationFnT | None = None,
    list_roots_callback: ListRootsFnT | None = None,
    logging_callback: LoggingFnT | None = None,
    message_handler: MessageHandlerFnT | None = None,
    client_info: types.Implementation | None = None,
    sampling_capabilities: types.SamplingCapability | None = None,
) -> AsyncIterator[ClientSession]:
    """
    Create an MCP `ClientSession` that communicates over Kafka topics (no subprocess/stdio bridge needed).

    This is the programmatic alternative to:
    - spawning `kafklient mcp-client` via `StdioServerParameters`
    - connecting to that subprocess with `mcp.client.stdio.stdio_client`

    Args:
        bootstrap_servers: Kafka bootstrap servers.
        consumer_topic: Response topic to consume from.
        producer_topic: Request topic to produce to.
        consumer_group_id: Consumer group id for the response consumer.
        auto_create_topics: Best-effort topic creation.
        assignment_timeout_s: Kafka consumer assignment timeout.
        read_timeout_seconds: Optional MCP client read timeout.
        initialize: If True, call `session.initialize()` before yielding.
    """

    # Always isolate sessions: filter responses by a per-session id to avoid mixing on shared topics.
    # Keep this aligned with `run_client_async` behavior: a stable session id for filtering.
    import uuid

    session_id: bytes = uuid.uuid4().hex.encode("utf-8")

    async with kafka_client_transport(
        bootstrap_servers=bootstrap_servers,
        consumer_topic=consumer_topic,
        producer_topic=producer_topic,
        consumer_group_id=consumer_group_id,
        auto_create_topics=auto_create_topics,
        assignment_timeout_s=assignment_timeout_s,
        session_id=session_id,
    ) as (read_stream, write_stream):
        async with ClientSession(
            read_stream,
            write_stream,
            read_timeout_seconds=read_timeout_seconds,
            sampling_callback=sampling_callback,
            elicitation_callback=elicitation_callback,
            list_roots_callback=list_roots_callback,
            logging_callback=logging_callback,
            message_handler=message_handler,
            client_info=client_info,
            sampling_capabilities=sampling_capabilities,
        ) as session:
            if initialize:
                await session.initialize()
            yield session


@asynccontextmanager
async def inprocess_client_session(
    server: Server,
    *,
    read_timeout_seconds: timedelta | None = None,
    initialize: bool = True,
    sampling_callback: SamplingFnT | None = None,
    elicitation_callback: ElicitationFnT | None = None,
    list_roots_callback: ListRootsFnT | None = None,
    logging_callback: LoggingFnT | None = None,
    message_handler: MessageHandlerFnT | None = None,
    client_info: types.Implementation | None = None,
    sampling_capabilities: types.SamplingCapability | None = None,
) -> AsyncIterator[ClientSession]:
    """
    Create an MCP `ClientSession` connected to a Python-native server instance in-process.

    This avoids stringly-typed subprocess configs (`StdioServerParameters`) entirely by wiring the
    client/server streams directly.
    """

    try:
        from fastmcp.server.tasks.capabilities import get_task_capabilities
        from fastmcp.utilities.logging import temporary_log_level
        from mcp.server.lowlevel.server import NotificationOptions

        from kafklient.mcp.server import _get_lifespan_context  # pyright: ignore[reportPrivateUsage]
    except Exception as e:  # pragma: no cover
        raise RuntimeError("In-process MCP session requires MCP server dependencies. Install `kafklient[mcp]`.") from e

    # Mirror `run_server_async` init behavior (capabilities + notifications).
    mcp_server = server._mcp_server  # pyright: ignore[reportPrivateUsage]
    init_opts = mcp_server.create_initialization_options(
        notification_options=NotificationOptions(tools_changed=True),
        experimental_capabilities=dict(get_task_capabilities()),
    )

    # Stream wiring:
    # - client writes -> server reads
    # - server writes -> client reads
    c2s_send, c2s_recv = anyio.create_memory_object_stream[SessionMessage](0)
    s2c_send, s2c_recv = anyio.create_memory_object_stream[SessionMessage](0)

    cancelled_exc = anyio.get_cancelled_exc_class()

    async def run_server_session() -> None:
        try:
            await mcp_server.run(c2s_recv, s2c_send, init_opts)
        except cancelled_exc:
            await checkpoint()
        except BaseException:
            logger.exception("In-process MCP server session crashed")
        finally:
            for s in (c2s_recv, s2c_send):
                try:
                    await s.aclose()
                except Exception:
                    pass

    with temporary_log_level(None):
        async with _get_lifespan_context(server):
            async with anyio.create_task_group() as tg:
                tg.start_soon(run_server_session)
                try:
                    async with ClientSession(
                        s2c_recv,
                        c2s_send,
                        read_timeout_seconds=read_timeout_seconds,
                        sampling_callback=sampling_callback,
                        elicitation_callback=elicitation_callback,
                        list_roots_callback=list_roots_callback,
                        logging_callback=logging_callback,
                        message_handler=message_handler,
                        client_info=client_info,
                        sampling_capabilities=sampling_capabilities,
                    ) as session:
                        if initialize:
                            await session.initialize()
                        yield session
                finally:
                    # Ensure the background server task is stopped.
                    tg.cancel_scope.cancel()
                    for s in (c2s_send, s2c_recv):
                        try:
                            await s.aclose()
                        except Exception:
                            pass
