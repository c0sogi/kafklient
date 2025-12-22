import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator
from uuid import uuid4

import anyio
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
from kafklient.types.config import ConsumerConfig, ProducerConfig

logger = logging.getLogger(__name__)


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
        parsers=[
            {
                "topics": [consumer_topic],
                "parser": lambda x: JSONRPCMessage.model_validate_json(x.value() or b""),
                "type": JSONRPCMessage,
            }
        ],
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

                # Build experimental capabilities
                experimental_capabilities = get_task_capabilities()

                await mcp_server.run(
                    read_stream,
                    write_stream,
                    mcp_server.create_initialization_options(
                        notification_options=NotificationOptions(tools_changed=True),
                        experimental_capabilities=experimental_capabilities,
                    ),
                )


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
        )
    )
