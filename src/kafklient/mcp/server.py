import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator
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
from kafklient.types.backend import Message as KafkaMessage
from kafklient.types.config import ConsumerConfig, ProducerConfig
from kafklient.types.parser import Parser

logger = logging.getLogger(__name__)
REPLY_TOPIC_HEADER_KEY = "x-reply-topic"
SESSION_ID_HEADER_KEY = "x-session-id"


def _extract_reply_topic(record: KafkaMessage, *, fallback: str) -> str:
    """Kafka headers에서 reply-topic을 추출합니다(없으면 fallback)."""
    try:
        headers = record.headers() or []
    except Exception:
        headers = []
    for k, v in headers:
        if k.lower() != REPLY_TOPIC_HEADER_KEY.lower():
            continue
        if v is None:
            continue
        try:
            if isinstance(v, bytes):
                return v.decode("utf-8")
            return str(v)
        except Exception:
            continue
    return fallback


def _extract_session_id(record: KafkaMessage) -> bytes | None:
    try:
        headers = record.headers() or []
    except Exception:
        headers = []
    for k, v in headers:
        if k.lower() != SESSION_ID_HEADER_KEY.lower():
            continue
        if v is None:
            return None
        if isinstance(v, bytes):
            return v
        return str(v).encode("utf-8")
    return None


def _session_key_from_bytes(b: bytes) -> str:
    try:
        return b.decode("utf-8")
    except Exception:
        return b.hex()


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
            # 단일 세션(레거시) 모드
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
            # 멀티 세션(세션 격리) 모드
            # ---------------------------
            # 핵심:
            # - 클라이언트가 요청 메시지에 x-reply-topic 헤더로 "내 응답 토픽"을 싣는다.
            # - 서버는 reply-topic(=세션 키)별로 독립적인 MCP ServerSession을 생성/유지한다.
            # - 각 세션의 write_stream은 해당 reply-topic으로만 produce => 응답/알림 혼선 방지.

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

                async def run_mcp_session() -> None:
                    await mcp_server.run(read_stream, write_stream, init_opts)

                async def pump_session_to_kafka() -> None:
                    try:
                        async with write_stream_reader:
                            async for session_message in write_stream_reader:
                                json_str = session_message.message.model_dump_json(by_alias=True, exclude_none=True)
                                headers: list[tuple[str, str | bytes | None]] | None = (
                                    [(SESSION_ID_HEADER_KEY, session.session_id)]
                                    if session.session_id is not None
                                    else None
                                )
                                await listener.produce(session.target_topic, json_str.encode("utf-8"), headers=headers)
                    except anyio.ClosedResourceError:
                        await checkpoint()

                tg.start_soon(run_mcp_session)
                tg.start_soon(pump_session_to_kafka)
                return session

            logger.info(f"Starting MCP server {mcp.name!r} with transport 'stdio' over Kafka (multi_session=True)")

            try:
                async with anyio.create_task_group() as tg:
                    async for record in stream:
                        msg = JSONRPCMessage.model_validate_json(record.value() or b"")
                        reply_topic = _extract_reply_topic(record, fallback=producer_topic)
                        session_id = _extract_session_id(record)
                        # NOTE:
                        # session_id(=브릿지 인스턴스 UUID 등)와 reply_topic(문자열)이 같은 네임스페이스에서
                        # 섞이면 충돌할 수 있습니다. 예: isolate_session=False인 클라이언트가 reply_topic을
                        # 우연히 다른 클라이언트의 session_id 문자열과 동일하게 쓰면 같은 세션으로 합쳐짐.
                        # 따라서 키 네임스페이스를 분리하여 충돌을 원천 차단합니다.
                        session_key = (
                            f"sid:{_session_key_from_bytes(session_id)}"
                            if session_id is not None
                            else f"topic:{reply_topic}"
                        )
                        # reply-topic이 producer_topic과 다르면 "전용 토픽 응답"을 의미(클라이언트가 명시적으로 분리한 경우)
                        # 같으면 공용 응답 토픽을 쓰되 session-id 헤더로 필터링하여 혼선 방지.
                        target_topic = reply_topic if reply_topic != producer_topic else producer_topic
                        session = await ensure_session(
                            session_key=session_key,
                            target_topic=target_topic,
                            session_id=session_id,
                            tg=tg,
                        )
                        try:
                            await session.read_stream_writer.send(SessionMessage(msg))
                        except anyio.ClosedResourceError:
                            # 세션이 이미 종료된 상태(브릿지 종료 등)에서 추가 메시지를 보내면 ClosedResourceError가 발생합니다.
                            # 이 예외가 서버 전체를 죽이지 않도록 세션을 정리하고, 필요 시 1회 재생성 후 재시도합니다.
                            logger.info(
                                f"Session stream closed (session_key={session_key!r}); dropping session and retrying"
                            )
                            sessions.pop(session_key, None)
                            try:
                                await session.read_stream_writer.aclose()
                            except Exception:
                                pass

                            # 재시도(1회): 같은 키로 계속 요청이 들어오는 경우를 대비
                            try:
                                session = await ensure_session(
                                    session_key=session_key,
                                    target_topic=target_topic,
                                    session_id=session_id,
                                    tg=tg,
                                )
                                await session.read_stream_writer.send(SessionMessage(msg))
                            except anyio.ClosedResourceError:
                                # 재생성 직후에도 닫혀있다면 해당 메시지는 드롭
                                logger.warning(
                                    f"Session stream closed again (session_key={session_key!r}); dropping message"
                                )
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
