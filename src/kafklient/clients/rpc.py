import asyncio
import inspect
import logging
import uuid
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Literal, Optional, Type, Union, override

from .._logging import get_logger
from ..types import KafkaError, Producer, T
from ..types.backend import KafkaException, Message, TopicPartition
from ..utils.task import Waiter
from .base_client import KafkaBaseClient, PartitionListener

logger: logging.Logger = get_logger(__name__)

# Handler type: sync or async function that takes parsed request and returns response bytes
RequestHandler = Callable[[T, Message], Union[bytes, Awaitable[bytes]]]


class _RPCRebalanceListener(PartitionListener):
    """Event-driven partition assignment notification for RPC consumer"""

    def __init__(self) -> None:
        self._assigned_event = asyncio.Event()

    async def on_partitions_revoked(self, partitions: list[TopicPartition]) -> None:
        self._assigned_event.clear()

    async def on_partitions_assigned(self, partitions: list[TopicPartition]) -> None:
        if partitions:
            self._assigned_event.set()

    async def on_partitions_lost(self, partitions: list[TopicPartition]) -> None:
        self._assigned_event.clear()

    def is_assigned(self) -> bool:
        return self._assigned_event.is_set()


@dataclass
class KafkaRPC(KafkaBaseClient):
    """
    RPC client that sends requests and waits for responses via Kafka.

    - Response topics are defined via ParserSpec.topics
    - Request topic is specified per request() call
    - Responses are matched by correlation_id
    """

    rebalance_listener: Optional[PartitionListener] = field(default_factory=_RPCRebalanceListener)
    waiters: dict[bytes, Waiter[object]] = field(default_factory=dict[bytes, Waiter[object]], init=False, repr=False)

    @override
    async def ready(self, *, timeout: float | None = None) -> None:
        """Public method to ensure consumer is ready for RPC requests."""
        await super().ready(timeout=timeout)
        await self.producer

    async def request(
        self,
        req_topic: str,
        req_value: bytes,
        req_headers_reply_to: Optional[list[str]],
        *,
        req_key: Optional[bytes] = None,
        req_headers: Optional[list[tuple[str, str | bytes | None]]] = None,
        res_timeout: float = 30.0,
        res_expect_type: Optional[Type[T]] = None,
        correlation_id: Optional[bytes] = None,
        propagate_corr_to: Literal["key", "header", "both"] = "key",
        correlation_header_key: str = "x-corr-id",
    ) -> T:
        if not req_topic or not req_topic.strip():
            raise ValueError("req_topic must be non-empty")

        # Build correlation ID
        if correlation_id:
            corr_id = correlation_id
        elif req_key:
            corr_id = req_key
        else:
            corr_id = uuid.uuid4().hex.encode("utf-8")
        corr_id = bytes(corr_id)

        # Build message key and headers
        msg_key = req_key
        msg_headers = list(req_headers or [])
        # add reply topic to headers
        for topic in req_headers_reply_to or ():
            msg_headers.append(("x-reply-topic", topic.encode("utf-8")))
        # propagate correlation id to key or header
        if (propagate_corr_to == "key" or propagate_corr_to == "both") and msg_key is None:
            msg_key = corr_id
        if propagate_corr_to == "header" or propagate_corr_to == "both":
            if not any(k.lower() == correlation_header_key.lower() for k, _ in msg_headers):
                msg_headers.append((correlation_header_key, corr_id))

        # Start client if needed
        if self.closed:
            await self.start()
        producer = await self.producer

        # Register waiter BEFORE sending to avoid race
        fut: asyncio.Future[T] = asyncio.get_running_loop().create_future()
        self.waiters[corr_id] = Waiter[T](future=fut, expect_type=res_expect_type)

        try:
            await self._produce_request(
                producer,
                topic=req_topic,
                value=req_value,
                key=msg_key,
                headers=msg_headers,
            )
            logger.debug(f"sent request corr_id={corr_id} topic={req_topic}")
        except Exception:
            self.waiters.pop(corr_id, None)
            raise

        # Wait for response
        try:
            return await asyncio.wait_for(fut, timeout=res_timeout)
        except asyncio.TimeoutError:
            self.waiters.pop(corr_id, None)
            raise TimeoutError(f"Timed out waiting for response (corr_id={corr_id})")
        except Exception:
            self.waiters.pop(corr_id, None)
            raise

    async def _produce_request(
        self,
        producer: Producer,
        *,
        topic: str,
        value: bytes,
        key: Optional[bytes],
        headers: list[tuple[str, str | bytes | None]] | None,
    ) -> None:
        loop = asyncio.get_running_loop()
        delivery_future: asyncio.Future[Message | None] = loop.create_future()

        def _on_delivery(err: Optional[KafkaError], msg: Message) -> None:
            if err:
                if not delivery_future.done():
                    loop.call_soon_threadsafe(delivery_future.set_exception, KafkaException(err))
            else:
                if not delivery_future.done():
                    loop.call_soon_threadsafe(delivery_future.set_result, msg)

        def _produce_and_flush() -> None:
            producer.produce(
                topic,
                value=value,
                key=key,
                headers=headers,
                on_delivery=_on_delivery,
            )
            producer.flush()

        await self._producer_executor.run(_produce_and_flush)
        await delivery_future

    async def _on_record(
        self,
        record: Message,
        parsed: tuple[object, Type[object]],
        cid: Optional[bytes],
    ) -> None:
        if not cid:
            return
        waiter = self.waiters.get(cid)
        if not waiter or waiter.future.done():
            return

        if waiter.expect_type is None:
            waiter.future.set_result(parsed[0])
            self.waiters.pop(cid, None)
            return

        obj, ot = parsed
        try:
            if ot == waiter.expect_type:
                waiter.future.set_result(obj)
                self.waiters.pop(cid, None)
                return
        except Exception:
            pass

        logger.debug(f"Response type mismatch for corr_id={cid!r}: expected {waiter.expect_type}, got {ot}")

    async def _on_stop_cleanup(self) -> None:
        for w in self.waiters.values():
            if not w.future.done():
                w.future.set_exception(RuntimeError("Client stopped before response"))
        self.waiters.clear()
        self._consumer_ready = False


@dataclass
class KafkaRPCServer(KafkaBaseClient):
    """
    RPC server that listens for requests and sends responses via Kafka.

    - Request topics are defined via ParserSpec.topics
    - Response topics are extracted from the `x-reply-topic` header of incoming requests
    - Responses are matched by correlation_id (propagated from request key or header)

    Usage:
        server = KafkaRPCServer(
            consumer_config={"bootstrap.servers": "localhost:9092", "group.id": "my-server"},
            producer_config={"bootstrap.servers": "localhost:9092"},
            parsers=[ParserSpec(topics=["requests"], type=MyRequest, parser=parse_request)],
        )

        @server.handler(MyRequest)
        async def handle_request(request: MyRequest, message: Message) -> bytes:
            return json.dumps({"result": "ok"}).encode()

        await server.start()
    """

    # Reply topic header key
    reply_topic_header_key: str = "x-reply-topic"

    # Correlation ID header key for responses
    correlation_header_key: str = "x-corr-id"

    # Whether to propagate correlation ID to response key, header, or both
    propagate_corr_to: Literal["key", "header", "both"] = "key"

    # Registered handlers: type -> handler function
    _handlers: dict[Type[object], RequestHandler[object]] = field(
        default_factory=dict[Type[object], RequestHandler[object]], init=False, repr=False
    )

    def handler(self, request_type: Type[T]) -> Callable[[RequestHandler[T]], RequestHandler[T]]:
        """
        Decorator to register a handler for a specific request type.

        Example:
            @server.handler(MyRequest)
            async def handle_request(request: MyRequest, message: Message) -> bytes:
                return json.dumps({"result": "ok"}).encode()
        """

        def decorator(
            func: RequestHandler[T],
        ) -> RequestHandler[T]:
            self._handlers[request_type] = func  # pyright: ignore[reportArgumentType]
            return func

        return decorator

    def register_handler(
        self,
        request_type: Type[T],
        handler: Callable[[T, Message], Union[bytes, Awaitable[bytes]]],
    ) -> None:
        """
        Register a handler for a specific request type programmatically.

        Example:
            async def handle_request(request: MyRequest, message: Message) -> bytes:
                return json.dumps({"result": "ok"}).encode()

            server.register_handler(MyRequest, handle_request)
        """
        self._handlers[request_type] = handler  # pyright: ignore[reportArgumentType]

    async def _on_record(
        self,
        record: Message,
        parsed: tuple[object, Type[object]],
        cid: Optional[bytes],
    ) -> None:
        obj, obj_type = parsed

        # Find handler for this type
        handler = self._handlers.get(obj_type)
        if handler is None:
            logger.debug(f"No handler registered for type {obj_type.__name__}")
            return

        # Extract reply topic from headers
        reply_topics = self._extract_reply_topics(record)
        if not reply_topics:
            logger.debug(f"No reply topic found in message headers (corr_id={cid!r})")
            return

        # Execute handler
        try:
            result = handler(obj, record)
            if inspect.isawaitable(result):
                response_bytes = await result
            else:
                response_bytes = result
        except Exception as e:
            logger.exception(f"Handler failed for type {obj_type.__name__} (corr_id={cid!r})")
            # Optionally send error response
            response_bytes = f'{{"error": "{str(e)}"}}'.encode("utf-8")

        # Send response to all reply topics
        producer = await self.producer
        for reply_topic in reply_topics:
            try:
                await self._produce_response(
                    producer,
                    topic=reply_topic,
                    value=response_bytes,
                    correlation_id=cid,
                )
                logger.debug(f"Sent response to {reply_topic} (corr_id={cid!r})")
            except Exception:
                logger.exception(f"Failed to send response to {reply_topic} (corr_id={cid!r})")

    def _extract_reply_topics(self, record: Message) -> list[str]:
        """Extract reply topic(s) from message headers."""
        reply_topics: list[str] = []
        headers = record.headers() or []
        for key, value in headers:
            if key.lower() == self.reply_topic_header_key.lower():
                if value is None:
                    continue
                try:
                    if isinstance(value, bytes):
                        value = value.decode("utf-8")
                    reply_topics.append(value)
                except Exception:
                    pass
        return reply_topics

    async def _produce_response(
        self,
        producer: Producer,
        *,
        topic: str,
        value: bytes,
        correlation_id: Optional[bytes],
    ) -> None:
        """Produce response message with correlation ID."""
        loop = asyncio.get_running_loop()
        delivery_future: asyncio.Future[Message | None] = loop.create_future()

        def _on_delivery(err: Optional[KafkaError], msg: Message) -> None:
            if err:
                if not delivery_future.done():
                    loop.call_soon_threadsafe(delivery_future.set_exception, KafkaException(err))
            else:
                if not delivery_future.done():
                    loop.call_soon_threadsafe(delivery_future.set_result, msg)

        # Build key and headers based on propagate_corr_to setting
        msg_key: Optional[bytes] = None
        msg_headers: list[tuple[str, str | bytes | None]] = []

        if correlation_id:
            if self.propagate_corr_to == "key" or self.propagate_corr_to == "both":
                msg_key = correlation_id
            if self.propagate_corr_to == "header" or self.propagate_corr_to == "both":
                msg_headers.append((self.correlation_header_key, correlation_id))

        def _produce_and_flush() -> None:
            producer.produce(
                topic,
                value=value,
                key=msg_key,
                headers=msg_headers if msg_headers else None,
                on_delivery=_on_delivery,
            )
            producer.flush()

        await self._producer_executor.run(_produce_and_flush)
        await delivery_future

    async def _on_stop_cleanup(self) -> None:
        """Cleanup on server stop."""
        pass
