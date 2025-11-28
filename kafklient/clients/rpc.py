import asyncio
import logging
import uuid
from dataclasses import InitVar, dataclass, field
from typing import Callable, Optional, Type, override

from beartype.door import is_bearable

from .._logging import get_logger
from ..types import Consumer, KafkaError, Producer, T_Co
from ..types.backend import KafkaException, Message, TopicPartition
from ..utils.task import Waiter
from .base_client import KafkaBaseClient, PartitionListener

logger: logging.Logger = get_logger(__name__)


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

    # Seek to end on assign to ensure we only receive new responses
    seek_to_end_on_assign: bool = True

    producer_factory: InitVar[Callable[[], Producer]] = lambda: Producer({
        "bootstrap.servers": "127.0.0.1:9092",
    })
    consumer_factory: InitVar[Callable[[], Consumer]] = lambda: Consumer({
        "bootstrap.servers": "127.0.0.1:9092",
        "group.id": f"rpc-request-{uuid.uuid4().hex}",
        "auto.offset.reset": "latest",
    })
    rebalance_listener: Optional[PartitionListener] = field(default_factory=_RPCRebalanceListener)
    _waiters: dict[bytes, Waiter[object]] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(
        self,
        corr_from_record: Optional[Callable[[Message, Optional[object]], Optional[bytes]]],
        producer_factory: Callable[[], Producer],
        consumer_factory: Callable[[], Consumer],
    ) -> None:
        super().__post_init__(corr_from_record)
        self._producer_factory = producer_factory
        self._consumer_factory = consumer_factory

    async def request(
        self,
        req_topic: str,
        req_value: bytes,
        *,
        req_key: Optional[bytes] = None,
        req_headers: Optional[list[tuple[str, str | bytes]]] = None,
        req_headers_reply_to: Optional[list[str]] = None,
        res_timeout: float = 30.0,
        res_expect_type: Optional[Type[T_Co]] = None,
        correlation_id: Optional[bytes] = None,
        propagate_corr_to: str = "both",
        correlation_header_key: str = "request_id",
    ) -> T_Co:
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
        if propagate_corr_to in ("key", "both") and msg_key is None:
            msg_key = corr_id
        if propagate_corr_to in ("header", "both"):
            if not any(k.lower() == correlation_header_key.lower() for k, _ in msg_headers):
                msg_headers.append((correlation_header_key, corr_id))

        if req_headers_reply_to:
            for topic in req_headers_reply_to:
                msg_headers.append(("x-reply-topic", topic.encode("utf-8")))

        # Start client if needed
        if self._closed:
            await self.start()
        producer = await self.producer

        # Register waiter BEFORE sending to avoid race
        fut: asyncio.Future[T_Co] = asyncio.get_running_loop().create_future()
        self._waiters[corr_id] = Waiter[T_Co](future=fut, expect_type=res_expect_type)

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
            self._waiters.pop(corr_id, None)
            raise

        # Wait for response
        try:
            return await asyncio.wait_for(fut, timeout=res_timeout)
        except asyncio.TimeoutError:
            self._waiters.pop(corr_id, None)
            raise TimeoutError(f"Timed out waiting for response (corr_id={corr_id})")
        except Exception:
            self._waiters.pop(corr_id, None)
            raise

    @override
    async def ready(self, *, timeout: float | None = None) -> None:
        """Public method to ensure consumer is ready for RPC requests."""
        await super().ready(timeout=timeout)
        await self.producer

    async def _produce_request(
        self,
        producer: Producer,
        *,
        topic: str,
        value: bytes,
        key: Optional[bytes],
        headers: list[tuple[str, str | bytes]] | None,
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
        parsed_candidates: list[tuple[object, Type[object]]],
        cid: Optional[bytes],
    ) -> None:
        if not cid:
            return
        waiter = self._waiters.get(cid)
        if not waiter or waiter.future.done():
            return

        expect = waiter.expect_type
        if expect is None:
            waiter.future.set_result(parsed_candidates[0][0])
            self._waiters.pop(cid, None)
            return

        for obj, _ in parsed_candidates:
            try:
                if is_bearable(obj, expect):  # pyright: ignore[reportArgumentType]
                    waiter.future.set_result(obj)
                    self._waiters.pop(cid, None)
                    return
            except Exception:
                pass

        logger.debug(
            f"Response type mismatch for corr_id={cid!r}: "
            f"expected {expect}, got [{', '.join(str(ot) for _, ot in parsed_candidates)}]"
        )

    async def _on_stop_cleanup(self) -> None:
        for w in self._waiters.values():
            if not w.future.done():
                w.future.set_exception(RuntimeError("Client stopped before response"))
        self._waiters.clear()
        self._consumer_ready = False
