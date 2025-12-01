import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from typing import Literal, Optional, Type, override

from .._logging import get_logger
from ..types import KafkaError, Producer, T_Co
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
    rebalance_listener: Optional[PartitionListener] = field(default_factory=_RPCRebalanceListener)
    waiters: dict[bytes, Waiter[object]] = field(default_factory=dict[bytes, Waiter[object]], init=False, repr=False)

    async def request(
        self,
        req_topic: str,
        req_value: bytes,
        req_headers_reply_to: Optional[list[str]],
        *,
        req_key: Optional[bytes] = None,
        req_headers: Optional[list[tuple[str, str | bytes]]] = None,
        res_timeout: float = 30.0,
        res_expect_type: Optional[Type[T_Co]] = None,
        correlation_id: Optional[bytes] = None,
        propagate_corr_to: Literal["key", "header", "both"] = "key",
        correlation_header_key: str = "x-corr-id",
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
        if self._closed:
            await self.start()
        producer = await self.producer

        # Register waiter BEFORE sending to avoid race
        fut: asyncio.Future[T_Co] = asyncio.get_running_loop().create_future()
        self.waiters[corr_id] = Waiter[T_Co](future=fut, expect_type=res_expect_type)

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
