import asyncio
import logging
import threading
from abc import ABC, abstractmethod
from dataclasses import InitVar, dataclass, field
from types import TracebackType
from typing import (
    Callable,
    Iterable,
    Literal,
    Optional,
    Protocol,
    Self,
    Type,
    final,
)

from .._logging import get_logger
from ..types import (
    OFFSET_END,
    AutoCommitConfig,
    Consumer,
    ConsumerConfig,
    KafkaError,
    Message,
    ParserSpec,
    Producer,
    ProducerConfig,
    TopicPartition,
)
from ..utils.executor import DedicatedThreadExecutor

logger: logging.Logger = get_logger(__name__)


class PartitionListener(Protocol):
    async def on_partitions_assigned(self, partitions: list[TopicPartition]) -> None: ...
    async def on_partitions_revoked(self, partitions: list[TopicPartition]) -> None: ...
    async def on_partitions_lost(self, partitions: list[TopicPartition]) -> None: ...


@dataclass
class KafkaBaseClient(ABC):
    """
    Group-managed subscription mode powered by sync Consumer/Producer
    with asyncio.to_thread() for non-blocking async API
    """

    # ---------- Behavior ----------
    seek_to_end_on_assign: bool = True  # 새 메시지부터
    metadata_refresh_min_interval_s: float = 5.0
    auto_commit: Optional[AutoCommitConfig] = None
    backoff_min: float = 0.5
    backoff_max: float = 10.0
    backoff_factor: float = 2.0
    assignment_timeout_s: float = 5.0
    rebalance_listener: Optional[PartitionListener] = None

    # ---------- Parser / Correlation ----------
    parsers: Iterable[ParserSpec[object]] = ()
    corr_header_keys: tuple[str, ...] = (
        "request_id",
        "correlation_id",
        "x-correlation-id",
    )
    corr_from_record: InitVar[Optional[Callable[[Message, Optional[object]], Optional[bytes]]]] = None
    """Correlation key extractor: (record, parsed or None) -> correlation_id (None if not found)"""

    _corr_from_record: Callable[[Message, Optional[object]], Optional[bytes]] = field(init=False, repr=False)
    _producer_factory: Optional[Callable[[], Producer]] = field(default=None, init=False, repr=False)
    _consumer_factory: Optional[Callable[[], Consumer]] = field(default=None, init=False, repr=False)
    _producer: Optional[Producer] = field(default=None, init=False, repr=False)
    _consumer: Optional[Consumer] = field(default=None, init=False, repr=False)
    _consumer_task: Optional[asyncio.Task[None]] = field(default=None, init=False, repr=False)
    _closed: bool = field(default=True, init=False, repr=False)
    _assigned_partitions: list[TopicPartition] = field(default_factory=list, init=False, repr=False)
    _assignment_event: asyncio.Event = field(default_factory=asyncio.Event, init=False, repr=False)
    _start_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)
    _stop_event: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _parsers_by_topic: dict[str, list[ParserSpec[object]]] = field(default_factory=dict, init=False, repr=False)
    _subscription_topics: set[str] = field(default_factory=set, init=False, repr=False)
    _pending_seek_partitions: list[TopicPartition] = field(default_factory=list, init=False, repr=False)
    _consumer_executor: DedicatedThreadExecutor = field(init=False, repr=False)
    _producer_executor: DedicatedThreadExecutor = field(init=False, repr=False)

    def __post_init__(self, corr_from_record: Optional[Callable[[Message, Optional[object]], Optional[bytes]]]) -> None:
        # Register parsers and collect static assignments (once at initialization)
        for ps in self.parsers:
            # Build topic index and subscription topics (ignore explicit partitioning)
            for topic in ps["topics"]:
                self._parsers_by_topic.setdefault(topic, []).append(ps)
                self._subscription_topics.add(topic)

        # Event loop reference for thread-safe callback scheduling
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # Dedicated thread executors for thread-unsafe Kafka operations
        # Separate threads for consumer and producer to avoid blocking
        self._consumer_executor = DedicatedThreadExecutor(name=f"{self.__class__.__name__}-consumer")
        self._producer_executor = DedicatedThreadExecutor(name=f"{self.__class__.__name__}-producer")

        # Default correlation key extractor: case-insensitive
        if corr_from_record is None:
            self._corr_from_record = self._default_corr_from_record
        else:
            self._corr_from_record = corr_from_record

    # ---------- Lifecycle ----------
    @final
    async def start(self, *, timeout: float | None = None) -> None:
        if not self._closed:
            return

        self._loop = asyncio.get_running_loop()
        self._stop_event.clear()
        self._consumer_executor.start(self._loop)
        self._producer_executor.start(self._loop)

        await self.ready(timeout=timeout)

        self._closed = False
        logger.info(f"{self.__class__.__name__} started")

    async def stop(self) -> None:
        if self._closed:
            return

        # Signal consumer loop to stop
        self._stop_event.set()

        # Wait for consumer task to notice the stop signal and exit
        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
            self._consumer_task = None

        # Cleanup on the dedicated threads
        if self._consumer_executor.is_running:
            if self._consumer is not None and self.auto_commit:
                try:
                    await self._consumer_executor.run(self._consumer.commit)
                except Exception:
                    logger.exception("Final commit failed")

            if self._consumer is not None:
                try:
                    consumer = self._consumer
                    self._consumer = None
                    await self._consumer_executor.run(consumer.close)
                except Exception:
                    logger.exception("Error closing consumer")

            self._consumer_executor.stop()

        if self._producer_executor.is_running:
            if self._producer is not None:
                try:
                    await self._producer_executor.run(self._producer.flush)
                except Exception:
                    logger.exception("Error flushing producer")
                self._producer = None

            self._producer_executor.stop()

        try:
            await self._on_stop_cleanup()
        except Exception:
            logger.exception("_on_stop_cleanup failed")

        self._closed = True
        self._loop = None
        logger.info(f"{self.__class__.__name__} stopped")

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        await self.stop()

    async def ready(self, *, timeout: float | None = None) -> None:
        """Ensure the consumer has an assignment before proceeding.
        Raises TimeoutError if the assignment is not received within the timeout.
        """
        if not self._subscription_topics:
            return
        await self.consumer
        if timeout is None:
            timeout = self.assignment_timeout_s
        await asyncio.wait_for(self._assignment_event.wait(), timeout=timeout)

    # ---------- 내부 공통 처리 ----------
    @property
    async def producer(self) -> Producer:
        async with self._start_lock:
            if self._producer is not None:
                return self._producer
            if self._producer_factory is None:
                raise ValueError("producer_factory is not set")
            self._producer = await self._producer_executor.run(self._producer_factory)
            return self._producer

    @property
    async def consumer(self) -> Consumer:
        async with self._start_lock:
            if self._consumer is not None:
                return self._consumer
            if self._consumer_factory is None:
                raise ValueError("consumer_factory is not set")
            self._consumer = await self._consumer_executor.run(self._consumer_factory)

            # Sync callbacks that schedule async handlers (runs on dedicated thread)
            def _on_assign(consumer: Consumer, partitions: list[TopicPartition]) -> None:
                self._assigned_partitions = sorted(partitions, key=self._tp_sort_key)
                # Store partitions for seek in consume loop (seek fails if called during callback)
                if self.seek_to_end_on_assign and partitions:
                    self._pending_seek_partitions = list(partitions)
                if self._loop and not self._stop_event.is_set():
                    asyncio.run_coroutine_threadsafe(self._handle_assign(partitions), self._loop)

            def _on_revoke(consumer: Consumer, partitions: list[TopicPartition]) -> None:
                revoked = {(tp.topic, tp.partition) for tp in partitions}
                self._assigned_partitions = [
                    tp for tp in self._assigned_partitions if (tp.topic, tp.partition) not in revoked
                ]
                if partitions:
                    self._assignment_event.clear()
                if self._loop and not self._stop_event.is_set():
                    asyncio.run_coroutine_threadsafe(
                        self._notify_rebalance_listener("on_partitions_revoked", partitions),
                        self._loop,
                    )

            def _on_lost(consumer: Consumer, partitions: list[TopicPartition]) -> None:
                lost = {(tp.topic, tp.partition) for tp in partitions}
                self._assigned_partitions = [
                    tp for tp in self._assigned_partitions if (tp.topic, tp.partition) not in lost
                ]
                if partitions:
                    self._assignment_event.clear()
                if self._loop and not self._stop_event.is_set():
                    asyncio.run_coroutine_threadsafe(
                        self._notify_rebalance_listener("on_partitions_lost", partitions),
                        self._loop,
                    )

            if self._subscription_topics:
                try:
                    consumer = self._consumer

                    def subscribe() -> None:
                        consumer.subscribe(
                            sorted(self._subscription_topics),
                            on_assign=_on_assign,
                            on_revoke=_on_revoke,
                            on_lost=_on_lost,
                        )

                    await self._consumer_executor.run(subscribe)
                except Exception:
                    logger.exception("Failed to subscribe to topics")
                    raise

            self._consumer_task = asyncio.create_task(self._consume_loop(), name=f"{self.__class__.__name__}_loop")
            return self._consumer

    async def _handle_assign(self, partitions: list[TopicPartition]) -> None:
        """Handle partition assignment notification (seek is done in consume loop)"""
        await self._notify_rebalance_listener("on_partitions_assigned", partitions)
        # If seek_to_end_on_assign is False, set event immediately
        # Otherwise, _consume_loop will set it after seek completes
        if not self.seek_to_end_on_assign and partitions:
            self._assignment_event.set()

    # Parsing + Dispatching
    def _parse_record(self, record: Message) -> tuple[list[tuple[object, Type[object]]], Optional[bytes]]:
        # (1) Extract correlation_id (before parsing)
        cid = None
        try:
            cid = self._corr_from_record(record, None)
        except Exception as ex:
            logger.exception(f"correlation_from_record(None) failed: {ex}")

        # (2) Parsing
        parsed_candidates: list[tuple[object, Type[object]]] = []
        for spec in self._parsers_by_topic.get((topic := record.topic()) or "") or ():
            try:
                parsed_candidates.append((spec["parser"](record), spec["type"]))
            except Exception as ex:
                logger.debug(
                    f"Parser failed (topic={topic}, out={getattr(spec['type'], '__name__', spec['type'])}): {ex}"
                )

        # (3) fallback: raw
        if not parsed_candidates:
            parsed_candidates.append((record, Message))

        return parsed_candidates, cid

    async def _consume_loop(self) -> None:
        backoff = self.backoff_min

        def _poll_and_maybe_seek(timeout: float) -> tuple[Message | None, bool]:
            """Poll consumer, and seek if pending partitions exist. Returns (message, did_seek)."""
            if self._consumer is None or self._stop_event.is_set():
                return None, False

            # Check for pending seek partitions (set during on_assign callback)
            did_seek = False
            if self._pending_seek_partitions:
                partitions_to_seek = self._pending_seek_partitions
                self._pending_seek_partitions = []
                for tp in partitions_to_seek:
                    try:
                        self._consumer.seek(TopicPartition(tp.topic, tp.partition, OFFSET_END))
                    except Exception as e:
                        logger.debug(f"seek_to_end skipped (partition not ready): {e}")
                # Poll once after seek to apply it before returning
                self._consumer.poll(0.01)
                did_seek = True

            return self._consumer.poll(timeout), did_seek

        try:
            while not self._stop_event.is_set():
                try:
                    if self._consumer is None:
                        break
                    rec, did_seek = await self._consumer_executor.run(lambda: _poll_and_maybe_seek(timeout=0.5))
                    if did_seek:
                        # Signal assignment is complete after seek
                        self._assignment_event.set()
                    if self._stop_event.is_set():
                        break
                    if rec is None:
                        continue
                    err = rec.error()
                    if err:
                        if err.code() == KafkaError._PARTITION_EOF:
                            continue
                        logger.error(f"Kafka error: {err}")
                        continue
                    parsed_candidates, cid = self._parse_record(rec)
                    try:
                        await self._on_record(rec, parsed_candidates, cid)
                    except Exception:
                        logger.exception("_on_record failed")
                    backoff = self.backoff_min
                except asyncio.CancelledError:
                    raise
                except Exception:
                    if self._stop_event.is_set():
                        break
                    logger.exception("Unexpected error in consumer loop; will retry")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * self.backoff_factor, self.backoff_max)
        except asyncio.CancelledError:
            pass

    async def _notify_rebalance_listener(
        self,
        method_name: Literal["on_partitions_assigned", "on_partitions_revoked", "on_partitions_lost"],
        partitions: list[TopicPartition],
    ) -> None:
        listener: PartitionListener | None = self.rebalance_listener
        if not listener:
            return
        try:
            match method_name:
                case "on_partitions_assigned":
                    await listener.on_partitions_assigned(partitions)
                case "on_partitions_revoked":
                    await listener.on_partitions_revoked(partitions)
                case "on_partitions_lost":
                    await listener.on_partitions_lost(partitions)
                case _:  # pyright: ignore[reportUnnecessaryComparison]
                    return
        except Exception:
            logger.exception(f"Rebalance listener {method_name} failed")

    # async def _wait_for_assignment(self, timeout: float | None = None) -> None:
    #     """Poll until the consumer receives an assignment."""
    #     if self._consumer is None or not self._subscription_topics:
    #         return

    #     consumer = self._consumer
    #     if timeout is None:
    #         timeout = self.assignment_timeout_s
    #     deadline = time.time() + timeout

    #     def _poll_until() -> bool:
    #         while True:
    #             consumer.poll(0.05)
    #             if consumer.assignment():
    #                 return True
    #             if time.time() >= deadline:
    #                 return False

    #     assigned = await asyncio.to_thread(_poll_until)
    #     if not assigned:
    #         raise TimeoutError(f"assignment not received within {timeout}s")

    @staticmethod
    def _tp_sort_key(tp: TopicPartition) -> tuple[str, int]:
        return (tp.topic, tp.partition)

    def _default_corr_from_record(self, rec: Message, parsed: Optional[object]) -> Optional[bytes]:
        # Default correlation key extractor: Header priority, case-insensitive
        try:
            headers = rec.headers() or []
            for k, v in headers:
                if (k or "").lower() in (k.lower() for k in self.corr_header_keys):
                    try:
                        return bytes(v)
                    except Exception:
                        pass
        except Exception:
            pass
        try:
            key = rec.key()
            if key:
                return bytes(key)
        except Exception:
            pass
        return None

    def assigned_table(self) -> list[dict[str, object]]:
        return [
            {
                "topic": tp.topic,
                "partition": tp.partition,
                "since": None,
                "source": "group",
                "seek_to_end_on_assign": self.seek_to_end_on_assign,
            }
            for tp in sorted(self._assigned_partitions, key=self._tp_sort_key)
        ]

    @abstractmethod
    async def _on_record(
        self,
        record: Message,
        parsed_candidates: list[tuple[object, Type[object]]],
        cid: Optional[bytes],
    ) -> None: ...

    @abstractmethod
    async def _on_stop_cleanup(self) -> None: ...


def create_consumer(config: ConsumerConfig, *, logger: logging.Logger | None = None) -> Consumer:
    return Consumer(dict(config), logger=logger or get_logger(__name__))


def create_producer(config: ProducerConfig, *, logger: logging.Logger | None = None) -> Producer:
    return Producer(dict(config), logger=logger or get_logger(__name__))
