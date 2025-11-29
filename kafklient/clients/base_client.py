import asyncio
import logging
import threading
import uuid
from abc import ABC, abstractmethod
from dataclasses import InitVar, dataclass, field
from functools import partial
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
    AdminClient,
    Consumer,
    ConsumerConfig,
    KafkaError,
    Message,
    NewTopic,
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


def default_producer_config() -> ProducerConfig:
    return {"bootstrap.servers": "127.0.0.1:9092"}


def default_consumer_config() -> ConsumerConfig:
    return {"bootstrap.servers": "127.0.0.1:9092", "auto.offset.reset": "latest"}


@dataclass
class KafkaBaseClient(ABC):
    """
    Group-managed subscription mode powered by sync Consumer/Producer
    with asyncio.to_thread() for non-blocking async API
    """

    producer_config: ProducerConfig = field(default_factory=default_producer_config)
    consumer_config: ConsumerConfig = field(default_factory=default_consumer_config)

    # ---------- Behavior ----------
    seek_to_end_on_assign: bool = True  # 새 메시지부터
    metadata_refresh_min_interval_s: float = 5.0
    backoff_min: float = 0.5
    backoff_max: float = 10.0
    backoff_factor: float = 2.0
    assignment_timeout_s: float = 5.0
    rebalance_listener: Optional[PartitionListener] = None

    # ---------- Auto Topic Creation ----------
    auto_create_topics: bool = False
    """Automatically create topics if they don't exist before subscribing."""
    topic_num_partitions: int = 1
    """Number of partitions for auto-created topics."""
    topic_replication_factor: int = 1
    """Replication factor for auto-created topics."""

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
    _producer: Optional[Producer] = field(default=None, init=False, repr=False)
    _consumer: Optional[Consumer] = field(default=None, init=False, repr=False)
    _consumer_task: Optional[asyncio.Task[None]] = field(default=None, init=False, repr=False)
    _closed: bool = field(default=True, init=False, repr=False)
    _assigned_partitions: list[TopicPartition] = field(default_factory=list[TopicPartition], init=False, repr=False)
    _assignment_event: asyncio.Event = field(default_factory=asyncio.Event, init=False, repr=False)
    _start_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)
    _stop_event: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _parsers_by_topic: dict[str, list[ParserSpec[object]]] = field(
        default_factory=dict[str, list[ParserSpec[object]]], init=False, repr=False
    )
    _subscription_topics: set[str] = field(default_factory=set[str], init=False, repr=False)
    _pending_seek_partitions: list[TopicPartition] = field(default_factory=list[TopicPartition], init=False, repr=False)
    _consumer_executor: DedicatedThreadExecutor = field(init=False, repr=False)
    _producer_executor: DedicatedThreadExecutor = field(init=False, repr=False)

    def __post_init__(
        self,
        corr_from_record: Optional[Callable[[Message, Optional[object]], Optional[bytes]]],
    ) -> None:
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

        # Auto-create topics if enabled
        if self.auto_create_topics and self._subscription_topics:
            await self._ensure_topics_exist(list(self._subscription_topics))

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

    @property
    def producer_factory(self) -> Callable[[], Producer]:
        producer_config = self.producer_config.copy()
        if "bootstrap.servers" not in producer_config or not producer_config["bootstrap.servers"]:
            raise ValueError("bootstrap.servers is required for producer factory")

        def _factory() -> Producer:
            return Producer(dict(producer_config), logger=logger)

        return _factory

    @property
    async def producer(self) -> Producer:
        async with self._start_lock:
            if self._producer is not None:
                return self._producer
            self._producer = await self._producer_executor.run(self.producer_factory)
            return self._producer

    @property
    def consumer_factory(self) -> Callable[[], Consumer]:
        consumer_config = self.consumer_config.copy()
        if "group.id" not in consumer_config or not consumer_config["group.id"]:
            consumer_config["group.id"] = f"{self.__class__.__name__}-{uuid.uuid4().hex[:8]}"
        if "bootstrap.servers" not in consumer_config or not consumer_config["bootstrap.servers"]:
            raise ValueError("bootstrap.servers is required for consumer factory")

        def _factory() -> Consumer:
            return Consumer(dict(consumer_config), logger=logger)

        return _factory

    @property
    async def consumer(self) -> Consumer:
        async with self._start_lock:
            if self._consumer is not None:
                return self._consumer
            self._consumer = await self._consumer_executor.run(self.consumer_factory)

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
                    rec, did_seek = await self._consumer_executor.run(partial(_poll_and_maybe_seek, timeout=0.5))
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

    @property
    def bootstrap_servers(self) -> str:
        if "bootstrap.servers" not in self.producer_config or not self.producer_config["bootstrap.servers"]:
            raise ValueError("bootstrap.servers is required")
        return self.producer_config["bootstrap.servers"]

    async def _ensure_topics_exist(self, topics: list[str]) -> None:
        """Check if topics exist and create them if they don't."""
        if not self.bootstrap_servers:
            logger.warning("Cannot auto-create topics: bootstrap.servers not available")
            return

        def _create_missing_topics() -> list[str]:
            admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
            try:
                # Get existing topics
                metadata = admin.list_topics(timeout=10)
                existing_topics = set(metadata.topics.keys())

                # Find missing topics
                missing_topics = [t for t in topics if t not in existing_topics]
                if not missing_topics:
                    return []

                # Create missing topics
                new_topics = [
                    NewTopic(
                        topic,
                        num_partitions=self.topic_num_partitions,
                        replication_factor=self.topic_replication_factor,
                    )
                    for topic in missing_topics
                ]
                futures = admin.create_topics(new_topics)

                # Wait for creation to complete
                created: list[str] = []
                for topic, future in futures.items():
                    try:
                        future.result(timeout=10)
                        created.append(topic)
                    except Exception as e:
                        logger.warning(f"Failed to create topic {topic}: {e}")
                return created
            finally:
                pass  # AdminClient doesn't have a close method

        try:
            created = await asyncio.to_thread(_create_missing_topics)
            if created:
                logger.info(f"Auto-created topics: {created}")
        except Exception:
            logger.exception("Failed to auto-create topics")

    async def produce(
        self,
        topic: str,
        value: str | bytes | None = None,
        key: str | bytes | None = None,
        partition: int | None = None,
        callback: Callable[[KafkaError | None, Message], None] | None = None,
        on_delivery: Callable[[KafkaError | None, Message], None] | None = None,
        timestamp: int = 0,
        headers: dict[str, str | bytes] | list[tuple[str, str | bytes]] | None = None,
        flush: bool = False,
        flush_timeout: float | None = None,
    ) -> None:
        producer = await self.producer

        def _produce() -> None:
            # Build kwargs, only include partition if specified (None causes TypeError)
            if partition is not None:
                producer.produce(
                    topic=topic,
                    value=value,
                    key=key,
                    headers=headers,
                    timestamp=timestamp,
                    callback=callback,
                    on_delivery=on_delivery,
                    partition=partition,
                )
            else:
                producer.produce(
                    topic=topic,
                    value=value,
                    key=key,
                    headers=headers,
                    timestamp=timestamp,
                    callback=callback,
                    on_delivery=on_delivery,
                )
            if flush:
                if flush_timeout is not None:
                    producer.flush(timeout=flush_timeout)
                else:
                    producer.flush()

        await self._producer_executor.run(_produce)

    async def flush(self, timeout: float | None = None) -> None:
        producer = await self.producer

        def _flush() -> None:
            if timeout is not None:
                producer.flush(timeout=timeout)
            else:
                producer.flush()

        await self._producer_executor.run(_flush)

    async def poll(self, *, timeout: Optional[float | int] = None) -> Message | None:
        consumer = await self.consumer

        def _poll() -> Message | None:
            if timeout is not None:
                return consumer.poll(timeout)
            else:
                return consumer.poll()

        return await self._consumer_executor.run(_poll)


def create_consumer(config: ConsumerConfig, *, logger: logging.Logger | None = None) -> Consumer:
    return Consumer(dict(config), logger=logger or get_logger(__name__))


def create_producer(config: ProducerConfig, *, logger: logging.Logger | None = None) -> Producer:
    return Producer(dict(config), logger=logger or get_logger(__name__))
