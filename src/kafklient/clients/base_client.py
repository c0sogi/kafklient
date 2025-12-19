import asyncio
import inspect
import logging
import threading
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from functools import partial
from types import TracebackType
from typing import (
    AsyncIterable,
    Callable,
    Iterable,
    Literal,
    Optional,
    Protocol,
    Self,
    Type,
    TypedDict,
    final,
)

from .._logging import get_logger
from ..types import (
    KAFKA_ERROR_PARTITION_EOF,
    OFFSET_END,
    AdminClient,
    Consumer,
    ConsumerConfig,
    CorrelationCallback,
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


class AssignedTable(TypedDict):
    topic: str
    partition: int
    offset: int
    metadata: Optional[str]
    leader_epoch: Optional[int]
    error: Optional[KafkaError]
    seek_to_end_on_assign: bool


class PartitionListener(Protocol):
    async def on_partitions_assigned(self, partitions: list[TopicPartition]) -> None: ...
    async def on_partitions_revoked(self, partitions: list[TopicPartition]) -> None: ...
    async def on_partitions_lost(self, partitions: list[TopicPartition]) -> None: ...


def default_producer_config() -> ProducerConfig:
    return {"bootstrap.servers": "127.0.0.1:9092"}


def default_consumer_config() -> ConsumerConfig:
    return {"bootstrap.servers": "127.0.0.1:9092", "auto.offset.reset": "latest"}


def default_corr_from_record(rec: Message, parsed: object) -> Optional[bytes]:
    def to_bytes(v: str | bytes | None) -> bytes | None:
        if v is None:
            return None
        if isinstance(v, str):
            return v.encode("utf-8")
        return v

    if key := rec.key():
        return bytes(key)
    return next((to_bytes(v) for k, v in rec.headers() or () if k.lower() == "x-corr-id"), None)


def create_consumer(config: ConsumerConfig) -> Consumer:
    config = config.copy()
    if "group.id" not in config or not config["group.id"]:
        config["group.id"] = f"consumer-{uuid.uuid4().hex[:8]}"
    if "bootstrap.servers" not in config or not config["bootstrap.servers"]:
        raise ValueError("bootstrap.servers is required")
    return Consumer(config)  # pyright: ignore[reportArgumentType]


def create_producer(config: ProducerConfig) -> Producer:
    config = config.copy()
    if "bootstrap.servers" not in config or not config["bootstrap.servers"]:
        raise ValueError("bootstrap.servers is required")
    if "client.id" not in config or not config["client.id"]:
        config["client.id"] = f"producer-{uuid.uuid4().hex[:8]}"
    return Producer(config)  # pyright: ignore[reportArgumentType]


@dataclass(kw_only=True)
class KafkaBaseClient(ABC):
    """
    Group-managed subscription mode powered by sync Consumer/Producer
    """

    # Main Configuration
    parsers: Iterable[ParserSpec[object]]
    """List of parser specs by topic. Each spec contains:
    - topics: list of topics to parse
    - type: type of the parsed object
    - parser: parser callback (sync or async)
    """
    corr_from_record: CorrelationCallback = field(default=default_corr_from_record)
    """Correlation key extractor: (record, parsed) -> correlation id (None if not found). Can be sync or async."""
    producer_config: ProducerConfig = field(default_factory=default_producer_config)
    """Producer configuration."""
    consumer_config: ConsumerConfig = field(default_factory=default_consumer_config)
    """Consumer configuration."""

    # Behavior
    seek_to_end_on_assign: bool = True
    """Seek to end on partition assignment."""
    metadata_refresh_min_interval_s: float = 5.0
    """Minimum interval for metadata refresh."""
    backoff_min: float = 0.5
    """Minimum backoff time."""
    backoff_max: float = 10.0
    """Maximum backoff time."""
    backoff_factor: float = 2.0
    """Backoff factor."""
    assignment_timeout_s: float = 5.0
    """Assignment timeout."""
    rebalance_listener: Optional[PartitionListener] = None
    """Rebalance listener."""

    # Auto Topic Creation
    auto_create_topics: bool = False
    """Automatically create topics if they don't exist before subscribing."""
    topic_num_partitions: int = 1
    """Number of partitions for auto-created topics."""
    topic_replication_factor: int = 1
    """Replication factor for auto-created topics."""
    topic_create_timeout: float = 10.0
    """Timeout for topic creation."""
    polling_timeout: float = 0.5
    """Timeout for polling."""

    # Internal State
    _loop: Optional[asyncio.AbstractEventLoop] = field(default=None, init=False, repr=False)
    _producer: Optional[Producer] = field(default=None, init=False, repr=False)
    _consumer: Optional[Consumer] = field(default=None, init=False, repr=False)
    _consumer_task: Optional[asyncio.Task[None]] = field(default=None, init=False, repr=False)
    _assigned_partitions: list[TopicPartition] = field(default_factory=list[TopicPartition], init=False, repr=False)
    _assignment_event: asyncio.Event = field(default_factory=asyncio.Event, init=False, repr=False)
    _start_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)
    _stop_event: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _pending_seek_partitions: list[TopicPartition] = field(default_factory=list[TopicPartition], init=False, repr=False)
    _consumer_executor: DedicatedThreadExecutor = field(default_factory=DedicatedThreadExecutor, init=False, repr=False)
    _producer_executor: DedicatedThreadExecutor = field(default_factory=DedicatedThreadExecutor, init=False, repr=False)

    # ---------- Lifecycle ----------

    @property
    def closed(self) -> bool:
        if self._loop is None or self._loop.is_closed():
            return True
        if self._consumer_task is None or self._consumer_task.done():
            return True
        if not self._consumer_executor.is_running or not self._producer_executor.is_running:
            return True
        return False

    async def ready(self, *, timeout: float | None = None) -> None:
        """Ensure the consumer has an assignment before proceeding.
        Raises TimeoutError if the assignment is not received within the timeout.
        """
        if not self.subscribed_topics:
            return
        await self.consumer
        if timeout is None:
            timeout = self.assignment_timeout_s
        await asyncio.wait_for(self._assignment_event.wait(), timeout=timeout)

    @final
    async def start(self, *, timeout: float | None = None) -> None:
        if not self.closed:
            return

        self._loop = asyncio.get_running_loop()
        self._stop_event.clear()
        self._consumer_executor.start(self._loop, name=f"{self.__class__.__name__}-consumer")
        self._producer_executor.start(self._loop, name=f"{self.__class__.__name__}-producer")

        # Auto-create topics if enabled
        if self.auto_create_topics and self.subscribed_topics:
            await self.create_topics(
                *self.subscribed_topics,
                timeout=self.topic_create_timeout,
                num_partitions=self.topic_num_partitions,
                replication_factor=self.topic_replication_factor,
            )

        await self.ready(timeout=timeout)
        logger.info(f"{self.__class__.__name__} started")

    async def stop(self) -> None:
        if self.closed:
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

    @property
    def producer_factory(self) -> Callable[[], Producer]:
        producer_config = self.producer_config.copy()
        if "bootstrap.servers" not in producer_config or not producer_config["bootstrap.servers"]:
            raise ValueError("bootstrap.servers is required for producer factory")

        return partial(create_producer, producer_config)

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

        return partial(create_consumer, consumer_config)

    @property
    async def consumer(self) -> Consumer:
        async with self._start_lock:
            if self._consumer is not None:
                return self._consumer
            self._consumer = await self._consumer_executor.run(self.consumer_factory)

            async def _notify_rebalance_listener(
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

            # Sync callbacks that schedule async handlers (runs on dedicated thread)
            def _on_assign(consumer: Consumer, partitions: list[TopicPartition]) -> None:
                async def _handle_assign(partitions: list[TopicPartition]) -> None:
                    """Handle partition assignment notification (seek is done in consume loop)"""
                    await _notify_rebalance_listener("on_partitions_assigned", partitions)
                    # If seek_to_end_on_assign is False, set event immediately
                    # Otherwise, _consume_loop will set it after seek completes
                    if not self.seek_to_end_on_assign and partitions:
                        self._assignment_event.set()

                self._assigned_partitions = sorted(partitions, key=self._tp_sort_key)
                # Store partitions for seek in consume loop (seek fails if called during callback)
                if self.seek_to_end_on_assign and partitions:
                    self._pending_seek_partitions = list(partitions)
                if self._loop and not self._stop_event.is_set():
                    asyncio.run_coroutine_threadsafe(_handle_assign(partitions), self._loop)

            def _on_revoke(consumer: Consumer, partitions: list[TopicPartition]) -> None:
                revoked = {(tp.topic, tp.partition) for tp in partitions}
                self._assigned_partitions = [
                    tp for tp in self._assigned_partitions if (tp.topic, tp.partition) not in revoked
                ]
                if partitions:
                    self._assignment_event.clear()
                if self._loop and not self._stop_event.is_set():
                    asyncio.run_coroutine_threadsafe(
                        _notify_rebalance_listener("on_partitions_revoked", partitions),
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
                        _notify_rebalance_listener("on_partitions_lost", partitions),
                        self._loop,
                    )

            async def _consume_loop() -> None:
                backoff = self.backoff_min

                def _poll_and_maybe_seek() -> tuple[Message | None, bool]:
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
                        self._consumer.poll(self.polling_timeout)
                        did_seek = True

                    return self._consumer.poll(self.polling_timeout), did_seek

                # Parsing + Dispatching
                async def _parse_record(
                    record: Message,
                ) -> AsyncIterable[tuple[tuple[object, Type[object]], Optional[bytes]]]:
                    yielded = False
                    for spec in self.parsers_by_topic.get((topic := record.topic()) or "") or ():
                        try:
                            # Support async parsers: await if result is awaitable
                            result: object = spec["parser"](record)
                            if inspect.isawaitable(result):
                                result = await result
                            # Support async correlation extractors: await if result is awaitable
                            cid_or_awaitable = self.corr_from_record(record, result)
                            cid: bytes | None = (
                                await cid_or_awaitable if inspect.isawaitable(cid_or_awaitable) else cid_or_awaitable
                            )
                            yield (result, spec["type"]), cid
                            yielded = True
                        except Exception as ex:
                            logger.debug(
                                f"Parser failed (topic={topic}, out={getattr(spec['type'], '__name__', spec['type'])}): {ex}"
                            )
                    # Fallback: yield raw Message if no parser matched or all parsers failed
                    if not yielded:
                        cid_or_awaitable = self.corr_from_record(record, record)
                        cid = await cid_or_awaitable if inspect.isawaitable(cid_or_awaitable) else cid_or_awaitable
                        yield (record, Message), cid

                try:
                    while not self._stop_event.is_set():
                        try:
                            if self._consumer is None:
                                break
                            record, did_seek = await self._consumer_executor.run(_poll_and_maybe_seek)
                            if did_seek:
                                # Signal assignment is complete after seek
                                self._assignment_event.set()
                            if self._stop_event.is_set():
                                break
                            if record is None:
                                continue
                            err = record.error()
                            if err:
                                if err.code() == KAFKA_ERROR_PARTITION_EOF:
                                    continue
                                logger.error(f"Kafka error: {err}")
                                continue
                            # parsed_candidates, cid = await _parse_record(rec)
                            async for parsed, cid in _parse_record(record):
                                try:
                                    await self._on_record(record, parsed, cid)
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

            if self.subscribed_topics:
                try:
                    consumer = self._consumer

                    def subscribe() -> None:
                        consumer.subscribe(
                            sorted(self.subscribed_topics),
                            on_assign=_on_assign,
                            on_revoke=_on_revoke,
                            on_lost=_on_lost,
                        )

                    await self._consumer_executor.run(subscribe)
                except Exception:
                    logger.exception("Failed to subscribe to topics")
                    raise

            self._consumer_task = asyncio.create_task(_consume_loop(), name=f"{self.__class__.__name__}_loop")
            return self._consumer

    @staticmethod
    def _tp_sort_key(tp: TopicPartition) -> tuple[str, int]:
        return (tp.topic, tp.partition)

    @abstractmethod
    async def _on_record(self, record: Message, parsed: tuple[object, Type[object]], cid: Optional[bytes]) -> None: ...

    @abstractmethod
    async def _on_stop_cleanup(self) -> None: ...

    async def create_topics(
        self, *topics: str, timeout: float = 10.0, num_partitions: int = 1, replication_factor: int = 1
    ) -> None:
        """Check if topics exist and create them if they don't."""
        if not self.bootstrap_servers:
            logger.warning("Cannot auto-create topics: bootstrap.servers not available")
            return

        def _create_missing_topics() -> list[str]:
            admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
            try:
                # Get existing topics
                metadata = admin.list_topics(timeout=timeout)
                existing_topics = set(metadata.topics.keys())

                # Find missing topics
                missing_topics = [t for t in topics if t not in existing_topics]
                if not missing_topics:
                    return []

                # Create missing topics
                new_topics = [
                    NewTopic(
                        topic,
                        num_partitions=num_partitions,
                        replication_factor=replication_factor,
                    )
                    for topic in missing_topics
                ]

                # Wait for creation to complete
                created: list[str] = []
                for topic, future in admin.create_topics(new_topics).items():  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                    try:
                        future.result(timeout=timeout)
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

    async def delete_all_topics(self, *, timeout: float = 10.0, exclude_system_topics: bool = True) -> list[str]:
        """Delete all topics in the Kafka cluster.

        Args:
            timeout: Timeout in seconds for deletion operations. Defaults to 10.0.
            exclude_system_topics: If True (default), excludes system topics like
                `__consumer_offsets`, `__transaction_state`, `_confluent-*`, etc.
                Set to False to delete all topics including system ones.

        Returns:
            List of successfully deleted topic names.

        Raises:
            ValueError: If bootstrap.servers is not configured.
        """
        if not self.bootstrap_servers:
            raise ValueError("Cannot delete topics: bootstrap.servers not available")

        def _is_system_topic(topic: str) -> bool:
            """Check if a topic is a system topic that should be protected."""
            # Kafka internal topics (double underscore prefix)
            if topic.startswith("__"):
                return True
            # Confluent Control Center topics (single underscore + confluent prefix)
            if topic.startswith("_confluent-"):
                return True
            # Other common system topic patterns
            if topic.startswith("_") and any(
                pattern in topic.lower() for pattern in ["schema", "connect", "ksql", "control"]
            ):
                return True
            return False

        def _delete_all_topics() -> list[str]:
            admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
            try:
                # Get all existing topics
                metadata = admin.list_topics(timeout=int(timeout))
                all_topics = list(metadata.topics.keys())

                # Filter out system topics if requested
                if exclude_system_topics:
                    topics_to_delete = [t for t in all_topics if not _is_system_topic(t)]
                    excluded = [t for t in all_topics if _is_system_topic(t)]
                    if excluded:
                        logger.info(f"Excluding system topics from deletion: {excluded}")
                else:
                    topics_to_delete = all_topics

                if not topics_to_delete:
                    logger.info("No topics to delete")
                    return []

                # Delete all topics
                # Wait for deletion to complete
                deleted: list[str] = []
                for topic, future in admin.delete_topics(topics_to_delete, operation_timeout=int(timeout)).items():  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                    try:
                        future.result(timeout=timeout)
                        deleted.append(topic)
                    except Exception as e:
                        logger.warning(f"Failed to delete topic {topic}: {e}")
                return deleted
            finally:
                pass  # AdminClient doesn't have a close method

        try:
            deleted = await asyncio.to_thread(_delete_all_topics)
            if deleted:
                logger.info(f"Deleted topics: {deleted}")
            return deleted
        except Exception:
            logger.exception("Failed to delete topics")
            raise

    async def produce(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        partition: int | None = None,
        callback: Callable[[KafkaError | None, Message], None] | None = None,
        on_delivery: Callable[[KafkaError | None, Message], None] | None = None,
        timestamp: int = 0,
        headers: dict[str, str | bytes | None] | list[tuple[str, str | bytes | None]] | None = None,
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

    @property
    def subscribed_topics(self) -> set[str]:
        subscribed_topics: set[str] = set()
        for ps in self.parsers:
            # Build topic index and subscription topics (ignore explicit partitioning)
            for topic in ps["topics"]:
                subscribed_topics.add(topic)
        return subscribed_topics

    @property
    def parsers_by_topic(self) -> dict[str, list[ParserSpec[object]]]:
        parsers_by_topic: dict[str, list[ParserSpec[object]]] = {}
        for ps in self.parsers:
            for topic in ps["topics"]:
                parsers_by_topic.setdefault(topic, []).append(ps)
        return parsers_by_topic

    @property
    def bootstrap_servers(self) -> str:
        if "bootstrap.servers" not in self.producer_config or not self.producer_config["bootstrap.servers"]:
            raise ValueError("bootstrap.servers is required")
        return self.producer_config["bootstrap.servers"]

    @property
    def assigned_table(self) -> list[AssignedTable]:
        return [
            AssignedTable(
                topic=tp.topic,
                partition=tp.partition,
                offset=tp.offset,
                metadata=tp.metadata,
                leader_epoch=tp.leader_epoch,
                error=tp.error,
                seek_to_end_on_assign=self.seek_to_end_on_assign,
            )
            for tp in sorted(self._assigned_partitions, key=self._tp_sort_key)
        ]
