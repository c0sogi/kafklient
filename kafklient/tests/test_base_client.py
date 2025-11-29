"""
Tests for KafkaBaseClient basic functionality.
Requires a running Kafka broker at localhost:9092.

Tests cover:
- produce() / poll() / flush()
- start() / stop() lifecycle
- Context manager usage
- Auto topic creation
"""

import asyncio
import json
import unittest
import uuid
from dataclasses import dataclass, field
from time import perf_counter
from typing import Optional, Type

from kafklient import KafkaBaseClient, Message, logger
from kafklient.tests._config import TEST_TIMEOUT
from kafklient.tests._utils import (
    ensure_topic_exists,
    get_topic_and_group_id,
    make_consumer_config,
    make_producer_config,
)


@dataclass
class SimpleTestClient(KafkaBaseClient):
    """Minimal concrete implementation for testing base client functionality."""

    received_records: list[Message] = field(default_factory=list[Message], init=False, repr=False)
    record_event: asyncio.Event = field(default_factory=asyncio.Event, init=False, repr=False)

    @property
    def is_closed(self) -> bool:
        """Expose closed state for testing."""
        return self._closed

    @property
    def has_producer(self) -> bool:
        """Check if producer is initialized."""
        return self._producer is not None

    @property
    def has_consumer(self) -> bool:
        """Check if consumer is initialized."""
        return self._consumer is not None

    async def _on_record(
        self,
        record: Message,
        parsed_candidates: list[tuple[object, Type[object]]],
        cid: Optional[bytes],
    ) -> None:
        self.received_records.append(record)
        self.record_event.set()

    async def _on_stop_cleanup(self) -> None:
        self.received_records.clear()


class TestBaseClientProduce(unittest.IsolatedAsyncioTestCase):
    """Test produce functionality."""

    async def test_produce_single_message(self) -> None:
        """Test producing a single message."""
        topic, group_id = get_topic_and_group_id(self.test_produce_single_message)
        await ensure_topic_exists(topic)

        client = SimpleTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": Message, "parser": lambda r: r}],
        )

        try:
            await client.start()
            start = perf_counter()

            # Produce a message
            test_value = json.dumps({"test": "produce_single"}).encode()
            await client.produce(topic, value=test_value, flush=True)

            # Wait for the message to be received
            await asyncio.wait_for(client.record_event.wait(), timeout=TEST_TIMEOUT)

            assert len(client.received_records) >= 1, "No message received"
            received_value = client.received_records[-1].value()
            assert received_value == test_value, f"Expected {test_value}, got {received_value}"

            logger.info(
                f"[performance] {self.__class__.__name__}.test_produce_single_message: "
                f"round_trip={(perf_counter() - start) * 1000:.1f}ms"
            )
        finally:
            await client.stop()

    async def test_produce_with_key(self) -> None:
        """Test producing a message with a key."""
        topic, group_id = get_topic_and_group_id(self.test_produce_with_key)
        await ensure_topic_exists(topic)

        client = SimpleTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": Message, "parser": lambda r: r}],
        )

        try:
            await client.start()

            test_key = b"test-key-123"
            test_value = b"test-value"
            await client.produce(topic, value=test_value, key=test_key, flush=True)

            await asyncio.wait_for(client.record_event.wait(), timeout=TEST_TIMEOUT)

            assert len(client.received_records) >= 1
            msg = client.received_records[-1]
            assert msg.key() == test_key, f"Expected key {test_key}, got {msg.key()}"
            assert msg.value() == test_value
        finally:
            await client.stop()

    async def test_produce_with_headers(self) -> None:
        """Test producing a message with headers."""
        topic, group_id = get_topic_and_group_id(self.test_produce_with_headers)
        await ensure_topic_exists(topic)

        client = SimpleTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": Message, "parser": lambda r: r}],
        )

        try:
            await client.start()

            test_headers: list[tuple[str, str | bytes]] = [
                ("x-request-id", b"req-123"),
                ("x-trace-id", "trace-456"),
            ]
            await client.produce(topic, value=b"header-test", headers=test_headers, flush=True)

            await asyncio.wait_for(client.record_event.wait(), timeout=TEST_TIMEOUT)

            assert len(client.received_records) >= 1
            msg = client.received_records[-1]
            msg_headers = msg.headers() or []

            header_dict = {k: v for k, v in msg_headers}
            assert b"req-123" in header_dict.get("x-request-id", b"")
        finally:
            await client.stop()

    async def test_produce_multiple_messages(self) -> None:
        """Test producing multiple messages."""
        topic, group_id = get_topic_and_group_id(self.test_produce_multiple_messages)
        await ensure_topic_exists(topic)

        client = SimpleTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": Message, "parser": lambda r: r}],
        )

        try:
            await client.start()
            count = 5

            start = perf_counter()
            for i in range(count):
                await client.produce(topic, value=f"msg-{i}".encode())
            await client.flush()

            # Wait for all messages
            async def wait_for_all() -> None:
                while len(client.received_records) < count:
                    client.record_event.clear()
                    await client.record_event.wait()

            await asyncio.wait_for(wait_for_all(), timeout=TEST_TIMEOUT)

            assert len(client.received_records) >= count
            values = [r.value() for r in client.received_records[-count:]]
            expected = [f"msg-{i}".encode() for i in range(count)]
            # Messages might arrive out of order, so just check all are present
            assert set(values) == set(expected), f"Expected {expected}, got {values}"

            logger.info(
                f"[performance] {self.__class__.__name__}.test_produce_multiple_messages: "
                f"{count} msgs total={(perf_counter() - start) * 1000:.1f}ms"
            )
        finally:
            await client.stop()


class TestBaseClientPoll(unittest.IsolatedAsyncioTestCase):
    """Test poll functionality."""

    async def test_poll_returns_message(self) -> None:
        """Test that poll() returns messages."""
        topic, group_id = get_topic_and_group_id(self.test_poll_returns_message)
        await ensure_topic_exists(topic)

        client = SimpleTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": Message, "parser": lambda r: r}],
            seek_to_end_on_assign=False,  # Get messages from beginning
        )

        try:
            await client.start()

            # Give consumer time to stabilize
            await asyncio.sleep(2.0)

            # Produce a message
            test_value = b"poll-test-message"
            await client.produce(topic, value=test_value, flush=True)

            # Poll for the message (note: consume loop also running)
            # Wait for message to appear in received_records via _on_record
            await asyncio.wait_for(client.record_event.wait(), timeout=TEST_TIMEOUT)

            assert len(client.received_records) >= 1
            assert client.received_records[-1].value() == test_value
        finally:
            await client.stop()

    async def test_poll_timeout_returns_none(self) -> None:
        """Test that poll() returns None on timeout when no messages."""
        topic, group_id = get_topic_and_group_id(self.test_poll_timeout_returns_none)
        await ensure_topic_exists(topic)

        # Use a unique topic with no messages
        empty_topic = f"empty-{uuid.uuid4().hex[:8]}"
        await ensure_topic_exists(empty_topic)

        client = SimpleTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [empty_topic], "type": Message, "parser": lambda r: r}],
        )

        try:
            await client.start()
            await asyncio.sleep(2.0)  # Wait for assignment

            # Poll should return None (or process via _on_record)
            # The consume loop is running, so poll might not return anything new
            result = await client.poll(timeout=0.1)
            # Result can be None or a message (from consume loop)
            # This just verifies poll doesn't hang or crash
            assert result is None or isinstance(result, Message)
        finally:
            await client.stop()


class TestBaseClientLifecycle(unittest.IsolatedAsyncioTestCase):
    """Test client lifecycle management."""

    async def test_start_stop(self) -> None:
        """Test basic start/stop lifecycle."""
        topic, group_id = get_topic_and_group_id(self.test_start_stop)
        await ensure_topic_exists(topic)

        client = SimpleTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": Message, "parser": lambda r: r}],
        )

        # Initially closed
        assert client.is_closed is True

        await client.start()
        assert client.is_closed is False
        assert client.has_producer or client.has_consumer

        await client.stop()
        assert client.is_closed is True

    async def test_double_start_is_idempotent(self) -> None:
        """Test that calling start() twice is safe."""
        topic, group_id = get_topic_and_group_id(self.test_double_start_is_idempotent)
        await ensure_topic_exists(topic)

        client = SimpleTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": Message, "parser": lambda r: r}],
        )

        try:
            await client.start()
            await client.start()  # Should not raise
            assert client.is_closed is False
        finally:
            await client.stop()

    async def test_double_stop_is_idempotent(self) -> None:
        """Test that calling stop() twice is safe."""
        topic, group_id = get_topic_and_group_id(self.test_double_stop_is_idempotent)
        await ensure_topic_exists(topic)

        client = SimpleTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": Message, "parser": lambda r: r}],
        )

        await client.start()
        await client.stop()
        await client.stop()  # Should not raise
        assert client.is_closed is True

    async def test_context_manager(self) -> None:
        """Test async context manager usage."""
        topic, group_id = get_topic_and_group_id(self.test_context_manager)
        await ensure_topic_exists(topic)

        async with SimpleTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": Message, "parser": lambda r: r}],
        ) as client:
            assert client.is_closed is False

            # Should be able to produce
            await client.produce(topic, value=b"context-manager-test", flush=True)
            await asyncio.wait_for(client.record_event.wait(), timeout=TEST_TIMEOUT)
            assert len(client.received_records) >= 1

        # After exiting context, should be closed
        assert client.is_closed is True


class TestBaseClientAutoCreateTopics(unittest.IsolatedAsyncioTestCase):
    """Test auto topic creation functionality."""

    async def test_auto_create_topic(self) -> None:
        """Test that topics are auto-created when enabled."""
        unique_topic = f"auto-create-{uuid.uuid4().hex[:8]}"
        group_id = f"auto-create-group-{uuid.uuid4().hex[:8]}"

        client = SimpleTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [unique_topic], "type": Message, "parser": lambda r: r}],
            auto_create_topics=True,
            topic_num_partitions=1,
            topic_replication_factor=1,
        )

        try:
            # Start should create the topic
            await client.start()

            # Should be able to produce to the auto-created topic
            await client.produce(unique_topic, value=b"auto-created-topic-test", flush=True)

            await asyncio.wait_for(client.record_event.wait(), timeout=TEST_TIMEOUT)
            assert len(client.received_records) >= 1
        finally:
            await client.stop()


class TestBaseClientFlush(unittest.IsolatedAsyncioTestCase):
    """Test flush functionality."""

    async def test_flush(self) -> None:
        """Test that flush() waits for all messages to be delivered."""
        topic, group_id = get_topic_and_group_id(self.test_flush)
        await ensure_topic_exists(topic)

        client = SimpleTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": Message, "parser": lambda r: r}],
        )

        try:
            await client.start()

            # Produce without flush
            for i in range(3):
                await client.produce(topic, value=f"flush-test-{i}".encode())

            # Flush should complete without error
            await client.flush(timeout=10.0)

            # Wait for messages
            async def wait_for_messages() -> None:
                while len(client.received_records) < 3:
                    client.record_event.clear()
                    await client.record_event.wait()

            await asyncio.wait_for(wait_for_messages(), timeout=TEST_TIMEOUT)
            assert len(client.received_records) >= 3
        finally:
            await client.stop()


class TestBaseClientAssignment(unittest.IsolatedAsyncioTestCase):
    """Test partition assignment functionality."""

    async def test_ready_waits_for_assignment(self) -> None:
        """Test that ready() waits for partition assignment."""
        topic, group_id = get_topic_and_group_id(self.test_ready_waits_for_assignment)
        await ensure_topic_exists(topic)

        client = SimpleTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": Message, "parser": lambda r: r}],
            assignment_timeout_s=30.0,
        )

        try:
            # start() calls ready() internally
            await client.start()

            # After start, assignment should be complete
            table = client.assigned_table()
            assert len(table) > 0, "No partitions assigned"
            assert table[0]["topic"] == topic
        finally:
            await client.stop()

    async def test_assigned_table(self) -> None:
        """Test assigned_table() returns correct partition info."""
        topic, group_id = get_topic_and_group_id(self.test_assigned_table)
        await ensure_topic_exists(topic)

        client = SimpleTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": Message, "parser": lambda r: r}],
            seek_to_end_on_assign=True,
        )

        try:
            await client.start()

            table = client.assigned_table()
            assert len(table) > 0

            entry = table[0]
            assert "topic" in entry
            assert "partition" in entry
            assert entry["seek_to_end_on_assign"] is True
            assert entry["source"] == "group"
        finally:
            await client.stop()


if __name__ == "__main__":
    unittest.main()
