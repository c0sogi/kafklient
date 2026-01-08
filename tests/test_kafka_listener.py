"""
Comprehensive tests for kafklient library.
Requires a running Kafka broker at 127.0.0.1:9092.

Run with: python test.py
"""

import asyncio
import json
import unittest
import uuid
from logging import getLogger
from time import perf_counter

from kafklient import KafkaListener, Message
from kafklient.types.parser import Parser
from tests._config import TEST_TIMEOUT
from tests._schema import FlagRecord, HelloRecord, IdxRecord
from tests._utils import (
    as_bool,
    as_int,
    as_str,
    get_topic_and_group_id,
    loads_json,
    make_consumer_config,
    produce_messages,
)

logger = getLogger(__name__)


class TestKafkaListener(unittest.IsolatedAsyncioTestCase):
    async def test_listener_receives_messages(self) -> None:
        """Test that KafkaListener can receive and parse messages."""
        topic, group_id = get_topic_and_group_id(self.test_listener_receives_messages)

        # Define a simple parser
        def parse_hello(rec: Message) -> HelloRecord:
            data = loads_json(rec.value())
            message = as_str(data.get("message"))
            count = as_int(data.get("count"))
            return HelloRecord(message=message, count=count)

        listener = KafkaListener(
            parsers=[Parser[HelloRecord](topics=[topic], factory=parse_hello)],
            consumer_config=make_consumer_config(group_id),
            auto_create_topics=True,
        )

        try:
            await listener.start()
            stream = await listener.subscribe(HelloRecord)

            test_data = {"message": "hello", "count": 42}
            start = perf_counter()
            await produce_messages(topic, [(None, json.dumps(test_data).encode())])

            received: HelloRecord | None = None

            async def receive() -> None:
                nonlocal received
                async for item in stream:
                    received = item
                    break

            await asyncio.wait_for(receive(), timeout=TEST_TIMEOUT)

            assert received is not None, "No message received"
            assert received.message == "hello"
            assert received.count == 42
            logger.info(
                f"[performance] {self.__class__.__name__}.test_listener_receives_messages: "
                f"propagation={(perf_counter() - start) * 1000:.1f}ms"
            )

        finally:
            await listener.stop()

    async def test_listener_multiple_messages(self) -> None:
        """Test that KafkaListener can receive multiple messages."""
        topic = self.test_listener_multiple_messages.__name__
        group_id = f"listener-multi-{uuid.uuid4().hex}"

        def parse_idx(rec: Message) -> IdxRecord:
            data = loads_json(rec.value())
            return IdxRecord(idx=as_int(data.get("idx")))

        listener = KafkaListener(
            parsers=[Parser[IdxRecord](topics=[topic], factory=parse_idx)],
            consumer_config=make_consumer_config(group_id),
            auto_create_topics=True,
        )

        try:
            await listener.start()
            stream = await listener.subscribe(IdxRecord)

            count = 5
            received: list[IdxRecord] = []
            arrival_times: list[float] = []

            async def receive_all() -> None:
                async for item in stream:
                    received.append(item)
                    arrival_times.append(perf_counter())
                    if len(received) >= count:
                        break

            produce_start_times: list[float] = []
            for i in range(count):
                produce_start_times.append(perf_counter())
                await produce_messages(topic, [(None, json.dumps({"idx": i}).encode())])

            await asyncio.wait_for(receive_all(), timeout=TEST_TIMEOUT)

            assert len(received) == count, f"Expected {count} messages, got {len(received)}"
            indices = sorted(record.idx for record in received)
            assert indices == list(range(count)), f"Expected {list(range(count))}, got {indices}"

            props = [
                (arrival_times[i] - produce_start_times[i]) * 1000
                for i in range(min(len(arrival_times), len(produce_start_times)))
            ]
            if props:
                logger.info(
                    f"[performance] {self.__class__.__name__}.test_listener_multiple_messages: "
                    f"{count} msgs avg={sum(props) / len(props):.1f}ms "
                    f"max={max(props):.1f}ms min={min(props):.1f}ms"
                )

        finally:
            await listener.stop()

    async def test_listener_context_manager(self) -> None:
        """Test that KafkaListener works as async context manager."""
        topic = self.test_listener_context_manager.__name__
        group_id = f"listener-ctx-{uuid.uuid4().hex}"

        def parse_flag(rec: Message) -> FlagRecord:
            data = loads_json(rec.value())
            return FlagRecord(test=as_bool(data.get("test")))

        async with KafkaListener(
            parsers=[Parser[FlagRecord](topics=[topic], factory=parse_flag)],
            consumer_config=make_consumer_config(group_id),
            auto_create_topics=True,
        ) as listener:
            stream = await listener.subscribe(FlagRecord)

            start = perf_counter()
            await produce_messages(topic, [(None, b'{"test": true}')])

            async def receive() -> FlagRecord | None:
                async for item in stream:
                    return item
                return None

            result = await asyncio.wait_for(receive(), timeout=TEST_TIMEOUT)
            assert result is not None, "No message received"
            assert result.test is True
            logger.info(
                f"[performance] {self.__class__.__name__}.test_listener_context_manager: "
                f"propagation={(perf_counter() - start) * 1000:.1f}ms"
            )
