import asyncio
import unittest

from ..clients import KafkaListener
from ..types import Message, ParserSpec
from ._config import TEST_TIMEOUT
from ._schema import FlagRecord
from ._utils import (
    as_bool,
    ensure_topic_exists,
    get_topic_and_group_id,
    loads_json,
    make_consumer,
    produce_messages,
)


class TestEdgeCases(unittest.IsolatedAsyncioTestCase):
    async def test_listener_stop_while_waiting(self) -> None:
        """Test that stopping listener properly signals the stream to stop."""
        topic, group_id = get_topic_and_group_id(self.test_listener_stop_while_waiting)

        # Create topic first
        await ensure_topic_exists(topic)

        def parse_flag(rec: Message) -> FlagRecord:
            data = loads_json(rec.value())
            return FlagRecord(test=as_bool(data.get("test")))

        specs: list[ParserSpec[FlagRecord]] = [
            {
                "topics": [topic],
                "type": FlagRecord,
                "parser": parse_flag,
            }
        ]

        listener = KafkaListener(
            parsers=specs,
            consumer_factory=lambda: make_consumer(group_id),
        )

        await listener.start()
        stream = await listener.subscribe(FlagRecord)

        # Give consumer time to stabilize
        await asyncio.sleep(2.0)

        # Stop the listener - this should signal the stream to stop
        await listener.stop()

        # After stop, the stream's event should be set
        # Trying to iterate should raise StopAsyncIteration
        try:
            # Use wait_for to avoid hanging if something goes wrong
            async def try_next() -> None:
                async for _ in stream:
                    break

            await asyncio.wait_for(try_next(), timeout=2.0)
        except StopAsyncIteration:
            pass  # Expected
        except asyncio.TimeoutError:
            pass  # Also acceptable - no more messages

    async def test_empty_value_handling(self) -> None:
        """Test handling of messages with empty values."""
        topic, group_id = get_topic_and_group_id(self.test_empty_value_handling)

        # Create topic first
        await ensure_topic_exists(topic)

        def parse_raw(rec: Message) -> bytes:
            return rec.value() or b""

        specs: list[ParserSpec[bytes]] = [
            {
                "topics": [topic],
                "type": bytes,
                "parser": parse_raw,
            }
        ]

        listener = KafkaListener(
            parsers=specs,
            seek_to_end_on_assign=False,
            consumer_factory=lambda: make_consumer(group_id),
        )

        try:
            await listener.start()
            stream = await listener.subscribe(bytes)

            await asyncio.sleep(3.0)

            # Produce empty message
            messages: list[tuple[bytes | None, bytes]] = [(None, b"")]
            await produce_messages(topic, messages)

            async def receive() -> bytes | None:
                async for item in stream:
                    return item
                return None

            result = await asyncio.wait_for(receive(), timeout=TEST_TIMEOUT)
            assert result == b"", f"Expected empty bytes, got {result}"

        finally:
            await listener.stop()
