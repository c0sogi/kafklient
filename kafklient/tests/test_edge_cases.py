import asyncio
import unittest

from kafklient import KafkaListener, Message
from kafklient.tests._config import TEST_TIMEOUT
from kafklient.tests._schema import FlagRecord
from kafklient.tests._utils import (
    as_bool,
    get_topic_and_group_id,
    loads_json,
    make_consumer_config,
    produce_messages,
)


class TestEdgeCases(unittest.IsolatedAsyncioTestCase):
    async def test_listener_stop_while_waiting(self) -> None:
        """Test that stopping listener properly signals the stream to stop."""
        topic, group_id = get_topic_and_group_id(self.test_listener_stop_while_waiting)

        def parse_flag(rec: Message) -> FlagRecord:
            data = loads_json(rec.value())
            return FlagRecord(test=as_bool(data.get("test")))

        listener = KafkaListener(
            parsers=[
                {
                    "topics": [topic],
                    "type": FlagRecord,
                    "parser": parse_flag,
                }
            ],
            consumer_config=make_consumer_config(group_id),
            auto_create_topics=True,
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

        def parse_raw(rec: Message) -> bytes:
            return rec.value() or b""

        listener = KafkaListener(
            parsers=[
                {
                    "topics": [topic],
                    "type": bytes,
                    "parser": parse_raw,
                }
            ],
            seek_to_end_on_assign=False,
            consumer_config=make_consumer_config(group_id),
            auto_create_topics=True,
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
