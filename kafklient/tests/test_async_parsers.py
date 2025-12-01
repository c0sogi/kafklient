"""
Tests for async parser and correlation extractor edge cases.
Requires a running Kafka broker at localhost:9092.

Tests cover:
- async def parser
- lambda wrapping async parser
- functools.partial wrapping async parser
- sync parser (baseline)
- async correlation extractor
- lambda/partial wrapping async correlation extractor
- combinations of async parser + async correlation extractor
"""

import asyncio
import unittest
from dataclasses import dataclass, field
from functools import partial
from typing import Optional, Type

from kafklient import KafkaBaseClient, Message
from kafklient.tests._config import TEST_TIMEOUT
from kafklient.tests._utils import (
    get_topic_and_group_id,
    make_consumer_config,
    make_producer_config,
)


@dataclass(frozen=True)
class ParsedMessage:
    """Simple parsed message for testing."""

    content: str
    parsed_by: str


@dataclass
class AsyncParserTestClient(KafkaBaseClient):
    """Test client that tracks parsed results and correlation IDs."""

    received: list[tuple[object, Type[object], Optional[bytes]]] = field(
        default_factory=list[tuple[object, Type[object], Optional[bytes]]], init=False, repr=False
    )
    record_event: asyncio.Event = field(default_factory=asyncio.Event, init=False, repr=False)

    async def _on_record(self, record: Message, parsed: tuple[object, Type[object]], cid: Optional[bytes]) -> None:
        self.received.append((parsed[0], parsed[1], cid))
        self.record_event.set()

    async def _on_stop_cleanup(self) -> None:
        self.received.clear()


# ============== Sync/Async Parser Functions ==============


def sync_parser(record: Message) -> ParsedMessage:
    """Sync parser function."""
    value = record.value() or b""
    return ParsedMessage(content=value.decode(), parsed_by="sync_parser")


async def async_parser(record: Message) -> ParsedMessage:
    """Async parser function with simulated async operation."""
    await asyncio.sleep(0.001)  # Simulate async I/O
    value = record.value() or b""
    return ParsedMessage(content=value.decode(), parsed_by="async_parser")


async def async_parser_with_arg(record: Message, extra: str) -> ParsedMessage:
    """Async parser with extra argument (for partial testing)."""
    await asyncio.sleep(0.001)
    value = record.value() or b""
    return ParsedMessage(content=value.decode(), parsed_by=f"async_parser_{extra}")


# ============== Sync/Async Correlation Functions ==============


def sync_corr(record: Message, parsed: object) -> bytes | None:
    """Sync correlation extractor."""
    return b"sync_corr_id"


async def async_corr(record: Message, parsed: object) -> bytes | None:
    """Async correlation extractor."""
    await asyncio.sleep(0.001)
    return b"async_corr_id"


async def async_corr_with_arg(record: Message, parsed: object, prefix: str) -> bytes | None:
    """Async correlation extractor with extra argument (for partial testing)."""
    await asyncio.sleep(0.001)
    return f"{prefix}_corr_id".encode()


# ============== Tests ==============


class TestAsyncParsers(unittest.IsolatedAsyncioTestCase):
    """Test async parser functionality."""

    async def test_sync_parser(self) -> None:
        """Baseline: sync parser should work normally."""
        topic, group_id = get_topic_and_group_id(self.test_sync_parser)
        client = AsyncParserTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": ParsedMessage, "parser": sync_parser}],
            auto_create_topics=True,
        )

        try:
            await client.start()
            await client.produce(topic, value=b"test_sync", flush=True)
            await asyncio.wait_for(client.record_event.wait(), timeout=TEST_TIMEOUT)

            assert len(client.received) >= 1
            parsed, ptype, _ = client.received[-1]
            assert isinstance(parsed, ParsedMessage)
            assert parsed.content == "test_sync"
            assert parsed.parsed_by == "sync_parser"
            assert ptype is ParsedMessage
        finally:
            await client.stop()

    async def test_async_def_parser(self) -> None:
        """Test async def parser is awaited correctly."""
        topic, group_id = get_topic_and_group_id(self.test_async_def_parser)
        client = AsyncParserTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": ParsedMessage, "parser": async_parser}],
            auto_create_topics=True,
        )

        try:
            await client.start()
            await client.produce(topic, value=b"test_async", flush=True)
            await asyncio.wait_for(client.record_event.wait(), timeout=TEST_TIMEOUT)

            assert len(client.received) >= 1
            parsed, _, _ = client.received[-1]
            assert isinstance(parsed, ParsedMessage)
            assert parsed.content == "test_async"
            assert parsed.parsed_by == "async_parser"
        finally:
            await client.stop()

    async def test_lambda_wrapping_async_parser(self) -> None:
        """Test lambda wrapping async parser is awaited correctly."""
        topic, group_id = get_topic_and_group_id(self.test_lambda_wrapping_async_parser)
        client = AsyncParserTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": ParsedMessage, "parser": lambda r: async_parser(r)}],
            auto_create_topics=True,
        )

        try:
            await client.start()
            await client.produce(topic, value=b"test_lambda_async", flush=True)
            await asyncio.wait_for(client.record_event.wait(), timeout=TEST_TIMEOUT)

            assert len(client.received) >= 1
            parsed, _, _ = client.received[-1]
            assert isinstance(parsed, ParsedMessage)
            assert parsed.content == "test_lambda_async"
            assert parsed.parsed_by == "async_parser"
        finally:
            await client.stop()

    async def test_partial_wrapping_async_parser(self) -> None:
        """Test functools.partial wrapping async parser is awaited correctly."""
        topic, group_id = get_topic_and_group_id(self.test_partial_wrapping_async_parser)
        client = AsyncParserTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[
                {"topics": [topic], "type": ParsedMessage, "parser": partial(async_parser_with_arg, extra="partial")}
            ],
            auto_create_topics=True,
        )

        try:
            await client.start()
            await client.produce(topic, value=b"test_partial_async", flush=True)
            await asyncio.wait_for(client.record_event.wait(), timeout=TEST_TIMEOUT)

            assert len(client.received) >= 1
            parsed, _, _ = client.received[-1]
            assert isinstance(parsed, ParsedMessage)
            assert parsed.content == "test_partial_async"
            assert parsed.parsed_by == "async_parser_partial"
        finally:
            await client.stop()


class TestAsyncCorrelationExtractor(unittest.IsolatedAsyncioTestCase):
    """Test async correlation extractor functionality."""

    async def test_sync_correlation_extractor(self) -> None:
        """Baseline: sync correlation extractor should work normally."""
        topic, group_id = get_topic_and_group_id(self.test_sync_correlation_extractor)
        client = AsyncParserTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": ParsedMessage, "parser": sync_parser}],
            corr_from_record=sync_corr,
            auto_create_topics=True,
        )

        try:
            await client.start()
            await client.produce(topic, value=b"test_sync_corr", flush=True)
            await asyncio.wait_for(client.record_event.wait(), timeout=TEST_TIMEOUT)

            assert len(client.received) >= 1
            _, _, cid = client.received[-1]
            assert cid == b"sync_corr_id"
        finally:
            await client.stop()

    async def test_async_def_correlation_extractor(self) -> None:
        """Test async def correlation extractor is awaited correctly."""
        topic, group_id = get_topic_and_group_id(self.test_async_def_correlation_extractor)
        client = AsyncParserTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": ParsedMessage, "parser": sync_parser}],
            corr_from_record=async_corr,
            auto_create_topics=True,
        )

        try:
            await client.start()
            await client.produce(topic, value=b"test_async_corr", flush=True)
            await asyncio.wait_for(client.record_event.wait(), timeout=TEST_TIMEOUT)

            assert len(client.received) >= 1
            _, _, cid = client.received[-1]
            assert cid == b"async_corr_id"
        finally:
            await client.stop()

    async def test_lambda_wrapping_async_correlation_extractor(self) -> None:
        """Test lambda wrapping async correlation extractor is awaited correctly."""
        topic, group_id = get_topic_and_group_id(self.test_lambda_wrapping_async_correlation_extractor)
        client = AsyncParserTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": ParsedMessage, "parser": sync_parser}],
            corr_from_record=lambda r, p: async_corr(r, p),
            auto_create_topics=True,
        )

        try:
            await client.start()
            await client.produce(topic, value=b"test_lambda_corr", flush=True)
            await asyncio.wait_for(client.record_event.wait(), timeout=TEST_TIMEOUT)

            assert len(client.received) >= 1
            _, _, cid = client.received[-1]
            assert cid == b"async_corr_id"
        finally:
            await client.stop()

    async def test_partial_wrapping_async_correlation_extractor(self) -> None:
        """Test functools.partial wrapping async correlation extractor is awaited correctly."""
        topic, group_id = get_topic_and_group_id(self.test_partial_wrapping_async_correlation_extractor)
        client = AsyncParserTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": ParsedMessage, "parser": sync_parser}],
            corr_from_record=partial(async_corr_with_arg, prefix="partial"),
            auto_create_topics=True,
        )

        try:
            await client.start()
            await client.produce(topic, value=b"test_partial_corr", flush=True)
            await asyncio.wait_for(client.record_event.wait(), timeout=TEST_TIMEOUT)

            assert len(client.received) >= 1
            _, _, cid = client.received[-1]
            assert cid == b"partial_corr_id"
        finally:
            await client.stop()


class TestAsyncParserAndCorrelationCombinations(unittest.IsolatedAsyncioTestCase):
    """Test combinations of async parser and async correlation extractor."""

    async def test_async_parser_with_async_corr(self) -> None:
        """Test async parser combined with async correlation extractor."""
        topic, group_id = get_topic_and_group_id(self.test_async_parser_with_async_corr)
        client = AsyncParserTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": ParsedMessage, "parser": async_parser}],
            corr_from_record=async_corr,
            auto_create_topics=True,
        )

        try:
            await client.start()
            await client.produce(topic, value=b"test_both_async", flush=True)
            await asyncio.wait_for(client.record_event.wait(), timeout=TEST_TIMEOUT)

            assert len(client.received) >= 1
            parsed, _, cid = client.received[-1]
            assert isinstance(parsed, ParsedMessage)
            assert parsed.content == "test_both_async"
            assert parsed.parsed_by == "async_parser"
            assert cid == b"async_corr_id"
        finally:
            await client.stop()

    async def test_lambda_parser_with_partial_corr(self) -> None:
        """Test lambda async parser combined with partial async correlation extractor."""
        topic, group_id = get_topic_and_group_id(self.test_lambda_parser_with_partial_corr)
        client = AsyncParserTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": ParsedMessage, "parser": lambda r: async_parser(r)}],
            corr_from_record=partial(async_corr_with_arg, prefix="combo"),
            auto_create_topics=True,
        )

        try:
            await client.start()
            await client.produce(topic, value=b"test_combo", flush=True)
            await asyncio.wait_for(client.record_event.wait(), timeout=TEST_TIMEOUT)

            assert len(client.received) >= 1
            parsed, _, cid = client.received[-1]
            assert isinstance(parsed, ParsedMessage)
            assert parsed.content == "test_combo"
            assert parsed.parsed_by == "async_parser"
            assert cid == b"combo_corr_id"
        finally:
            await client.stop()

    async def test_partial_parser_with_lambda_corr(self) -> None:
        """Test partial async parser combined with lambda async correlation extractor."""
        topic, group_id = get_topic_and_group_id(self.test_partial_parser_with_lambda_corr)
        client = AsyncParserTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[
                {"topics": [topic], "type": ParsedMessage, "parser": partial(async_parser_with_arg, extra="mixed")}
            ],
            corr_from_record=lambda r, p: async_corr(r, p),
            auto_create_topics=True,
        )

        try:
            await client.start()
            await client.produce(topic, value=b"test_mixed", flush=True)
            await asyncio.wait_for(client.record_event.wait(), timeout=TEST_TIMEOUT)

            assert len(client.received) >= 1
            parsed, _, cid = client.received[-1]
            assert isinstance(parsed, ParsedMessage)
            assert parsed.content == "test_mixed"
            assert parsed.parsed_by == "async_parser_mixed"
            assert cid == b"async_corr_id"
        finally:
            await client.stop()


class TestEdgeCases(unittest.IsolatedAsyncioTestCase):
    """Test edge cases for async parsers and correlation extractors."""

    async def test_parser_returning_none(self) -> None:
        """Test parser that may return None-ish values."""
        topic, group_id = get_topic_and_group_id(self.test_parser_returning_none)

        async def nullable_parser(record: Message) -> str | None:
            await asyncio.sleep(0.001)
            value = record.value()
            if value == b"empty":
                return None
            return (value or b"").decode()

        client = AsyncParserTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": str, "parser": nullable_parser}],
            auto_create_topics=True,
        )

        try:
            await client.start()
            await client.produce(topic, value=b"not_empty", flush=True)
            await asyncio.wait_for(client.record_event.wait(), timeout=TEST_TIMEOUT)

            assert len(client.received) >= 1
            parsed, _, _ = client.received[-1]
            assert parsed == "not_empty"
        finally:
            await client.stop()

    async def test_corr_returning_none(self) -> None:
        """Test correlation extractor that returns None."""
        topic, group_id = get_topic_and_group_id(self.test_corr_returning_none)

        async def nullable_corr(record: Message, parsed: object) -> bytes | None:
            await asyncio.sleep(0.001)
            return None  # Always return None

        client = AsyncParserTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[{"topics": [topic], "type": ParsedMessage, "parser": sync_parser}],
            corr_from_record=nullable_corr,
            auto_create_topics=True,
        )

        try:
            await client.start()
            await client.produce(topic, value=b"test_null_corr", flush=True)
            await asyncio.wait_for(client.record_event.wait(), timeout=TEST_TIMEOUT)

            assert len(client.received) >= 1
            _, _, cid = client.received[-1]
            assert cid is None
        finally:
            await client.stop()

    async def test_multiple_parsers_mixed_sync_async(self) -> None:
        """Test multiple parsers with mixed sync and async parsers on same topic."""
        topic, group_id = get_topic_and_group_id(self.test_multiple_parsers_mixed_sync_async)

        # Two parsers on same topic: one sync, one async
        client = AsyncParserTestClient(
            producer_config=make_producer_config(),
            consumer_config=make_consumer_config(group_id),
            parsers=[
                {"topics": [topic], "type": ParsedMessage, "parser": sync_parser},
                {"topics": [topic], "type": ParsedMessage, "parser": async_parser},
            ],
            auto_create_topics=True,
        )

        try:
            await client.start()
            await client.produce(topic, value=b"test_multi", flush=True)

            # Wait for both parsers to process
            async def wait_for_both() -> None:
                while len(client.received) < 2:
                    client.record_event.clear()
                    await client.record_event.wait()

            await asyncio.wait_for(wait_for_both(), timeout=TEST_TIMEOUT)

            assert len(client.received) >= 2
            parsed_by_list = [r[0].parsed_by for r in client.received[-2:] if isinstance(r[0], ParsedMessage)]
            assert "sync_parser" in parsed_by_list
            assert "async_parser" in parsed_by_list
        finally:
            await client.stop()


if __name__ == "__main__":
    unittest.main()
