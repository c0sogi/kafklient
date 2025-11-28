import asyncio
import json
import unittest
from time import perf_counter
from typing import cast

from .. import logger
from ..clients import KafkaRPC
from ..types import Message, ParserSpec
from ._config import TEST_TIMEOUT
from ._schema import EchoPayload, RPCResponsePayload
from ._utils import (
    as_int,
    as_str,
    ensure_topic_exists,
    get_topic_and_group_id,
    loads_json,
    make_consumer,
    make_producer,
    make_ready_consumer,
)


class TestKafkaRPC(unittest.IsolatedAsyncioTestCase):
    async def test_rpc_request_response(self) -> None:
        """Test basic RPC request/response pattern."""
        request_topic, client_group = get_topic_and_group_id(self.test_rpc_request_response, suffix="request")
        reply_topic, server_group = get_topic_and_group_id(self.test_rpc_request_response, suffix="reply")

        await ensure_topic_exists(request_topic)
        await ensure_topic_exists(reply_topic)

        def parse_reply(rec: Message) -> bytes:
            return rec.value() or b""

        specs: list[ParserSpec[bytes]] = [{"topics": [reply_topic], "type": bytes, "parser": parse_reply}]

        rpc = KafkaRPC(
            parsers=specs,
            producer_factory=make_producer,
            consumer_factory=lambda: make_consumer(client_group),
        )

        server_ready = asyncio.Event()
        server_stop = asyncio.Event()

        async def echo_server() -> None:
            consumer = await asyncio.to_thread(make_ready_consumer, server_group, [request_topic])
            producer = make_producer()
            server_ready.set()

            def poll_one() -> Message | None:
                return consumer.poll(0.1)

            while not server_stop.is_set():
                msg = await asyncio.to_thread(poll_one)
                if msg is None or msg.error():
                    continue

                headers = msg.headers() or []
                reply_to: str | None = None
                corr_id: bytes | None = None
                for k, v in headers:
                    if k == "x-reply-topic":
                        reply_to = v.decode()
                    if k.lower() == "request_id":
                        corr_id = v

                if reply_to:
                    reply_headers: list[tuple[str, str | bytes]] = [("request_id", corr_id)] if corr_id else []
                    reply_value = msg.value() or b""
                    reply_topic_name = reply_to

                    def send() -> None:
                        producer.produce(
                            reply_topic_name,
                            value=reply_value,
                            headers=reply_headers,
                        )
                        producer.flush()

                    await asyncio.to_thread(send)

            consumer.close()

        server_task = asyncio.create_task(echo_server())

        try:
            # Wait for server to be ready (partition assigned)
            await server_ready.wait()
            await rpc.start()

            num_requests = 20
            durations: list[float] = []
            for _ in range(num_requests):
                start = perf_counter()
                response = await rpc.request(
                    req_topic=request_topic,
                    req_value=b"ping",
                    req_headers_reply_to=[reply_topic],
                    res_timeout=TEST_TIMEOUT,
                    res_expect_type=bytes,
                )
                elapsed = perf_counter() - start
                durations.append(elapsed)

                assert response == b"ping", f"Expected b'ping', got {response}"
                self.assertLess(
                    elapsed,
                    1.0,
                    f"RPC request-response exceeded 1s (took {elapsed:.3f}s)",
                )

            avg_ms = (sum(durations) / num_requests) * 1000
            max_ms = max(durations) * 1000
            min_ms = min(durations) * 1000
            logger.info(
                f"[performance] {self.__class__.__name__}.test_rpc_request_response: "
                f"{num_requests} requests avg={avg_ms:.1f}ms "
                f"max={max_ms:.1f}ms min={min_ms:.1f}ms"
            )

        finally:
            server_stop.set()
            await rpc.stop()
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

    async def test_rpc_with_json_response(self) -> None:
        """Test RPC with JSON parsing."""
        request_topic, client_group = get_topic_and_group_id(self.test_rpc_with_json_response, suffix="request")
        reply_topic, server_group = get_topic_and_group_id(self.test_rpc_with_json_response, suffix="reply")

        await ensure_topic_exists(request_topic)
        await ensure_topic_exists(reply_topic)

        def parse_json_reply(rec: Message) -> RPCResponsePayload:
            data = loads_json(rec.value())
            echo_value = data.get("echo")
            if isinstance(echo_value, dict):
                echo_raw = cast(dict[str, object], echo_value)
            else:
                echo_raw = {}
            return RPCResponsePayload(
                status=as_str(data.get("status")),
                echo=EchoPayload(
                    action=as_str(echo_raw.get("action")),
                    value=as_int(echo_raw.get("value")),
                ),
                server=as_str(data.get("server")),
            )

        specs: list[ParserSpec[RPCResponsePayload]] = [
            {
                "topics": [reply_topic],
                "type": RPCResponsePayload,
                "parser": parse_json_reply,
            }
        ]

        rpc = KafkaRPC(
            parsers=specs,
            producer_factory=make_producer,
            consumer_factory=lambda: make_consumer(client_group),
        )

        server_ready = asyncio.Event()
        server_stop = asyncio.Event()

        async def json_server() -> None:
            consumer = await asyncio.to_thread(make_ready_consumer, server_group, [request_topic])
            producer = make_producer()
            server_ready.set()

            def poll_one() -> Message | None:
                return consumer.poll(0.1)

            while not server_stop.is_set():
                msg = await asyncio.to_thread(poll_one)
                if msg is None or msg.error():
                    continue

                headers = msg.headers() or []
                reply_to: str | None = None
                corr_id: bytes | None = None
                for k, v in headers:
                    if k == "x-reply-topic":
                        reply_to = v.decode()
                    if k.lower() == "request_id":
                        corr_id = v

                if reply_to:
                    req_data = loads_json(msg.value())
                    response_bytes = json.dumps({
                        "status": "ok",
                        "echo": {
                            "action": as_str(req_data.get("action")),
                            "value": as_int(req_data.get("value")),
                        },
                        "server": "json-server",
                    }).encode()
                    reply_headers: list[tuple[str, str | bytes]] = [("request_id", corr_id)] if corr_id else []
                    reply_topic_name = reply_to

                    def send() -> None:
                        producer.produce(
                            reply_topic_name,
                            value=response_bytes,
                            headers=reply_headers,
                        )
                        producer.flush()

                    await asyncio.to_thread(send)

            consumer.close()

        server_task = asyncio.create_task(json_server())

        try:
            await server_ready.wait()
            await rpc.start()

            num_requests = 20
            durations: list[float] = []
            for _ in range(num_requests):
                start = perf_counter()
                response = await rpc.request(
                    req_topic=request_topic,
                    req_value=json.dumps({"action": "test", "value": 123}).encode(),
                    req_headers_reply_to=[reply_topic],
                    res_timeout=TEST_TIMEOUT,
                    res_expect_type=RPCResponsePayload,
                )
                elapsed = perf_counter() - start
                durations.append(elapsed)

                self.assertLess(
                    elapsed,
                    1.0,
                    f"RPC request-response (json) exceeded 1s (took {elapsed:.3f}s)",
                )

                assert response.status == "ok"
                assert response.echo.action == "test"
                assert response.echo.value == 123
                assert response.server == "json-server"

            avg_ms = (sum(durations) / num_requests) * 1000
            max_ms = max(durations) * 1000
            min_ms = min(durations) * 1000
            logger.info(
                f"[performance] {self.__class__.__name__}.test_rpc_with_json_response: "
                f"{num_requests} requests avg={avg_ms:.1f}ms "
                f"max={max_ms:.1f}ms min={min_ms:.1f}ms"
            )

        finally:
            server_stop.set()
            await rpc.stop()
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

    async def test_rpc_timeout(self) -> None:
        """Test that RPC properly times out when no response."""
        request_topic, client_group = get_topic_and_group_id(self.test_rpc_timeout, suffix="request")
        reply_topic, _ = get_topic_and_group_id(self.test_rpc_timeout, suffix="reply")

        await ensure_topic_exists(request_topic)
        await ensure_topic_exists(reply_topic)

        def parse_reply(rec: Message) -> bytes:
            return rec.value() or b""

        specs: list[ParserSpec[bytes]] = [{"topics": [reply_topic], "type": bytes, "parser": parse_reply}]

        rpc = KafkaRPC(
            parsers=specs,
            producer_factory=make_producer,
            consumer_factory=lambda: make_consumer(client_group),
        )

        try:
            await rpc.start()

            # No server running, should timeout quickly
            with self.assertRaises(TimeoutError):
                await rpc.request(
                    req_topic=request_topic,
                    req_value=b"hello",
                    req_headers_reply_to=[reply_topic],
                    res_timeout=0.5,  # Short timeout for test speed
                )

        finally:
            await rpc.stop()
