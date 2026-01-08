import asyncio
import logging
import traceback
import unittest
import uuid
from datetime import timedelta
from typing import Any, cast

from pydantic import BaseModel

from tests._config import KAFKA_BOOTSTRAP, TEST_TIMEOUT


class _ElicitForm(BaseModel):
    a: int
    b: int


class TestMcpKafkaAdvancedFeatures(unittest.IsolatedAsyncioTestCase):
    async def test_kafka_elicitation_roundtrip(self) -> None:
        """
        E2E: server tool triggers `elicitation/create` (server->client request).
        Client answers via `elicitation_callback`. Verify tool gets the value.
        """
        try:
            import mcp.types as types
            from mcp.client.session import ClientSession
            from mcp.server import FastMCP
            from mcp.server.fastmcp.server import Context
            from mcp.shared.context import RequestContext

            from kafklient.mcp.server import run_server_async
            from kafklient.mcp.session import kafka_client_session
        except Exception:  # pragma: no cover
            trb = traceback.format_exc()
            raise unittest.SkipTest(f"MCP dependencies not installed: {trb}")

        suffix = f"elic-{uuid.uuid4().hex[:8]}"
        req_topic = f"mcp-requests-adv-{suffix}"
        res_topic = f"mcp-responses-adv-{suffix}"
        server_group_id = f"mcp-server-{suffix}"
        client_group_id = f"mcp-client-{suffix}"
        server_ready = asyncio.Event()

        mcp = FastMCP("Kafka MCP advanced features (elicitation)")

        @mcp.tool()
        async def add_via_elicitation(  # pyright: ignore[reportUnusedFunction]
            ctx: Context[Any, Any, Any], default: int = 0
        ) -> int:
            result = await ctx.elicit(message="Provide a and b", schema=_ElicitForm)
            match result.action:
                case "accept":
                    return int(result.data.a) + int(result.data.b)
                case _:
                    return default

        async def elicitation_callback(
            context: RequestContext[ClientSession, Any],
            params: types.ElicitRequestParams,
        ) -> types.ElicitResult:
            _ = context
            # Respond to form-mode elicitation.
            if getattr(params, "mode", None) == "form":
                return types.ElicitResult(action="accept", content={"a": 2, "b": 3})
            return types.ElicitResult(action="cancel")

        server_task = asyncio.create_task(
            run_server_async(
                mcp=mcp,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                consumer_topic=req_topic,
                producer_topic=res_topic,
                consumer_group_id=server_group_id,
                ready_event=server_ready,
                auto_create_topics=True,
                show_banner=False,
                log_level="error",
                multi_session=True,
                advertise_all_capabilities=True,
            )
        )

        try:
            await asyncio.wait_for(server_ready.wait(), timeout=TEST_TIMEOUT)

            async with kafka_client_session(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                consumer_topic=res_topic,
                producer_topic=req_topic,
                consumer_group_id=client_group_id,
                isolate_session=True,
                auto_create_topics=True,
                assignment_timeout_s=5.0,
                read_timeout_seconds=timedelta(seconds=TEST_TIMEOUT),
                initialize=True,
                elicitation_callback=elicitation_callback,
            ) as session:
                result = await asyncio.wait_for(
                    session.call_tool("add_via_elicitation", {"default": 0}), timeout=TEST_TIMEOUT
                )
                self.assertFalse(result.isError, f"tool call failed: {result!r}")
                text = "".join(c.text for c in result.content if isinstance(c, types.TextContent))
                self.assertIn("5", text)
        finally:
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

    async def test_kafka_sampling_roundtrip(self) -> None:
        """
        E2E: server tool triggers `sampling/createMessage` (server->client request).
        Client answers via `sampling_callback`. Verify tool receives the sampled text.
        """
        try:
            import mcp.types as types
            from mcp.client.session import ClientSession
            from mcp.server import FastMCP
            from mcp.server.fastmcp.server import Context
            from mcp.server.session import ServerSession
            from mcp.shared.context import RequestContext

            from kafklient.mcp.server import run_server_async
            from kafklient.mcp.session import kafka_client_session
        except Exception:  # pragma: no cover
            trb = traceback.format_exc()
            raise unittest.SkipTest(f"MCP dependencies not installed: {trb}")

        suffix = f"samp-{uuid.uuid4().hex[:8]}"
        req_topic = f"mcp-requests-adv-{suffix}"
        res_topic = f"mcp-responses-adv-{suffix}"
        server_group_id = f"mcp-server-{suffix}"
        client_group_id = f"mcp-client-{suffix}"
        server_ready = asyncio.Event()

        mcp = FastMCP("Kafka MCP advanced features (sampling)")

        @mcp.tool()
        async def sample_once(ctx: Context[Any, Any, Any]) -> str:  # pyright: ignore[reportUnusedFunction]
            # Call server->client sampling via the underlying ServerSession.
            server_session = cast(ServerSession, ctx.request_context.session)
            resp = await server_session.create_message(
                messages=[types.SamplingMessage(role="user", content=types.TextContent(type="text", text="hi"))],
                max_tokens=8,
            )
            # Normalize to text
            if isinstance(resp.content, types.TextContent):
                return resp.content.text
            return "<non-text>"

        async def sampling_callback(
            context: RequestContext[ClientSession, Any],
            params: types.CreateMessageRequestParams,
        ) -> types.CreateMessageResult:
            _ = (context, params)
            return types.CreateMessageResult(
                role="assistant",
                content=types.TextContent(type="text", text="pong"),
                model="test",
                stopReason="endTurn",
            )

        server_task = asyncio.create_task(
            run_server_async(
                mcp=mcp,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                consumer_topic=req_topic,
                producer_topic=res_topic,
                consumer_group_id=server_group_id,
                ready_event=server_ready,
                auto_create_topics=True,
                show_banner=False,
                log_level="error",
                multi_session=True,
                advertise_all_capabilities=True,
            )
        )

        try:
            await asyncio.wait_for(server_ready.wait(), timeout=TEST_TIMEOUT)

            async with kafka_client_session(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                consumer_topic=res_topic,
                producer_topic=req_topic,
                consumer_group_id=client_group_id,
                isolate_session=True,
                auto_create_topics=True,
                assignment_timeout_s=5.0,
                read_timeout_seconds=timedelta(seconds=TEST_TIMEOUT),
                initialize=True,
                sampling_callback=sampling_callback,
            ) as session:
                result = await asyncio.wait_for(session.call_tool("sample_once", {}), timeout=TEST_TIMEOUT)
                self.assertFalse(result.isError, f"tool call failed: {result!r}")
                text = "".join(c.text for c in result.content if isinstance(c, types.TextContent))
                self.assertIn("pong", text)
        finally:
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

    async def test_kafka_resource_updated_notification_delivery(self) -> None:
        """
        E2E: server sends `notifications/resources/updated`; verify it reaches the client.

        Note: this does NOT test resources/subscribe semantics (server-side handler is required for that).
        """
        try:
            import mcp.types as types
            from mcp.server import FastMCP
            from mcp.server.fastmcp.server import Context
            from mcp.server.session import ServerSession
            from pydantic import AnyUrl

            from kafklient.mcp.server import run_server_async
            from kafklient.mcp.session import kafka_client_session
        except Exception:  # pragma: no cover
            trb = traceback.format_exc()
            raise unittest.SkipTest(f"MCP dependencies not installed: {trb}")

        suffix = f"resupd-{uuid.uuid4().hex[:8]}"
        req_topic = f"mcp-requests-adv-{suffix}"
        res_topic = f"mcp-responses-adv-{suffix}"
        server_group_id = f"mcp-server-{suffix}"
        client_group_id = f"mcp-client-{suffix}"
        server_ready = asyncio.Event()

        got_notification = asyncio.Event()

        async def message_handler(
            message: object,
        ) -> None:
            # Called for requests/responses/notifications. We only care about the notification.
            if isinstance(message, types.ServerNotification) and isinstance(
                message.root, types.ResourceUpdatedNotification
            ):
                got_notification.set()

        mcp = FastMCP("Kafka MCP advanced features (resources updated)")

        @mcp.tool()
        async def trigger_resource_updated(  # pyright: ignore[reportUnusedFunction]
            ctx: Context[Any, Any, Any],
        ) -> bool:
            server_session = cast(ServerSession, ctx.request_context.session)
            await server_session.send_resource_updated(AnyUrl("https://example.com/resource"))
            return True

        server_task = asyncio.create_task(
            run_server_async(
                mcp=mcp,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                consumer_topic=req_topic,
                producer_topic=res_topic,
                consumer_group_id=server_group_id,
                ready_event=server_ready,
                auto_create_topics=True,
                show_banner=False,
                log_level="error",
                multi_session=True,
                advertise_all_capabilities=True,
            )
        )

        try:
            await asyncio.wait_for(server_ready.wait(), timeout=TEST_TIMEOUT)

            async with kafka_client_session(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                consumer_topic=res_topic,
                producer_topic=req_topic,
                consumer_group_id=client_group_id,
                isolate_session=True,
                auto_create_topics=True,
                assignment_timeout_s=5.0,
                read_timeout_seconds=timedelta(seconds=TEST_TIMEOUT),
                initialize=True,
                message_handler=message_handler,
            ) as session:
                result = await asyncio.wait_for(session.call_tool("trigger_resource_updated", {}), timeout=TEST_TIMEOUT)
                self.assertFalse(result.isError, f"tool call failed: {result!r}")
                await asyncio.wait_for(got_notification.wait(), timeout=TEST_TIMEOUT)
        finally:
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

    async def test_kafka_resource_subscribe_semantics(self) -> None:
        """
        E2E: `resources/subscribe` / `resources/unsubscribe` semantics over Kafka.

        Expectations:
        - After subscribing to URI A, server notifications for A are delivered, and B are filtered out.
        - After unsubscribing from URI A, further notifications for A are filtered out.
        """
        try:
            import mcp.types as types
            from mcp.server import FastMCP
            from mcp.server.fastmcp.server import Context
            from mcp.server.session import ServerSession
            from pydantic import AnyUrl

            from kafklient.mcp.server import run_server_async
            from kafklient.mcp.session import kafka_client_session
        except Exception:  # pragma: no cover
            trb = traceback.format_exc()
            raise unittest.SkipTest(f"MCP dependencies not installed: {trb}")

        # Enable debug logs for transport-level subscribe filtering (helps diagnose failures).
        logging.getLogger("kafklient.mcp.server").setLevel(logging.DEBUG)

        suffix = f"sub-{uuid.uuid4().hex[:8]}"
        req_topic = f"mcp-requests-adv-{suffix}"
        res_topic = f"mcp-responses-adv-{suffix}"
        server_group_id = f"mcp-server-{suffix}"
        client_group_id = f"mcp-client-{suffix}"
        server_ready = asyncio.Event()

        uri_a = AnyUrl("https://example.com/a")
        uri_b = AnyUrl("https://example.com/b")

        received: list[str] = []
        got_a = asyncio.Event()
        got_b = asyncio.Event()

        async def message_handler(message: object) -> None:
            if isinstance(message, types.ServerNotification) and isinstance(
                message.root, types.ResourceUpdatedNotification
            ):
                received_uri = str(message.root.params.uri)
                received.append(received_uri)
                if received_uri.endswith("/a"):
                    got_a.set()
                if received_uri.endswith("/b"):
                    got_b.set()

        mcp = FastMCP("Kafka MCP advanced features (subscribe semantics)")

        @mcp.tool()
        async def emit_two_updates(ctx: Context[Any, Any, Any]) -> bool:  # pyright: ignore[reportUnusedFunction]
            server_session = cast(ServerSession, ctx.request_context.session)
            await server_session.send_resource_updated(uri_a)
            await server_session.send_resource_updated(uri_b)
            return True

        server_task = asyncio.create_task(
            run_server_async(
                mcp=mcp,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                consumer_topic=req_topic,
                producer_topic=res_topic,
                consumer_group_id=server_group_id,
                ready_event=server_ready,
                auto_create_topics=True,
                show_banner=False,
                log_level="error",
                multi_session=True,
                advertise_all_capabilities=True,
            )
        )

        try:
            await asyncio.wait_for(server_ready.wait(), timeout=TEST_TIMEOUT)

            async with kafka_client_session(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                consumer_topic=res_topic,
                producer_topic=req_topic,
                consumer_group_id=client_group_id,
                isolate_session=True,
                auto_create_topics=True,
                assignment_timeout_s=5.0,
                read_timeout_seconds=timedelta(seconds=TEST_TIMEOUT),
                initialize=True,
                message_handler=message_handler,
            ) as session:
                caps = session.get_server_capabilities()
                self.assertIsNotNone(caps, "expected server capabilities after initialize()")
                if caps is not None:
                    self.assertIsNotNone(caps.resources, "expected resources capability to be present")
                    if caps.resources is not None:
                        self.assertTrue(caps.resources.subscribe, "expected resources.subscribe=True to be advertised")
                        self.assertTrue(
                            caps.resources.listChanged, "expected resources.listChanged=True to be advertised"
                        )
                    self.assertIsNotNone(caps.tools, "expected tools capability to be present")
                    if caps.tools is not None:
                        self.assertTrue(caps.tools.listChanged, "expected tools.listChanged=True to be advertised")
                    self.assertIsNotNone(caps.prompts, "expected prompts capability to be present")
                    if caps.prompts is not None:
                        self.assertTrue(caps.prompts.listChanged, "expected prompts.listChanged=True to be advertised")
                    self.assertIsNotNone(caps.logging, "expected logging capability to be present")

                # Subscribe to A (MCP request resources/subscribe)
                _ = await asyncio.wait_for(session.subscribe_resource(uri_a), timeout=TEST_TIMEOUT)

                # Emit A + B updates; only A should arrive
                got_a.clear()
                got_b.clear()
                await asyncio.wait_for(session.call_tool("emit_two_updates", {}), timeout=TEST_TIMEOUT)
                await asyncio.wait_for(got_a.wait(), timeout=TEST_TIMEOUT)
                try:
                    await asyncio.wait_for(got_b.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
                else:
                    raise AssertionError(f"Unexpected /b notification received; received={received!r}")

                # Unsubscribe from A; A should no longer arrive
                _ = await asyncio.wait_for(session.unsubscribe_resource(uri_a), timeout=TEST_TIMEOUT)
                got_a.clear()
                await asyncio.wait_for(session.call_tool("emit_two_updates", {}), timeout=TEST_TIMEOUT)
                try:
                    await asyncio.wait_for(got_a.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
                else:
                    raise AssertionError(f"Unexpected /a notification after unsubscribe; received={received!r}")
        finally:
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    unittest.main()
