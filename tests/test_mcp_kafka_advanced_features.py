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
            )
        )

        try:
            await asyncio.wait_for(server_ready.wait(), timeout=TEST_TIMEOUT)

            async with kafka_client_session(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                consumer_topic=res_topic,
                producer_topic=req_topic,
                consumer_group_id=client_group_id,
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
            )
        )

        try:
            await asyncio.wait_for(server_ready.wait(), timeout=TEST_TIMEOUT)

            async with kafka_client_session(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                consumer_topic=res_topic,
                producer_topic=req_topic,
                consumer_group_id=client_group_id,
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
            )
        )

        try:
            await asyncio.wait_for(server_ready.wait(), timeout=TEST_TIMEOUT)

            async with kafka_client_session(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                consumer_topic=res_topic,
                producer_topic=req_topic,
                consumer_group_id=client_group_id,
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
        - Kafka transport MUST NOT interfere with SDK semantics. Concretely, the observed notifications over
          Kafka must match the observed notifications in-process for the same server behavior, even if the SDK
          changes its subscribe/notification behavior in the future.
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
            from kafklient.mcp.session import inprocess_client_session, kafka_client_session
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

        def make_server(name: str) -> tuple[FastMCP, set[str]]:
            subscribed_uris: set[str] = set()

            async def on_subscribe_uri(uri: AnyUrl) -> None:
                subscribed_uris.add(str(uri))

            async def on_unsubscribe_uri(uri: AnyUrl) -> None:
                subscribed_uris.remove(str(uri))

            mcp = FastMCP(name)
            mcp._mcp_server.subscribe_resource()(on_subscribe_uri)  # pyright: ignore[reportPrivateUsage]
            mcp._mcp_server.unsubscribe_resource()(on_unsubscribe_uri)  # pyright: ignore[reportPrivateUsage]

            @mcp.tool()
            async def emit_two_updates_filtered(  # pyright: ignore[reportUnusedFunction]
                ctx: Context[Any, Any, Any],
            ) -> bool:
                server_session = cast(ServerSession, ctx.request_context.session)
                if str(uri_a) in subscribed_uris:
                    await server_session.send_resource_updated(uri_a)
                if str(uri_b) in subscribed_uris:
                    await server_session.send_resource_updated(uri_b)
                return True

            @mcp.tool()
            async def emit_two_updates_unfiltered(  # pyright: ignore[reportUnusedFunction]
                ctx: Context[Any, Any, Any],
            ) -> bool:
                server_session = cast(ServerSession, ctx.request_context.session)
                await server_session.send_resource_updated(uri_a)
                await server_session.send_resource_updated(uri_b)
                return True

            return mcp, subscribed_uris

        async def run_unfiltered_probe(
            *,
            session: object,
            received: list[str],
        ) -> bool:
            # Activate subscribe state (whatever the SDK decides that means).
            _ = await asyncio.wait_for(cast(Any, session).subscribe_resource(uri_a), timeout=TEST_TIMEOUT)

            # Emit A+B unfiltered.
            await asyncio.wait_for(
                cast(Any, session).call_tool("emit_two_updates_unfiltered", {}), timeout=TEST_TIMEOUT
            )

            # Poll for delivery; we intentionally do not assume SDK semantics, we only measure them.
            deadline_a = asyncio.get_running_loop().time() + TEST_TIMEOUT
            while True:
                if any(u.endswith("/a") for u in received):
                    break
                if asyncio.get_running_loop().time() > deadline_a:
                    raise TimeoutError("Did not receive /a notification in time")
                await asyncio.sleep(0.01)

            deadline_b = asyncio.get_running_loop().time() + 1.0
            while True:
                if any(u.endswith("/b") for u in received):
                    return True
                if asyncio.get_running_loop().time() > deadline_b:
                    return False
                await asyncio.sleep(0.01)

        # 0) Establish SDK baseline in-process (no Kafka). This is what Kafka transport must match.
        inproc_mcp, _inproc_subscribed = make_server("In-process MCP advanced features (subscribe semantics)")
        inproc_received: list[str] = []

        async def inproc_message_handler(message: object) -> None:
            if isinstance(message, types.ServerNotification) and isinstance(
                message.root, types.ResourceUpdatedNotification
            ):
                inproc_received.append(str(message.root.params.uri))

        async with inprocess_client_session(
            inproc_mcp,
            read_timeout_seconds=timedelta(seconds=TEST_TIMEOUT),
            initialize=True,
            message_handler=inproc_message_handler,
        ) as inproc_session:
            inproc_b_arrived = await run_unfiltered_probe(
                session=inproc_session,
                received=inproc_received,
            )

        # 1) Kafka transport should match the in-process observation.
        kafka_mcp, kafka_subscribed = make_server("Kafka MCP advanced features (subscribe semantics)")
        kafka_received: list[str] = []

        async def kafka_message_handler(message: object) -> None:
            if isinstance(message, types.ServerNotification) and isinstance(
                message.root, types.ResourceUpdatedNotification
            ):
                kafka_received.append(str(message.root.params.uri))

        server_task = asyncio.create_task(
            run_server_async(
                mcp=kafka_mcp,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                consumer_topic=req_topic,
                producer_topic=res_topic,
                consumer_group_id=server_group_id,
                ready_event=server_ready,
                auto_create_topics=True,
                show_banner=False,
                log_level="error",
                extra_capabilities={"resources_updated", "prompts_changed", "tools_changed", "resources_changed"},
            )
        )

        try:
            await asyncio.wait_for(server_ready.wait(), timeout=TEST_TIMEOUT)

            async with kafka_client_session(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                consumer_topic=res_topic,
                producer_topic=req_topic,
                consumer_group_id=client_group_id,
                auto_create_topics=True,
                assignment_timeout_s=5.0,
                read_timeout_seconds=timedelta(seconds=TEST_TIMEOUT),
                initialize=True,
                message_handler=kafka_message_handler,
            ) as kafka_session:
                kafka_b_arrived = await run_unfiltered_probe(
                    session=kafka_session,
                    received=kafka_received,
                )
                self.assertEqual(
                    kafka_b_arrived,
                    inproc_b_arrived,
                    f"Kafka transport must match in-process subscribe/update behavior; "
                    f"inproc_received={inproc_received!r}, kafka_received={kafka_received!r}",
                )

                # Now validate explicit server-side filtering still works end-to-end.
                _ = await asyncio.wait_for(kafka_session.unsubscribe_resource(uri_a), timeout=TEST_TIMEOUT)
                self.assertNotIn(str(uri_a), kafka_subscribed, f"expected {uri_a} to be unsubscribed")
                # Server-side filtered emit should now send nothing for A (and B is never subscribed in this test).
                await asyncio.wait_for(kafka_session.call_tool("emit_two_updates_filtered", {}), timeout=TEST_TIMEOUT)
        finally:
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    unittest.main()
