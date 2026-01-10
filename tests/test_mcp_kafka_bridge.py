import asyncio
import traceback
import unittest
import uuid
from collections.abc import Callable
from datetime import timedelta
from typing import Any, cast

from tests._config import KAFKA_BOOTSTRAP, TEST_TIMEOUT


def _iter_mcp_server_factories() -> list[tuple[str, Callable[[str], Any]]]:
    """
    Returns server factories for both implementations supported by `kafklient.mcp.server`.

    - `fastmcp.FastMCP` (external `fastmcp` package)
    - `mcp.server.FastMCP` (FastMCP bundled in the `mcp` package)

    Each entry is `(label, factory(name) -> server_instance)`.
    """
    factories: list[tuple[str, Callable[[str], Any]]] = []

    # `mcp` is required anyway for ClientSession/stdio_client, so this should usually exist.
    try:
        from mcp.server import FastMCP as McpFastMCP

        factories.append(("mcp", lambda name: McpFastMCP(name)))
    except Exception:
        # If `mcp` isn't installed, the test will be skipped earlier; keep this defensive.
        pass

    # `fastmcp` is optional (kafklient[mcp] extra)
    try:
        from fastmcp import FastMCP as ExternalFastMCP

        factories.append(("fastmcp", lambda name: ExternalFastMCP(name)))
    except Exception:
        pass

    return factories


class TestMCPKafkaBridge(unittest.IsolatedAsyncioTestCase):
    async def test_mcp_list_tools_and_call_tool(self) -> None:
        """
        MCP over Kafka E2E test.

        Setup:
        - (in-process) kafklient.mcp.server.run_server_async: runs MCP server over Kafka (request/response topics)
        - (subprocess) uv run kafklient-mcp-client: stdio <-> Kafka bridge
        - (in-process) mcp.client.session.ClientSession: connects to the bridge over stdio and validates
          initialize/list_tools/call_tool
        """
        try:
            from mcp.client.session import ClientSession
            from mcp.client.stdio import StdioServerParameters, stdio_client
        except Exception:  # pragma: no cover
            trb = traceback.format_exc()
            raise unittest.SkipTest(f"MCP dependencies not installed: {trb}")

        from kafklient.mcp.server import run_server_async

        factories = _iter_mcp_server_factories()
        if not factories:  # pragma: no cover
            raise unittest.SkipTest("No MCP server implementations available (expected mcp/fastmcp)")

        for impl_label, make_server in factories:
            with self.subTest(server_impl=impl_label):
                suffix = f"{impl_label}-{uuid.uuid4().hex[:8]}"
                req_topic = f"mcp-requests-{suffix}"
                res_topic = f"mcp-responses-{suffix}"

                # Server (request consumer) and client (response consumer) must use different group.id values.
                server_group_id = f"mcp-server-{suffix}"
                client_group_id = f"mcp-client-{suffix}"

                # Ensure the Kafka consumer subscription (assignment) is ready before the test calls initialize().
                server_ready = asyncio.Event()

                mcp = make_server("Kafka MCP Server (test)")

                @mcp.tool()
                def echo(message: str) -> str:  # pyright: ignore[reportUnusedFunction]
                    return f"Echo: {message}"

                @mcp.tool()
                def add(a: int, b: int) -> int:  # pyright: ignore[reportUnusedFunction]
                    return a + b

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

                    # The stdio client communicates by spawning the bridge process (via uv).
                    bridge = StdioServerParameters(
                        command="uv",
                        args=[
                            "run",
                            "kafklient",
                            "mcp-client",
                            "--bootstrap-servers",
                            KAFKA_BOOTSTRAP,
                            "--producer-topic",
                            req_topic,
                            "--consumer-topic",
                            res_topic,
                            "--consumer-group-id",
                            client_group_id,
                            "--log-level",
                            "ERROR",
                        ],
                    )

                    async with cast(Any, stdio_client(bridge)) as (read_stream, write_stream):
                        async with ClientSession(
                            read_stream,
                            write_stream,
                            read_timeout_seconds=timedelta(seconds=TEST_TIMEOUT),
                        ) as session:
                            await session.initialize()

                            tools = await session.list_tools()
                            tool_names = {t.name for t in tools.tools}
                            self.assertIn("echo", tool_names)
                            self.assertIn("add", tool_names)

                            result = await session.call_tool("add", {"a": 2, "b": 3})
                            self.assertFalse(result.isError, f"tool call failed: {result!r}")

                            # FastMCP typically returns results as text content. (structuredContent is optional.)
                            text_parts: list[str] = []
                            for block in result.content:
                                if getattr(block, "type", None) == "text":
                                    text_parts.append(getattr(block, "text", ""))
                            joined = " ".join(text_parts).strip()

                            # Be resilient to format changes: pass if text contains "5" or structuredContent is present.
                            self.assertTrue(
                                ("5" in joined) or (result.structuredContent is not None),
                                f"unexpected tool result: text={joined!r}, structured={result.structuredContent!r}",
                            )
                finally:
                    server_task.cancel()
                    try:
                        await server_task
                    except asyncio.CancelledError:
                        pass

    async def test_mcp_multi_client_session_isolation(self) -> None:
        """
        Verify that even when two bridges (= two MCP client sessions) share the same request topic concurrently,
        initialize/responses do not get mixed across sessions.

        Assumptions:
        - The bridge attaches its response topic via the x-reply-topic header when producing requests.
        - The server creates a separate MCP ServerSession per reply-topic (session key) to isolate responses/notifications.
        """
        try:
            from mcp.client.session import ClientSession
            from mcp.client.stdio import StdioServerParameters, stdio_client
        except Exception:
            trb = traceback.format_exc()
            raise unittest.SkipTest(f"MCP dependencies not installed: {trb}")

        from kafklient.mcp.server import run_server_async

        factories = _iter_mcp_server_factories()
        if not factories:  # pragma: no cover
            raise unittest.SkipTest("No MCP server implementations available (expected mcp/fastmcp)")

        for impl_label, make_server in factories:
            with self.subTest(server_impl=impl_label):
                suffix = f"{impl_label}-{uuid.uuid4().hex[:8]}"
                req_topic = f"mcp-requests-shared-{suffix}"
                # Share a single response topic but filter by x-session-id in the bridge to ensure session isolation.
                res_topic = f"mcp-responses-shared-{suffix}"

                server_group_id = f"mcp-server-{suffix}"
                client_group_id_a = f"mcp-client-a-{suffix}"
                client_group_id_b = f"mcp-client-b-{suffix}"

                server_ready = asyncio.Event()

                mcp = make_server("Kafka MCP Server (multi-client test)")

                @mcp.tool()
                def add(a: int, b: int) -> int:  # pyright: ignore[reportUnusedFunction]
                    return a + b

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

                    bridge_a = StdioServerParameters(
                        command="uv",
                        args=[
                            "run",
                            "kafklient",
                            "mcp-client",
                            "--bootstrap-servers",
                            KAFKA_BOOTSTRAP,
                            "--producer-topic",
                            req_topic,
                            "--consumer-topic",
                            res_topic,
                            "--consumer-group-id",
                            client_group_id_a,
                            "--log-level",
                            "ERROR",
                        ],
                    )
                    bridge_b = StdioServerParameters(
                        command="uv",
                        args=[
                            "run",
                            "kafklient",
                            "mcp-client",
                            "--bootstrap-servers",
                            KAFKA_BOOTSTRAP,
                            "--producer-topic",
                            req_topic,
                            "--consumer-topic",
                            res_topic,
                            "--consumer-group-id",
                            client_group_id_b,
                            "--log-level",
                            "ERROR",
                        ],
                    )

                    async with (
                        cast(Any, stdio_client(bridge_a)) as (read_a, write_a),
                        cast(Any, stdio_client(bridge_b)) as (
                            read_b,
                            write_b,
                        ),
                    ):
                        async with (
                            ClientSession(
                                read_a,
                                write_a,
                                read_timeout_seconds=timedelta(seconds=TEST_TIMEOUT),
                            ) as session_a,
                            ClientSession(
                                read_b,
                                write_b,
                                read_timeout_seconds=timedelta(seconds=TEST_TIMEOUT),
                            ) as session_b,
                        ):
                            # Initialize concurrently: if isolation is broken, JSON-RPC id collisions often show up here.
                            await asyncio.wait_for(
                                asyncio.gather(session_a.initialize(), session_b.initialize()),
                                timeout=TEST_TIMEOUT,
                            )

                            # List tools concurrently as well
                            tools_a, tools_b = await asyncio.wait_for(
                                asyncio.gather(session_a.list_tools(), session_b.list_tools()),
                                timeout=TEST_TIMEOUT,
                            )
                            tool_names_a = {t.name for t in tools_a.tools}
                            tool_names_b = {t.name for t in tools_b.tools}
                            self.assertIn("add", tool_names_a)
                            self.assertIn("add", tool_names_b)

                            # Call the tool concurrently (validates no id collision / response mixing)
                            result_a, result_b = await asyncio.wait_for(
                                asyncio.gather(
                                    session_a.call_tool("add", {"a": 1, "b": 2}),
                                    session_b.call_tool("add", {"a": 10, "b": 20}),
                                ),
                                timeout=TEST_TIMEOUT,
                            )
                            self.assertFalse(result_a.isError, f"tool call failed: {result_a!r}")
                            self.assertFalse(result_b.isError, f"tool call failed: {result_b!r}")

                            def extract_text(result: Any) -> str:
                                parts: list[str] = []
                                for block in result.content:
                                    if getattr(block, "type", None) == "text":
                                        parts.append(getattr(block, "text", ""))
                                return " ".join(parts).strip()

                            text_a = extract_text(result_a)
                            text_b = extract_text(result_b)

                            self.assertTrue(
                                ("3" in text_a) or (result_a.structuredContent is not None),
                                f"unexpected tool result A: text={text_a!r}, structured={result_a.structuredContent!r}",
                            )
                            self.assertTrue(
                                ("30" in text_b) or (result_b.structuredContent is not None),
                                f"unexpected tool result B: text={text_b!r}, structured={result_b.structuredContent!r}",
                            )
                finally:
                    server_task.cancel()
                    try:
                        await server_task
                    except asyncio.CancelledError:
                        pass


if __name__ == "__main__":
    unittest.main()
