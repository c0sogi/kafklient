import asyncio
import traceback
import unittest
import uuid
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from tests._config import KAFKA_BOOTSTRAP, TEST_TIMEOUT


def _iter_mcp_server_factories() -> list[tuple[str, Callable[[str], Any]]]:
    factories: list[tuple[str, Callable[[str], Any]]] = []

    try:
        from mcp.server import FastMCP as McpFastMCP

        factories.append(("mcp", lambda name: McpFastMCP(name)))
    except Exception:
        pass

    try:
        from fastmcp import FastMCP as ExternalFastMCP

        factories.append(("fastmcp", lambda name: ExternalFastMCP(name)))
    except Exception:
        pass

    return factories


class TestMcpClientSessionHelpers(unittest.IsolatedAsyncioTestCase):
    async def test_inprocess_client_session_list_tools(self) -> None:
        try:
            from kafklient.mcp.session import inprocess_client_session
        except Exception:  # pragma: no cover
            trb = traceback.format_exc()
            raise unittest.SkipTest(f"MCP dependencies not installed: {trb}")

        factories = _iter_mcp_server_factories()
        if not factories:  # pragma: no cover
            raise unittest.SkipTest("No MCP server implementations available (expected mcp/fastmcp)")

        for impl_label, make_server in factories:
            with self.subTest(server_impl=impl_label):
                mcp = make_server("In-process MCP server (test)")

                @mcp.tool()
                def add(a: int, b: int) -> int:  # pyright: ignore[reportUnusedFunction]
                    return a + b

                async with inprocess_client_session(
                    mcp,
                    read_timeout_seconds=timedelta(seconds=TEST_TIMEOUT),
                    initialize=True,
                ) as session:
                    tools = await asyncio.wait_for(session.list_tools(), timeout=TEST_TIMEOUT)
                    tool_names = {t.name for t in tools.tools}
                    self.assertIn("add", tool_names)

    async def test_kafka_client_session_without_subprocess(self) -> None:
        """
        E2E sanity check: connect to MCP-over-Kafka server directly using `kafka_client_session`
        (no stdio bridge subprocess / no StdioServerParameters).
        """
        try:
            from kafklient.mcp.server import run_server_async
            from kafklient.mcp.session import kafka_client_session
        except Exception:  # pragma: no cover
            trb = traceback.format_exc()
            raise unittest.SkipTest(f"MCP dependencies not installed: {trb}")

        factories = _iter_mcp_server_factories()
        if not factories:  # pragma: no cover
            raise unittest.SkipTest("No MCP server implementations available (expected mcp/fastmcp)")

        for impl_label, make_server in factories:
            with self.subTest(server_impl=impl_label):
                suffix = f"{impl_label}-{uuid.uuid4().hex[:8]}"
                req_topic = f"mcp-requests-direct-{suffix}"
                res_topic = f"mcp-responses-direct-{suffix}"

                server_group_id = f"mcp-server-{suffix}"
                client_group_id = f"mcp-client-{suffix}"

                server_ready = asyncio.Event()

                mcp = make_server("Kafka MCP server (direct session test)")

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

                    async with kafka_client_session(
                        bootstrap_servers=KAFKA_BOOTSTRAP,
                        consumer_topic=res_topic,
                        producer_topic=req_topic,
                        consumer_group_id=client_group_id,
                        auto_create_topics=True,
                        assignment_timeout_s=5.0,
                        read_timeout_seconds=timedelta(seconds=TEST_TIMEOUT),
                        initialize=True,
                    ) as session:
                        tools = await asyncio.wait_for(session.list_tools(), timeout=TEST_TIMEOUT)
                        tool_names = {t.name for t in tools.tools}
                        self.assertIn("add", tool_names)

                        result = await asyncio.wait_for(
                            session.call_tool("add", {"a": 2, "b": 3}), timeout=TEST_TIMEOUT
                        )
                        self.assertFalse(result.isError, f"tool call failed: {result!r}")
                finally:
                    server_task.cancel()
                    try:
                        await server_task
                    except asyncio.CancelledError:
                        pass


if __name__ == "__main__":
    unittest.main()
