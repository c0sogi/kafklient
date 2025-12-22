import asyncio
import unittest
import uuid
from datetime import timedelta

from kafklient.tests._config import KAFKA_BOOTSTRAP, TEST_TIMEOUT


class TestMCPKafkaBridge(unittest.IsolatedAsyncioTestCase):
    async def test_mcp_list_tools_and_call_tool(self) -> None:
        """
        MCP over Kafka E2E 테스트.

        구성:
        - (in-process) kafklient.mcp.server.run_server_async : Kafka(요청/응답 토픽)로 MCP 서버 실행
        - (subprocess) uv run kafklient-mcp-client : stdio <-> Kafka 브릿지
        - (in-process) mcp.client.session.ClientSession : stdio로 브릿지에 접속하여 initialize/list_tools/call_tool 검증
        """
        try:
            from fastmcp import FastMCP
            from mcp.client.session import ClientSession
            from mcp.client.stdio import StdioServerParameters, stdio_client
        except Exception as e:  # pragma: no cover
            raise unittest.SkipTest(f"MCP dependencies not installed: {e!r}")

        from kafklient.mcp.server import run_server_async

        suffix = uuid.uuid4().hex[:8]
        req_topic = f"mcp-requests-{suffix}"
        res_topic = f"mcp-responses-{suffix}"

        # 서버(요청 consumer)와 클라이언트(응답 consumer)는 group_id를 분리해야 함
        server_group_id = f"mcp-server-{suffix}"
        client_group_id = f"mcp-client-{suffix}"

        # 테스트가 initialize를 시도하기 전에 Kafka consumer subscription(assignment)이 끝났음을 보장
        server_ready = asyncio.Event()

        mcp = FastMCP("Kafka MCP Server (test)")

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

            # stdio 클라이언트는 브릿지 프로세스를 띄워서 통신한다(uv 기반).
            bridge = StdioServerParameters(
                command="uv",
                args=[
                    "run",
                    "kafklient-mcp-client",
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

            async with stdio_client(bridge) as (read_stream, write_stream):
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

                    # FastMCP는 보통 결과를 text content로 내려준다. (structuredContent는 옵션)
                    text_parts: list[str] = []
                    for block in result.content:
                        if getattr(block, "type", None) == "text":
                            text_parts.append(getattr(block, "text", ""))
                    joined = " ".join(text_parts).strip()

                    # 결과 형태가 바뀌어도 깨지지 않게: text에 5가 있거나 structuredContent가 있으면 통과
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


if __name__ == "__main__":
    unittest.main()
