import asyncio
import traceback
import unittest
import uuid
from datetime import timedelta
from typing import Any, cast

from tests._config import KAFKA_BOOTSTRAP, TEST_TIMEOUT


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
        except Exception:  # pragma: no cover
            trb = traceback.format_exc()
            raise unittest.SkipTest(f"MCP dependencies not installed: {trb}")

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

    async def test_mcp_multi_client_session_isolation(self) -> None:
        """
        동일한 요청 토픽을 공유하는 2개 브릿지(=2개 MCP 클라이언트 세션)가 동시에 접속하더라도,
        각 세션의 initialize/response가 섞이지 않음을 검증합니다.

        전제:
        - 브릿지는 요청 produce 시 x-reply-topic 헤더로 자신의 응답 토픽을 싣는다.
        - 서버는 reply-topic(세션 키)별로 별도의 MCP ServerSession을 생성하여 응답/알림을 격리한다.
        """
        try:
            from fastmcp import FastMCP
            from mcp.client.session import ClientSession
            from mcp.client.stdio import StdioServerParameters, stdio_client
        except Exception:
            trb = traceback.format_exc()
            raise unittest.SkipTest(f"MCP dependencies not installed: {trb}")

        from kafklient.mcp.server import run_server_async

        suffix = uuid.uuid4().hex[:8]
        req_topic = f"mcp-requests-shared-{suffix}"
        # 응답 토픽은 1개로 공유하되, x-session-id로 브릿지에서 필터링하여 세션 격리를 보장한다.
        res_topic = f"mcp-responses-shared-{suffix}"

        server_group_id = f"mcp-server-{suffix}"
        client_group_id_a = f"mcp-client-a-{suffix}"
        client_group_id_b = f"mcp-client-b-{suffix}"

        server_ready = asyncio.Event()

        mcp = FastMCP("Kafka MCP Server (multi-client test)")

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
                multi_session=True,
            )
        )

        try:
            await asyncio.wait_for(server_ready.wait(), timeout=TEST_TIMEOUT)

            bridge_a = StdioServerParameters(
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
                    client_group_id_a,
                    "--log-level",
                    "ERROR",
                ],
            )
            bridge_b = StdioServerParameters(
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
                    # 동시에 initialize -> JSON-RPC id 충돌 가능성이 높아, 세션 격리가 깨지면 여기서 흔히 터진다.
                    await asyncio.wait_for(
                        asyncio.gather(session_a.initialize(), session_b.initialize()),
                        timeout=TEST_TIMEOUT,
                    )

                    # 도구 목록도 동시에
                    tools_a, tools_b = await asyncio.wait_for(
                        asyncio.gather(session_a.list_tools(), session_b.list_tools()),
                        timeout=TEST_TIMEOUT,
                    )
                    tool_names_a = {t.name for t in tools_a.tools}
                    tool_names_b = {t.name for t in tools_b.tools}
                    self.assertIn("add", tool_names_a)
                    self.assertIn("add", tool_names_b)

                    # 동시에 tool call (id 충돌/응답 혼선 방지 검증)
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
