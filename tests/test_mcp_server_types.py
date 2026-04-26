from __future__ import annotations

import unittest
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kafklient.mcp.server import Server


def _accept_server(server: Server) -> Server:
    return server


class TestMCPServerTypes(unittest.TestCase):
    def test_server_protocol_accepts_supported_fastmcp_implementations(self) -> None:
        try:
            from fastmcp import FastMCP as ExternalFastMCP
            from mcp.server import FastMCP as McpFastMCP
        except Exception as e:
            raise unittest.SkipTest(f"MCP dependencies not installed: {e}") from e

        mcp_server = McpFastMCP("mcp")
        external_server = ExternalFastMCP("fastmcp")

        self.assertIs(_accept_server(mcp_server), mcp_server)
        self.assertIs(_accept_server(external_server), external_server)
