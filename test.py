from fastmcp import FastMCP

mcp = FastMCP("Test MCP")


@mcp.tool()
def add(a: int, b: int) -> int:
    return a + b
