"""A tiny FastMCP server used as a real backend in fastmcp_backend.py.

Requires FastMCP:  pip install fastmcp

Run on its own with:  python examples/fastmcp_server.py   (speaks MCP over stdio)
"""

from fastmcp import FastMCP

mcp = FastMCP("demo")


@mcp.tool
def add(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b


@mcp.tool
def echo(text: str) -> str:
    """Echo the input text."""
    return text


if __name__ == "__main__":
    mcp.run()  # stdio transport by default
