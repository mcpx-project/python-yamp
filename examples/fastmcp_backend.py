"""Front a real FastMCP server with a yamp forward proxy.

yamp spawns the FastMCP server (fastmcp_server.py) as a subprocess and talks to
it over stdio, exactly as a client would. A yamp ForwardRouter presents it to
the client under the namespace `demo`, so its tools appear as `demo__add` and
`demo__echo`.

Requires FastMCP for the backend:  pip install fastmcp
Run:  cd python && ../.venv/bin/python examples/fastmcp_backend.py

Note: this example is not run in the yamp test suite, because it needs FastMCP
installed. The wiring is the point: any subprocess that speaks MCP over stdio is
a yamp backend.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from yamp import jsonrpc
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


class StreamWriterEnd:
    """Adapt an asyncio.StreamWriter (a subprocess stdin) to yamp's WriteEnd."""

    def __init__(self, writer: asyncio.StreamWriter) -> None:
        self._writer = writer

    async def write(self, data: bytes) -> None:
        self._writer.write(data)
        await self._writer.drain()

    def write_eof(self) -> None:
        if self._writer.can_write_eof():
            self._writer.write_eof()


async def main():
    server = Path(__file__).with_name("fastmcp_server.py")
    proc = await asyncio.create_subprocess_exec(
        sys.executable, str(server),
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
    )
    # yamp talks to the FastMCP subprocess over its stdio, using MCP stdio framing.
    backend = Backend("demo", LineTransport(proc.stdout, StreamWriterEnd(proc.stdin)))

    c2p, p2c = MemoryPipe(), MemoryPipe()
    router = ForwardRouter(LineTransport(c2p.reader, p2c), [backend])
    router_task = asyncio.create_task(router.serve())
    client = LineTransport(p2c.reader, c2p)

    await client.send(jsonrpc.encode(jsonrpc.request("1", "initialize", {
        "protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "demo"},
    })))
    init = jsonrpc.decode(await client.receive())
    print("serverInfo:", init["result"]["serverInfo"])

    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
    await client.send(jsonrpc.encode(jsonrpc.request("2", "tools/list", {})))
    listing = jsonrpc.decode(await client.receive())
    print("namespaced tools:", [t["name"] for t in listing["result"]["tools"]])

    await client.send_eof()
    router_task.cancel()
    proc.terminate()
    await proc.wait()


if __name__ == "__main__":
    asyncio.run(main())
