"""Runnable example: a forward router aggregating two backends.

Wires two in-process backends behind a ForwardRouter, runs the MCP handshake
and a tools/list, and prints the aggregated, namespaced tool surface.

Run:  cd python && ../.venv/bin/python examples/forward_router.py
(or with any Python 3.11+ once the yamp package is importable)
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from yamp import jsonrpc
from yamp.forward import PROXY_PROTOCOL_VERSION
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


async def backend(read_pipe, write_pipe, name, tools):
    """A minimal MCP backend: handshake, then answer tools/list."""
    t = LineTransport(read_pipe.reader, write_pipe)
    init = jsonrpc.decode(await t.receive())
    await t.send(jsonrpc.encode(jsonrpc.result(init["id"], {
        "protocolVersion": PROXY_PROTOCOL_VERSION,
        "capabilities": {"tools": {}},
        "serverInfo": {"name": f"{name}-server"},
    })))
    await t.receive()  # notifications/initialized
    while True:
        raw = await t.receive()
        if raw is None:
            await t.send_eof()
            return
        msg = jsonrpc.decode(raw)
        if msg["method"] == "tools/list":
            await t.send(jsonrpc.encode(jsonrpc.result(msg["id"], {
                "tools": [{"name": tool} for tool in tools]
            })))


async def main():
    c2r, r2c = MemoryPipe(), MemoryPipe()
    backends, tasks = [], []
    for name, tools in [("github", ["create_issue", "search"]), ("slack", ["post_message"])]:
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        backends.append(Backend(name, LineTransport(b2pr.reader, pr2b)))
        tasks.append(asyncio.create_task(backend(pr2b, b2pr, name, tools)))

    router = ForwardRouter(LineTransport(c2r.reader, r2c), backends)
    router_task = asyncio.create_task(router.serve())
    client = LineTransport(r2c.reader, c2r)

    await client.send(jsonrpc.encode(jsonrpc.request("1", "initialize", {
        "protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "demo"},
    })))
    init = jsonrpc.decode(await client.receive())
    print("client sees serverInfo:", init["result"]["serverInfo"])

    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
    await client.send(jsonrpc.encode(jsonrpc.request("2", "tools/list", {})))
    listing = jsonrpc.decode(await client.receive())
    names = [t["name"] for t in listing["result"]["tools"]]
    print("aggregated, namespaced tools:", json.dumps(names, indent=2))

    await client.send_eof()
    await asyncio.wait_for(asyncio.gather(router_task, *tasks), timeout=5)


if __name__ == "__main__":
    asyncio.run(main())
