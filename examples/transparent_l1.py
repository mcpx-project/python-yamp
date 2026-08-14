"""Runnable example: a transparent Level 1 proxy that filters by header.

The proxy reads the Mcp-Method header to decide, without parsing the body. Here
it blocks tools/call and allows tools/list. The body is deliberately opaque, to
show routing and filtering happen on headers alone.

Run:  cd python && ../.venv/bin/python examples/transparent_l1.py
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from yamp.transparent import HeaderPolicy, TransparentL1, encode_envelope
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


async def backend(read_pipe, write_pipe):
    t = LineTransport(read_pipe.reader, write_pipe)
    while True:
        raw = await t.receive()
        if raw is None:
            await t.send_eof()
            return
        await t.send(encode_envelope({"Mcp-From": "backend"}, "ok"))


async def main():
    c2p, p2c = MemoryPipe(), MemoryPipe()
    p2b, b2p = MemoryPipe(), MemoryPipe()
    policy = HeaderPolicy(blocked_methods={"tools/call"})
    proxy = TransparentL1(LineTransport(c2p.reader, p2c), LineTransport(b2p.reader, p2b), policy)
    backend_task = asyncio.create_task(backend(p2b, b2p))
    proxy_task = asyncio.create_task(proxy.serve())
    client = LineTransport(p2c.reader, c2p)

    # allowed: reaches the backend
    await client.send(encode_envelope({"Mcp-Method": "tools/list"}, "OPAQUE"))
    allowed = json.loads(await client.receive())
    print("tools/list allowed, backend replied:", allowed["body"])

    # blocked: never reaches the backend
    await client.send(encode_envelope({"Mcp-Method": "tools/call"}, "OPAQUE"))
    blocked = json.loads(await client.receive())
    print("tools/call blocked by policy, proxy replied:", json.loads(blocked["body"])["error"])
    print("messages blocked so far:", proxy.blocked)

    await client.send_eof()
    await asyncio.wait_for(asyncio.gather(proxy_task, backend_task), timeout=5)


if __name__ == "__main__":
    asyncio.run(main())
