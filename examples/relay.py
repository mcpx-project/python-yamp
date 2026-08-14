"""Runnable example: a Layer 1 relay bridging stdio and HTTP framing.

The client speaks newline-delimited JSON (MCP stdio). The backend speaks
content-length framing (HTTP style). The relay bridges the two without
inspecting or modifying the payload.

Run:  cd python && ../.venv/bin/python examples/relay.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from yamp.relay import Relay
from yamp.transport.framed import FramedTransport
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


async def framed_echo(read_pipe, write_pipe):
    t = FramedTransport(read_pipe.reader, write_pipe)
    while True:
        message = await t.receive()
        if message is None:
            await t.send_eof()
            return
        await t.send(message)


async def main():
    c2r, r2c = MemoryPipe(), MemoryPipe()
    r2b, b2r = MemoryPipe(), MemoryPipe()
    relay = Relay(LineTransport(c2r.reader, r2c), FramedTransport(b2r.reader, r2b))
    backend = asyncio.create_task(framed_echo(r2b, b2r))
    relay_task = asyncio.create_task(relay.run())

    client = LineTransport(r2c.reader, c2r)
    await client.send(b'{"jsonrpc":"2.0","id":1,"method":"ping"}')
    reply = await client.receive()
    print("client (stdio framing) sent a ping; backend (HTTP framing) echoed it:")
    print("  ", reply.decode())

    await client.send_eof()
    await asyncio.wait_for(asyncio.gather(relay_task, backend), timeout=5)


if __name__ == "__main__":
    asyncio.run(main())
