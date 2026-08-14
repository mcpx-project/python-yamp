"""σ5 output-size bounds and graceful drain (Python arm). Mirrors the Rust arm.

`set_output_limit(max_bytes)` caps a server-originated result: a local handler's
result whose encoded form exceeds the cap is rejected with a server-class error
instead of emitted. `set_drain_timeout(ms)` gives in-flight server-originated work
a bounded window to finish (and send its response) on shutdown before it is
cancelled; the default of 0 cancels immediately.
"""

import asyncio

from yamp import jsonrpc, server
from yamp.errors import INTERNAL_ERROR
from yamp.handler import Registry
from yamp.router import ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


# --- pure surface ---


def test_max_output_bytes_is_the_frame_ceiling():
    from yamp.transport.framed import MAX_FRAME_BYTES

    assert server.MAX_OUTPUT_BYTES == MAX_FRAME_BYTES


def test_exceeds_output_cap():
    small = {"content": [{"type": "text", "text": "ok"}]}
    assert server.exceeds_output_cap(small, 1000) is False
    assert server.exceeds_output_cap(small, 10) is True
    assert server.exceeds_output_cap(small, 0) is False  # unbounded


# --- output cap wired into the served route ---


class BoundsHandler:
    id = "srv"

    def list_tools(self):
        return [
            {"name": "big", "inputSchema": {"type": "object"}},
            {"name": "small", "inputSchema": {"type": "object"}},
        ]

    async def call_tool(self, name, arguments):
        text = "x" * 500 if name == "big" else "ok"
        return {"content": [{"type": "text", "text": text}]}


def _new(limit):
    c2r, r2c = MemoryPipe(), MemoryPipe()
    router = ForwardRouter(LineTransport(c2r.reader, r2c), [], registry=Registry([BoundsHandler()])).set_output_limit(limit)
    client = LineTransport(r2c.reader, c2r)
    return router, client


async def _handshake(client):
    await client.send(jsonrpc.encode(jsonrpc.request("i", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
    await client.receive()
    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))


async def _call(client, id, name):
    await client.send(jsonrpc.encode(jsonrpc.request(id, "tools/call", {"name": name, "arguments": {}})))
    return jsonrpc.decode(await client.receive())


def test_oversize_local_result_is_a_server_error():
    async def scenario():
        router, client = _new(100)  # small cap
        rt = asyncio.create_task(router.serve())
        await _handshake(client)
        big = await _call(client, "A", "srv__big")
        small = await _call(client, "B", "srv__small")
        await client.send_eof()
        await asyncio.wait_for(rt, 5)
        return big, small

    big, small = asyncio.run(scenario())
    # The 500-byte result exceeds the 100-byte cap: a server-class error, not a result.
    assert "result" not in big
    assert big["error"]["code"] == INTERNAL_ERROR
    assert big["error"]["data"]["errorId"] == "E5000"
    # The small result is under the cap and served normally.
    assert small["result"]["content"][0]["text"] == "ok"


def test_default_limit_does_not_trip():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [], registry=Registry([BoundsHandler()]))  # default cap
        client = LineTransport(r2c.reader, c2r)
        rt = asyncio.create_task(router.serve())
        await _handshake(client)
        big = await _call(client, "A", "srv__big")
        await client.send_eof()
        await asyncio.wait_for(rt, 5)
        return big

    big = asyncio.run(scenario())
    assert big["result"]["content"][0]["text"] == "x" * 500  # served, well under 64 MiB


# --- graceful drain ---


class GateHandler:
    id = "srv"

    def __init__(self):
        self.enter = asyncio.Event()
        self.gate = asyncio.Event()

    def list_tools(self):
        return [{"name": "block", "inputSchema": {"type": "object"}}]

    async def call_tool(self, name, arguments):
        self.enter.set()
        await self.gate.wait()
        return {"content": [{"type": "text", "text": "done"}]}


def test_graceful_drain_lets_in_flight_call_finish():
    async def scenario():
        h = GateHandler()
        c2r, r2c = MemoryPipe(), MemoryPipe()
        router = (
            ForwardRouter(LineTransport(c2r.reader, r2c), [], registry=Registry([h]))
            .set_worker_pool(0, 0)
            .set_drain_timeout(5000)  # generous window
        )
        client = LineTransport(r2c.reader, c2r)
        rt = asyncio.create_task(router.serve())
        await _handshake(client)
        await client.send(jsonrpc.encode(jsonrpc.request("A", "tools/call", {"name": "srv__block", "arguments": {}})))
        await asyncio.wait_for(h.enter.wait(), 5)  # the call is in flight
        await client.send_eof()  # shutdown begins; drain waits for the call
        h.gate.set()  # let it finish inside the drain window
        r = jsonrpc.decode(await client.receive())
        await asyncio.wait_for(rt, 5)
        return r

    r = asyncio.run(scenario())
    assert r["id"] == "A"
    assert r["result"]["content"][0]["text"] == "done"  # completed, not cancelled


def test_zero_drain_cancels_in_flight_call():
    # The default (0) drains by cancelling at once: the call never responds, and
    # serve still returns promptly (no hang).
    async def scenario():
        h = GateHandler()  # gate never released
        c2r, r2c = MemoryPipe(), MemoryPipe()
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [], registry=Registry([h])).set_worker_pool(0, 0)
        client = LineTransport(r2c.reader, c2r)
        rt = asyncio.create_task(router.serve())
        await _handshake(client)
        await client.send(jsonrpc.encode(jsonrpc.request("A", "tools/call", {"name": "srv__block", "arguments": {}})))
        await asyncio.wait_for(h.enter.wait(), 5)
        await client.send_eof()
        await asyncio.wait_for(rt, 5)  # must not hang

    asyncio.run(scenario())
