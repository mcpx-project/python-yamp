"""σ2 worker pool for server-originated calls (Python arm). Mirrors the Rust arm.

With ``set_worker_pool(cap, idle_ms)`` a tools/call that resolves to a local
handler runs as a bounded, cancellable concurrent task: the per-connection cap
serializes beyond it, ``notifications/cancelled`` stops a running call (and sends
no response), the idle deadline kills a stalled one, and a shutdown drains the
in-flight set. Off by default, so the route loop stays serial and byte-identical.
The event-gated handlers make concurrency deterministic (no reliance on sleeps
except the idle deadline, which is a must-fire timeout).
"""

import asyncio

from yamp import jsonrpc
from yamp.errors import INTERNAL_ERROR
from yamp.handler import Registry
from yamp.router import ForwardRouter, _CALL_METHODS
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


class PoolHandler:
    """`block` waits on a per-key gate the test releases (recording entry on a
    per-key event); `fast` returns at once."""

    id = "srv"

    def __init__(self):
        self.enter: dict = {}
        self.gate: dict = {}

    def list_tools(self):
        return [{"name": "block", "inputSchema": {"type": "object"}}, {"name": "fast", "inputSchema": {"type": "object"}}]

    async def call_tool(self, name, arguments):
        if name == "fast":
            return {"content": [{"type": "text", "text": "fast"}]}
        key = arguments["k"]
        self.enter[key].set()
        await self.gate[key].wait()
        return {"content": [{"type": "text", "text": key}]}


def _new(cap, idle_ms, handler):
    c2r, r2c = MemoryPipe(), MemoryPipe()
    router = ForwardRouter(LineTransport(c2r.reader, r2c), [], registry=Registry([handler])).set_worker_pool(cap, idle_ms)
    client = LineTransport(r2c.reader, c2r)
    return router, client


async def _handshake(client):
    await client.send(jsonrpc.encode(jsonrpc.request("i", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
    await client.receive()
    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))


def _call(id, name, arguments, meta=None):
    params = {"name": name, "arguments": arguments}
    if meta is not None:
        params["_meta"] = meta
    return jsonrpc.encode(jsonrpc.request(id, "tools/call", params))


def test_cap_serializes_beyond_the_limit():
    async def scenario():
        h = PoolHandler()
        for k in ("a", "b"):
            h.enter[k], h.gate[k] = asyncio.Event(), asyncio.Event()
        router, client = _new(1, 0, h)
        rt = asyncio.create_task(router.serve())
        await _handshake(client)

        await client.send(_call("A", "srv__block", {"k": "a"}))
        await asyncio.wait_for(h.enter["a"].wait(), 5)  # A holds the only slot
        await client.send(_call("B", "srv__block", {"k": "b"}))
        await asyncio.sleep(0.05)
        assert not h.enter["b"].is_set()  # B cannot enter while A holds the slot

        h.gate["a"].set()  # free the slot; B proceeds
        await asyncio.wait_for(h.enter["b"].wait(), 5)
        h.gate["b"].set()

        r1 = jsonrpc.decode(await client.receive())
        r2 = jsonrpc.decode(await client.receive())
        await client.send_eof()
        await asyncio.wait_for(rt, 5)
        return r1, r2

    r1, r2 = asyncio.run(scenario())
    assert r1["id"] == "A" and r1["result"]["content"][0]["text"] == "a"
    assert r2["id"] == "B" and r2["result"]["content"][0]["text"] == "b"


def test_cancellation_stops_a_running_call_with_no_response():
    async def scenario():
        h = PoolHandler()
        h.enter["a"], h.gate["a"] = asyncio.Event(), asyncio.Event()
        router, client = _new(0, 0, h)  # unbounded
        rt = asyncio.create_task(router.serve())
        await _handshake(client)

        await client.send(_call("A", "srv__block", {"k": "a"}))
        await asyncio.wait_for(h.enter["a"].wait(), 5)
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/cancelled", {"requestId": "A"})))
        await client.send(_call("B", "srv__fast", {}))
        await client.send(_call("C", "srv__fast", {}))

        # A was cancelled and sends nothing; only B and C respond.
        r1 = jsonrpc.decode(await client.receive())
        r2 = jsonrpc.decode(await client.receive())
        await client.send_eof()
        await asyncio.wait_for(rt, 5)
        return r1, r2

    r1, r2 = asyncio.run(scenario())
    assert {r1["id"], r2["id"]} == {"B", "C"}  # never "A"


def test_idle_deadline_kills_a_stalled_call():
    async def scenario():
        h = PoolHandler()
        h.enter["a"], h.gate["a"] = asyncio.Event(), asyncio.Event()  # gate never released
        router, client = _new(0, 50, h)  # 50ms idle deadline
        rt = asyncio.create_task(router.serve())
        await _handshake(client)

        await client.send(_call("A", "srv__block", {"k": "a"}))
        r = jsonrpc.decode(await client.receive())
        await client.send_eof()
        await asyncio.wait_for(rt, 5)
        return r

    r = asyncio.run(scenario())
    assert r["id"] == "A"
    assert r["error"]["code"] == INTERNAL_ERROR
    assert r["error"]["data"]["errorId"] == "E5000"


def test_progress_notifications_touch_the_inflight_call():
    async def scenario():
        h = PoolHandler()
        h.enter["a"], h.gate["a"] = asyncio.Event(), asyncio.Event()
        router, client = _new(0, 0, h)
        rt = asyncio.create_task(router.serve())
        await _handshake(client)

        await client.send(_call("A", "srv__block", {"k": "a"}, meta={"progressToken": "tok"}))
        await asyncio.wait_for(h.enter["a"].wait(), 5)
        # A tracked token resets the deadline; an unknown token and a token-less
        # progress are no-ops. All three exercise the receive-side touch path.
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/progress", {"progressToken": "tok", "progress": 1})))
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/progress", {"progressToken": "nope"})))
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/progress", {})))
        h.gate["a"].set()

        r = jsonrpc.decode(await client.receive())
        await client.send_eof()
        await asyncio.wait_for(rt, 5)
        return r

    r = asyncio.run(scenario())
    assert r["id"] == "A" and r["result"]["content"][0]["text"] == "a"


def test_shutdown_drains_in_flight_calls():
    async def scenario():
        h = PoolHandler()
        h.enter["a"], h.gate["a"] = asyncio.Event(), asyncio.Event()  # never released
        router, client = _new(0, 0, h)
        rt = asyncio.create_task(router.serve())
        await _handshake(client)

        await client.send(_call("A", "srv__block", {"k": "a"}))
        await asyncio.wait_for(h.enter["a"].wait(), 5)
        # Close with a call still running: the drain cancels it, no response.
        await client.send_eof()
        await asyncio.wait_for(rt, 5)  # must not hang

    asyncio.run(scenario())


def test_is_local_call_branches():
    # A tools/call whose prefix names a handler is pooled; a routed name is not;
    # a non-tools call method is never pooled.
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [], registry=Registry([PoolHandler()]))
        tools_cap = _CALL_METHODS["tools/call"]
        prompts_cap = _CALL_METHODS["prompts/get"]
        assert router._is_local_call({"params": {"name": "srv__block"}}, tools_cap) is True
        assert router._is_local_call({"params": {"name": "gh__x"}}, tools_cap) is False
        assert router._is_local_call({"params": {"name": "srv__x"}}, prompts_cap) is False

    asyncio.run(scenario())
