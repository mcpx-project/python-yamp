"""Soak / leak coverage for the per-backend demux reader under churn.

The router spawns one reader task per backend (`Backend._read_loop`) that
demultiplexes backend replies into per-request futures held in `_pending`. A bug
in the teardown path would leak a reader task, a mock, or a pending future on
every session, growing without bound under churn. Nothing else in the suite runs
enough sessions to surface that.

This drives many full router sessions back to back and asserts two invariants
that a leak would violate: every backend drains its `_pending` map to empty when
its session ends, and the live asyncio-task count returns to its pre-churn
baseline instead of growing by O(sessions). The in-memory transports use no real
file descriptors, so task and pending-map growth are the faithful in-process
analog of the fd/task/pending-map leaks the soak tier guards against. Mirrors the
Rust arm's tests/soak.rs.
"""

import asyncio

from yamp import jsonrpc
from yamp.forward import PROXY_PROTOCOL_VERSION
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe

CYCLES = 100
REQUESTS_PER_CYCLE = 5
BACKENDS = 2


async def _mock(read_pipe, write_pipe, name):
    transport = LineTransport(read_pipe.reader, write_pipe)
    init = jsonrpc.decode(await transport.receive())
    await transport.send(
        jsonrpc.encode(
            jsonrpc.result(
                init["id"],
                {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": {"tools": {}}, "serverInfo": {"name": name}},
            )
        )
    )
    await transport.receive()  # notifications/initialized
    while True:
        raw = await transport.receive()
        if raw is None:
            await transport.send_eof()
            return
        message = jsonrpc.decode(raw)
        if message["method"] == "tools/list":
            await transport.send(jsonrpc.encode(jsonrpc.result(message["id"], {"tools": [{"name": f"{name}_tool"}]})))
        elif message["method"] == "tools/call":
            await transport.send(jsonrpc.encode(jsonrpc.result(message["id"], {"content": [{"type": "text", "text": "ok"}]})))


async def _one_session():
    c2r, r2c = MemoryPipe(), MemoryPipe()
    backends = []
    mocks = []
    for i in range(BACKENDS):
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        backends.append(Backend(f"b{i}", LineTransport(b2pr.reader, pr2b)))
        mocks.append(asyncio.create_task(_mock(pr2b, b2pr, f"b{i}")))
    router = ForwardRouter(LineTransport(c2r.reader, r2c), backends)
    router_task = asyncio.create_task(router.serve())
    client = LineTransport(r2c.reader, c2r)

    await client.send(
        jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}}))
    )
    await client.receive()
    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
    for i in range(REQUESTS_PER_CYCLE):
        await client.send(jsonrpc.encode(jsonrpc.request(f"l{i}", "tools/list", {})))
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.request(f"k{i}", "tools/call", {"name": "b0__b0_tool", "arguments": {}})))
        await client.receive()
    await client.send_eof()
    await asyncio.wait_for(asyncio.gather(router_task, *mocks), timeout=15)

    # Every backend must drain its pending map when the session ends: a reply
    # future left behind is a demux leak.
    for backend in backends:
        assert backend._pending == {}, f"backend {backend.id} leaked {len(backend._pending)} pending futures"


async def _soak():
    await asyncio.sleep(0)  # let any startup task settle before the snapshot
    baseline = len(asyncio.all_tasks())
    for _ in range(CYCLES):
        await _one_session()
    # A backend's reader task is cancelled (not awaited) on close; give the loop a
    # few ticks to finish unwinding the cancellations before counting.
    for _ in range(5):
        await asyncio.sleep(0)
    grown = len(asyncio.all_tasks()) - baseline
    # Only this coroutine should remain; a per-session task leak would grow this
    # by O(CYCLES). Allow a single slot of slack for loop bookkeeping.
    assert grown <= 1, f"leaked {grown} tasks over {CYCLES} churn cycles"


def test_demux_reader_no_leak_under_churn():
    asyncio.run(_soak())
