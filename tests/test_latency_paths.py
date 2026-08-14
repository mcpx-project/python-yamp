"""Latency-tier coverage for paths added after the original δ0-δ5 tier.

Extends the ≤10 ms per-message budget to a many-backend (40, 100) fan-out and to
the signing/audit hot path, which the original tier did not exercise. Mirrors
the Rust arm's tests/latency_paths.rs.
"""

import asyncio
import statistics
import time

from yamp import jsonrpc
from yamp.cache import ListCache
from yamp.forward import PROXY_PROTOCOL_VERSION
from yamp.handler import BackendsHandler, Registry
from yamp.instrument import LATENCY_BUDGET_MS, within_budget
from yamp.router import Backend, ForwardRouter
from yamp.signing import AuditLog
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe

WARMUP = 20
SAMPLES = 200


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
        elif message["method"] == "tasks/get":
            await transport.send(jsonrpc.encode(jsonrpc.result(message["id"], {"status": "completed"})))


async def _measure(n, request, audit=None, cache=None, registry=None, token=None):
    c2r, r2c = MemoryPipe(), MemoryPipe()
    tasks = []
    backends = []
    for i in range(n):
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        backends.append(Backend(f"b{i}", LineTransport(b2pr.reader, pr2b), token=token))
        tasks.append(asyncio.create_task(_mock(pr2b, b2pr, f"b{i}")))
    router = ForwardRouter(LineTransport(c2r.reader, r2c), backends, audit=audit, cache=cache, registry=registry)
    router_task = asyncio.create_task(router.serve())
    client = LineTransport(r2c.reader, c2r)
    await client.send(
        jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}}))
    )
    await client.receive()
    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
    for _ in range(WARMUP):
        await client.send(request)
        await client.receive()
    latencies = []
    for _ in range(SAMPLES):
        start = time.perf_counter()
        await client.send(request)
        await client.receive()
        latencies.append((time.perf_counter() - start) * 1000.0)
    await client.send_eof()
    await asyncio.wait_for(asyncio.gather(router_task, *tasks), timeout=15)
    return latencies


def _assert_budget(label, latencies):
    median = statistics.median(latencies)
    under = sum(1 for x in latencies if within_budget(x)) / len(latencies)
    print(f"\n[latency {label}] median={median:.4f}ms budget={LATENCY_BUDGET_MS}ms within={under:.3%}")
    assert within_budget(median)
    assert under >= 0.99


def test_fanout_40_backends_within_budget():
    request = jsonrpc.encode(jsonrpc.request("l", "tools/list", {}))
    _assert_budget("fanout-40", asyncio.run(_measure(40, request)))


def test_fanout_100_backends_within_budget():
    request = jsonrpc.encode(jsonrpc.request("l", "tools/list", {}))
    _assert_budget("fanout-100", asyncio.run(_measure(100, request)))


def test_audited_call_within_budget():
    # A single backend, but every tools/call appends a signed attestation and
    # outcome to the audit log: the signing hot path must stay within budget.
    request = jsonrpc.encode(jsonrpc.request("s", "tools/call", {"name": "b0_tool", "arguments": {}}))
    _assert_budget("audited-call", asyncio.run(_measure(1, request, audit=AuditLog("secret"))))


def test_cache_hit_within_budget():
    # A shared list cache over 40 backends: the warmup fills the cache, so every
    # sampled tools/list is a fresh hit that skips the backend fan-out entirely
    # (SEP §6). The cache-hit path must stay within budget.
    request = jsonrpc.encode(jsonrpc.request("l", "tools/list", {}))
    _assert_budget("cache-hit", asyncio.run(_measure(40, request, cache=ListCache())))


def test_dispatch_handler_within_budget():
    # A tools/call to the yamp__backends meta-tool is served in-process by the
    # local handler registry (draft §5.3), never touching a backend. The dispatch
    # path must stay within budget.
    registry = Registry([BackendsHandler(provider=lambda: [{"id": "b0", "available": True}])])
    request = jsonrpc.encode(jsonrpc.request("d", "tools/call", {"name": "yamp__backends", "arguments": {}}))
    _assert_budget("dispatch-handler", asyncio.run(_measure(1, request, registry=registry)))


def test_tasks_get_within_budget():
    # A tasks/get reverse-resolves its namespaced taskId to the originating
    # backend and forwards with the backend's own id (SEP-2663). Task routing is
    # stateless, so the namespaced id is served directly. This path must stay
    # within budget.
    request = jsonrpc.encode(jsonrpc.request("t", "tasks/get", {"taskId": "b0__task-1"}))
    _assert_budget("tasks-get", asyncio.run(_measure(1, request)))


def test_auth_injection_within_budget():
    # Every request the backend forwards has the client's credential stripped and
    # the backend's own token injected into _meta (SEP §13.1, confused deputy).
    # The credential-injection path must stay within budget.
    request = jsonrpc.encode(jsonrpc.request("a", "tools/call", {"name": "b0_tool", "arguments": {}}))
    _assert_budget("auth-injection", asyncio.run(_measure(1, request, token="backend-token")))
