import asyncio

import pytest

from yamp import jsonrpc
from yamp.forward import (
    ForwardProxy,
    HandshakeError,
    PROXY_PROTOCOL_VERSION,
    PROXY_SERVER_INFO,
)
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe

BACKEND_INFO = {"name": "backend-xyz", "version": "9.9"}
BACKEND_CAPS = {"tools": {"listChanged": True}}


async def _mock_backend(read_pipe: MemoryPipe, write_pipe: MemoryPipe, log: dict):
    transport = LineTransport(read_pipe.reader, write_pipe)

    init = jsonrpc.decode(await transport.receive())
    log["backend_methods"].append(init["method"])
    log["backend_saw_client"] = init["params"]["clientInfo"]["name"]
    await transport.send(
        jsonrpc.encode(
            jsonrpc.result(
                init["id"],
                {
                    "protocolVersion": PROXY_PROTOCOL_VERSION,
                    "capabilities": BACKEND_CAPS,
                    "serverInfo": BACKEND_INFO,
                },
            )
        )
    )
    note = jsonrpc.decode(await transport.receive())
    log["backend_methods"].append(note["method"])

    while True:
        raw = await transport.receive()
        if raw is None:
            await transport.send_eof()
            return
        message = jsonrpc.decode(raw)
        if message.get("method") == "tools/list":
            await transport.send(
                jsonrpc.encode(
                    jsonrpc.result(message["id"], {"tools": [{"name": "echo"}]})
                )
            )


async def _run_session():
    c2r, r2c = MemoryPipe(), MemoryPipe()
    r2b, b2r = MemoryPipe(), MemoryPipe()
    proxy = ForwardProxy(
        client=LineTransport(c2r.reader, r2c),
        backend=LineTransport(b2r.reader, r2b),
    )
    log = {"backend_methods": [], "backend_saw_client": None}
    backend_task = asyncio.create_task(_mock_backend(r2b, b2r, log))
    proxy_task = asyncio.create_task(proxy.serve())

    client = LineTransport(r2c.reader, c2r)
    received_raw = []

    await client.send(
        jsonrpc.encode(
            jsonrpc.request(
                "c1",
                "initialize",
                {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "test-client"},
                },
            )
        )
    )
    init_resp_raw = await client.receive()
    received_raw.append(init_resp_raw)

    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
    await client.send(jsonrpc.encode(jsonrpc.request("c2", "tools/list", {})))
    tools_resp_raw = await client.receive()
    received_raw.append(tools_resp_raw)

    await client.send_eof()
    await asyncio.wait_for(asyncio.gather(proxy_task, backend_task), timeout=5)

    return {
        "init": jsonrpc.decode(init_resp_raw),
        "tools": jsonrpc.decode(tools_resp_raw),
        "received_raw": received_raw,
        "log": log,
        "proxy": proxy,
    }


def test_gate1_dual_handshake_with_backend():
    out = asyncio.run(_run_session())
    # The proxy performed its own initialize + initialized with the backend,
    # presenting itself (not the test client) as the initiator.
    assert out["log"]["backend_methods"] == ["initialize", "notifications/initialized"]
    assert out["log"]["backend_saw_client"] == PROXY_SERVER_INFO["name"]


def test_gate2_client_sees_proxy_serverinfo_not_backend():
    out = asyncio.run(_run_session())
    assert out["init"]["result"]["serverInfo"] == PROXY_SERVER_INFO
    assert out["proxy"].backend_server_info == BACKEND_INFO  # held internally


def test_gate3_protocol_version_is_proxys_highest():
    out = asyncio.run(_run_session())
    # Client requested 2024-11-05; the proxy still presents its own highest.
    assert out["init"]["result"]["protocolVersion"] == PROXY_PROTOCOL_VERSION


def test_gate4_requests_forwarded_and_no_backend_identity_leak():
    out = asyncio.run(_run_session())
    assert out["init"]["result"]["capabilities"] == BACKEND_CAPS
    assert out["tools"]["result"]["tools"] == [{"name": "echo"}]
    for raw in out["received_raw"]:
        assert b"backend-xyz" not in raw


def test_rejects_non_initialize_first_message():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        r2b, b2r = MemoryPipe(), MemoryPipe()
        proxy = ForwardProxy(
            client=LineTransport(c2r.reader, r2c),
            backend=LineTransport(b2r.reader, r2b),
        )
        client = LineTransport(r2c.reader, c2r)
        await client.send(jsonrpc.encode(jsonrpc.request("x", "tools/list", {})))
        with pytest.raises(HandshakeError):
            await proxy.serve()
        reply = jsonrpc.decode(await client.receive())
        assert reply["error"]["code"] == -32600

    asyncio.run(scenario())


async def _measure_forward_latency(samples: int = 300):
    import time

    c2r, r2c = MemoryPipe(), MemoryPipe()
    r2b, b2r = MemoryPipe(), MemoryPipe()
    proxy = ForwardProxy(
        client=LineTransport(c2r.reader, r2c),
        backend=LineTransport(b2r.reader, r2b),
    )
    log = {"backend_methods": [], "backend_saw_client": None}
    backend_task = asyncio.create_task(_mock_backend(r2b, b2r, log))
    proxy_task = asyncio.create_task(proxy.serve())

    client = LineTransport(r2c.reader, c2r)
    await client.send(
        jsonrpc.encode(
            jsonrpc.request(
                "c1",
                "initialize",
                {"protocolVersion": "2024-11-05", "capabilities": {},
                 "clientInfo": {"name": "test-client"}},
            )
        )
    )
    await client.receive()
    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))

    latencies = []
    request = jsonrpc.encode(jsonrpc.request("t", "tools/list", {}))
    for _ in range(50):
        await client.send(request)
        await client.receive()
    for _ in range(samples):
        start = time.perf_counter()
        await client.send(request)
        await client.receive()
        latencies.append((time.perf_counter() - start) * 1000.0)

    await client.send_eof()
    await asyncio.wait_for(asyncio.gather(proxy_task, backend_task), timeout=5)
    return latencies


def test_forward_path_latency_within_budget():
    import statistics

    from yamp.instrument import within_budget

    latencies = asyncio.run(_measure_forward_latency())
    median = statistics.median(latencies)
    under = sum(1 for x in latencies if within_budget(x)) / len(latencies)
    print(f"\n[latency δ1 forward] median={median:.4f}ms within={under:.3%}")
    assert within_budget(median)
    assert under >= 0.99


def test_backend_closing_during_initialize_raises():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        r2b, b2r = MemoryPipe(), MemoryPipe()
        proxy = ForwardProxy(
            client=LineTransport(c2r.reader, r2c),
            backend=LineTransport(b2r.reader, r2b),
        )

        async def dead_backend():
            transport = LineTransport(r2b.reader, b2r)
            await transport.receive()  # consume the proxy's initialize
            await transport.send_eof()  # then die without responding

        backend_task = asyncio.create_task(dead_backend())
        client = LineTransport(r2c.reader, c2r)
        await client.send(
            jsonrpc.encode(
                jsonrpc.request(
                    "c1",
                    "initialize",
                    {"protocolVersion": "2024-11-05", "capabilities": {},
                     "clientInfo": {"name": "test-client"}},
                )
            )
        )
        with pytest.raises(HandshakeError):
            await proxy.serve()
        await backend_task

    asyncio.run(scenario())


def test_empty_client_closes_cleanly():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        r2b, b2r = MemoryPipe(), MemoryPipe()
        proxy = ForwardProxy(
            client=LineTransport(c2r.reader, r2c),
            backend=LineTransport(b2r.reader, r2b),
        )
        c2r.write_eof()
        await asyncio.wait_for(proxy.serve(), timeout=5)

    asyncio.run(scenario())
