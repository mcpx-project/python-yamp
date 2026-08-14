"""δ21: the served ForwardRouter appends attestation/outcome audit records.

A routed call emits a pre-call attestation and a post-call outcome to a shared,
tamper-evident AuditLog (SEP-2828/2787), best-effort and off the reply path. A
resilient backend failure still records an outcome (ok=False).
"""

import asyncio

from yamp import jsonrpc, signing
from yamp.forward import PROXY_PROTOCOL_VERSION
from yamp.resilience import CircuitBreaker
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


async def _mock_backend(read_pipe, write_pipe, name, tools, drop_calls=False):
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
            await transport.send(jsonrpc.encode(jsonrpc.result(message["id"], {"tools": [{"name": t} for t in tools]})))
        elif message["method"] == "tools/call":
            if drop_calls:
                continue  # never respond: the router's request times out
            await transport.send(
                jsonrpc.encode(jsonrpc.result(message["id"], {"content": [{"type": "text", "text": "ok"}]}))
            )


async def _run(audit, backend, call_name):
    c2r, r2c = MemoryPipe(), MemoryPipe()
    name, tools, drop, resilient = backend
    pr2b, b2pr = MemoryPipe(), MemoryPipe()
    breaker = CircuitBreaker(3, 30.0) if resilient else None
    timeout = 0.3 if resilient else None
    backend_obj = Backend(name, LineTransport(b2pr.reader, pr2b), breaker=breaker, request_timeout=timeout)
    backend_task = asyncio.create_task(_mock_backend(pr2b, b2pr, name, tools, drop))
    router = ForwardRouter(LineTransport(c2r.reader, r2c), [backend_obj], audit=audit)
    router_task = asyncio.create_task(router.serve())
    client = LineTransport(r2c.reader, c2r)
    await client.send(
        jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}}))
    )
    await client.receive()
    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
    await client.send(jsonrpc.encode(jsonrpc.request("s", "tools/call", {"name": call_name, "arguments": {}})))
    response = jsonrpc.decode(await client.receive())
    await client.send_eof()
    await asyncio.wait_for(asyncio.gather(router_task, backend_task), timeout=5)
    return response


def test_audit_records_attestation_and_outcome():
    audit = signing.AuditLog("secret")
    response = asyncio.run(_run(audit, ("b", ["search"], False, False), "search"))
    assert response["result"]["content"][0]["text"] == "ok"
    kinds = [entry["record"]["type"] for entry in audit.records]
    assert kinds == ["attestation", "outcome"]
    assert audit.records[0]["record"]["name"] == "search"
    assert audit.records[0]["record"]["principal"] == "anonymous"
    assert audit.records[1]["record"]["ok"] is True
    assert audit.verify()  # signatures and hash chain intact


def test_audit_records_failure_outcome_on_resilient_backend():
    audit = signing.AuditLog("secret")
    response = asyncio.run(_run(audit, ("b", ["search"], True, True), "search"))
    assert "error" in response  # backend timed out -> SERVER_NOT_AVAILABLE
    kinds = [entry["record"]["type"] for entry in audit.records]
    assert kinds == ["attestation", "outcome"]
    assert audit.records[1]["record"]["ok"] is False
    assert audit.verify()
