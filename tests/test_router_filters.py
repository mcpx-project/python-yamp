"""ε0 filter-chain seam in the served router (Python arm). Mirrors the Rust arm.

A filter chain runs on each client call request before routing: a deny returns a
clean -32001 and the backend is never touched; a mutate substitutes the call
arguments the backend receives; absent a chain the request routes unchanged.
"""

import asyncio
import json

from yamp import filters, jsonrpc, signing
from yamp.forward import PROXY_PROTOCOL_VERSION
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


class _Fixed(filters.Filter):
    """A filter that returns a fixed verdict, recording whether it was reached."""

    def __init__(self, verdict):
        self._verdict = verdict

    def evaluate(self, hook, message):
        return self._verdict


async def _echo_backend(read_pipe, write_pipe):
    # Echoes the arguments it received, so the client result reveals exactly what
    # reached the backend (used to observe a mutation, or its absence).
    transport = LineTransport(read_pipe.reader, write_pipe)
    init = jsonrpc.decode(await transport.receive())
    await transport.send(
        jsonrpc.encode(
            jsonrpc.result(init["id"], {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": {"tools": {}}, "serverInfo": {"name": "gh"}})
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
            await transport.send(jsonrpc.encode(jsonrpc.result(message["id"], {"tools": [{"name": "echo"}]})))
        elif message["method"] == "tools/call":
            args = message["params"].get("arguments")
            await transport.send(
                jsonrpc.encode(jsonrpc.result(message["id"], {"content": [{"type": "text", "text": json.dumps(args)}]}))
            )


def _run(chain, audit=None):
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        backend = Backend("gh", LineTransport(b2pr.reader, pr2b))
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [backend], filter_chain=chain, audit=audit)
        router_task = asyncio.create_task(router.serve())
        backend_task = asyncio.create_task(_echo_backend(pr2b, b2pr))
        client = LineTransport(r2c.reader, c2r)
        await client.send(
            jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}}))
        )
        jsonrpc.decode(await client.receive())
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
        await client.send(
            jsonrpc.encode(jsonrpc.request("s", "tools/call", {"name": "gh__echo", "arguments": {"secret": "raw"}}))
        )
        response = jsonrpc.decode(await client.receive())
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(router_task, backend_task), timeout=5)
        return response

    return asyncio.run(scenario())


def test_deny_returns_policy_error_and_audits():
    from yamp import errors

    audit = signing.AuditLog("k")
    response = _run(filters.FilterChain([_Fixed({"kind": "deny", "reason": "blocked by dlp"})]), audit=audit)
    assert response["error"]["code"] == errors.POLICY_DENIED
    assert response["error"]["message"] == "blocked by dlp"
    # The deny is recorded as a failed outcome (best-effort accountability).
    records = [entry["record"] for entry in audit.records]
    assert any(r["type"] == "outcome" and r["ok"] is False for r in records)


def test_mutate_substitutes_arguments_reaching_backend():
    response = _run(filters.FilterChain([_Fixed({"kind": "mutate", "arguments": {"secret": "[redacted]"}})]))
    reached = json.loads(response["result"]["content"][0]["text"])
    assert reached == {"secret": "[redacted]"}


def test_allow_routes_unchanged():
    response = _run(filters.FilterChain([_Fixed({"kind": "allow"})]))
    reached = json.loads(response["result"]["content"][0]["text"])
    assert reached == {"secret": "raw"}
