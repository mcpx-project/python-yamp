"""Server spine (σ0): pure-server mode is a registry with zero backends.

The router originates `server/discover` and `tools/list` from the handler
surface (no backends), and attaches the server's SEP-2549 cache directives to
those list results.
"""

import asyncio

from yamp import doctor, jsonrpc, version
from yamp.cache import CACHE_SCOPE_KEY, TTL_MS_KEY
from yamp.handler import BackendsHandler, Registry
from yamp.router import ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


def _run():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        registry = Registry([BackendsHandler(lambda: [])])
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [], registry=registry).set_list_directives(300000, "public")
        router_task = asyncio.create_task(router.serve())
        client = LineTransport(r2c.reader, c2r)

        await client.send(jsonrpc.encode(jsonrpc.request("i", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        init = jsonrpc.decode(await client.receive())
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))

        async def call(id, method, params):
            await client.send(jsonrpc.encode(jsonrpc.request(id, method, params)))
            return jsonrpc.decode(await client.receive())

        discover = await call("d", "server/discover", {})
        listing = await call("l", "tools/list", {})
        called = await call("c", "tools/call", {"name": "yamp__backends", "arguments": {}})

        await client.send_eof()
        await asyncio.wait_for(router_task, timeout=5)
        return init, discover, listing, called

    return asyncio.run(scenario())


def test_pure_server_mode_serves_from_handlers_with_cache_directives():
    init, discover, listing, called = _run()

    # The handshake succeeds with zero backends.
    assert init["result"]["serverInfo"]["name"] == "yamp"

    # tools/list is composed from the handler surface alone, and carries the
    # server's cache directives.
    names = [t["name"] for t in listing["result"]["tools"]]
    assert names == ["yamp__backends"]
    assert listing["result"][TTL_MS_KEY] == 300000
    assert listing["result"][CACHE_SCOPE_KEY] == "public"

    # server/discover answers from the same surface, also with directives.
    discover_names = [t["name"] for t in discover["result"]["tools"]]
    assert discover_names == ["yamp__backends"]
    assert discover["result"][TTL_MS_KEY] == 300000

    # A local handler originates its response (server behavior, no backend).
    assert "content" in called["result"]


def test_directives_are_absent_by_default():
    # Without set_list_directives, no cache metadata is attached (unchanged proxy
    # behavior); this is what keeps existing flows byte-identical.
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        registry = Registry([BackendsHandler(lambda: [])])
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [], registry=registry)
        router_task = asyncio.create_task(router.serve())
        client = LineTransport(r2c.reader, c2r)
        await client.send(jsonrpc.encode(jsonrpc.request("i", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        jsonrpc.decode(await client.receive())
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
        await client.send(jsonrpc.encode(jsonrpc.request("l", "tools/list", {})))
        listing = jsonrpc.decode(await client.receive())
        await client.send_eof()
        await asyncio.wait_for(router_task, timeout=5)
        return listing

    listing = asyncio.run(scenario())
    assert TTL_MS_KEY not in listing["result"]
    assert CACHE_SCOPE_KEY not in listing["result"]


def test_server_advertises_a_supported_protocol_version():
    # σ6 per-revision conformance: the pure-server handshake advertises a protocol
    # version drawn from the single supported set, whatever the client requested,
    # and a doctor preflight on that surface and version is clean.
    init, _discover, listing, _called = _run()
    advertised = init["result"]["protocolVersion"]
    assert advertised in version.SUPPORTED_PROTOCOL_VERSIONS
    findings = doctor.check_server(listing["result"]["tools"], advertised)
    assert doctor.is_ok(findings)
