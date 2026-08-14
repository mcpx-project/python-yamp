import asyncio

from yamp import jsonrpc
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


async def _backend(read_pipe, write_pipe, name):
    """A backend serving tools, prompts, and resources; calls echo what they got."""
    t = LineTransport(read_pipe.reader, write_pipe)
    init = jsonrpc.decode(await t.receive())
    await t.send(jsonrpc.encode(jsonrpc.result(init["id"], {
        "capabilities": {"tools": {}, "prompts": {}, "resources": {}}, "serverInfo": {"name": name}})))
    await t.receive()
    while True:
        raw = await t.receive()
        if raw is None:
            await t.send_eof()
            return
        m = jsonrpc.decode(raw)
        method, params = m.get("method"), m.get("params", {})
        if method == "tools/list":
            body = {"tools": [{"name": "search"}]}
        elif method == "prompts/list":
            body = {"prompts": [{"name": "greet"}]}
        elif method == "resources/list":
            body = {"resources": [{"uri": f"file:///{name}.md", "name": name}]}
        elif method in ("tools/call", "prompts/get"):
            body = {"echo_name": params.get("name")}
        elif method == "resources/read":
            body = {"echo_uri": params.get("uri")}
        else:
            body = {}
        await t.send(jsonrpc.encode(jsonrpc.result(m["id"], body)))


async def _handshake(client):
    await client.send(jsonrpc.encode(jsonrpc.request(
        "c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
    await client.receive()
    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))


def test_prompts_and_resources_namespaced_and_routed():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        a_in, a_out = MemoryPipe(), MemoryPipe()
        b_in, b_out = MemoryPipe(), MemoryPipe()
        backends = [Backend("a", LineTransport(a_out.reader, a_in)),
                    Backend("b", LineTransport(b_out.reader, b_in))]
        router = ForwardRouter(LineTransport(c2r.reader, r2c), backends)
        tasks = [asyncio.create_task(router.serve()),
                 asyncio.create_task(_backend(a_in, a_out, "a")),
                 asyncio.create_task(_backend(b_in, b_out, "b"))]
        client = LineTransport(r2c.reader, c2r)

        await _handshake(client)

        async def call(id, method, params):
            await client.send(jsonrpc.encode(jsonrpc.request(id, method, params)))
            return jsonrpc.decode(await client.receive())

        prompts = await call("2", "prompts/list", {})
        resources = await call("3", "resources/list", {})
        got_prompt = await call("4", "prompts/get", {"name": "a__greet"})
        read = await call("5", "resources/read", {"uri": "file:///b/b.md"})

        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
        return prompts, resources, got_prompt, read

    prompts, resources, got_prompt, read = asyncio.run(scenario())
    assert sorted(p["name"] for p in prompts["result"]["prompts"]) == ["a__greet", "b__greet"]
    assert sorted(r["uri"] for r in resources["result"]["resources"]) == ["file:///a/a.md", "file:///b/b.md"]
    # prompts/get a__greet routed to backend a, forwarded original name "greet"
    assert got_prompt["result"]["echo_name"] == "greet"
    # resources/read routed to backend b, forwarded original uri
    assert read["result"]["echo_uri"] == "file:///b.md"


def test_single_backend_passes_names_through():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        a_in, a_out = MemoryPipe(), MemoryPipe()
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [Backend("solo", LineTransport(a_out.reader, a_in))])
        tasks = [asyncio.create_task(router.serve()),
                 asyncio.create_task(_backend(a_in, a_out, "solo"))]
        client = LineTransport(r2c.reader, c2r)

        await _handshake(client)
        await client.send(jsonrpc.encode(jsonrpc.request("2", "tools/list", {})))
        listing = jsonrpc.decode(await client.receive())
        await client.send(jsonrpc.encode(jsonrpc.request("3", "tools/call", {"name": "search"})))
        called = jsonrpc.decode(await client.receive())

        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
        return listing, called

    listing, called = asyncio.run(scenario())
    # single backend: no prefix on the surface
    assert [t["name"] for t in listing["result"]["tools"]] == ["search"]
    # and the call forwards the name unchanged
    assert called["result"]["echo_name"] == "search"
