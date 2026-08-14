"""δ17 dispatch seam integration (Python arm). Mirrors the Rust arm.

Local handlers and routed backends share one namespaced surface: tools/list
merges both, a tools/call whose prefix names a handler is served locally
(including RestToMcp as a Conversion-mode handler), and an unknown method still
returns -32601.
"""

import asyncio
import json

from yamp import jsonrpc
from yamp.forward import PROXY_PROTOCOL_VERSION
from yamp.handler import BackendsHandler, Registry
from yamp.jsonrpc import METHOD_NOT_FOUND
from yamp.rest import RestToMcp
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe

SPEC = {
    "baseUrl": "https://api.example.com",
    "operations": [
        {"name": "get_user", "method": "GET", "path": "/users/{id}", "parameters": [{"name": "id", "in": "path"}]},
    ],
}


async def _mock_backend(read_pipe, write_pipe, name, tools):
    transport = LineTransport(read_pipe.reader, write_pipe)
    init = jsonrpc.decode(await transport.receive())
    await transport.send(
        jsonrpc.encode(
            jsonrpc.result(init["id"], {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": {"tools": {}}, "serverInfo": {"name": name}})
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
            await transport.send(
                jsonrpc.encode(jsonrpc.result(message["id"], {"content": [{"type": "text", "text": f"{name}:{message['params']['name']}"}]}))
            )


def _run(registry):
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        backend = Backend("gh", LineTransport(b2pr.reader, pr2b))
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [backend], registry=registry)
        router_task = asyncio.create_task(router.serve())
        backend_task = asyncio.create_task(_mock_backend(pr2b, b2pr, "gh", ["search"]))
        client = LineTransport(r2c.reader, c2r)
        await client.send(
            jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}}))
        )
        init = jsonrpc.decode(await client.receive())
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))

        async def call(id, method, params):
            await client.send(jsonrpc.encode(jsonrpc.request(id, method, params)))
            return jsonrpc.decode(await client.receive())

        listing = await call("l", "tools/list", {})
        backends_tool = await call("b", "tools/call", {"name": "yamp__backends", "arguments": {}})
        rest_tool = await call("r", "tools/call", {"name": "rest__get_user", "arguments": {"id": "42"}})
        routed = await call("s", "tools/call", {"name": "gh__search", "arguments": {}})
        unknown = await call("u", "does/not-exist", {})

        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(router_task, backend_task), timeout=5)
        return init, listing, backends_tool, rest_tool, routed, unknown

    return asyncio.run(scenario())


def test_dispatch_merges_and_serves_local_handlers():
    http_calls = []

    async def fake_http(method, url, headers, body):
        http_calls.append((method, url))
        return 200, b'{"name": "ada"}'

    registry = Registry([
        RestToMcp(SPEC, fake_http),  # Conversion-mode handler, id "rest"
        BackendsHandler(provider=lambda: [{"id": "gh"}]),  # meta-tool, id "yamp"
    ])
    init, listing, backends_tool, rest_tool, routed, unknown = _run(registry)

    # tools/list merges routed backend + both local handlers, all namespaced.
    names = {t["name"] for t in listing["result"]["tools"]}
    assert names == {"gh__search", "rest__get_user", "yamp__backends"}
    # capabilities advertise tools even though the local surface adds them.
    assert "tools" in init["result"]["capabilities"]

    # A meta-tool call is served entirely inside yamp.
    assert json.loads(backends_tool["result"]["content"][0]["text"]) == [{"id": "gh"}]

    # RestToMcp is served directly (Conversion mode): the call reached the fake
    # HTTP layer with the path parameter substituted, no backend process.
    assert rest_tool["result"]["content"][0]["text"] == '{"name": "ada"}'
    assert http_calls == [("GET", "https://api.example.com/users/42")]

    # A real backend tool still routes normally.
    assert routed["result"]["content"][0]["text"] == "gh:search"

    # An unknown method is still rejected.
    assert unknown["error"]["code"] == METHOD_NOT_FOUND
