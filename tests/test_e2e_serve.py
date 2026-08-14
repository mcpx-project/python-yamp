"""δ-testinfra: automated per-mode e2e for the served stdio (TCP) entrypoint.

Boots serve.py's per-connection handler against two live stub backends over real
TCP sockets and drives a full initialize -> tools/list -> tools/call as a client,
asserting the composed handshake, the namespaced surface, and a routed call.
Mirrors the Rust arm's tests/e2e_serve.rs, which spawns the yamp-serve binary.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # python/ for the entrypoint

import serve
from yamp import jsonrpc
from yamp.cache import ListCache
from yamp.forward import PROXY_PROTOCOL_VERSION
from yamp.transport.line import LineTransport


async def _stub_backend(reader, writer):
    transport = LineTransport(reader, serve.WriterEnd(writer))
    init = jsonrpc.decode(await transport.receive())
    await transport.send(
        jsonrpc.encode(
            jsonrpc.result(
                init["id"],
                {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": {"tools": {}}, "serverInfo": {"name": "stub"}},
            )
        )
    )
    await transport.receive()  # notifications/initialized
    while True:
        raw = await transport.receive()
        if raw is None:
            return
        message = jsonrpc.decode(raw)
        if message["method"] == "tools/list":
            await transport.send(jsonrpc.encode(jsonrpc.result(message["id"], {"tools": [{"name": "echo"}]})))
        elif message["method"] == "tools/call":
            text = f"echoed:{message['params']['name']}"
            await transport.send(jsonrpc.encode(jsonrpc.result(message["id"], {"content": [{"type": "text", "text": text}]})))


def test_e2e_stdio_initialize_list_call():
    async def scenario():
        backend_server = await asyncio.start_server(_stub_backend, "127.0.0.1", 0)
        b0 = backend_server.sockets[0].getsockname()[1]
        backend_server2 = await asyncio.start_server(_stub_backend, "127.0.0.1", 0)
        b1 = backend_server2.sockets[0].getsockname()[1]

        cache = ListCache()
        specs = [("b0", "127.0.0.1", b0), ("b1", "127.0.0.1", b1)]
        proxy = await asyncio.start_server(lambda r, w: serve.handle_client(r, w, specs, cache), "127.0.0.1", 0)
        pport = proxy.sockets[0].getsockname()[1]

        reader, writer = await asyncio.open_connection("127.0.0.1", pport)
        client = LineTransport(reader, serve.WriterEnd(writer))

        async def call(id, method, params):
            await client.send(jsonrpc.encode(jsonrpc.request(id, method, params)))
            return jsonrpc.decode(await asyncio.wait_for(client.receive(), timeout=5))

        await client.send(
            jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}}))
        )
        init = jsonrpc.decode(await asyncio.wait_for(client.receive(), timeout=5))
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
        listing = await call("l", "tools/list", {})
        called = await call("s", "tools/call", {"name": "b0__echo", "arguments": {}})

        writer.close()
        for server_obj in (proxy, backend_server, backend_server2):
            server_obj.close()
            await server_obj.wait_closed()
        return init, listing, called

    init, listing, called = asyncio.run(scenario())
    assert init["result"]["protocolVersion"] == PROXY_PROTOCOL_VERSION
    assert "tools" in init["result"]["capabilities"]
    names = {t["name"] for t in listing["result"]["tools"]}
    assert names == {"b0__echo", "b1__echo"}  # two backends -> prefixed surface
    assert called["result"]["content"][0]["text"] == "echoed:echo"  # routed, prefix stripped
