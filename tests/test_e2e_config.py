"""δ-testinfra: automated e2e for the --config handler path (serve.py).

Boots serve.py's per-connection handler from a config that declares a routed MCP
backend, a REST Conversion handler (served locally, δ17), and the yamp__backends
meta-tool. Drives a full initialize -> tools/list -> tools/call over real TCP and
asserts all three surfaces appear on one namespaced tools/list and that a
tools/call reaches each: the routed backend, the local REST layer, and the local
meta-tool. Mirrors the Rust arm's tests/e2e_config.rs.
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # python/ for the entrypoint

import serve
from yamp import jsonrpc
from yamp.cache import ListCache
from yamp.config import from_dict, parse_address
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
        if message.get("method") == "tools/list":
            await transport.send(jsonrpc.encode(jsonrpc.result(message["id"], {"tools": [{"name": "echo"}]})))
        elif message.get("method") == "tools/call":
            text = f"echoed:{message['params']['name']}"
            await transport.send(jsonrpc.encode(jsonrpc.result(message["id"], {"content": [{"type": "text", "text": text}]})))


async def _stub_http(reader, writer):
    # A minimal HTTP/1.1 endpoint for the REST handler: any request -> 200 pong.
    await reader.readuntil(b"\r\n\r\n")
    body = b"pong"
    writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n" % len(body) + body)
    await writer.drain()
    writer.close()


def test_e2e_config_routes_backend_rest_and_meta_tool():
    async def scenario():
        backend_server = await asyncio.start_server(_stub_backend, "127.0.0.1", 0)
        b0 = backend_server.sockets[0].getsockname()[1]
        http_server = await asyncio.start_server(_stub_http, "127.0.0.1", 0)
        http_port = http_server.sockets[0].getsockname()[1]

        config = from_dict({
            "listen": "127.0.0.1:0",
            "backends": {"b0": {"address": f"127.0.0.1:{b0}"}},
            "handlers": {
                "metaTools": True,
                "rest": [{
                    "id": "api",
                    "baseUrl": f"http://127.0.0.1:{http_port}",
                    "operations": [{"name": "ping", "method": "GET", "path": "/ping"}],
                }],
            },
        })
        specs = [(b.id, *parse_address(b.addresses[0])) for b in config.backends]
        cache = ListCache()
        proxy = await asyncio.start_server(
            lambda r, w: serve.handle_client(r, w, specs, cache, config.handlers, config.namespacing),
            "127.0.0.1", 0,
        )
        pport = proxy.sockets[0].getsockname()[1]

        reader, writer = await asyncio.open_connection("127.0.0.1", pport)
        client = LineTransport(reader, serve.WriterEnd(writer))

        async def call(id, method, params):
            await client.send(jsonrpc.encode(jsonrpc.request(id, method, params)))
            return jsonrpc.decode(await asyncio.wait_for(client.receive(), timeout=5))

        await client.send(
            jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}}))
        )
        await asyncio.wait_for(client.receive(), timeout=5)
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
        listing = await call("l", "tools/list", {})
        rest_call = await call("r", "tools/call", {"name": "api__ping", "arguments": {}})
        meta_call = await call("m", "tools/call", {"name": "yamp__backends", "arguments": {}})
        backend_call = await call("s", "tools/call", {"name": "b0__echo", "arguments": {}})

        writer.close()
        for server_obj in (proxy, backend_server, http_server):
            server_obj.close()
            await server_obj.wait_closed()
        return listing, rest_call, meta_call, backend_call

    listing, rest_call, meta_call, backend_call = asyncio.run(scenario())
    names = {t["name"] for t in listing["result"]["tools"]}
    # One namespaced surface merges the routed backend, the REST handler, and the meta-tool.
    assert {"b0__echo", "api__ping", "yamp__backends"} <= names
    assert rest_call["result"]["content"][0]["text"] == "pong"  # reached the REST layer
    assert "b0" in meta_call["result"]["content"][0]["text"]  # local meta-tool reported the backend
    assert backend_call["result"]["content"][0]["text"] == "echoed:echo"  # routed
