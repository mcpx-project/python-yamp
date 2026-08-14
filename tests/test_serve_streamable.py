"""δ18: the Streamable HTTP entrypoint publishes the Server Card (SEP-2127).

Boots the real request handler on an ephemeral port and asserts a plain
GET /.well-known/mcp returns the proxy's self-description, with no session and
no backends involved.
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # python/ for the entrypoint

import serve_streamable
from yamp import jsonrpc
from yamp.config import BackendConfig, Resilience
from yamp.forward import PROXY_PROTOCOL_VERSION
from yamp.transport.line import LineTransport


def _state(backends, policy=None):
    return {"backends": backends, "resilience": Resilience(), "policy": policy}


async def _http_get(port, path):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
    await writer.drain()
    head = await reader.readuntil(b"\r\n\r\n")
    status = head.split(b"\r\n", 1)[0].decode()
    headers = {}
    for line in head.split(b"\r\n")[1:]:
        name, sep, value = line.partition(b":")
        if sep:
            headers[name.decode().strip().lower()] = value.decode().strip()
    length = int(headers.get("content-length", "0"))
    body = await reader.readexactly(length) if length else b""
    writer.close()
    await writer.wait_closed()
    return status, headers, body


async def _http_post(port, body: bytes, session_id=None):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    head = f"POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Length: {len(body)}\r\n"
    if session_id is not None:
        head += f"Mcp-Session-Id: {session_id}\r\n"
    writer.write(head.encode() + b"\r\n" + body)
    await writer.drain()
    raw_head = await reader.readuntil(b"\r\n\r\n")
    status = raw_head.split(b"\r\n", 1)[0].decode()
    headers = {}
    for line in raw_head.split(b"\r\n")[1:]:
        name, sep, value = line.partition(b":")
        if sep:
            headers[name.decode().strip().lower()] = value.decode().strip()
    length = int(headers.get("content-length", "0"))
    resp_body = await reader.readexactly(length) if length else b""
    writer.close()
    await writer.wait_closed()
    return status, headers, resp_body


async def _stub_backend(reader, writer):
    transport = LineTransport(reader, serve_streamable.WriterEnd(writer))
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


def test_e2e_streamable_initialize_list_call():
    # Full Streamable HTTP flow over real sockets: initialize (mints a session),
    # notifications/initialized, tools/list (composed surface), tools/call
    # (routed, prefix stripped), against two live stub backends.
    async def scenario():
        b0 = await asyncio.start_server(_stub_backend, "127.0.0.1", 0)
        b1 = await asyncio.start_server(_stub_backend, "127.0.0.1", 0)
        backends = [
            BackendConfig(id="b0", addresses=[f"127.0.0.1:{b0.sockets[0].getsockname()[1]}"]),
            BackendConfig(id="b1", addresses=[f"127.0.0.1:{b1.sockets[0].getsockname()[1]}"]),
        ]
        sessions: dict = {}
        handler = serve_streamable.make_handler(_state(backends), sessions)
        proxy = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = proxy.sockets[0].getsockname()[1]
        try:
            init_body = jsonrpc.encode(
                jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})
            )
            status, headers, body = await _http_post(port, init_body)
            sid = headers.get("mcp-session-id")
            init = jsonrpc.decode(body)
            # notifications/initialized on the same session (a notification -> 202).
            note_status, _h, _b = await _http_post(
                port, jsonrpc.encode(jsonrpc.notification("notifications/initialized")), session_id=sid
            )
            _s, _h, list_body = await _http_post(
                port, jsonrpc.encode(jsonrpc.request("l", "tools/list", {})), session_id=sid
            )
            _s, _h, call_body = await _http_post(
                port,
                jsonrpc.encode(jsonrpc.request("s", "tools/call", {"name": "b0__echo", "arguments": {}})),
                session_id=sid,
            )
            return status, sid, init, note_status, jsonrpc.decode(list_body), jsonrpc.decode(call_body)
        finally:
            for session in sessions.values():
                await session.close()
            for server_obj in (proxy, b0, b1):
                server_obj.close()
                await server_obj.wait_closed()

    status, sid, init, note_status, listing, called = asyncio.run(scenario())
    assert status == "HTTP/1.1 200 OK"
    assert sid  # a session id was minted
    assert init["result"]["protocolVersion"] == PROXY_PROTOCOL_VERSION
    assert note_status == "HTTP/1.1 202 Accepted"
    names = {t["name"] for t in listing["result"]["tools"]}
    assert names == {"b0__echo", "b1__echo"}  # two backends -> prefixed surface
    assert called["result"]["content"][0]["text"] == "echoed:echo"  # routed, prefix stripped


def test_well_known_server_card():
    async def scenario():
        handler = serve_streamable.make_handler(_state([]), {})
        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            return await _http_get(port, "/.well-known/mcp")
        finally:
            server.close()
            await server.wait_closed()

    status, headers, body = asyncio.run(scenario())
    assert status == "HTTP/1.1 200 OK"
    assert headers["content-type"] == "application/json"
    card = json.loads(body)
    assert card["role"] == "intermediary"
    assert "protocolVersions" in card
    assert card["transports"] == ["stdio", "streamable-http"]


def test_status_endpoint():
    # GET /status returns the read-only operational snapshot: proxy identity plus
    # the configured backends and live session count. No session required.
    async def scenario():
        backends = [BackendConfig(id="b0", addresses=["127.0.0.1:1"]), BackendConfig(id="b1", addresses=["127.0.0.1:2"])]
        handler = serve_streamable.make_handler(_state(backends), {})
        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            return await _http_get(port, "/status")
        finally:
            server.close()
            await server.wait_closed()

    line, headers, body = asyncio.run(scenario())
    assert line == "HTTP/1.1 200 OK"
    assert headers["content-type"] == "application/json"
    snap = json.loads(body)
    assert snap["status"] == "ok"
    assert snap["role"] == "intermediary"
    assert [b["id"] for b in snap["backends"]] == ["b0", "b1"]
    assert snap["sessions"] == 0


def test_tap_redacts_client_capture(capsys):
    # With --tap, each client request is captured to stderr with credentials masked.
    async def scenario():
        handler = serve_streamable.make_handler(_state([]), {}, tap_enabled=True)
        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            body = json.dumps(
                {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                 "params": {"_meta": {"authorization": "Bearer SECRET"}}}
            ).encode()
            return await _http_post(port, body)  # no session: 400, but tapped first
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(scenario())
    err = capsys.readouterr().err
    assert "Bearer SECRET" not in err
    assert '"authorization":"***"' in err
    assert '"direction":"c2s"' in err


def test_malformed_json_post_is_400():
    # A malformed JSON body must return 400, not silently drop the connection.
    async def scenario():
        handler = serve_streamable.make_handler(_state([]), {})
        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            return await _http_post(port, b"{ this is not valid json ")
        finally:
            server.close()
            await server.wait_closed()

    status, _headers, body = asyncio.run(scenario())
    assert status == "HTTP/1.1 400 Bad Request"
    assert json.loads(body)["error"]["code"] == serve_streamable.INVALID_PARAMS


def test_unknown_path_is_404():
    async def scenario():
        handler = serve_streamable.make_handler(_state([]), {})
        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            return await _http_get(port, "/nope")
        finally:
            server.close()
            await server.wait_closed()

    status, _headers, _body = asyncio.run(scenario())
    assert status == "HTTP/1.1 404 Not Found"
