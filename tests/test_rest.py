import asyncio
import http.server
import threading

from yamp import jsonrpc
from yamp.rest import RestToMcp, default_http_call
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe

SPEC = {
    "baseUrl": "https://api.example.com",
    "operations": [
        {"name": "get_user", "method": "GET", "path": "/users/{id}",
         "parameters": [{"name": "id", "in": "path"}, {"name": "verbose", "in": "query"}]},
        {"name": "create_issue", "method": "POST", "path": "/issues", "body": ["title"]},
    ],
}


def test_rest_backend_behind_router_translates_calls():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        rt2rest, rest2rt = MemoryPipe(), MemoryPipe()
        calls = []

        async def fake_http(method, url, headers, body):
            calls.append((method, url, body))
            return 200, b'{"ok": true}'

        rest = RestToMcp(SPEC, fake_http)
        backend = Backend("api", LineTransport(rest2rt.reader, rt2rest))
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [backend])

        router_task = asyncio.create_task(router.serve())
        rest_task = asyncio.create_task(rest.serve(LineTransport(rt2rest.reader, rest2rt)))
        client = LineTransport(r2c.reader, c2r)

        await client.send(jsonrpc.encode(jsonrpc.request(
            "1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))

        await client.send(jsonrpc.encode(jsonrpc.request("2", "tools/list", {})))
        listing = jsonrpc.decode(await client.receive())

        await client.send(jsonrpc.encode(jsonrpc.request(
            "3", "tools/call", {"name": "get_user", "arguments": {"id": 5, "verbose": "true"}})))
        got = jsonrpc.decode(await client.receive())

        await client.send(jsonrpc.encode(jsonrpc.request(
            "4", "tools/call", {"name": "create_issue", "arguments": {"title": "hi"}})))
        made = jsonrpc.decode(await client.receive())

        await client.send(jsonrpc.encode(jsonrpc.request(
            "5", "tools/call", {"name": "missing", "arguments": {}})))
        unknown = jsonrpc.decode(await client.receive())

        # omit the optional query parameter: no query string
        await client.send(jsonrpc.encode(jsonrpc.request(
            "6", "tools/call", {"name": "get_user", "arguments": {"id": 7}})))
        await client.receive()

        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(router_task, rest_task), timeout=5)
        return listing, got, made, unknown, calls

    listing, got, made, unknown, calls = asyncio.run(scenario())
    names = [t["name"] for t in listing["result"]["tools"]]
    assert names == ["get_user", "create_issue"]
    # GET with path + query substitution
    assert calls[0] == ("GET", "https://api.example.com/users/5?verbose=true", None)
    assert got["result"]["content"][0]["text"] == '{"ok": true}'
    # POST with a JSON body
    assert calls[1] == ("POST", "https://api.example.com/issues", b'{"title": "hi"}')
    assert made["result"]["isError"] is False
    # unknown operation
    assert unknown["result"]["isError"] is True
    # omitted query parameter -> no query string
    assert calls[2] == ("GET", "https://api.example.com/users/7", None)


def test_rest_encodes_path_and_query_against_injection():
    calls = []

    async def fake_http(method, url, headers, body):
        calls.append(url)
        return 200, b"{}"

    async def scenario():
        rest = RestToMcp(SPEC, fake_http)
        # Adversarial arguments: a path-traversal value and a query value that
        # tries to smuggle an extra parameter. Both must be percent-encoded.
        await rest.call_tool("get_user", {"id": "../admin", "verbose": "a&b=c"})

    asyncio.run(scenario())
    assert calls[0] == "https://api.example.com/users/..%2Fadmin?verbose=a%26b%3Dc"


def test_rest_serve_replies_to_unknown_method():
    async def scenario():
        rt2rest, rest2rt = MemoryPipe(), MemoryPipe()
        rest = RestToMcp(SPEC, lambda *a: None)
        rest_task = asyncio.create_task(rest.serve(LineTransport(rt2rest.reader, rest2rt)))
        peer = LineTransport(rest2rt.reader, rt2rest)
        await peer.send(jsonrpc.encode(jsonrpc.request(
            "1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        await peer.receive()
        await peer.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
        # An unknown request (carries an id) must get a reply, not hang.
        await peer.send(jsonrpc.encode(jsonrpc.request("2", "resources/list", {})))
        reply = jsonrpc.decode(await peer.receive())
        await peer.send_eof()
        await asyncio.wait_for(rest_task, timeout=5)
        return reply

    reply = asyncio.run(scenario())
    assert reply["error"]["code"] == jsonrpc.METHOD_NOT_FOUND


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/ok":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"g":1}')
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"missing")

    def do_POST(self):
        length = int(self.headers.get("content-length", 0))
        self.rfile.read(length)
        self.send_response(201)
        self.end_headers()
        self.wfile.write(b'{"p":1}')

    def log_message(self, *args):
        pass


def test_default_http_call_against_local_server():
    server = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        async def scenario():
            ok = await default_http_call("GET", f"http://127.0.0.1:{port}/ok", {}, None)
            missing = await default_http_call("GET", f"http://127.0.0.1:{port}/nope", {}, None)
            posted = await default_http_call("POST", f"http://127.0.0.1:{port}/x", {"Content-Type": "application/json"}, b"{}")
            return ok, missing, posted

        ok, missing, posted = asyncio.run(scenario())
        assert ok == (200, b'{"g":1}')
        assert missing == (404, b"missing")
        assert posted == (201, b'{"p":1}')
    finally:
        server.shutdown()
        thread.join()
