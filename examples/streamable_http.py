"""End-to-end example: yamp as a Streamable HTTP proxy.

Starts two mock MCP backends over TCP, starts the Streamable HTTP server
(serve_streamable) in-process, and runs an HTTP client through the full MCP
flow: initialize (receives an Mcp-Session-Id), notifications/initialized, then
tools/list and a tools/call. Prints what the client sees.

Run:  cd python && ../.venv/bin/python examples/streamable_http.py
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from serve_streamable import WriterEnd, serve
from yamp import jsonrpc
from yamp.config import BackendConfig, ProxyConfig, Resilience
from yamp.transport.line import LineTransport


async def mock_backend(port, name, tools, push=None):
    async def handle(reader, writer):
        t = LineTransport(reader, WriterEnd(writer))
        init = jsonrpc.decode(await t.receive())
        await t.send(jsonrpc.encode(jsonrpc.result(init["id"], {
            "protocolVersion": "2025-06-18", "capabilities": {"tools": {}},
            "serverInfo": {"name": f"{name}-server"}})))
        await t.receive()  # notifications/initialized
        if push is not None:
            # A server-initiated notification; the proxy routes it to the
            # client's SSE stream.
            await t.send(jsonrpc.encode(jsonrpc.notification("notifications/message", {"level": "info", "data": push})))
        while True:
            raw = await t.receive()
            if raw is None:
                await t.send_eof()
                return
            msg = jsonrpc.decode(raw)
            if msg["method"] == "tools/list":
                await t.send(jsonrpc.encode(jsonrpc.result(msg["id"], {"tools": [{"name": x} for x in tools]})))
            elif msg["method"] == "tools/call":
                await t.send(jsonrpc.encode(jsonrpc.result(msg["id"], {"content": [{"type": "text", "text": f"{name}:{msg['params']['name']}"}]})))
    server = await asyncio.start_server(handle, "127.0.0.1", port)
    async with server:
        await server.serve_forever()


async def http_post(host, port, body, session_id=None):
    reader, writer = await asyncio.open_connection(host, port)
    head = (
        f"POST /mcp HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
    )
    if session_id:
        head += f"Mcp-Session-Id: {session_id}\r\n"
    head += "Connection: close\r\n\r\n"
    writer.write(head.encode() + body)
    await writer.drain()
    raw_head = await reader.readuntil(b"\r\n\r\n")
    lines = raw_head.split(b"\r\n")
    status = lines[0].decode().split(" ", 1)[1]
    headers = {}
    for line in lines[1:]:
        name, sep, value = line.partition(b":")
        if sep:
            headers[name.decode().strip().lower()] = value.decode().strip()
    length = int(headers.get("content-length", "0"))
    resp = await reader.readexactly(length) if length else b""
    writer.close()
    return status, headers, resp


async def main():
    tasks = [
        asyncio.create_task(mock_backend(9101, "github", ["create_issue", "search"], push="a github webhook fired")),
        asyncio.create_task(mock_backend(9102, "slack", ["post_message"])),
    ]
    config = ProxyConfig(
        listen="127.0.0.1:9100",
        backends=[
            BackendConfig("github", ["127.0.0.1:9101"]),
            BackendConfig("slack", ["127.0.0.1:9102"]),
        ],
        resilience=Resilience(),
    )
    server_task = asyncio.create_task(serve(config))
    await asyncio.sleep(0.2)  # let listeners come up

    init = jsonrpc.encode(jsonrpc.request("1", "initialize", {
        "protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "demo"}}))
    status, headers, body = await http_post("127.0.0.1", 9100, init)
    session = headers["mcp-session-id"]
    print("initialize ->", status, "| Mcp-Session-Id:", session)
    print("  serverInfo:", json.loads(body)["result"]["serverInfo"])

    await http_post("127.0.0.1", 9100, jsonrpc.encode(jsonrpc.notification("notifications/initialized")), session)

    _, _, listing = await http_post("127.0.0.1", 9100, jsonrpc.encode(jsonrpc.request("2", "tools/list", {})), session)
    print("  tools:", [t["name"] for t in json.loads(listing)["result"]["tools"]])

    _, _, called = await http_post("127.0.0.1", 9100, jsonrpc.encode(jsonrpc.request("3", "tools/call", {"name": "github__search"})), session)
    print("  tools/call github__search ->", json.loads(called)["result"]["content"][0]["text"])

    # Open the server-to-client SSE stream (GET /mcp) and read the first event.
    reader, writer = await asyncio.open_connection("127.0.0.1", 9100)
    writer.write(
        f"GET /mcp HTTP/1.1\r\nHost: 127.0.0.1\r\nAccept: text/event-stream\r\n"
        f"Mcp-Session-Id: {session}\r\nConnection: keep-alive\r\n\r\n".encode()
    )
    await writer.drain()
    await reader.readuntil(b"\r\n\r\n")  # SSE response headers
    event = await reader.readuntil(b"\n\n")
    pushed = json.loads(event.split(b"data: ", 1)[1].rstrip(b"\n"))
    print("  GET /mcp SSE stream, backend-initiated push ->", pushed["method"], pushed["params"]["data"])
    writer.close()

    for t in tasks + [server_task]:
        t.cancel()


if __name__ == "__main__":
    asyncio.run(main())
