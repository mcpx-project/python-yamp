import asyncio

from yamp import jsonrpc
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


async def _backend_with_push(read_pipe, write_pipe):
    t = LineTransport(read_pipe.reader, write_pipe)
    init = jsonrpc.decode(await t.receive())
    await t.send(jsonrpc.encode(jsonrpc.result(init["id"], {
        "capabilities": {"tools": {}}, "serverInfo": {"name": "b"}})))
    await t.receive()  # notifications/initialized
    # A server-initiated notification, not a response to any request.
    await t.send(jsonrpc.encode(jsonrpc.notification("notifications/message", {"level": "info", "data": "hi"})))
    while True:
        raw = await t.receive()
        if raw is None:
            await t.send_eof()
            return
        msg = jsonrpc.decode(raw)
        if msg.get("method") == "tools/call":
            await t.send(jsonrpc.encode(jsonrpc.result(msg["id"], {"content": [{"type": "text", "text": "ok"}]})))


async def _handshake(client):
    await client.send(jsonrpc.encode(jsonrpc.request(
        "c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
    await client.receive()  # initialize response
    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))


def test_backend_initiated_message_forwarded_to_client():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        backend = Backend("b", LineTransport(b2pr.reader, pr2b))
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [backend])  # default sink -> client
        router_task = asyncio.create_task(router.serve())
        backend_task = asyncio.create_task(_backend_with_push(pr2b, b2pr))
        client = LineTransport(r2c.reader, c2r)

        await _handshake(client)
        # The backend pushed a notification; with the default sink it is
        # forwarded to the client, not confused with a response.
        pushed = jsonrpc.decode(await client.receive())
        # Normal request/response still demuxes correctly by id.
        await client.send(jsonrpc.encode(jsonrpc.request("c2", "tools/call", {"name": "b__x"})))
        called = jsonrpc.decode(await client.receive())

        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(router_task, backend_task), timeout=5)
        return pushed, called

    pushed, called = asyncio.run(scenario())
    assert pushed["method"] == "notifications/message"
    assert pushed["params"]["data"] == "hi"
    assert called["result"]["content"][0]["text"] == "ok"


def test_server_message_routed_to_custom_sink():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        received = []

        async def sink(backend_id, message):
            received.append((backend_id, message))

        backend = Backend("b", LineTransport(b2pr.reader, pr2b))
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [backend], on_server_message=sink)
        router_task = asyncio.create_task(router.serve())
        backend_task = asyncio.create_task(_backend_with_push(pr2b, b2pr))
        client = LineTransport(r2c.reader, c2r)

        await _handshake(client)
        # With a custom sink, the client transport carries only responses.
        await client.send(jsonrpc.encode(jsonrpc.request("c2", "tools/call", {"name": "b__x"})))
        called = jsonrpc.decode(await client.receive())

        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(router_task, backend_task), timeout=5)
        return received, called

    received, called = asyncio.run(scenario())
    assert received and received[0][0] == "b"
    assert received[0][1]["method"] == "notifications/message"
    assert called["result"]["content"][0]["text"] == "ok"
