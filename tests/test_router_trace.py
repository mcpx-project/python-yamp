import asyncio

from yamp import jsonrpc
from yamp.observability import PROXY_HOPS_KEY, TRACEPARENT
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


async def _echo_meta_backend(read_pipe, write_pipe):
    t = LineTransport(read_pipe.reader, write_pipe)
    init = jsonrpc.decode(await t.receive())
    await t.send(jsonrpc.encode(jsonrpc.result(init["id"], {"capabilities": {"tools": {}}, "serverInfo": {"name": "b"}})))
    await t.receive()
    while True:
        raw = await t.receive()
        if raw is None:
            await t.send_eof()
            return
        m = jsonrpc.decode(raw)
        if m.get("method") == "tools/list":
            await t.send(jsonrpc.encode(jsonrpc.result(m["id"], {"tools": [{"name": "t"}]})))
        elif m.get("method") == "tools/call":
            # echo back the _meta the proxy forwarded
            await t.send(jsonrpc.encode(jsonrpc.result(m["id"], {"received_meta": m["params"].get("_meta", {})})))


def test_forward_path_adds_hop_and_trace_context():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        b_in, b_out = MemoryPipe(), MemoryPipe()
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [Backend("b", LineTransport(b_out.reader, b_in))])
        tasks = [asyncio.create_task(router.serve()), asyncio.create_task(_echo_meta_backend(b_in, b_out))]
        client = LineTransport(r2c.reader, c2r)

        await client.send(jsonrpc.encode(jsonrpc.request(
            "1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))

        await client.send(jsonrpc.encode(jsonrpc.request("2", "tools/list", {})))
        listing = jsonrpc.decode(await client.receive())
        await client.send(jsonrpc.encode(jsonrpc.request("3", "tools/call", {"name": "t"})))
        called = jsonrpc.decode(await client.receive())

        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
        return listing, called

    listing, called = asyncio.run(scenario())
    # the response to the client carries this proxy's hop
    assert listing["result"]["_meta"][PROXY_HOPS_KEY][0]["mode"] == "forward"
    assert called["result"]["_meta"][PROXY_HOPS_KEY][0]["mode"] == "forward"
    # the request forwarded to the backend carried a hop and a trace context
    forwarded = called["result"]["received_meta"]
    assert forwarded[PROXY_HOPS_KEY][0]["mode"] == "forward"
    assert TRACEPARENT in forwarded


def test_trace_can_be_disabled():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        b_in, b_out = MemoryPipe(), MemoryPipe()
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [Backend("b", LineTransport(b_out.reader, b_in))], trace=False)
        tasks = [asyncio.create_task(router.serve()), asyncio.create_task(_echo_meta_backend(b_in, b_out))]
        client = LineTransport(r2c.reader, c2r)

        await client.send(jsonrpc.encode(jsonrpc.request(
            "1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
        await client.send(jsonrpc.encode(jsonrpc.request("2", "tools/call", {"name": "t"})))
        called = jsonrpc.decode(await client.receive())

        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
        return called

    called = asyncio.run(scenario())
    assert "_meta" not in called["result"]
    assert called["result"]["received_meta"] == {}
