import asyncio
import json

from yamp import jsonrpc
from yamp.capability import PROXY_SEARCH_TOOL
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


async def _many_tools_backend(read_pipe, write_pipe, count):
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
            tools = [{"name": f"tool{i}", "description": f"desc{i}"} for i in range(count)]
            await t.send(jsonrpc.encode(jsonrpc.result(m["id"], {"tools": tools})))


def test_progressive_disclosure_and_search():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        b_in, b_out = MemoryPipe(), MemoryPipe()
        router = ForwardRouter(
            LineTransport(c2r.reader, r2c), [Backend("b", LineTransport(b_out.reader, b_in))],
            disclose=True, disclose_threshold=3,
        )
        tasks = [asyncio.create_task(router.serve()), asyncio.create_task(_many_tools_backend(b_in, b_out, 5))]
        client = LineTransport(r2c.reader, c2r)

        await client.send(jsonrpc.encode(jsonrpc.request(
            "1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))

        await client.send(jsonrpc.encode(jsonrpc.request("2", "tools/list", {})))
        listing = jsonrpc.decode(await client.receive())
        await client.send(jsonrpc.encode(jsonrpc.request(
            "3", "tools/call", {"name": PROXY_SEARCH_TOOL, "arguments": {"query": "tool4"}})))
        searched = jsonrpc.decode(await client.receive())

        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
        return listing, searched

    listing, searched = asyncio.run(scenario())
    names = [t["name"] for t in listing["result"]["tools"]]
    # over threshold: a curated prefix (3) plus the search meta-tool
    assert names == ["tool0", "tool1", "tool2", PROXY_SEARCH_TOOL]
    # the search meta-tool is served by the proxy and finds the hidden tool
    found = json.loads(searched["result"]["content"][0]["text"])
    assert found == ["tool4"]
