import asyncio

import pytest

from yamp import jsonrpc
from yamp.forward import HandshakeError, PROXY_PROTOCOL_VERSION
from yamp.router import (
    Backend,
    ForwardRouter,
    INTERNAL_ERROR,
    METHOD_NOT_FOUND,
)
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


def _client_router(backends):
    c2r, r2c = MemoryPipe(), MemoryPipe()
    router = ForwardRouter(LineTransport(c2r.reader, r2c), backends)
    client = LineTransport(r2c.reader, c2r)
    return client, router


def test_rejects_non_initialize_first_message():
    async def scenario():
        client, router = _client_router([])
        await client.send(jsonrpc.encode(jsonrpc.request("x", "tools/list", {})))
        with pytest.raises(HandshakeError):
            await router.serve()
        reply = jsonrpc.decode(await client.receive())
        assert reply["error"]["code"] == -32600

    asyncio.run(scenario())


def test_empty_client_closes_cleanly():
    async def scenario():
        client, router = _client_router([])
        client._writer.write_eof()  # client end closes immediately
        await asyncio.wait_for(router.serve(), timeout=5)

    asyncio.run(scenario())


async def _mock_backend(read_pipe, write_pipe, tools_call_reply):
    transport = LineTransport(read_pipe.reader, write_pipe)
    init = jsonrpc.decode(await transport.receive())
    await transport.send(
        jsonrpc.encode(
            jsonrpc.result(
                init["id"],
                {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": {"tools": {}},
                 "serverInfo": {"name": "b-server"}},
            )
        )
    )
    await transport.receive()
    while True:
        raw = await transport.receive()
        if raw is None:
            await transport.send_eof()
            return
        message = jsonrpc.decode(raw)
        if message["method"] == "tools/list":
            await transport.send(
                jsonrpc.encode(jsonrpc.result(message["id"], {"tools": [{"name": "t"}]}))
            )
        elif message["method"] == "tools/call":
            await transport.send(jsonrpc.encode(tools_call_reply(message["id"])))


async def _one_backend_session(tools_call_reply):
    c2r, r2c = MemoryPipe(), MemoryPipe()
    pr2b, b2pr = MemoryPipe(), MemoryPipe()
    backend = Backend("b", LineTransport(b2pr.reader, pr2b))
    router = ForwardRouter(LineTransport(c2r.reader, r2c), [backend])
    router_task = asyncio.create_task(router.serve())
    backend_task = asyncio.create_task(_mock_backend(pr2b, b2pr, tools_call_reply))
    client = LineTransport(r2c.reader, c2r)
    await client.send(
        jsonrpc.encode(
            jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})
        )
    )
    await client.receive()
    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
    return client, router_task, backend_task


def test_unroutable_method_and_ignored_notification():
    async def scenario():
        client, router_task, backend_task = await _one_backend_session(
            lambda id: jsonrpc.result(id, {"content": []})
        )
        # A client notification (no id) is ignored, not routed.
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/cancelled")))
        # A non-tools request is not routable in δ2.
        await client.send(jsonrpc.encode(jsonrpc.request("p", "ping", {})))
        reply = jsonrpc.decode(await client.receive())
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(router_task, backend_task), timeout=5)
        return reply

    reply = asyncio.run(scenario())
    assert reply["error"]["code"] == METHOD_NOT_FOUND


def test_non_object_params_do_not_crash_serve_loop():
    async def scenario():
        client, router_task, backend_task = await _one_backend_session(
            lambda id: jsonrpc.result(id, {"content": []})
        )
        # A hostile client sends params as JSON null, then as an array; neither
        # is a JSON object, and neither must crash the serve loop (regression for
        # the _params guard, matching the Rust arm's tolerant params access).
        await client.send(jsonrpc.encode({"jsonrpc": "2.0", "id": "n1", "method": "tools/call", "params": None}))
        first = jsonrpc.decode(await client.receive())
        await client.send(jsonrpc.encode({"jsonrpc": "2.0", "id": "n2", "method": "tools/list", "params": [1, 2]}))
        second = jsonrpc.decode(await client.receive())
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(router_task, backend_task), timeout=5)
        return first, second

    first, second = asyncio.run(scenario())
    # The point is that neither non-object params crashed the loop: both requests
    # got a reply, and the second (a tools/list) was still served normally.
    assert first["id"] == "n1" and ("result" in first or "error" in first)
    assert second["id"] == "n2"
    assert len(second["result"]["tools"]) == 1


def test_backend_error_is_propagated():
    async def scenario():
        client, router_task, backend_task = await _one_backend_session(
            lambda id: jsonrpc.error(id, INTERNAL_ERROR, "boom")
        )
        await client.send(
            jsonrpc.encode(jsonrpc.request("s", "tools/call", {"name": "b__t", "arguments": {}}))
        )
        reply = jsonrpc.decode(await client.receive())
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(router_task, backend_task), timeout=5)
        return reply

    reply = asyncio.run(scenario())
    assert reply["error"]["code"] == INTERNAL_ERROR
    assert reply["error"]["message"] == "boom"


def test_backend_closing_mid_request_raises():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        backend = Backend("b", LineTransport(b2pr.reader, pr2b))
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [backend])

        async def dying_backend():
            transport = LineTransport(pr2b.reader, b2pr)
            init = jsonrpc.decode(await transport.receive())
            await transport.send(
                jsonrpc.encode(
                    jsonrpc.result(init["id"], {"capabilities": {}, "serverInfo": {}})
                )
            )
            await transport.receive()  # initialized
            await transport.receive()  # the tools/list request
            await transport.send_eof()  # die instead of answering

        backend_task = asyncio.create_task(dying_backend())
        router_task = asyncio.create_task(router.serve())
        client = LineTransport(r2c.reader, c2r)
        await client.send(
            jsonrpc.encode(
                jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})
            )
        )
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
        await client.send(jsonrpc.encode(jsonrpc.request("l", "tools/list", {})))
        with pytest.raises(HandshakeError):
            await router_task
        await backend_task

    asyncio.run(scenario())
