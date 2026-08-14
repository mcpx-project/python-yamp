import asyncio

from yamp import jsonrpc
from yamp.resilience import PROXY_PARTIAL_KEY, SERVER_NOT_AVAILABLE, CircuitBreaker
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


async def _responder(read_pipe, write_pipe, tools, answer_calls=True, answer_list=True):
    t = LineTransport(read_pipe.reader, write_pipe)
    init = jsonrpc.decode(await t.receive())
    await t.send(jsonrpc.encode(jsonrpc.result(init["id"], {
        "capabilities": {"tools": {}}, "serverInfo": {"name": "b"}})))
    await t.receive()  # notifications/initialized
    while True:
        raw = await t.receive()
        if raw is None:
            await t.send_eof()
            return
        msg = jsonrpc.decode(raw)
        if msg.get("method") == "tools/list" and answer_list:
            await t.send(jsonrpc.encode(jsonrpc.result(msg["id"], {"tools": [{"name": n} for n in tools]})))
        elif msg.get("method") == "tools/call" and answer_calls:
            await t.send(jsonrpc.encode(jsonrpc.result(msg["id"], {"content": [{"type": "text", "text": "ok"}]})))
        elif msg.get("method") == "ping" and answer_calls:
            await t.send(jsonrpc.encode(jsonrpc.result(msg["id"], {})))
        # otherwise ignore (used to force a timeout)


async def _dead_on_handshake(read_pipe, write_pipe):
    t = LineTransport(read_pipe.reader, write_pipe)
    await t.receive()  # initialize
    await t.send_eof()  # die without responding, so the handshake fails


async def _handshake(client):
    await client.send(jsonrpc.encode(jsonrpc.request(
        "c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
    await client.receive()
    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))


def test_open_breaker_gives_partial_list_and_unavailable_call():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        gh_in, gh_out = MemoryPipe(), MemoryPipe()
        sl_in, sl_out = MemoryPipe(), MemoryPipe()

        # slack is down at startup: its handshake fails, so its breaker opens
        # and it is left out of the surface (startup tolerance).
        github = Backend("github", LineTransport(gh_out.reader, gh_in), breaker=CircuitBreaker(reset_timeout=100))
        slack = Backend("slack", LineTransport(sl_out.reader, sl_in),
                        breaker=CircuitBreaker(failure_threshold=1, reset_timeout=100))
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [github, slack])

        router_task = asyncio.create_task(router.serve())
        gh_task = asyncio.create_task(_responder(gh_in, gh_out, ["search"]))
        sl_task = asyncio.create_task(_dead_on_handshake(sl_in, sl_out))
        client = LineTransport(r2c.reader, c2r)

        await _handshake(client)
        # slack was unavailable before it was ever advertised, so the list is
        # partial but there is no surface transition to announce.
        await client.send(jsonrpc.encode(jsonrpc.request("c2", "tools/list", {})))
        listing = jsonrpc.decode(await client.receive())
        await client.send(jsonrpc.encode(jsonrpc.request("c3", "tools/call", {"name": "slack__post"})))
        blocked = jsonrpc.decode(await client.receive())
        await client.send(jsonrpc.encode(jsonrpc.request("c4", "tools/call", {"name": "github__search"})))
        allowed = jsonrpc.decode(await client.receive())

        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(router_task, gh_task, sl_task), timeout=5)
        return listing, blocked, allowed

    listing, blocked, allowed = asyncio.run(scenario())
    names = [t["name"] for t in listing["result"]["tools"]]
    assert names == ["github__search"]
    assert listing["result"]["_meta"][PROXY_PARTIAL_KEY]["unavailable_backends"] == ["slack"]
    assert blocked["error"]["code"] == SERVER_NOT_AVAILABLE
    assert allowed["result"]["content"][0]["text"] == "ok"


def test_timeout_trips_breaker():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        gh_in, gh_out = MemoryPipe(), MemoryPipe()

        breaker = CircuitBreaker(failure_threshold=1, reset_timeout=100)
        github = Backend("github", LineTransport(gh_out.reader, gh_in), breaker=breaker, request_timeout=0.05)
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [github])

        router_task = asyncio.create_task(router.serve())
        # answer_calls=False so tools/call never gets a response and times out
        gh_task = asyncio.create_task(_responder(gh_in, gh_out, ["search"], answer_calls=False))
        client = LineTransport(r2c.reader, c2r)

        await _handshake(client)
        await client.send(jsonrpc.encode(jsonrpc.request("c2", "tools/call", {"name": "github__search"})))
        first = jsonrpc.decode(await client.receive())        # -32003 after timeout
        changed = jsonrpc.decode(await client.receive())      # breaker opened -> list_changed
        state_open = not github.available()
        await client.send(jsonrpc.encode(jsonrpc.request("c3", "tools/call", {"name": "github__search"})))
        second = jsonrpc.decode(await client.receive())       # -32003 fast, breaker open

        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(router_task, gh_task), timeout=5)
        return first, changed, state_open, second

    first, changed, state_open, second = asyncio.run(scenario())
    assert first["error"]["code"] == SERVER_NOT_AVAILABLE
    assert changed["method"] == "notifications/tools/list_changed"
    assert state_open is True
    assert second["error"]["code"] == SERVER_NOT_AVAILABLE


def test_health_ping_success_keeps_backend_available():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        gh_in, gh_out = MemoryPipe(), MemoryPipe()
        github = Backend("github", LineTransport(gh_out.reader, gh_in),
                         breaker=CircuitBreaker(reset_timeout=100), request_timeout=1.0)
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [github])
        router_task = asyncio.create_task(router.serve())
        gh_task = asyncio.create_task(_responder(gh_in, gh_out, ["search"]))
        client = LineTransport(r2c.reader, c2r)

        await _handshake(client)
        await router._health_check_once(github)  # ping succeeds
        available = github.available()
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(router_task, gh_task), timeout=5)
        return available

    assert asyncio.run(scenario()) is True


def test_health_loop_opens_breaker_on_ping_failure():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        gh_in, gh_out = MemoryPipe(), MemoryPipe()
        # answer_calls=False: handshake works, but ping is ignored and times out
        github = Backend("github", LineTransport(gh_out.reader, gh_in),
                         breaker=CircuitBreaker(failure_threshold=1, reset_timeout=100), request_timeout=0.05)
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [github], health_interval=0.01)
        router_task = asyncio.create_task(router.serve())
        gh_task = asyncio.create_task(_responder(gh_in, gh_out, ["search"], answer_calls=False))
        client = LineTransport(r2c.reader, c2r)

        await _handshake(client)
        changed = jsonrpc.decode(await asyncio.wait_for(client.receive(), timeout=3))
        opened = not github.available()
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(router_task, gh_task), timeout=5)
        return changed, opened

    changed, opened = asyncio.run(scenario())
    assert changed["method"] == "notifications/tools/list_changed"
    assert opened is True


def test_resilient_list_marks_a_failing_available_backend_partial():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        gh_in, gh_out = MemoryPipe(), MemoryPipe()
        # breaker closed (available), but tools/list is ignored and times out
        github = Backend("github", LineTransport(gh_out.reader, gh_in),
                         breaker=CircuitBreaker(reset_timeout=100), request_timeout=0.05)
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [github])
        router_task = asyncio.create_task(router.serve())
        gh_task = asyncio.create_task(_responder(gh_in, gh_out, ["search"], answer_list=False))
        client = LineTransport(r2c.reader, c2r)

        await _handshake(client)
        await client.send(jsonrpc.encode(jsonrpc.request("c2", "tools/list", {})))
        listing = jsonrpc.decode(await client.receive())
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(router_task, gh_task), timeout=5)
        return listing

    listing = asyncio.run(scenario())
    assert listing["result"]["tools"] == []
    assert listing["result"]["_meta"][PROXY_PARTIAL_KEY]["unavailable_backends"] == ["github"]


def test_non_resilient_handshake_failure_propagates():
    import pytest
    from yamp.forward import HandshakeError

    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        gh_in, gh_out = MemoryPipe(), MemoryPipe()
        github = Backend("github", LineTransport(gh_out.reader, gh_in))  # no breaker
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [github])
        router_task = asyncio.create_task(router.serve())
        gh_task = asyncio.create_task(_dead_on_handshake(gh_in, gh_out))
        client = LineTransport(r2c.reader, c2r)
        await client.send(jsonrpc.encode(jsonrpc.request(
            "c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        with pytest.raises(HandshakeError):
            await asyncio.wait_for(router_task, timeout=5)
        gh_task.cancel()

    asyncio.run(scenario())


def test_non_resilient_call_failure_propagates():
    import pytest
    from yamp.forward import HandshakeError

    async def dead_after_call(read_pipe, write_pipe):
        t = LineTransport(read_pipe.reader, write_pipe)
        init = jsonrpc.decode(await t.receive())
        await t.send(jsonrpc.encode(jsonrpc.result(init["id"], {"capabilities": {}, "serverInfo": {"name": "b"}})))
        await t.receive()  # initialized
        await t.receive()  # tools/call
        await t.send_eof()  # die before answering

    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        gh_in, gh_out = MemoryPipe(), MemoryPipe()
        github = Backend("github", LineTransport(gh_out.reader, gh_in))  # no breaker
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [github])
        router_task = asyncio.create_task(router.serve())
        gh_task = asyncio.create_task(dead_after_call(gh_in, gh_out))
        client = LineTransport(r2c.reader, c2r)
        await _handshake(client)
        await client.send(jsonrpc.encode(jsonrpc.request("c2", "tools/call", {"name": "github__x"})))
        with pytest.raises(HandshakeError):
            await asyncio.wait_for(router_task, timeout=5)
        gh_task.cancel()

    asyncio.run(scenario())
