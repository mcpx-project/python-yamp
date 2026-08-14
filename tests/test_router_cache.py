"""δ12 list-cache integration tests (Python arm). Mirrors the Rust arm.

Exercises the cache through the real ForwardRouter fan-out: a fresh hit skips
the backend request, a backend list_changed invalidates, and an opening breaker
invalidates. The hit-collapse claim is measured by counting backend requests,
not asserted.
"""

import asyncio

from yamp import jsonrpc
from yamp.cache import ListCache
from yamp.forward import PROXY_PROTOCOL_VERSION
from yamp.resilience import CircuitBreaker
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


async def _mock_backend(read_pipe, write_pipe, name, tools, counts):
    transport = LineTransport(read_pipe.reader, write_pipe)
    init = jsonrpc.decode(await transport.receive())
    await transport.send(
        jsonrpc.encode(
            jsonrpc.result(
                init["id"],
                {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": {"tools": {}}, "serverInfo": {"name": name}},
            )
        )
    )
    await transport.receive()  # notifications/initialized
    while True:
        raw = await transport.receive()
        if raw is None:
            await transport.send_eof()
            return
        message = jsonrpc.decode(raw)
        method = message["method"]
        if method == "tools/list":
            counts[name] = counts.get(name, 0) + 1
            result = {"tools": [{"name": t} for t in tools], "ttlMs": 60000, "cacheScope": "public"}
            await transport.send(jsonrpc.encode(jsonrpc.result(message["id"], result)))
        elif method == "ping":
            await transport.send(jsonrpc.encode(jsonrpc.result(message["id"], {})))


async def _setup(cache, backends_spec, principal=None, breakers=False):
    c2r, r2c = MemoryPipe(), MemoryPipe()
    counts: dict[str, int] = {}
    backend_objs = []
    backend_tasks = []
    for name, tools in backends_spec:
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        breaker = CircuitBreaker(failure_threshold=1) if breakers else None
        backend_objs.append(Backend(name, LineTransport(b2pr.reader, pr2b), breaker=breaker))
        backend_tasks.append(asyncio.create_task(_mock_backend(pr2b, b2pr, name, tools, counts)))
    router = ForwardRouter(LineTransport(c2r.reader, r2c), backend_objs, cache=cache, principal=principal)
    router_task = asyncio.create_task(router.serve())
    client = LineTransport(r2c.reader, c2r)
    await client.send(
        jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}}))
    )
    await client.receive()
    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
    return client, counts, router, router_task, backend_tasks


async def _list(client):
    await client.send(jsonrpc.encode(jsonrpc.request("l", "tools/list", {})))
    return jsonrpc.decode(await client.receive())


async def _teardown(client, router_task, backend_tasks):
    await client.send_eof()
    await asyncio.wait_for(asyncio.gather(router_task, *backend_tasks), timeout=5)


def test_cache_hit_collapses_fetches():
    async def scenario():
        cache = ListCache()
        client, counts, _r, router_task, backend_tasks = await _setup(cache, [("gh", ["a"]), ("gl", ["b"])])
        listings = [await _list(client) for _ in range(5)]
        await _teardown(client, router_task, backend_tasks)
        return counts, listings

    counts, listings = asyncio.run(scenario())
    # Five client list calls, but each backend was queried exactly once (SEP §6).
    assert counts == {"gh": 1, "gl": 1}
    names = {tool["name"] for tool in listings[-1]["result"]["tools"]}
    assert names == {"gh__a", "gl__b"}


def test_shared_cache_collapses_across_connections():
    async def scenario():
        cache = ListCache()  # one cache, two independent client connections
        a_client, a_counts, _r, a_task, a_backends = await _setup(cache, [("gh", ["a"])])
        await _list(a_client)  # first connection primes the cache
        b_client, b_counts, _r2, b_task, b_backends = await _setup(cache, [("gh", ["a"])])
        await _list(b_client)  # second connection should hit the shared cache
        await _teardown(a_client, a_task, a_backends)
        await _teardown(b_client, b_task, b_backends)
        return a_counts, b_counts

    a_counts, b_counts = asyncio.run(scenario())
    assert a_counts == {"gh": 1}  # only the first connection queried the backend
    assert b_counts == {}  # the second served entirely from the shared cache


def test_list_changed_invalidates_cache():
    async def scenario():
        cache = ListCache()
        client, counts, router, router_task, backend_tasks = await _setup(cache, [("gh", ["a"])])
        await _list(client)  # miss: fetch and cache
        cached_before = cache.get("gh", "tools/list", None) is not None
        # A backend's own list_changed flows through the real server-message path
        # and invalidates its cached list (SEP §6.2).
        await router._server_message("gh", jsonrpc.notification("notifications/tools/list_changed"))
        invalidated = cache.get("gh", "tools/list", None) is None
        await _list(client)  # miss again after invalidation
        await _teardown(client, router_task, backend_tasks)
        return counts, cached_before, invalidated

    counts, cached_before, invalidated = asyncio.run(scenario())
    assert cached_before
    assert invalidated
    assert counts["gh"] == 2  # re-fetched after the notification


def test_breaker_open_invalidates_cache():
    async def scenario():
        cache = ListCache()
        client, counts, router, router_task, backend_tasks = await _setup(
            cache, [("gh", ["a"]), ("gl", ["b"])], breakers=True
        )
        await _list(client)  # prime both backends into the cache
        primed = cache.get("gh", "tools/list", None) is not None
        # Trip gh's breaker, then run the surface check the router runs on health
        # transitions; the departed backend's cache entry must be dropped.
        router._backends["gh"].breaker.record_failure()
        await router.emit_if_surface_changed()
        gh_after = cache.get("gh", "tools/list", None)
        gl_after = cache.get("gl", "tools/list", None)
        await _teardown(client, router_task, backend_tasks)
        return primed, gh_after, gl_after

    primed, gh_after, gl_after = asyncio.run(scenario())
    assert primed
    assert gh_after is None  # invalidated on breaker-open
    assert gl_after is not None  # the healthy backend keeps its cache


def test_private_scope_not_shared_across_principals():
    async def scenario():
        cache = ListCache()
        # A backend that marks its list private; two connections, two principals.
        a_client, a_counts, _r, a_task, a_backends = await _setup(
            _PrivateCache(cache), [("gh", ["a"])], principal="alice"
        )
        await _list(a_client)
        b_client, b_counts, _r2, b_task, b_backends = await _setup(
            _PrivateCache(cache), [("gh", ["a"])], principal="bob"
        )
        await _list(b_client)
        await _teardown(a_client, a_task, a_backends)
        await _teardown(b_client, b_task, b_backends)
        return a_counts, b_counts

    a_counts, b_counts = asyncio.run(scenario())
    assert a_counts == {"gh": 1}
    assert b_counts == {"gh": 1}  # bob cannot use alice's private cache entry


class _PrivateCache:
    """Wrap a ListCache so stored results are tagged private, to drive the
    cross-principal isolation path through the router without a private-aware
    mock backend."""

    def __init__(self, inner):
        self._inner = inner

    def get(self, backend_id, method, principal):
        return self._inner.get(backend_id, method, principal)

    def put(self, backend_id, method, principal, result):
        tagged = dict(result)
        tagged["cacheScope"] = "private"
        self._inner.put(backend_id, method, principal, tagged)
