import asyncio

import pytest

from yamp import jsonrpc
from yamp.forward import PROXY_PROTOCOL_VERSION
from yamp.router import Backend, ForwardRouter, INVALID_PARAMS
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe

BACKENDS = [("gh", ["create_issue", "search"]), ("gl", ["create_issue"])]


async def _mock_backend(read_pipe, write_pipe, name, tools, calls):
    transport = LineTransport(read_pipe.reader, write_pipe)
    init = jsonrpc.decode(await transport.receive())
    await transport.send(
        jsonrpc.encode(
            jsonrpc.result(
                init["id"],
                {
                    "protocolVersion": PROXY_PROTOCOL_VERSION,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": f"{name}-server"},
                },
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
        if message["method"] == "tools/list":
            await transport.send(
                jsonrpc.encode(
                    jsonrpc.result(message["id"], {"tools": [{"name": t} for t in tools]})
                )
            )
        elif message["method"] == "tools/call":
            calls.append(message["params"]["name"])
            await transport.send(
                jsonrpc.encode(
                    jsonrpc.result(
                        message["id"],
                        {"content": [{"type": "text", "text": f"{name}:{message['params']['name']}"}]},
                    )
                )
            )


async def _setup():
    c2r, r2c = MemoryPipe(), MemoryPipe()
    calls: dict[str, list[str]] = {name: [] for name, _ in BACKENDS}
    backend_objs = []
    backend_tasks = []
    for name, tools in BACKENDS:
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        backend_objs.append(Backend(name, LineTransport(b2pr.reader, pr2b)))
        backend_tasks.append(
            asyncio.create_task(_mock_backend(pr2b, b2pr, name, tools, calls[name]))
        )
    router = ForwardRouter(LineTransport(c2r.reader, r2c), backend_objs)
    router_task = asyncio.create_task(router.serve())
    client = LineTransport(r2c.reader, c2r)

    await client.send(
        jsonrpc.encode(
            jsonrpc.request(
                "c1",
                "initialize",
                {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "c"}},
            )
        )
    )
    init = jsonrpc.decode(await client.receive())
    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
    return client, init, calls, router_task, backend_tasks


async def _teardown(client, router_task, backend_tasks):
    await client.send_eof()
    await asyncio.wait_for(asyncio.gather(router_task, *backend_tasks), timeout=5)


def test_router_gates():
    async def scenario():
        client, init, calls, router_task, backend_tasks = await _setup()

        async def call(id, method, params):
            await client.send(jsonrpc.encode(jsonrpc.request(id, method, params)))
            return jsonrpc.decode(await client.receive())

        listing = await call("l", "tools/list", {})
        routed = await call("s", "tools/call", {"name": "gh__search", "arguments": {}})
        no_delim = await call("u", "tools/call", {"name": "nope", "arguments": {}})
        bad_backend = await call("z", "tools/call", {"name": "zz__x", "arguments": {}})

        await _teardown(client, router_task, backend_tasks)
        return init, listing, routed, no_delim, bad_backend, calls

    init, listing, routed, no_delim, bad_backend, calls = asyncio.run(scenario())

    # gate 1 (prefix) + gate 3 (aggregate) + gate 5 (collision distinct)
    names = {tool["name"] for tool in listing["result"]["tools"]}
    assert names == {"gh__create_issue", "gh__search", "gl__create_issue"}

    # gate 2: routed to exactly one backend, prefix stripped
    assert calls["gh"] == ["search"]
    assert calls["gl"] == []
    assert routed["result"]["content"][0]["text"] == "gh:search"

    # gate 4: unresolvable names rejected, not silently forwarded
    assert no_delim["error"]["code"] == INVALID_PARAMS
    assert bad_backend["error"]["code"] == INVALID_PARAMS

    # composed capabilities advertise tools
    assert "tools" in init["result"]["capabilities"]


def test_server_discover_composes_backend_tools():
    # SEP §2.1: the router answers server/discover by composing the same
    # namespaced tool surface as tools/list, from all healthy backends.
    async def scenario():
        client, _init, _calls, router_task, backend_tasks = await _setup()
        await client.send(jsonrpc.encode(jsonrpc.request("d", "server/discover", {})))
        discover = jsonrpc.decode(await client.receive())
        await _teardown(client, router_task, backend_tasks)
        return discover

    discover = asyncio.run(scenario())
    names = {tool["name"] for tool in discover["result"]["tools"]}
    assert names == {"gh__create_issue", "gh__search", "gl__create_issue"}


def test_priority_strategy_drops_lower_priority_collision():
    # gh and gl both offer 'create_issue'; with priority [gh, gl] only gh's copy
    # survives (SEP §3.4). Names stay prefixed, so reverse resolution still works.
    from yamp.config import Namespacing

    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        calls: dict[str, list[str]] = {name: [] for name, _ in BACKENDS}
        backend_objs = []
        backend_tasks = []
        for name, tools in BACKENDS:
            pr2b, b2pr = MemoryPipe(), MemoryPipe()
            backend_objs.append(Backend(name, LineTransport(b2pr.reader, pr2b)))
            backend_tasks.append(
                asyncio.create_task(_mock_backend(pr2b, b2pr, name, tools, calls[name]))
            )
        router = ForwardRouter(
            LineTransport(c2r.reader, r2c),
            backend_objs,
            namespacing=Namespacing(strategy="priority", priority=["gh", "gl"]),
        )
        router_task = asyncio.create_task(router.serve())
        client = LineTransport(r2c.reader, c2r)
        await client.send(
            jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}}))
        )
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
        await client.send(jsonrpc.encode(jsonrpc.request("l", "tools/list", {})))
        listing = jsonrpc.decode(await client.receive())
        await client.send(jsonrpc.encode(jsonrpc.request("s", "tools/call", {"name": "gh__create_issue", "arguments": {}})))
        routed = jsonrpc.decode(await client.receive())
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(router_task, *backend_tasks), timeout=5)
        return listing, routed

    listing, routed = asyncio.run(scenario())
    names = {t["name"] for t in listing["result"]["tools"]}
    assert names == {"gh__create_issue", "gh__search"}  # gl__create_issue dropped
    assert routed["result"]["content"][0]["text"] == "gh:create_issue"


def test_filter_keyword_preselect_and_name_patterns():
    # gh declares keyword "git", gl declares "chat". A filtered tools/list with
    # keyword "git" must skip gl entirely (fan-out drops) and name patterns
    # further trim the composed surface (SEP-2564/2614).
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        queried: dict[str, int] = {name: 0 for name, _ in BACKENDS}
        backend_objs = []
        backend_tasks = []
        keyword_map = {"gh": ["git"], "gl": ["chat"]}
        for name, tools in BACKENDS:
            pr2b, b2pr = MemoryPipe(), MemoryPipe()

            async def counting_backend(rp, wp, n, ts):
                t = LineTransport(rp.reader, wp)
                init = jsonrpc.decode(await t.receive())
                await t.send(jsonrpc.encode(jsonrpc.result(init["id"], {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": {"tools": {}}, "serverInfo": {"name": n}})))
                await t.receive()
                while True:
                    raw = await t.receive()
                    if raw is None:
                        await t.send_eof()
                        return
                    m = jsonrpc.decode(raw)
                    if m["method"] == "tools/list":
                        queried[n] += 1
                        await t.send(jsonrpc.encode(jsonrpc.result(m["id"], {"tools": [{"name": x} for x in ts]})))

            backend_objs.append(Backend(name, LineTransport(b2pr.reader, pr2b), keywords=keyword_map[name]))
            backend_tasks.append(asyncio.create_task(counting_backend(pr2b, b2pr, name, tools)))
        router = ForwardRouter(LineTransport(c2r.reader, r2c), backend_objs)
        router_task = asyncio.create_task(router.serve())
        client = LineTransport(r2c.reader, c2r)
        await client.send(jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
        await client.send(jsonrpc.encode(jsonrpc.request("l", "tools/list", {"filter": {"keywords": ["git"], "namePatterns": ["gh__create*"]}})))
        listing = jsonrpc.decode(await client.receive())
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(router_task, *backend_tasks), timeout=5)
        return listing, queried

    listing, queried = asyncio.run(scenario())
    assert queried == {"gh": 1, "gl": 0}  # gl skipped by keyword pre-select
    names = {t["name"] for t in listing["result"]["tools"]}
    assert names == {"gh__create_issue"}  # name pattern trimmed gh__search too


def test_invalid_backend_id_rejected():
    async def scenario():
        pipe = MemoryPipe()
        with pytest.raises(ValueError):
            Backend("bad id!", LineTransport(pipe.reader, MemoryPipe()))

    asyncio.run(scenario())


def test_router_fanout_latency_within_budget():
    import statistics

    from yamp.instrument import within_budget

    async def scenario():
        client, _init, _calls, router_task, backend_tasks = await _setup()
        request = jsonrpc.encode(jsonrpc.request("t", "tools/list", {}))
        for _ in range(50):
            await client.send(request)
            await client.receive()
        latencies = []
        import time

        for _ in range(300):
            start = time.perf_counter()
            await client.send(request)
            await client.receive()
            latencies.append((time.perf_counter() - start) * 1000.0)
        await _teardown(client, router_task, backend_tasks)
        return latencies

    latencies = asyncio.run(scenario())
    median = statistics.median(latencies)
    under = sum(1 for x in latencies if within_budget(x)) / len(latencies)
    print(f"\n[latency δ2 fanout] median={median:.4f}ms within={under:.3%}")
    assert within_budget(median)
    assert under >= 0.99


async def _run_namespacing(namespacing, actions, backends=BACKENDS):
    # Spin up the router under a collision strategy, replay a list of
    # (id, method, params) client actions, and return the decoded responses.
    c2r, r2c = MemoryPipe(), MemoryPipe()
    calls: dict[str, list[str]] = {name: [] for name, _ in backends}
    backend_objs = []
    backend_tasks = []
    for name, tools in backends:
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        backend_objs.append(Backend(name, LineTransport(b2pr.reader, pr2b)))
        backend_tasks.append(asyncio.create_task(_mock_backend(pr2b, b2pr, name, tools, calls[name])))
    router = ForwardRouter(LineTransport(c2r.reader, r2c), backend_objs, namespacing=namespacing)
    router_task = asyncio.create_task(router.serve())
    client = LineTransport(r2c.reader, c2r)
    await client.send(
        jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}}))
    )
    await client.receive()
    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
    responses = []
    for id, method, params in actions:
        await client.send(jsonrpc.encode(jsonrpc.request(id, method, params)))
        responses.append(jsonrpc.decode(await client.receive()))
    await client.send_eof()
    await asyncio.wait_for(asyncio.gather(router_task, *backend_tasks), timeout=5)
    return responses, calls


def test_manual_strategy_renames_and_routes():
    # gh__create_issue is renamed to new_issue; a tools/call by the exposed name
    # reverse-resolves to gh, and an unknown exposed name is rejected (SEP §3.4).
    from yamp.config import Namespacing

    ns = Namespacing(strategy="manual", overrides={"gh__create_issue": "new_issue"})
    responses, calls = asyncio.run(
        _run_namespacing(
            ns,
            [
                ("l", "tools/list", {}),
                ("s", "tools/call", {"name": "new_issue", "arguments": {}}),
                ("g", "tools/call", {"name": "gl__create_issue", "arguments": {}}),
                ("u", "tools/call", {"name": "missing", "arguments": {}}),
            ],
        )
    )
    listing, renamed, plain, unknown = responses
    names = {t["name"] for t in listing["result"]["tools"]}
    assert names == {"new_issue", "gh__search", "gl__create_issue"}
    assert renamed["result"]["content"][0]["text"] == "gh:create_issue"
    assert calls["gh"] == ["create_issue"]
    assert plain["result"]["content"][0]["text"] == "gl:create_issue"
    assert unknown["error"]["code"] == INVALID_PARAMS


def test_manual_strategy_rejects_unresolved_collision():
    # Two names mapped to one exposed name is an unresolved collision: tools/list
    # is rejected rather than served as a silent duplicate (SEP §3.4).
    from yamp.config import Namespacing

    ns = Namespacing(strategy="manual", overrides={"gh__search": "dup", "gl__create_issue": "dup"})
    responses, _calls = asyncio.run(_run_namespacing(ns, [("l", "tools/list", {})]))
    assert "error" in responses[0]
    assert "manual collision" in responses[0]["error"]["message"]


def test_passthrough_strategy_keeps_originals_and_routes():
    # passthrough keeps original names (duplicates and all); a tools/call resolves
    # through the reverse map to the first backend offering the name (SEP §3.4).
    from yamp.config import Namespacing

    ns = Namespacing(strategy="passthrough")
    responses, calls = asyncio.run(
        _run_namespacing(
            ns,
            [
                ("l", "tools/list", {}),
                ("a", "tools/call", {"name": "search", "arguments": {}}),
                ("b", "tools/call", {"name": "create_issue", "arguments": {}}),
                ("u", "tools/call", {"name": "missing", "arguments": {}}),
            ],
        )
    )
    listing, search, create, unknown = responses
    names = sorted(t["name"] for t in listing["result"]["tools"])
    assert names == ["create_issue", "create_issue", "search"]  # duplicate kept
    assert search["result"]["content"][0]["text"] == "gh:search"
    assert create["result"]["content"][0]["text"] == "gh:create_issue"  # first backend wins
    assert unknown["error"]["code"] == INVALID_PARAMS


def test_passthrough_strategy_warms_reverse_map_on_cold_call():
    # A tools/call under passthrough before any tools/list still resolves: the
    # reverse map is warmed by an on-demand list fan-out.
    from yamp.config import Namespacing

    ns = Namespacing(strategy="passthrough")
    responses, calls = asyncio.run(
        _run_namespacing(ns, [("a", "tools/call", {"name": "search", "arguments": {}})])
    )
    assert responses[0]["result"]["content"][0]["text"] == "gh:search"
    assert calls["gh"] == ["search"]
