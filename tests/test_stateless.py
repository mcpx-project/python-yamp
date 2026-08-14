import asyncio
import json

import pytest

from yamp.forward import PROXY_SERVER_INFO
from yamp.stateless import (
    CLIENT_INFO_META_KEY,
    INVALID_PARAMS,
    METHOD_NOT_FOUND,
    StatelessBackend,
    StatelessForwarder,
    StatelessRequest,
    StatelessResponse,
    decode_request,
    decode_response,
    encode_request,
    encode_response,
)
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe
from yamp.version import (
    PROTOCOL_VERSION_META_KEY,
    STATELESS_PROTOCOL_VERSION,
    SUPPORTED_PROTOCOL_VERSIONS,
    UNSUPPORTED_PROTOCOL_VERSION,
)

BACKENDS = [("gh", ["a", "b"]), ("gl", ["c"])]


async def _mock_backend(read_pipe, write_pipe, name, tools, log):
    transport = LineTransport(read_pipe.reader, write_pipe)
    while True:
        raw = await transport.receive()
        if raw is None:
            await transport.send_eof()
            return
        request = decode_request(raw)
        log.append(request)
        if request.method == "server/discover":
            body = json.dumps({"tools": [{"name": t} for t in tools]})
            await transport.send(
                encode_response(StatelessResponse(meta={"backend": name}, body=body))
            )
        elif request.method == "tools/call":
            await transport.send(
                encode_response(
                    StatelessResponse(
                        meta={"backend": name, "echoed_client": request.meta.get(CLIENT_INFO_META_KEY)},
                        body=f"RESULT:{request.name}:{request.body}",
                    )
                )
            )


async def _setup():
    c2r, r2c = MemoryPipe(), MemoryPipe()
    logs: dict[str, list] = {name: [] for name, _ in BACKENDS}
    backends = []
    tasks = []
    for name, tools in BACKENDS:
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        backends.append(StatelessBackend(name, LineTransport(b2pr.reader, pr2b)))
        tasks.append(asyncio.create_task(_mock_backend(pr2b, b2pr, name, tools, logs[name])))
    forwarder = StatelessForwarder(LineTransport(c2r.reader, r2c), backends)
    forwarder_task = asyncio.create_task(forwarder.serve())
    client = LineTransport(r2c.reader, c2r)
    return client, logs, forwarder_task, tasks


async def _exchange(client, request: StatelessRequest) -> StatelessResponse:
    await client.send(encode_request(request))
    return decode_response(await client.receive())


def test_wire_round_trips():
    req = StatelessRequest("tools/call", "gh__x", {"k": 1}, "body")
    assert decode_request(encode_request(req)) == req
    resp = StatelessResponse({"m": 2}, "b")
    assert decode_response(encode_response(resp)) == resp


def test_gate1_discover_composition():
    async def scenario():
        client, logs, forwarder_task, tasks = await _setup()
        resp = await _exchange(client, StatelessRequest("server/discover", None))
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(forwarder_task, *tasks), timeout=5)
        return resp, logs

    resp, logs = asyncio.run(scenario())
    names = {tool["name"] for tool in json.loads(resp.body)["tools"]}
    assert names == {"gh__a", "gh__b", "gl__c"}
    # No handshake was performed against the backends (gate 4).
    assert all(entry.method != "initialize" for log in logs.values() for entry in log)


def test_gate2_meta_injection_and_forwarding():
    async def scenario():
        client, logs, forwarder_task, tasks = await _setup()
        resp = await _exchange(
            client,
            StatelessRequest("tools/call", "gh__a", {"trace": "t1"}, "PAYLOAD"),
        )
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(forwarder_task, *tasks), timeout=5)
        return resp, logs

    resp, logs = asyncio.run(scenario())
    forwarded = logs["gh"][0]
    # Proxy injected its identity, and preserved the client's own meta.
    assert forwarded.meta[CLIENT_INFO_META_KEY] == PROXY_SERVER_INFO
    assert forwarded.meta["trace"] == "t1"
    # Backend _meta is forwarded back to the client.
    assert resp.meta["backend"] == "gh"
    assert resp.meta["echoed_client"] == PROXY_SERVER_INFO


def test_gate3_routes_by_header_without_body_inspection():
    async def scenario():
        client, logs, forwarder_task, tasks = await _setup()
        # Body is deliberately not valid JSON; routing must still work and the
        # body must arrive unchanged, proving no body parse on the route path.
        opaque = "NOT-JSON <<{[}>>"
        resp = await _exchange(client, StatelessRequest("tools/call", "gl__c", {}, opaque))
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(forwarder_task, *tasks), timeout=5)
        return resp, logs, opaque

    resp, logs, opaque = asyncio.run(scenario())
    assert logs["gl"][0].body == opaque  # forwarded byte-for-byte
    assert logs["gh"] == []  # routed to exactly one backend
    assert resp.body == f"RESULT:c:{opaque}"


def test_unknown_and_unroutable():
    async def scenario():
        client, logs, forwarder_task, tasks = await _setup()
        bad = await _exchange(client, StatelessRequest("tools/call", "nope", {}, ""))
        other = await _exchange(client, StatelessRequest("resources/read", None, {}, ""))
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(forwarder_task, *tasks), timeout=5)
        return bad, other

    bad, other = asyncio.run(scenario())
    assert json.loads(bad.body)["error"]["code"] == INVALID_PARAMS
    assert json.loads(other.body)["error"]["code"] == METHOD_NOT_FOUND


def test_invalid_backend_id_rejected():
    async def scenario():
        pipe = MemoryPipe()
        with pytest.raises(ValueError):
            StatelessBackend("bad id!", LineTransport(pipe.reader, MemoryPipe()))

    asyncio.run(scenario())


def test_backend_closing_during_exchange_raises():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        backend = StatelessBackend("b", LineTransport(b2pr.reader, pr2b))
        forwarder = StatelessForwarder(LineTransport(c2r.reader, r2c), [backend])

        async def dying_backend():
            transport = LineTransport(pr2b.reader, b2pr)
            await transport.receive()  # take the request
            await transport.send_eof()  # then die without responding

        backend_task = asyncio.create_task(dying_backend())
        forwarder_task = asyncio.create_task(forwarder.serve())
        client = LineTransport(r2c.reader, c2r)
        await client.send(encode_request(StatelessRequest("server/discover", None)))
        with pytest.raises(ConnectionError):
            await forwarder_task
        await backend_task

    asyncio.run(scenario())


def test_version_negotiation_matrix():
    async def scenario():
        client, logs, forwarder_task, tasks = await _setup()
        # Omitted: accepted, defaulted to the stateless version and pinned in the
        # forwarded _meta so the backend sees a self-describing request.
        omitted = await _exchange(client, StatelessRequest("tools/call", "gh__a", {}, "x"))
        # Supported: echoed through, not rewritten.
        supported = await _exchange(
            client,
            StatelessRequest(
                "tools/call",
                "gh__b",
                {PROTOCOL_VERSION_META_KEY: STATELESS_PROTOCOL_VERSION},
                "y",
            ),
        )
        # Unsupported: rejected before routing, with the supported set in data.
        unsupported = await _exchange(
            client,
            StatelessRequest("tools/call", "gh__a", {PROTOCOL_VERSION_META_KEY: "2024-11-05"}, "z"),
        )
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(forwarder_task, *tasks), timeout=5)
        return omitted, supported, unsupported, logs

    omitted, supported, unsupported, logs = asyncio.run(scenario())
    # Both accepted calls reached the backend with the version pinned in _meta.
    assert logs["gh"][0].meta[PROTOCOL_VERSION_META_KEY] == STATELESS_PROTOCOL_VERSION
    assert logs["gh"][1].meta[PROTOCOL_VERSION_META_KEY] == STATELESS_PROTOCOL_VERSION
    assert omitted.body.startswith("RESULT:a")
    assert supported.body.startswith("RESULT:b")
    # The unsupported request never reached a backend and carries -32004.
    assert len(logs["gh"]) == 2
    error = json.loads(unsupported.body)["error"]
    assert error["code"] == UNSUPPORTED_PROTOCOL_VERSION
    assert error["data"] == {
        "requested": "2024-11-05",
        "supported": list(SUPPORTED_PROTOCOL_VERSIONS),
    }


def test_stateless_latency_within_budget():
    import statistics
    import time

    from yamp.instrument import within_budget

    async def scenario():
        client, _logs, forwarder_task, tasks = await _setup()
        request = encode_request(StatelessRequest("tools/call", "gh__a", {}, "x"))
        for _ in range(50):
            await client.send(request)
            await client.receive()
        latencies = []
        for _ in range(300):
            start = time.perf_counter()
            await client.send(request)
            await client.receive()
            latencies.append((time.perf_counter() - start) * 1000.0)
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(forwarder_task, *tasks), timeout=5)
        return latencies

    latencies = asyncio.run(scenario())
    median = statistics.median(latencies)
    under = sum(1 for x in latencies if within_budget(x)) / len(latencies)
    print(f"\n[latency δ3 stateless] median={median:.4f}ms within={under:.3%}")
    assert within_budget(median)
    assert under >= 0.99
