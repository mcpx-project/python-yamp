import asyncio
import json

from yamp import jsonrpc
from yamp.forward import PROXY_PROTOCOL_VERSION
from yamp.stateless import (
    StatelessBackend,
    StatelessRequest,
    StatelessResponse,
    decode_request,
    encode_request,
    encode_response,
)
from yamp.transparent import encode_envelope
from yamp.transparent_l2 import (
    PROXY_HOPS_KEY,
    TransparentL2Stateful,
    TransparentL2Stateless,
    proxy_hop,
)
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


# ---- stateless L2: hop tracing + discover filtering ----

async def _mock_stateless_backend(read_pipe, write_pipe, tools, log):
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
            await transport.send(encode_response(StatelessResponse(meta={}, body=body)))
        elif request.method == "tools/call":
            await transport.send(
                encode_response(StatelessResponse(meta={}, body=f"RESULT:{request.name}"))
            )


async def _stateless(client_ops, backends_spec, tool_filter=None):
    c2r, r2c = MemoryPipe(), MemoryPipe()
    logs: dict[str, list] = {}
    backends = []
    tasks = []
    for name, tools in backends_spec:
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        logs[name] = []
        backends.append(StatelessBackend(name, LineTransport(b2pr.reader, pr2b)))
        tasks.append(asyncio.create_task(_mock_stateless_backend(pr2b, b2pr, tools, logs[name])))
    proxy = TransparentL2Stateless(LineTransport(c2r.reader, r2c), backends, tool_filter)
    proxy_task = asyncio.create_task(proxy.serve())
    client = LineTransport(r2c.reader, c2r)
    result = await client_ops(client)
    await client.send_eof()
    await asyncio.wait_for(asyncio.gather(proxy_task, *tasks), timeout=5)
    return result, logs


def test_gate1_2_hop_appended_not_replaced():
    async def ops(client):
        # first call carries an existing upstream hop; second carries none
        await client.send(
            encode_request(
                StatelessRequest("tools/call", "b__x", {PROXY_HOPS_KEY: [{"name": "upstream"}]}, "p")
            )
        )
        await client.receive()
        await client.send(encode_request(StatelessRequest("tools/call", "b__x", {}, "p")))
        await client.receive()
        return None

    _, logs = asyncio.run(_stateless(ops, [("b", ["x"])]))
    first_hops = logs["b"][0].meta[PROXY_HOPS_KEY]
    second_hops = logs["b"][1].meta[PROXY_HOPS_KEY]
    assert first_hops == [{"name": "upstream"}, proxy_hop()]  # appended, not replaced
    assert second_hops == [proxy_hop()]  # augmented when absent


def test_gate3_discover_filter_and_namespace():
    from yamp.stateless import decode_response

    async def ops(client):
        await client.send(encode_request(StatelessRequest("server/discover", None, {})))
        return decode_response(await client.receive())

    (resp, logs) = asyncio.run(
        _stateless(ops, [("gh", ["a", "secret"]), ("gl", ["c"])], tool_filter=lambda n: n != "secret")
    )
    names = {tool["name"] for tool in json.loads(resp.body)["tools"]}
    assert names == {"gh__a", "gl__c"}  # 'secret' filtered, others namespaced
    # backends saw the hop on the forwarded discover
    assert logs["gh"][0].meta[PROXY_HOPS_KEY] == [proxy_hop()]


def test_header_body_validation():
    # SEP-2243: a Level 2 intermediary parses the body, so a body that disagrees
    # with the transport headers is rejected before routing.
    from yamp.errors import HEADER_MISMATCH
    from yamp.stateless import decode_response

    async def ops(client):
        agree = json.dumps({"method": "tools/call", "params": {"name": "b__x"}})
        await client.send(encode_request(StatelessRequest("tools/call", "b__x", {}, agree)))
        routed = decode_response(await client.receive())
        bad_method = json.dumps({"method": "resources/read", "params": {"name": "b__x"}})
        await client.send(encode_request(StatelessRequest("tools/call", "b__x", {}, bad_method)))
        m1 = decode_response(await client.receive())
        bad_name = json.dumps({"method": "tools/call", "params": {"name": "b__evil"}})
        await client.send(encode_request(StatelessRequest("tools/call", "b__x", {}, bad_name)))
        m2 = decode_response(await client.receive())
        return routed, m1, m2

    (routed, m1, m2), logs = asyncio.run(_stateless(ops, [("b", ["x"])]))
    assert routed.body == "RESULT:x"  # agreeing call was routed, prefix stripped
    assert json.loads(m1.body)["error"]["code"] == HEADER_MISMATCH  # method disagrees
    assert json.loads(m2.body)["error"]["code"] == HEADER_MISMATCH  # tool name disagrees
    assert len(logs["b"]) == 1  # neither mismatched call reached the backend


def test_header_body_mismatch_ignores_non_object_and_opaque_bodies():
    from yamp.transparent_l2 import header_body_mismatch

    # Valid JSON that is not a JSON-RPC object carries nothing to validate.
    assert header_body_mismatch(StatelessRequest("tools/call", "b__x", {}, "[1,2,3]")) is None
    assert header_body_mismatch(StatelessRequest("tools/call", "b__x", {}, "42")) is None
    # Empty and non-JSON (opaque) bodies pass too.
    assert header_body_mismatch(StatelessRequest("tools/call", "b__x", {}, "")) is None
    assert header_body_mismatch(StatelessRequest("tools/call", "b__x", {}, "NOT-JSON")) is None
    # A body with no method field and no tool name is not a mismatch.
    assert header_body_mismatch(StatelessRequest("tools/call", "b__x", {}, '{"jsonrpc":"2.0"}')) is None
    # A hostile non-object `params` (null / list / scalar) must not crash the
    # check; it carries no name to validate, matching the Rust arm.
    assert header_body_mismatch(StatelessRequest("tools/call", "b__x", {}, '{"params":null}')) is None
    assert header_body_mismatch(StatelessRequest("tools/call", "b__x", {}, '{"params":[1,2]}')) is None
    assert header_body_mismatch(StatelessRequest("tools/call", "b__x", {}, '{"params":"x"}')) is None


def test_stateless_error_paths():
    from yamp.stateless import INVALID_PARAMS, METHOD_NOT_FOUND, decode_response

    async def ops(client):
        await client.send(encode_request(StatelessRequest("tools/call", "nope", {}, "")))
        bad = decode_response(await client.receive())
        await client.send(encode_request(StatelessRequest("resources/read", None, {}, "")))
        other = decode_response(await client.receive())
        return bad, other

    (bad, other), _ = asyncio.run(_stateless(ops, [("b", ["x"])]))
    assert json.loads(bad.body)["error"]["code"] == INVALID_PARAMS
    assert json.loads(other.body)["error"]["code"] == METHOD_NOT_FOUND


# ---- stateful L2: dual-handshake toggle ----

async def _jsonrpc_backend(read_pipe, write_pipe):
    transport = LineTransport(read_pipe.reader, write_pipe)
    init = jsonrpc.decode(await transport.receive())
    await transport.send(
        jsonrpc.encode(
            jsonrpc.result(
                init["id"],
                {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": {}, "serverInfo": {"name": "backend"}},
            )
        )
    )
    await transport.receive()  # notifications/initialized
    while True:
        raw = await transport.receive()
        if raw is None:
            await transport.send_eof()
            return


def test_gate4_5_dual_handshake_follows_forward_and_records():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        proxy = TransparentL2Stateful(
            LineTransport(c2r.reader, r2c), LineTransport(b2pr.reader, pr2b), dual_handshake=True
        )
        backend_task = asyncio.create_task(_jsonrpc_backend(pr2b, b2pr))
        proxy_task = asyncio.create_task(proxy.serve())
        client = LineTransport(r2c.reader, c2r)
        await client.send(
            jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {}}))
        )
        init = jsonrpc.decode(await client.receive())
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(proxy_task, backend_task), timeout=5)
        return init, proxy

    init, proxy = asyncio.run(scenario())
    assert init["result"]["serverInfo"]["name"] == "yamp"  # §2.2 dual handshake
    assert proxy.dual_handshake_performed is True


async def _envelope_backend(read_pipe, write_pipe, received):
    transport = LineTransport(read_pipe.reader, write_pipe)
    while True:
        raw = await transport.receive()
        if raw is None:
            await transport.send_eof()
            return
        received.append(raw)
        await transport.send(encode_envelope({"Mcp-From": "backend"}, "ok"))


def test_gate6_passthrough_follows_level1():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        proxy = TransparentL2Stateful(
            LineTransport(c2r.reader, r2c), LineTransport(b2pr.reader, pr2b), dual_handshake=False
        )
        received = []
        backend_task = asyncio.create_task(_envelope_backend(pr2b, b2pr, received))
        proxy_task = asyncio.create_task(proxy.serve())
        client = LineTransport(r2c.reader, c2r)
        envelope = encode_envelope({}, '{"jsonrpc":"2.0","id":1,"method":"initialize"}')
        await client.send(envelope)
        await client.receive()
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(proxy_task, backend_task), timeout=5)
        return received, proxy, envelope

    received, proxy, envelope = asyncio.run(scenario())
    assert received == [envelope]  # unmodified passthrough (Level 1)
    assert proxy.dual_handshake_performed is False


def test_l2_stateless_latency_within_budget():
    import statistics
    import time

    from yamp.instrument import within_budget

    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        backend = StatelessBackend("b", LineTransport(b2pr.reader, pr2b))
        proxy = TransparentL2Stateless(LineTransport(c2r.reader, r2c), [backend])
        backend_task = asyncio.create_task(_mock_stateless_backend(pr2b, b2pr, ["x"], []))
        proxy_task = asyncio.create_task(proxy.serve())
        client = LineTransport(r2c.reader, c2r)
        request = encode_request(StatelessRequest("tools/call", "b__x", {}, "p"))
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
        await asyncio.wait_for(asyncio.gather(proxy_task, backend_task), timeout=5)
        return latencies

    latencies = asyncio.run(scenario())
    median = statistics.median(latencies)
    under = sum(1 for x in latencies if within_budget(x)) / len(latencies)
    print(f"\n[latency δ5 L2 stateless] median={median:.4f}ms within={under:.3%}")
    assert within_budget(median)
    assert under >= 0.99
