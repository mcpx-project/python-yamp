import asyncio
import json

import pytest

from yamp.errors import POLICY_DENIED
from yamp.transparent import (
    AllowAll,
    HeaderPolicy,
    TransparentL1,
    encode_envelope,
    peek_headers,
    recover_original_destination,
    select_backend,
)
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


# ---- unit: header and destination helpers ----

def test_peek_headers_ignores_body():
    raw = encode_envelope({"Mcp-Method": "tools/call"}, "NOT-JSON <<{[")
    assert peek_headers(raw) == {"Mcp-Method": "tools/call"}


def test_policies():
    assert AllowAll().allow({"Mcp-Method": "anything"})
    policy = HeaderPolicy(blocked_methods={"tools/call"}, blocked_names={"danger"})
    assert policy.allow({"Mcp-Method": "tools/list"})
    assert not policy.allow({"Mcp-Method": "tools/call"})
    assert not policy.allow({"Mcp-Name": "danger"})
    assert policy.allow({})  # stateful message with no headers


def test_select_backend_and_recovery_stub():
    backend_a, backend_b = object(), object()
    table = {("10.0.0.1", 443): backend_a, ("10.0.0.2", 443): backend_b}
    assert select_backend(("10.0.0.2", 443), table) is backend_b
    with pytest.raises(KeyError):
        select_backend(("10.0.0.9", 443), table)
    with pytest.raises(NotImplementedError):
        recover_original_destination(object())


# ---- integration: TransparentL1 ----

async def _mock_backend(read_pipe, write_pipe, received):
    transport = LineTransport(read_pipe.reader, write_pipe)
    while True:
        raw = await transport.receive()
        if raw is None:
            await transport.send_eof()
            return
        received.append(raw)
        envelope = json.loads(raw)
        await transport.send(encode_envelope({"Mcp-From": "backend"}, "RESP:" + envelope["body"]))


async def _run(policy, envelopes):
    c2r, r2c = MemoryPipe(), MemoryPipe()
    pr2b, b2pr = MemoryPipe(), MemoryPipe()
    received = []
    proxy = TransparentL1(
        LineTransport(c2r.reader, r2c),
        LineTransport(b2pr.reader, pr2b),
        policy,
    )
    backend_task = asyncio.create_task(_mock_backend(pr2b, b2pr, received))
    proxy_task = asyncio.create_task(proxy.serve())
    client = LineTransport(r2c.reader, c2r)
    responses = []
    for envelope in envelopes:
        await client.send(envelope)
        responses.append(await client.receive())
    await client.send_eof()
    await asyncio.wait_for(asyncio.gather(proxy_task, backend_task), timeout=5)
    return received, responses, proxy


def test_gate2_3_stateful_passthrough_unmodified():
    # A stateful connection carries no Mcp-Method header. initialize and its
    # response must cross unmodified, byte for byte, with no injected messages.
    init = encode_envelope({}, '{"jsonrpc":"2.0","id":1,"method":"initialize"}')
    initialized = encode_envelope({}, '{"jsonrpc":"2.0","method":"notifications/initialized"}')

    received, responses, _ = asyncio.run(_run(AllowAll(), [init, initialized]))

    assert received == [init, initialized]  # forwarded unmodified, nothing injected
    assert responses[0] == encode_envelope(
        {"Mcp-From": "backend"}, 'RESP:{"jsonrpc":"2.0","id":1,"method":"initialize"}'
    )


def test_standard_headers_forwarded_byte_identical():
    # SEP-2792: an intermediary must not rewrite mirrored standard headers in
    # place. TransparentL1 forwards raw bytes, so traceparent and Accept-Language
    # arrive byte-for-byte, never normalized.
    tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    sent = encode_envelope({"traceparent": tp, "Accept-Language": "fr-CH, fr;q=0.9"}, "payload")
    received, _responses, _ = asyncio.run(_run(AllowAll(), [sent]))
    assert received[0] == sent  # byte-for-byte, no normalization
    assert peek_headers(received[0])["traceparent"] == tp


def test_gate4_header_filter_blocks_without_body_parse():
    policy = HeaderPolicy(blocked_methods={"tools/call"})
    allowed = encode_envelope({"Mcp-Method": "tools/list"}, "OPAQUE<<{[")
    blocked = encode_envelope({"Mcp-Method": "tools/call", "Mcp-Name": "x"}, "ALSO-OPAQUE }}")

    received, responses, proxy = asyncio.run(_run(policy, [allowed, blocked]))

    # Allowed forwarded unmodified; blocked never reached the backend.
    assert received == [allowed]
    assert proxy.blocked == 1
    blocked_body = json.loads(json.loads(responses[1])["body"])
    assert blocked_body["error"]["code"] == POLICY_DENIED


def test_gate5_both_modes_detected_per_connection():
    # With a policy that blocks tools/call: a stateful message (no header)
    # passes through; a stateless tools/call is blocked.
    policy = HeaderPolicy(blocked_methods={"tools/call"})
    stateful = encode_envelope({}, '{"jsonrpc":"2.0","id":1,"method":"tools/call"}')
    stateless = encode_envelope({"Mcp-Method": "tools/call"}, "opaque")

    received, responses, proxy = asyncio.run(_run(policy, [stateful, stateless]))

    assert received == [stateful]  # stateful body is opaque to L1, passes through
    assert proxy.blocked == 1  # the header-tagged stateless call is blocked


def test_transparent_latency_within_budget():
    import statistics
    import time

    from yamp.instrument import within_budget

    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        received = []
        proxy = TransparentL1(
            LineTransport(c2r.reader, r2c), LineTransport(b2pr.reader, pr2b), AllowAll()
        )
        backend_task = asyncio.create_task(_mock_backend(pr2b, b2pr, received))
        proxy_task = asyncio.create_task(proxy.serve())
        client = LineTransport(r2c.reader, c2r)
        envelope = encode_envelope({"Mcp-Method": "tools/list"}, "x")
        for _ in range(50):
            await client.send(envelope)
            await client.receive()
        latencies = []
        for _ in range(300):
            start = time.perf_counter()
            await client.send(envelope)
            await client.receive()
            latencies.append((time.perf_counter() - start) * 1000.0)
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(proxy_task, backend_task), timeout=5)
        return latencies

    latencies = asyncio.run(scenario())
    median = statistics.median(latencies)
    under = sum(1 for x in latencies if within_budget(x)) / len(latencies)
    print(f"\n[latency δ4 transparent] median={median:.4f}ms within={under:.3%}")
    assert within_budget(median)
    assert under >= 0.99
