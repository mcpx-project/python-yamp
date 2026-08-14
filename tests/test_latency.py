"""Proxy latency budget gate (δ0).

Requirement: a relay hop must add no more than ``LATENCY_BUDGET_MS`` per
message. This measures per-message round-trip latency through the line->framed
bridge and enforces the budget. The measured median is printed so the number
can be tracked and improved across increments.
"""

import asyncio
import statistics
import time

from yamp.instrument import LATENCY_BUDGET_MS, within_budget
from yamp.relay import Relay
from yamp.transport.framed import FramedTransport
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe

from tests.test_relay import _echo_framed_backend

WARMUP = 50
SAMPLES = 500


async def _measure():
    c2r, r2c = MemoryPipe(), MemoryPipe()
    r2b, b2r = MemoryPipe(), MemoryPipe()
    relay = Relay(
        client=LineTransport(c2r.reader, r2c),
        backend=FramedTransport(b2r.reader, r2b),
    )
    backend_task = asyncio.create_task(_echo_framed_backend(r2b, b2r))
    relay_task = asyncio.create_task(relay.run())
    client_in = LineTransport(r2c.reader, c2r)

    async def round_trip(payload: bytes) -> bytes | None:
        await c2r.write(payload + b"\n")
        return await client_in.receive()

    for _ in range(WARMUP):
        assert await round_trip(b'{"warm":true}') is not None

    latencies_ms = []
    for i in range(SAMPLES):
        start = time.perf_counter()
        message = await round_trip(b'{"id":%d}' % i)
        latencies_ms.append((time.perf_counter() - start) * 1000.0)
        assert message is not None

    c2r.write_eof()
    await asyncio.wait_for(asyncio.gather(relay_task, backend_task), timeout=5)
    return latencies_ms


def test_relay_added_latency_within_budget():
    latencies = asyncio.run(_measure())
    median = statistics.median(latencies)
    under = sum(1 for x in latencies if within_budget(x)) / len(latencies)
    print(f"\n[latency] median={median:.4f}ms budget={LATENCY_BUDGET_MS}ms "
          f"within={under:.3%}")
    assert within_budget(median)
    # Allow at most one scheduler outlier in 100 to exceed the budget.
    assert under >= 0.99
