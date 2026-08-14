import asyncio

from yamp.relay import Relay
from yamp.transport.framed import FramedTransport
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


async def _echo_framed_backend(inbound: MemoryPipe, outbound: MemoryPipe):
    """A backend that speaks content-length framing and echoes each message."""
    transport = FramedTransport(inbound.reader, outbound)
    while True:
        message = await transport.receive()
        if message is None:
            outbound.write_eof()
            return
        await transport.send(message)


async def _run_bridge(messages):
    # Client speaks line framing (stdio); backend speaks content-length (HTTP).
    c2r, r2c = MemoryPipe(), MemoryPipe()  # client <-> relay
    r2b, b2r = MemoryPipe(), MemoryPipe()  # relay <-> backend

    relay = Relay(
        client=LineTransport(c2r.reader, r2c),
        backend=FramedTransport(b2r.reader, r2b),
    )
    backend_task = asyncio.create_task(_echo_framed_backend(r2b, b2r))
    relay_task = asyncio.create_task(relay.run())

    for message in messages:
        await c2r.write(message + b"\n")
    c2r.write_eof()

    client_in = LineTransport(r2c.reader, c2r)
    received = []
    while True:
        message = await client_in.receive()
        if message is None:
            break
        received.append(message)

    await asyncio.wait_for(asyncio.gather(relay_task, backend_task), timeout=5)
    return received


def test_bridge_preserves_messages_and_boundaries():
    messages = [b'{"jsonrpc":"2.0","id":1}', b'{"jsonrpc":"2.0","id":2}', b"ping"]
    received = asyncio.run(_run_bridge(messages))
    assert received == messages


def test_bridge_is_byte_faithful_for_framing_like_payloads():
    # Payloads containing newline-free but framing-suggestive bytes must survive
    # the line -> content-length bridge unchanged.
    messages = [
        b'{"m":"has \\r\\n and Content-Length: 5 inside"}',
        b'{"unicode":"cafe\\u0301"}',
    ]
    received = asyncio.run(_run_bridge(messages))
    assert received == messages


def test_relay_stops_when_client_closes_immediately():
    received = asyncio.run(_run_bridge([]))
    assert received == []
