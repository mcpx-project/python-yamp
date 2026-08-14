"""ext-proc callout transport (ε3): pure envelope/parse plus the async client."""

import asyncio

from yamp import callout, filters, jsonrpc
from yamp.callout import CalloutClient, VerdictCache
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe

CTX = {"method": "tools/call", "tool": "gh__x", "direction": "u2c", "content_types": ["image/png"]}


# ---- pure functions ----


def test_request_envelope_and_digest_and_budget():
    req = callout.callout_request(CTX, "preview", b"scan", True)
    assert req["callout"] == "1" and req["phase"] == "preview" and req["ieof"] is True
    assert req["context"] == CTX and req["content"] == "c2Nhbg=="  # base64("scan")
    assert callout.content_digest(b"") == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    assert callout.exceeds_budget(30, 20) and not callout.exceeds_budget(10, 20)
    assert not callout.exceeds_budget(30, 0)  # 0 = unlimited


def test_parse_verdict_kinds_and_failure_policy():
    import base64

    assert callout.parse_verdict({"verdict": "allow"}, "fail_closed") == {"kind": "allow"}
    assert callout.parse_verdict({"verdict": "deny", "reason": "x"}, "fail_closed") == {"kind": "deny", "reason": "x"}
    mutated = callout.parse_verdict({"verdict": "mutate", "content": base64.b64encode(b"ok").decode()}, "fail_closed")
    assert mutated == {"kind": "mutate", "bytes": b"ok".hex()}
    assert callout.parse_verdict({"verdict": "mutate"}, "fail_closed") == {"kind": "mutate", "bytes": None}
    assert callout.parse_verdict({"verdict": "mutate", "content": "!!!bad"}, "fail_closed") == {"kind": "mutate", "bytes": None}
    assert callout.parse_verdict({"verdict": "annotate", "provenance": {"s": 1}}, "fail_closed") == {"kind": "annotate", "provenance": {"s": 1}}
    assert callout.parse_verdict({"verdict": "continue"}, "fail_closed") == {"kind": "continue"}
    # A malformed response is resolved by the failure policy, not trusted.
    assert callout.parse_verdict({"x": 1}, "fail_closed")["kind"] == "deny"
    assert callout.parse_verdict({"x": 1}, "fail_open")["kind"] == "allow"
    assert callout.parse_verdict("not a dict", "fail_closed")["kind"] == "deny"


# ---- async client against a scripted in-process service ----


def _drive(responder, contents, cache=None, **client_kwargs):
    log = []

    async def service(read_pipe, write_pipe):
        transport = LineTransport(read_pipe.reader, write_pipe)
        try:
            while True:
                raw = await transport.receive()
                if raw is None:
                    return
                request = jsonrpc.decode(raw)
                log.append(request["phase"])
                reply = responder(request)
                if reply == "close":
                    return
                if reply == "garbage":
                    await transport.send(b"{not json")
                    continue
                if reply == "noreply":
                    continue
                await transport.send(jsonrpc.encode(reply))
        finally:
            write_pipe.write_eof()  # unblock the client's receive() on any exit

    async def scenario():
        c2s, s2c = MemoryPipe(), MemoryPipe()
        client = CalloutClient(LineTransport(s2c.reader, c2s), **client_kwargs)
        service_task = asyncio.create_task(service(c2s, s2c))
        results = [await client.scan(CTX, content, cache) for content in contents]
        c2s.write_eof()
        await asyncio.wait_for(service_task, timeout=5)
        return results, log

    return asyncio.run(scenario())


def test_allow_via_service():
    (results, log) = _drive(lambda req: {"verdict": "allow"}, [b"payload"])
    assert results == [{"kind": "allow"}]
    assert log == ["preview"]


def test_early_deny_in_preview_skips_body():
    (results, log) = _drive(lambda req: {"verdict": "deny", "reason": "bad"}, [b"hello world"], preview_bytes=3)
    assert results[0]["kind"] == "deny"
    assert log == ["preview"]  # the body was never sent


def test_continue_escalates_to_body():
    def responder(req):
        return {"verdict": "allow"} if req["phase"] == "body" else {"verdict": "continue"}

    (results, log) = _drive(responder, [b"hello world"], preview_bytes=3)
    assert results[0] == {"kind": "allow"}
    assert log == ["preview", "body"]


def test_cache_avoids_rescan():
    cache = VerdictCache()
    (results, log) = _drive(lambda req: {"verdict": "allow"}, [b"same", b"same"], cache=cache)
    assert results == [{"kind": "allow"}, {"kind": "allow"}]
    assert log == ["preview"]  # the second scan hit the cache


def test_budget_rejects_without_calling():
    (results, log) = _drive(lambda req: {"verdict": "allow"}, [b"too big"], max_bytes=2)
    assert results[0]["kind"] == "deny" and "byte budget" in results[0]["reason"]
    assert log == []  # the service was never contacted


def test_service_close_is_failure_policy():
    (results, log) = _drive(lambda req: "close", [b"payload"])
    assert results[0] == {"kind": "deny", "reason": "callout closed"}


def test_garbage_response_is_decode_error():
    (results, log) = _drive(lambda req: "garbage", [b"payload"])
    assert results[0] == {"kind": "deny", "reason": "callout decode error"}


def test_deadline_bounds_a_hung_scanner():
    (results, log) = _drive(lambda req: "noreply", [b"payload"], deadline=0.05)
    assert results[0] == {"kind": "deny", "reason": "callout deadline exceeded"}


def test_transport_send_error_is_failure_policy():
    class _Broken:
        async def send(self, payload):
            raise OSError("boom")

        async def receive(self):
            return None

    client = CalloutClient(_Broken(), failure_policy=filters.FAIL_OPEN)
    verdict = asyncio.run(client.scan(CTX, b"payload"))
    assert verdict == {"kind": "allow", "reason": "callout transport error"}
