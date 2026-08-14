"""Reference ICAP bridge (ε4): pure ICAP mapping plus the end-to-end scanner."""

import asyncio
import base64

from yamp import icap, jsonrpc
from yamp.callout import CalloutClient
from yamp.icap import ContentScanner
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


# ---- pure functions ----


def test_mode_and_deref():
    assert icap.icap_mode("c2u") == "REQMOD"
    assert icap.icap_mode("u2c") == "RESPMOD"
    assert icap.should_deref("resource_link", True) is True
    assert icap.should_deref("resource_link", False) is False
    assert icap.should_deref("image", True) is False


def test_icap_to_callout_mapping():
    assert icap.icap_to_callout({"status": 204}) == {"verdict": "allow"}
    modified = base64.b64encode(b"clean").decode()
    assert icap.icap_to_callout({"status": 200, "modified": modified, "istag": "av-1"}) == {
        "verdict": "mutate",
        "content": modified,
        "provenance": {"icap": "modified", "istag": "av-1"},
    }
    assert icap.icap_to_callout({"status": 200}) == {"verdict": "allow"}
    assert icap.icap_to_callout({"status": 403}) == {"verdict": "deny", "reason": "ICAP policy blocked"}
    assert icap.icap_to_callout({"status": 200, "threat": "eicar"}) == {"verdict": "quarantine", "reason": "eicar"}
    assert icap.icap_to_callout({"status": 500})["reason"] == "unexpected ICAP status"


# ---- end-to-end: ContentScanner against a scripted bridge service ----


def _message():
    return {
        "jsonrpc": "2.0",
        "id": 5,
        "result": {
            "content": [
                {"type": "text", "text": "hello"},
                {"type": "image", "data": base64.b64encode(b"rawimg").decode(), "mimeType": "image/png"},
                {"type": "resource_link", "uri": "file:///x", "mimeType": "text/plain"},
            ]
        },
    }


def _scan(message, icap_responses):
    log = []

    async def service(read_pipe, write_pipe):
        transport = LineTransport(read_pipe.reader, write_pipe)
        index = 0
        try:
            while True:
                raw = await transport.receive()
                if raw is None:
                    return
                log.append(jsonrpc.decode(raw)["context"]["direction"])
                response = icap_responses[index] if index < len(icap_responses) else {"status": 204}
                index += 1
                await transport.send(jsonrpc.encode(icap.icap_to_callout(response)))
        finally:
            write_pipe.write_eof()

    async def scenario():
        c2s, s2c = MemoryPipe(), MemoryPipe()
        scanner = ContentScanner(CalloutClient(LineTransport(s2c.reader, c2s)))
        service_task = asyncio.create_task(service(c2s, s2c))
        outcome = await scanner.scan(message, "u2c")
        c2s.write_eof()
        await asyncio.wait_for(service_task, timeout=5)
        return outcome, log

    return asyncio.run(scenario())


def test_clean_content_passes_unchanged():
    message = _message()
    outcome, log = _scan(message, [{"status": 204}, {"status": 204}])
    assert outcome == {"action": "forward", "message": message}
    assert log == ["u2c", "u2c"]  # text + image scanned; resource_link skipped


def test_infected_block_is_quarantined():
    outcome, _ = _scan(_message(), [{"status": 200, "threat": "eicar"}])
    assert outcome["action"] == "block"
    assert outcome["quarantined"] is True
    assert outcome["response"]["error"]["message"] == "eicar"


def test_modified_body_is_substituted_and_annotated():
    cleaned = base64.b64encode(b"cleaned").decode()
    outcome, _ = _scan(_message(), [{"status": 204}, {"status": 200, "modified": cleaned, "istag": "av-1"}])
    assert outcome["action"] == "forward"
    image = outcome["message"]["result"]["content"][1]["data"]
    assert base64.b64decode(image) == b"cleaned"
    assert outcome["message"]["result"]["_meta"] == {"icap": "modified", "istag": "av-1"}


def test_resource_link_is_skipped_without_deref():
    # Only the text and image blocks trigger a callout; the resource_link does not.
    _, log = _scan(_message(), [{"status": 204}, {"status": 204}])
    assert len(log) == 2


def test_text_block_mutation_rewrites_text():
    cleaned = base64.b64encode(b"scrubbed").decode()
    message = {"jsonrpc": "2.0", "id": 1, "result": {"content": [{"type": "text", "text": "dirty"}]}}
    outcome, _ = _scan(message, [{"status": 200, "modified": cleaned}])
    assert outcome["message"]["result"]["content"][0]["text"] == "scrubbed"
