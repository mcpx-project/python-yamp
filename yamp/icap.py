"""Reference ICAP bridge (ε4).

An ordinary tier-2 extension: nothing in the core knows ICAP exists. Two halves.

The bridge-service side translates an ICAP response into a callout response
(:func:`icap_to_callout`), so the same ε3 wire protocol carries it: a threat
quarantines; ``204`` passes unmodified; ``200`` with a modified body substitutes
the CDR-rewritten content and annotates provenance; ``403`` denies; any other
status denies, because a security bridge fails safe. Client-to-upstream payloads
are REQMOD, upstream-to-client RESPMOD (:func:`icap_mode`). A ``resource_link`` is
dereferenced only on an explicit opt-in (:func:`should_deref`): a link fetch is an
SSRF surface, never a default. These three are pure and pinned in the corpus.

The yamp side (:class:`ContentScanner`) iterates a message's content blocks (ε1),
calls the out-of-process bridge per block (ε3), applies the block verdicts
(substitute bytes, annotate provenance, or block on deny/quarantine), and
aggregates to an outcome shaped like the filter chain's. It is integration-tested
end to end against a scripted bridge service.
"""

from __future__ import annotations

import base64 as _b64
import binascii
from copy import deepcopy

from . import content, filters
from .callout import CalloutClient, VerdictCache
from .jsonrpc import Message

REQMOD = "REQMOD"
RESPMOD = "RESPMOD"


def icap_mode(direction: str) -> str:
    """REQMOD for client->upstream payloads, RESPMOD for upstream->client."""
    return REQMOD if direction == filters.C2U else RESPMOD


def should_deref(kind: str, enabled: bool) -> bool:
    """Whether to dereference a ``resource_link`` for scanning. Off by default: a
    link fetch is an SSRF surface, so dereferencing is an explicit opt-in."""
    return kind == "resource_link" and enabled


def icap_to_callout(response: Message) -> Message:
    """Translate an ICAP response into a callout response (§6.5). ``content`` and
    ``modified`` stay base64 on the wire; the yamp side decodes them."""
    threat = response.get("threat")
    if isinstance(threat, str) and threat:
        return {"verdict": "quarantine", "reason": threat}
    status = response.get("status")
    if status == 204:
        return {"verdict": "allow"}
    if status == 200:
        modified = response.get("modified")
        if isinstance(modified, str):
            return {"verdict": "mutate", "content": modified, "provenance": {"icap": "modified", "istag": response.get("istag")}}
        return {"verdict": "allow"}
    if status == 403:
        return {"verdict": "deny", "reason": "ICAP policy blocked"}
    return {"verdict": "deny", "reason": "unexpected ICAP status"}


def _block_payload(block: Message) -> bytes | None:
    if block.get("bytes") is not None:
        return bytes.fromhex(block["bytes"])
    if block.get("text") is not None:
        return block["text"].encode("utf-8")
    return None


def _apply_mutation(message: Message, block: Message, new_hex: str) -> Message:
    new_bytes = bytes.fromhex(new_hex)
    if block.get("bytes") is not None:
        return content.set_bytes(message, block["path"], new_bytes)
    return content.set_text(message, block["path"], new_bytes.decode("utf-8", errors="replace"))


def _annotate(message: Message, provenance: Message) -> Message:
    out = deepcopy(message)
    holder = out["result"] if isinstance(out.get("result"), dict) else out.setdefault("params", {})
    meta = dict(holder.get("_meta") or {})
    meta.update(provenance)
    holder["_meta"] = meta
    return out


class ContentScanner:
    """The yamp side of the ICAP bridge: scan a message's content blocks through
    the out-of-process bridge and apply the verdicts."""

    def __init__(self, client: CalloutClient, cache: VerdictCache | None = None) -> None:
        self._client = client
        self._cache = cache

    async def scan(self, message: Message, direction: str) -> Message:
        context = filters.message_context(message, direction)
        working = message
        provenance: Message = {}
        for block in content.blocks(message):
            payload = _block_payload(block)
            if payload is None:
                continue  # resource_link/unknown: no inline bytes (deref is opt-in)
            verdict = await self._client.scan(context, payload, self._cache)
            kind = verdict["kind"]
            if kind in (filters.DENY, filters.QUARANTINE):
                response = filters.deny_response(message.get("id"), verdict.get("reason", ""))
                return {"action": "block", "response": response, "quarantined": kind == filters.QUARANTINE}
            if kind == filters.MUTATE and verdict.get("bytes") is not None:
                working = _apply_mutation(working, block, verdict["bytes"])
            if verdict.get("provenance"):
                provenance.update(verdict["provenance"])
        if provenance:
            working = _annotate(working, provenance)
        return {"action": "forward", "message": working}
