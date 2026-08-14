"""ext-proc callout transport (ε3).

A tier-2 extension runs out of process (an ICAP/AV/DLP/CDR bridge, an LLM-based
scanner). The proxy reaches it with a framed callout that reuses the existing
:mod:`yamp.transport` framing, so no gRPC dependency is pulled in. A callout
carries the ε2 interest context and a content chunk; the service answers with one
of the ε0 verdicts (or the preview ``continue`` signal). Three protections wrap
every callout: the verdict is cached by content digest so a duplicate payload is
not rescanned; a byte budget rejects an oversize payload rather than buffering
it; and a deadline bounds a slow or hung scanner. On any transport failure the
host applies the filter's failure policy (fail-closed denies), never the scanner.

The envelope encoders and the verdict parser are pure and mirror the Rust arm;
the differential corpus pins them. The async client is integration-tested per arm
against a scripted in-process service.
"""

from __future__ import annotations

import asyncio
import base64 as _b64
import binascii
import hashlib

from . import filters
from .jsonrpc import Message, decode, encode
from .transport.base import Transport

CALLOUT_VERSION = "1"
PHASE_PREVIEW = "preview"
PHASE_BODY = "body"

_VALID = filters.VERDICTS | {filters.CONTINUE}


def content_digest(content: bytes) -> str:
    """The verdict-cache key: SHA-256 of the content, content-addressed."""
    return hashlib.sha256(content).hexdigest()


def callout_request(context: Message, phase: str, content: bytes, ieof: bool) -> Message:
    """The callout request envelope the proxy sends to the service."""
    return {
        "callout": CALLOUT_VERSION,
        "phase": phase,
        "context": context,
        "ieof": ieof,
        "content": _b64.b64encode(content).decode("ascii"),
    }


def exceeds_budget(size: int, max_bytes: int) -> bool:
    """Whether ``size`` exceeds a positive ``max_bytes`` budget (0 = unlimited)."""
    return max_bytes > 0 and size > max_bytes


def _decode_hex(raw) -> str | None:
    if not isinstance(raw, str):
        return None
    try:
        return _b64.b64decode(raw, validate=True).hex()
    except (binascii.Error, ValueError):
        return None


def parse_verdict(response, failure_policy: str) -> Message:
    """Parse a service response into a block-scoped verdict. A ``mutate`` carries
    replacement bytes (base64 on the wire, hex in the verdict); ``annotate``
    provenance; ``deny``/``quarantine`` a reason. A malformed response is resolved
    by the failure policy, never trusted."""
    kind = response.get("verdict") if isinstance(response, dict) else None
    if kind not in _VALID:
        return {"kind": filters.resolve_failure(failure_policy), "reason": "malformed callout response"}
    if kind == filters.CONTINUE:
        return {"kind": filters.CONTINUE}
    verdict: Message = {"kind": kind}
    if kind in (filters.DENY, filters.QUARANTINE):
        verdict["reason"] = response.get("reason", "")
    if kind == filters.MUTATE:
        verdict["bytes"] = _decode_hex(response.get("content"))
        if "provenance" in response:  # a modified body may also annotate (§6.5)
            verdict["provenance"] = response["provenance"]
    if kind == filters.ANNOTATE:
        verdict["provenance"] = response.get("provenance", {})
    return verdict


class VerdictCache:
    """A content-addressed cache of callout verdicts (§6.4): identical content
    digests share a verdict, so retries and duplicate payloads are not rescanned."""

    def __init__(self) -> None:
        self._map: dict[str, Message] = {}

    def get(self, digest: str) -> Message | None:
        return self._map.get(digest)

    def put(self, digest: str, verdict: Message) -> None:
        self._map[digest] = verdict


class CalloutClient:
    """An out-of-process callout over a framed transport, with a preview phase, a
    byte budget, a deadline, an optional verdict cache, and a failure policy."""

    def __init__(
        self,
        transport: Transport,
        failure_policy: str = filters.FAIL_CLOSED,
        max_bytes: int = 0,
        preview_bytes: int = 0,
        deadline: float | None = None,
    ) -> None:
        self._transport = transport
        self._failure_policy = failure_policy
        self._max_bytes = max_bytes
        self._preview_bytes = preview_bytes
        self._deadline = deadline

    def _failed(self, reason: str) -> Message:
        return {"kind": filters.resolve_failure(self._failure_policy), "reason": reason}

    async def scan(self, context: Message, content: bytes, cache: VerdictCache | None = None) -> Message:
        """Scan ``content``, consulting ``cache`` first and populating it after.
        Runs the preview phase, escalating to a body call only when the service
        continues."""
        if exceeds_budget(len(content), self._max_bytes):
            return self._failed("payload exceeds byte budget")
        digest = content_digest(content)
        if cache is not None:
            hit = cache.get(digest)
            if hit is not None:
                return hit
        verdict = await self._exchange(context, content)
        if verdict.get("kind") == filters.CONTINUE:
            verdict = await self._call(context, PHASE_BODY, content, True)
        if cache is not None and verdict.get("kind") != filters.CONTINUE:
            cache.put(digest, verdict)
        return verdict

    async def _exchange(self, context: Message, content: bytes) -> Message:
        n = min(self._preview_bytes if self._preview_bytes > 0 else len(content), len(content))
        ieof = n >= len(content)
        return await self._call(context, PHASE_PREVIEW, content[:n], ieof)

    async def _call(self, context: Message, phase: str, content: bytes, ieof: bool) -> Message:
        request = callout_request(context, phase, content, ieof)
        try:
            await self._transport.send(encode(request))
            raw = await self._receive()
        except asyncio.TimeoutError:
            return self._failed("callout deadline exceeded")
        except OSError:
            return self._failed("callout transport error")
        if raw is None:
            return self._failed("callout closed")
        try:
            response = decode(raw)
        except ValueError:
            return self._failed("callout decode error")
        return parse_verdict(response, self._failure_policy)

    async def _receive(self):
        if self._deadline is not None:
            return await asyncio.wait_for(self._transport.receive(), self._deadline)
        return await self._transport.receive()
