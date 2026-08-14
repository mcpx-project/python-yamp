"""Extension filter chain: hook points, the closed verdict set, and host-
enforced failure policy (ε0; paper §6.3/§6.4).

A filter observes a message at a hook point and returns one of five verdicts:
``allow``, ``deny``, ``mutate``, ``annotate``, ``quarantine``. The chain applies
them in order, accumulating ``mutate``/``annotate`` onto the message and short-
circuiting on ``deny``/``quarantine``. A filter that raises is resolved by its
declared failure policy (fail-closed -> deny, the secure default; fail-open ->
allow, for advisory filters), and that resolution is enforced by the host here,
never trusted to the filter. ``deny`` and ``quarantine`` map to a clean
``-32001`` policy error (:data:`errors.POLICY_DENIED`).

The verdict transforms and the failure resolution are pure and deterministic, so
both arms produce identical outcomes; the differential corpus pins them. This
module declares the whole hook-point taxonomy; ε0 wires only the request phase
into the router, and later increments attach the rest.
"""

from __future__ import annotations

from typing import Any

from . import content, errors
from .jsonrpc import Message, error

# Hook points (§6.3). ε0 declares the taxonomy; the router wires REQUEST first.
CONNECTION = "connection"
LIFECYCLE = "lifecycle"
REQUEST = "request"
RESPONSE = "response"
NOTIFICATION = "notification"
CONTENT_BLOCK = "content_block"
CATALOG = "catalog"
AUTH_SESSION = "auth_session"
HOOK_POINTS = frozenset(
    {CONNECTION, LIFECYCLE, REQUEST, RESPONSE, NOTIFICATION, CONTENT_BLOCK, CATALOG, AUTH_SESSION}
)

# The closed verdict set (§6.4).
ALLOW = "allow"
DENY = "deny"
MUTATE = "mutate"
ANNOTATE = "annotate"
QUARANTINE = "quarantine"
VERDICTS = frozenset({ALLOW, DENY, MUTATE, ANNOTATE, QUARANTINE})

# Failure policy (§6.4), enforced by the host.
FAIL_OPEN = "fail_open"
FAIL_CLOSED = "fail_closed"

# Message direction, for interest declarations (ICAP REQMOD vs RESPMOD).
C2U = "c2u"  # client -> upstream (a request being routed)
U2C = "u2c"  # upstream -> client (a result travelling back)

# The ICAP-Preview continuation signal: the filter wants the rest of the payload.
CONTINUE = "continue"


def resolve_failure(policy: str) -> str:
    """Resolve a raised filter to a verdict kind by its failure policy.

    fail-closed denies (the secure default for security-class filters);
    fail-open allows (permitted for advisory filters only).
    """
    return DENY if policy == FAIL_CLOSED else ALLOW


def deny_response(id: Any, reason: str) -> Message:
    """The clean JSON-RPC policy error a ``deny``/``quarantine`` maps to."""
    return error(id, errors.POLICY_DENIED, reason)


def _apply_one(verdict: Message, message: Message) -> Message:
    """Apply one non-terminal verdict to a request, returning the message.

    ``allow`` passes it through; ``mutate`` substitutes the call arguments (the
    seam by which a CDR-rewritten payload re-enters the call); ``annotate``
    merges provenance into ``params._meta``.
    """
    kind = verdict["kind"]
    if kind == ALLOW:
        return message
    params = dict(message.get("params") or {})
    if kind == MUTATE:
        params["arguments"] = verdict["arguments"]
    elif kind == ANNOTATE:
        meta = dict(params.get("_meta") or {})
        meta.update(verdict["provenance"])
        params["_meta"] = meta
    updated = dict(message)
    updated["params"] = params
    return updated


def chain_outcome(verdicts: list[Message], request: Message) -> Message:
    """Reduce an ordered verdict list against a request to a single outcome.

    Accumulates ``mutate``/``annotate`` onto the message and short-circuits on
    the first ``deny``/``quarantine`` with a ``-32001`` response. Returns either
    ``{"action": "forward", "message": ...}`` or
    ``{"action": "block", "response": ..., "quarantined": bool}``.
    """
    message = request
    for verdict in verdicts:
        kind = verdict["kind"]
        if kind in (DENY, QUARANTINE):
            response = deny_response(request.get("id"), verdict.get("reason", ""))
            return {"action": "block", "response": response, "quarantined": kind == QUARANTINE}
        message = _apply_one(verdict, message)
    return {"action": "forward", "message": message}


# ---- Interest declaration (§6.4): uninterested traffic pays zero cost. ----
# An interest is a dict with optional list fields ``methods``, ``directions``,
# ``tools``, ``content_types``. An absent or empty list matches anything; ``"*"``
# is an explicit wildcard; ``content_types`` also honors ``type/*`` prefixes.


def _matches(declared: Any, value: str | None) -> bool:
    if not declared or "*" in declared:
        return True
    return value in declared


def _mime_match(pattern: str, mime: str) -> bool:
    if pattern in (mime, "*"):
        return True
    return pattern.endswith("/*") and mime.startswith(pattern[:-1])


def _matches_content(declared: Any, present: list[str]) -> bool:
    if not declared or "*" in declared:
        return True
    return any(_mime_match(pattern, mime) for pattern in declared for mime in present)


def interested(interest: Message, context: Message) -> bool:
    """Whether a filter declaring ``interest`` cares about a message ``context``.
    Every declared dimension must match; an undeclared dimension matches all."""
    return (
        _matches(interest.get("methods"), context.get("method"))
        and _matches(interest.get("directions"), context.get("direction"))
        and _matches(interest.get("tools"), context.get("tool"))
        and _matches_content(interest.get("content_types"), context.get("content_types") or [])
    )


def message_context(message: Message, direction: str) -> Message:
    """The interest-matching context of a message: its method, the called tool
    (for tools/call), the direction, and the content types it carries."""
    method = message.get("method")
    params = message.get("params") or {}
    tool = params.get("name") if method == "tools/call" else None
    mimes = sorted({b["mime"] for b in content.blocks(message) if b.get("mime")})
    return {"method": method, "tool": tool, "direction": direction, "content_types": mimes}


# ---- Preview phase (§6.4), modeled on ICAP Preview. ----


def preview(data: bytes, n: int) -> Message:
    """The first ``n`` bytes offered to a filter, plus ``ieof`` (the preview is
    the entire payload, so no continuation exists). Bytes are hex, JSON-safe."""
    take = max(0, n)
    return {"preview": data[:take].hex(), "ieof": take >= len(data)}


def preview_resolve(decision: str, ieof: bool) -> Message:
    """Resolve a filter's preview response. A terminal verdict decides early
    (the payload is never fully buffered); ``continue`` proceeds to a full scan
    when the preview already held everything, else asks the host for the rest."""
    if decision != CONTINUE:
        return {"action": "verdict", "kind": decision}
    return {"action": "scan_full" if ieof else "need_more"}


class Filter:
    """A message filter. Subclasses set ``name``/``failure_policy``, may declare
    an :meth:`interest`, and return a verdict dict (``{"kind": ...}``) from
    :meth:`evaluate`. Raising is allowed; the host resolves it through the
    failure policy."""

    name: str = "filter"
    failure_policy: str = FAIL_CLOSED

    def interest(self) -> Message:
        return {}

    def evaluate(self, hook: str, message: Message) -> Message:  # pragma: no cover - interface
        raise NotImplementedError


class FilterChain:
    """An ordered chain of filters run at one hook point.

    Each filter sees the message as shaped by the filters before it; a raised
    filter is resolved by its failure policy; evaluation stops at the first
    terminal verdict. The outcome semantics live in :func:`chain_outcome`, so
    the chain never re-implements them.
    """

    def __init__(self, filters: list[Filter]):
        self._filters = list(filters)

    def run(self, hook: str, request: Message) -> Message:
        context = message_context(request, C2U)
        verdicts: list[Message] = []
        message = request
        for handler in self._filters:
            if not interested(handler.interest(), context):
                continue  # uninterested traffic pays zero cost (§6.4)
            try:
                verdict = handler.evaluate(hook, message)
            except Exception:
                verdict = {"kind": resolve_failure(getattr(handler, "failure_policy", FAIL_CLOSED))}
            verdicts.append(verdict)
            if verdict["kind"] in (DENY, QUARANTINE):
                break
            message = _apply_one(verdict, message)
        return chain_outcome(verdicts, request)
