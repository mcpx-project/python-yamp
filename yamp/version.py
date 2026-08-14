"""Protocol version negotiation (SEP §2.1, §2.2; corpus SEP-2575).

Single source of the MCP protocol versions this proxy can serve and of the rule
applied when a peer names one. Two modes negotiate differently:

- Stateful (SEP §2.2): the intermediary always presents its own highest version
  and lets the MCP client-side handshake settle; it never rejects. That value is
  ``STATEFUL_PROTOCOL_VERSION``, re-exported as ``forward.PROXY_PROTOCOL_VERSION``.
- Stateless (SEP-2575): each request is self-describing and carries its version
  in ``_meta`` under ``PROTOCOL_VERSION_META_KEY``. There is no handshake to fall
  back on, so a request naming a version the proxy cannot serve is rejected with
  ``-32004 UNSUPPORTED_PROTOCOL_VERSION`` whose data names the supported set.

Keeping the version set and the codes here (not per module) follows the repo's
single-source convention, the same way JSON-RPC codes live in ``jsonrpc``.
"""

from __future__ import annotations

from .errors import UNSUPPORTED_PROTOCOL_VERSION  # single source; re-exported here

# Newest first. The head is the proxy's own highest version (SEP §2.2); every
# member is accepted on stateless negotiation.
SUPPORTED_PROTOCOL_VERSIONS: tuple[str, ...] = ("2026-07-28", "2025-06-18")

# MCP stateless semantics (SEP-2575): sessionless, per-request _meta.
STATELESS_PROTOCOL_VERSION = "2026-07-28"
# Legacy dual-handshake semantics (SEP §2.2), the version the stateful served
# path advertises.
STATEFUL_PROTOCOL_VERSION = "2025-06-18"

# Where a stateless request carries its protocol version (SEP-2575). There is no
# initialize handshake to pin it, so it travels per-request in _meta.
PROTOCOL_VERSION_META_KEY = "io.modelcontextprotocol/protocolVersion"

# The -32004 code for a version the proxy cannot serve lives in the errors
# registry and is re-exported above; the data payload names what would work.


def is_supported(version: str) -> bool:
    return version in SUPPORTED_PROTOCOL_VERSIONS


def negotiate(requested: str | None, default: str = STATELESS_PROTOCOL_VERSION) -> str | None:
    """Resolve the version a stateless request will run under.

    A request that omits the version accepts ``default``. A request that names a
    supported version gets exactly that version. Any other named version cannot
    be served and yields ``None`` (the caller emits ``-32004``).
    """
    if requested is None:
        return default
    return requested if requested in SUPPORTED_PROTOCOL_VERSIONS else None


def unsupported_error_data(requested: str | None) -> dict:
    """The ``error.data`` for a ``-32004``: what was asked, what would work."""
    return {"requested": requested, "supported": list(SUPPORTED_PROTOCOL_VERSIONS)}
