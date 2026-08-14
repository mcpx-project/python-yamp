"""Server-origination concerns (σ0).

yamp is also an MCP server: it originates responses from local handlers rather
than only routing (pure-server mode is a registry with zero backends). One thing
a server must do that a bare proxy need not is attach cache metadata to its list
results, so a downstream cache honors them exactly as yamp's own `ListCache`
does. The SEP-2549 directive keys are the single source in :mod:`cache`; ``ttlMs``
is emitted as an integer so both arms agree byte-for-byte.
"""

from __future__ import annotations

from . import cache, jsonrpc
from .jsonrpc import Message
from .transport.framed import MAX_FRAME_BYTES

# σ5: a server originates responses, so it is accountable for the
# size of what it emits. The ceiling is the same one the framing decoder accepts
# on input (MAX_FRAME_BYTES), so a server never emits a frame it would itself
# refuse to read; a routed backend response needs no separate cap, since the
# decoder that read it already enforced this bound. Single-sourced from the
# transport so the two limits cannot drift.
MAX_OUTPUT_BYTES = MAX_FRAME_BYTES


def exceeds_output_cap(result: object, max_bytes: int = MAX_OUTPUT_BYTES) -> bool:
    """Whether a server-originated ``result``'s encoded (compact) form exceeds
    ``max_bytes``. A cap of zero (or less) is unbounded. Both arms encode compactly
    and the byte length is key-order-independent, so the verdict agrees across arms
    for a given result."""
    if max_bytes <= 0:
        return False
    return len(jsonrpc.encode(result)) > max_bytes


def list_directives(ttl_ms: int, cache_scope: str) -> Message:
    """The SEP-2549 cache directives a server advertises on a list result."""
    return {cache.TTL_MS_KEY: ttl_ms, cache.CACHE_SCOPE_KEY: cache_scope}


def attach_directives(result: Message, ttl_ms: int, cache_scope: str) -> Message:
    """Return ``result`` with the cache directives attached (top-level), the seam
    the served ``tools/list`` and ``server/discover`` results use."""
    out = dict(result)
    out.update(list_directives(ttl_ms, cache_scope))
    return out
