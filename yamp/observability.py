"""Observability (SEP §7, §12): hop tracing and W3C Trace Context.

Single home for the proxy-hop helpers (reused by the transparent Level 2 layer)
and for W3C Trace Context propagation in ``_meta``. Trace ids come from an
injectable source so the behavior is deterministic under test.
"""

from __future__ import annotations

import os
from typing import Callable

from .forward import PROXY_NAME, PROXY_VERSION

PROXY_HOPS_KEY = "io.modelcontextprotocol/proxy-hops"
TRACEPARENT = "traceparent"
TRACESTATE = "tracestate"
BAGGAGE = "baggage"


def proxy_hop(mode: str = "transparent") -> dict:
    """One hop entry (SEP §7.1). Timestamp is added by the deployment; it is
    omitted here so the record is deterministic."""
    return {"name": PROXY_NAME, "mode": mode, "version": PROXY_VERSION, "layers": [1, 2, 3, 4, 5]}


def append_hop(meta: dict, mode: str = "transparent") -> dict:
    hops = list(meta.get(PROXY_HOPS_KEY, []))
    hops.append(proxy_hop(mode))
    augmented = dict(meta)
    augmented[PROXY_HOPS_KEY] = hops
    return augmented


def make_traceparent(trace_id: str, span_id: str) -> str:
    return f"00-{trace_id}-{span_id}-01"


def _default_ids() -> tuple[str, str]:
    return os.urandom(16).hex(), os.urandom(8).hex()


def ensure_trace_context(
    meta: dict,
    new_ids: Callable[[], tuple[str, str]] = _default_ids,
) -> dict:
    """Return ``meta`` with a W3C ``traceparent`` guaranteed present.

    An existing ``traceparent`` is preserved; ``tracestate`` and ``baggage`` are
    forwarded unchanged. When no ``traceparent`` is present one is generated
    (SEP §7.2 / SEP-414).
    """
    augmented = dict(meta)
    if TRACEPARENT not in augmented:
        trace_id, span_id = new_ids()
        augmented[TRACEPARENT] = make_traceparent(trace_id, span_id)
    return augmented
