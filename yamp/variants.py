"""Server variants and variant-bound cursors (corpus SEP-2053).

SEP-2053 lets a server expose several parallel *variants* (for example
``claude-optimized`` and ``compact``) that reshape the same capabilities. A
client enumerates them from the negotiated extension payload during
``initialize`` and selects one per request via a canonical ``_meta`` key. All
selection is stateless: the variant rides in ``_meta``, never in session state.

yamp is a proxy, so its obligation is a routing one, not a variant
implementation. Three pieces:

- *Enumeration*: compose the backends' ``availableVariants`` into the proxy's own
  advertised extension. A variant the proxy offers must be selectable on every
  backend that supports variants at all, so the composition is an intersection;
  a backend that does not carry the extension is variant-agnostic and imposes no
  constraint.
- *Selection*: forward the client's per-request variant to backends, and reject a
  variant the composed set does not contain with ``-32602`` before any backend is
  touched (SEP-2053 selection rules).
- *Cursor binding*: pagination cursors are variant-scoped (SEP-2053 rule 2-3).
  When the proxy aggregates a paginated list it mints one opaque composite cursor
  that binds the active variant and each paginating backend's own cursor. A
  continuation request reverse-resolves it to exactly those backends with their
  own cursors, and is rejected with ``-32602`` if its variant differs from the
  one the cursor was minted under.

The composite cursor is hex-encoded canonical JSON (opaque to clients, no new
dependency; SEP-2053 permits any opaque encoding), so the proxy carries no
per-cursor state.
"""

from __future__ import annotations

import json

# The negotiated extension id and the canonical per-request selection key
# (SEP-2053 "Extension id" / "Canonical per-request _meta key").
EXTENSION_ID = "io.modelcontextprotocol/server-variants"
SERVER_VARIANT_META_KEY = "io.modelcontextprotocol/server-variant"

# Marks a cursor this proxy minted, so a raw backend cursor is never mistaken for
# a composite one. Versioned so the encoding can evolve.
CURSOR_PREFIX = "yv1:"


def selected_variant(params: dict | None) -> str | None:
    """The per-request variant id from a request's ``_meta``, or ``None``."""
    meta = params.get("_meta") if isinstance(params, dict) else None
    if isinstance(meta, dict):
        variant = meta.get(SERVER_VARIANT_META_KEY)
        if isinstance(variant, str):
            return variant
    return None


def _payload(capabilities: dict | None) -> list | None:
    ext = (capabilities or {}).get("extensions")
    payload = ext.get(EXTENSION_ID) if isinstance(ext, dict) else None
    if not isinstance(payload, dict):
        return None
    variants = payload.get("availableVariants")
    return variants if isinstance(variants, list) else None


def available_variants(capabilities: dict | None) -> list[str]:
    """The variant ids a backend advertises, in declared (ranked) order."""
    variants = _payload(capabilities)
    if variants is None:
        return []
    return [v["id"] for v in variants if isinstance(v, dict) and isinstance(v.get("id"), str)]


def compose_variants(backend_caps: list[dict]) -> list[dict]:
    """Compose the proxy's ``availableVariants`` across backends (SEP-2053).

    A variant is offered only if every variant-supporting backend offers it (an
    intersection): the proxy cannot honestly serve a variant one of its backends
    cannot. Backends without the extension are variant-agnostic and impose no
    constraint. Order and descriptions follow the first supporting backend, so its
    default (the first entry) stays the proxy's default. Returns ``[]`` when no
    backend supports variants or the intersection is empty.
    """
    supporting = [variants for caps in backend_caps if (variants := _payload(caps))]
    if not supporting:
        return []
    id_sets = [
        {v["id"] for v in variants if isinstance(v, dict) and isinstance(v.get("id"), str)}
        for variants in supporting
    ]
    common = set.intersection(*id_sets)
    return [v for v in supporting[0] if isinstance(v, dict) and v.get("id") in common]


def bind_cursor(variant: str | None, cursors: dict[str, str]) -> str:
    """Mint one opaque composite cursor binding the active variant and each
    paginating backend's own cursor (SEP-2053 rule 2). Canonical JSON, hex-encoded
    so the cursor is opaque and the proxy holds no per-cursor state."""
    body = json.dumps(
        {"v": variant, "c": cursors}, separators=(",", ":"), sort_keys=True, ensure_ascii=False
    )
    return CURSOR_PREFIX + body.encode("utf-8").hex()


def resolve_cursor(cursor: object) -> tuple[str | None, dict[str, str]] | None:
    """Reverse a proxy composite cursor to ``(variant, {backend_id: cursor})``.

    Returns ``None`` when the value is not a proxy-minted cursor or is malformed,
    so a raw backend cursor or a hostile string is never mistaken for one.
    """
    if not isinstance(cursor, str) or not cursor.startswith(CURSOR_PREFIX):
        return None
    try:
        body = bytes.fromhex(cursor[len(CURSOR_PREFIX):]).decode("utf-8")
        payload = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(payload, dict) or not isinstance(payload.get("c"), dict):
        return None
    cursors = {k: v for k, v in payload["c"].items() if isinstance(k, str) and isinstance(v, str)}
    variant = payload.get("v")
    return (variant if isinstance(variant, str) else None, cursors)


def mismatch_data(cursor_variant: str | None, requested_variant: str | None) -> dict:
    """The ``-32602`` error data for a cursor used under the wrong variant
    (SEP-2053 rule 3)."""
    return {"cursorVariant": cursor_variant, "requestedVariant": requested_variant}
