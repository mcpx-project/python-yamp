"""Typed content-block iterator (ε1, content-block hook).

A single traversal that finds every content block in a message and yields a
normalized descriptor per block, so a scanner never reimplements MCP's JSON
shape. It covers the two places typed content lives: a tool result's
``result.content[]`` (text/image/audio/resource/resource_link) and a
``resources/read`` result's ``result.contents[]``. Binary payloads (image/audio
``data``, resource ``blob``) are base64-decoded to raw bytes, surfaced as
lowercase hex so the descriptor is JSON-safe and byte-comparable across arms.

Each descriptor carries the ``path`` to its payload field, so a mutation writes
back exactly there (:func:`set_text`/:func:`set_bytes`, the seam a CDR rewrite
uses) without reconstructing the wire shape. Pure and deterministic; the
differential corpus pins the traversal and both mutations. The Python arm uses
its stdlib ``base64`` (the Rust arm's :mod:`base64` module matches it byte-for-
byte), keeping one base64 source per arm.
"""

from __future__ import annotations

import base64 as _b64
import binascii
from copy import deepcopy
from typing import Any

from .jsonrpc import Message


def _bytes_hex(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        return _b64.b64decode(value, validate=True).hex()
    except (binascii.Error, ValueError):
        return None


def _descriptor(kind, path, mime=None, uri=None, text=None, bytes_hex=None) -> Message:
    return {"kind": kind, "path": path, "mime": mime, "uri": uri, "text": text, "bytes": bytes_hex}


def blocks(message: Message) -> list[Message]:
    """Every content block in ``message``, normalized. Empty when there is no
    recognized content array."""
    out: list[Message] = []
    result = message.get("result")
    if not isinstance(result, dict):
        return out
    for index, item in enumerate(result.get("content") or []):
        _content_block(index, item, out)
    for index, item in enumerate(result.get("contents") or []):
        uri = item.get("uri")
        mime = item.get("mimeType")
        if "text" in item:
            out.append(_descriptor("resource", ["result", "contents", index, "text"], mime, uri, item["text"]))
        elif "blob" in item:
            out.append(_descriptor("resource", ["result", "contents", index, "blob"], mime, uri, bytes_hex=_bytes_hex(item.get("blob"))))
    return out


def _content_block(index: int, item: Message, out: list[Message]) -> None:
    base = ["result", "content", index]
    kind = item.get("type")
    if kind == "text":
        out.append(_descriptor("text", base + ["text"], text=item.get("text")))
    elif kind in ("image", "audio"):
        out.append(_descriptor(kind, base + ["data"], mime=item.get("mimeType"), bytes_hex=_bytes_hex(item.get("data"))))
    elif kind == "resource_link":
        out.append(_descriptor("resource_link", base + ["uri"], mime=item.get("mimeType"), uri=item.get("uri")))
    elif kind == "resource":
        resource = item.get("resource") or {}
        uri = resource.get("uri")
        mime = resource.get("mimeType")
        if "text" in resource:
            out.append(_descriptor("resource", base + ["resource", "text"], mime, uri, resource["text"]))
        elif "blob" in resource:
            out.append(_descriptor("resource", base + ["resource", "blob"], mime, uri, bytes_hex=_bytes_hex(resource.get("blob"))))
    else:
        out.append(_descriptor("unknown", base))


def _navigate(root: Any, path: list) -> tuple[Any, Any] | None:
    # Return (container, key) for the slot at path, or None if unreachable.
    current = root
    for segment in path[:-1]:
        if isinstance(segment, str) and isinstance(current, dict) and segment in current:
            current = current[segment]
        elif isinstance(segment, int) and isinstance(current, list) and 0 <= segment < len(current):
            current = current[segment]
        else:
            return None
    leaf = path[-1]
    if isinstance(leaf, str) and isinstance(current, dict) and leaf in current:
        return current, leaf
    if isinstance(leaf, int) and isinstance(current, list) and 0 <= leaf < len(current):
        return current, leaf
    return None


def set_text(message: Message, path: list, text: str) -> Message:
    """Replace the text payload at ``path`` with ``text``, returning a new message."""
    out = deepcopy(message)
    slot = _navigate(out, path)
    if slot is not None:
        container, key = slot
        container[key] = text
    return out


def set_bytes(message: Message, path: list, data: bytes) -> Message:
    """Replace the binary payload at ``path`` with base64-encoded ``data``,
    returning a new message. The seam by which a CDR-rewritten payload re-enters."""
    out = deepcopy(message)
    slot = _navigate(out, path)
    if slot is not None:
        container, key = slot
        container[key] = _b64.b64encode(data).decode("ascii")
    return out
