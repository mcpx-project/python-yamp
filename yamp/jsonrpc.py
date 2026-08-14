"""Minimal JSON-RPC 2.0 message helpers.

δ1 makes the proxy protocol-aware: it parses envelopes to drive the
initialize handshake. Payloads are still forwarded unchanged once past the
handshake; this module only builds and inspects the handful of messages the
handshake needs.
"""

from __future__ import annotations

import json
from typing import Any

Message = dict[str, Any]

# JSON-RPC 2.0 error codes used across the proxy layers. Centralized here (the
# JSON-RPC layer) so no module redefines them.
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603


def encode(message: Message) -> bytes:
    return json.dumps(message, separators=(",", ":")).encode("utf-8")


def decode(payload: bytes) -> Message:
    return json.loads(payload)


def request(id: Any, method: str, params: Message | None = None) -> Message:
    message: Message = {"jsonrpc": "2.0", "id": id, "method": method}
    if params is not None:
        message["params"] = params
    return message


def notification(method: str, params: Message | None = None) -> Message:
    message: Message = {"jsonrpc": "2.0", "method": method}
    if params is not None:
        message["params"] = params
    return message


def result(id: Any, value: Message) -> Message:
    return {"jsonrpc": "2.0", "id": id, "result": value}


def error(id: Any, code: int, message: str, data: Message | None = None) -> Message:
    body: Message = {"code": code, "message": message}
    if data is not None:
        body["data"] = data
    return {"jsonrpc": "2.0", "id": id, "error": body}


def error_response(id: Any, error: Message) -> Message:
    """Wrap an already-built error body (e.g. from :func:`errors.error_object`)
    into a JSON-RPC response, so a normalized error is emitted verbatim."""
    return {"jsonrpc": "2.0", "id": id, "error": error}


def method_of(message: Message) -> str | None:
    return message.get("method")
