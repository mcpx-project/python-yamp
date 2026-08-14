"""Redacting live capture (Track U ``tap``, the ``tcpdump``-for-the-proxy analog).

A tap lets an operator watch live traffic, so it must never surface a credential.
:func:`redact` is the single source of that guarantee: a pure, deterministic deep
masking of every sensitive value in a JSON message, pinned across arms in the
differential corpus. :func:`capture` wraps a message into a redacted capture record.
The serving entrypoints print these records under ``--tap``.
"""

from __future__ import annotations

MASK = "***"

# Keys whose values carry credentials or identity and must never appear in a capture.
# Matched case-insensitively, so Authorization / apiKey / API_KEY all redact.
SENSITIVE_KEYS = frozenset(
    {"authorization", "token", "secret", "password", "apikey", "api_key", "credential", "claims"}
)


def redact(message):
    """Return a copy of a JSON message with every sensitive value masked, wherever it
    appears in the tree. Pure and deterministic, so a capture is safe to log and is
    pinned across arms."""
    if isinstance(message, dict):
        return {key: (MASK if key.lower() in SENSITIVE_KEYS else redact(value)) for key, value in message.items()}
    if isinstance(message, list):
        return [redact(item) for item in message]
    return message


def capture(direction: str, message) -> dict:
    """A redacted capture record for one message: its ``direction`` (``c2s``/``s2c``),
    its method and id for quick scanning, and the fully redacted payload."""
    method = message.get("method") if isinstance(message, dict) else None
    ident = message.get("id") if isinstance(message, dict) else None
    return {"direction": direction, "method": method, "id": ident, "message": redact(message)}
