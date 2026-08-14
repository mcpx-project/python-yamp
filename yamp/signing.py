"""Signing / attesting proxy (corpus SEP-2828, SEP-2787, SEP-2809).

The newest corpus movement models the proxy itself as an accountable, signing
participant: before a call it emits a client attestation, and after it a paired,
signed outcome record, on a best-effort path that never blocks traffic. Records
are canonicalized to RFC-8785 (JCS) bytes, signed detached, and hash-chained so
the log is tamper-evident.

The digest is SHA-256 and the detached signature is HMAC-SHA256 over the
record's RFC-8785 canonical bytes (RFC 2104, keyed by the audit secret): real,
production-grade primitives, both in the Python stdlib (`hashlib`/`hmac`) and
hand-rolled to the same bytes in the Rust arm, so the two arms stay byte-identical
with no external crypto dependency. An asymmetric Ed25519 detached signature (the
other construction SEP-2828 allows) is not used, because the Python arm is
stdlib-only and the standard library ships no Ed25519; a deployment that can take
a crypto dependency substitutes it behind this same interface.
"""

from __future__ import annotations

import hashlib
import hmac
import json

# The chain and signature are 256-bit, so the genesis link is 64 hex zeros.
GENESIS = "0" * 64


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical(record: dict) -> bytes:
    """RFC-8785-style canonical bytes: object keys sorted at every level, no
    insignificant whitespace, UTF-8. Records should hold only ints/strings so the
    two arms serialize numbers identically."""
    return json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sign(secret: str, record: dict) -> str:
    """A detached HMAC-SHA256 signature over the record's canonical bytes."""
    return hmac.new(secret.encode("utf-8"), canonical(record), hashlib.sha256).hexdigest()


def verify(secret: str, record: dict, signature: str) -> bool:
    return hmac.compare_digest(sign(secret, record), signature)


def chain(prev_hash: str, record: dict) -> str:
    """The next hash-chain link: SHA-256 of the previous link and this record."""
    return _sha256((prev_hash + "|").encode("utf-8") + canonical(record))


def attestation_record(principal: str, method: str, name: str | None) -> dict:
    """A pre-call client attestation (SEP-2787)."""
    return {"type": "attestation", "principal": principal, "method": method, "name": name}


def outcome_record(method: str, name: str | None, ok: bool) -> dict:
    """A post-call signed outcome record (SEP-2828)."""
    return {"type": "outcome", "method": method, "name": name, "ok": ok}


class AuditLog:
    """A tamper-evident, append-only log of signed, hash-chained records."""

    def __init__(self, secret: str, genesis: str = GENESIS) -> None:
        self._secret = secret
        self._genesis = genesis
        self._head = genesis
        self.records: list[dict] = []

    def append(self, record: dict) -> dict:
        entry = {
            "record": record,
            "prev": self._head,
            "signature": sign(self._secret, record),
            "hash": chain(self._head, record),
        }
        self._head = entry["hash"]
        self.records.append(entry)
        return entry

    def verify(self) -> bool:
        """Whether every record's signature and chain link is intact."""
        head = self._genesis
        for entry in self.records:
            if not verify(self._secret, entry["record"], entry["signature"]):
                return False
            if entry["prev"] != head:
                return False
            if chain(head, entry["record"]) != entry["hash"]:
                return False
            head = entry["hash"]
        return True
