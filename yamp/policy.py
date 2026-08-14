"""Layer 5 policy (SEP §4, draft §6.5).

Credential injection is the recommended default: the proxy holds per-backend
credentials and injects them into backend requests, and the client's own
credentials are never forwarded to a backend (SEP §13.1, confused deputy).
Header forwarding is scoped per backend and may rename headers. Client
authentication is pluggable.
"""

from __future__ import annotations

from typing import Protocol

AUTHORIZATION = "Authorization"


class Authenticator(Protocol):
    def authenticate(self, client_headers: dict[str, str]) -> bool: ...


class BearerAuthenticator:
    def __init__(self, valid_tokens: set[str]) -> None:
        self._valid = set(valid_tokens)

    def authenticate(self, client_headers: dict[str, str]) -> bool:
        header = client_headers.get(AUTHORIZATION, "")
        return header.startswith("Bearer ") and header[len("Bearer ") :] in self._valid


class PolicyLayer:
    def __init__(
        self,
        backend_tokens: dict[str, str] | None = None,
        forward_headers: dict[str, list[dict]] | None = None,
        client_authenticator: Authenticator | None = None,
    ) -> None:
        self._backend_tokens = backend_tokens or {}
        self._forward_headers = forward_headers or {}
        self._authenticator = client_authenticator

    def authorize_client(self, client_headers: dict[str, str]) -> bool:
        if self._authenticator is None:
            return True
        return self._authenticator.authenticate(client_headers)

    def backend_headers(self, backend_id: str, client_headers: dict[str, str]) -> dict[str, str]:
        """Build the headers to send to ``backend_id``.

        Backend credentials are injected; the client's own ``Authorization`` is
        not forwarded unless an explicit forward rule requests it. Forward rules
        are scoped to this backend only.
        """
        headers: dict[str, str] = {}
        token = self._backend_tokens.get(backend_id)
        if token is not None:
            headers[AUTHORIZATION] = f"Bearer {token}"
        for rule in self._forward_headers.get(backend_id, []):
            source = rule["name"]
            if source in client_headers:
                headers[rule.get("backendHeader", source)] = client_headers[source]
        return headers
