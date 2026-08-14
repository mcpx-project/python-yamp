"""Authentication propagation (SEP §4, draft §9; corpus SEP-2468).

Obligations wired into the served path:

- Credential injection with confused-deputy protection (SEP §13.1): the proxy
  holds each backend's own credential and injects it when forwarding, and it
  never forwards a client's credential to a backend. The credential travels in
  the request ``_meta`` under ``authorization``.
- Issuer/audience validation (SEP-2468): before trusting a client's token the
  proxy checks its ``iss`` and ``aud`` claims, so a token minted for one
  audience cannot be replayed against another (a confused-deputy attack).

Two more token-propagation strategies the draft names (§9.1-9.2) have their
protocol-defined building blocks here. Their live network legs (an OAuth redirect,
a call to a token endpoint) are deployment-integration, like the transparent-mode
platform hook, so they are not wired into the stateless route path; the
deterministic, security-critical transformations are:

- RFC 8693 token exchange: build a token-exchange request that swaps the client's
  token for a backend-scoped one, and read the issued token from the response.
- OAuth 2.1 + PKCE: derive the S256 code challenge from a caller-supplied verifier
  and build the authorization and token requests (RFC 7636), so a proxy acting as
  an OAuth client to a backend uses PKCE.
"""

from __future__ import annotations

import base64
import hashlib

AUTHORIZATION_META_KEY = "authorization"
CLAIMS_META_KEY = "claims"

# RFC 8693 token exchange.
GRANT_TYPE_TOKEN_EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange"
TOKEN_TYPE_ACCESS_TOKEN = "urn:ietf:params:oauth:token-type:access_token"
# OAuth 2.1 + PKCE (RFC 7636): S256 is the only challenge method a compliant
# client uses; the draft §9.2 SHOULD is PKCE, so the plain method is not offered.
CODE_CHALLENGE_METHOD = "S256"


def forward_meta(meta: dict, backend_token: str | None) -> dict:
    """The ``_meta`` to forward to a backend: the client's credential dropped,
    the backend's own injected if the proxy holds one (SEP §13.1)."""
    out = dict(meta)
    out.pop(AUTHORIZATION_META_KEY, None)  # never forward the client's credential
    if backend_token is not None:
        out[AUTHORIZATION_META_KEY] = f"Bearer {backend_token}"
    return out


def claims_valid(claims: dict, issuer: str | None, audience: str | None) -> bool:
    """Whether a token's claims satisfy the configured issuer and audience.

    A configured issuer must equal the ``iss`` claim; a configured audience must
    appear in the ``aud`` claim (a string or a list). Unconfigured checks pass.
    """
    if issuer is not None and claims.get("iss") != issuer:
        return False
    if audience is not None:
        aud = claims.get("aud")
        allowed = aud if isinstance(aud, list) else [aud]
        if audience not in allowed:
            return False
    return True


def token_exchange_request(
    subject_token: str,
    audience: str | None = None,
    scope: str | None = None,
    subject_token_type: str = TOKEN_TYPE_ACCESS_TOKEN,
    requested_token_type: str | None = None,
) -> dict:
    """The RFC 8693 §2.1 token-exchange request parameters: swap the client's
    ``subject_token`` for a token the named backend ``audience`` accepts. The proxy
    posts these (form-encoded) to the authorization server's token endpoint."""
    params = {
        "grant_type": GRANT_TYPE_TOKEN_EXCHANGE,
        "subject_token": subject_token,
        "subject_token_type": subject_token_type,
    }
    if audience is not None:
        params["audience"] = audience
    if scope is not None:
        params["scope"] = scope
    if requested_token_type is not None:
        params["requested_token_type"] = requested_token_type
    return params


def parse_token_exchange_response(body: dict) -> str | None:
    """The issued token from an RFC 8693 §2.2 response (``access_token``), or
    ``None`` when the response carries no string access token."""
    if not isinstance(body, dict):
        return None
    token = body.get("access_token")
    return token if isinstance(token, str) else None


def code_challenge(verifier: str) -> str:
    """The PKCE S256 code challenge for a caller-supplied verifier (RFC 7636
    §4.2): ``base64url(sha256(verifier))`` with the padding stripped."""
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def authorization_request(
    client_id: str,
    redirect_uri: str,
    challenge: str,
    scope: str | None = None,
    state: str | None = None,
) -> dict:
    """The OAuth 2.1 authorization-request parameters carrying the PKCE challenge
    (RFC 7636 §4.3), for the proxy to redirect to the authorization endpoint."""
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code_challenge": challenge,
        "code_challenge_method": CODE_CHALLENGE_METHOD,
    }
    if scope is not None:
        params["scope"] = scope
    if state is not None:
        params["state"] = state
    return params


def token_request(client_id: str, code: str, redirect_uri: str, code_verifier: str) -> dict:
    """The OAuth 2.1 authorization-code token-request parameters, proving the PKCE
    verifier (RFC 7636 §4.5) to redeem the code for a token."""
    return {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "code": code,
        "redirect_uri": redirect_uri,
        "code_verifier": code_verifier,
    }
