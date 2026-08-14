from yamp.errors import UNAUTHORIZED
from yamp.policy import (
    AUTHORIZATION,
    BearerAuthenticator,
    PolicyLayer,
)


def test_credential_injection_does_not_leak_client_token():
    policy = PolicyLayer(backend_tokens={"github": "GH_SECRET"})
    headers = policy.backend_headers("github", {AUTHORIZATION: "Bearer CLIENT_TOKEN"})
    # Backend gets the proxy's credential; the client's token is not forwarded.
    assert headers[AUTHORIZATION] == "Bearer GH_SECRET"
    assert "CLIENT_TOKEN" not in headers[AUTHORIZATION]


def test_backend_without_credentials_gets_none():
    policy = PolicyLayer(backend_tokens={"github": "GH"})
    assert policy.backend_headers("slack", {}) == {}


def test_header_forwarding_is_scoped_and_can_rename():
    policy = PolicyLayer(
        forward_headers={
            "atlassian": [
                {"name": "X-Atlassian-Token"},
                {"name": AUTHORIZATION, "backendHeader": "X-Original-Auth"},
            ]
        }
    )
    client = {"X-Atlassian-Token": "tok", AUTHORIZATION: "Bearer C"}
    atlassian = policy.backend_headers("atlassian", client)
    assert atlassian["X-Atlassian-Token"] == "tok"
    assert atlassian["X-Original-Auth"] == "Bearer C"
    # A backend without a forward rule receives none of them.
    assert policy.backend_headers("github", client) == {}


def test_client_authentication():
    policy = PolicyLayer(client_authenticator=BearerAuthenticator({"good"}))
    assert policy.authorize_client({AUTHORIZATION: "Bearer good"})
    assert not policy.authorize_client({AUTHORIZATION: "Bearer bad"})
    assert not policy.authorize_client({})


def test_no_authenticator_allows_all():
    assert PolicyLayer().authorize_client({}) is True
    assert UNAUTHORIZED == -32002  # split off -32001 (policy denial) in δ13
