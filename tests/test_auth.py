import asyncio

from yamp import auth, jsonrpc
from yamp.errors import UNAUTHORIZED
from yamp.forward import PROXY_PROTOCOL_VERSION
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


def test_forward_meta_injects_backend_and_drops_client():
    # A client credential is dropped; the backend's own is injected.
    out = auth.forward_meta({"authorization": "Bearer CLIENT", "traceparent": "tp"}, "GH_SECRET")
    assert out["authorization"] == "Bearer GH_SECRET"
    assert out["traceparent"] == "tp"  # unrelated meta preserved
    # No backend token: the client's credential is still dropped.
    assert "authorization" not in auth.forward_meta({"authorization": "Bearer CLIENT"}, None)


def test_claims_valid():
    assert auth.claims_valid({"iss": "idp", "aud": "yamp"}, "idp", "yamp")
    assert auth.claims_valid({"iss": "idp", "aud": ["a", "yamp"]}, None, "yamp")  # list audience
    assert auth.claims_valid({}, None, None)  # unconfigured passes
    assert not auth.claims_valid({"iss": "evil"}, "idp", None)  # wrong issuer
    assert not auth.claims_valid({"aud": "other"}, None, "yamp")  # wrong audience


def test_token_exchange_request_rfc8693():
    req = auth.token_exchange_request("CLIENT_TOK", audience="https://gh.example", scope="repo")
    assert req["grant_type"] == "urn:ietf:params:oauth:grant-type:token-exchange"
    assert req["subject_token"] == "CLIENT_TOK"
    assert req["subject_token_type"] == "urn:ietf:params:oauth:token-type:access_token"
    assert req["audience"] == "https://gh.example"
    assert req["scope"] == "repo"
    # Optional fields are omitted when not given.
    minimal = auth.token_exchange_request("T")
    assert "audience" not in minimal and "scope" not in minimal and "requested_token_type" not in minimal
    # A requested token type is carried when asked.
    typed = auth.token_exchange_request("T", requested_token_type=auth.TOKEN_TYPE_ACCESS_TOKEN)
    assert typed["requested_token_type"] == auth.TOKEN_TYPE_ACCESS_TOKEN


def test_parse_token_exchange_response():
    assert auth.parse_token_exchange_response({"access_token": "BACKEND_TOK", "token_type": "Bearer"}) == "BACKEND_TOK"
    assert auth.parse_token_exchange_response({"error": "invalid_request"}) is None
    assert auth.parse_token_exchange_response({"access_token": 123}) is None  # non-string
    assert auth.parse_token_exchange_response("nope") is None


def test_pkce_code_challenge_rfc7636_vector():
    # RFC 7636 Appendix B worked example.
    verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
    assert auth.code_challenge(verifier) == "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
    assert "=" not in auth.code_challenge("short")  # padding stripped


def test_oauth_request_builders():
    authz = auth.authorization_request("proxy", "https://cb", "CHAL", scope="mcp", state="xyz")
    assert authz["response_type"] == "code"
    assert authz["code_challenge"] == "CHAL"
    assert authz["code_challenge_method"] == "S256"
    assert authz["scope"] == "mcp" and authz["state"] == "xyz"
    bare = auth.authorization_request("proxy", "https://cb", "CHAL")
    assert "scope" not in bare and "state" not in bare
    tok = auth.token_request("proxy", "AUTH_CODE", "https://cb", "VERIFIER")
    assert tok["grant_type"] == "authorization_code"
    assert tok["code"] == "AUTH_CODE" and tok["code_verifier"] == "VERIFIER"


async def _echo_meta_backend(read_pipe, write_pipe, seen):
    t = LineTransport(read_pipe.reader, write_pipe)
    init = jsonrpc.decode(await t.receive())
    seen.append(("initialize", init.get("params", {}).get("_meta", {})))
    await t.send(jsonrpc.encode(jsonrpc.result(init["id"], {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": {"tools": {}}, "serverInfo": {"name": "gh"}})))
    await t.receive()
    while True:
        raw = await t.receive()
        if raw is None:
            await t.send_eof()
            return
        m = jsonrpc.decode(raw)
        if m["method"] == "tools/call":
            seen.append(("tools/call", m.get("params", {}).get("_meta", {})))
            await t.send(jsonrpc.encode(jsonrpc.result(m["id"], {"content": [{"type": "text", "text": "ok"}]})))


def test_backend_credential_injected_and_client_not_leaked_e2e():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        seen = []
        backend = Backend("gh", LineTransport(b2pr.reader, pr2b), token="GH_SECRET")
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [backend])
        rt = asyncio.create_task(router.serve())
        bt = asyncio.create_task(_echo_meta_backend(pr2b, b2pr, seen))
        client = LineTransport(r2c.reader, c2r)
        await client.send(jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
        # The client tries to smuggle its own credential in _meta.
        await client.send(jsonrpc.encode(jsonrpc.request("s", "tools/call", {"name": "gh__x", "arguments": {}, "_meta": {"authorization": "Bearer CLIENT_TOKEN"}})))
        await client.receive()
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(rt, bt), timeout=5)
        return seen

    seen = asyncio.run(scenario())
    call_meta = [meta for kind, meta in seen if kind == "tools/call"][0]
    assert call_meta["authorization"] == "Bearer GH_SECRET"  # backend's own injected
    # The client's credential never reached the backend, anywhere.
    assert all("CLIENT_TOKEN" not in str(meta) for _kind, meta in seen)
    # The backend handshake carried the proxy identity, no client credential.
    init_meta = [meta for kind, meta in seen if kind == "initialize"][0]
    assert "authorization" not in init_meta or init_meta["authorization"] == "Bearer GH_SECRET"


def test_handshake_rejects_invalid_issuer_audience():
    async def scenario(claims):
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        # The router rejects the handshake before it ever touches the backend, so
        # no backend responder is needed.
        backend = Backend("gh", LineTransport(b2pr.reader, pr2b))
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [backend], issuer="idp", audience="yamp")
        rt = asyncio.create_task(router.serve())
        client = LineTransport(r2c.reader, c2r)
        await client.send(jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}, "_meta": {"claims": claims}})))
        response = jsonrpc.decode(await client.receive())
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(rt, return_exceptions=True), timeout=5)
        return response

    bad = asyncio.run(scenario({"iss": "evil", "aud": "yamp"}))
    assert bad["error"]["code"] == UNAUTHORIZED
    wrong_aud = asyncio.run(scenario({"iss": "idp", "aud": "other"}))
    assert wrong_aud["error"]["code"] == UNAUTHORIZED
