"""δ13 canonical error-code registry tests (Python arm). Mirrors the Rust arm."""

import asyncio

from yamp import errors, jsonrpc
from yamp.forward import PROXY_PROTOCOL_VERSION
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


def test_canonical_assignments():
    assert errors.NO_SESSION == -32000
    assert errors.POLICY_DENIED == -32001
    assert errors.UNAUTHORIZED == -32002
    assert errors.SERVER_NOT_AVAILABLE == -32003
    assert errors.UNSUPPORTED_PROTOCOL_VERSION == -32004
    assert errors.HEADER_MISMATCH == -32005


def test_proxy_codes_distinct_and_in_server_range():
    proxy = [
        errors.NO_SESSION,
        errors.POLICY_DENIED,
        errors.UNAUTHORIZED,
        errors.SERVER_NOT_AVAILABLE,
        errors.UNSUPPORTED_PROTOCOL_VERSION,
        errors.HEADER_MISMATCH,
    ]
    assert len(set(proxy)) == len(proxy)  # no overload
    for code in proxy:
        assert -32099 <= code <= -32000  # JSON-RPC server-defined range


def test_registry_lists_standard_and_proxy_codes():
    assert errors.ALL_CODES >= {
        jsonrpc.INVALID_REQUEST,
        jsonrpc.METHOD_NOT_FOUND,
        jsonrpc.INVALID_PARAMS,
        jsonrpc.INTERNAL_ERROR,
        errors.NO_SESSION,
        errors.POLICY_DENIED,
        errors.UNAUTHORIZED,
        errors.SERVER_NOT_AVAILABLE,
        errors.UNSUPPORTED_PROTOCOL_VERSION,
    }


def test_modules_source_codes_from_registry():
    # The scattered constants now resolve to the single registry values.
    from yamp import resilience, transparent, version

    assert resilience.SERVER_NOT_AVAILABLE == errors.SERVER_NOT_AVAILABLE
    assert version.UNSUPPORTED_PROTOCOL_VERSION == errors.UNSUPPORTED_PROTOCOL_VERSION
    assert transparent.POLICY_DENIED == errors.POLICY_DENIED


def test_registry_ids_are_unique_and_http_classed():
    # Every registered code has a stable id and reason phrase, ids are unique, and
    # the leading digit encodes the HTTP-style class (4xxx client, 5xxx server).
    ids = [eid for _, eid, _, _, _ in errors.REGISTRY]
    assert len(set(ids)) == len(ids)
    for code, eid, phrase, cause, hint in errors.REGISTRY:
        assert eid.startswith("E4") or eid.startswith("E5")
        assert errors.id(code) == eid
        assert errors.reason(code) == phrase
        assert phrase and cause and hint  # every entry carries a reason, cause, and fix hint
        assert errors.cause(code) == cause
        assert errors.hint(code) == hint


def test_describe_and_docs_url():
    # The full human description a dashboard or explain surface renders, keyed on the
    # yamp identity, with an in-index anchor derived from the id.
    assert errors.docs_url(errors.METHOD_NOT_FOUND) == "ERRORS.md#e4004"
    assert errors.describe(errors.METHOD_NOT_FOUND) == {
        "code": errors.METHOD_NOT_FOUND,
        "errorId": "E4004",
        "reason": "Method Not Found",
        "cause": "The requested method or namespaced tool is exposed by no backend or handler.",
        "hint": "List the available tools and call one of the exposed names.",
        "docsUrl": "ERRORS.md#e4004",
    }
    # An unknown backend code is unnamed throughout (SEP-2678): empty fields, no anchor.
    assert errors.cause(-31234) == "" and errors.hint(-31234) == "" and errors.docs_url(-31234) == ""


def test_lookup_of_unknown_code_returns_empty():
    # A backend code the proxy does not define is unnamed (SEP-2678): id/reason
    # both empty, and error_object still builds with an empty id and no detail.
    assert errors.id(-31234) == ""
    assert errors.reason(-31234) == ""
    assert errors.error_object(-31234) == {"code": -31234, "message": "", "data": {"errorId": ""}}


def test_error_object_normalized_shape():
    with_detail = errors.error_object(errors.INVALID_PARAMS, "input schema validation failed")
    assert with_detail == {
        "code": errors.INVALID_PARAMS,
        "message": "Invalid Params",
        "data": {"errorId": "E4002", "detail": "input schema validation failed"},
    }
    # Without a detail, data carries only the stable id.
    assert errors.error_object(errors.INTERNAL_ERROR) == {
        "code": errors.INTERNAL_ERROR,
        "message": "Internal Error",
        "data": {"errorId": "E5000"},
    }


async def _error_backend(read_pipe, write_pipe, code):
    transport = LineTransport(read_pipe.reader, write_pipe)
    init = jsonrpc.decode(await transport.receive())
    await transport.send(
        jsonrpc.encode(
            jsonrpc.result(
                init["id"],
                {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": {"tools": {}}, "serverInfo": {"name": "b"}},
            )
        )
    )
    await transport.receive()  # notifications/initialized
    while True:
        raw = await transport.receive()
        if raw is None:
            await transport.send_eof()
            return
        message = jsonrpc.decode(raw)
        if message["method"] == "tools/call":
            # A backend-specific code the proxy does not define.
            await transport.send(
                jsonrpc.encode(
                    {"jsonrpc": "2.0", "id": message["id"], "error": {"code": code, "message": "backend-specific"}}
                )
            )


def test_unknown_backend_error_code_passes_through_unchanged():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        backend = Backend("b", LineTransport(b2pr.reader, pr2b))
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [backend])
        router_task = asyncio.create_task(router.serve())
        backend_task = asyncio.create_task(_error_backend(pr2b, b2pr, -31234))
        client = LineTransport(r2c.reader, c2r)
        await client.send(
            jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}}))
        )
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
        # Single backend passes names through, so the tool name is unprefixed.
        await client.send(jsonrpc.encode(jsonrpc.request("t", "tools/call", {"name": "do", "arguments": {}})))
        response = jsonrpc.decode(await client.receive())
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(router_task, backend_task), timeout=5)
        return response

    response = asyncio.run(scenario())
    # The proxy must not rewrite a code it did not negotiate (SEP-2678).
    assert response["error"]["code"] == -31234
    assert response["error"]["message"] == "backend-specific"
    assert -31234 not in errors.ALL_CODES  # genuinely unknown to the proxy
