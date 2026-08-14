"""Generate the cross-arm differential golden corpus.

Emits ``conformance/differential-corpus.json``: a list of `(op, in, out)` cases
whose `out` is computed by the Python arm's real functions. Both test suites
replay the same file and assert their arm reproduces `out` byte-for-byte, so the
Rust and Python implementations are pinned to identical wire output for these
pure, deterministic operations.

Run from the repo root:  python python/tools/gen_differential_corpus.py
"""

import base64 as _b64
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from yamp import auth, callout, capability, config, content, doctor, errors, filters, icap, namespace, pool, schema, security, server, signing, status, subscriptions, tap, tasks, variants, version


def _pair(resolved):
    return list(resolved) if resolved is not None else None


def _vcaps(ids):
    return {"extensions": {variants.EXTENSION_ID: {"availableVariants": [{"id": i} for i in ids]}}}


def build():
    cases = []

    def add(op, value, out):
        cases.append({"op": op, "in": value, "out": out})

    # namespace: prefix / split for names and URIs (SEP §3).
    for backend, name in [("gh", "create_issue"), ("a_b", "tool__with__delims"), ("x", "y")]:
        add("namespace_prefix", {"id": backend, "name": name}, namespace.prefix(backend, name))
    for name in ["gh__create_issue", "a__b__c", "nodelimiter", "__leading", "trailing__"]:
        add("namespace_split", name, _pair(namespace.split(name)))
    for backend, uri in [("docs", "file:///reports/q3.md"), ("docs", "https://ex.com/p"), ("docs", "scheme://auth")]:
        add("namespace_prefix_uri", {"id": backend, "uri": uri}, namespace.prefix_uri(backend, uri))
    for uri in ["file:///docs/reports/q3.md", "scheme://auth/docs", "mailto:x", "scheme://auth"]:
        add("namespace_split_uri", uri, _pair(namespace.split_uri(uri)))

    # signing: RFC-8785-style canonical bytes, detached signature, chain link.
    records = [
        {"b": 2, "a": 1, "nested": {"z": 26, "y": 25}},
        signing.attestation_record("alice", "tools/call", "gh__create_issue"),
        signing.outcome_record("tools/call", "gh__create_issue", True),
    ]
    for record in records:
        add("signing_canonical", record, signing.canonical(record).decode("utf-8"))
        add("signing_sign", {"secret": "s3cr3t", "record": record}, signing.sign("s3cr3t", record))
        add("signing_chain", {"prev": signing.GENESIS, "record": record}, signing.chain(signing.GENESIS, record))

    # capability composition (SEP §2.3): sampling/logging any-backend, elicitation
    # client, extensions unioned.
    compose_inputs = [
        {"backends": [{"tools": {}, "sampling": {}}, {"logging": {}}], "client": {"elicitation": {}}},
        {"backends": [{"tools": {}, "extensions": {"a": 1}}, {"extensions": {"b": 2}}], "client": {}},
        {"backends": [{"tools": {}}], "client": None},
    ]
    for value in compose_inputs:
        out = capability.compose_capabilities(value["backends"], value["client"])
        add("capability_compose", value, out)

    # server variants (SEP-2053): variant composition (intersection), the opaque
    # composite cursor encoding, per-request selection, and enumeration.
    for backends in [
        [_vcaps(["a", "b", "c"]), _vcaps(["b", "c", "d"]), {}],
        [_vcaps(["a"]), _vcaps(["b"])],
        [{}, {}],
    ]:
        add("variants_compose", {"backends": backends}, variants.compose_variants(backends))
    for variant, cursors in [
        ("a", {"b0": "backend-p2"}),
        (None, {"b0": "p1", "b1": "p9"}),
        ("compact", {}),
        # Non-ASCII variant and cursors: both arms must emit raw-UTF-8 canonical
        # JSON (Python ensure_ascii=False, Rust serde_json), so the hex matches.
        ("café", {"b0": "naïve", "b1": "straße"}),
    ]:
        add("variants_bind_cursor", {"variant": variant, "cursors": cursors}, variants.bind_cursor(variant, cursors))
    for params in [{"_meta": {variants.SERVER_VARIANT_META_KEY: "compact"}}, {"_meta": {}}, {}]:
        add("variants_selected", params, variants.selected_variant(params))
    for caps in [_vcaps(["a", "b"]), {}]:
        add("variants_available", caps, variants.available_variants(caps))

    # token propagation (draft §9): the PKCE S256 challenge derivation and the RFC
    # 8693 token-exchange request, both deterministic and byte-identical.
    for verifier in ["dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk", "abc123", "x"]:
        add("auth_code_challenge", verifier, auth.code_challenge(verifier))
    for value in [{"subject_token": "T1", "audience": "https://gh", "scope": "repo"}, {"subject_token": "T2"}]:
        add(
            "auth_token_exchange_request",
            value,
            auth.token_exchange_request(value["subject_token"], value.get("audience"), value.get("scope")),
        )

    # extension filter chain (ε0; §6.3/§6.4): failure resolution, the deny
    # response mapping, and the verdict-chain outcome transforms. Pure and
    # deterministic, so both hosts must reduce a verdict list identically.
    for policy in ["fail_closed", "fail_open"]:
        add("filter_resolve_failure", policy, filters.resolve_failure(policy))
    for value in [{"id": 7, "reason": "blocked by dlp"}, {"id": "c1", "reason": ""}]:
        add("filter_deny_response", value, filters.deny_response(value["id"], value["reason"]))
    req = {
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {"name": "gh__create_issue", "arguments": {"title": "x"}, "_meta": {"trace": "t1"}},
    }
    chain_cases = [
        {"verdicts": [{"kind": "allow"}], "request": req},
        {"verdicts": [{"kind": "annotate", "provenance": {"scanner": "clean"}}], "request": req},
        {"verdicts": [{"kind": "mutate", "arguments": {"title": "[redacted]"}}], "request": req},
        {"verdicts": [{"kind": "deny", "reason": "infected"}], "request": req},
        {"verdicts": [{"kind": "quarantine", "reason": "dlp hit"}], "request": req},
        {
            "verdicts": [
                {"kind": "annotate", "provenance": {"scanner": "av"}},
                {"kind": "mutate", "arguments": {"title": "clean"}},
                {"kind": "allow"},
            ],
            "request": req,
        },
        {
            "verdicts": [
                {"kind": "mutate", "arguments": {"title": "y"}},
                {"kind": "deny", "reason": "second filter"},
            ],
            "request": req,
        },
    ]
    for value in chain_cases:
        add("filter_chain_outcome", value, filters.chain_outcome(value["verdicts"], value["request"]))

    # base64 primitive (ε1): standard alphabet with padding, both directions. In
    # and out are hex/base64 strings so the vector is byte-exact.
    for raw in [b"hello world", b"\x00\x01\x02\x03", b"", b"\x89PNG\r\n", b"f", b"fo"]:
        add("base64_encode", raw.hex(), _b64.b64encode(raw).decode("ascii"))
    for text in ["aGVsbG8gd29ybGQ=", "AAECAw==", "Zg=="]:
        add("base64_decode", text, _b64.b64decode(text, validate=True).hex())

    # content-block iterator (ε1; §6.3): the traversal over a tool result and a
    # resources/read result, then the text and binary write-back mutations.
    img_b64 = _b64.b64encode(b"\x89PNGdata").decode("ascii")
    blob_b64 = _b64.b64encode(b"\x00\x01\x02\x03").decode("ascii")
    tool_result = {
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "content": [
                {"type": "text", "text": "hello world"},
                {"type": "image", "data": img_b64, "mimeType": "image/png"},
                {"type": "resource_link", "uri": "file:///a.txt", "mimeType": "text/plain"},
                {"type": "resource", "resource": {"uri": "file:///b.bin", "mimeType": "application/octet-stream", "blob": blob_b64}},
                {"type": "widget", "foo": 1},
            ]
        },
    }
    read_result = {
        "jsonrpc": "2.0",
        "id": 2,
        "result": {
            "contents": [
                {"uri": "file:///c.txt", "mimeType": "text/plain", "text": "doc body"},
                {"uri": "file:///d.bin", "mimeType": "application/octet-stream", "blob": blob_b64},
            ]
        },
    }
    no_content = {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "t"}}
    for value in [tool_result, read_result, no_content]:
        add("content_blocks", value, content.blocks(value))
    add(
        "content_set_text",
        {"message": tool_result, "path": ["result", "content", 0, "text"], "text": "[redacted]"},
        content.set_text(tool_result, ["result", "content", 0, "text"], "[redacted]"),
    )
    add(
        "content_set_bytes",
        {"message": tool_result, "path": ["result", "content", 1, "data"], "bytes": b"NEWIMG".hex()},
        content.set_bytes(tool_result, ["result", "content", 1, "data"], b"NEWIMG"),
    )

    # interest declaration (ε2; §6.4): uninterested traffic pays zero cost.
    call_ctx = {"method": "tools/call", "tool": "gh__create_issue", "direction": "c2u", "content_types": ["image/png"]}
    interest_cases = [
        {"interest": {}, "context": call_ctx},
        {"interest": {"methods": ["tools/call"]}, "context": call_ctx},
        {"interest": {"methods": ["resources/read"]}, "context": call_ctx},
        {"interest": {"methods": ["*"]}, "context": call_ctx},
        {"interest": {"tools": ["gh__create_issue"]}, "context": call_ctx},
        {"interest": {"tools": ["fs__read"]}, "context": call_ctx},
        {"interest": {"directions": ["c2u"]}, "context": call_ctx},
        {"interest": {"content_types": ["image/*"]}, "context": call_ctx},
        {"interest": {"content_types": ["image/*"]}, "context": {"method": "tools/call", "tool": None, "direction": "c2u", "content_types": ["text/plain"]}},
        {"interest": {"content_types": ["application/pdf"]}, "context": call_ctx},
        {"interest": {"methods": ["tools/call"], "content_types": ["image/png"]}, "context": call_ctx},
    ]
    for value in interest_cases:
        add("interest_match", value, filters.interested(value["interest"], value["context"]))

    for value in [{"message": tool_result, "direction": "u2c"}, {"message": no_content, "direction": "c2u"}]:
        add("message_context", value, filters.message_context(value["message"], value["direction"]))

    # preview phase (ε2; §6.4), modeled on ICAP Preview.
    for value in [
        {"data": b"the quick brown fox".hex(), "n": 4},
        {"data": b"short".hex(), "n": 5},
        {"data": b"short".hex(), "n": 99},
        {"data": b"".hex(), "n": 3},
    ]:
        add("preview_slice", value, filters.preview(bytes.fromhex(value["data"]), value["n"]))
    for value in [
        {"decision": "deny", "ieof": False},
        {"decision": "allow", "ieof": True},
        {"decision": "continue", "ieof": True},
        {"decision": "continue", "ieof": False},
    ]:
        add("preview_resolve", value, filters.preview_resolve(value["decision"], value["ieof"]))

    # ext-proc callout (ε3; §6.4): the request envelope, the content digest, the
    # byte-budget check, and the verdict parser (including failure-policy on a
    # malformed response). All pure, so both hosts must agree byte-for-byte.
    ctx = {"method": "tools/call", "tool": "gh__x", "direction": "u2c", "content_types": ["image/png"]}
    for value in [
        {"context": ctx, "phase": "preview", "content": b"scan me".hex(), "ieof": False},
        {"context": ctx, "phase": "body", "content": b"".hex(), "ieof": True},
    ]:
        add("callout_request", value, callout.callout_request(ctx, value["phase"], bytes.fromhex(value["content"]), value["ieof"]))
    for raw in [b"scan me", b"", b"\x00\x01\x02"]:
        add("callout_digest", raw.hex(), callout.content_digest(raw))
    for value in [{"size": 10, "max_bytes": 20}, {"size": 30, "max_bytes": 20}, {"size": 30, "max_bytes": 0}]:
        add("callout_budget", value, callout.exceeds_budget(value["size"], value["max_bytes"]))
    mutate_b64 = _b64.b64encode(b"cleaned").decode("ascii")
    parse_cases = [
        {"response": {"verdict": "allow"}, "failure_policy": "fail_closed"},
        {"response": {"verdict": "deny", "reason": "infected"}, "failure_policy": "fail_closed"},
        {"response": {"verdict": "quarantine", "reason": "dlp"}, "failure_policy": "fail_closed"},
        {"response": {"verdict": "mutate", "content": mutate_b64}, "failure_policy": "fail_closed"},
        {"response": {"verdict": "mutate"}, "failure_policy": "fail_closed"},
        {"response": {"verdict": "mutate", "content": "!!!not-base64"}, "failure_policy": "fail_closed"},
        {"response": {"verdict": "mutate", "content": mutate_b64, "provenance": {"icap": "modified"}}, "failure_policy": "fail_closed"},
        {"response": {"verdict": "annotate", "provenance": {"scanner": "av"}}, "failure_policy": "fail_closed"},
        {"response": {"verdict": "continue"}, "failure_policy": "fail_closed"},
        {"response": {"nonsense": 1}, "failure_policy": "fail_closed"},
        {"response": {"nonsense": 1}, "failure_policy": "fail_open"},
        {"response": {"verdict": "bogus"}, "failure_policy": "fail_closed"},
    ]
    for value in parse_cases:
        add("callout_parse", value, callout.parse_verdict(value["response"], value["failure_policy"]))

    # reference ICAP bridge (ε4; §6.5): mode, the ICAP-to-callout translation
    # (204 pass / 200 modify+annotate / threat quarantine / 403 deny), and the
    # resource_link SSRF opt-in. All pure, so both hosts must agree.
    for direction in ["c2u", "u2c"]:
        add("icap_mode", direction, icap.icap_mode(direction))
    icap_cases = [
        {"status": 204},
        {"status": 200, "modified": _b64.b64encode(b"cleaned").decode("ascii"), "istag": "av-1"},
        {"status": 200},
        {"status": 403},
        {"status": 200, "threat": "eicar"},
        {"status": 500},
    ]
    for value in icap_cases:
        add("icap_to_callout", value, icap.icap_to_callout(value))
    for value in [{"kind": "resource_link", "enabled": True}, {"kind": "resource_link", "enabled": False}, {"kind": "image", "enabled": True}]:
        add("icap_should_deref", value, icap.should_deref(value["kind"], value["enabled"]))

    # server-origination (σ0; §5): the SEP-2549 cache directives a server attaches
    # to its list results, emitted with an integer ttlMs so both arms agree.
    for value in [{"ttl_ms": 300000, "cache_scope": "public"}, {"ttl_ms": 0, "cache_scope": "private"}]:
        add("server_list_directives", value, server.list_directives(value["ttl_ms"], value["cache_scope"]))
    add(
        "server_attach_directives",
        {"result": {"tools": [{"name": "gh__x"}]}, "ttl_ms": 60000, "cache_scope": "public"},
        server.attach_directives({"tools": [{"name": "gh__x"}]}, 60000, "public"),
    )

    # schema validation (σ1; §5): the accept/reject verdict for a value against a
    # schema. Both arms take a JSON Schema library (the single dependency exception), so the
    # corpus pins the boolean verdict only, on keywords whose meaning is identical
    # across drafts (type/required/enum/minimum/items) — not any library's error
    # text, which the wire error never carries.
    _obj = {"type": "object", "properties": {"n": {"type": "integer"}}, "required": ["n"]}
    schema_cases = [
        (_obj, {"n": 1}),  # conforms
        (_obj, {}),  # missing required
        (_obj, {"n": "x"}),  # wrong type
        ({"type": "object"}, {"anything": 1}),  # permissive: accepts any object
        ({"type": "string", "enum": ["a", "b"]}, "a"),  # enum member
        ({"type": "string", "enum": ["a", "b"]}, "c"),  # enum violation
        ({"type": "integer", "minimum": 10}, 12),  # bound satisfied
        ({"type": "integer", "minimum": 10}, 4),  # bound violated
        ({"type": "array", "items": {"type": "integer"}}, [1, 2, 3]),  # items conform
        ({"type": "array", "items": {"type": "integer"}}, [1, "two"]),  # item violates
        ({"type": "not-a-type"}, {}),  # unparseable schema fails closed
    ]
    for sch, val in schema_cases:
        add("schema_validate", {"schema": sch, "value": val}, schema.is_valid(sch, val))

    # normalized error registry (§12, Track U E-code registry): the stable reason
    # phrase per code, and the normalized error object every emitter builds.
    for code in [errors.INVALID_PARAMS, errors.INTERNAL_ERROR, errors.UNAUTHORIZED, errors.NO_SESSION, -31234]:
        add("error_reason", code, errors.reason(code))
    error_object_cases = [
        {"code": errors.INVALID_PARAMS, "detail": "input schema validation failed"},
        {"code": errors.INTERNAL_ERROR, "detail": "output schema validation failed"},
        {"code": errors.UNAUTHORIZED},
        {"code": -31234},
    ]
    for value in error_object_cases:
        add("error_object", value, errors.error_object(value["code"], value.get("detail")))
    # The full human description per registered code (id, reason, cause, hint, docs
    # anchor) that the generated error index and any explain surface render. Sweeping
    # the whole registry pins the cause/hint strings byte-identical across arms.
    for code, *_ in errors.REGISTRY:
        add("error_describe", code, errors.describe(code))

    # worker-pool substrate (σ2; §5): the deterministic pieces of the pool — the
    # admission decision, the idle-deadline arithmetic, and the two message
    # extractors. The concurrent execution itself is timing and not pinned.
    for in_flight, cap in [(0, 0), (3, 0), (0, 1), (1, 1), (2, 4), (4, 4), (5, 4)]:
        add("pool_admit", {"in_flight": in_flight, "cap": cap}, pool.admit(in_flight, cap))
    for now_ms, idle_ms in [(1000, 0), (1000, 5000), (0, 1)]:
        add("pool_deadline", {"now_ms": now_ms, "idle_ms": idle_ms}, pool.deadline(now_ms, idle_ms))
    for deadline_ms, now_ms in [(0, 9999), (6000, 5999), (6000, 6000), (6000, 6001)]:
        add("pool_expired", {"deadline_ms": deadline_ms, "now_ms": now_ms}, pool.expired(deadline_ms, now_ms))
    cancel_cases = [
        {"method": "notifications/cancelled", "params": {"requestId": "c-7"}},
        {"method": "notifications/cancelled", "params": {"requestId": 42}},
        {"method": "notifications/cancelled", "params": {}},
        {"method": "notifications/progress", "params": {"requestId": "c-7"}},
    ]
    for value in cancel_cases:
        add("pool_cancel_id", value, pool.cancel_request_id(value))
    progress_cases = [
        {"method": "notifications/progress", "params": {"progressToken": "t-3", "progress": 5}},
        {"method": "notifications/progress", "params": {"progress": 5}},
        {"method": "notifications/cancelled", "params": {"progressToken": "t-3"}},
    ]
    for value in progress_cases:
        add("pool_progress_token", value, pool.progress_token(value))

    # server-side task origination (σ3; §5): the request augmentation check, the
    # server-generated id, and the task handle shape. The store and background
    # execution are stateful/timing and not pinned.
    augment_cases = [
        {"_meta": {tasks.TASK_META_KEY: {}}},
        {"_meta": {tasks.TASK_META_KEY: {"ttl": 30}}},
        {"_meta": {"other": 1}},
        {"_meta": {}},
        {},
    ]
    for value in augment_cases:
        add("task_augmented", value, tasks.is_task_augmented(value))
    for seq in [1, 2, 42]:
        add("task_new_id", seq, tasks.new_task_id(seq))
    handle_cases = [
        {"task_id": "task-1", "status": tasks.STATUS_WORKING},
        {"task_id": "task-2", "status": tasks.STATUS_COMPLETED, "result": {"content": [{"type": "text", "text": "ok"}]}},
        {"task_id": "task-3", "status": tasks.STATUS_FAILED, "error": errors.error_object(errors.INTERNAL_ERROR, "task execution failed")},
        {"task_id": "task-4", "status": tasks.STATUS_CANCELLED},
    ]
    for value in handle_cases:
        add("task_handle", value, tasks.task_handle(value["task_id"], value["status"], value.get("result"), value.get("error")))

    # subscriptions (σ4): the server-originated updated notification and the
    # backend->client uri re-namespacing.
    for uri in ["file:///reports/q3.md", "https://ex.com/p", "scheme://auth"]:
        add("subscription_updated", uri, subscriptions.updated_notification(uri))
    namespace_updated_cases = [
        {"message": {"jsonrpc": "2.0", "method": subscriptions.UPDATED_METHOD, "params": {"uri": "file:///reports/q3.md"}}, "backend_id": "docs"},
        {"message": {"jsonrpc": "2.0", "method": subscriptions.UPDATED_METHOD, "params": {"uri": "scheme://auth"}}, "backend_id": "docs"},
        {"message": {"jsonrpc": "2.0", "method": subscriptions.UPDATED_METHOD, "params": {"other": 1}}, "backend_id": "docs"},
    ]
    for value in namespace_updated_cases:
        add("subscription_namespace_updated", value, subscriptions.namespace_updated(value["message"], value["backend_id"]))

    # server output cap (σ5): the encoded-size verdict. ASCII-only results, so the
    # compact encoding is byte-identical across arms; a max_bytes of 0 is unbounded.
    output_cap_cases = [
        {"result": {"content": [{"type": "text", "text": "ok"}]}, "max_bytes": 1000},  # under
        {"result": {"content": [{"type": "text", "text": "ok"}]}, "max_bytes": 10},  # over
        {"result": {"content": [{"type": "text", "text": "x" * 64}]}, "max_bytes": 64},  # over by the text alone
        {"result": {"a": 1}, "max_bytes": 0},  # unbounded
    ]
    for value in output_cap_cases:
        add("server_output_cap", value, server.exceeds_output_cap(value["result"], value["max_bytes"]))

    # doctor server preflight (σ6): the ordered findings for a server surface plus
    # advertised protocol version. ASCII tool names, so the repr-formatted messages
    # are byte-identical across arms.
    good_tool = {"name": "srv__do", "inputSchema": {"type": "object"}}
    supported = version.STATEFUL_PROTOCOL_VERSION
    doctor_cases = [
        {"tools": [good_tool], "protocol_version": supported},  # clean
        {"tools": [], "protocol_version": supported},  # no tools
        {"tools": [good_tool], "protocol_version": "2020-01-01"},  # unsupported version
        {"tools": [good_tool, good_tool], "protocol_version": supported},  # duplicate name
        {"tools": [{"name": "srv__x"}], "protocol_version": supported},  # missing inputSchema
        {"tools": [{"inputSchema": {"type": "object"}}], "protocol_version": supported},  # unnamed tool
    ]
    for value in doctor_cases:
        add("doctor_check", value, doctor.check_server(value["tools"], value["protocol_version"]))

    # doctor CLI rendering (Track U): the human text report and the ok verdict the
    # `yamp-doctor` entrypoint prints, swept across the three strictness modes so the
    # verdict line and exit verdict are pinned per mode. ASCII messages, so the
    # rendered lines are byte-identical across arms.
    render_findings = [
        [],  # clean surface
        [doctor.finding(doctor.LEVEL_WARNING, "no-tools", "server exposes no tools")],  # warning only
        [
            doctor.finding(doctor.LEVEL_ERROR, "unnamed-tool", "a tool has no name"),
            doctor.finding(doctor.LEVEL_WARNING, "missing-input-schema", "tool 'srv__x' has no object inputSchema"),
        ],  # error present
    ]
    for findings in render_findings:
        for mode in doctor.MODES:
            add(
                "doctor_render",
                {"findings": findings, "mode": mode},
                {"text": doctor.render_text(findings, mode), "ok": doctor.servable(findings, mode)},
            )

    # status snapshot (Track U): the read-only status object the GET /status endpoint
    # serves, composed from the proxy identity plus operational counts. Pure in its
    # inputs, so both arms produce the identical object.
    status_cases = [
        {"backend_ids": [], "sessions": 0},
        {"backend_ids": ["b0"], "sessions": 1},
        {"backend_ids": ["b0", "b1"], "sessions": 7},
    ]
    for value in status_cases:
        add("status_snapshot", value, status.snapshot(value["backend_ids"], value["sessions"]))

    # config explain / effective (Track U): one key's effective value and provenance,
    # and the whole resolved view. Pure over the raw document, so both arms agree; the
    # float default (resetTimeout) is swept to pin numeric formatting parity.
    explain_cases = [
        {"raw": {}, "key": "resilience.failureThreshold"},  # default int
        {"raw": {"resilience": {"failureThreshold": 9}}, "key": "resilience.failureThreshold"},  # from config
        {"raw": {"listen": "127.0.0.1:9100"}, "key": "listen"},  # required key, from config
        {"raw": {}, "key": "namespacing.strategy"},  # default string
        {"raw": {"resilience": {"resetTimeout": 45.0}}, "key": "resilience.resetTimeout"},  # float from config
        {"raw": {}, "key": "resilience.resetTimeout"},  # float default
        {"raw": {}, "key": "auth.clientTokens"},  # default empty list
        {"raw": {"resilience": "nope"}, "key": "resilience.failureThreshold"},  # non-dict intermediate
        {"raw": {}, "key": "bogus.key"},  # unknown key
    ]
    for value in explain_cases:
        add("config_explain", value, config.explain(value["raw"], value["key"]))
    for entry in [config.explain(v["raw"], v["key"]) for v in explain_cases]:
        add("config_explain_line", entry, config.explain_line(entry))
    effective_cases = [
        {"raw": {}},  # all defaults
        {"raw": {"listen": "a:1", "resilience": {"resetTimeout": 45.0}, "handlers": {"metaTools": True}}},
    ]
    for value in effective_cases:
        add("config_effective", value, config.effective(value["raw"]))

    # config diff (Track U): keys whose effective value differs between two documents,
    # with provenance on both sides. Pure over the two raw documents.
    diff_cases = [
        {"left": {}, "right": {}},  # identical (all defaults): no changes
        {"left": {"listen": "a:1"}, "right": {"listen": "b:2"}},  # value changed, both config
        {"left": {}, "right": {"resilience": {"failureThreshold": 9}}},  # default -> config
        {"left": {"handlers": {"metaTools": True}}, "right": {}},  # config -> default
    ]
    for value in diff_cases:
        add("config_diff", value, config.diff(value["left"], value["right"]))
    for entry in [e for v in diff_cases for e in config.diff(v["left"], v["right"])]:
        add("config_diff_line", entry, config.diff_line(entry))

    # secure zero-config defaults (Track U, U7): loopback classification and the
    # bind-safety gate. The refusal message is single-sourced, so pinning it here
    # confirms both arms produce it byte-identically.
    for host in ["127.0.0.1", "localhost", "::1", "[::1]", "127.5.5.5", "0.0.0.0", "::", "10.0.0.4", "example.com", ""]:
        add("security_is_loopback", host, security.is_loopback(host))
    bind_cases = [
        {"host": "127.0.0.1", "has_client_auth": False},  # loopback: safe
        {"host": "0.0.0.0", "has_client_auth": False},  # public, no auth: refused
        {"host": "0.0.0.0", "has_client_auth": True},  # public, authed: safe
        {"host": "10.0.0.4", "has_client_auth": False},  # public, no auth: refused
        {"host": "", "has_client_auth": False},  # empty host (wildcard), no auth: refused
    ]
    for value in bind_cases:
        add("security_check_bind", value, security.check_bind(value["host"], value["has_client_auth"]))

    # config adapt (Track U, U9): normalize human shorthands to canonical JSON. The
    # canonical outputs are already-canonical, so re-adapting is the identity (pinned
    # by including a canonical case).
    adapt_cases = [
        {"listen": 9100, "backends": ["b0=127.0.0.1:9101", "b1=127.0.0.1:9102,127.0.0.1:9202"]},
        {"listen": ":9100", "backends": {"b0": "127.0.0.1:9101"}},
        {"listen": "127.0.0.1:9100", "backends": {"b0": {"addresses": ["127.0.0.1:9101"]}}},  # already canonical
        {"backends": {"b0": {"address": "127.0.0.1:9101"}}, "auth": {"clientTokens": ["t"]}},  # passthrough keys
        {"listen": True},  # a bool is not a port: passed through unchanged
        {"backends": ["malformed", "b0=127.0.0.1:9101"]},  # a list item without '=' is skipped
        {"backends": "weird"},  # a non-list, non-map backends value passes through
    ]
    for value in adapt_cases:
        add("config_adapt", value, config.adapt(value))

    # tap redaction (Track U): deep-mask every credential/identity value in a message,
    # and the redacted capture record. Single-sourced, so both arms redact identically.
    redact_cases = [
        {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
         "params": {"name": "b__do", "arguments": {"apiKey": "sk-live-123", "note": "keep"},
                    "_meta": {"authorization": "Bearer abc", "claims": {"iss": "x"}}}},
        {"jsonrpc": "2.0", "id": 2, "result": {"content": [{"type": "text", "text": "ok"}], "token": "t"}},
        {"nested": [{"secret": "s"}, {"Password": "p"}, {"fine": 1}]},
        "not-an-object",
    ]
    for value in redact_cases:
        add("tap_redact", value, tap.redact(value))
    capture_cases = [
        {"direction": "c2s", "message": redact_cases[0]},
        {"direction": "s2c", "message": redact_cases[1]},
        {"direction": "c2s", "message": "not-an-object"},
    ]
    for value in capture_cases:
        add("tap_capture", value, tap.capture(value["direction"], value["message"]))

    # config diagnostics (Track U, U4/U8): the config-error catalog (slug, hint, docs
    # anchor), byte-identical across arms, and the diagnosed slug for each of the
    # top-10 field-failure causes (U8 verified against corpus).
    add("config_error_catalog", {}, config.error_catalog())
    diagnose_cases = [
        "not-a-json-object",  # not-object
        {"backends": "x", "listen": "a:1"},  # backends-not-object
        {"backends": {"b__x": {"address": "h:1"}}, "listen": "a:1"},  # invalid-backend-id
        {"backends": {"b0": {}}, "listen": "a:1"},  # backend-no-addresses (empty spec)
        {"backends": {"b0": "not-an-object"}, "listen": "a:1"},  # backend-no-addresses (non-object spec)
        {"backends": {"b0": {"address": "h:1"}}},  # missing-listen
        {"listen": "a:1", "namespacing": {"strategy": "nope"}},  # unknown-collision-strategy
        {"listen": "a:1", "handlers": {"rest": [{"id": "h__x", "baseUrl": "http://x"}]}},  # invalid-handler-id
        {"listen": "a:1", "backends": {"b0": {"address": "h:1"}},
         "handlers": {"rest": [{"id": "b0", "baseUrl": "http://x"}]}},  # handler-backend-collision
        {"listen": "a:1", "handlers": {"rest": [{"id": "h0"}]}},  # handler-missing-baseurl
        {"listen": "a:1", "backends": {"b0": {"address": "h:1"}}},  # valid -> null
    ]
    for value in diagnose_cases:
        add("config_diagnose_slug", value, (config.diagnose(value) or {}).get("slug"))

    return {"cases": cases}


def main():
    corpus = build()
    out_path = ROOT / "conformance" / "differential-corpus.json"
    out_path.parent.mkdir(exist_ok=True)
    out_path.write_text(json.dumps(corpus, indent=2, sort_keys=True) + "\n")
    print(f"wrote {len(corpus['cases'])} cases to {out_path}")


if __name__ == "__main__":
    main()
