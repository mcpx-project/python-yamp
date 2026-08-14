"""JSON Schema validation for server-originated tools (σ1).

A server that originates responses (yamp's server role, not the proxy role) is
accountable for the shape of what it accepts and returns. σ1 validates a local
handler's ``tools/call`` ``arguments`` against the tool's ``inputSchema`` before
the handler runs, and the handler's result against ``outputSchema`` before it
leaves. A bad input is the caller's fault (``INVALID_PARAMS``, a client-class
error); a bad output is the server's own fault (``INTERNAL_ERROR``, a
server-class error). Both are built through the normalized :mod:`errors`
registry, so each carries its stable ``errorId``.

stdlib has no JSON Schema validator and one cannot be hand-rolled to byte-parity
with the Rust arm, so this is the single dependency exception: both arms take a validator
dependency (``jsonschema`` here, the ``jsonschema`` crate there). The
differential corpus therefore pins the accept/reject *verdict* only, not the
library's internal error text; the wire error yamp emits is arm-independent (the
reason phrase plus a fixed ``detail``), so it stays byte-identical.

The proxy role never validates a routed backend's calls: a transparent proxy
must not assume a schema it did not author. Validation is a server-role act,
wired only into the local-handler dispatch branch and off by default.
"""

from __future__ import annotations

import jsonschema

from . import errors


def is_valid(schema: dict, value) -> bool:
    """Validate ``value`` against ``schema``, returning ``True`` iff it conforms.

    An unparseable schema fails closed (``False``): a server that cannot
    understand its own contract must not silently accept traffic against it. This
    is the pure, corpus-pinned verdict.
    """
    try:
        validator_cls = jsonschema.validators.validator_for(schema)
        validator_cls.check_schema(schema)
        return validator_cls(schema).is_valid(value)
    except jsonschema.exceptions.SchemaError:
        return False


def validate_call_args(input_schema: dict | None, arguments) -> dict | None:
    """Validate a ``tools/call``'s ``arguments`` against the tool's
    ``inputSchema``. Returns the normalized ``INVALID_PARAMS`` error object when
    it fails, or ``None`` to proceed. An absent schema imposes no contract, so
    any arguments pass.
    """
    if input_schema is not None and not is_valid(input_schema, arguments):
        return errors.error_object(errors.INVALID_PARAMS, "input schema validation failed")
    return None


def validate_call_result(output_schema: dict | None, result: dict) -> dict | None:
    """Validate a handler's result against the tool's ``outputSchema``. Per MCP a
    tool that declares an ``outputSchema`` returns its typed result in
    ``result.structuredContent``; that value must conform. Returns the
    normalized ``INTERNAL_ERROR`` error object when it does not (the server
    produced output it promised not to), or ``None`` to proceed. An absent schema
    imposes no contract.
    """
    if output_schema is not None:
        structured = result.get("structuredContent")
        if not is_valid(output_schema, structured):
            return errors.error_object(errors.INTERNAL_ERROR, "output schema validation failed")
    return None
