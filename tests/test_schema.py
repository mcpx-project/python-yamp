"""σ1 schema-validation unit tests (Python arm). Mirrors the Rust arm.

Covers the pure verdict and the two server-role wrappers; the router wiring is
exercised end-to-end in test_router_schema.py.
"""

from yamp import errors, schema

_S = {"type": "object", "properties": {"n": {"type": "integer"}}, "required": ["n"]}


def test_is_valid_verdicts():
    assert schema.is_valid(_S, {"n": 1}) is True
    assert schema.is_valid(_S, {}) is False  # missing required
    assert schema.is_valid(_S, {"n": "x"}) is False  # wrong type
    assert schema.is_valid({"type": "object"}, {"anything": 1}) is True


def test_is_valid_bad_schema_fails_closed():
    # A schema the validator cannot understand rejects everything (fail-closed):
    # a server that cannot read its own contract must not accept traffic.
    assert schema.is_valid({"type": "not-a-type"}, {}) is False


def test_validate_call_args():
    assert schema.validate_call_args(None, {"anything": 1}) is None  # no contract
    assert schema.validate_call_args(_S, {"n": 2}) is None  # conforms
    err = schema.validate_call_args(_S, {})
    assert err == errors.error_object(errors.INVALID_PARAMS, "input schema validation failed")


def test_validate_call_result():
    out = {"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"]}
    assert schema.validate_call_result(None, {"structuredContent": {}}) is None  # no contract
    assert schema.validate_call_result(out, {"structuredContent": {"ok": True}}) is None  # conforms
    # Missing structuredContent entirely also fails: the server promised typed output.
    err = schema.validate_call_result(out, {"content": []})
    assert err == errors.error_object(errors.INTERNAL_ERROR, "output schema validation failed")
