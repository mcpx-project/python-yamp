"""σ6 server-role preflight check (Python arm). Mirrors the Rust arm.

`doctor.check_server` inspects a server's exposed tool surface and advertised
protocol version and returns ordered findings (error/warning) without raising, the
`nginx -t` analog for the server role. `is_ok` is false only on an error.
"""

from yamp import doctor, version
from yamp.handler import BackendsHandler, Registry

SUPPORTED = version.STATEFUL_PROTOCOL_VERSION
GOOD = {"name": "srv__do", "inputSchema": {"type": "object"}}


def _codes(findings):
    return [f["code"] for f in findings]


def test_clean_surface_has_no_findings():
    findings = doctor.check_server([GOOD], SUPPORTED)
    assert findings == []
    assert doctor.is_ok(findings)


def test_no_tools_is_an_advisory_warning():
    findings = doctor.check_server([], SUPPORTED)
    assert _codes(findings) == ["no-tools"]
    assert findings[0]["level"] == doctor.LEVEL_WARNING
    assert doctor.is_ok(findings)  # a warning does not block serving


def test_unsupported_protocol_version_is_an_error():
    findings = doctor.check_server([GOOD], "2020-01-01")
    assert _codes(findings) == ["unsupported-protocol-version"]
    assert not doctor.is_ok(findings)
    # The message names the supported set so an operator can fix it.
    assert "2026-07-28" in findings[0]["message"]


def test_duplicate_tool_name_is_an_error():
    findings = doctor.check_server([GOOD, GOOD], SUPPORTED)
    assert _codes(findings) == ["duplicate-tool"]
    assert not doctor.is_ok(findings)


def test_missing_input_schema_is_a_warning():
    findings = doctor.check_server([{"name": "srv__x"}], SUPPORTED)
    assert _codes(findings) == ["missing-input-schema"]
    assert doctor.is_ok(findings)


def test_unnamed_tool_is_an_error():
    findings = doctor.check_server([{"inputSchema": {"type": "object"}}], SUPPORTED)
    assert _codes(findings) == ["unnamed-tool"]
    assert not doctor.is_ok(findings)


def test_findings_are_ordered_deterministically():
    # version, then no-tools, then per-tool, then sorted duplicates. Here: an
    # unsupported version and two identical unnamed... use a mix to pin order.
    tools = [{"name": "b__t"}, {"name": "a__t", "inputSchema": {"type": "object"}}, {"name": "b__t"}]
    findings = doctor.check_server(tools, "2020-01-01")
    assert _codes(findings) == [
        "unsupported-protocol-version",  # version first
        "missing-input-schema",  # b__t (first), no schema
        "missing-input-schema",  # b__t (third), no schema
        "duplicate-tool",  # duplicates last, name sorted
    ]


def test_check_registry_runs_over_the_handler_surface():
    registry = Registry([BackendsHandler(lambda: [])])
    findings = doctor.check_registry(registry, SUPPORTED)
    assert findings == []  # yamp__backends is a well-formed tool
    assert doctor.is_ok(findings)
