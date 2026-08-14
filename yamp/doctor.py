"""Server-role preflight check (σ6; the ``nginx -t`` analog).

A server that originates responses should be able to say, before it accepts any
traffic, whether its configuration is coherent. :func:`check_server` inspects the
composed tool surface a server would expose and the protocol version it would
advertise, and returns an ordered list of findings (``error``/``warning``) rather
than raising, so a caller can print them all at once the way ``nginx -t`` reports
every problem in a config. The check is pure and deterministic, so its verdict is
pinned in the differential corpus and agrees across both arms.

This is the diagnostic half of Track U's ``doctor`` verb, scoped here to the
server role; the CLI wiring (a ``--check`` flag, exit codes, ``--json`` output)
is Track U.
"""

from __future__ import annotations

from . import version

LEVEL_OK = "ok"
LEVEL_WARNING = "warning"
LEVEL_ERROR = "error"


def finding(level: str, code: str, message: str) -> dict:
    """One diagnostic: a severity ``level``, a stable ``code`` a tool keys on, and
    a human ``message``."""
    return {"level": level, "code": code, "message": message}


def check_server(tools: list[dict], protocol_version: str) -> list[dict]:
    """Diagnose a server's exposed tool surface and advertised protocol version.

    ``tools`` is the composed, namespaced tool list a server would serve (for
    example ``Registry.list_tools()``). Findings are returned in a fixed order so
    the result is deterministic: the protocol-version check, then the empty-surface
    check, then per-tool checks in list order, then duplicate-name errors with the
    names sorted.
    """
    findings: list[dict] = []
    if not version.is_supported(protocol_version):
        findings.append(
            finding(
                LEVEL_ERROR,
                "unsupported-protocol-version",
                f"advertised protocol version {protocol_version!r} is not in the supported set {list(version.SUPPORTED_PROTOCOL_VERSIONS)}",
            )
        )
    if not tools:
        findings.append(finding(LEVEL_WARNING, "no-tools", "server exposes no tools"))
    names: list[str] = []
    for tool in tools:
        name = tool.get("name")
        if not isinstance(name, str) or not name:
            findings.append(finding(LEVEL_ERROR, "unnamed-tool", "a tool has no name"))
            continue
        names.append(name)
        if not isinstance(tool.get("inputSchema"), dict):
            findings.append(finding(LEVEL_WARNING, "missing-input-schema", f"tool {name!r} has no object inputSchema"))
    duplicates = sorted({name for name in names if names.count(name) > 1})
    for name in duplicates:
        findings.append(finding(LEVEL_ERROR, "duplicate-tool", f"tool name {name!r} is exposed more than once"))
    return findings


def is_ok(findings: list[dict]) -> bool:
    """Whether the findings are clean enough to serve: no ``error``. A ``warning``
    is advisory (the server can still run), matching ``nginx -t``'s split between a
    fatal config error and a warning."""
    return not any(f["level"] == LEVEL_ERROR for f in findings)


def check_registry(registry, protocol_version: str = version.STATEFUL_PROTOCOL_VERSION) -> list[dict]:
    """Run :func:`check_server` over a :class:`~yamp.handler.Registry`'s composed
    tool surface, the one-call server preflight."""
    return check_server(registry.list_tools(), protocol_version)


MODE_DEFAULT = "default"
MODE_STRICT = "strict"
MODE_LENIENT = "lenient"
MODES = (MODE_DEFAULT, MODE_STRICT, MODE_LENIENT)


def servable(findings: list[dict], mode: str = MODE_DEFAULT) -> bool:
    """Whether the composed surface is servable under the chosen strictness.

    ``default`` follows ``nginx -t``: only an ``error`` blocks serving, a
    ``warning`` is advisory. ``strict`` treats any finding as blocking (a CI gate
    wanting a wholly clean surface). ``lenient`` reports findings but never blocks
    on the surface (a config that loads is accepted; only an unloadable file,
    handled by the caller, fails)."""
    if mode == MODE_LENIENT:
        return True
    if mode == MODE_STRICT:
        return not findings
    return is_ok(findings)


def render_text(findings: list[dict], mode: str = MODE_DEFAULT) -> str:
    """Format findings for a human, the ``nginx -t`` textual report: one line per
    finding (``level: [code] message``) followed by a verdict line that reflects the
    active ``mode``. Deterministic and byte-identical across arms, so the CLI's
    output is pinned in the corpus."""
    lines = [f"{f['level']}: [{f['code']}] {f['message']}" for f in findings]
    lines.append("config ok" if servable(findings, mode) else "config invalid")
    return "\n".join(lines)


def report(findings: list[dict], mode: str = MODE_DEFAULT) -> dict:
    """Machine-readable preflight report (the ``--json`` shape): the ``ok`` verdict
    under ``mode`` and the ordered findings."""
    return {"ok": servable(findings, mode), "findings": findings}


def exit_code(findings: list[dict], mode: str = MODE_DEFAULT) -> int:
    """Process exit code for the preflight: ``0`` when the surface is servable under
    ``mode``, ``1`` when a finding blocks it."""
    return 0 if servable(findings, mode) else 1
