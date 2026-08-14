"""Track U: `config explain` / `effective` provenance and the yamp-config CLI.

Beyond the differential corpus (which pins explain/effective/explain_line across
arms), this guards that the EXPLAIN_KEYS default table cannot drift from the
dataclass defaults ``from_dict`` actually resolves, and drives the CLI entrypoint.
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # python/ for the entrypoint

import config_cli
from yamp import config as cfg


def test_explain_keys_match_resolved_defaults():
    # Resolve a minimal config so every optional key falls back to its default, then
    # assert the provenance table's default equals what from_dict actually produced.
    resolved = cfg.from_dict({"listen": "127.0.0.1:9100"})
    actual = {
        "resilience.failureThreshold": resolved.resilience.failure_threshold,
        "resilience.resetTimeout": resolved.resilience.reset_timeout,
        "resilience.healthInterval": resolved.resilience.health_interval,
        "resilience.requestTimeout": resolved.resilience.request_timeout,
        "resilience.enabled": resolved.resilience.explicit_enabled,
        "namespacing.strategy": resolved.namespacing.strategy,
        "auth.clientTokens": resolved.client_tokens,
        "handlers.metaTools": resolved.handlers.meta_tools,
        "audit.secret": resolved.audit_secret,
    }
    table = dict(cfg.EXPLAIN_KEYS)
    for key, expected in actual.items():
        assert table[key] == expected, f"{key}: table default {table[key]!r} != resolved {expected!r}"


def test_explain_provenance():
    assert cfg.explain({}, "resilience.failureThreshold") == {
        "key": "resilience.failureThreshold", "value": 5, "source": "default"}
    assert cfg.explain({"resilience": {"failureThreshold": 9}}, "resilience.failureThreshold") == {
        "key": "resilience.failureThreshold", "value": 9, "source": "config"}
    assert cfg.explain({}, "nope")["source"] == "unknown"


def _write(tmp_path, data):
    path = tmp_path / "config.json"
    path.write_text(json.dumps(data))
    return str(path)


def _run(command, path, key=None, as_json=False, to=None):
    return config_cli.run(argparse.Namespace(command=command, config=path, key=key, json=as_json, to=to))


def test_cli_validate_valid_and_invalid(tmp_path, capsys):
    good = _write(tmp_path, {"listen": "127.0.0.1:9100", "backends": {"b0": {"address": "127.0.0.1:9101"}}})
    assert _run("validate", good) == 0
    assert capsys.readouterr().out.strip() == "config valid"
    # A missing 'listen' fails schema conformance: invalid, exit 1 (not 2).
    bad = _write(tmp_path, {"backends": {"b0": {"address": "127.0.0.1:9101"}}})
    assert _run("validate", bad) == 1
    assert capsys.readouterr().out.startswith("config invalid:")


def test_cli_validate_json(tmp_path, capsys):
    good = _write(tmp_path, {"listen": "127.0.0.1:9100"})
    assert _run("validate", good, as_json=True) == 0
    assert json.loads(capsys.readouterr().out) == {"valid": True}
    bad = _write(tmp_path, {"listen": "127.0.0.1:9100", "namespacing": {"strategy": "nope"}})
    assert _run("validate", bad, as_json=True) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["valid"] is False
    # U4: a schema error carries a slug, fix hint, and docs URL.
    error = report["error"]
    assert error["slug"] == "unknown-collision-strategy"
    assert error["hint"] and error["docsUrl"] == "CONFIG_ERRORS.md#unknown-collision-strategy"


def test_cli_validate_malformed_json_carries_line_column(tmp_path, capsys):
    path = tmp_path / "c.json"
    path.write_text('{\n  "listen": ,\n}')  # syntax error on line 2
    assert _run("validate", str(path), as_json=True) == 1
    error = json.loads(capsys.readouterr().out)["error"]
    # U4: a parse error carries line/column plus a fix hint and docs URL.
    assert error["slug"] == "invalid-json"
    assert error["line"] == 2 and error["column"] > 0
    assert error["docsUrl"] == "CONFIG_ERRORS.md#invalid-json"


def test_config_docs_url_unknown_slug():
    assert cfg.config_docs_url("no-such-slug") == ""


def test_cli_explain_default_and_config(tmp_path, capsys):
    path = _write(tmp_path, {"listen": "127.0.0.1:9100", "resilience": {"failureThreshold": 9}})
    assert _run("explain", path, key="resilience.failureThreshold") == 0
    assert capsys.readouterr().out.strip() == "resilience.failureThreshold = 9 (config)"
    assert _run("explain", path, key="resilience.resetTimeout") == 0
    assert capsys.readouterr().out.strip() == "resilience.resetTimeout = 30.0 (default)"


def test_cli_explain_unknown_key_exits_two(tmp_path, capsys):
    path = _write(tmp_path, {"listen": "127.0.0.1:9100"})
    assert _run("explain", path, key="bogus.key") == 2
    assert capsys.readouterr().out.strip() == "bogus.key = null (unknown)"


def test_cli_effective_json(tmp_path, capsys):
    path = _write(tmp_path, {"listen": "127.0.0.1:9100"})
    assert _run("effective", path, as_json=True) == 0
    entries = json.loads(capsys.readouterr().out)
    assert {e["key"] for e in entries} == {k for k, _ in cfg.EXPLAIN_KEYS}
    listen = next(e for e in entries if e["key"] == "listen")
    assert listen == {"key": "listen", "value": "127.0.0.1:9100", "source": "config"}


def test_cli_diff_reports_changed_keys(tmp_path, capsys):
    a = _write(tmp_path, {"listen": "127.0.0.1:9100"})
    b = tmp_path / "b.json"
    b.write_text(json.dumps({"listen": "127.0.0.1:9100", "resilience": {"failureThreshold": 9}}))
    # A default -> config change on failureThreshold; listen is identical so omitted.
    assert _run("diff", a, to=str(b)) == 1
    assert capsys.readouterr().out.strip() == "resilience.failureThreshold: 5 (default) -> 9 (config)"


def test_cli_diff_identical_configs(tmp_path, capsys):
    a = _write(tmp_path, {"listen": "127.0.0.1:9100"})
    b = tmp_path / "b.json"
    b.write_text(json.dumps({"listen": "127.0.0.1:9100"}))
    assert _run("diff", a, to=str(b)) == 0
    assert capsys.readouterr().out.strip() == "no differences"


def test_cli_diff_unreadable_other_exits_two(tmp_path, capsys):
    a = _write(tmp_path, {"listen": "127.0.0.1:9100"})
    b = tmp_path / "b.json"
    b.write_text("{ not json")
    assert _run("diff", a, to=str(b)) == 2
    assert "error:" in capsys.readouterr().err


def test_adapt_round_trips_and_is_idempotent():
    # U9: adapt output re-validates (loads via from_dict) and re-adapting is the
    # identity, so it yields an identical effective configuration.
    human = {"listen": 9100, "backends": ["b0=127.0.0.1:9101", "b1=127.0.0.1:9102,127.0.0.1:9202"]}
    canonical = cfg.adapt(human)
    assert canonical["listen"] == "127.0.0.1:9100"  # bare port -> secure loopback default
    assert canonical["backends"] == {
        "b0": {"addresses": ["127.0.0.1:9101"]},
        "b1": {"addresses": ["127.0.0.1:9102", "127.0.0.1:9202"]},
    }
    resolved = cfg.from_dict(canonical)  # re-validates
    assert [b.id for b in resolved.backends] == ["b0", "b1"]
    assert resolved.backends[1].addresses == ["127.0.0.1:9102", "127.0.0.1:9202"]
    assert cfg.adapt(canonical) == canonical  # idempotent on its own output


def test_cli_adapt_emits_canonical(tmp_path, capsys):
    path = _write(tmp_path, {"listen": ":9100", "backends": {"b0": "127.0.0.1:9101"}})
    assert config_cli.run(argparse.Namespace(command="adapt", config=path)) == 0
    canonical = json.loads(capsys.readouterr().out)
    assert canonical == {"listen": "127.0.0.1:9100", "backends": {"b0": {"addresses": ["127.0.0.1:9101"]}}}


def test_cli_unreadable_config_exits_two(tmp_path, capsys):
    path = _write(tmp_path, {"listen": "x"})
    Path(path).write_text("{ not json")
    assert _run("explain", path, key="listen") == 2
    assert "error:" in capsys.readouterr().err
