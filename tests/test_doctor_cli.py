"""Track U: the ``yamp-doctor`` CLI entrypoint (server-role config preflight).

Drives doctor.py's ``run`` over temp config files and asserts the rendered report
and the process exit code: 0 when servable (warnings advisory), 1 when an error
makes the surface unservable, 2 when the config cannot be loaded. Mirrors the Rust
arm's tests/doctor_cli.rs, which spawns the yamp-doctor binary.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # python/ for the entrypoint

import doctor as cli
from yamp import doctor


def _write(tmp_path, data):
    path = tmp_path / "config.json"
    path.write_text(json.dumps(data))
    return str(path)


def test_proxy_config_no_local_tools_is_servable(tmp_path, capsys):
    # A pure forward-proxy config exposes no *local* handler tools, so the
    # server-role surface is empty: an advisory warning, but still servable.
    path = _write(tmp_path, {"listen": "127.0.0.1:9100", "backends": {"b0": {"address": "127.0.0.1:9101"}}})
    code = cli.run(path, as_json=False)
    out = capsys.readouterr().out
    assert code == 0
    assert "warning: [no-tools] server exposes no tools" in out
    assert out.strip().endswith("config ok")


def test_meta_tool_surface_is_servable_json(tmp_path, capsys):
    path = _write(
        tmp_path,
        {"listen": "127.0.0.1:9100", "backends": {"b0": {"address": "127.0.0.1:9101"}}, "handlers": {"metaTools": True}},
    )
    code = cli.run(path, as_json=True)
    report = json.loads(capsys.readouterr().out)
    assert code == 0
    assert report["ok"] is True
    assert "findings" in report


def test_strict_mode_blocks_on_warning(tmp_path, capsys):
    # --strict escalates the advisory no-tools warning into a blocking finding, so
    # the same proxy config that is servable by default is rejected under strict.
    path = _write(tmp_path, {"listen": "127.0.0.1:9100", "backends": {"b0": {"address": "127.0.0.1:9101"}}})
    code = cli.run(path, mode=doctor.MODE_STRICT)
    out = capsys.readouterr().out
    assert code == 1
    assert out.strip().endswith("config invalid")


def test_lenient_mode_accepts_warning(tmp_path, capsys):
    path = _write(tmp_path, {"listen": "127.0.0.1:9100", "backends": {"b0": {"address": "127.0.0.1:9101"}}})
    code = cli.run(path, mode=doctor.MODE_LENIENT)
    out = capsys.readouterr().out
    assert code == 0
    assert out.strip().endswith("config ok")


def test_unloadable_config_exits_two(tmp_path, capsys):
    # Missing 'listen' makes the config unloadable: a fatal config-file problem. Doctor
    # identifies the specific field-failure cause (U8), not a generic config-load error.
    path = _write(tmp_path, {"backends": {"b0": {"address": "127.0.0.1:9101"}}})
    code = cli.run(path, as_json=False)
    out = capsys.readouterr().out
    assert code == 2
    assert "error: [missing-listen]" in out
    assert out.strip().endswith("config invalid")


def test_unloadable_config_json_exits_two(tmp_path, capsys):
    path = _write(tmp_path, {"backends": {"b0": {"address": "127.0.0.1:9101"}}})
    code = cli.run(path, as_json=True)
    report = json.loads(capsys.readouterr().out)
    assert code == 2
    assert report["ok"] is False
    assert report["findings"][0]["code"] == "missing-listen"
