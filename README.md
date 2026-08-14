# yamp, Python arm

Python implementation of yamp (Yet Another MCP Proxy), a forward and transparent
proxy for the Model Context Protocol that works in both MCP protocol modes,
stateful and stateless.

Built in lockstep with the [Rust arm](https://github.com/mcpx-project/rust-yamp)
from one shared increment sequence. Specifications, user guide, conformance
declaration, and benchmarks live in the
[docs repository](https://github.com/mcpx-project/docs).

The data plane is standard library only, with one exception. JSON Schema
validation for server-originated calls takes the `jsonschema` package, because
the standard library has no validator and one cannot be hand-rolled to
byte-parity with the Rust arm.

## Install and test

```
python -m venv .venv
.venv/bin/pip install jsonschema pytest pytest-cov
.venv/bin/python -m pytest
```

## Running as a server

Point a server at one or more backends.

```
python serve_streamable.py --listen 127.0.0.1:9100 --backend github=127.0.0.1:9101
```

Server entrypoints:

| Entrypoint | Transport |
|---|---|
| `serve.py` | TCP with stdio framing |
| `serve_stateless.py` | TCP, stateless forwarder |
| `serve_http.py` | stateless HTTP |
| `serve_streamable.py` | Streamable HTTP with `Mcp-Session-Id` sessions and a `GET /mcp` SSE stream |

A non-loopback bind is refused without client authentication unless `--insecure`
is passed, and a client credential is structurally never forwarded to a backend.

## Configuration

For pools and resilience, the Streamable HTTP server reads a config file.

```
python serve_streamable.py --config proxy.json
```

A backend id maps to one or more addresses, tried in order with failover at
connect time.

```json
{
  "listen": "127.0.0.1:9100",
  "backends": { "github": { "addresses": ["10.0.0.1:9101", "10.0.0.2:9101"] } },
  "resilience": { "failureThreshold": 3, "requestTimeout": 5, "healthInterval": 10 }
}
```

The collision strategy for duplicated tool names across backends is declared in
the `namespacing` section.

```json
{
  "listen": "127.0.0.1:9100",
  "backends": {
    "github": { "addresses": ["127.0.0.1:9101"] },
    "gitlab": { "addresses": ["127.0.0.1:9102"] }
  },
  "namespacing": { "collisionStrategy": "priority" }
}
```

Reload the config without restarting. In-flight sessions are kept and the
document is validated before it is applied, so a typo leaves the running config
in place.

```
kill -HUP <pid>
```

## Operator tooling

Diagnostic entrypoints, each supporting `--json` and documented exit codes.

```
python doctor.py --config proxy.json                  # nginx -t analog: server-surface preflight
python config_cli.py validate  --config proxy.json    # does the document load and conform
python config_cli.py explain   --config proxy.json listen
python config_cli.py effective --config proxy.json    # every key's resolved value and provenance
python config_cli.py diff --config a.json --to b.json
python config_cli.py adapt --config human.json        # normalize shorthands to canonical JSON
```

At runtime, `GET /status` returns a read-only operational snapshot and `--tap`
prints a credential-redacted capture of each client request.

A config error names the specific field-failure cause with a fix hint and a link
into [`CONFIG_ERRORS.md`](CONFIG_ERRORS.md). Every emitted error id is documented
in [`ERRORS.md`](ERRORS.md). Both files are generated from the single-source
registries by `tools/gen_error_index.py` and a staleness gate keeps them honest.

## Layout

```
yamp/         the package
serve*.py     server entrypoints
doctor.py     server-surface preflight CLI
config_cli.py config validation, provenance, and normalization CLI
examples/     runnable programs the user guide references
tests/        the suite
tools/        corpus and error-index generators
conformance/  cross-arm test corpus, shared with the Rust arm
```

## Cross-arm corpus

`conformance/` pins this arm to the Rust arm. `differential-corpus.json` pins
pure functions, `flow-corpus.json` pins whole message flows, and
`sep-0000-traceability.json` maps spec clauses to the tests that evidence them.
This arm is the generator; `tools/gen_differential_corpus.py` and
`tools/gen_flow_corpus.py` produce the corpus and the docs repository's
`tools/sync-corpus.sh` propagates it to the Rust arm.

## Status

460 tests at 100% line coverage. Per-message proxy overhead is held at or under
10 ms, enforced as a test tier.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
