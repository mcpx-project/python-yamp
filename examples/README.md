# Examples (Python arm)

Each is a self-contained program. Run from the `python/` directory:

```
../.venv/bin/python examples/<name>.py
```

| Example | Mode shown | Runs standalone |
|---|---|---|
| `relay.py` | Layer 1 relay bridging stdio and HTTP framing (δ0) | yes |
| `forward_router.py` | forward proxy, two backends, namespacing (δ2) | yes |
| `transparent_l1.py` | transparent Level 1, header filtering (δ4) | yes |
| `chained_proxies.py` | proxy behind a transparent proxy, chained hops (δ5) | yes |
| `streamable_http.py` | yamp as a Streamable HTTP proxy, full session flow over HTTP | yes |
| `fastmcp_server.py` | a real FastMCP server used as a backend | needs `pip install fastmcp` |
| `fastmcp_backend.py` | yamp fronting the FastMCP server over stdio | needs `pip install fastmcp` |

The Rust arm has the matching `forward_router` example:

```
cd rust && cargo run --example forward_router
```

For the stateless, resilience, policy, capability, and observability layers, the
test suites (`python/tests/`, `rust/tests/`) are the most complete worked
examples, one file per layer.
