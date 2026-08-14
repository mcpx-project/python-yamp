"""Config-file loader (SEP Section 10 schema, practical subset).

A JSON document describes the listen address, the backends (each id maps to one
or more addresses for failover), and optional resilience settings. Servers read
it with ``--config file.json`` instead of repeated ``--backend`` flags.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from . import collision, namespace, security


@dataclass
class Namespacing:
    """Collision resolution config (SEP §3.4). The active strategy is declared;
    ``prefix`` is the default and requires no further settings."""

    strategy: str = collision.PREFIX
    overrides: dict[str, str] = field(default_factory=dict)  # namespaced -> exposed (manual)
    priority: list[str] = field(default_factory=list)  # backend ids, highest priority first


@dataclass
class RestHandlerConfig:
    """One REST-to-MCP Conversion handler served locally (δ17)."""

    id: str
    base_url: str
    operations: list[dict] = field(default_factory=list)


@dataclass
class HandlerConfig:
    """Local handlers the proxy serves itself (draft §5.7 Conversion, meta-tools)."""

    meta_tools: bool = False  # enable the yamp__backends meta-tool
    rest: list[RestHandlerConfig] = field(default_factory=list)


@dataclass
class Resilience:
    failure_threshold: int = 5
    reset_timeout: float = 30.0
    health_interval: float | None = None
    request_timeout: float | None = None
    # An explicit on/off from config ``resilience.enabled``; when None the
    # meaningfully-configured heuristic decides.
    explicit_enabled: bool | None = None

    @property
    def enabled(self) -> bool:
        # An explicit ``resilience.enabled`` in config wins; otherwise a breaker
        # is attached only when resilience is meaningfully configured (any
        # non-default timing setting), so an operator who wants breakers with
        # default timings can still ask for them explicitly.
        if self.explicit_enabled is not None:
            return self.explicit_enabled
        return (
            self.health_interval is not None
            or self.request_timeout is not None
            or self.failure_threshold != 5
            or self.reset_timeout != 30.0
        )


@dataclass
class BackendConfig:
    id: str
    addresses: list[str]  # tried in order for failover
    token: str | None = None


@dataclass
class ProxyConfig:
    listen: str
    backends: list[BackendConfig]
    resilience: Resilience = field(default_factory=Resilience)
    client_tokens: list[str] = field(default_factory=list)  # bearer tokens the proxy accepts
    namespacing: Namespacing = field(default_factory=Namespacing)
    handlers: HandlerConfig = field(default_factory=HandlerConfig)
    audit_secret: str | None = None  # enables the signed accountability log (SEP-2828)


def parse_address(address: str) -> tuple[str, int]:
    host, _, port = address.rpartition(":")
    return host, int(port)


# Config-error catalog (Track U, U4/U8): the field-failure causes yamp diagnoses, in
# check order. Each entry pairs a stable slug with a one-line description (for the
# generated index) and a fix hint. The docs URL is derived from the slug so it cannot
# drift (``CONFIG_ERRORS.md#slug``). Mirrored across arms and pinned in the corpus.
CONFIG_ERRORS = [
    ("not-object", "the config is not a JSON object", "wrap the settings in a top-level { ... } object"),
    ("backends-not-object", "'backends' is not a JSON object", "make 'backends' a map of id to { address }"),
    ("invalid-backend-id", "a backend id is empty or contains the reserved '__' delimiter", "rename the backend so its id has no '__'"),
    ("backend-no-addresses", "a backend declares no address", "give the backend an 'address' or a non-empty 'addresses'"),
    ("missing-listen", "the config has no 'listen' address", 'add "listen": "127.0.0.1:PORT"'),
    ("unknown-collision-strategy", "namespacing.strategy is not a supported strategy", "set it to prefix, priority, manual, or passthrough"),
    ("invalid-handler-id", "a rest handler id is missing or invalid", "give the handler a non-empty id without '__'"),
    ("handler-backend-collision", "a handler id collides with a backend id", "rename the handler or the backend"),
    ("handler-missing-baseurl", "a rest handler has no 'baseUrl'", "add a 'baseUrl' to the handler"),
    ("invalid-json", "the config is not valid JSON", "fix the JSON syntax at the reported line and column"),
]

_CONFIG_ERROR_META = {slug: (description, hint) for slug, description, hint in CONFIG_ERRORS}


def config_docs_url(slug: str) -> str:
    """The stable in-index anchor for a config-error slug (``""`` if unknown)."""
    return f"CONFIG_ERRORS.md#{slug}" if slug in _CONFIG_ERROR_META else ""


def error_catalog() -> list[dict]:
    """The whole catalog as structured entries, for the generated index and cross-arm
    pinning: each ``{slug, description, hint, docsUrl}``."""
    return [
        {"slug": slug, "description": description, "hint": hint, "docsUrl": config_docs_url(slug)}
        for slug, description, hint in CONFIG_ERRORS
    ]


def _first_error(data) -> tuple[str, str] | None:
    """The first schema violation in ``data`` as ``(slug, message)``, or ``None`` when
    the document conforms. One source: :func:`from_dict` raises from it and
    :func:`diagnose` reports from it, so the two never diverge."""
    if not isinstance(data, dict):
        return ("not-object", "config must be a JSON object")
    backends = data.get("backends", {})
    if not isinstance(backends, dict):
        return ("backends-not-object", "'backends' must be a JSON object")
    for backend_id, spec in backends.items():
        if not namespace.valid_backend_id(backend_id):
            return ("invalid-backend-id", f"invalid backend id: {backend_id!r}")
        addresses = list(spec.get("addresses", [])) if isinstance(spec, dict) else []
        if isinstance(spec, dict) and "address" in spec:
            addresses.append(spec["address"])
        if not addresses:
            return ("backend-no-addresses", f"backend {backend_id!r} has no addresses")
    if "listen" not in data:
        return ("missing-listen", "config is missing 'listen'")
    strategy = data.get("namespacing", {}).get("strategy", collision.PREFIX)
    if strategy not in collision.STRATEGIES:
        return ("unknown-collision-strategy", f"unknown collision strategy: {strategy!r}")
    backend_ids = set(backends)
    for spec in data.get("handlers", {}).get("rest", []):
        hid = spec.get("id")
        if not hid or not namespace.valid_backend_id(hid):
            return ("invalid-handler-id", f"invalid rest handler id: {hid!r}")
        if hid in backend_ids:
            return ("handler-backend-collision", f"handler id {hid!r} collides with a backend id")
        if "baseUrl" not in spec:
            return ("handler-missing-baseurl", f"rest handler {hid!r} is missing 'baseUrl'")
    return None


def parse_error_finding(message: str, line: int, column: int) -> dict:
    """The structured finding for a JSON *parse* failure, carrying the line and column
    the loader recovered from the source text (U4)."""
    _description, hint = _CONFIG_ERROR_META["invalid-json"]
    return {"slug": "invalid-json", "message": message, "line": line, "column": column,
            "hint": hint, "docsUrl": config_docs_url("invalid-json")}


def diagnose(data) -> dict | None:
    """Diagnose the first schema violation as a structured finding (``slug``,
    ``message``, ``hint``, ``docsUrl``), or ``None`` when the document conforms. This
    is the U4/U8 field-failure identification, pure over the raw document (line/column
    for a JSON *parse* error is added by the loader, which has the source text)."""
    err = _first_error(data)
    if err is None:
        return None
    slug, message = err
    _description, hint = _CONFIG_ERROR_META[slug]
    return {"slug": slug, "message": message, "hint": hint, "docsUrl": config_docs_url(slug)}


def from_dict(data: dict) -> ProxyConfig:
    err = _first_error(data)
    if err is not None:
        raise ValueError(err[1])
    section = data.get("resilience", {})
    resilience = Resilience(
        failure_threshold=section.get("failureThreshold", 5),
        reset_timeout=section.get("resetTimeout", 30.0),
        health_interval=section.get("healthInterval"),
        request_timeout=section.get("requestTimeout"),
        explicit_enabled=section.get("enabled"),
    )
    backends = []
    for backend_id, spec in data.get("backends", {}).items():
        addresses = list(spec.get("addresses", []))
        if "address" in spec:
            addresses.append(spec["address"])
        backends.append(BackendConfig(id=backend_id, addresses=addresses, token=spec.get("token")))
    client_tokens = list(data.get("auth", {}).get("clientTokens", []))
    ns = data.get("namespacing", {})
    strategy = ns.get("strategy", collision.PREFIX)
    namespacing = Namespacing(
        strategy=strategy,
        overrides=dict(ns.get("overrides", {})),
        priority=list(ns.get("priority", [])),
    )
    section = data.get("handlers", {})
    rest_handlers = []
    for spec in section.get("rest", []):
        hid = spec.get("id")
        rest_handlers.append(
            RestHandlerConfig(id=hid, base_url=spec["baseUrl"], operations=list(spec.get("operations", [])))
        )
    handlers = HandlerConfig(meta_tools=bool(section.get("metaTools", False)), rest=rest_handlers)
    # A non-empty audit secret enables the signed accountability log (SEP-2828);
    # an empty secret is treated as absent.
    audit_secret = data.get("audit", {}).get("secret") or None
    return ProxyConfig(
        listen=data["listen"],
        backends=backends,
        resilience=resilience,
        client_tokens=client_tokens,
        namespacing=namespacing,
        handlers=handlers,
        audit_secret=audit_secret,
    )


def load_config(path: str) -> ProxyConfig:
    with open(path) as handle:
        return from_dict(json.load(handle))


# Provenance table for `config explain` (Track U): every explainable config key in
# dotted JSON form, paired with its built-in default. A key the loaded document sets
# is sourced from the config; an absent key falls back to this default. List order is
# the display order for `config effective`. Mirrored in the Rust arm and pinned in
# the differential corpus, and a test ties each default to what ``from_dict``
# actually resolves so the table cannot drift from the dataclass defaults.
EXPLAIN_KEYS = [
    ("listen", None),
    ("resilience.failureThreshold", 5),
    ("resilience.resetTimeout", 30.0),
    ("resilience.healthInterval", None),
    ("resilience.requestTimeout", None),
    ("resilience.enabled", None),
    ("namespacing.strategy", collision.PREFIX),
    ("auth.clientTokens", []),
    ("handlers.metaTools", False),
    ("audit.secret", None),
]

SOURCE_CONFIG = "config"
SOURCE_DEFAULT = "default"
SOURCE_UNKNOWN = "unknown"


def _lookup(raw: dict, key: str) -> tuple[bool, object]:
    node = raw
    for part in key.split("."):
        if not isinstance(node, dict) or part not in node:
            return False, None
        node = node[part]
    return True, node


def explain(raw: dict, key: str) -> dict:
    """Explain one config key: its effective value and where it came from.

    ``source`` is ``config`` when the loaded document set the key, ``default`` when it
    fell back to the built-in default, and ``unknown`` for an unrecognized key."""
    present, value = _lookup(raw, key)
    if present:
        return {"key": key, "value": value, "source": SOURCE_CONFIG}
    for known, default in EXPLAIN_KEYS:
        if known == key:
            return {"key": key, "value": default, "source": SOURCE_DEFAULT}
    return {"key": key, "value": None, "source": SOURCE_UNKNOWN}


def effective(raw: dict) -> list[dict]:
    """Explain every known key in order: the resolved config with per-key provenance."""
    return [explain(raw, key) for key, _ in EXPLAIN_KEYS]


def explain_line(entry: dict) -> str:
    """One human line for an explained key, ``key = <json-value> (source)``. The value
    is compact sorted JSON so the text is byte-identical across arms."""
    rendered = json.dumps(entry["value"], sort_keys=True, separators=(",", ":"))
    return f"{entry['key']} = {rendered} ({entry['source']})"


def diff(left: dict, right: dict) -> list[dict]:
    """Diff two config documents over the resolved view: every known key whose
    effective value differs between ``left`` and ``right``, in table order. Each entry
    carries both sides' value and provenance, so a pure default-vs-default match is
    omitted while a real behavioral change is shown."""
    changes = []
    for key, _ in EXPLAIN_KEYS:
        a = explain(left, key)
        b = explain(right, key)
        if a["value"] != b["value"]:
            changes.append({
                "key": key,
                "left": {"value": a["value"], "source": a["source"]},
                "right": {"value": b["value"], "source": b["source"]},
            })
    return changes


def diff_line(entry: dict) -> str:
    """One human line for a diffed key, ``key: <left> (source) -> <right> (source)``.
    Values are compact sorted JSON so the text is byte-identical across arms."""
    left = json.dumps(entry["left"]["value"], sort_keys=True, separators=(",", ":"))
    right = json.dumps(entry["right"]["value"], sort_keys=True, separators=(",", ":"))
    return f"{entry['key']}: {left} ({entry['left']['source']}) -> {right} ({entry['right']['source']})"


def _adapt_addresses(spec: str) -> list[str]:
    return [addr.strip() for addr in spec.split(",")]


def _adapt_listen(listen):
    # A bare port (int) or ":port" binds the secure loopback default (U7).
    if isinstance(listen, bool):  # bool is an int subclass; a bool listen is not a port
        return listen
    if isinstance(listen, int):
        return f"{security.DEFAULT_BIND_HOST}:{listen}"
    if isinstance(listen, str) and listen.startswith(":"):
        return f"{security.DEFAULT_BIND_HOST}{listen}"
    return listen


def _adapt_backends(backends):
    if isinstance(backends, list):
        # A list of "id=host:port[,host:port]" strings, like the --backend CLI flag.
        out = {}
        for item in backends:
            if not isinstance(item, str) or "=" not in item:
                continue
            bid, spec = item.split("=", 1)
            out[bid] = {"addresses": _adapt_addresses(spec)}
        return out
    if isinstance(backends, dict):
        # A map whose value may be a bare "host:port[,...]" string.
        out = {}
        for bid, value in backends.items():
            out[bid] = {"addresses": _adapt_addresses(value)} if isinstance(value, str) else value
        return out
    return backends


def adapt(raw: dict) -> dict:
    """Normalize a human-friendly config document to the canonical SEP schema.

    Expands operator shorthands so the result loads via :func:`from_dict` and yields
    the intended effective config: a ``listen`` given as a bare port (int) or ``:port``
    becomes ``127.0.0.1:port`` (the secure loopback default), a ``backends`` list of
    ``id=host:port`` strings becomes the canonical map, and a backend value that is a
    bare ``host:port`` string becomes ``{"addresses": [...]}``. Every other key passes
    through, and a canonical document is returned unchanged, so ``adapt`` is idempotent
    on its own output (U9)."""
    out = dict(raw)
    if "listen" in out:
        out["listen"] = _adapt_listen(out["listen"])
    if "backends" in out:
        out["backends"] = _adapt_backends(out["backends"])
    return out
