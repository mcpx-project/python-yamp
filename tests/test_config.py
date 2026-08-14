import json

import pytest

from yamp.config import from_dict, load_config, parse_address


def test_from_dict_full():
    config = from_dict({
        "listen": "127.0.0.1:9100",
        "backends": {
            "github": {"addresses": ["127.0.0.1:9101", "127.0.0.1:9111"], "token": "t"},
            "slack": {"address": "127.0.0.1:9102"},
        },
        "resilience": {"failureThreshold": 3, "resetTimeout": 20, "healthInterval": 5, "requestTimeout": 2},
    })
    assert config.listen == "127.0.0.1:9100"
    by_id = {b.id: b for b in config.backends}
    assert by_id["github"].addresses == ["127.0.0.1:9101", "127.0.0.1:9111"]
    assert by_id["github"].token == "t"
    assert by_id["slack"].addresses == ["127.0.0.1:9102"]
    assert config.resilience.failure_threshold == 3
    assert config.resilience.health_interval == 5
    assert config.resilience.enabled is True


def test_defaults_are_not_resilient():
    config = from_dict({"listen": "x:1", "backends": {"a": {"address": "h:1"}}})
    assert config.resilience.enabled is False


def test_explicit_enabled_overrides_the_heuristic():
    # Breakers on despite default timings.
    on = from_dict({"listen": "x:1", "backends": {"a": {"address": "h:1"}}, "resilience": {"enabled": True}})
    assert on.resilience.enabled is True
    # Breakers off despite non-default timings.
    off = from_dict({
        "listen": "x:1", "backends": {"a": {"address": "h:1"}},
        "resilience": {"enabled": False, "failureThreshold": 3},
    })
    assert off.resilience.enabled is False


def test_client_tokens():
    config = from_dict({"listen": "x:1", "backends": {"a": {"address": "h:1"}}, "auth": {"clientTokens": ["t1", "t2"]}})
    assert config.client_tokens == ["t1", "t2"]
    plain = from_dict({"listen": "x:1", "backends": {"a": {"address": "h:1"}}})
    assert plain.client_tokens == []


def test_namespacing_defaults_to_prefix():
    config = from_dict({"listen": "x:1", "backends": {"a": {"address": "h:1"}}})
    assert config.namespacing.strategy == "prefix"
    assert config.namespacing.overrides == {}
    assert config.namespacing.priority == []


def test_namespacing_parsed():
    config = from_dict(
        {
            "listen": "x:1",
            "backends": {"a": {"address": "h:1"}},
            "namespacing": {
                "strategy": "priority",
                "priority": ["gh", "gl"],
                "overrides": {"github__create_issue": "gh_new_issue"},
            },
        }
    )
    assert config.namespacing.strategy == "priority"
    assert config.namespacing.priority == ["gh", "gl"]
    assert config.namespacing.overrides == {"github__create_issue": "gh_new_issue"}


def test_namespacing_unknown_strategy_rejected():
    with pytest.raises(ValueError):
        from_dict(
            {"listen": "x:1", "backends": {"a": {"address": "h:1"}}, "namespacing": {"strategy": "bogus"}}
        )


def test_handlers_defaults_empty():
    config = from_dict({"listen": "x:1", "backends": {"a": {"address": "h:1"}}})
    assert config.handlers.meta_tools is False
    assert config.handlers.rest == []


def test_handlers_parsed():
    config = from_dict(
        {
            "listen": "x:1",
            "backends": {"a": {"address": "h:1"}},
            "handlers": {
                "metaTools": True,
                "rest": [{"id": "gh", "baseUrl": "https://api.example.com", "operations": [{"name": "get"}]}],
            },
        }
    )
    assert config.handlers.meta_tools is True
    assert config.handlers.rest[0].id == "gh"
    assert config.handlers.rest[0].base_url == "https://api.example.com"
    assert config.handlers.rest[0].operations == [{"name": "get"}]


def test_handler_id_validation():
    base = {"listen": "x:1", "backends": {"a": {"address": "h:1"}}}
    with pytest.raises(ValueError):  # invalid id (delimiter)
        from_dict({**base, "handlers": {"rest": [{"id": "bad__id", "baseUrl": "http://x"}]}})
    with pytest.raises(ValueError):  # collides with a backend id
        from_dict({**base, "handlers": {"rest": [{"id": "a", "baseUrl": "http://x"}]}})
    with pytest.raises(ValueError):  # missing baseUrl
        from_dict({**base, "handlers": {"rest": [{"id": "gh"}]}})


def test_parse_address():
    assert parse_address("127.0.0.1:9101") == ("127.0.0.1", 9101)


def test_parse_address_rejects_missing_or_bad_port():
    # Both arms fail fast on these rather than silently binding port 0.
    with pytest.raises(ValueError):
        parse_address("localhost")
    with pytest.raises(ValueError):
        parse_address("localhost:abc")


def test_invalid_backend_id():
    with pytest.raises(ValueError):
        from_dict({"listen": "x:1", "backends": {"a__b": {"address": "h:1"}}})


def test_backend_without_addresses():
    with pytest.raises(ValueError):
        from_dict({"listen": "x:1", "backends": {"a": {}}})


def test_missing_listen():
    with pytest.raises(ValueError):
        from_dict({"backends": {"a": {"address": "h:1"}}})


def test_audit_secret_parsed_and_defaults_absent():
    base = {"listen": "x:1", "backends": {"a": {"address": "h:1"}}}
    assert from_dict(base).audit_secret is None
    assert from_dict({**base, "audit": {"secret": "s3cret"}}).audit_secret == "s3cret"
    # An empty secret is treated as absent, so it does not enable the log.
    assert from_dict({**base, "audit": {"secret": ""}}).audit_secret is None


def test_load_config_from_file(tmp_path):
    path = tmp_path / "c.json"
    path.write_text(json.dumps({"listen": "127.0.0.1:9100", "backends": {"a": {"address": "h:1"}}}))
    config = load_config(str(path))
    assert config.listen == "127.0.0.1:9100"
    assert config.backends[0].id == "a"
