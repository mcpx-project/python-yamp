from yamp.cache import DEFAULT_TTL_MS, ListCache


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


def test_hit_within_ttl_then_expires():
    clock = Clock()
    cache = ListCache(now=clock)
    cache.put("gh", "tools/list", None, {"tools": [{"name": "a"}], "ttlMs": 1000})
    assert cache.get("gh", "tools/list", None) == {"tools": [{"name": "a"}], "ttlMs": 1000}
    clock.advance(0.999)
    assert cache.get("gh", "tools/list", None) is not None
    clock.advance(0.001)  # now at ttl boundary: stale
    assert cache.get("gh", "tools/list", None) is None


def test_default_ttl_when_absent():
    clock = Clock()
    cache = ListCache(now=clock)
    cache.put("gh", "tools/list", None, {"tools": []})
    clock.advance(DEFAULT_TTL_MS / 1000.0 - 1)
    assert cache.get("gh", "tools/list", None) is not None
    clock.advance(1)
    assert cache.get("gh", "tools/list", None) is None


def test_ttl_zero_or_negative_not_cached():
    cache = ListCache()
    cache.put("gh", "tools/list", None, {"tools": [], "ttlMs": 0})
    assert cache.get("gh", "tools/list", None) is None
    cache.put("gh", "tools/list", None, {"tools": [], "ttlMs": -5})
    assert cache.get("gh", "tools/list", None) is None


def test_ttl_zero_drops_prior_entry():
    cache = ListCache()
    cache.put("gh", "tools/list", None, {"tools": [{"name": "a"}], "ttlMs": 1000})
    assert cache.get("gh", "tools/list", None) is not None
    cache.put("gh", "tools/list", None, {"tools": [], "ttlMs": 0})
    assert cache.get("gh", "tools/list", None) is None


def test_non_numeric_ttl_falls_back_to_default():
    clock = Clock()
    cache = ListCache(now=clock)
    # A bool is not a valid ttl even though bool is an int subtype.
    cache.put("gh", "tools/list", None, {"tools": [], "ttlMs": True})
    assert cache.get("gh", "tools/list", None) is not None
    cache.put("gl", "tools/list", None, {"tools": [], "ttlMs": "soon"})
    assert cache.get("gl", "tools/list", None) is not None


def test_public_served_to_any_principal():
    cache = ListCache()
    cache.put("gh", "tools/list", "alice", {"tools": [{"name": "a"}], "ttlMs": 1000, "cacheScope": "public"})
    assert cache.get("gh", "tools/list", "bob") is not None
    assert cache.get("gh", "tools/list", None) is not None


def test_private_isolated_across_principals():
    cache = ListCache()
    cache.put("gh", "tools/list", "alice", {"tools": [{"name": "secret"}], "ttlMs": 1000, "cacheScope": "private"})
    assert cache.get("gh", "tools/list", "alice") is not None
    assert cache.get("gh", "tools/list", "bob") is None
    assert cache.get("gh", "tools/list", None) is None


def test_unknown_scope_treated_as_public():
    cache = ListCache()
    cache.put("gh", "tools/list", "alice", {"tools": [], "ttlMs": 1000, "cacheScope": "weird"})
    assert cache.get("gh", "tools/list", "bob") is not None


def test_invalidate_backend_drops_only_that_backend():
    cache = ListCache()
    cache.put("gh", "tools/list", None, {"tools": [], "ttlMs": 1000})
    cache.put("gh", "prompts/list", None, {"prompts": [], "ttlMs": 1000})
    cache.put("gl", "tools/list", None, {"tools": [], "ttlMs": 1000})
    cache.invalidate_backend("gh")
    assert cache.get("gh", "tools/list", None) is None
    assert cache.get("gh", "prompts/list", None) is None
    assert cache.get("gl", "tools/list", None) is not None


def test_miss_on_empty_cache():
    assert ListCache().get("gh", "tools/list", None) is None
