from yamp import namespace


def test_prefix_uri():
    assert namespace.prefix_uri("docs", "file:///reports/q3.md") == "file:///docs/reports/q3.md"
    assert namespace.prefix_uri("docs", "https://ex.com/p") == "https://ex.com/docs/p"
    assert namespace.prefix_uri("docs", "scheme://auth") == "scheme://auth/docs"
    assert namespace.prefix_uri("docs", "mailto:x") == "mailto:x"  # not hierarchical


def test_split_uri():
    assert namespace.split_uri("file:///docs/reports/q3.md") == ("docs", "file:///reports/q3.md")
    assert namespace.split_uri("https://ex.com/docs/p") == ("docs", "https://ex.com/p")
    assert namespace.split_uri("scheme://auth/docs") == ("docs", "scheme://auth")


def test_split_uri_rejects():
    assert namespace.split_uri("mailto:x") is None  # no ://
    assert namespace.split_uri("scheme://auth") is None  # no path
    assert namespace.split_uri("scheme://auth/") is None  # empty first segment


def test_uri_round_trips():
    for uri in ("file:///reports/q3.md", "https://ex.com/a/b", "s3://bucket/key/name"):
        namespaced = namespace.prefix_uri("b0", uri)
        assert namespace.split_uri(namespaced) == ("b0", uri)
