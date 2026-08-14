from yamp import routing
from yamp.version import SUPPORTED_PROTOCOL_VERSIONS


def test_name_matches_globs():
    assert routing.name_matches("gh__create_issue", [])  # no patterns matches all
    assert routing.name_matches("gh__create_issue", ["gh__*"])  # prefix
    assert routing.name_matches("gh__create_issue", ["*issue"])  # suffix
    assert routing.name_matches("gh__create_issue", ["*create*"])  # contains
    assert routing.name_matches("gh__create_issue", ["*"])  # wildcard
    assert routing.name_matches("gh__create_issue", ["gh__create_issue"])  # exact
    assert not routing.name_matches("gh__search", ["gh__create*"])
    assert not routing.name_matches("gh__search", ["gl__*"])


def test_backend_selected_by_keywords():
    # No filter keywords: always selected.
    assert routing.backend_selected(["git"], [])
    # No backend keywords: always selected (surface unknown).
    assert routing.backend_selected([], ["git"])
    # Intersection selects; disjoint skips.
    assert routing.backend_selected(["git", "vcs"], ["vcs"])
    assert not routing.backend_selected(["git"], ["chat"])


def test_server_card():
    card = routing.server_card()
    assert card["name"] == "yamp"
    assert card["role"] == "intermediary"
    assert card["protocolVersions"] == list(SUPPORTED_PROTOCOL_VERSIONS)
    assert "streamable-http" in card["transports"]
