from yamp import namespace


def test_valid_backend_id():
    assert namespace.valid_backend_id("gh")
    assert namespace.valid_backend_id("gh-1")
    assert namespace.valid_backend_id("gh_1")  # single underscore is fine
    assert not namespace.valid_backend_id("gh!")
    assert not namespace.valid_backend_id("gh.x")
    assert not namespace.valid_backend_id("gh/x")
    assert not namespace.valid_backend_id("")


def test_backend_id_with_delimiter_rejected():
    # Regression: an id containing the "__" delimiter would break reverse
    # resolution (prefix "a__b"+"__"+"tool" splits back to backend "a").
    assert not namespace.valid_backend_id("a__b")
    assert namespace.split(namespace.prefix("a__b", "tool")) == ("a", "b__tool")


def test_prefix():
    assert namespace.prefix("gh", "create_issue") == "gh__create_issue"


def test_split_round_trips_prefix():
    assert namespace.split("gh__create_issue") == ("gh", "create_issue")


def test_split_uses_first_delimiter_only():
    # An original name that itself contains the delimiter must survive.
    assert namespace.prefix("gh", "a__b") == "gh__a__b"
    assert namespace.split("gh__a__b") == ("gh", "a__b")


def test_split_rejects_unresolvable_names():
    assert namespace.split("nodelimiter") is None
    assert namespace.split("__missing_backend") is None
    assert namespace.split("gh__") is None
