from yamp import version


def test_supported_set_is_newest_first():
    assert version.SUPPORTED_PROTOCOL_VERSIONS[0] == version.STATELESS_PROTOCOL_VERSION
    assert version.STATEFUL_PROTOCOL_VERSION in version.SUPPORTED_PROTOCOL_VERSIONS
    assert version.is_supported(version.STATELESS_PROTOCOL_VERSION)
    assert version.is_supported(version.STATEFUL_PROTOCOL_VERSION)
    assert not version.is_supported("1999-01-01")


def test_negotiate_defaults_when_omitted():
    assert version.negotiate(None) == version.STATELESS_PROTOCOL_VERSION
    assert version.negotiate(None, default=version.STATEFUL_PROTOCOL_VERSION) == (
        version.STATEFUL_PROTOCOL_VERSION
    )


def test_negotiate_echoes_supported():
    for supported in version.SUPPORTED_PROTOCOL_VERSIONS:
        assert version.negotiate(supported) == supported


def test_negotiate_rejects_unsupported():
    assert version.negotiate("2024-11-05") is None
    assert version.negotiate("") is None


def test_unsupported_error_data_names_supported_set():
    data = version.unsupported_error_data("2024-11-05")
    assert data == {
        "requested": "2024-11-05",
        "supported": list(version.SUPPORTED_PROTOCOL_VERSIONS),
    }
