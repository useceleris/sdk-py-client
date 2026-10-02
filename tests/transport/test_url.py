from urllib.parse import parse_qs, urlsplit

import pytest

from useceleris_client._connection_url import create_credential_url, validate_base_url
from useceleris_client._credential_types import Credentials
from useceleris_client._credentials import get_safe_parsed_connection_configuration
from useceleris_client._errors import ConfigurationError


@pytest.mark.parametrize(
    "url",
    [
        "ws://example.test",
        "https://example.test",
        "wss://user:pass@example.test",
        "wss://user@example.test",
        "wss://example.test?",
        "wss://example.test#",
        "not a url",
        "wss://example.test:port",
        "wss://:443",
        "wss://[::1",
    ],
)
def test_rejects_unsafe_url(url: str) -> None:
    with pytest.raises(ConfigurationError, match=r"^Invalid connection URL\. "):
        validate_base_url(url, True)


def test_does_not_chain_the_parse_error() -> None:
    with pytest.raises(ConfigurationError) as caught:
        validate_base_url("wss://example.test:synthetic-secret", True)

    assert caught.value.__context__ is None
    assert "synthetic-secret" not in repr(caught.value)


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "127.1.2.3", "[::1]"])
def test_requires_explicit_opt_in_for_loopback(host: str) -> None:
    with pytest.raises(ConfigurationError):
        validate_base_url(f"ws://{host}", False)

    assert validate_base_url(f"ws://{host}", True).scheme == "ws"


@pytest.mark.parametrize(
    "host", ["localhost.evil.test", "128.0.0.1", "0.0.0.0", "[::]"]
)
def test_rejects_non_loopback_insecure_hosts(host: str) -> None:
    with pytest.raises(ConfigurationError):
        validate_base_url(f"ws://{host}", True)


@pytest.mark.parametrize("reference", ["", "x:y", "a\n", "é", "x" * 256])
def test_rejects_invalid_channel_reference(reference: str) -> None:
    with pytest.raises(
        ConfigurationError, match=r"^Invalid connection configuration\."
    ):
        get_safe_parsed_connection_configuration(
            {"base_url": "wss://example.test", "channel_reference": reference}
        )


@pytest.mark.parametrize("reference", ["a", "a" * 255, "Room_1-A"])
def test_accepts_a_valid_channel_reference(reference: str) -> None:
    configuration = get_safe_parsed_connection_configuration(
        {"base_url": "wss://example.test", "channel_reference": reference}
    )

    assert configuration["channel_reference"] == reference


@pytest.mark.parametrize(
    "base",
    ["wss://example.test", "wss://example.test/", "wss://example.test/prefix///"],
)
def test_preserves_path_and_opaque_query_values(base: str) -> None:
    original = validate_base_url(base, False)
    result = urlsplit(
        create_credential_url(
            original, "a" * 255, Credentials(payload="+/%=&識", signature="%2B")
        )
    )

    assert result.path == f"{original.path.rstrip('/')}/channel/{'a' * 255}"
    assert parse_qs(result.query) == {"payload": ["+/%=&識"], "signature": ["%2B"]}
    assert original.query == ""
