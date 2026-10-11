import re
from importlib.metadata import version

from useceleris_client._constants import SDK_LANGUAGE, SDK_VERSION
from useceleris_client._encode import encode_client_command
from useceleris_client._message_id import generate_message_id, message_id_prefix


def test_generated_ids_name_the_sdk_and_its_version_then_32_hex_digits() -> None:
    identifier = generate_message_id()
    prefix = message_id_prefix("py", SDK_VERSION)

    assert identifier.startswith(prefix), identifier
    assert re.fullmatch(r"[0-9a-f]{32}", identifier[len(prefix) :]), identifier


# end function test_generated_ids_name_the_sdk_and_its_version_then_32_hex_digits


def test_generated_ids_hold_only_letters_digits_dots_and_underscores() -> None:
    for _ in range(100):
        assert re.fullmatch(r"[A-Za-z0-9._]+", generate_message_id())


# end function test_generated_ids_hold_only_letters_digits_dots_and_underscores


def test_generated_ids_are_different_on_every_call() -> None:
    assert len({generate_message_id() for _ in range(1_000)}) == 1_000


# end function test_generated_ids_are_different_on_every_call


def test_the_version_is_the_installed_distributions() -> None:
    assert SDK_LANGUAGE == "py"
    assert version("useceleris-client") == SDK_VERSION


# end function test_the_version_is_the_installed_distributions


def test_every_other_version_character_becomes_an_underscore() -> None:
    assert message_id_prefix("py", "1.2.0-beta.1") == "msg__py_v1.2.0_beta.1__"
    assert message_id_prefix("py", "1.2.0+build/7") == "msg__py_v1.2.0_build_7__"
    assert message_id_prefix("py", "1.0.0") == "msg__py_v1.0.0__"


# end function test_every_other_version_character_becomes_an_underscore


def test_a_generated_id_is_sent_as_a_bulk_string() -> None:
    # Client commands write identifiers as bulk strings: "$", the length, LF,
    # the bytes, LF. The server reads them without a scan for the end.
    identifier = generate_message_id()
    frame = encode_client_command(
        {
            "command": "PUB",
            "segment_id": "chat",
            "payload": b"hi",
            "message_id": identifier,
        }
    )

    assert (
        frame == f"@PUB\n$4\nchat\n${len(identifier)}\n{identifier}\n$2\nhi\n".encode()
    )


# end function test_a_generated_id_is_sent_as_a_bulk_string
