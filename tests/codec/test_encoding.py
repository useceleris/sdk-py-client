import math
from typing import Any

import pytest

from tests.codec.vectors import ENCODING_VECTORS, INVALID_IDENTIFIER_VECTORS
from useceleris_client._encode import encode_client_command
from useceleris_client._errors import ConfigurationError

MISSING = object()

LIMIT = 2 * 1024 * 1024


def encode(command: object) -> bytes:
    # The encoder takes a validated command type; these tests hand it
    # anything an untyped caller could.
    untyped: Any = command
    return encode_client_command(untyped)


# end function encode


@pytest.mark.parametrize(
    ("command", "expected"),
    [(vector.command, vector.expected) for vector in ENCODING_VECTORS],
    ids=[vector.name for vector in ENCODING_VECTORS],
)
def test_encodes_golden_vector(command: dict[str, Any], expected: bytes) -> None:
    assert encode(command) == expected


# end function test_encodes_golden_vector


@pytest.mark.parametrize(
    "command",
    [
        None,
        {},
        {"command": "NODE_PUB", "segment_id": "s"},
        {"command": "NODE_FUTURE", "segment_id": "s"},
        *(
            {"command": "SUB", "segment_id": segment_id}
            for segment_id in ["", "a\n", "a\r", 1, None]
        ),
        {"command": "SUB"},
        {"command": "PUB", "segment_id": "s", "payload": None},
        {"command": "PUB", "segment_id": "s", "payload": [], "message_id": "m"},
        {"command": "PUB", "segment_id": "s", "payload": "x"},
        {"command": "PUB", "segment_id": "s", "payload": b"x", "message_id": ""},
        {"command": "PUB", "segment_id": "s", "payload": b"x", "message_id": None},
    ],
)
def test_rejects_invalid_command(command: object) -> None:
    with pytest.raises(ConfigurationError):
        encode(command)


# end function test_rejects_invalid_command


def presence_list(**fields: object) -> dict[str, object]:
    command = {
        "command": "PRES_LIST",
        "segment_id": "s",
        "page": 1,
        "per_page": 1,
        "request_id": "1",
    }
    command.update(fields)
    return {key: value for key, value in command.items() if value is not MISSING}


# end function presence_list


@pytest.mark.parametrize(
    "page", [0, -1, 2147483648, 1.5, math.nan, math.inf, True, "1", None, MISSING]
)
def test_rejects_page(page: object) -> None:
    with pytest.raises(ConfigurationError):
        encode(presence_list(page=page))


# end function test_rejects_page


@pytest.mark.parametrize(
    "per_page", [0, -1, 101, 1.5, math.nan, math.inf, True, "1", None, MISSING]
)
def test_rejects_per_page(per_page: object) -> None:
    with pytest.raises(ConfigurationError):
        encode(presence_list(per_page=per_page))


# end function test_rejects_per_page


def test_strips_extras_and_leaves_inputs_untouched() -> None:
    command = {"command": "PUB", "segment_id": "s", "payload": b"x", "extra": "ignored"}
    original = dict(command)

    result = encode(command)

    assert command == original
    assert result == b"@PUB\n$1\ns\n$-1\n$1\nx\n"


# end function test_strips_extras_and_leaves_inputs_untouched


def test_counts_complete_encoded_overhead_at_the_exact_limit() -> None:
    # 2 MiB is the whole encoded command, not the payload: "@PUB\n", "$1\ns\n",
    # "$-1\n", "$2097128\n" and the closing LF are 24 bytes.
    command = {"command": "PUB", "segment_id": "s", "payload": bytes(LIMIT - 24)}

    assert len(encode(command)) == LIMIT

    with pytest.raises(ConfigurationError):
        encode({**command, "payload": bytes(LIMIT - 23)})

    # Characters at the limit, but each one is two UTF-8 bytes.
    with pytest.raises(ConfigurationError):
        encode({"command": "SUB", "segment_id": "é" * LIMIT})


# end function test_counts_complete_encoded_overhead_at_the_exact_limit


def test_names_the_failed_field_without_repeating_the_input() -> None:
    with pytest.raises(ConfigurationError) as caught:
        encode({"command": "SECRET", "segment_id": "synthetic-secret"})

    error = caught.value
    assert error.code == "Configuration"
    assert str(error) == (
        "Invalid command. command: Input should be 'PUB', 'SUB', 'UNSUB', "
        "'PRES_SUB', 'PRES_UNSUB' or 'PRES_LIST'."
    )
    assert error.__cause__ is None
    assert error.__context__ is None
    assert "synthetic-secret" not in repr(error)


# end function test_names_the_failed_field_without_repeating_the_input


def test_names_a_field_of_the_command_layout() -> None:
    with pytest.raises(ConfigurationError) as caught:
        encode({"command": "PUB", "segment_id": "s", "payload": b"", "message_id": ""})

    assert str(caught.value) == "Invalid command. message_id: Must not be empty."


# end function test_names_a_field_of_the_command_layout


@pytest.mark.parametrize("identifier", INVALID_IDENTIFIER_VECTORS)
def test_rejects_ill_formed_identifier(identifier: str) -> None:
    commands: list[dict[str, object]] = [
        {"command": "SUB", "segment_id": identifier},
        {"command": "UNSUB", "segment_id": identifier},
        {"command": "PRES_SUB", "segment_id": identifier},
        {"command": "PRES_UNSUB", "segment_id": identifier},
        presence_list(segment_id=identifier),
        presence_list(request_id=identifier),
        {"command": "PUB", "segment_id": identifier, "payload": b""},
        {"command": "PUB", "segment_id": "s", "message_id": identifier, "payload": b""},
    ]

    for command in commands:
        with pytest.raises(
            ConfigurationError,
            match="Must not contain CR, LF or unpaired UTF-16 surrogates",
        ):
            encode(command)


# end function test_rejects_ill_formed_identifier


def test_isolates_valid_encoding_from_a_previous_oversized_command() -> None:
    with pytest.raises(ConfigurationError, match=r"^Encoded command exceeds 2 MiB\."):
        encode({"command": "PUB", "segment_id": "s", "payload": bytes(LIMIT - 23)})

    assert encode({"command": "SUB", "segment_id": "chat"}) == b"@SUB\n$4\nchat\n"


# end function test_isolates_valid_encoding_from_a_previous_oversized_command
