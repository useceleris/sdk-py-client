import pytest

from useceleris_client._decode import decode_server_message
from useceleris_client._errors import ProtocolError
from useceleris_client._messages import NoticeFrame


def located(message: str, field: str, offset: int) -> str:
    return f"{message} Field: {field}, byte offset {offset}."


# end function located


@pytest.mark.parametrize(
    ("wire", "message", "field", "offset"),
    [
        (b"", "Missing field marker.", "message", 0),
        (b"?", "Unexpected server message marker.", "message", 0),
        (b"*0\n!", "Trailing data after server message.", "message", 3),
        (
            b"*2\n@UNKNOWN\n@SERVER_MSG\n:1\n$0\n\n",
            "Unknown command inside array has ambiguous boundaries.",
            "command",
            3,
        ),
        (b"@SERVER_MSG\n:abc\n", "Expected decimal digits.", "timestamp", 12),
        (b"@SERVER_MSG\n:+1\n", "Expected decimal digits.", "timestamp", 12),
        (
            b"@SERVER_MSG\n:9223372036854775808\n",
            "Integer is outside -9223372036854775808 to 9223372036854775807.",
            "timestamp",
            12,
        ),
        (
            b"@SERVER_MSG\n:000000000000000000000\n",
            "Line exceeds its 20-byte limit.",
            "timestamp",
            12,
        ),
        (b"@SERVER_MSG\n:1", "Unterminated line.", "timestamp", 12),
        (b"@SERVER_MSG\n+1\n", "Expected Integer64 marker.", "timestamp", 12),
        (
            b"@PRES_LIST_RESPONSE\n+s\n$1\n1\n;2147483648\n",
            "Integer is outside -2147483648 to 2147483647.",
            "total",
            28,
        ),
        (
            b"@PRES_LIST_RESPONSE\n+s\n$1\n1\n;000000000001\n",
            "Line exceeds its 11-byte limit.",
            "total",
            28,
        ),
        (
            b"@PRES_LIST_RESPONSE\n+s\n$1\n1\n:1\n",
            "Expected Integer32 marker.",
            "total",
            28,
        ),
        (
            b"@SERVER_MSG\n:1\n$9\nx\n",
            "Bulk payload exceeds remaining message bytes.",
            "payload",
            15,
        ),
        (b"@SERVER_MSG\n:1\n$1\nx!", "Missing bulk byte terminator.", "payload", 15),
        (b"@SERVER_MSG\n:1\n$-2\n", "Invalid bulk byte length.", "payload", 15),
        (b"@SERVER_MSG\n:1\n$-1\n", "Payload cannot be null.", "payload", 15),
        (
            b"@SERVER_MSG\n:1\n*0\n",
            "Expected simple or bulk byte marker.",
            "payload",
            15,
        ),
        (
            b"@MSG\n+u\n+\n",
            "Identifier must be nonempty and CR/LF-free.",
            "segment_id",
            8,
        ),
        (b"@MSG\n$-1\n", "Identifier cannot be null.", "token_reference", 5),
        (b"*-1\n", "Array length cannot be negative.", "messages", 0),
        (b"*4096\n", "Array length exceeds the 4096-fragment budget.", "messages", 0),
        (b"-Bad\n", "Invalid error header.", "error", 0),
        (
            b"-Err\n+Bad Name\n$-1\n$6\nsecret\n$-1\n",
            "Invalid error name.",
            "error_type",
            5,
        ),
        (
            b"-Err\nParserError\nsecret",
            "Expected simple string marker.",
            "error_type",
            5,
        ),
        (
            b"-Err\n+ParserError\n$3\nSUB\n$6\nsecret\n$-1\n",
            "Sub type must be a simple string or null.",
            "error_sub_type",
            18,
        ),
        (
            b"-Err\n+ParserError\n$-1\n$-1\n$-1\n",
            "Payload cannot be null.",
            "error_message",
            22,
        ),
        (
            b"-Err\n+ParserError\n$-1\n$6\nsecret\n@X\n",
            "Unexpected resource marker.",
            "resource",
            32,
        ),
    ],
)
def test_reports_field_and_offset(
    wire: bytes, message: str, field: str, offset: int
) -> None:
    with pytest.raises(ProtocolError) as caught:
        decode_server_message(wire)

    error = caught.value
    assert error.code == "ProtocolError"
    assert (error.field, error.offset) == (field, offset)
    assert str(error) == located(message, field, offset)


# end function test_reports_field_and_offset


@pytest.mark.parametrize("flag", ["2", "-1"])
def test_rejects_a_presence_flag_other_than_join_or_leave(flag: str) -> None:
    with pytest.raises(ProtocolError) as caught:
        decode_server_message(f"@PRES_NOTIFY\n+s\n+u\n+c\n;{flag}\n:1\n".encode())

    assert str(caught.value) == located("Presence event must be 0 or 1.", "event", 22)
    assert (caught.value.field, caught.value.offset) == ("event", 22)


# end function test_rejects_a_presence_flag_other_than_join_or_leave


@pytest.mark.parametrize(
    "text",
    [
        "",
        " ",
        "0x10",
        "0o10",
        "0b10",
        "+1",
        "1 ",
        "\t1",
        "1\r\r",
        "--1",
        "1.5",
        "1e2",
        # Spellings int() accepts that the protocol does not.
        "1_000",
        "\u0661",
        "\uff11",
    ],
)
def test_rejects_non_protocol_numeric_text(text: str) -> None:
    with pytest.raises(ProtocolError):
        decode_server_message(f"@SERVER_MSG\n:{text}\n$0\n\n".encode())


# end function test_rejects_non_protocol_numeric_text


@pytest.mark.parametrize("text", ["0001", "-0", "-0001"])
def test_preserves_accepted_decimal_spelling(text: str) -> None:
    decoded = decode_server_message(f"@SERVER_MSG\n:{text}\n$0\n\n".encode())

    assert decoded == NoticeFrame(int(text), b"")


# end function test_preserves_accepted_decimal_spelling


def test_reports_the_field_start_for_invalid_utf8_without_retaining_input() -> None:
    with pytest.raises(ProtocolError) as caught:
        decode_server_message(b"@MSG\n+\xffsynthetic-secret\n")

    error = caught.value
    assert str(error) == located("Invalid UTF-8 text.", "token_reference", 5)
    assert (error.field, error.offset) == ("token_reference", 5)
    assert error.__cause__ is None
    assert error.__context__ is None
    assert "synthetic-secret" not in repr(error)


# end function test_reports_the_field_start_for_invalid_utf8_without_retaining_input


def test_reports_array_resource_limits_at_their_start() -> None:
    with pytest.raises(ProtocolError) as caught:
        decode_server_message(b"*1\n" * 32 + b"*0\n")

    assert str(caught.value) == located(
        "Arrays are nested deeper than 32 levels.", "messages", 96
    )
    assert (caught.value.field, caught.value.offset) == ("messages", 96)


# end function test_reports_array_resource_limits_at_their_start


def test_reports_a_bulk_that_fills_the_message_without_its_terminator() -> None:
    with pytest.raises(ProtocolError) as caught:
        decode_server_message(b"@SERVER_MSG\n:1\n$1\nx")

    assert str(caught.value) == located("Missing bulk byte terminator.", "payload", 15)


# end function test_reports_a_bulk_that_fills_the_message_without_its_terminator


@pytest.mark.parametrize("name", ["_X", "1X"])
def test_requires_an_error_name_to_start_with_a_letter(name: str) -> None:
    with pytest.raises(ProtocolError) as caught:
        decode_server_message(f"-Err\n+{name}\n$-1\n$0\n\n$-1\n".encode())

    assert str(caught.value) == located("Invalid error name.", "error_type", 5)


# end function test_requires_an_error_name_to_start_with_a_letter


def test_treats_only_minus_one_as_a_null_sub_type() -> None:
    with pytest.raises(ProtocolError) as caught:
        decode_server_message(b"-Err\n+X\n$-2\n$0\n\n$-1\n")

    assert str(caught.value) == located(
        "Sub type must be a simple string or null.", "error_sub_type", 8
    )


# end function test_treats_only_minus_one_as_a_null_sub_type


def test_counts_resource_arrays_against_the_depth_limit() -> None:
    header = b"-Err\n+X\n$-1\n$0\n\n"

    decode_server_message(header + b"*1\n" * 31 + b"$-1\n")

    with pytest.raises(ProtocolError, match=r"^Arrays are nested deeper than 32"):
        decode_server_message(header + b"*1\n" * 32 + b"$-1\n")


# end function test_counts_resource_arrays_against_the_depth_limit


@pytest.mark.parametrize(
    ("wire", "limit", "field"),
    [
        (b"@" + b"A" * 19 + b"\n", 18, "command"),
        (b"-Err\n+" + b"A" * 65 + b"\n$-1\n$0\n\n$-1\n", 64, "error_type"),
    ],
)
def test_bounds_command_and_error_names(wire: bytes, limit: int, field: str) -> None:
    with pytest.raises(ProtocolError) as caught:
        decode_server_message(wire)

    assert str(caught.value).startswith(f"Line exceeds its {limit}-byte limit.")
    assert caught.value.field == field


# end function test_bounds_command_and_error_names
