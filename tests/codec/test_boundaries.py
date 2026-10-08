import pytest

from useceleris_client._decode import decode_server_message
from useceleris_client._errors import ProtocolError
from useceleris_client._messages import ErrorFrame, NoticeFrame


@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_accepts_maximum_length_numeric_and_error_name_headers(newline: str) -> None:
    notice = decode_server_message(
        f"@SERVER_MSG{newline}:{'0' * 19}1{newline}$0{newline}{newline}".encode()
    )
    error = decode_server_message(
        (
            f"-Err{newline}+{'A' * 64}{newline}+{'B' * 64}{newline}"
            f"$1{newline}m{newline}$-1{newline}"
        ).encode()
    )

    assert notice == NoticeFrame(1, b"")
    assert isinstance(error, ErrorFrame)
    assert (error.type, error.sub_type) == ("A" * 64, "B" * 64)


# end function test_accepts_maximum_length_numeric_and_error_name_headers


@pytest.mark.parametrize(
    ("header", "reason"),
    [
        ("0" * 20, "Unterminated line. Field: timestamp, byte offset 12."),
        ("0" * 21, "Unterminated line. Field: timestamp, byte offset 12."),
        (
            "0" * 22,
            "Line exceeds its 20-byte limit. Field: timestamp, byte offset 12.",
        ),
        (
            "0" * 21 + "\n",
            "Line exceeds its 20-byte limit. Field: timestamp, byte offset 12.",
        ),
        (
            "0" * 21 + "\r\n",
            "Line exceeds its 20-byte limit. Field: timestamp, byte offset 12.",
        ),
    ],
)
def test_preserves_bounded_header_failure(header: str, reason: str) -> None:
    with pytest.raises(ProtocolError) as caught:
        decode_server_message(f"@SERVER_MSG\n:{header}".encode())

    assert str(caught.value) == reason
    assert (caught.value.field, caught.value.offset) == ("timestamp", 12)


# end function test_preserves_bounded_header_failure


def test_copies_binary_payloads_containing_newline_and_marker_bytes() -> None:
    pattern = b"\n\r@$*:+-"
    payload = (pattern * (65536 // len(pattern) + 1))[:65536]

    message = decode_server_message(b"@SERVER_MSG\n:1\n$65536\n" + payload + b"\n")

    assert message == NoticeFrame(1, payload)


# end function test_copies_binary_payloads_containing_newline_and_marker_bytes


def test_rejects_a_long_malformed_header_without_searching_the_rest() -> None:
    with pytest.raises(ProtocolError) as caught:
        decode_server_message(b"@SERVER_MSG\n:" + b"0" * 65536 + b"\n")

    assert str(caught.value) == (
        "Line exceeds its 20-byte limit. Field: timestamp, byte offset 12."
    )
    assert (caught.value.field, caught.value.offset) == ("timestamp", 12)


# end function test_rejects_a_long_malformed_header_without_searching_the_rest
