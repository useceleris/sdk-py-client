import contextlib
from typing import Any

import pytest

from tests.codec.vectors import DECODING_VECTORS, MALFORMED_VECTORS
from useceleris_client._decode import decode_server_message
from useceleris_client._errors import ProtocolError
from useceleris_client._messages import ArrayFrame, MessageFrame, ServerMessage


@pytest.mark.parametrize(
    ("data", "expected"),
    [(vector.data, vector.expected) for vector in DECODING_VECTORS],
    ids=[vector.name for vector in DECODING_VECTORS],
)
def test_decodes_golden_vector(data: bytes, expected: ServerMessage) -> None:
    assert decode_server_message(data) == expected


@pytest.mark.parametrize("data", MALFORMED_VECTORS)
def test_rejects_malformed_message(data: bytes) -> None:
    with pytest.raises(ProtocolError):
        decode_server_message(data)


def test_rejects_every_truncation_of_a_fixed_peer_message() -> None:
    data = DECODING_VECTORS[0].data

    for length in range(len(data)):
        with pytest.raises(ProtocolError):
            decode_server_message(data[:length])


def test_bounds_nesting_and_counts_fields_as_fragments() -> None:
    assert isinstance(decode_server_message(b"*1\n" * 31 + b"*0\n"), ArrayFrame)

    with pytest.raises(ProtocolError):
        decode_server_message(b"*1\n" * 32 + b"*0\n")

    assert isinstance(decode_server_message(b"*4095\n" + b"*0\n" * 4095), ArrayFrame)

    with pytest.raises(ProtocolError):
        decode_server_message(b"*1366\n" + b"@SERVER_MSG\n:1\n$0\n\n" * 1366)


def test_decodes_messages_larger_than_one_mebibyte() -> None:
    # LIMIT-01. A prime-plan delivery: a full 1024 KiB payload plus framing.
    payload_length = 1024 * 1024
    data = (
        f"@MSG\n+user\n+chat\n+msg_1\n:1\n${payload_length}\n".encode()
        + bytes(payload_length)
        + b"\n"
    )

    message = decode_server_message(data)

    assert len(data) > 1024 * 1024
    assert isinstance(message, MessageFrame)
    assert message.segment_id == "chat"
    assert len(message.payload) == payload_length


def test_rejects_non_byte_input_with_a_safe_error() -> None:
    untyped: Any = None

    with pytest.raises(ProtocolError):
        decode_server_message(untyped)

    with pytest.raises(ProtocolError) as caught:
        decode_server_message(b"synthetic-secret")

    error = caught.value
    assert error.code == "ProtocolError"
    assert str(error) == (
        "Unexpected server message marker. Field: message, byte offset 0."
    )
    assert (error.field, error.offset) == ("message", 0)
    assert error.__cause__ is None
    assert error.__context__ is None
    assert "synthetic-secret" not in repr(error)


def test_handles_deterministic_mutated_inputs_without_native_exceptions() -> None:
    seed = 0xCE1E

    for attempt in range(512):
        data = bytearray(DECODING_VECTORS[attempt % len(DECODING_VECTORS)].data)
        seed = (seed * 1664525 + 1013904223) & 0xFFFFFFFF
        data[seed % len(data)] = seed & 255

        with contextlib.suppress(ProtocolError):
            decode_server_message(bytes(data))
