import asyncio
import json

import pytest

from tests.live.helpers import (
    GENERATED_MESSAGE_ID,
    connected_channel,
    next_message,
    unique_channel_reference,
)
from useceleris_client import (
    create_payload_codec,
    json_payload,
    read_json,
    read_text,
    text_payload,
)

pytestmark = pytest.mark.live

# Payloads are opaque bytes to the SDK and the server: whatever a caller
# encodes is what the peer decodes. These vectors are hand-encoded so the
# suite needs no serializer dependency, and each is decoded on arrival so the
# vector proves itself rather than merely matching a copy of itself.

# protobuf wire format:
#   field 1 (varint)           = 150
#   field 2 (length-delimited) = "안녕 celeris"
#   field 3 (embedded message) = { field 1 (varint) = 1 }
PROTOBUF_VECTOR = bytes.fromhex(
    "08 96 01 12 0e ec 95 88 eb 85 95 20 63 65 6c 65 72 69 73 1a 02 08 01"
)

# MessagePack: fixmap(3) { "id": 7, "bin": bin8 <00 ff 10>, "txt": "héllo" }
MESSAGE_PACK_VECTOR = bytes.fromhex(
    "83 a2 69 64 07 a3 62 69 6e c4 03 00 ff 10 a3 74 78 74 a6 68 c3 a9 6c 6c 6f"
)

JSON_VECTOR = json.dumps(
    {"id": 7, "txt": "héllo 안녕", "nested": {"ok": True}},
    ensure_ascii=False,
    separators=(",", ":"),
).encode()


def read_varint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0

    while True:
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift

        if byte & 0x80 == 0:
            return value, offset

        shift += 7


# end function read_varint


@pytest.mark.parametrize(
    ("label", "vector"),
    [
        ("json", JSON_VECTOR),
        ("messagepack", MESSAGE_PACK_VECTOR),
        ("protobuf", PROTOBUF_VECTOR),
    ],
)
async def test_round_trips_a_payload_byte_identically(
    label: str, vector: bytes
) -> None:
    reference = unique_channel_reference(f"fmt-{label}")
    publisher = await connected_channel(reference)
    receiver = await connected_channel(reference)
    receiver.segment("formats").subscribe()
    await asyncio.sleep(1.5)

    await publisher.segment("formats").publish(vector)
    message = await next_message(
        receiver.segment("formats"), lambda message: True, f"the {label} delivery", 20
    )

    assert message.payload == vector
    assert GENERATED_MESSAGE_ID.fullmatch(message.metadata.message_id)
    await publisher.close()
    await receiver.close()


# end function test_round_trips_a_payload_byte_identically


async def test_delivers_payloads_that_decode_to_their_intended_values() -> None:
    reference = unique_channel_reference("fmt-decode")
    publisher = await connected_channel(reference)
    receiver = await connected_channel(reference)
    received: dict[int, bytes] = {}
    receiver.segment("formats").on_message(
        lambda payload, metadata: received.__setitem__(len(payload), payload)
    )
    receiver.segment("formats").subscribe()
    await asyncio.sleep(1.5)

    for vector in [JSON_VECTOR, MESSAGE_PACK_VECTOR, PROTOBUF_VECTOR]:
        await publisher.segment("formats").publish(vector)

    await next_message(
        receiver.segment("formats"),
        lambda message: len(received) >= 3,
        "all three format deliveries",
        20,
    )

    assert json.loads(received[len(JSON_VECTOR)]) == {
        "id": 7,
        "txt": "héllo 안녕",
        "nested": {"ok": True},
    }

    # MessagePack: fixmap header, then the bin8 field's exact bytes.
    message_pack = received[len(MESSAGE_PACK_VECTOR)]
    assert message_pack[0] & 0xF0 == 0x80
    binary_start = message_pack.index(0xC4)
    assert message_pack[binary_start : binary_start + 5] == bytes(
        [0xC4, 0x03, 0x00, 0xFF, 0x10]
    )

    # protobuf: field 1 is a varint carrying 150, field 2 is the string.
    protobuf = received[len(PROTOBUF_VECTOR)]
    assert protobuf[0] == 0x08
    field_one, after_field_one = read_varint(protobuf, 1)
    assert field_one == 150
    assert protobuf[after_field_one] == 0x12
    length, after_length = read_varint(protobuf, after_field_one + 1)
    assert protobuf[after_length : after_length + length].decode() == "안녕 celeris"

    await publisher.close()
    await receiver.close()


# end function test_delivers_payloads_that_decode_to_their_intended_values


async def test_carries_helper_and_codec_payloads_through_the_live_server() -> None:
    reference = unique_channel_reference("fmt-helpers")
    publisher = await connected_channel(reference)
    receiver = await connected_channel(reference)
    # Deliveries from one origin keep their order, so arrival order is the
    # publish order.
    received: list[bytes] = []
    receiver.segment("formats").on_message(
        lambda payload, metadata: received.append(payload)
    )
    receiver.segment("formats").subscribe()
    await asyncio.sleep(1.5)

    # The codec wraps an arbitrary serializer; here the protobuf vector.
    vector_codec = create_payload_codec(
        encode=lambda value: PROTOBUF_VECTOR,
        decode=lambda payload: {"marker": payload[0]},
    )

    await publisher.segment("formats").publish(text_payload("hi"))
    await publisher.segment("formats").publish(json_payload({"ok": True}))
    await publisher.segment("formats").publish(
        vector_codec.encode_payload({"marker": 8})
    )

    await next_message(
        receiver.segment("formats"),
        lambda message: len(received) >= 3,
        "the three helper deliveries",
        20,
    )

    assert read_text(received[0]) == "hi"
    assert read_json(received[1]) == {"ok": True}
    assert vector_codec.read_payload(received[2]) == {"marker": 8}
    assert received[2] == PROTOBUF_VECTOR
    await publisher.close()
    await receiver.close()


# end function test_carries_helper_and_codec_payloads_through_the_live_server
