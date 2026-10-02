import contextlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from useceleris_client._errors import ConfigurationError

# Payloads are opaque bytes on the wire. These helpers cover the two encodings
# applications reach for first; every other format goes through
# create_payload_codec, which keeps serializer libraries out of this package.

Value = TypeVar("Value")

_SURROGATE = re.compile("[\ud800-\udfff]")


def text_payload(value: str) -> bytes:
    with contextlib.suppress(UnicodeEncodeError):
        return value.encode()

    # Raised once the encode error, which holds the text, is suppressed.
    raise ConfigurationError(
        "Text contains unpaired surrogates, so it cannot be encoded as UTF-8."
    )


def json_payload(value: object) -> bytes:
    serialized: str | None

    try:
        # Compact, as JSON.stringify writes it. NaN and infinities, which
        # JSON cannot hold, are refused rather than written as null.
        serialized = json.dumps(
            value, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        )
    except (TypeError, ValueError, RecursionError):
        serialized = None

    if serialized is None:
        raise ConfigurationError(
            "Value is not JSON-serializable: it is circular, or contains NaN, an "
            "infinity or a type JSON cannot represent."
        )

    # An unpaired surrogate can only sit inside a JSON string, so it is written
    # as an escape, as JSON.stringify writes it; the text stays well-formed.
    return text_payload(
        _SURROGATE.sub(lambda match: f"\\u{ord(match.group()):04x}", serialized)
    )


def read_text(payload: bytes) -> str:
    with contextlib.suppress(UnicodeDecodeError):
        return payload.decode()

    raise ConfigurationError(
        "Payload is not valid UTF-8, so it cannot be read as text."
    )


def _reject_constant(constant: str) -> Any:
    raise ValueError("Not JSON")


# Returns Any: a payload from a peer you do not control should be checked
# against a schema before it is trusted.
def read_json(payload: bytes) -> Any:
    text = read_text(payload)

    # JSON has no NaN or infinities, though json.loads accepts them.
    with contextlib.suppress(ValueError):
        return json.loads(text, parse_constant=_reject_constant)

    # json's own message quotes the text, so it is not passed on.
    raise ConfigurationError("Payload is valid UTF-8 but not valid JSON.")


@dataclass(frozen=True)
class PayloadCodec(Generic[Value]):
    encode_payload: Callable[[Value], bytes]
    read_payload: Callable[[bytes], Value]


# Bring your own serializer: protobuf, MessagePack, CBOR, Avro, anything.
# Failures from the supplied functions propagate unchanged: they are the
# caller's errors, not this package's.
def create_payload_codec(
    *, encode: Callable[[Value], bytes], decode: Callable[[bytes], Value]
) -> PayloadCodec[Value]:
    if not callable(encode) or not callable(decode):
        raise ConfigurationError("Codec must provide encode and decode functions.")

    return PayloadCodec(encode_payload=encode, read_payload=decode)
