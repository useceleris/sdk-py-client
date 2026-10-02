import json
import math
from typing import Any

import pytest

from useceleris_client import (
    ConfigurationError,
    create_payload_codec,
    json_payload,
    read_json,
    read_text,
    text_payload,
)


class TestTextPayloads:
    @pytest.mark.parametrize("value", ["", "hello", "héllo 안녕 🛰️"])
    def test_round_trips_byte_identically(self, value: str) -> None:
        payload = text_payload(value)

        assert payload == value.encode()
        assert read_text(payload) == value

    def test_rejects_payloads_that_are_not_valid_utf8(self) -> None:
        # A lone continuation byte cannot start a sequence.
        with pytest.raises(ConfigurationError) as caught:
            read_text(b"\x80")

        assert caught.value.code == "Configuration"
        assert str(caught.value) == (
            "Payload is not valid UTF-8, so it cannot be read as text."
        )
        assert caught.value.__context__ is None

    def test_preserves_a_byte_order_mark(self) -> None:
        assert read_text(b"\xef\xbb\xbfx") == "﻿x"

    def test_rejects_text_with_unpaired_surrogates(self) -> None:
        with pytest.raises(ConfigurationError) as caught:
            text_payload("synthetic-secret\ud800")

        assert str(caught.value) == (
            "Text contains unpaired surrogates, so it cannot be encoded as UTF-8."
        )
        assert caught.value.__context__ is None
        assert "synthetic-secret" not in repr(caught.value)


class TestJsonPayloads:
    def test_round_trips_values_through_the_wire_encoding(self) -> None:
        value = {"id": 7, "text": "héllo", "nested": {"ok": True}, "list": [1, 2]}
        payload = json_payload(value)

        assert read_text(payload) == (
            '{"id":7,"text":"héllo","nested":{"ok":true},"list":[1,2]}'
        )
        assert read_json(payload) == value

    @pytest.mark.parametrize(
        "value", [math.nan, math.inf, -math.inf, {1, 2}, b"bytes", object()]
    )
    def test_rejects_values_json_cannot_represent(self, value: object) -> None:
        with pytest.raises(ConfigurationError) as caught:
            json_payload(value)

        assert str(caught.value) == (
            "Value is not JSON-serializable: it is circular, or contains NaN, an "
            "infinity or a type JSON cannot represent."
        )

    def test_rejects_nesting_too_deep_to_serialize(self) -> None:
        # Deep enough to exhaust the interpreter's recursion protection on
        # every supported version.
        value: Any = []

        for _ in range(1_000_000):
            value = [value]

        with pytest.raises(
            ConfigurationError, match=r"^Value is not JSON-serializable"
        ):
            json_payload(value)

    def test_escapes_unpaired_surrogates_as_json_stringify_does(self) -> None:
        value = {"lone": chr(0xD800), "pair": "😀", "text": chr(0xDFFF) + "x"}

        payload = json_payload(value)

        assert payload == ('{"lone":"\\ud800","pair":"😀","text":"\\udfffx"}'.encode())
        assert read_json(payload) == value

    def test_rejects_circular_structures_without_leaking_the_input(self) -> None:
        circular: dict[str, Any] = {"secret": "synthetic-marker"}
        circular["self"] = circular

        with pytest.raises(ConfigurationError) as caught:
            json_payload(circular)

        assert str(caught.value) == (
            "Value is not JSON-serializable: it is circular, or contains NaN, an "
            "infinity or a type JSON cannot represent."
        )
        assert caught.value.__context__ is None
        assert "synthetic-marker" not in repr(caught.value)

    @pytest.mark.parametrize("text", ["{ not json", "NaN", "[Infinity]", "-Infinity"])
    def test_rejects_payloads_that_are_not_valid_json(self, text: str) -> None:
        with pytest.raises(ConfigurationError) as caught:
            read_json(text.encode())

        assert str(caught.value) == "Payload is valid UTF-8 but not valid JSON."
        assert caught.value.__context__ is None


class TestPayloadCodecs:
    def test_round_trips_through_the_supplied_encoder(self) -> None:
        codec = create_payload_codec(
            encode=lambda value: json.dumps(value).encode(),
            decode=lambda payload: json.loads(payload),
        )

        payload = codec.encode_payload({"body": "hello"})

        assert payload == b'{"body": "hello"}'
        assert codec.read_payload(payload) == {"body": "hello"}

    def test_propagates_the_callers_own_failures_unchanged(self) -> None:
        failure = RuntimeError("synthetic-decoder-failure")

        def fail(payload: bytes) -> str:
            raise failure

        codec = create_payload_codec(encode=lambda value: b"", decode=fail)

        with pytest.raises(RuntimeError) as caught:
            codec.read_payload(b"")

        assert caught.value is failure

    @pytest.mark.parametrize(
        ("encode", "decode"), [(None, lambda payload: payload), (bytes, 1)]
    )
    def test_rejects_functions_that_are_not_callable(
        self, encode: Any, decode: Any
    ) -> None:
        with pytest.raises(ConfigurationError) as caught:
            create_payload_codec(encode=encode, decode=decode)

        assert str(caught.value) == "Codec must provide encode and decode functions."
