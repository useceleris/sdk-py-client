import contextlib
import re

from useceleris_client._constants import (
    MAXIMUM_COMMAND_NAME_BYTES,
    MAXIMUM_DEPTH,
    MAXIMUM_ERROR_NAME_BYTES,
    MAXIMUM_FRAGMENTS,
    MAXIMUM_INTEGER32,
    MAXIMUM_INTEGER32_LINE_BYTES,
    MAXIMUM_INTEGER64,
    MAXIMUM_INTEGER64_LINE_BYTES,
    MINIMUM_INTEGER32,
    MINIMUM_INTEGER64,
)
from useceleris_client._errors import ProtocolError, ServerErrorResource
from useceleris_client._messages import (
    ArrayFrame,
    ErrorFrame,
    IgnoredFrame,
    MessageFrame,
    NoticeFrame,
    PresenceConnection,
    PresenceListFrame,
    PresenceNotifyFrame,
    ServerMessage,
)

# int() also accepts underscores, Unicode digits and surrounding whitespace,
# none of which the protocol allows, so the grammar is checked first.
_DECIMAL = re.compile("-?[0-9]+")

_ERROR_NAME = re.compile("[A-Za-z][A-Za-z0-9_]*")


class MessageDecoder:
    def __init__(self, data: bytes) -> None:
        self._bytes = data
        self._offset = 0
        self._fragments = 0

    # end method __init__

    def decode(self) -> ServerMessage:
        message = self._read_message(0, True)

        if self._offset != len(self._bytes):
            raise ProtocolError(
                "Trailing data after server message.", "message", self._offset
            )

        return message

    # end method decode

    # Returns the marker as a one-byte bytes object, so callers match it
    # against the protocol character itself.
    def _read_marker(self, field: str) -> bytes:
        field_start_offset = self._offset
        self._fragments += 1

        if self._fragments > MAXIMUM_FRAGMENTS:
            raise ProtocolError(
                f"Server message has more than {MAXIMUM_FRAGMENTS} fragments.",
                field,
                field_start_offset,
            )

        if self._offset >= len(self._bytes):
            raise ProtocolError("Missing field marker.", field, field_start_offset)

        self._offset += 1
        return self._bytes[field_start_offset : self._offset]

    # end method _read_marker

    def _read_line(
        self, field: str, field_start_offset: int, maximum_length: int | None = None
    ) -> bytes:
        if maximum_length is None:
            maximum_length = len(self._bytes)

        line_start = self._offset
        # The content, one CR and the LF: no further byte can end the line
        # within its limit.
        search_end = min(len(self._bytes), line_start + maximum_length + 2)
        newline = self._bytes.find(b"\n", line_start, search_end)

        if newline < 0:
            if search_end - line_start == maximum_length + 2:
                raise ProtocolError(
                    f"Line exceeds its {maximum_length}-byte limit.",
                    field,
                    field_start_offset,
                )

            raise ProtocolError("Unterminated line.", field, field_start_offset)

        self._offset = newline + 1
        content_end = newline

        if self._bytes[content_end - 1 : content_end] == b"\r":
            content_end -= 1

        if content_end - line_start > maximum_length:
            raise ProtocolError(
                f"Line exceeds its {maximum_length}-byte limit.",
                field,
                field_start_offset,
            )

        return self._bytes[line_start:content_end]

    # end method _read_line

    def _read_text(self, data: bytes, field: str, field_start_offset: int) -> str:
        with contextlib.suppress(UnicodeDecodeError):
            return data.decode()

        # Raised once the decode error, which holds the received bytes, is
        # suppressed, so it is not chained to this one.
        raise ProtocolError("Invalid UTF-8 text.", field, field_start_offset)

    # end method _read_text

    # The decimal grammar every numeric field shares. The range defaults to
    # signed 64-bit, which also bounds bulk and array lengths.
    def _read_decimal(
        self,
        field: str,
        field_start_offset: int,
        maximum_line_bytes: int = MAXIMUM_INTEGER64_LINE_BYTES,
        minimum: int = MINIMUM_INTEGER64,
        maximum: int = MAXIMUM_INTEGER64,
    ) -> int:
        text = self._read_text(
            self._read_line(field, field_start_offset, maximum_line_bytes),
            field,
            field_start_offset,
        )

        if _DECIMAL.fullmatch(text) is None:
            raise ProtocolError("Expected decimal digits.", field, field_start_offset)

        value = int(text)

        if value < minimum or value > maximum:
            raise ProtocolError(
                f"Integer is outside {minimum} to {maximum}.",
                field,
                field_start_offset,
            )

        return value

    # end method _read_decimal

    def _read_integer64(self, field: str) -> int:
        field_start_offset = self._offset

        if self._read_marker(field) != b":":
            raise ProtocolError("Expected Integer64 marker.", field, field_start_offset)

        return self._read_decimal(field, field_start_offset)

    # end method _read_integer64

    def _read_integer32(self, field: str) -> int:
        field_start_offset = self._offset

        if self._read_marker(field) != b";":
            raise ProtocolError("Expected Integer32 marker.", field, field_start_offset)

        return self._read_integer32_digits(field, field_start_offset)

    # end method _read_integer32

    def _read_integer32_digits(self, field: str, field_start_offset: int) -> int:
        return self._read_decimal(
            field,
            field_start_offset,
            MAXIMUM_INTEGER32_LINE_BYTES,
            MINIMUM_INTEGER32,
            MAXIMUM_INTEGER32,
        )

    # end method _read_integer32_digits

    def _read_bytes(self, field: str) -> bytes | None:
        field_start_offset = self._offset

        match self._read_marker(field):
            case b"+":
                return self._read_line(field, field_start_offset)
            case b"$":
                return self._read_bulk_bytes(field, field_start_offset)
            case _:
                raise ProtocolError(
                    "Expected simple or bulk byte marker.", field, field_start_offset
                )

    # end method _read_bytes

    def _read_bulk_bytes(self, field: str, field_start_offset: int) -> bytes | None:
        length = self._read_decimal(field, field_start_offset)

        if length == -1:
            return None

        if length < 0:
            raise ProtocolError("Invalid bulk byte length.", field, field_start_offset)

        if length > len(self._bytes) - self._offset:
            raise ProtocolError(
                "Bulk payload exceeds remaining message bytes.",
                field,
                field_start_offset,
            )

        end = self._offset + length
        result = self._bytes[self._offset : end]
        self._offset = end

        if self._bytes[self._offset : self._offset + 1] == b"\r":
            self._offset += 1

        if self._bytes[self._offset : self._offset + 1] != b"\n":
            raise ProtocolError(
                "Missing bulk byte terminator.", field, field_start_offset
            )

        self._offset += 1
        return result

    # end method _read_bulk_bytes

    def _read_identifier(self, field: str) -> str:
        field_start_offset = self._offset
        text = self._read_nullable_identifier(field)

        if text is None:
            raise ProtocolError("Identifier cannot be null.", field, field_start_offset)

        return text

    # end method _read_identifier

    def _read_nullable_identifier(self, field: str) -> str | None:
        field_start_offset = self._offset
        data = self._read_bytes(field)

        if data is None:
            return None

        text = self._read_text(data, field, field_start_offset)

        if not text or "\r" in text or "\n" in text:
            raise ProtocolError(
                "Identifier must be nonempty and CR/LF-free.",
                field,
                field_start_offset,
            )

        return text

    # end method _read_nullable_identifier

    def _read_payload(self, field: str = "payload") -> bytes:
        field_start_offset = self._offset
        payload = self._read_bytes(field)

        if payload is None:
            raise ProtocolError("Payload cannot be null.", field, field_start_offset)

        return payload

    # end method _read_payload

    def _read_array_length(
        self, depth: int, field: str, marker_already_read: bool = False
    ) -> int:
        field_start_offset = self._offset - 1 if marker_already_read else self._offset

        if depth >= MAXIMUM_DEPTH:
            raise ProtocolError(
                f"Arrays are nested deeper than {MAXIMUM_DEPTH} levels.",
                field,
                field_start_offset,
            )

        if not marker_already_read and self._read_marker(field) != b"*":
            raise ProtocolError("Expected array marker.", field, field_start_offset)

        length = self._read_decimal(field, field_start_offset)

        if length < 0:
            raise ProtocolError(
                "Array length cannot be negative.", field, field_start_offset
            )

        if length > MAXIMUM_FRAGMENTS - self._fragments:
            raise ProtocolError(
                f"Array length exceeds the {MAXIMUM_FRAGMENTS}-fragment budget.",
                field,
                field_start_offset,
            )

        return length

    # end method _read_array_length

    def _read_connections(self, depth: int) -> tuple[PresenceConnection, ...]:
        length = self._read_array_length(depth, "connections")
        connections: list[PresenceConnection] = []

        for _ in range(length):
            field_start_offset = self._offset

            if self._read_array_length(depth + 1, "connection") != 3:
                raise ProtocolError(
                    "Presence connection must contain three fields.",
                    "connection",
                    field_start_offset,
                )

            connections.append(
                PresenceConnection(
                    token_reference=self._read_identifier("token_reference"),
                    connection_id=self._read_identifier("connection_id"),
                    timestamp=self._read_integer64("timestamp"),
                )
            )

        return tuple(connections)

    # end method _read_connections

    def _read_message(self, depth: int, tail: bool) -> ServerMessage:
        field_start_offset = self._offset

        match self._read_marker("message"):
            case b"*":
                return self._read_message_array(depth, tail)
            case b"-":
                return self._read_error_message(depth)
            case b"@":
                return self._read_command_message(depth, tail)
            case _:
                raise ProtocolError(
                    "Unexpected server message marker.", "message", field_start_offset
                )

    # end method _read_message

    def _read_message_array(self, depth: int, tail: bool) -> ServerMessage:
        length = self._read_array_length(depth, "messages", True)
        messages = [
            self._read_message(depth + 1, tail and index == length - 1)
            for index in range(length)
        ]

        return ArrayFrame(messages=tuple(messages))

    # end method _read_message_array

    def _read_error_message(self, depth: int) -> ServerMessage:
        field_start_offset = self._offset - 1
        header = self._read_text(
            self._read_line("error", field_start_offset, 3), "error", field_start_offset
        )

        if header != "Err":
            raise ProtocolError("Invalid error header.", "error", field_start_offset)

        # Every field is self-delimiting, so an error may sit anywhere in a
        # batch.
        return ErrorFrame(
            type=self._read_error_type(),
            sub_type=self._read_error_sub_type(),
            message=self._read_payload("error_message"),
            resource=self._read_resource(depth),
        )

    # end method _read_error_message

    def _read_error_type(self) -> str:
        field_start_offset = self._offset

        if self._read_marker("error_type") != b"+":
            raise ProtocolError(
                "Expected simple string marker.", "error_type", field_start_offset
            )

        return self._read_error_name("error_type", field_start_offset)

    # end method _read_error_type

    def _read_error_sub_type(self) -> str | None:
        field_start_offset = self._offset

        marker = self._read_marker("error_sub_type")

        if marker == b"+":
            return self._read_error_name("error_sub_type", field_start_offset)

        if (
            marker == b"$"
            and self._read_decimal("error_sub_type", field_start_offset) == -1
        ):
            return None

        raise ProtocolError(
            "Sub type must be a simple string or null.",
            "error_sub_type",
            field_start_offset,
        )

    # end method _read_error_sub_type

    # Error types and sub types are names such as PermissionDeniedError and
    # PRES_LIST: bounded, and restricted to letters, digits and underscores.
    def _read_error_name(self, field: str, field_start_offset: int) -> str:
        name = self._read_text(
            self._read_line(field, field_start_offset, MAXIMUM_ERROR_NAME_BYTES),
            field,
            field_start_offset,
        )

        if _ERROR_NAME.fullmatch(name) is None:
            raise ProtocolError("Invalid error name.", field, field_start_offset)

        return name

    # end method _read_error_name

    # Any single fragment the error's type and sub type define: null, a
    # string, an Integer64, an Integer32, or an array of these.
    def _read_resource(self, depth: int) -> ServerErrorResource:
        field_start_offset = self._offset

        match self._read_marker("resource"):
            case b"+":
                return self._read_text(
                    self._read_line("resource", field_start_offset),
                    "resource",
                    field_start_offset,
                )
            case b"$":
                data = self._read_bulk_bytes("resource", field_start_offset)

                if data is None:
                    return None

                return self._read_text(data, "resource", field_start_offset)
            case b":":
                return self._read_decimal("resource", field_start_offset)
            case b";":
                return self._read_integer32_digits("resource", field_start_offset)
            case b"*":
                length = self._read_array_length(depth + 1, "resource", True)
                return tuple(self._read_resource(depth + 1) for _ in range(length))
            case _:
                raise ProtocolError(
                    "Unexpected resource marker.", "resource", field_start_offset
                )

    # end method _read_resource

    def _read_command_message(self, depth: int, tail: bool) -> ServerMessage:
        field_start_offset = self._offset - 1
        command = self._read_text(
            self._read_line("command", field_start_offset, MAXIMUM_COMMAND_NAME_BYTES),
            "command",
            field_start_offset,
        )

        match command:
            case "MSG":
                return self._read_peer_message()
            case "SERVER_MSG":
                return self._read_server_notice()
            case "PRES_NOTIFY":
                return self._read_presence_notification()
            case "PRES_LIST_RESPONSE":
                return self._read_presence_response(depth)
            case _:
                return self._skip_unknown_command(depth, tail, field_start_offset)

    # end method _read_command_message

    def _skip_unknown_command(
        self, depth: int, tail: bool, field_start_offset: int
    ) -> ServerMessage:
        # A command this version does not know carries an unknown number of
        # fields, so its end is only knowable when it runs to the end of the
        # transport message. Newer servers may add commands; skipping them
        # keeps this client working instead of killing its connection
        # (DECODE-01).
        if depth != 0 and not tail:
            raise ProtocolError(
                "Unknown command inside array has ambiguous boundaries.",
                "command",
                field_start_offset,
            )

        self._offset = len(self._bytes)
        return IgnoredFrame()

    # end method _skip_unknown_command

    def _read_peer_message(self) -> ServerMessage:
        return MessageFrame(
            token_reference=self._read_identifier("token_reference"),
            segment_id=self._read_identifier("segment_id"),
            message_id=self._read_nullable_identifier("message_id"),
            timestamp=self._read_integer64("timestamp"),
            payload=self._read_payload(),
        )

    # end method _read_peer_message

    def _read_server_notice(self) -> ServerMessage:
        return NoticeFrame(
            timestamp=self._read_integer64("timestamp"), payload=self._read_payload()
        )

    # end method _read_server_notice

    def _read_presence_notification(self) -> ServerMessage:
        segment_id = self._read_identifier("segment_id")
        token_reference = self._read_identifier("token_reference")
        connection_id = self._read_identifier("connection_id")
        event_offset = self._offset
        event = self._read_integer32("event")

        # A join/leave flag, not metadata: narrowed here rather than passed
        # through raw, and any other value is not a flag this client knows.
        if event not in (0, 1):
            raise ProtocolError("Presence event must be 0 or 1.", "event", event_offset)

        return PresenceNotifyFrame(
            segment_id=segment_id,
            token_reference=token_reference,
            connection_id=connection_id,
            joined=event == 1,
            timestamp=self._read_integer64("timestamp"),
        )

    # end method _read_presence_notification

    def _read_presence_response(self, depth: int) -> ServerMessage:
        return PresenceListFrame(
            segment_id=self._read_identifier("segment_id"),
            request_id=self._read_identifier("request_id"),
            total=self._read_integer32("total"),
            per_page=self._read_integer32("per_page"),
            current_page=self._read_integer32("current_page"),
            from_=self._read_integer32("from"),
            to=self._read_integer32("to"),
            connections=self._read_connections(depth),
        )

    # end method _read_presence_response


# end class MessageDecoder


# Test seam: production decodes through the connection's message callback;
# the codec suites use this wrapper. It is not part of the package surface.
def decode_server_message(data: bytes) -> ServerMessage:
    if not isinstance(data, bytes):
        raise ProtocolError("Expected byte buffer.", "message", 0)

    return MessageDecoder(data).decode()


# end function decode_server_message
