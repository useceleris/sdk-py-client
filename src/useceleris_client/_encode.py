from useceleris_client._commands import (
    ClientCommand,
    InterestCommand,
    PresenceListCommand,
    PublishCommand,
    parse_client_command,
)
from useceleris_client._constants import MAXIMUM_COMMAND_BYTES
from useceleris_client._errors import ConfigurationError


def encode_client_command(command: ClientCommand) -> bytes:
    return _CommandEncoder().encode(parse_client_command(command))


def _command_too_large() -> ConfigurationError:
    return ConfigurationError(
        "Encoded command exceeds 2 MiB. That is the most the server accepts on "
        "any plan; send a smaller payload."
    )


class _CommandEncoder:
    def __init__(self) -> None:
        self._encoded = bytearray()

    def encode(self, command: ClientCommand) -> bytes:
        if command["command"] == "PUB":
            self._write_publish_command(command)
        elif command["command"] == "PRES_LIST":
            self._write_presence_list_command(command)
        else:
            self._write_segment_command(command)

        return bytes(self._encoded)

    def _write_publish_command(self, command: PublishCommand) -> None:
        self._append_text("@PUB\n")
        self._append_bulk(command["segment_id"])

        if "message_id" in command:
            self._append_bulk(command["message_id"])
        else:
            self._append_text("$-1\n")

        self._append_bulk(command["payload"])

    def _write_presence_list_command(self, command: PresenceListCommand) -> None:
        self._append_text("@PRES_LIST\n")
        self._append_bulk(command["segment_id"])
        self._append_text(f";{command['page']}\n;{command['per_page']}\n")
        self._append_bulk(command["request_id"])

    def _write_segment_command(self, command: InterestCommand) -> None:
        self._append_text(f"@{command['command']}\n")
        self._append_bulk(command["segment_id"])

    def _append(self, data: bytes) -> None:
        if len(self._encoded) + len(data) > MAXIMUM_COMMAND_BYTES:
            raise _command_too_large()

        self._encoded += data

    def _append_text(self, text: str) -> None:
        # UTF-8 cannot be shorter than the code point count.
        if len(text) > MAXIMUM_COMMAND_BYTES:
            raise _command_too_large()

        self._append(text.encode())

    def _append_bulk(self, value: str | bytes) -> None:
        if len(value) > MAXIMUM_COMMAND_BYTES:
            raise _command_too_large()

        data = value.encode() if isinstance(value, str) else value
        self._append_text(f"${len(data)}\n")
        self._append(data)
        self._append_text("\n")
