from collections.abc import Callable
from typing import Any
from unittest.mock import Mock

from useceleris_client._websocket import WebSocket


class FakeWebSocket:
    """Stands in for the package's WebSocket: the test drives every event.

    ``send`` and ``close`` are mocks, so a test can make them raise or change
    state. Closing reports the close synchronously, as a socket that closes at
    once would.
    """

    CONNECTING = WebSocket.CONNECTING
    OPEN = WebSocket.OPEN
    CLOSING = WebSocket.CLOSING
    CLOSED = WebSocket.CLOSED

    def __init__(self, url: str) -> None:
        self.url = url
        self.ready_state = WebSocket.CONNECTING
        self.buffered_amount = 0
        self.on_open: Callable[[], None] | None = None
        self.on_message: Callable[[bytes | str], None] | None = None
        self.on_error: Callable[[], None] | None = None
        self.on_close: Callable[[], None] | None = None
        self.send = Mock()
        self.close = Mock(side_effect=self.disconnect)

    # end method __init__

    def open(self) -> None:
        self.ready_state = WebSocket.OPEN

        if self.on_open is not None:
            self.on_open()

    # end method open

    def receive(self, data: object) -> None:
        # Any data, as a socket could deliver it.
        untyped: Any = self.on_message

        if untyped is not None:
            untyped(data)

    # end method receive

    def fail(self) -> None:
        if self.on_error is not None:
            self.on_error()

    # end method fail

    def disconnect(self) -> None:
        self.ready_state = WebSocket.CLOSED

        if self.on_close is not None:
            self.on_close()

    # end method disconnect

    def sent_frames(self) -> list[str]:
        return [call.args[0].decode() for call in self.send.call_args_list]

    # end method sent_frames

    def sent_commands(self) -> list[str]:
        """Every command sent, in order, with each `*N` frame split into its
        commands."""
        return [
            command.decode()
            for call in self.send.call_args_list
            for command in _split_frame(call.args[0])
        ]

    # end method sent_commands


# end class FakeWebSocket


def _split_frame(frame: bytes) -> list[bytes]:
    if not frame.startswith(b"*"):
        return [frame]

    commands: list[bytes] = []
    offset = frame.index(b"\n") + 1

    while offset < len(frame):
        start = offset
        offset = frame.index(b"\n", offset) + 1

        # Fields follow the command name until the next command starts.
        while offset < len(frame) and frame[offset : offset + 1] != b"@":
            line_end = frame.index(b"\n", offset)
            field = frame[offset:line_end]
            offset = line_end + 1

            if field.startswith(b"$") and field != b"$-1":
                offset += int(field[1:]) + 1

        commands.append(frame[start:offset])

    return commands


# end function _split_frame
