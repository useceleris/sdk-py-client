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


# end class FakeWebSocket
