import asyncio
import logging
from collections import deque
from collections.abc import Callable

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, ConnectionClosedError

from useceleris_client._constants import CLOSE_BUDGET_MS

# Records nothing. It is not registered with logging, so configuring logging
# cannot turn it on: the library's handshake log line carries the credential
# URL.
_SILENT = logging.Logger("useceleris_client.websocket", logging.CRITICAL + 1)


class WebSocket:
    """A browser-style WebSocket over the websockets library.

    ``send`` is synchronous: it queues the frame and counts it in
    ``buffered_amount`` until a writer task hands it to the network, so a
    caller can see a full writer and back off. Events arrive through the
    ``on_*`` callbacks, which the connection layer installs and removes. A
    failed connection reports ``on_error`` and then ``on_close``; a clean one
    reports ``on_close`` alone.
    """

    CONNECTING = 0
    OPEN = 1
    CLOSING = 2
    CLOSED = 3

    def __init__(self, url: str) -> None:
        self.ready_state = WebSocket.CONNECTING
        self.buffered_amount = 0
        self.on_open: Callable[[], None] | None = None
        self.on_message: Callable[[bytes | str], None] | None = None
        self.on_error: Callable[[], None] | None = None
        self.on_close: Callable[[], None] | None = None
        self._outbound: deque[bytes] = deque()
        self._outbound_ready = asyncio.Event()
        self._connection: ClientConnection | None = None
        self._close_deadline: asyncio.TimerHandle | None = None
        self._task = asyncio.get_running_loop().create_task(self._run(url))

    def send(self, data: bytes) -> None:
        if self.ready_state != WebSocket.OPEN:
            raise RuntimeError("WebSocket is not open.")

        self._outbound.append(data)
        self.buffered_amount += len(data)
        self._outbound_ready.set()

    def close(self) -> None:
        if self.ready_state in (WebSocket.CLOSING, WebSocket.CLOSED):
            return

        connecting = self.ready_state == WebSocket.CONNECTING
        self.ready_state = WebSocket.CLOSING

        if connecting:
            self._task.cancel()
            return

        # Frames queued before close still go out, ahead of the close frame,
        # but a peer that stops reading cannot hold the socket open past the
        # close budget.
        self._outbound_ready.set()
        self._close_deadline = asyncio.get_running_loop().call_later(
            CLOSE_BUDGET_MS / 1000, self._abort
        )

    async def _run(self, url: str) -> None:
        try:
            connection = await connect(
                url,
                # The library logs the handshake request, credentials and all.
                logger=_SILENT,
                # The caller owns the deadline for the whole attempt.
                open_timeout=None,
                # LIMIT-01: received messages are never size-checked. Without
                # compression an unbounded frame cannot be a decompression
                # bomb.
                max_size=None,
                compression=None,
            )
        except (Exception, asyncio.CancelledError):
            # A refused or unreachable handshake, or close() while
            # connecting. This task is the socket's own, so cancelling it
            # ends here.
            self._finish(failed=True)
            return

        self._connection = connection
        self.ready_state = WebSocket.OPEN
        self._dispatch(self.on_open)
        writer = asyncio.get_running_loop().create_task(self._write(connection))
        failed = True

        try:
            # Lets whatever waits on the open run before any frame is read, as
            # a browser resolves the open before it delivers a message: frames
            # the server sent with the handshake are already buffered.
            await asyncio.sleep(0)

            async for message in connection:
                self._dispatch(self.on_message, message)

            failed = False
        except ConnectionClosedError:
            pass
        finally:
            writer.cancel()
            # Closed even when the peer closed first: Python 3.10 otherwise
            # warns that a TLS transport was never closed.
            connection.transport.close()
            self._finish(failed)

    async def _write(self, connection: ClientConnection) -> None:
        try:
            while True:
                if self._outbound:
                    data = self._outbound[0]
                    await connection.send(data)
                    self._outbound.popleft()
                    self.buffered_amount -= len(data)
                elif self.ready_state == WebSocket.CLOSING:
                    await connection.close()
                    return
                else:
                    self._outbound_ready.clear()
                    await self._outbound_ready.wait()
        except ConnectionClosed:
            # The reader sees the same closure and reports it.
            return

    def _abort(self) -> None:
        if self._connection is not None:
            self._connection.transport.abort()

    def _finish(self, failed: bool) -> None:
        self.ready_state = WebSocket.CLOSED
        self._outbound.clear()
        self.buffered_amount = 0

        if self._close_deadline is not None:
            self._close_deadline.cancel()

        if failed:
            self._dispatch(self.on_error)

        self._dispatch(self.on_close)

    def _dispatch(self, callback: Callable[..., None] | None, *values: object) -> None:
        if callback is None:
            return

        try:
            callback(*values)
        except (Exception, asyncio.CancelledError) as error:
            # Like an exception in a browser event handler: reported, and the
            # socket carries on.
            asyncio.get_running_loop().call_exception_handler(
                {"message": "WebSocket event callback failed", "exception": error}
            )
