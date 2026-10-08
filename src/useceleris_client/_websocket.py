import asyncio
import contextlib
import logging
import threading
from collections import deque
from collections.abc import Callable

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, ConnectionClosedError

from useceleris_client._constants import (
    CLOSE_BUDGET_MS,
    TRANSPORT_THREAD_NAME,
    USER_LOOP_CHECK_INTERVAL_MS,
)

# Records nothing. It is not registered with logging, so configuring logging
# cannot turn it on: the library's handshake log line carries the credential
# URL.
_SILENT = logging.Logger("useceleris_client.websocket", logging.CRITICAL + 1)


class WebSocket:
    """A browser-style WebSocket over the websockets library.

    The connection runs on a daemon thread with its own event loop
    (HEARTBEAT-01), so the library keeps answering pings and sending its own
    while a listener holds the caller's loop. Every event reaches the
    ``on_*`` callbacks on the loop that created the socket, one at a time and
    in order. The thread reads the next frame only once the caller's loop has
    delivered the previous one (DEV-01).

    ``send`` is synchronous: it queues the frame and counts it in
    ``buffered_amount`` until the writer hands it to the network, so a caller
    can see a full writer and back off. A failed connection reports
    ``on_error`` and then ``on_close``; a clean one reports ``on_close``
    alone. The thread has ended by the time ``on_close`` runs.
    """

    CONNECTING = 0
    OPEN = 1
    CLOSING = 2
    CLOSED = 3

    def __init__(self, url: str) -> None:
        self.ready_state = WebSocket.CONNECTING
        self.on_open: Callable[[], None] | None = None
        self.on_message: Callable[[bytes | str], None] | None = None
        self.on_error: Callable[[], None] | None = None
        self.on_close: Callable[[], None] | None = None
        self._user_loop = asyncio.get_running_loop()
        # Guards what both threads touch: the outbound frames, their byte
        # count, and the handoff of the transport loop.
        self._lock = threading.Lock()
        self._outbound: deque[bytes] = deque()
        self._buffered_amount = 0
        self._close_requested = False
        # Set on the transport thread once it runs.
        self._transport_loop: asyncio.AbstractEventLoop | None = None
        self._transport_task: asyncio.Task[bool] | None = None
        self._outbound_ready: asyncio.Event | None = None
        self._connection: ClientConnection | None = None
        self._closing = False
        self._thread = threading.Thread(
            target=self._run_thread,
            args=(url,),
            name=TRANSPORT_THREAD_NAME,
            daemon=True,
        )
        self._thread.start()

    # end method __init__

    @property
    def buffered_amount(self) -> int:
        return self._buffered_amount

    # end method buffered_amount

    def send(self, data: bytes) -> None:
        if self.ready_state != WebSocket.OPEN:
            raise RuntimeError("WebSocket is not open.")

        with self._lock:
            self._outbound.append(data)
            self._buffered_amount += len(data)

        self._call_on_transport_loop(self._wake_writer)

    # end method send

    def close(self) -> None:
        if self.ready_state in (WebSocket.CLOSING, WebSocket.CLOSED):
            return

        self.ready_state = WebSocket.CLOSING

        with self._lock:
            self._close_requested = True

        self._call_on_transport_loop(self._begin_closing)

    # end method close

    # The transport thread.

    def _run_thread(self, url: str) -> None:
        failed = True

        try:
            failed = asyncio.run(self._run(url))
        finally:
            # The last thing the thread does, so the join in _finish is
            # immediate. A caller's loop that is already closed has nobody
            # left to tell.
            with contextlib.suppress(RuntimeError):
                self._user_loop.call_soon_threadsafe(self._finish, failed)

    # end method _run_thread

    async def _run(self, url: str) -> bool:
        """Returns whether the connection failed."""
        loop = asyncio.get_running_loop()
        self._transport_task = asyncio.current_task()
        self._outbound_ready = asyncio.Event()

        with self._lock:
            self._transport_loop = loop
            close_requested = self._close_requested

        if close_requested:
            return True

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
            # connecting. This task is the thread's own, so cancelling it
            # ends here.
            return True

        self._connection = connection
        writer = loop.create_task(self._write(connection))
        watchdog = loop.create_task(self._watch_user_loop())
        failed = True

        try:
            # The open is delivered before any frame is read, as a browser
            # resolves the open before it delivers a message: frames the
            # server sent with the handshake are already buffered.
            await self._hand_over(self._open)

            async for message in connection:
                await self._hand_over(self._dispatch_message, message)

            failed = False
        except (ConnectionClosedError, RuntimeError, asyncio.CancelledError):
            # The connection failed, or the caller's loop closed under it.
            pass
        finally:
            writer.cancel()
            watchdog.cancel()
            # Closed even when the peer closed first: Python 3.10 otherwise
            # warns that a TLS transport was never closed.
            connection.transport.close()

        return failed

    # end method _run

    async def _hand_over(self, callback: Callable[..., None], *values: object) -> None:
        """Runs the callback on the caller's loop and waits until it has
        returned. The library meanwhile keeps handling pings on this loop."""
        transport_loop = asyncio.get_running_loop()
        delivered = asyncio.Event()

        def acknowledge() -> None:
            with contextlib.suppress(RuntimeError):
                transport_loop.call_soon_threadsafe(delivered.set)

        # end function acknowledge

        def deliver() -> None:
            callback(*values)
            # After whatever the callback woke has had its turn, such as the
            # task awaiting the open.
            self._user_loop.call_soon(acknowledge)

        # end function deliver

        self._user_loop.call_soon_threadsafe(deliver)
        await delivered.wait()

    # end method _hand_over

    async def _write(self, connection: ClientConnection) -> None:
        assert self._outbound_ready is not None

        try:
            while True:
                with self._lock:
                    data = self._outbound[0] if self._outbound else None

                if data is not None:
                    await connection.send(data)

                    with self._lock:
                        self._outbound.popleft()
                        self._buffered_amount -= len(data)
                elif self._closing:
                    await connection.close()
                    return
                else:
                    self._outbound_ready.clear()
                    await self._outbound_ready.wait()
        except ConnectionClosed:
            # The reader sees the same closure and reports it.
            return

    # end method _write

    async def _watch_user_loop(self) -> None:
        # A caller's loop that closes without closing the socket would leave
        # this thread waiting on it forever. Nothing signals that a loop
        # closed, so it is polled.
        while True:
            await asyncio.sleep(USER_LOOP_CHECK_INTERVAL_MS / 1000)

            if self._user_loop.is_closed():
                break

        if self._transport_task is not None:
            self._transport_task.cancel()

    # end method _watch_user_loop

    def _wake_writer(self) -> None:
        if self._outbound_ready is not None:
            self._outbound_ready.set()

    # end method _wake_writer

    def _begin_closing(self) -> None:
        if self._connection is None:
            if self._transport_task is not None:
                self._transport_task.cancel()

            return

        # Frames queued before close still go out, ahead of the close frame,
        # but a peer that stops reading cannot hold the socket open past the
        # close budget.
        self._closing = True
        self._wake_writer()
        asyncio.get_running_loop().call_later(CLOSE_BUDGET_MS / 1000, self._abort)

    # end method _begin_closing

    def _abort(self) -> None:
        if self._connection is not None:
            self._connection.transport.abort()

    # end method _abort

    # The caller's loop.

    def _call_on_transport_loop(self, callback: Callable[[], None]) -> None:
        with self._lock:
            loop = self._transport_loop

        # A thread that has not started its loop yet reads the close request
        # itself; one that has finished needs nothing more.
        if loop is not None:
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(callback)

    # end method _call_on_transport_loop

    def _open(self) -> None:
        # A close that came while the handshake completed wins.
        if self.ready_state != WebSocket.CONNECTING:
            return

        self.ready_state = WebSocket.OPEN
        self._dispatch(self.on_open)

    # end method _open

    def _dispatch_message(self, message: bytes | str) -> None:
        self._dispatch(self.on_message, message)

    # end method _dispatch_message

    def _finish(self, failed: bool) -> None:
        self._thread.join()
        self.ready_state = WebSocket.CLOSED

        with self._lock:
            self._outbound.clear()
            self._buffered_amount = 0

        if failed:
            self._dispatch(self.on_error)

        self._dispatch(self.on_close)

    # end method _finish

    def _dispatch(self, callback: Callable[..., None] | None, *values: object) -> None:
        if callback is None:
            return

        try:
            callback(*values)
        except (Exception, asyncio.CancelledError) as error:
            # Like an exception in a browser event handler: reported, and the
            # socket carries on.
            self._user_loop.call_exception_handler(
                {"message": "WebSocket event callback failed", "exception": error}
            )

    # end method _dispatch


# end class WebSocket
