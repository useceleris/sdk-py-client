import asyncio
import contextlib
from collections.abc import Callable

from useceleris_client._connection_url import create_credential_url, validate_base_url
from useceleris_client._constants import (
    DEFAULT_CONNECT_TIMEOUT_MS,
    MAXIMUM_BUFFERED_BYTES,
    MAXIMUM_COMMAND_BYTES,
)
from useceleris_client._credential_types import CredentialProvider, CredentialRequest
from useceleris_client._credentials import (
    ConnectionConfiguration,
    Recovery,
    get_safe_parsed_connection_configuration,
    get_safe_parsed_credentials,
)
from useceleris_client._decode import MessageDecoder
from useceleris_client._errors import (
    CelerisConnectionError,
    ConfigurationError,
    ProtocolError,
)
from useceleris_client._messages import ServerMessage
from useceleris_client._timers import LOOP_TIMERS, Timers
from useceleris_client._websocket import WebSocket


class ConnectionHandle:
    def __init__(
        self, socket: WebSocket, remove_data_listeners: Callable[[], None]
    ) -> None:
        self._socket = socket
        self._remove_data_listeners = remove_data_listeners
        self._closed = False

    # end method __init__

    @property
    def is_open(self) -> bool:
        return not self._closed and self._socket.ready_state == WebSocket.OPEN

    # end method is_open

    @property
    def buffered_amount(self) -> int:
        return self._socket.buffered_amount

    # end method buffered_amount

    def send(self, data: bytes) -> None:
        if not self.is_open:
            raise CelerisConnectionError(
                "NotConnected",
                "Connection is not open; the WebSocket is closing or closed.",
            )

        if not isinstance(data, bytes) or len(data) > MAXIMUM_COMMAND_BYTES:
            raise ConfigurationError(
                "Invalid outgoing command. It must be bytes of at most 2 MiB."
            )

        if self._socket.buffered_amount + len(data) > MAXIMUM_BUFFERED_BYTES:
            raise CelerisConnectionError(
                "Backpressure",
                "WebSocket buffer is full: this command would take unsent data "
                "past 2 MiB. Retry once the buffer drains.",
            )

        with contextlib.suppress(Exception):
            self._socket.send(data)
            return

        # The socket's send failed after hand-off, so acceptance is
        # uncertain. Raised outside the handler, without the native error.
        raise CelerisConnectionError(
            "DeliveryUnknown",
            "WebSocket send failed after the command was handed over, so it may "
            "or may not have been sent.",
        )

    # end method send

    def close(self) -> None:
        if self._closed:
            return

        self._closed = True
        # The close callback stays installed so the socket's own close remains
        # observable through on_close; only the data callbacks are removed.
        self._remove_data_listeners()

        # The handle stays closed when the socket's close fails.
        if self._socket.ready_state not in (WebSocket.CLOSING, WebSocket.CLOSED):
            with contextlib.suppress(Exception):
                self._socket.close()

    # end method close


# end class ConnectionHandle


async def open_connection(
    configuration: ConnectionConfiguration,
    *,
    credential_provider: CredentialProvider,
    on_message: Callable[[ServerMessage], None],
    on_close: Callable[[], None] | None = None,
    on_error: Callable[[CelerisConnectionError | ProtocolError], None] | None = None,
    timeout_ms: float = DEFAULT_CONNECT_TIMEOUT_MS,
    timers: Timers = LOOP_TIMERS,
    cancellation: "asyncio.Future[None] | None" = None,
) -> ConnectionHandle:
    """Requests credentials and opens the socket under one deadline.

    Completing ``cancellation`` abandons the attempt with a Cancelled error;
    cancelling the awaiting task abandons it too and raises CancelledError.
    """
    config = get_safe_parsed_connection_configuration(configuration)

    if not callable(credential_provider) or not callable(on_message):
        raise ConfigurationError(
            "Invalid connection options. credential_provider and on_message must "
            "be callable."
        )

    base_url = validate_base_url(
        config["base_url"], config.get("allow_insecure_loopback", False)
    )
    request = _credential_request(config["channel_reference"], config.get("recovery"))
    loop = asyncio.get_running_loop()
    opened: asyncio.Future[ConnectionHandle] = loop.create_future()
    socket: WebSocket | None = None
    credentials_task: asyncio.Task[None] | None = None
    settled = False

    def remove_attempt_listeners() -> None:
        deadline.cancel()

        if cancellation is not None:
            cancellation.remove_done_callback(cancel)

        if socket is not None:
            socket.on_open = None
            socket.on_error = None
            socket.on_close = None

    # end function remove_attempt_listeners

    def fail(error: ConfigurationError | CelerisConnectionError) -> None:
        nonlocal settled

        if settled:
            return

        settled = True
        remove_attempt_listeners()

        # Cancels a credential provider that is still running.
        if credentials_task is not None:
            credentials_task.cancel()

        # The selected error is the one reported, whatever closing does.
        if socket is not None:
            with contextlib.suppress(Exception):
                socket.close()

        if not opened.done():
            opened.set_exception(error)

    # end function fail

    def cancel(_: object = None) -> None:
        fail(CelerisConnectionError("Cancelled", "Connection attempt cancelled."))

    # end function cancel

    def handshake_failed() -> None:
        fail(
            CelerisConnectionError(
                "Transport",
                "WebSocket handshake failed: the server refused the connection or "
                "could not be reached. Check the base URL, the credentials and "
                "the channel reference.",
            )
        )

    # end function handshake_failed

    def socket_opened() -> None:
        nonlocal settled

        if settled or socket is None:
            return

        # The caller was cancelled as the socket opened: the attempt is
        # abandoned, and so is the socket.
        if opened.done():
            cancel()
            return

        settled = True
        remove_attempt_listeners()
        opened.set_result(_create_handle(socket, on_message, on_close, on_error))

    # end function socket_opened

    async def request_credentials_and_open_socket() -> None:
        nonlocal socket

        try:
            provided = await credential_provider(request)
        except (Exception, asyncio.CancelledError):
            # A settled attempt cancelled the provider itself; otherwise the
            # provider failed, and its error, which may hold secrets, is not
            # passed on.
            if not settled:
                fail(
                    CelerisConnectionError(
                        "Transport",
                        "Credential acquisition failed: the credential provider "
                        "raised an error.",
                    )
                )

            return

        if settled:
            return

        try:
            credentials = get_safe_parsed_credentials(provided)
        except ConfigurationError as error:
            # Names which credential field failed, never its value.
            fail(error)
            return

        try:
            socket = WebSocket(
                create_credential_url(
                    base_url, config["channel_reference"], credentials
                )
            )
        except Exception:
            fail(CelerisConnectionError("Transport", "WebSocket creation failed."))
            return

        socket.on_open = socket_opened
        socket.on_error = handshake_failed
        socket.on_close = handshake_failed

    # end function request_credentials_and_open_socket

    deadline = timers.call_later(
        timeout_ms,
        lambda: fail(
            CelerisConnectionError(
                "Timeout", f"Connection attempt timed out after {timeout_ms} ms."
            )
        ),
    )

    if cancellation is not None:
        if cancellation.done():
            cancel()
        else:
            cancellation.add_done_callback(cancel)

    if not settled:
        credentials_task = loop.create_task(request_credentials_and_open_socket())

    try:
        return await opened
    except asyncio.CancelledError:
        # The caller was cancelled: abandon the attempt, closing a socket that
        # opened before the caller could resume, then let the cancellation
        # through.
        if opened.done() and not opened.cancelled() and opened.exception() is None:
            opened.result().close()

        cancel()
        raise


# end function open_connection


def _credential_request(
    channel_reference: str, recovery: Recovery | None
) -> CredentialRequest:
    if recovery is None or recovery["reason"] == "initial":
        return CredentialRequest(channel_reference=channel_reference, reason="initial")

    return CredentialRequest(
        channel_reference=channel_reference,
        reason="reconnect",
        disconnected_at=recovery["disconnected_at"],
        replay_lookback_ms=recovery["replay_lookback_ms"],
    )


# end function _credential_request


def _create_handle(
    socket: WebSocket,
    on_message: Callable[[ServerMessage], None],
    on_close: Callable[[], None] | None,
    on_error: Callable[[CelerisConnectionError | ProtocolError], None] | None,
) -> ConnectionHandle:
    def remove_data_listeners() -> None:
        socket.on_message = None
        socket.on_error = None

    # end function remove_data_listeners

    handle = ConnectionHandle(socket, remove_data_listeners)

    def report(error: CelerisConnectionError | ProtocolError) -> None:
        # Callback failures cannot escape socket event dispatch.
        if on_error is not None:
            with contextlib.suppress(Exception, asyncio.CancelledError):
                on_error(error)

    # end function report

    def report_error(error: CelerisConnectionError | ProtocolError) -> None:
        report(error)
        handle.close()

    # end function report_error

    # A decoder is built per transport message over that message's own bytes,
    # so nothing spans frames and a bad frame cannot desynchronize the next
    # one. Dropping it costs exactly that frame, which is why these report
    # without closing the socket (DECODE-01).
    def receive_message(data: bytes | str) -> None:
        if not isinstance(data, bytes):
            report(ProtocolError("Expected a binary WebSocket message.", "message", 0))
            return

        message: ServerMessage

        try:
            message = MessageDecoder(data).decode()
        except ProtocolError as error:
            report(error)
            return
        except Exception:
            report(ProtocolError("Message decoding failed.", "message", 0))
            return

        try:
            on_message(message)
        except (Exception, asyncio.CancelledError):
            report_error(
                CelerisConnectionError("Transport", "Message callback failed.")
            )

    # end function receive_message

    def receive_error() -> None:
        report_error(CelerisConnectionError("Transport", "WebSocket failed."))

    # end function receive_error

    def receive_close() -> None:
        # Once only. Closing the handle removes the data callbacks and marks
        # it closed.
        socket.on_close = None
        handle.close()

        if on_close is not None:
            with contextlib.suppress(Exception, asyncio.CancelledError):
                on_close()

    # end function receive_close

    socket.on_message = receive_message
    socket.on_error = receive_error
    socket.on_close = receive_close
    return handle


# end function _create_handle
