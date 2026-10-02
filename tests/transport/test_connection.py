import asyncio
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import AsyncMock, Mock
from urllib.parse import parse_qs, urlsplit

import pytest

from tests.helpers.tasks import failure_of, flush
from tests.helpers.timers import FakeTimers
from tests.helpers.websocket import FakeWebSocket
from useceleris_client import _connection
from useceleris_client._connection import ConnectionHandle, open_connection
from useceleris_client._credential_types import CredentialRequest, Credentials
from useceleris_client._credentials import ConnectionConfiguration
from useceleris_client._errors import (
    CelerisConnectionError,
    ConfigurationError,
    ProtocolError,
)
from useceleris_client._messages import NoticeFrame

CREDENTIALS = Credentials(payload="a+/=&%識", signature="sig+/=")

CONFIGURATION: ConnectionConfiguration = {
    "base_url": "wss://example.test/prefix/",
    "channel_reference": "room-1",
}


@dataclass
class Callbacks:
    credential_provider: AsyncMock = field(
        default_factory=lambda: AsyncMock(return_value=CREDENTIALS)
    )
    on_message: Mock = field(default_factory=Mock)
    on_close: Mock = field(default_factory=Mock)
    on_error: Mock = field(default_factory=Mock)


def attempt(
    callbacks: Callbacks,
    timers: FakeTimers,
    configuration: ConnectionConfiguration = CONFIGURATION,
    **options: Any,
) -> "asyncio.Task[ConnectionHandle]":
    return asyncio.ensure_future(
        open_connection(
            configuration,
            credential_provider=callbacks.credential_provider,
            on_message=callbacks.on_message,
            on_close=callbacks.on_close,
            on_error=callbacks.on_error,
            timers=timers,
            **options,
        )
    )


class TestConnectionAttempt:
    async def test_awaits_open_and_passes_fresh_initial_credentials(
        self, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        callbacks = Callbacks()
        pending = attempt(callbacks, timers)

        await flush()
        assert not pending.done()
        assert len(sockets) == 1
        callbacks.credential_provider.assert_awaited_once_with(
            CredentialRequest(channel_reference="room-1", reason="initial")
        )

        url = urlsplit(sockets[0].url)
        assert url.path == "/prefix/channel/room-1"
        assert parse_qs(url.query) == {
            "payload": [CREDENTIALS.payload],
            "signature": [CREDENTIALS.signature],
        }

        sockets[0].open()
        await pending

        second = attempt(callbacks, timers)
        await flush()
        sockets[1].open()
        await second
        assert callbacks.credential_provider.await_count == 2

    async def test_passes_complete_reconnect_context(
        self, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        callbacks = Callbacks()
        pending = attempt(
            callbacks,
            timers,
            {
                **CONFIGURATION,
                "recovery": {
                    "reason": "reconnect",
                    "disconnected_at": 1_000,
                    "replay_lookback_ms": 9_500,
                },
            },
        )

        await flush()
        sockets[0].open()
        await pending

        callbacks.credential_provider.assert_awaited_once_with(
            CredentialRequest(
                channel_reference="room-1",
                reason="reconnect",
                disconnected_at=1_000,
                replay_lookback_ms=9_500,
            )
        )

    @pytest.mark.parametrize(
        "value",
        [
            None,
            {"payload": "a", "signature": "b"},
            Credentials(payload="", signature="x"),
            Credentials(payload="\ud800", signature="x"),
            Credentials(payload=1, signature="x"),  # type: ignore[arg-type]
        ],
    )
    async def test_rejects_invalid_credentials(
        self, sockets: list[FakeWebSocket], timers: FakeTimers, value: object
    ) -> None:
        callbacks = Callbacks()
        callbacks.credential_provider.return_value = value

        with pytest.raises(ConfigurationError, match=r"^Invalid credentials\. "):
            await attempt(callbacks, timers)

        assert sockets == []

    async def test_names_the_failed_credential_field_without_its_value(
        self, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        callbacks = Callbacks()
        callbacks.credential_provider.return_value = Credentials(
            payload="synthetic-secret\ud800", signature="x"
        )

        error = await failure_of(attempt(callbacks, timers))

        assert str(error) == (
            "Invalid credentials. payload: Must not contain unpaired UTF-16 surrogates."
        )
        assert "synthetic-secret" not in repr(error)

    @pytest.mark.parametrize("asynchronous", [False, True])
    async def test_sanitizes_provider_failure(
        self, sockets: list[FakeWebSocket], timers: FakeTimers, asynchronous: bool
    ) -> None:
        def raise_secret(request: CredentialRequest) -> Credentials:
            raise ConfigurationError("synthetic-secret") from Exception("synthetic")

        async def raise_secret_later(request: CredentialRequest) -> Credentials:
            return raise_secret(request)

        callbacks = Callbacks()
        provider = raise_secret_later if asynchronous else raise_secret
        error = await failure_of(
            asyncio.ensure_future(
                open_connection(
                    CONFIGURATION,
                    credential_provider=provider,  # type: ignore[arg-type]
                    on_message=callbacks.on_message,
                    timers=timers,
                )
            )
        )

        assert isinstance(error, CelerisConnectionError)
        assert error.code == "Transport"
        assert str(error) == (
            "Credential acquisition failed: the credential provider raised an error."
        )
        assert "synthetic" not in repr(error)
        assert error.__cause__ is None
        assert error.__context__ is None

    @pytest.mark.parametrize("event", ["error", "close"])
    async def test_rejects_failure_before_open_and_ignores_later_events(
        self, sockets: list[FakeWebSocket], timers: FakeTimers, event: str
    ) -> None:
        pending = attempt(Callbacks(), timers)
        await flush()

        if event == "error":
            sockets[0].fail()
        else:
            sockets[0].disconnect()

        error = await failure_of(pending)
        assert isinstance(error, CelerisConnectionError)
        assert error.code == "Transport"
        sockets[0].open()

    async def test_converts_websocket_construction_failure_safely(
        self, monkeypatch: pytest.MonkeyPatch, timers: FakeTimers
    ) -> None:
        def refuse(url: str) -> None:
            raise RuntimeError("synthetic-secret")

        monkeypatch.setattr(_connection, "WebSocket", refuse)

        error = await failure_of(attempt(Callbacks(), timers))

        assert isinstance(error, CelerisConnectionError)
        assert error.code == "Transport"
        assert "synthetic-secret" not in repr(error)
        assert error.__context__ is None

    @pytest.mark.parametrize("reason", ["cancellation", "caller", "timeout"])
    async def test_cancels_the_provider_and_ignores_late_credentials(
        self, sockets: list[FakeWebSocket], timers: FakeTimers, reason: str
    ) -> None:
        provider_cancelled = asyncio.Event()

        # Ignores its cancellation and returns credentials anyway.
        async def stubborn_provider(request: CredentialRequest) -> Credentials:
            try:
                await asyncio.get_running_loop().create_future()
            except asyncio.CancelledError:
                provider_cancelled.set()

            return CREDENTIALS

        cancellation = asyncio.get_running_loop().create_future()
        callbacks = Callbacks()
        pending = asyncio.ensure_future(
            open_connection(
                CONFIGURATION,
                credential_provider=stubborn_provider,
                on_message=callbacks.on_message,
                timers=timers,
                cancellation=cancellation,
            )
        )
        await flush()

        if reason == "cancellation":
            cancellation.set_result(None)
        elif reason == "caller":
            pending.cancel()
        else:
            await timers.advance(15_000)

        error = await failure_of(pending)

        if reason == "caller":
            assert isinstance(error, asyncio.CancelledError)
        else:
            assert isinstance(error, CelerisConnectionError)
            assert error.code == (
                "Cancelled" if reason == "cancellation" else "Timeout"
            )

        await flush()
        assert provider_cancelled.is_set()
        assert sockets == []
        assert timers.count == 0

    async def test_uses_one_deadline_for_credentials_and_handshake(
        self, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        async def slow_provider(request: CredentialRequest) -> Credentials:
            await timers.sleep(10_000)
            return CREDENTIALS

        callbacks = Callbacks(credential_provider=AsyncMock(side_effect=slow_provider))
        pending = attempt(callbacks, timers)

        await timers.advance(10_000)
        assert len(sockets) == 1

        await timers.advance(5_000)
        error = await failure_of(pending)
        assert isinstance(error, CelerisConnectionError)
        assert error.code == "Timeout"
        assert str(error) == "Connection attempt timed out after 15000 ms."
        sockets[0].close.assert_called_once()
        assert timers.count == 0

    async def test_honors_a_custom_timeout(
        self, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        async def never(request: CredentialRequest) -> Credentials:
            await asyncio.get_running_loop().create_future()
            raise AssertionError("unreachable")

        callbacks = Callbacks(credential_provider=AsyncMock(side_effect=never))
        pending = attempt(callbacks, timers, timeout_ms=5_000)

        await timers.advance(5_000)
        error = await failure_of(pending)
        assert isinstance(error, CelerisConnectionError)
        assert error.code == "Timeout"
        assert timers.count == 0

    async def test_pre_cancellation_skips_the_provider(
        self, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        cancellation = asyncio.get_running_loop().create_future()
        cancellation.set_result(None)
        callbacks = Callbacks()

        error = await failure_of(attempt(callbacks, timers, cancellation=cancellation))

        assert isinstance(error, CelerisConnectionError)
        assert error.code == "Cancelled"
        callbacks.credential_provider.assert_not_called()
        assert timers.count == 0

    async def test_cancels_during_handshake_and_removes_the_deadline(
        self, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        cancellation = asyncio.get_running_loop().create_future()
        pending = attempt(Callbacks(), timers, cancellation=cancellation)
        await flush()

        cancellation.set_result(None)

        error = await failure_of(pending)
        assert isinstance(error, CelerisConnectionError)
        assert error.code == "Cancelled"
        sockets[0].close.assert_called_once()
        sockets[0].open()
        assert timers.count == 0

    async def test_rejects_options_that_are_not_callable(
        self, timers: FakeTimers
    ) -> None:
        untyped: Any = None

        with pytest.raises(ConfigurationError, match="must be callable"):
            await open_connection(
                CONFIGURATION, credential_provider=untyped, on_message=untyped
            )


class TestOpenConnection:
    @pytest.fixture
    async def connected(
        self, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> tuple[ConnectionHandle, Callbacks, FakeWebSocket]:
        callbacks = Callbacks()
        pending = attempt(callbacks, timers)
        await flush()
        sockets[-1].open()

        return await pending, callbacks, sockets[-1]

    async def test_decodes_binary_messages_in_arrival_order(
        self,
        connected: tuple[ConnectionHandle, Callbacks, FakeWebSocket],
        timers: FakeTimers,
    ) -> None:
        _, callbacks, socket = connected
        assert timers.count == 0

        socket.receive(b"@SERVER_MSG\n:1\n$1\na\n")
        socket.receive(b"@SERVER_MSG\n:2\n$1\nb\n")

        assert [call.args for call in callbacks.on_message.call_args_list] == [
            (NoticeFrame(1, b"a"),),
            (NoticeFrame(2, b"b"),),
        ]

    @pytest.mark.parametrize("data", ["text", bytearray(b"x"), memoryview(b"x")])
    async def test_drops_unsupported_message_data_without_closing(
        self,
        connected: tuple[ConnectionHandle, Callbacks, FakeWebSocket],
        data: object,
    ) -> None:
        _, callbacks, socket = connected

        socket.receive(data)

        (error,) = callbacks.on_error.call_args.args
        assert isinstance(error, ProtocolError)
        assert error.field == "message"
        # DECODE-01: the frame is dropped, the connection is not.
        socket.close.assert_not_called()

    async def test_keeps_delivering_messages_after_an_undecodable_frame(
        self, connected: tuple[ConnectionHandle, Callbacks, FakeWebSocket]
    ) -> None:
        _, callbacks, socket = connected

        socket.receive(b"not a frame")
        socket.receive(b"@SERVER_MSG\n:1\n$2\nhi\n")

        callbacks.on_error.assert_called_once()
        assert isinstance(callbacks.on_error.call_args.args[0], ProtocolError)
        callbacks.on_message.assert_called_once_with(NoticeFrame(1, b"hi"))
        socket.close.assert_not_called()

    async def test_delivers_a_received_message_of_any_size(
        self, connected: tuple[ConnectionHandle, Callbacks, FakeWebSocket]
    ) -> None:
        # LIMIT-01: the message has already been received by now, so the SDK
        # processes it rather than measuring and discarding it.
        _, callbacks, socket = connected
        payload_length = 1024 * 1024

        socket.receive(
            f"@SERVER_MSG\n:1\n${payload_length}\n".encode()
            + bytes(payload_length)
            + b"\n"
        )

        callbacks.on_error.assert_not_called()
        callbacks.on_message.assert_called_once()

    @pytest.mark.parametrize(
        "failure",
        [
            RuntimeError("synthetic-secret"),
            ProtocolError("synthetic-secret", "synthetic-field", 9),
        ],
    )
    async def test_contains_message_and_error_callback_failures(
        self,
        connected: tuple[ConnectionHandle, Callbacks, FakeWebSocket],
        failure: Exception,
    ) -> None:
        _, callbacks, socket = connected
        callbacks.on_message.side_effect = failure
        callbacks.on_error.side_effect = RuntimeError("another-secret")

        socket.receive(b"@SERVER_MSG\n:1\n$0\n\n")

        (error,) = callbacks.on_error.call_args.args
        assert isinstance(error, CelerisConnectionError)
        assert (error.code, str(error)) == ("Transport", "Message callback failed.")
        assert error.__context__ is None
        assert not hasattr(error, "field")
        assert "synthetic" not in repr(error)
        socket.close.assert_called_once()

    async def test_sends_bytes_within_exact_limits(
        self, connected: tuple[ConnectionHandle, Callbacks, FakeWebSocket]
    ) -> None:
        handle, _, socket = connected

        handle.send(b"\x01\x02")
        socket.send.assert_called_once_with(b"\x01\x02")

        # The command bound is the server's 2 MiB transport ceiling, and the
        # buffer bound equals it, so a maximum command fits an empty buffer.
        handle.send(bytes(2 * 1024 * 1024))

        with pytest.raises(ConfigurationError, match=r"^Invalid outgoing command\."):
            handle.send(bytes(2 * 1024 * 1024 + 1))

        untyped: Any = bytearray(b"x")

        with pytest.raises(ConfigurationError, match=r"^Invalid outgoing command\."):
            handle.send(untyped)

        socket.buffered_amount = 2 * 1024 * 1024 - 1
        handle.send(b"\x00")

        with pytest.raises(CelerisConnectionError) as caught:
            handle.send(b"\x00\x00")

        assert caught.value.code == "Backpressure"
        assert str(caught.value) == (
            "WebSocket buffer is full: this command would take unsent data past "
            "2 MiB. Retry once the buffer drains."
        )

    async def test_sanitizes_socket_send_failures(
        self, connected: tuple[ConnectionHandle, Callbacks, FakeWebSocket]
    ) -> None:
        handle, _, socket = connected
        socket.send.side_effect = RuntimeError("synthetic-secret")

        with pytest.raises(CelerisConnectionError) as caught:
            handle.send(b"")

        assert caught.value.code == "DeliveryUnknown"
        assert str(caught.value) == (
            "WebSocket send failed after the command was handed over, so it may "
            "or may not have been sent."
        )
        assert caught.value.__context__ is None

    async def test_closes_once_and_reports_the_close_after_an_explicit_close(
        self, connected: tuple[ConnectionHandle, Callbacks, FakeWebSocket]
    ) -> None:
        handle, callbacks, socket = connected

        handle.close()
        handle.close()

        socket.close.assert_called_once()
        callbacks.on_close.assert_called_once()
        socket.disconnect()
        callbacks.on_close.assert_called_once()

        with pytest.raises(
            CelerisConnectionError,
            match=r"^Connection is not open; the WebSocket is closing or closed\.$",
        ):
            handle.send(b"")

    async def test_reports_an_unexpected_close_once_and_contains_its_failure(
        self, connected: tuple[ConnectionHandle, Callbacks, FakeWebSocket]
    ) -> None:
        handle, callbacks, socket = connected
        callbacks.on_close.side_effect = RuntimeError("synthetic-secret")

        socket.disconnect()
        callbacks.on_close.assert_called_once()
        socket.disconnect()
        callbacks.on_close.assert_called_once()
        assert not handle.is_open

        with pytest.raises(CelerisConnectionError, match=r"^Connection is not open;"):
            handle.send(b"")


async def test_closes_a_socket_that_opens_as_the_caller_is_cancelled(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    pending = attempt(Callbacks(), timers)
    await flush()

    # Both land in the same loop iteration, before the caller resumes.
    pending.cancel()
    sockets[0].open()

    assert isinstance(await failure_of(pending), asyncio.CancelledError)
    sockets[0].close.assert_called_once()
    assert sockets[0].on_message is None
    assert timers.count == 0


async def test_closes_a_socket_that_opened_before_the_cancelled_caller_resumed(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    pending = attempt(Callbacks(), timers)
    await flush()

    # The open resolves the attempt; the cancellation lands before the caller
    # takes the handle.
    sockets[0].open()
    pending.cancel()

    assert isinstance(await failure_of(pending), asyncio.CancelledError)
    sockets[0].close.assert_called_once()
