import asyncio
import logging
import shutil
import ssl
import subprocess
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from http import HTTPStatus
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.http11 import Request, Response

from useceleris_client import (
    CelerisConnectionError,
    Credentials,
    MessageMetadata,
    ProtocolError,
    create_client,
)
from useceleris_client._connection import ConnectionHandle, open_connection
from useceleris_client._messages import NoticeFrame, ServerMessage
from useceleris_client._websocket import WebSocket

CREDENTIALS = Credentials(payload="a+/=&%識", signature="sig+/=")


@dataclass
class LocalServer:
    """A local WebSocket server; handle() decides what each connection does."""

    handle: Callable[[ServerConnection], Awaitable[None]]
    paths: list[str] = field(default_factory=list)
    received: list[bytes | str] = field(default_factory=list)
    url: str = ""

    async def serve(self, connection: ServerConnection) -> None:
        assert connection.request is not None
        self.paths.append(connection.request.path)
        await self.handle(connection)


async def collect(connection: ServerConnection, server: LocalServer) -> None:
    async for message in connection:
        server.received.append(message)


async def discard(connection: ServerConnection) -> None:
    async for _ in connection:
        pass


@pytest.fixture
async def local_server() -> AsyncIterator[LocalServer]:
    server = LocalServer(handle=lambda connection: collect(connection, server))

    async with serve(server.serve, "127.0.0.1", 0, max_size=None) as running:
        server.url = f"ws://127.0.0.1:{port_of(running)}/prefix/"
        yield server


def port_of(server: Server) -> int:
    port: int = server.sockets[0].getsockname()[1]
    return port


@dataclass
class Events:
    messages: list[ServerMessage] = field(default_factory=list)
    errors: list[CelerisConnectionError | ProtocolError] = field(default_factory=list)
    closed: asyncio.Event = field(default_factory=asyncio.Event)


async def provide(request: object) -> Credentials:
    return CREDENTIALS


async def connect_to(url: str, events: Events, **options: Any) -> ConnectionHandle:
    return await open_connection(
        {
            "base_url": url,
            "channel_reference": "room-1",
            "allow_insecure_loopback": True,
        },
        credential_provider=provide,
        on_message=events.messages.append,
        on_error=events.errors.append,
        on_close=events.closed.set,
        **options,
    )


async def eventually(condition: Callable[[], bool]) -> None:
    for _ in range(500):
        if condition():
            return

        await asyncio.sleep(0.01)

    raise AssertionError("The condition did not hold within five seconds.")


async def test_opens_with_the_credential_url_and_exchanges_binary_frames(
    local_server: LocalServer,
) -> None:
    events = Events()

    async def notify_then_collect(connection: ServerConnection) -> None:
        await connection.send(b"@SERVER_MSG\n:1\n$2\nhi\n")
        await collect(connection, local_server)

    local_server.handle = notify_then_collect
    handle = await connect_to(local_server.url, events)

    path = urlsplit(local_server.paths[0])
    assert path.path == "/prefix/channel/room-1"
    assert parse_qs(path.query) == {
        "payload": [CREDENTIALS.payload],
        "signature": [CREDENTIALS.signature],
    }

    handle.send(b"@SUB\n$4\nchat\n")
    await eventually(lambda: bool(local_server.received) and bool(events.messages))

    assert local_server.received == [b"@SUB\n$4\nchat\n"]
    assert events.messages == [NoticeFrame(1, b"hi")]
    assert handle.buffered_amount == 0

    handle.close()
    await asyncio.wait_for(events.closed.wait(), 5)
    assert not handle.is_open


async def test_reports_a_text_frame_as_a_protocol_error_and_stays_open(
    local_server: LocalServer,
) -> None:
    events = Events()

    async def send_text(connection: ServerConnection) -> None:
        await connection.send("text")
        await connection.send(b"@SERVER_MSG\n:1\n$0\n\n")
        await collect(connection, local_server)

    local_server.handle = send_text
    handle = await connect_to(local_server.url, events)
    await eventually(lambda: bool(events.messages))

    assert len(events.errors) == 1
    assert isinstance(events.errors[0], ProtocolError)
    assert handle.is_open
    handle.close()
    await asyncio.wait_for(events.closed.wait(), 5)


async def test_receives_frames_larger_than_one_mebibyte(
    local_server: LocalServer,
) -> None:
    # LIMIT-01: received messages are never size-checked, unlike the
    # library's 1 MiB default.
    events = Events()
    payload_length = 3 * 1024 * 1024

    async def send_large(connection: ServerConnection) -> None:
        await connection.send(
            f"@SERVER_MSG\n:1\n${payload_length}\n".encode()
            + bytes(payload_length)
            + b"\n"
        )
        await collect(connection, local_server)

    local_server.handle = send_large
    handle = await connect_to(local_server.url, events)
    await eventually(lambda: bool(events.messages))

    message = events.messages[0]
    assert isinstance(message, NoticeFrame)
    assert len(message.payload) == payload_length
    handle.close()
    await asyncio.wait_for(events.closed.wait(), 5)


async def test_sends_queued_frames_ahead_of_the_close() -> None:
    received: list[bytes | str] = []
    finished = asyncio.Event()

    async def collect_all(connection: ServerConnection) -> None:
        async for message in connection:
            received.append(message)

        finished.set()

    async with serve(collect_all, "127.0.0.1", 0) as running:
        events = Events()
        handle = await connect_to(f"ws://127.0.0.1:{port_of(running)}", events)

        for index in range(5):
            handle.send(f"frame-{index}".encode())

        handle.close()
        await asyncio.wait_for(finished.wait(), 5)
        await asyncio.wait_for(events.closed.wait(), 5)

    assert received == [f"frame-{index}".encode() for index in range(5)]


async def test_reports_a_server_initiated_close() -> None:
    async def close_at_once(connection: ServerConnection) -> None:
        await connection.close()

    async with serve(close_at_once, "127.0.0.1", 0) as running:
        events = Events()
        await connect_to(f"ws://127.0.0.1:{port_of(running)}", events)
        await asyncio.wait_for(events.closed.wait(), 5)

    assert events.errors == []


async def test_reports_a_refused_handshake_as_a_transport_failure() -> None:
    def refuse(connection: ServerConnection, request: Request) -> Response:
        return connection.respond(HTTPStatus.UNAUTHORIZED, "no\n")

    async def unused(connection: ServerConnection) -> None:
        raise AssertionError("unreachable")

    async with serve(unused, "127.0.0.1", 0, process_request=refuse) as running:
        with pytest.raises(CelerisConnectionError) as caught:
            await connect_to(f"ws://127.0.0.1:{port_of(running)}", Events())

    assert caught.value.code == "Transport"
    assert str(caught.value).startswith("WebSocket handshake failed")


async def test_reports_an_unreachable_server_as_a_transport_failure() -> None:
    async with serve(discard, "127.0.0.1", 0) as running:
        port = port_of(running)

    with pytest.raises(CelerisConnectionError) as caught:
        await connect_to(f"ws://127.0.0.1:{port}", Events())

    assert caught.value.code == "Transport"


@dataclass(frozen=True)
class SelfSignedCertificate:
    context: ssl.SSLContext
    path: Path


@pytest.fixture
def self_signed_certificate(tmp_path: Path) -> SelfSignedCertificate:
    assert shutil.which("openssl"), "openssl is needed to generate a certificate"
    certificate = tmp_path / "certificate.pem"
    key = tmp_path / "key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(certificate),
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
    )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate, key)
    return SelfSignedCertificate(context, certificate)


async def test_rejects_a_server_certificate_it_cannot_verify(
    self_signed_certificate: SelfSignedCertificate,
) -> None:
    # SEC-02: TLS is verified against the system trust store.
    async with serve(
        discard, "127.0.0.1", 0, ssl=self_signed_certificate.context
    ) as running:
        with pytest.raises(CelerisConnectionError) as caught:
            await connect_to(f"wss://127.0.0.1:{port_of(running)}", Events())

    assert caught.value.code == "Transport"


async def test_accepts_the_same_certificate_once_it_is_trusted(
    self_signed_certificate: SelfSignedCertificate, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The control for the test above: only trust decides the outcome.
    monkeypatch.setenv("SSL_CERT_FILE", str(self_signed_certificate.path))
    events = Events()

    async with serve(
        discard, "127.0.0.1", 0, ssl=self_signed_certificate.context
    ) as running:
        handle = await connect_to(f"wss://127.0.0.1:{port_of(running)}", events)
        assert handle.is_open
        handle.close()
        await asyncio.wait_for(events.closed.wait(), 5)


async def test_closing_while_connecting_reports_error_then_close() -> None:
    order: list[str] = []
    closed = asyncio.Event()
    handshake_started = asyncio.Event()
    release_handshake = asyncio.Event()

    async def stall(connection: ServerConnection, request: Request) -> None:
        handshake_started.set()
        await release_handshake.wait()

    async with serve(discard, "127.0.0.1", 0, process_request=stall) as running:
        socket = WebSocket(f"ws://127.0.0.1:{port_of(running)}")
        socket.on_error = lambda: order.append("error")

        def record_close() -> None:
            order.append("close")
            closed.set()

        socket.on_close = record_close
        await asyncio.wait_for(handshake_started.wait(), 5)

        socket.close()
        await asyncio.wait_for(closed.wait(), 5)
        release_handshake.set()

    assert order == ["error", "close"]
    assert socket.ready_state == WebSocket.CLOSED


async def test_a_client_round_trips_a_message_through_a_local_server() -> None:
    # Echoes every PUB back as a MSG on the same segment.
    async def echo(connection: ServerConnection) -> None:
        async for frame in connection:
            assert isinstance(frame, bytes)

            if frame.startswith(b"@PUB\n"):
                await connection.send(b"@MSG\n+user\n+chat\n+echo-1\n:7\n$5\nhello\n")

    async with serve(echo, "127.0.0.1", 0) as running:
        client = create_client(
            base_url=f"ws://127.0.0.1:{port_of(running)}",
            allow_insecure_loopback=True,
            credential_provider=provide,
        )
        channel = client.channel("room-1")
        received: list[tuple[bytes, MessageMetadata]] = []
        chat = channel.segment("chat")
        chat.on_message(lambda payload, metadata: received.append((payload, metadata)))

        await channel.connect()
        chat.subscribe()
        await chat.publish(b"hello")
        await eventually(lambda: bool(received))
        await channel.close()

    assert received == [
        (
            b"hello",
            MessageMetadata(
                token_reference="user",
                segment_id="chat",
                message_id="echo-1",
                timestamp=7,
            ),
        )
    ]
    assert channel.state == "closed"


async def test_reports_the_open_before_any_frame_sent_with_the_handshake() -> None:
    # A browser resolves the open before it delivers a message. Frames the
    # server sends with its handshake response must not reach listeners while
    # the channel is still connecting.
    async def greet(connection: ServerConnection) -> None:
        await connection.send(b"@MSG\n+user\n+default\n+early-1\n:1\n$1\nx\n")
        await discard(connection)

    async with serve(greet, "127.0.0.1", 0) as running:
        channel = create_client(
            base_url=f"ws://127.0.0.1:{port_of(running)}",
            allow_insecure_loopback=True,
            credential_provider=provide,
        ).channel("room-1")
        log: list[str] = []
        channel.events().on_state_change(lambda state: log.append(f"state:{state}"))
        channel.default_segment().on_message(
            lambda payload, metadata: log.append(f"message in {channel.state}")
        )

        await channel.connect()
        await eventually(lambda: "message in connected" in log)
        await channel.close()

    assert log[:3] == ["state:connecting", "state:connected", "message in connected"]


async def test_keeps_credentials_out_of_the_logs(
    local_server: LocalServer, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    events = Events()

    handle = await connect_to(local_server.url, events)
    handle.send(b"@SUB\n$4\nchat\n")
    await eventually(lambda: bool(local_server.received))
    handle.close()
    await asyncio.wait_for(events.closed.wait(), 5)

    client_records = [
        record.getMessage()
        for record in caplog.records
        if not record.name.startswith("websockets.server")
    ]
    assert not any(
        CREDENTIALS.payload in message or CREDENTIALS.signature in message
        for message in client_records
    )
    assert not any("payload=" in message for message in client_records)
