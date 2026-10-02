import asyncio
import contextlib
from collections.abc import AsyncIterator
from urllib.parse import urlsplit

import pytest

from tests.live.helpers import (
    client_id,
    connected_channel,
    next_message,
    sign_credentials,
    signing_secret,
    unique_channel_reference,
    websocket_url,
)
from useceleris_client import (
    CredentialRequest,
    Credentials,
    MessageMetadata,
    RecoveryEvent,
    create_client,
)

pytestmark = pytest.mark.live


class DroppingProxy:
    """Forwards TCP connections to the realtime service. It can cut every
    connection at once and refuse new ones, as a network outage would."""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.refusing = False
        self._links: list[tuple[asyncio.StreamWriter, asyncio.StreamWriter]] = []

    async def link(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        if self.refusing:
            writer.transport.abort()
            return

        upstream_reader, upstream_writer = await asyncio.open_connection(
            self.host, self.port
        )
        self._links.append((writer, upstream_writer))
        await asyncio.gather(
            self._pipe(reader, upstream_writer),
            self._pipe(upstream_reader, writer),
            return_exceptions=True,
        )

    async def _pipe(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        with contextlib.suppress(ConnectionError):
            while data := await reader.read(65536):
                writer.write(data)
                await writer.drain()

        writer.transport.abort()

    def drop_all(self) -> None:
        for client_side, upstream_side in self._links:
            client_side.transport.abort()
            upstream_side.transport.abort()

        self._links.clear()


@pytest.fixture
async def proxy() -> AsyncIterator[tuple[DroppingProxy, str]]:
    target = urlsplit(websocket_url())
    dropping = DroppingProxy(target.hostname or "localhost", target.port or 80)
    server = await asyncio.start_server(dropping.link, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    async with server:
        yield dropping, f"ws://127.0.0.1:{port}"
        dropping.drop_all()


async def test_recovers_after_an_outage_with_replay_and_restored_subscriptions(
    proxy: tuple[DroppingProxy, str],
) -> None:
    dropping, proxied_url = proxy
    reference = unique_channel_reference("reconnect")
    requests: list[CredentialRequest] = []

    # The canonical mapping: a reconnect replays what the outage missed.
    async def provide(request: CredentialRequest) -> Credentials:
        requests.append(request)

        if request.replay_lookback_ms is None:
            return sign_credentials(client_id(), signing_secret())

        return sign_credentials(
            client_id(), signing_secret(), replay=request.replay_lookback_ms
        )

    receiver = create_client(
        base_url=proxied_url, allow_insecure_loopback=True, credential_provider=provide
    ).channel(reference)
    recoveries: list[RecoveryEvent] = []
    delivered: list[tuple[bytes, str]] = []
    receiver.events().on_recovery(recoveries.append)

    def record(payload: bytes, metadata: MessageMetadata) -> None:
        delivered.append((payload, metadata.message_id))

    receiver.segment("chat").on_message(record)
    receiver.segment("chat").subscribe()
    await receiver.connect()
    publisher = await connected_channel(reference)
    await asyncio.sleep(1.5)

    await publisher.segment("chat").publish(b"before")
    await next_message(
        receiver.segment("chat"),
        lambda message: message.payload == b"before",
        "the delivery before the outage",
    )

    # The outage: every connection is cut and new ones are refused for a
    # while, so the next publish can only arrive by replay.
    dropping.refusing = True
    dropping.drop_all()
    await asyncio.sleep(0.5)
    assert receiver.state == "reconnecting"
    await publisher.segment("chat").publish(b"during")
    await asyncio.sleep(2)
    dropping.refusing = False

    await next_message(
        receiver.segment("chat"),
        lambda message: message.payload == b"during",
        "the replayed delivery from the outage",
        30,
    )
    assert receiver.state == "connected"
    assert len(recoveries) == 1
    assert recoveries[0].possible_gaps
    assert recoveries[0].possible_duplicates

    reconnects = [request for request in requests if request.reason == "reconnect"]
    assert reconnects
    assert all(isinstance(request.disconnected_at, int) for request in reconnects)
    assert all(
        request.replay_lookback_ms is not None and request.replay_lookback_ms >= 5_000
        for request in reconnects
    )

    # The subscription was restored on the new connection.
    await publisher.segment("chat").publish(b"after")
    await next_message(
        receiver.segment("chat"),
        lambda message: message.payload == b"after",
        "a live delivery after recovery",
    )

    # Replay redelivered "before" too; the dedup window absorbed it.
    ids = [message_id for _, message_id in delivered]
    assert len(ids) == len(set(ids))
    assert [payload for payload, _ in delivered] == [b"before", b"during", b"after"]

    await publisher.close()
    await receiver.close()
