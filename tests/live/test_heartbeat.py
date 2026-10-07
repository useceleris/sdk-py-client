import asyncio
import time

import pytest

from tests.live.helpers import (
    DroppingProxy,
    client_id,
    connected_channel,
    next_message,
    sign_credentials,
    signing_secret,
    started,
    unique_channel_reference,
    wait_for,
)
from useceleris_client import (
    Channel,
    ChannelState,
    CredentialRequest,
    Credentials,
    create_client,
)

pytestmark = pytest.mark.live


# HEARTBEAT-01: the server closes a connection it has heard no ping or pong
# from for 60 seconds. A listener holding delivery for 90 seconds keeps its
# connection, and the time it holds delivery never counts against a ping.
@pytest.mark.timeout(180)
async def test_a_listener_blocking_ninety_seconds_keeps_its_connection(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("heartbeat")
    blocking = await connected_channel(reference, opened=opened)
    peer = await connected_channel(reference, opened=opened)
    blocking.segment("chat").subscribe()
    peer.segment("chat").subscribe()
    states: list[ChannelState] = []
    blocking.events().on_state_change(states.append)
    loop = asyncio.get_running_loop()
    listener_started: asyncio.Future[None] = loop.create_future()
    listener_returned: asyncio.Future[None] = loop.create_future()

    def block(payload: bytes) -> None:
        if payload != b"block" or listener_started.done():
            return

        listener_started.set_result(None)
        time.sleep(90)
        listener_returned.set_result(None)

    blocking.segment("chat").on_message(lambda payload, metadata: block(payload))
    await asyncio.sleep(1.5)

    await peer.segment("chat").publish(b"block")
    await asyncio.wait_for(listener_started, 15)
    await asyncio.wait_for(listener_returned, 120)

    # A close the server sent while the listener ran is read now.
    await asyncio.sleep(3)

    assert states == []
    after = await started(
        next_message(
            peer.segment("chat"),
            lambda message: message.payload == b"after",
            "the publish after the listener returned",
        )
    )
    await blocking.segment("chat").publish(b"after")
    await after

    assert blocking.state == "connected"


# HEARTBEAT-01: a path that silently stops carrying anything, while TCP to the
# proxy stays up, is found dead by the websockets library's keepalive. It
# pings every 20 s and fails the connection when a pong is 20 s late, then
# waits up to 10 s for TCP to close before it aborts. So the channel starts
# recovering 30 to 50 seconds after the path died, and delivers again.
@pytest.mark.timeout(120)
async def test_a_silently_dead_path_is_found_and_recovered(
    proxy: DroppingProxy, opened: list[Channel]
) -> None:
    reference = unique_channel_reference("blackhole")
    reconnects: list[CredentialRequest] = []

    async def provide(request: CredentialRequest) -> Credentials:
        if request.reason == "reconnect":
            reconnects.append(request)

        return sign_credentials(client_id(), signing_secret())

    channel = create_client(
        base_url=proxy.url, allow_insecure_loopback=True, credential_provider=provide
    ).channel(reference)
    opened.append(channel)
    channel.segment("chat").subscribe()
    await channel.connect()
    publisher = await connected_channel(reference, opened=opened)
    changes: list[tuple[ChannelState, float]] = []
    channel.events().on_state_change(
        lambda state: changes.append((state, time.monotonic()))
    )

    reconnected = await started(
        wait_for(
            channel.events().on_state_change,
            lambda state: state == "connected",
            "the reconnected state",
            65,
        )
    )
    proxy.blackhole_open_links()
    died_at = time.monotonic()
    await reconnected

    assert [state for state, _ in changes] == ["reconnecting", "connected"]
    # A ping in flight when the path died counts from when it was sent.
    assert changes[0][1] - died_at >= 30 - 0.5
    assert len(reconnects) >= 1
    await asyncio.sleep(1.5)

    after = await started(
        next_message(
            channel.segment("chat"),
            lambda message: message.payload == b"after",
            "the delivery after the recovery",
        )
    )
    await publisher.segment("chat").publish(b"after")
    await after

    assert channel.state == "connected"
