import asyncio
import os

import pytest

from tests.live.helpers import (
    GENERATED_MESSAGE_ID,
    connected_channel,
    next_message,
    next_presence,
    peer_websocket_url,
    started,
    unique_channel_reference,
)
from useceleris_client import Channel

pytestmark = pytest.mark.live


# The second connection opens through CELERIS_WS_URL_PEER, a gateway that
# routes to a different server node. Without it, the suite skips. Checked at
# run time, once the session fixture has read .env.
@pytest.fixture(autouse=True)
def requires_peer() -> None:
    if not os.environ.get("CELERIS_WS_URL_PEER"):
        pytest.skip("CELERIS_WS_URL_PEER is not set.")


async def test_fans_out_publishes_across_nodes() -> None:
    reference = unique_channel_reference("xnode")
    primary = await connected_channel(reference)
    secondary = await connected_channel(reference, base_url=peer_websocket_url())
    secondary.segment("chat").subscribe()
    await asyncio.sleep(2)

    await primary.segment("chat").publish(b"across")
    message = await next_message(
        secondary.segment("chat"),
        lambda received: received.payload == b"across",
        "cross-node delivery",
        25,
    )

    assert GENERATED_MESSAGE_ID.fullmatch(message.metadata.message_id)
    await primary.close()
    await secondary.close()


async def test_reports_consistent_presence_across_nodes() -> None:
    reference = unique_channel_reference("xpres")
    primary = await connected_channel(reference)
    secondary = await connected_channel(reference, base_url=peer_websocket_url())
    primary.segment("room").subscribe()
    secondary.segment("room").subscribe()
    await asyncio.sleep(3)

    from_primary = await primary.segment("room").presence_list(page=1, per_page=25)
    from_secondary = await secondary.segment("room").presence_list(page=1, per_page=25)

    assert from_primary.total == from_secondary.total
    assert from_primary.total >= 2
    await primary.close()
    await secondary.close()


async def test_delivers_a_presence_join_from_another_node() -> None:
    reference = unique_channel_reference("xpres-event")
    watcher = await connected_channel(reference)
    watcher.segment("room").subscribe_presence()
    await asyncio.sleep(2)

    joiner = await connected_channel(reference, base_url=peer_websocket_url())
    joined = asyncio.ensure_future(
        next_presence(
            watcher.segment("room"),
            lambda event: event.joined,
            "a cross-node join",
            20,
        )
    )
    await asyncio.sleep(0)
    joiner.segment("room").subscribe()

    assert (await joined).segment_id == "room"
    await joiner.close()
    await watcher.close()


async def test_delivers_a_presence_leave_from_another_node(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("xpres-leave")
    watcher = await connected_channel(reference, opened=opened)
    watcher.segment("room").subscribe_presence()
    await asyncio.sleep(2)
    leaver = await connected_channel(
        reference, base_url=peer_websocket_url(), opened=opened
    )
    joined = await started(
        next_presence(
            watcher.segment("room"),
            lambda event: event.joined,
            "the cross-node join",
            20,
        )
    )
    leaver.segment("room").subscribe()
    join = await joined

    left = await started(
        next_presence(
            watcher.segment("room"),
            lambda event: (
                not event.joined and event.connection_id == join.connection_id
            ),
            "the cross-node leave",
            20,
        )
    )
    await leaver.close()
    await left


async def test_replays_history_published_on_another_node(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("xreplay")
    publisher = await connected_channel(reference, opened=opened)

    for body in [b"h1", b"h2", b"h3"]:
        await publisher.segment("history").publish(body)

    await asyncio.sleep(2)
    receiver = await connected_channel(
        reference, base_url=peer_websocket_url(), opened=opened, replay=True
    )
    replayed: list[bytes] = []
    receiver.segment("history").on_message(
        lambda payload, metadata: replayed.append(payload)
    )
    last = await started(
        next_message(
            receiver.segment("history"),
            lambda message: message.payload == b"h3",
            "the cross-node replay",
            25,
        )
    )
    receiver.segment("history").subscribe()
    await last
    await asyncio.sleep(2)

    assert replayed == [b"h1", b"h2", b"h3"]


@pytest.mark.timeout(90)
async def test_keeps_one_origins_order_across_nodes(opened: list[Channel]) -> None:
    reference = unique_channel_reference("xorder")
    publisher = await connected_channel(reference, opened=opened)
    receiver = await connected_channel(
        reference, base_url=peer_websocket_url(), opened=opened
    )
    received: list[bytes] = []
    receiver.segment("chat").on_message(
        lambda payload, metadata: received.append(payload)
    )
    receiver.segment("chat").subscribe()
    await asyncio.sleep(2)
    bodies = [f"o{index}".encode() for index in range(30)]

    last = await started(
        next_message(
            receiver.segment("chat"),
            lambda message: message.payload == b"o29",
            "the last ordered message",
            30,
        )
    )

    for start in range(0, len(bodies), 10):
        for body in bodies[start : start + 10]:
            await publisher.segment("chat").publish(body)

        await asyncio.sleep(1.1)

    await last
    await asyncio.sleep(2)

    assert received == bodies
