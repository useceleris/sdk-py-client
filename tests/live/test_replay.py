import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest

from tests.live.helpers import (
    GENERATED_MESSAGE_ID,
    DeliveredMessage,
    connected_channel,
    next_message,
    qualification_client,
    unique_channel_reference,
    wait_for,
)
from useceleris_client import Channel, MessageMetadata

pytestmark = pytest.mark.live

# The server keeps at most this many messages per segment for replay.
BACKLOG_CAPACITY = 100


@pytest.fixture
async def opened() -> AsyncIterator[list[Channel]]:
    """Every channel a test opens, closed even when the test fails."""
    channels: list[Channel] = []
    yield channels

    for channel in channels:
        await channel.close()


async def open_channel(opened: list[Channel], reference: str, **claims: Any) -> Channel:
    channel = await connected_channel(reference, **claims)
    opened.append(channel)

    return channel


def received(channel: Channel, segment_id: str) -> list[tuple[bytes, str]]:
    """Payloads and message ids one segment listener receives, in arrival order."""
    deliveries: list[tuple[bytes, str]] = []
    channel.segment(segment_id).on_message(
        lambda payload, metadata: deliveries.append((payload, metadata.message_id))
    )

    return deliveries


def bodies(deliveries: list[tuple[bytes, str]]) -> list[bytes]:
    return [payload for payload, _ in deliveries]


async def arrival(
    channel: Channel, segment_id: str, body: bytes
) -> asyncio.Future[DeliveredMessage]:
    """Starts waiting for a segment listener on the channel to receive this
    payload; await the result after the action that causes the delivery."""
    delivered = asyncio.ensure_future(
        next_message(
            channel.segment(segment_id),
            lambda message: message.payload == body,
            f"{body!r} on {segment_id}",
            25,
        )
    )
    await asyncio.sleep(0)

    return delivered


async def publish_all(
    publisher: Channel, segment_id: str, message_bodies: list[bytes]
) -> None:
    for body in message_bodies:
        await publisher.segment(segment_id).publish(body)


async def test_replays_recent_messages_with_identical_ids(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("replay")
    publisher = await open_channel(opened, reference)
    live_receiver = await open_channel(opened, reference)
    live = received(live_receiver, "history")
    live_receiver.segment("history").subscribe()
    await asyncio.sleep(1.5)

    last_live = await arrival(live_receiver, "history", b"three")
    await publish_all(publisher, "history", [b"one", b"two", b"three"])
    await last_live

    # A fresh connection with a replay claim receives the same messages
    # again, ids preserved (REV-01).
    replay_receiver = await open_channel(opened, reference, replay=60_000)
    replayed = received(replay_receiver, "history")
    last_replayed = await arrival(replay_receiver, "history", b"three")
    replay_receiver.segment("history").subscribe()
    await last_replayed

    assert replayed == live
    assert bodies(replayed) == [b"one", b"two", b"three"]

    for _, message_id in replayed:
        assert GENERATED_MESSAGE_ID.fullmatch(message_id)


async def test_replays_nothing_to_a_token_without_a_replay_claim(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("no-replay")
    publisher = await open_channel(opened, reference)
    await publish_all(publisher, "history", [b"old-1", b"old-2"])
    await asyncio.sleep(1.5)

    receiver = await open_channel(opened, reference)
    history = received(receiver, "history")
    receiver.segment("history").subscribe()
    await asyncio.sleep(1.5)

    # The live message proves the join, so a replay would have arrived first.
    live = await arrival(receiver, "history", b"live")
    await publish_all(publisher, "history", [b"live"])
    await live
    await asyncio.sleep(1.5)

    assert bodies(history) == [b"live"]


async def test_replays_in_publish_order(opened: list[Channel]) -> None:
    reference = unique_channel_reference("replay-order")
    publisher = await open_channel(opened, reference)
    published = [f"m{index}".encode() for index in range(10)]
    await publish_all(publisher, "history", published)
    await asyncio.sleep(1.5)

    receiver = await open_channel(opened, reference, replay=True)
    history = received(receiver, "history")
    last = await arrival(receiver, "history", published[-1])
    receiver.segment("history").subscribe()
    await last
    await asyncio.sleep(1.5)

    assert bodies(history) == published


@pytest.mark.timeout(120)
async def test_replays_at_most_the_last_100_messages_of_a_segment(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("replay-capacity")
    publisher = await open_channel(opened, reference)
    published = [f"m{index}".encode() for index in range(BACKLOG_CAPACITY + 5)]

    # Paced below the per-second publish limit, so no publish is resent.
    for start in range(0, len(published), 10):
        await publish_all(publisher, "history", published[start : start + 10])
        await asyncio.sleep(1.1)

    receiver = await open_channel(opened, reference, replay=True)
    history = received(receiver, "history")
    last = await arrival(receiver, "history", published[-1])
    receiver.segment("history").subscribe()
    await last
    await asyncio.sleep(1.5)

    assert bodies(history) == published[-BACKLOG_CAPACITY:]


async def test_replays_only_the_window_of_a_numeric_replay_claim(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("replay-window")
    publisher = await open_channel(opened, reference)
    await publish_all(publisher, "history", [b"old"])
    await asyncio.sleep(5)
    await publish_all(publisher, "history", [b"recent"])
    await asyncio.sleep(1.5)

    receiver = await open_channel(opened, reference, replay=3_000)
    history = received(receiver, "history")
    recent = await arrival(receiver, "history", b"recent")
    receiver.segment("history").subscribe()
    await recent
    await asyncio.sleep(2.5)

    assert bodies(history) == [b"recent"]


async def test_replays_the_default_segment_on_connect(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("replay-default")
    publisher = await open_channel(opened, reference)
    await publish_all(publisher, "default", [b"d1", b"d2"])
    await asyncio.sleep(1.5)

    # The listener is in place before connect, when the server joins default.
    receiver = qualification_client(replay=True).channel(reference)
    opened.append(receiver)
    lobby = received(receiver, "default")
    last = await arrival(receiver, "default", b"d2")
    await receiver.connect()
    await last

    assert bodies(lobby) == [b"d1", b"d2"]


async def test_replays_each_segments_backlog_when_that_segment_is_joined(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("replay-per-join")
    publisher = await open_channel(opened, reference)
    await publish_all(publisher, "alpha", [b"a1"])
    await publish_all(publisher, "beta", [b"b1"])
    await asyncio.sleep(1.5)

    receiver = await open_channel(opened, reference, replay=True)
    alpha = received(receiver, "alpha")
    beta = received(receiver, "beta")
    alpha_replay = await arrival(receiver, "alpha", b"a1")
    receiver.segment("alpha").subscribe()
    await alpha_replay
    await asyncio.sleep(1.5)
    assert bodies(beta) == []

    beta_replay = await arrival(receiver, "beta", b"b1")
    receiver.segment("beta").subscribe()
    await beta_replay

    assert bodies(alpha) == [b"a1"]
    assert bodies(beta) == [b"b1"]


async def test_replays_to_a_segment_joined_by_publishing(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("replay-publish-join")
    publisher = await open_channel(opened, reference)
    await publish_all(publisher, "history", [b"h1"])
    await asyncio.sleep(1.5)

    receiver = await open_channel(opened, reference, replay=True)
    history = received(receiver, "history")
    replay = await arrival(receiver, "history", b"h1")
    await receiver.segment("history").publish(b"joining")
    await replay
    await asyncio.sleep(1.5)

    assert bodies(history) == [b"h1"]


async def test_recovers_what_a_rejoin_missed_and_drops_what_it_already_delivered(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("replay-rejoin")
    publisher = await open_channel(opened, reference)
    receiver = await open_channel(opened, reference, replay=True)
    history = received(receiver, "history")
    first = receiver.segment("history").subscribe()
    await asyncio.sleep(1.5)
    one = await arrival(receiver, "history", b"one")
    await publish_all(publisher, "history", [b"one"])
    await one

    first.cancel()
    await asyncio.sleep(1.5)
    await publish_all(publisher, "history", [b"two"])
    await asyncio.sleep(1.5)

    # The re-join replays "one" and "two"; the dedup window drops "one".
    two = await arrival(receiver, "history", b"two")
    receiver.segment("history").subscribe()
    await two
    three = await arrival(receiver, "history", b"three")
    await publish_all(publisher, "history", [b"three"])
    await three
    await asyncio.sleep(1.5)

    assert bodies(history) == [b"one", b"two", b"three"]
    assert len({message_id for _, message_id in history}) == 3


async def test_replays_to_the_channel_listener_one_time_for_each_message(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("replay-channel-listener")
    publisher = await open_channel(opened, reference)
    await publish_all(publisher, "history", [b"h1", b"h2"])
    await asyncio.sleep(1.5)

    receiver = await open_channel(opened, reference, replay=True)
    seen: list[str] = []

    def record(payload: bytes, metadata: MessageMetadata) -> None:
        seen.append(f"{metadata.segment_id}:{payload.decode()}")

    receiver.events().on_message(record)

    def register(deliver: Callable[[list[str]], None]) -> Callable[[], None]:
        return receiver.events().on_message(lambda payload, metadata: deliver(seen))

    both = asyncio.ensure_future(
        wait_for(
            register,
            lambda current: len(current) >= 2,
            "both replayed messages",
            25,
        )
    )
    await asyncio.sleep(0)
    membership = receiver.segment("history").subscribe()
    await both

    # A re-join replays both again; the channel listener sees neither twice.
    membership.cancel()
    await asyncio.sleep(1.5)
    receiver.segment("history").subscribe()
    await asyncio.sleep(4)

    assert seen == ["history:h1", "history:h2"]
