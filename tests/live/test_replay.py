import asyncio

import pytest

from tests.live.helpers import (
    GENERATED_MESSAGE_ID,
    connected_channel,
    next_message,
    unique_channel_reference,
)
from useceleris_client import MessageMetadata

pytestmark = pytest.mark.live


async def test_replays_recent_messages_with_identical_ids() -> None:
    reference = unique_channel_reference("replay")
    publisher = await connected_channel(reference)
    live_receiver = await connected_channel(reference)
    live_ids: list[str] = []
    live_receiver.segment("history").on_message(
        lambda payload, metadata: live_ids.append(metadata.message_id)
    )
    live_receiver.segment("history").subscribe()
    await asyncio.sleep(1.5)

    for body in [b"one", b"two", b"three"]:
        await publisher.segment("history").publish(body)

    await next_message(
        live_receiver.segment("history"),
        lambda message: len(live_ids) >= 3,
        "the three live deliveries",
        20,
    )
    await live_receiver.close()

    # A fresh connection with a replay lookback receives the same messages
    # again, ids preserved (REV-01).
    replay_receiver = await connected_channel(reference, replay=60_000)
    replayed: dict[bytes, str] = {}

    def record(payload: bytes, metadata: MessageMetadata) -> None:
        replayed[payload] = metadata.message_id

    replay_receiver.segment("history").on_message(record)
    replay_receiver.segment("history").subscribe()
    await next_message(
        replay_receiver.segment("history"),
        lambda message: len(replayed) >= 3,
        "the replayed history",
        25,
    )

    for index, body in enumerate([b"one", b"two", b"three"]):
        assert replayed[body] == live_ids[index]
        assert GENERATED_MESSAGE_ID.fullmatch(replayed[body])

    await publisher.close()
    await replay_receiver.close()


async def test_keeps_the_dedup_window_effective_against_overlapping_replay() -> None:
    reference = unique_channel_reference("dedup")
    publisher = await connected_channel(reference)
    receiver = await connected_channel(reference, replay=60_000)
    delivered: list[str] = []
    receiver.segment("history").on_message(
        lambda payload, metadata: delivered.append(metadata.message_id)
    )
    membership = receiver.segment("history").subscribe()
    await asyncio.sleep(1.5)

    await publisher.segment("history").publish(b"first")
    await next_message(
        receiver.segment("history"),
        lambda message: len(delivered) >= 1,
        "the live delivery",
        20,
    )

    # Re-join the segment on the same channel: the token's replay window
    # redelivers history; the client's dedup window absorbs it.
    membership.cancel()
    await asyncio.sleep(1.5)
    receiver.segment("history").subscribe()
    await asyncio.sleep(4)

    assert len(set(delivered)) == len(delivered)
    await publisher.close()
    await receiver.close()
