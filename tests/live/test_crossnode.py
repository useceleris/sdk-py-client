import asyncio

import pytest

from tests.live.helpers import (
    GENERATED_MESSAGE_ID,
    connected_channel,
    next_message,
    unique_channel_reference,
)

pytestmark = pytest.mark.live

# Connections are opened through the configured entrypoint, which distributes
# them across the stack's nodes; these assert fan-out and presence consistency
# between independent connections.


async def test_fans_out_publishes_across_nodes() -> None:
    reference = unique_channel_reference("xnode")
    primary = await connected_channel(reference)
    secondary = await connected_channel(reference)
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
    secondary = await connected_channel(reference)
    primary.segment("room").subscribe()
    secondary.segment("room").subscribe()
    await asyncio.sleep(3)

    from_primary = await primary.segment("room").presence_list(page=1, per_page=25)
    from_secondary = await secondary.segment("room").presence_list(page=1, per_page=25)

    assert from_primary.total == from_secondary.total
    assert from_primary.total >= 2
    await primary.close()
    await secondary.close()
