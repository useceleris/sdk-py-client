import asyncio
import time

import pytest

from tests.live.helpers import (
    connected_channel,
    next_message,
    next_presence,
    unique_channel_reference,
)
from useceleris_client import ChannelError, PresenceEvent, ServerError

pytestmark = pytest.mark.live


async def test_delivers_typed_join_and_leave_notifications_to_watchers() -> None:
    reference = unique_channel_reference("watch")
    watcher = await connected_channel(reference)
    watched = watcher.segment("room")
    watched.subscribe_presence()
    await asyncio.sleep(1.5)

    actor = await connected_channel(reference)
    actor.segment("room").subscribe()

    join = await next_presence(
        watched, lambda event: event.joined, "a typed join notification"
    )
    assert join.segment_id == "room"
    assert join.token_reference != ""
    assert join.connection_id != ""
    assert isinstance(join.timestamp, int)
    assert join.timestamp > 0

    # The same connection leaving produces the mirror event. The waiter is
    # registered before the close, because the notification can arrive while
    # close() is still settling.
    leaving = asyncio.ensure_future(
        next_presence(
            watched,
            lambda event: (
                not event.joined and event.connection_id == join.connection_id
            ),
            "a typed leave notification",
        )
    )
    await asyncio.sleep(0)
    await actor.close()
    leave = await leaving

    assert leave.segment_id == "room"
    assert leave.token_reference == join.token_reference
    await watcher.close()


async def test_pages_presence_snapshots_with_raw_metadata() -> None:
    reference = unique_channel_reference("plist")
    first = await connected_channel(reference)
    second = await connected_channel(reference)
    first.segment("room").subscribe()
    second.segment("room").subscribe()
    await asyncio.sleep(2.5)

    page = await first.segment("room").presence_list(page=1, per_page=10)

    assert page.segment_id == "room"
    assert page.total >= 2
    assert len(page.connections) >= 2
    assert page.from_ == 1

    for connection in page.connections:
        assert connection.connection_id != ""
        assert isinstance(connection.timestamp, int)

    beyond = await first.segment("room").presence_list(page=50, per_page=10)

    assert beyond.connections == ()
    assert beyond.from_ > beyond.to
    await first.close()
    await second.close()


async def test_rejects_a_write_only_tokens_presence_query_at_once() -> None:
    write_only = await connected_channel(
        unique_channel_reference("pdeny"),
        token_permission={"read": False, "write": True},
    )
    errors: list[ChannelError] = []
    write_only.events().on_error(errors.append)

    # Presence needs read access. The denial names the query by its request
    # id, so the caller hears it at once instead of waiting out the deadline.
    started = time.monotonic()

    with pytest.raises(ServerError) as caught:
        await write_only.segment("room").presence_list(page=1, per_page=10)

    assert time.monotonic() - started < 2
    assert (caught.value.type, caught.value.sub_type, caught.value.resource) == (
        "PermissionDeniedError",
        "PRES_LIST",
        "1",
    )
    assert errors == []
    assert write_only.state == "connected"
    await write_only.close()


async def test_stops_notices_after_presence_cancellation_while_membership_holds() -> (
    None
):
    reference = unique_channel_reference("unwatch")
    watcher = await connected_channel(reference)
    watching = watcher.segment("room").subscribe_presence()
    watcher.segment("room").on_message(lambda payload, metadata: None)
    await asyncio.sleep(1.5)

    watching.cancel()
    await asyncio.sleep(1.5)

    events: list[PresenceEvent] = []
    watcher.segment("room").on_presence(events.append)
    actor = await connected_channel(reference)
    actor.segment("room").subscribe()
    await actor.segment("room").publish(b"still-member")

    # Membership persisted (PRES_SUB force-joined): the message arrives even
    # though presence notices no longer do.
    await next_message(
        watcher.segment("room"),
        lambda message: message.payload == b"still-member",
        "delivery proving persistent membership",
    )

    assert events == []
    await actor.close()
    await watcher.close()
