import asyncio
import time

import pytest

from tests.live.helpers import (
    connected_channel,
    next_message,
    next_presence,
    started,
    unique_channel_reference,
)
from useceleris_client import (
    Channel,
    ChannelError,
    PresenceEvent,
    PresencePage,
    ServerError,
)

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


async def test_stops_notices_after_presence_cancellation_while_subscription_stays() -> (
    None
):
    reference = unique_channel_reference("unwatch")
    watcher = await connected_channel(reference)
    watcher.segment("room").subscribe()
    watching = watcher.segment("room").subscribe_presence()
    await asyncio.sleep(1.5)

    watching.cancel()
    await asyncio.sleep(1.5)

    events: list[PresenceEvent] = []
    watcher.segment("room").on_presence(events.append)
    actor = await connected_channel(reference)
    actor.segment("room").subscribe()
    await actor.segment("room").publish(b"still-member")

    # The message subscription keeps delivering after presence stops.
    await next_message(
        watcher.segment("room"),
        lambda message: message.payload == b"still-member",
        "delivery proving the subscription stayed",
    )

    assert events == []
    await actor.close()
    await watcher.close()


def presence_events(channel: Channel, segment_id: str) -> list[PresenceEvent]:
    seen: list[PresenceEvent] = []
    channel.segment(segment_id).on_presence(seen.append)

    return seen


async def test_hides_a_connections_own_join_and_shows_it_to_a_sibling_of_the_same_token(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("presence-self")
    first = await connected_channel(reference, opened=opened, reference="alice")
    second = await connected_channel(reference, opened=opened, reference="alice")
    first_saw = presence_events(first, "room")
    second_saw = presence_events(second, "room")
    first.segment("room").subscribe_presence()
    second.segment("room").subscribe_presence()
    await asyncio.sleep(1.5)

    sibling_join = await started(
        next_presence(
            second.segment("room"), lambda event: event.joined, "the sibling's join"
        )
    )
    first.segment("room").subscribe()
    first_join = await sibling_join
    await asyncio.sleep(1.5)

    assert first_join.token_reference == "alice"
    assert first_saw == []
    assert len(second_saw) == 1


async def test_sends_a_leave_when_a_member_unsubscribes_and_stays_connected(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("presence-unsubscribe")
    watcher = await connected_channel(reference, opened=opened)
    actor = await connected_channel(reference, opened=opened, reference="actor")
    watcher.segment("room").subscribe_presence()
    await asyncio.sleep(1.5)

    joined = await started(
        next_presence(
            watcher.segment("room"),
            lambda event: event.joined and event.token_reference == "actor",
            "the actor's join",
        )
    )
    membership = actor.segment("room").subscribe()
    join = await joined
    left = await started(
        next_presence(
            watcher.segment("room"),
            lambda event: not event.joined and event.token_reference == "actor",
            "the actor's leave",
        )
    )
    membership.cancel()
    leave = await left

    assert leave.connection_id == join.connection_id
    assert actor.state == "connected"


async def test_announces_default_segment_joins_on_connect_and_leaves_on_close(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("presence-default")
    watcher = await connected_channel(reference, opened=opened, reference="watcher")
    watcher.default_segment().subscribe_presence()
    await asyncio.sleep(1.5)

    joined = await started(
        next_presence(
            watcher.default_segment(),
            lambda event: event.joined and event.token_reference == "late",
            "the join from connect",
        )
    )
    late = await connected_channel(reference, opened=opened, reference="late")
    join = await joined

    assert join.segment_id == "default"
    page = await watcher.default_segment().presence_list(page=1, per_page=10)

    assert sorted(connection.token_reference for connection in page.connections) == [
        "late",
        "watcher",
    ]
    left = await started(
        next_presence(
            watcher.default_segment(),
            lambda event: (
                not event.joined and event.connection_id == join.connection_id
            ),
            "the leave from close",
        )
    )
    await late.close()
    await left


async def test_pages_through_several_full_pages_and_past_the_end(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("presence-pages")
    members = [
        await connected_channel(reference, opened=opened, reference="m1"),
        await connected_channel(reference, opened=opened, reference="m2"),
        await connected_channel(reference, opened=opened, reference="m3"),
    ]

    for member in members:
        member.segment("room").subscribe()

    await asyncio.sleep(3)
    pages: list[PresencePage] = []

    for page in range(1, 5):
        pages.append(
            await members[0].segment("room").presence_list(page=page, per_page=1)
        )

    for index, page_of_one in enumerate(pages[:3]):
        assert (
            page_of_one.total,
            page_of_one.per_page,
            page_of_one.current_page,
            page_of_one.from_,
            page_of_one.to,
        ) == (3, 1, index + 1, index + 1, index + 1)
        assert len(page_of_one.connections) == 1

    assert (pages[3].total, pages[3].from_, pages[3].to) == (3, 4, 3)
    assert pages[3].connections == ()
    connections = [
        connection for page_of_one in pages for connection in page_of_one.connections
    ]

    assert len({connection.connection_id for connection in connections}) == 3
    assert sorted(connection.token_reference for connection in connections) == [
        "m1",
        "m2",
        "m3",
    ]
