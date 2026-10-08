import asyncio
from typing import Any

import pytest

from tests.live.helpers import (
    connected_channel,
    next_error,
    next_message,
    next_presence,
    started,
    unique_channel_reference,
)
from useceleris_client import Channel, ChannelError, ServerError

pytestmark = pytest.mark.live

# A token that can read "readonly", write "writeonly", and nothing else: no
# other segment, and not "default" either.
SEGMENT_PERMISSIONS: dict[str, Any] = {
    "reference": "limited",
    "token_permission": [
        {"segment_id": "readonly", "read": True, "write": False},
        {"segment_id": "writeonly", "read": False, "write": True},
    ],
}


def received(channel: Channel, segment_id: str) -> list[bytes]:
    bodies: list[bytes] = []
    channel.segment(segment_id).on_message(
        lambda payload, metadata: bodies.append(payload)
    )

    return bodies


# end function received


async def denial(
    channel: Channel, sub_type: str, segment_id: str
) -> asyncio.Future[ChannelError]:
    """Starts waiting for a permission denial that names this command and
    segment."""

    def matches(error: ChannelError) -> bool:
        return (
            isinstance(error, ServerError)
            and error.type == "PermissionDeniedError"
            and error.sub_type == sub_type
            and error.resource == segment_id
        )

    # end function matches

    return await started(
        next_error(channel, matches, f"a {sub_type} denial for {segment_id}")
    )


# end function denial


async def test_lets_a_read_only_segment_receive_and_refuses_its_publish(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("perm-read")
    publisher = await connected_channel(reference, opened=opened)
    limited = await connected_channel(reference, opened=opened, **SEGMENT_PERMISSIONS)
    readonly = received(limited, "readonly")
    limited.segment("readonly").subscribe()
    await asyncio.sleep(1.5)

    delivered = await started(
        next_message(
            limited.segment("readonly"),
            lambda message: message.payload == b"hello",
            "the read-only delivery",
        )
    )
    await publisher.segment("readonly").publish(b"hello")
    await delivered

    refused = await denial(limited, "PUB", "readonly")
    await limited.segment("readonly").publish(b"refused")
    await refused

    assert readonly == [b"hello"]
    assert limited.state == "connected"


# end function test_lets_a_read_only_segment_receive_and_refuses_its_publish


async def test_lets_a_write_only_segment_publish_and_receive_nothing(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("perm-write")
    reader = await connected_channel(reference, opened=opened)
    limited = await connected_channel(reference, opened=opened, **SEGMENT_PERMISSIONS)
    writeonly = received(limited, "writeonly")
    reader.segment("writeonly").subscribe()
    limited.segment("writeonly").subscribe()
    await asyncio.sleep(1.5)

    arrived = await started(
        next_message(
            reader.segment("writeonly"),
            lambda message: message.payload == b"from-limited",
            "the write-only publish at the reader",
        )
    )
    await limited.segment("writeonly").publish(b"from-limited")
    await arrived
    await reader.segment("writeonly").publish(b"unheard")
    await asyncio.sleep(2.5)

    assert writeonly == []


# end function test_lets_a_write_only_segment_publish_and_receive_nothing


async def test_refuses_presence_on_a_segment_without_read_access(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("perm-presence")
    limited = await connected_channel(reference, opened=opened, **SEGMENT_PERMISSIONS)

    refused_watch = await denial(limited, "PRES_SUB", "writeonly")
    limited.segment("writeonly").subscribe_presence()
    await refused_watch

    with pytest.raises(ServerError) as caught:
        await limited.segment("writeonly").presence_list(page=1, per_page=10)

    assert (caught.value.type, caught.value.sub_type) == (
        "PermissionDeniedError",
        "PRES_LIST",
    )
    page = await limited.segment("readonly").presence_list(page=1, per_page=10)

    assert page.connections == ()


# end function test_refuses_presence_on_a_segment_without_read_access


async def test_refuses_every_command_on_a_segment_the_token_does_not_list(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("perm-unlisted")
    limited = await connected_channel(reference, opened=opened, **SEGMENT_PERMISSIONS)

    refused_join = await denial(limited, "SUB", "secret")
    limited.segment("secret").subscribe()
    await refused_join
    refused_publish = await denial(limited, "PUB", "secret")
    await limited.segment("secret").publish(b"x")
    await refused_publish
    refused_watch = await denial(limited, "PRES_SUB", "secret")
    limited.segment("secret").subscribe_presence()
    await refused_watch

    assert limited.state == "connected"


# end function test_refuses_every_command_on_a_segment_the_token_does_not_list


async def test_gives_an_unlisted_default_segment_no_read_and_no_write_access(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("perm-default")
    publisher = await connected_channel(reference, opened=opened)
    limited = await connected_channel(reference, opened=opened, **SEGMENT_PERMISSIONS)
    lobby = received(limited, "default")
    readonly = received(limited, "readonly")
    limited.segment("readonly").subscribe()
    await asyncio.sleep(1.5)

    await publisher.default_segment().publish(b"unheard")
    control = await started(
        next_message(
            limited.segment("readonly"),
            lambda message: message.payload == b"control",
            "the read-only control",
        )
    )
    await publisher.segment("readonly").publish(b"control")
    await control
    refused = await denial(limited, "PUB", "default")
    await limited.default_segment().publish(b"refused")
    await refused
    await asyncio.sleep(1.5)

    assert lobby == []
    assert readonly == [b"control"]


# end function test_gives_an_unlisted_default_segment_no_read_and_no_write_access


async def test_shows_the_reference_claim_in_message_metadata_presence_events_and_lists(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("token-reference")
    watcher = await connected_channel(reference, opened=opened, reference="watcher")
    alice = await connected_channel(reference, opened=opened, reference="alice")
    watcher.segment("room").subscribe()
    watcher.segment("room").subscribe_presence()
    await asyncio.sleep(1.5)

    joined = await started(
        next_presence(
            watcher.segment("room"),
            lambda event: event.joined and event.token_reference == "alice",
            "alice's join",
        )
    )
    alice.segment("room").subscribe()
    await joined

    message = await started(
        next_message(
            watcher.segment("room"),
            lambda delivery: delivery.payload == b"hi",
            "alice's message",
        )
    )
    await alice.segment("room").publish(b"hi")

    assert (await message).metadata.token_reference == "alice"
    page = await watcher.segment("room").presence_list(page=1, per_page=10)

    assert sorted(connection.token_reference for connection in page.connections) == [
        "alice",
        "watcher",
    ]


# end function test_shows_the_reference_claim_in_message_metadata_presence_events_and_lists
