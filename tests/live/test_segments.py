import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest

from tests.live.helpers import (
    connected_channel,
    next_error,
    next_message,
    qualification_client,
    unique_channel_reference,
    wait_for,
)
from useceleris_client import Channel, MessageMetadata, ServerError

pytestmark = pytest.mark.live


def received(channel: Channel, segment_id: str) -> list[bytes]:
    """The payloads one segment listener receives."""
    payloads: list[bytes] = []
    channel.segment(segment_id).on_message(
        lambda payload, metadata: payloads.append(payload)
    )

    return payloads


# end function received


async def publish_and_await(
    publisher: Channel, receiver: Channel, segment_id: str, body: bytes
) -> None:
    """Publishes and waits until the receiver's segment delivers that payload."""
    delivered = asyncio.ensure_future(
        next_message(
            receiver.segment(segment_id),
            lambda message: message.payload == body,
            f"{body!r} on {segment_id}",
        )
    )
    await asyncio.sleep(0)

    await publisher.segment(segment_id).publish(body)
    await delivered


# end function publish_and_await


async def confirm_quiet(publisher: Channel, receiver: Channel) -> None:
    """A negative check needs proof the connection was live: a control message
    on the default segment, which always delivers, then time for a stray
    delivery to land."""
    await publish_and_await(publisher, receiver, "default", b"control")
    await asyncio.sleep(2.5)


# end function confirm_quiet


@pytest.fixture
async def opened() -> AsyncIterator[list[Channel]]:
    """Every channel a test opens, closed even when the test fails."""
    channels: list[Channel] = []
    yield channels

    for channel in channels:
        await channel.close()


# end function opened


async def pair(
    opened: list[Channel], label: str, **receiver_claims: Any
) -> tuple[Channel, Channel]:
    reference = unique_channel_reference(label)
    publisher = await connected_channel(reference)
    opened.append(publisher)
    receiver = await connected_channel(reference, **receiver_claims)
    opened.append(receiver)

    return publisher, receiver


# end function pair


class TestReceiving:
    async def test_delivers_nothing_to_a_listener_without_a_subscription(
        self, opened: list[Channel]
    ) -> None:
        publisher, receiver = await pair(opened, "listener-only")
        chat = received(receiver, "chat")

        await publisher.segment("chat").publish(b"unheard")
        await confirm_quiet(publisher, receiver)

        assert chat == []

    # end method test_delivers_nothing_to_a_listener_without_a_subscription

    async def test_delivers_after_a_subscription_made_before_connecting(
        self, opened: list[Channel]
    ) -> None:
        reference = unique_channel_reference("before-connect")
        publisher = await connected_channel(reference)
        opened.append(publisher)
        receiver = qualification_client().channel(reference)
        opened.append(receiver)
        chat = received(receiver, "chat")
        receiver.segment("chat").subscribe()
        await receiver.connect()
        await asyncio.sleep(1.5)

        await publish_and_await(publisher, receiver, "chat", b"hello")

        assert chat == [b"hello"]

    # end method test_delivers_after_a_subscription_made_before_connecting

    async def test_delivers_after_a_subscription_made_once_connected(
        self, opened: list[Channel]
    ) -> None:
        publisher, receiver = await pair(opened, "after-connect")
        chat = received(receiver, "chat")
        receiver.segment("chat").subscribe()
        await asyncio.sleep(1.5)

        await publish_and_await(publisher, receiver, "chat", b"hello")

        assert chat == [b"hello"]

    # end method test_delivers_after_a_subscription_made_once_connected

    async def test_delivers_to_a_listener_attached_after_subscribing(
        self, opened: list[Channel]
    ) -> None:
        publisher, receiver = await pair(opened, "listener-later")
        receiver.segment("chat").subscribe()
        await asyncio.sleep(1.5)
        chat = received(receiver, "chat")

        await publish_and_await(publisher, receiver, "chat", b"hello")

        assert chat == [b"hello"]

    # end method test_delivers_to_a_listener_attached_after_subscribing


# end class TestReceiving


class TestLeaving:
    async def test_stops_on_cancel_and_resumes_on_a_new_subscription(
        self, opened: list[Channel]
    ) -> None:
        publisher, receiver = await pair(opened, "rejoin")
        chat = received(receiver, "chat")
        first = receiver.segment("chat").subscribe()
        await asyncio.sleep(1.5)
        await publish_and_await(publisher, receiver, "chat", b"one")

        first.cancel()
        await asyncio.sleep(1.5)
        await publisher.segment("chat").publish(b"two")
        await confirm_quiet(publisher, receiver)
        assert chat == [b"one"]

        receiver.segment("chat").subscribe()
        await asyncio.sleep(1.5)
        await publish_and_await(publisher, receiver, "chat", b"three")

        assert chat == [b"one", b"three"]

    # end method test_stops_on_cancel_and_resumes_on_a_new_subscription

    async def test_leaves_only_when_the_last_handle_cancels(
        self, opened: list[Channel]
    ) -> None:
        publisher, receiver = await pair(opened, "refcount")
        chat = received(receiver, "chat")
        first = receiver.segment("chat").subscribe()
        second = receiver.segment("chat").subscribe()
        await asyncio.sleep(1.5)

        first.cancel()
        await asyncio.sleep(1.5)
        await publish_and_await(publisher, receiver, "chat", b"one")

        second.cancel()
        await asyncio.sleep(1.5)
        await publisher.segment("chat").publish(b"two")
        await confirm_quiet(publisher, receiver)

        assert chat == [b"one"]

    # end method test_leaves_only_when_the_last_handle_cancels

    async def test_routes_each_segments_messages_to_its_own_listeners_only(
        self, opened: list[Channel]
    ) -> None:
        publisher, receiver = await pair(opened, "demux")
        alpha = received(receiver, "alpha")
        beta = received(receiver, "beta")
        receiver.segment("alpha").subscribe()
        beta_membership = receiver.segment("beta").subscribe()
        await asyncio.sleep(1.5)

        await publish_and_await(publisher, receiver, "alpha", b"a")
        await publish_and_await(publisher, receiver, "beta", b"b")
        assert alpha == [b"a"]
        assert beta == [b"b"]

        beta_membership.cancel()
        await asyncio.sleep(1.5)
        await publisher.segment("beta").publish(b"late")
        await publish_and_await(publisher, receiver, "alpha", b"still")
        await asyncio.sleep(2.5)

        assert alpha == [b"a", b"still"]
        assert beta == [b"b"]

    # end method test_routes_each_segments_messages_to_its_own_listeners_only

    async def test_keeps_other_listeners_and_the_subscription_when_one_stops(
        self, opened: list[Channel]
    ) -> None:
        publisher, receiver = await pair(opened, "dispose")
        stopped: list[bytes] = []
        stop_listening = receiver.segment("chat").on_message(
            lambda payload, metadata: stopped.append(payload)
        )
        kept = received(receiver, "chat")
        receiver.segment("chat").subscribe()
        await asyncio.sleep(1.5)
        await publish_and_await(publisher, receiver, "chat", b"one")

        stop_listening()
        await publish_and_await(publisher, receiver, "chat", b"two")

        assert stopped == [b"one"]
        assert kept == [b"one", b"two"]

    # end method test_keeps_other_listeners_and_the_subscription_when_one_stops


# end class TestLeaving


# A publish is not a membership: only a subscription receives (SEG-01).
class TestAPublishDoesNotJoin:
    async def test_delivers_nothing_for_a_publish_alone_and_delivers_once_subscribed(
        self, opened: list[Channel]
    ) -> None:
        publisher, receiver = await pair(opened, "publish-no-join")
        chat = received(receiver, "chat")
        publisher.segment("chat").subscribe()
        await asyncio.sleep(1.5)

        # The publish lands, which proves that it ran on the server.
        await publish_and_await(receiver, publisher, "chat", b"from-receiver")
        await publisher.segment("chat").publish(b"unheard")
        await confirm_quiet(publisher, receiver)
        assert chat == []

        receiver.segment("chat").subscribe()
        await asyncio.sleep(1.5)
        await publish_and_await(publisher, receiver, "chat", b"heard")

        assert chat == [b"heard"]

    # end method test_delivers_nothing_for_a_publish_alone_and_delivers_once_subscribed

    async def test_gives_no_echo_on_a_named_segment_before_a_subscription(
        self, opened: list[Channel]
    ) -> None:
        reference = unique_channel_reference("publish-no-echo")
        publisher = await connected_channel(reference)
        opened.append(publisher)
        echoing = await connected_channel(reference, allow_echo=True)
        opened.append(echoing)
        chat = received(echoing, "chat")

        await echoing.segment("chat").publish(b"unheard")
        await confirm_quiet(publisher, echoing)
        assert chat == []

        echoing.segment("chat").subscribe()
        await asyncio.sleep(1.5)
        await publish_and_await(echoing, echoing, "chat", b"echoed")

        assert chat == [b"echoed"]

    # end method test_gives_no_echo_on_a_named_segment_before_a_subscription

    async def test_does_not_list_a_connection_that_only_publishes_in_presence(
        self, opened: list[Channel]
    ) -> None:
        publisher, receiver = await pair(opened, "publish-no-presence")
        publisher.segment("chat").subscribe()
        await asyncio.sleep(1.5)

        await publish_and_await(receiver, publisher, "chat", b"from-receiver")
        await asyncio.sleep(3)

        page = await publisher.segment("chat").presence_list(page=1, per_page=25)

        assert len(page.connections) == 1

    # end method test_does_not_list_a_connection_that_only_publishes_in_presence

    # Watching presence is not membership: it neither joins nor holds.
    async def test_never_joins_or_holds_a_segment_for_a_presence_subscription(
        self, opened: list[Channel]
    ) -> None:
        publisher, receiver = await pair(opened, "presence-watch")
        chat = received(receiver, "chat")
        receiver.segment("chat").subscribe_presence()
        await asyncio.sleep(1.5)
        await publisher.segment("chat").publish(b"unheard")
        await confirm_quiet(publisher, receiver)
        assert chat == []

        messages = receiver.segment("chat").subscribe()
        await asyncio.sleep(1.5)
        await publish_and_await(publisher, receiver, "chat", b"one")

        messages.cancel()
        await asyncio.sleep(1.5)
        await publisher.segment("chat").publish(b"two")
        await confirm_quiet(publisher, receiver)

        assert chat == [b"one"]

    # end method test_never_joins_or_holds_a_segment_for_a_presence_subscription


# end class TestAPublishDoesNotJoin


# Write access publishes and read access subscribes. The two are separate.
class TestTokenPermissions:
    async def test_refuses_a_write_only_tokens_subscription_and_delivers_its_publish(
        self, opened: list[Channel]
    ) -> None:
        publisher, receiver = await pair(
            opened,
            "publish-write-only",
            token_permission={"read": False, "write": True},
        )
        chat = received(receiver, "chat")
        publisher.segment("chat").subscribe()
        await asyncio.sleep(1.5)

        denial = asyncio.ensure_future(
            next_error(
                receiver,
                lambda error: (
                    isinstance(error, ServerError)
                    and error.type == "PermissionDeniedError"
                    and error.sub_type == "SUB"
                    and error.resource == "chat"
                ),
                "the subscription denial",
            )
        )
        await asyncio.sleep(0)
        receiver.segment("chat").subscribe()
        await denial

        await publish_and_await(receiver, publisher, "chat", b"written")
        await publisher.segment("chat").publish(b"unheard")
        await asyncio.sleep(2.5)

        assert chat == []
        assert receiver.state == "connected"

    # end method test_refuses_a_write_only_tokens_subscription_and_delivers_its_publish

    async def test_refuses_a_read_only_tokens_publish_and_delivers_once_subscribed(
        self, opened: list[Channel]
    ) -> None:
        publisher, receiver = await pair(
            opened,
            "publish-read-only",
            token_permission={"read": True, "write": False},
        )
        chat = received(receiver, "chat")

        denial = asyncio.ensure_future(
            next_error(
                receiver,
                lambda error: (
                    isinstance(error, ServerError)
                    and error.type == "PermissionDeniedError"
                ),
                "the publish denial",
            )
        )
        await asyncio.sleep(0)
        await receiver.segment("chat").publish(b"refused")
        await denial
        await publisher.segment("chat").publish(b"unheard")
        await confirm_quiet(publisher, receiver)
        assert chat == []

        receiver.segment("chat").subscribe()
        await asyncio.sleep(1.5)
        await publish_and_await(publisher, receiver, "chat", b"heard")

        assert chat == [b"heard"]

    # end method test_refuses_a_read_only_tokens_publish_and_delivers_once_subscribed


# end class TestTokenPermissions


# One channel is one WebSocket; its segments share it (SEG-01).
class TestConnections:
    async def test_multiplexes_a_channels_segments_over_one_connection(
        self, opened: list[Channel]
    ) -> None:
        reference = unique_channel_reference("multiplex")
        publisher = await connected_channel(reference)
        opened.append(publisher)
        receiver = await connected_channel(reference)
        opened.append(receiver)
        receiver.segment("alpha").subscribe()
        receiver.segment("beta").subscribe()
        await asyncio.sleep(3)

        async def connections_in(segment_id: str) -> list[str]:
            page = await publisher.segment(segment_id).presence_list(
                page=1, per_page=25
            )

            return [connection.connection_id for connection in page.connections]

        # end function connections_in

        alpha = await connections_in("alpha")
        assert len(alpha) == 1
        assert await connections_in("beta") == alpha

        second = await connected_channel(reference)
        opened.append(second)
        second.segment("alpha").subscribe()
        await asyncio.sleep(3)

        both = await connections_in("alpha")
        assert len(both) == 2
        assert len(set(both)) == 2

    # end method test_multiplexes_a_channels_segments_over_one_connection


# end class TestConnections


class TestChannelWideListener:
    async def test_removes_one_channel_listener_and_leaves_every_other_listener(
        self, opened: list[Channel]
    ) -> None:
        publisher, receiver = await pair(opened, "channel-listener-remove")
        removed: list[bytes] = []
        kept: list[bytes] = []
        remove_channel_listener = receiver.events().on_message(
            lambda payload, metadata: removed.append(payload)
        )
        receiver.events().on_message(lambda payload, metadata: kept.append(payload))
        chat = received(receiver, "chat")
        receiver.segment("chat").subscribe()
        await asyncio.sleep(1.5)
        await publish_and_await(publisher, receiver, "chat", b"one")

        remove_channel_listener()
        await publish_and_await(publisher, receiver, "chat", b"two")

        assert removed == [b"one"]
        assert kept == [b"one", b"two"]
        assert chat == [b"one", b"two"]

    # end method test_removes_one_channel_listener_and_leaves_every_other_listener

    async def test_catches_deliveries_no_segment_listener_asked_for(
        self, opened: list[Channel]
    ) -> None:
        publisher, receiver = await pair(opened, "channel-listener")
        seen: list[str] = []

        def record(payload: bytes, metadata: MessageMetadata) -> None:
            seen.append(f"{metadata.segment_id}:{payload.decode()}")

        # end function record

        receiver.events().on_message(record)
        # A subscription with no segment listener, and the default segment.
        receiver.segment("unlistened").subscribe()
        await asyncio.sleep(1.5)

        def register(deliver: Callable[[list[str]], None]) -> Callable[[], None]:
            return receiver.events().on_message(lambda payload, metadata: deliver(seen))

        # end function register

        both = asyncio.ensure_future(
            wait_for(
                register,
                lambda current: len(current) >= 2,
                "both channel-wide deliveries",
            )
        )
        await asyncio.sleep(0)
        await publisher.segment("unlistened").publish(b"x")
        await publisher.default_segment().publish(b"y")
        await both

        assert sorted(seen) == ["default:y", "unlistened:x"]

    # end method test_catches_deliveries_no_segment_listener_asked_for


# end class TestChannelWideListener
