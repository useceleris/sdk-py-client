import asyncio
from collections.abc import Callable
from typing import Any

import pytest

from tests.helpers.channel import (
    ChannelSetup,
    create_test_channel,
    error_frame,
    establish,
    message_frame,
)
from tests.helpers.tasks import failure_of, flush
from tests.helpers.timers import FakeTimers
from tests.helpers.websocket import FakeWebSocket
from useceleris_client import (
    CelerisConnectionError,
    ChannelError,
    ConfigurationError,
    MessageMetadata,
    ProtocolError,
    Segment,
    ServerError,
)

LISTENER_FAILURE = (
    "A listener callback raised an error; the channel caught it and kept running."
)


@pytest.fixture
async def setup(sockets: list[FakeWebSocket], timers: FakeTimers) -> ChannelSetup:
    return await establish(create_test_channel(timers), sockets)


# end function setup


def bump_buffer(socket: FakeWebSocket) -> None:
    def bump(data: bytes) -> None:
        socket.buffered_amount += 1

    # end function bump

    socket.send.side_effect = bump


# end function bump_buffer


def record_ids(segment: Segment) -> list[str]:
    delivered: list[str] = []

    def record(payload: bytes, metadata: MessageMetadata) -> None:
        delivered.append(metadata.message_id)

    # end function record

    segment.on_message(record)
    return delivered


# end function record_ids


def publish(segment: Segment, payload: bytes, **options: Any) -> "asyncio.Task[None]":
    return asyncio.ensure_future(segment.publish(payload, **options))


# end function publish


class TestSegmentProxies:
    async def test_creates_side_effect_free_stateless_proxies(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        first = setup.channel.segment("chat")
        second = setup.channel.segment("chat")

        assert isinstance(first, Segment)
        assert first is not second
        assert first.segment_id == "chat"
        assert setup.channel.default_segment().segment_id == "default"
        sockets[-1].send.assert_not_called()

    # end method test_creates_side_effect_free_stateless_proxies

    @pytest.mark.parametrize("identifier", ["", "bad\nid", "bad\rid", "\ud800", None])
    async def test_rejects_invalid_segment_identifiers(
        self, timers: FakeTimers, identifier: Any
    ) -> None:
        with pytest.raises(ConfigurationError, match=r"^Invalid segment ID\. "):
            create_test_channel(timers).channel.segment(identifier)

    # end method test_rejects_invalid_segment_identifiers

    async def test_shares_one_interest_count_across_instances(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        first = setup.channel.segment("chat").subscribe()
        second = setup.channel.segment("chat").subscribe()
        assert sockets[-1].sent_frames() == ["@SUB\n$4\nchat\n"]

        first.cancel()
        first.cancel()
        assert sockets[-1].sent_frames() == ["@SUB\n$4\nchat\n"]

        second.cancel()
        assert sockets[-1].sent_frames() == ["@SUB\n$4\nchat\n", "@UNSUB\n$4\nchat\n"]

    # end method test_shares_one_interest_count_across_instances

    async def test_multiplexes_segments_over_one_socket_another_channel_opens_another(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup.channel.segment("alpha").subscribe()
        setup.channel.segment("beta").subscribe()
        await setup.channel.segment("gamma").publish(b"x")
        assert len(sockets) == 1
        assert len(sockets[-1].sent_frames()) == 3

        await establish(
            create_test_channel(timers, channel_reference="room-2"), sockets
        )
        assert len(sockets) == 2

    # end method test_multiplexes_segments_over_one_socket_another_channel_opens_another

    async def test_never_sends_sub_or_unsub_for_the_default_segment(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        setup.channel.default_segment().subscribe().cancel()

        sockets[-1].send.assert_not_called()

    # end method test_never_sends_sub_or_unsub_for_the_default_segment


# end class TestSegmentProxies


class TestPublish:
    async def test_returns_on_local_acceptance_with_exact_bytes_and_a_generated_id(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        await setup.channel.default_segment().publish(b"hi")
        await setup.channel.segment("chat").publish(b"yo", message_id="m-1")

        assert sockets[-1].sent_frames() == [
            "@PUB\n$7\ndefault\n$11\ngenerated-1\n$2\nhi\n",
            "@PUB\n$4\nchat\n$3\nm-1\n$2\nyo\n",
        ]

    # end method test_returns_on_local_acceptance_with_exact_bytes_and_a_generated_id

    async def test_rejects_a_publish_with_not_connected_outside_recovery(
        self, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup = create_test_channel(timers, maximum_reconnect_attempts=1)
        segment = setup.channel.segment("chat")

        async def assert_refused(state: str) -> None:
            error = await failure_of(publish(segment, b"x"))
            assert isinstance(error, CelerisConnectionError)
            assert (error.code, str(error)) == (
                "NotConnected",
                f"Channel is not connected; it is {state}.",
            )

        # end function assert_refused

        await assert_refused("idle")

        connecting = asyncio.ensure_future(setup.channel.connect())
        await flush()
        await assert_refused("connecting")

        sockets[-1].open()
        await connecting
        sockets[-1].disconnect()
        await timers.advance(0)
        sockets[-1].fail()
        await flush()
        await assert_refused("failed")

        await setup.channel.close()
        await assert_refused("closed")

        for socket in sockets:
            socket.send.assert_not_called()

    # end method test_rejects_a_publish_with_not_connected_outside_recovery

    async def test_rejects_invalid_options_before_writing(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        segment = setup.channel.default_segment()

        error = await failure_of(publish(segment, b"x", message_id=""))
        assert isinstance(error, ConfigurationError)
        assert str(error) == "Invalid command. message_id: Must not be empty."

        untyped: Any = bytearray(b"x")
        error = await failure_of(publish(segment, untyped))
        assert isinstance(error, ConfigurationError)
        assert str(error) == "Invalid command. payload: Input should be a valid bytes."

        error = await failure_of(publish(segment, bytes(2 * 1024 * 1024)))
        assert isinstance(error, ConfigurationError)
        assert str(error).startswith("Encoded command exceeds 2 MiB.")
        sockets[-1].send.assert_not_called()

        await segment.publish(b"")
        assert sockets[-1].sent_frames() == [
            "@PUB\n$7\ndefault\n$11\ngenerated-3\n$0\n\n"
        ]

    # end method test_rejects_invalid_options_before_writing

    async def test_queues_publishes_behind_a_full_writer_and_sends_them_on_drain(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        bump_buffer(socket)
        lobby = setup.channel.default_segment()

        for _ in range(64):
            await lobby.publish(b"x")

        queued = [publish(lobby, b"x") for _ in range(64)]
        await flush()

        error = await failure_of(publish(lobby, b"x"))
        assert isinstance(error, CelerisConnectionError)
        assert (error.code, str(error)) == (
            "Backpressure",
            "The publish queue is full (size 64). Retry once some publishes have "
            "gone out.",
        )
        assert socket.send.call_count == 64

        socket.buffered_amount = 0
        await timers.advance(50)
        await asyncio.gather(*queued)

        assert socket.send.call_count == 128
        assert setup.channel.state == "connected"

    # end method test_queues_publishes_behind_a_full_writer_and_sends_them_on_drain

    async def test_maps_a_socket_send_failure_to_delivery_unknown(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        errors: list[ChannelError] = []
        setup.channel.events().on_error(errors.append)
        sockets[-1].send.side_effect = RuntimeError("synthetic-secret")

        error = await failure_of(publish(setup.channel.default_segment(), b"x"))

        assert isinstance(error, CelerisConnectionError)
        assert error.code == "DeliveryUnknown"
        assert setup.channel.state == "connected"
        assert errors == []

    # end method test_maps_a_socket_send_failure_to_delivery_unknown


# end class TestPublish


class TestSubscriptionsAndFlush:
    async def test_flushes_registered_interests_on_connect_in_order(
        self, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup = create_test_channel(timers)
        setup.channel.segment("beta").subscribe()
        setup.channel.segment("alpha").subscribe()
        setup.channel.default_segment().subscribe()

        await establish(setup, sockets)

        assert sockets[-1].sent_frames() == ["@SUB\n$4\nbeta\n", "@SUB\n$5\nalpha\n"]

    # end method test_flushes_registered_interests_on_connect_in_order

    async def test_reflushes_interests_after_reconnect_and_skips_cancelled_ones(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup.channel.segment("chat").subscribe()
        dropped = setup.channel.segment("gone").subscribe()

        sockets[-1].disconnect()
        # Disconnected: no UNSUB, and absent from the next flush.
        dropped.cancel()
        await timers.advance(0)
        await flush()
        sockets[-1].open()
        await flush()

        assert setup.channel.state == "connected"
        assert sockets[-1].sent_frames() == ["@SUB\n$4\nchat\n"]

    # end method test_reflushes_interests_after_reconnect_and_skips_cancelled_ones

    async def test_sends_a_subscription_queued_behind_a_full_writer_first(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        errors: list[ChannelError] = []
        setup.channel.events().on_error(errors.append)
        socket = sockets[-1]
        bump_buffer(socket)

        for _ in range(64):
            await setup.channel.default_segment().publish(b"x")

        queued = publish(setup.channel.segment("lobby"), b"y")
        await flush()
        setup.channel.segment("chat").subscribe()
        assert socket.send.call_count == 64

        socket.buffered_amount = 0
        await timers.advance(50)
        await queued

        assert socket.sent_frames()[64:] == [
            "@SUB\n$4\nchat\n",
            "@PUB\n$5\nlobby\n$12\ngenerated-65\n$1\ny\n",
        ]
        assert setup.channel.state == "connected"
        assert errors == []

    # end method test_sends_a_subscription_queued_behind_a_full_writer_first

    async def test_restores_more_than_64_subscriptions_on_connect_as_it_drains(
        self, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup = create_test_channel(timers)

        for index in range(65):
            setup.channel.segment(f"segment-{index}").subscribe()

        pending = asyncio.ensure_future(setup.channel.connect())
        await flush()
        socket = sockets[-1]
        bump_buffer(socket)
        socket.open()
        await pending

        assert setup.channel.state == "connected"
        assert socket.send.call_count == 64

        socket.buffered_amount = 0
        await timers.advance(50)

        assert socket.send.call_count == 65
        assert socket.sent_frames()[-1] == "@SUB\n$10\nsegment-64\n"

    # end method test_restores_more_than_64_subscriptions_on_connect_as_it_drains

    async def test_carries_nothing_from_a_failed_connect_into_the_next_one(
        self, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup = create_test_channel(timers)
        subscription = setup.channel.segment("chat").subscribe()

        failed = asyncio.ensure_future(setup.channel.connect())
        await flush()
        sockets[-1].send.side_effect = RuntimeError("synthetic-secret")
        sockets[-1].open()

        error = await failure_of(failed)
        assert isinstance(error, CelerisConnectionError)
        assert error.code == "Transport"
        assert setup.channel.state == "failed"

        subscription.cancel()
        await establish(setup, sockets)
        setup.channel.segment("lobby").subscribe()

        assert sockets[-1].sent_frames() == ["@SUB\n$5\nlobby\n"]

    # end method test_carries_nothing_from_a_failed_connect_into_the_next_one

    async def test_rejects_subscribe_on_a_closed_channel(
        self, timers: FakeTimers
    ) -> None:
        channel = create_test_channel(timers).channel
        await channel.close()

        with pytest.raises(
            CelerisConnectionError,
            match=r"^Channel is closed; create a new one with client\.channel\(\)\.$",
        ):
            channel.segment("chat").subscribe()

    # end method test_rejects_subscribe_on_a_closed_channel


# end class TestSubscriptionsAndFlush


class TestDeliveryAndDedup:
    async def test_routes_messages_to_their_segments_listeners_only(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        chat = record_ids(setup.channel.segment("chat"))
        lobby = record_ids(setup.channel.default_segment())

        sockets[-1].receive(message_frame("chat", "id-1", "hi"))
        sockets[-1].receive(message_frame("default", "id-2", "yo"))
        sockets[-1].receive(message_frame("other", "id-3", "no"))

        assert chat == ["id-1"]
        assert lobby == ["id-2"]

    # end method test_routes_messages_to_their_segments_listeners_only

    async def test_delivers_to_every_listener_and_preserves_fields(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        seen: list[tuple[bytes, MessageMetadata]] = []

        def record(payload: bytes, metadata: MessageMetadata) -> None:
            seen.append((payload, metadata))

        # end function record

        setup.channel.segment("chat").on_message(record)
        setup.channel.segment("chat").on_message(record)

        sockets[-1].receive(message_frame("chat", "id-1", "hi"))

        assert len(seen) == 2
        assert seen[0] == (
            b"hi",
            MessageMetadata(
                token_reference="user",
                segment_id="chat",
                message_id="id-1",
                timestamp=1,
            ),
        )

    # end method test_delivers_to_every_listener_and_preserves_fields

    async def test_deduplicates_by_id_recording_ids_even_without_listeners(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        sockets[-1].receive(message_frame("chat", "id-1", "hi"))
        delivered = record_ids(setup.channel.segment("chat"))
        sockets[-1].receive(message_frame("chat", "id-1", "hi"))
        sockets[-1].receive(message_frame("chat", "id-2", "hi"))
        sockets[-1].receive(message_frame("chat", "id-2", "hi"))

        assert delivered == ["id-2"]

    # end method test_deduplicates_by_id_recording_ids_even_without_listeners

    async def test_evicts_the_oldest_id_beyond_the_window_size(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        delivered = record_ids(setup.channel.segment("chat"))

        sockets[-1].receive(message_frame("chat", "id-0", "x"))

        for index in range(1, 1025):
            sockets[-1].receive(message_frame("chat", f"id-{index}", "x"))

        sockets[-1].receive(message_frame("chat", "id-0", "x"))

        assert len(delivered) == 1026
        assert delivered[-1] == "id-0"

    # end method test_evicts_the_oldest_id_beyond_the_window_size

    async def test_keeps_the_window_across_reconnect_and_clears_it_on_connect(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        delivered = record_ids(setup.channel.segment("chat"))
        setup.channel.segment("chat").subscribe()

        sockets[-1].receive(message_frame("chat", "id-1", "x"))
        sockets[-1].disconnect()
        await timers.advance(0)
        await flush()
        sockets[-1].open()
        await flush()
        sockets[-1].receive(message_frame("chat", "id-1", "x"))
        assert delivered == ["id-1"]

        await setup.channel.close()
        fresh = await establish(create_test_channel(timers), sockets)
        redelivered = record_ids(fresh.channel.segment("chat"))
        sockets[-1].receive(message_frame("chat", "id-1", "x"))
        assert redelivered == ["id-1"]

    # end method test_keeps_the_window_across_reconnect_and_clears_it_on_connect

    async def test_drops_a_null_id_message_without_dropping_the_connection(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        errors: list[ChannelError] = []
        setup.channel.events().on_error(errors.append)
        delivered = record_ids(setup.channel.segment("chat"))

        sockets[-1].receive(message_frame("chat", None, "x"))

        assert delivered == []
        assert len(errors) == 1
        assert isinstance(errors[0], ProtocolError)
        assert str(errors[0]) == (
            "Server message is missing its identifier. Field: message_id, byte "
            "offset 0."
        )
        # The message is undeliverable because it cannot be deduplicated, but
        # that is one frame's problem, not the connection's (DECODE-01).
        assert setup.channel.state == "connected"
        sockets[-1].close.assert_not_called()
        assert timers.count == 0

    # end method test_drops_a_null_id_message_without_dropping_the_connection

    async def test_skips_an_internal_node_command_without_reporting_it(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        errors: list[ChannelError] = []
        states: list[str] = []
        setup.channel.events().on_error(errors.append)
        setup.channel.events().on_state_change(states.append)
        delivered = record_ids(setup.channel.segment("chat"))

        sockets[-1].receive(b"@NODE_PUB\n+node-1\n$4\nbody\n")
        sockets[-1].receive(message_frame("chat", "id-1", "x"))

        assert errors == []
        assert states == []
        assert delivered == ["id-1"]

    # end method test_skips_an_internal_node_command_without_reporting_it

    async def test_reports_error_frames_while_staying_connected(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        errors: list[ChannelError] = []
        setup.channel.events().on_error(errors.append)
        delivered = record_ids(setup.channel.segment("chat"))

        # The server greets every connect with untagged prose notices.
        sockets[-1].receive(b'@SERVER_MSG\n:1\n$29\nSuccessfully connected to "x"\n')
        sockets[-1].receive(error_frame("RateLimitError", "slow down"))
        sockets[-1].receive(message_frame("chat", "id-1", "x"))

        # The server's own fields reach the consumer (ERR-01).
        assert len(errors) == 1
        error = errors[0]
        assert isinstance(error, ServerError)
        assert (error.type, error.sub_type, str(error), error.resource) == (
            "RateLimitError",
            None,
            "slow down",
            None,
        )
        assert setup.channel.state == "connected"
        assert delivered == ["id-1"]

    # end method test_reports_error_frames_while_staying_connected

    async def test_reports_a_permission_denial_with_its_command_and_segment(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        errors: list[ChannelError] = []
        setup.channel.events().on_error(errors.append)
        delivered = record_ids(setup.channel.segment("chat"))

        # A denied publish returns locally; the error arrives afterwards.
        await setup.channel.segment("chat").publish(b"x")
        sockets[-1].receive(
            error_frame("PermissionDeniedError", "denied", "PUB", "$4\nchat\n")
        )
        sockets[-1].receive(message_frame("chat", "id-1", "x"))

        assert len(errors) == 1
        error = errors[0]
        assert isinstance(error, ServerError)
        assert (error.type, error.sub_type, str(error), error.resource) == (
            "PermissionDeniedError",
            "PUB",
            "denied",
            "chat",
        )
        assert setup.channel.state == "connected"
        assert delivered == ["id-1"]

    # end method test_reports_a_permission_denial_with_its_command_and_segment

    @pytest.mark.parametrize(
        "type",
        [
            "ParserError",
            "SendError",
            "PermissionDeniedError",
            "RateLimitError",
            "MessageSizeLimitError",
            "InternalError",
            # A type a newer server adds still reaches the consumer.
            "SomeFutureError",
        ],
    )
    async def test_surfaces_an_error_frame_with_every_field(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], type: str
    ) -> None:
        errors: list[ChannelError] = []
        setup.channel.events().on_error(errors.append)

        sockets[-1].receive(error_frame(type, "what happened", "SUB", "*2\n+a\n:7\n"))

        assert len(errors) == 1
        error = errors[0]
        assert isinstance(error, ServerError)
        assert (error.type, error.sub_type, str(error), error.resource) == (
            type,
            "SUB",
            "what happened",
            ("a", 7),
        )
        assert setup.channel.state == "connected"

    # end method test_surfaces_an_error_frame_with_every_field

    async def test_surfaces_the_servers_message_size_rejection_exactly(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        errors: list[ChannelError] = []
        message = (
            "Message size limit exceeded; payload size = 65537 bytes; size limit = "
            "64 KB"
        )
        setup.channel.events().on_error(errors.append)

        sockets[-1].receive(error_frame("MessageSizeLimitError", message))

        assert isinstance(errors[0], ServerError)
        assert (errors[0].type, str(errors[0])) == ("MessageSizeLimitError", message)

    # end method test_surfaces_the_servers_message_size_rejection_exactly

    async def test_never_fails_while_delivering_a_malformed_error_message(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        errors: list[ChannelError] = []
        setup.channel.events().on_error(errors.append)

        sockets[-1].receive(b"-Err\n+SendError\n$-1\n$6\nbad \xff\xfe\n$-1\n")

        assert len(errors) == 1
        assert isinstance(errors[0], ServerError)
        assert errors[0].type == "SendError"
        assert str(errors[0]).startswith("bad ")
        assert setup.channel.state == "connected"

    # end method test_never_fails_while_delivering_a_malformed_error_message

    async def test_strips_a_leading_byte_order_mark_from_an_error_message(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        errors: list[ChannelError] = []
        setup.channel.events().on_error(errors.append)

        sockets[-1].receive(b"-Err\n+SendError\n$-1\n$6\n\xef\xbb\xbfbad\n$-1\n")

        assert str(errors[0]) == "bad"

    # end method test_strips_a_leading_byte_order_mark_from_an_error_message

    async def test_contains_throwing_listeners_and_honors_mid_dispatch_disposal(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        errors: list[ChannelError] = []
        order: list[str] = []
        setup.channel.events().on_error(errors.append)
        dispose_second: Callable[[], None] = lambda: None  # noqa: E731

        def first(payload: bytes, metadata: MessageMetadata) -> None:
            order.append("first")
            dispose_second()
            raise RuntimeError("listener-secret")

        # end function first

        setup.channel.segment("chat").on_message(first)
        dispose_second = setup.channel.segment("chat").on_message(
            lambda payload, metadata: order.append("second")
        )
        setup.channel.segment("chat").on_message(
            lambda payload, metadata: order.append("third")
        )

        sockets[-1].receive(message_frame("chat", "id-1", "x"))

        assert order == ["first", "third"]
        assert len(errors) == 1
        assert str(errors[0]) == LISTENER_FAILURE
        assert setup.channel.state == "connected"

    # end method test_contains_throwing_listeners_and_honors_mid_dispatch_disposal

    async def test_fans_out_nested_arrays_preserving_arrival_order(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        delivered = record_ids(setup.channel.segment("chat"))
        first = "@MSG\n$4\nuser\n$4\nchat\n$4\nal-1\n:1\n$1\na\n"
        second = "@MSG\n$4\nuser\n$4\nchat\n$4\nal-2\n:2\n$1\nb\n"

        sockets[-1].receive(f"*2\n{first}{second}".encode())

        assert delivered == ["al-1", "al-2"]

    # end method test_fans_out_nested_arrays_preserving_arrival_order


# end class TestDeliveryAndDedup


class TestChannelWideDelivery:
    async def test_receives_every_segments_deliveries_with_or_without_listeners(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        seen: list[str] = []

        def record(payload: bytes, metadata: MessageMetadata) -> None:
            seen.append(f"{metadata.segment_id}:{payload.decode()}")

        # end function record

        setup.channel.events().on_message(record)
        setup.channel.segment("chat").on_message(lambda payload, metadata: None)

        sockets[-1].receive(message_frame("chat", "id-1", "hi"))
        sockets[-1].receive(message_frame("default", "id-2", "yo"))
        sockets[-1].receive(message_frame("joined-by-publish", "id-3", "ok"))

        assert seen == ["chat:hi", "default:yo", "joined-by-publish:ok"]

    # end method test_receives_every_segments_deliveries_with_or_without_listeners

    async def test_runs_after_the_segments_listeners_in_the_same_dispatch(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        order: list[str] = []
        setup.channel.events().on_message(
            lambda payload, metadata: order.append("channel")
        )
        setup.channel.segment("chat").on_message(
            lambda payload, metadata: order.append("segment")
        )

        sockets[-1].receive(message_frame("chat", "id-1", "x"))

        assert order == ["segment", "channel"]

    # end method test_runs_after_the_segments_listeners_in_the_same_dispatch

    async def test_sees_a_duplicate_once_and_never_a_delivery_without_an_id(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        delivered: list[str] = []
        setup.channel.events().on_message(
            lambda payload, metadata: delivered.append(metadata.message_id)
        )

        sockets[-1].receive(message_frame("chat", "id-1", "x"))
        sockets[-1].receive(message_frame("other", "id-1", "x"))
        sockets[-1].receive(message_frame("chat", None, "x"))

        assert delivered == ["id-1"]

    # end method test_sees_a_duplicate_once_and_never_a_delivery_without_an_id

    async def test_contains_a_raising_listener_and_stops_after_disposal(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        errors: list[ChannelError] = []
        delivered: list[str] = []
        setup.channel.events().on_error(errors.append)

        def raise_failure(payload: bytes, metadata: MessageMetadata) -> None:
            raise RuntimeError("listener-secret")

        # end function raise_failure

        stop_raising = setup.channel.events().on_message(raise_failure)
        stop_recording = setup.channel.events().on_message(
            lambda payload, metadata: delivered.append(metadata.message_id)
        )

        sockets[-1].receive(message_frame("chat", "id-1", "x"))
        stop_raising()
        stop_recording()
        sockets[-1].receive(message_frame("chat", "id-2", "x"))

        assert delivered == ["id-1"]
        assert len(errors) == 1
        assert str(errors[0]) == LISTENER_FAILURE
        assert setup.channel.state == "connected"

    # end method test_contains_a_raising_listener_and_stops_after_disposal

    async def test_puts_nothing_on_the_wire_for_a_listener_alone(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        setup.channel.segment("chat").on_message(lambda payload, metadata: None)
        setup.channel.events().on_message(lambda payload, metadata: None)

        sockets[-1].send.assert_not_called()

    # end method test_puts_nothing_on_the_wire_for_a_listener_alone

    async def test_removes_only_its_own_channel_listener(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        delivered: list[str] = []
        remove_channel_listener = setup.channel.events().on_message(
            lambda payload, metadata: delivered.append("removed")
        )
        setup.channel.events().on_message(
            lambda payload, metadata: delivered.append("channel")
        )
        setup.channel.segment("chat").on_message(
            lambda payload, metadata: delivered.append("segment")
        )
        setup.channel.segment("chat").subscribe()

        remove_channel_listener()
        remove_channel_listener()
        sockets[-1].receive(message_frame("chat", "id-1", "x"))

        assert delivered == ["segment", "channel"]
        assert sockets[-1].sent_frames() == ["@SUB\n$4\nchat\n"]

    # end method test_removes_only_its_own_channel_listener

    # The server decides what arrives; the SDK never gates on subscriptions.
    async def test_delivers_whatever_the_subscription_state(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        delivered = record_ids(setup.channel.segment("chat"))
        setup.channel.segment("chat").subscribe().cancel()

        sockets[-1].receive(message_frame("chat", "id-1", "x"))

        assert delivered == ["id-1"]

    # end method test_delivers_whatever_the_subscription_state


# end class TestChannelWideDelivery


def rate_limit_frame() -> bytes:
    return error_frame("RateLimitError", "Rate limit exceeded")


# end function rate_limit_frame


# Eight limits in a row, each followed by its resend round: from here on the
# limit is treated as a used-up quota.
async def exhaust_rate_limit(socket: FakeWebSocket, timers: FakeTimers) -> None:
    for _ in range(8):
        socket.receive(rate_limit_frame())
        await timers.advance(31_000)


# end function exhaust_rate_limit


class TestRateLimitRecovery:
    # R1, R3
    async def test_pauses_then_resends_recent_subscriptions_before_recent_publishes(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        errors: list[ChannelError] = []
        setup.channel.events().on_error(errors.append)
        chat = setup.channel.segment("chat")
        lobby = setup.channel.segment("lobby")
        chat.subscribe()
        chat.subscribe_presence()
        await lobby.publish(b"a")
        socket = sockets[-1]
        socket.send.reset_mock()

        socket.receive(rate_limit_frame())
        queued = publish(lobby, b"b")
        await timers.advance(999)

        assert len(errors) == 1
        assert isinstance(errors[0], ServerError)
        assert errors[0].type == "RateLimitError"
        socket.send.assert_not_called()

        await timers.advance(1)
        await queued

        assert socket.sent_frames() == [
            "@SUB\n$4\nchat\n",
            "@PRES_SUB\n$4\nchat\n",
            "@PUB\n$5\nlobby\n$11\ngenerated-1\n$1\na\n",
            "@PUB\n$5\nlobby\n$11\ngenerated-2\n$1\nb\n",
        ]

    # end method test_pauses_then_resends_recent_subscriptions_before_recent_publishes

    # R2
    async def test_resends_a_command_sent_exactly_2000_ms_before_the_limit_not_2001(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        setup.channel.segment("early").subscribe()
        await setup.channel.segment("chat").publish(b"old")
        setup.clocks.monotonic += 1
        setup.channel.segment("edge").subscribe()
        await setup.channel.segment("chat").publish(b"edge")
        setup.clocks.monotonic += 2_000
        socket.send.reset_mock()

        socket.receive(rate_limit_frame())
        await timers.advance(1_000)

        assert socket.sent_frames() == [
            "@SUB\n$4\nedge\n",
            "@PUB\n$4\nchat\n$11\ngenerated-2\n$4\nedge\n",
        ]

    # end method test_resends_a_command_sent_exactly_2000_ms_before_the_limit_not_2001

    # R4
    async def test_resends_a_publish_byte_for_byte_with_its_original_id_and_only_once(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        await setup.channel.segment("chat").publish(b"x", message_id="original-id")
        original = socket.send.call_args_list[0].args[0]
        socket.send.reset_mock()

        socket.receive(rate_limit_frame())
        await timers.advance(1_000)

        assert [call.args[0] for call in socket.send.call_args_list] == [original]

        socket.send.reset_mock()
        socket.receive(rate_limit_frame())
        await timers.advance(31_000)

        socket.send.assert_not_called()

    # end method test_resends_a_publish_byte_for_byte_with_its_original_id_and_only_once

    # R6
    async def test_sends_one_command_with_the_final_state_for_a_toggle_while_paused(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        sockets[-1].receive(rate_limit_frame())

        setup.channel.segment("chat").subscribe().cancel()
        setup.channel.segment("chat").subscribe()
        setup.channel.segment("lobby").subscribe().cancel()
        await timers.advance(1_000)

        # The lobby nets out to never subscribed: UNSUB is its final state.
        assert sockets[-1].sent_frames() == [
            "@SUB\n$4\nchat\n",
            "@UNSUB\n$5\nlobby\n",
        ]

    # end method test_sends_one_command_with_the_final_state_for_a_toggle_while_paused

    # R7
    async def test_backs_off_on_consecutive_limits_and_starts_over_after_quiet(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup.clocks.random_value = 1
        socket = sockets[-1]
        setup.channel.segment("chat").subscribe()

        # 1 000 ms plus the reconnect delay for the streak index, which is
        # capped at 30 000 ms. The seventh limit is the first to reach the cap,
        # and the eighth stays there.
        pauses_ms = [1_500, 2_000, 3_000, 5_000, 9_000, 17_000, 31_000, 31_000]

        for pause_ms in pauses_ms:
            socket.send.reset_mock()
            socket.receive(rate_limit_frame())
            await timers.advance(pause_ms - 1)
            socket.send.assert_not_called()

            await timers.advance(1)
            assert socket.sent_frames() == ["@SUB\n$4\nchat\n"]

        # Past the last pause and its suspect window, the streak starts over.
        setup.clocks.monotonic += 40_000
        setup.channel.segment("lobby").subscribe()
        socket.send.reset_mock()
        socket.receive(rate_limit_frame())
        await timers.advance(1_499)
        socket.send.assert_not_called()

        await timers.advance(1)
        assert socket.sent_frames() == ["@SUB\n$5\nlobby\n"]

    # end method test_backs_off_on_consecutive_limits_and_starts_over_after_quiet

    async def test_withdraws_a_queued_publish_when_its_caller_is_cancelled(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        socket.receive(rate_limit_frame())

        pending = publish(setup.channel.segment("chat"), b"x")
        await flush()
        pending.cancel()

        assert isinstance(await failure_of(pending), asyncio.CancelledError)
        await timers.advance(1_000)
        socket.send.assert_not_called()

    # end method test_withdraws_a_queued_publish_when_its_caller_is_cancelled

    async def test_rejects_queued_publishes_when_the_channel_closes(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        sockets[-1].receive(rate_limit_frame())
        pending = publish(setup.channel.segment("chat"), b"x")
        await flush()

        await setup.channel.close()

        error = await failure_of(pending)
        assert isinstance(error, CelerisConnectionError)
        assert (error.code, str(error)) == (
            "Cancelled",
            "Channel closed before the publish was sent.",
        )

    # end method test_rejects_queued_publishes_when_the_channel_closes

    async def test_never_sends_a_change_ahead_of_an_earlier_publish_to_its_segment(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        sockets[-1].receive(rate_limit_frame())
        chat = setup.channel.segment("chat")

        subscription = chat.subscribe()
        published = publish(chat, b"x")
        await flush()
        subscription.cancel()
        await timers.advance(1_000)
        await published

        # Publishing joins the segment, so the UNSUB has to follow it.
        assert sockets[-1].sent_frames() == [
            "@PUB\n$4\nchat\n$11\ngenerated-1\n$1\nx\n",
            "@UNSUB\n$4\nchat\n",
        ]

    # end method test_never_sends_a_change_ahead_of_an_earlier_publish_to_its_segment

    # R9
    async def test_resends_at_the_eighth_limit_and_drops_publishes_from_the_ninth(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        setup.channel.segment("chat").subscribe()

        for _ in range(7):
            socket.receive(rate_limit_frame())
            await timers.advance(31_000)

        await setup.channel.segment("lobby").publish(b"x")

        # The eighth limit in a row still resends both kinds.
        socket.send.reset_mock()
        socket.receive(rate_limit_frame())
        await timers.advance(1_000)
        assert socket.sent_frames() == [
            "@SUB\n$4\nchat\n",
            "@PUB\n$5\nlobby\n$11\ngenerated-1\n$1\nx\n",
        ]

        await setup.channel.segment("lobby").publish(b"y")

        # The ninth is a quota: nothing is resent, the publishes are dropped,
        # and the subscription waits for the probe.
        socket.send.reset_mock()
        socket.receive(rate_limit_frame())
        await timers.advance(59_999)
        socket.send.assert_not_called()

        await timers.advance(1)
        assert socket.sent_frames() == ["@SUB\n$4\nchat\n"]

        await timers.advance(31_000)
        assert socket.sent_frames() == ["@SUB\n$4\nchat\n"]

    # end method test_resends_at_the_eighth_limit_and_drops_publishes_from_the_ninth

    # R10
    async def test_resends_dropped_subscriptions_on_a_doubling_probe_up_to_one_hour(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        setup.channel.segment("chat").subscribe()
        await exhaust_rate_limit(socket, timers)

        probe_delays_ms = [
            60_000,
            120_000,
            240_000,
            480_000,
            960_000,
            1_920_000,
            3_600_000,
            3_600_000,
        ]

        for delay_ms in probe_delays_ms:
            socket.send.reset_mock()
            socket.receive(rate_limit_frame())
            await timers.advance(delay_ms - 1)
            socket.send.assert_not_called()

            await timers.advance(1)
            assert socket.sent_frames() == ["@SUB\n$4\nchat\n"]

    # end method test_resends_dropped_subscriptions_on_a_doubling_probe_up_to_one_hour

    # R11
    async def test_resends_normally_again_once_commands_go_a_window_without_a_limit(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        setup.channel.segment("chat").subscribe()
        await exhaust_rate_limit(socket, timers)
        socket.receive(rate_limit_frame())
        await timers.advance(60_000)

        setup.clocks.monotonic += 3_000
        setup.channel.segment("lobby").subscribe()
        socket.send.reset_mock()
        socket.receive(rate_limit_frame())
        await timers.advance(1_000)

        assert socket.sent_frames() == ["@SUB\n$5\nlobby\n"]

    # end method test_resends_normally_again_once_commands_go_a_window_without_a_limit

    # R12
    async def test_keeps_probing_across_a_reconnect_instead_of_starting_over(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup.channel.segment("chat").subscribe()
        await exhaust_rate_limit(sockets[-1], timers)
        sockets[-1].receive(rate_limit_frame())

        sockets[-1].disconnect()
        await timers.advance(0)
        await flush()
        restored = sockets[-1]
        restored.open()
        await flush()
        assert restored.sent_frames() == ["@SUB\n$4\nchat\n"]

        restored.send.reset_mock()
        restored.receive(rate_limit_frame())
        await timers.advance(119_999)
        restored.send.assert_not_called()

        await timers.advance(1)
        assert restored.sent_frames() == ["@SUB\n$4\nchat\n"]

    # end method test_keeps_probing_across_a_reconnect_instead_of_starting_over

    async def test_holds_a_change_only_behind_publishes_queued_before_it(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        sockets[-1].receive(rate_limit_frame())
        chat = setup.channel.segment("chat")

        first = publish(chat, b"1")
        await flush()
        chat.subscribe()
        second = publish(chat, b"2")
        await flush()
        await timers.advance(1_000)
        await asyncio.gather(first, second)

        assert sockets[-1].sent_frames() == [
            "@PUB\n$4\nchat\n$11\ngenerated-1\n$1\n1\n",
            "@SUB\n$4\nchat\n",
            "@PUB\n$4\nchat\n$11\ngenerated-2\n$1\n2\n",
        ]

    # end method test_holds_a_change_only_behind_publishes_queued_before_it

    # R8
    async def test_counts_one_episode_when_several_frames_report_the_same_burst(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        setup.channel.segment("chat").subscribe()

        # Eight episodes, each reported through two limit frames: still eight
        # resend rounds, not a give-up at four.
        for _ in range(8):
            socket.send.reset_mock()
            socket.receive(rate_limit_frame())
            socket.receive(rate_limit_frame())
            await timers.advance(1_000)
            assert socket.sent_frames() == ["@SUB\n$4\nchat\n"]

        socket.send.reset_mock()
        socket.receive(rate_limit_frame())
        await timers.advance(59_999)
        socket.send.assert_not_called()

        await timers.advance(1)
        assert socket.sent_frames() == ["@SUB\n$4\nchat\n"]

    # end method test_counts_one_episode_when_several_frames_report_the_same_burst

    # R15
    async def test_treats_a_late_report_while_probing_as_the_quota_still_exhausted(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        setup.channel.segment("chat").subscribe()
        await exhaust_rate_limit(socket, timers)
        socket.receive(rate_limit_frame())
        await timers.advance(60_000)
        assert socket.sent_frames()[-1] == "@SUB\n$4\nchat\n"

        # The dropped probe's report lands past the suspect window but far
        # inside the confirmation span: probing continues, doubled.
        setup.clocks.monotonic += 2_500
        socket.send.reset_mock()
        socket.receive(rate_limit_frame())
        await timers.advance(119_999)
        socket.send.assert_not_called()

        await timers.advance(1)
        assert socket.sent_frames() == ["@SUB\n$4\nchat\n"]

    # end method test_treats_a_late_report_while_probing_as_the_quota_still_exhausted

    async def test_ends_probing_when_a_limit_arrives_long_after_accepted_traffic(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        setup.channel.segment("chat").subscribe()
        await exhaust_rate_limit(socket, timers)
        socket.receive(rate_limit_frame())
        await timers.advance(60_000)

        # The probe's frames were accepted; a limit far later is a new burst,
        # handled with normal resend rounds again.
        setup.clocks.monotonic += 40_000
        socket.receive(rate_limit_frame())
        setup.channel.segment("lobby").subscribe()
        socket.send.reset_mock()
        await timers.advance(1_000)
        assert socket.sent_frames() == ["@SUB\n$5\nlobby\n"]

        socket.receive(rate_limit_frame())
        socket.send.reset_mock()
        await timers.advance(1_000)
        assert socket.sent_frames() == ["@SUB\n$5\nlobby\n"]

    # end method test_ends_probing_when_a_limit_arrives_long_after_accepted_traffic

    async def test_counts_a_sent_presence_query_as_proof_the_quota_returned(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        setup.channel.segment("chat").subscribe()
        await exhaust_rate_limit(socket, timers)
        socket.receive(rate_limit_frame())
        await timers.advance(1_000)

        query = asyncio.ensure_future(
            setup.channel.segment("chat").presence_list(page=1, per_page=25)
        )
        await flush()
        setup.clocks.monotonic += 2_500
        socket.send.reset_mock()
        setup.channel.segment("lobby").subscribe()

        assert socket.sent_frames() == ["@SUB\n$5\nlobby\n", "@SUB\n$4\nchat\n"]
        query.cancel()
        await failure_of(query)

    # end method test_counts_a_sent_presence_query_as_proof_the_quota_returned

    # R5
    async def test_resends_at_most_the_last_64_publishes(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        lobby = setup.channel.segment("lobby")

        for index in range(1, 66):
            await lobby.publish(str(index).encode())

        socket.send.reset_mock()
        socket.receive(rate_limit_frame())
        await timers.advance(1_000)

        assert socket.sent_frames() == [
            publish_frame("lobby", f"generated-{index}", str(index))
            for index in range(2, 66)
        ]

    # end method test_resends_at_most_the_last_64_publishes

    # R13
    async def test_rejects_a_presence_query_while_sending_is_paused(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        sockets[-1].receive(rate_limit_frame())

        error = await failure_of(
            asyncio.ensure_future(
                setup.channel.segment("chat").presence_list(page=1, per_page=25)
            )
        )

        assert isinstance(error, CelerisConnectionError)
        assert (error.code, str(error)) == (
            "Backpressure",
            "Sending is paused after a rate limit; try again in a moment.",
        )

    # end method test_rejects_a_presence_query_while_sending_is_paused

    # R14
    async def test_resends_subscriptions_restored_on_reconnect_when_a_limit_follows(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup.channel.segment("chat").subscribe()
        sockets[-1].disconnect()
        await timers.advance(0)
        await flush()
        restored = sockets[-1]
        restored.open()
        await flush()
        restored.send.reset_mock()

        restored.receive(rate_limit_frame())
        await timers.advance(1_000)

        assert restored.sent_frames() == ["@SUB\n$4\nchat\n"]
        assert setup.channel.state == "connected"

    # end method test_resends_subscriptions_restored_on_reconnect_when_a_limit_follows

    async def test_replaces_the_socket_when_a_subscription_write_fails(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        errors: list[ChannelError] = []
        setup.channel.events().on_error(errors.append)
        failing = sockets[-1]
        failing.send.side_effect = RuntimeError("synthetic-secret")

        setup.channel.segment("chat").subscribe()

        assert setup.channel.state == "reconnecting"
        failing.close.assert_called()

        await timers.advance(0)
        await flush()
        sockets[-1].open()
        await flush()

        assert setup.channel.state == "connected"
        assert sockets[-1].sent_frames() == ["@SUB\n$4\nchat\n"]
        assert errors == []

    # end method test_replaces_the_socket_when_a_subscription_write_fails


# end class TestRateLimitRecovery


# Holds the fake writer full: any send is refused as backpressure.
FULL_BUFFER = 2 * 1024 * 1024


def publish_frame(segment_id: str, message_id: str, body: str) -> str:
    return (
        f"@PUB\n${len(segment_id)}\n{segment_id}\n${len(message_id)}\n{message_id}"
        f"\n${len(body)}\n{body}\n"
    )


# end function publish_frame


async def start_reconnect_attempt(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> FakeWebSocket:
    """Runs the scheduled attempt: randomness is 0, so its delay is zero."""
    socket_count = len(sockets)
    await timers.advance(0)
    assert len(sockets) == socket_count + 1
    return sockets[-1]


# end function start_reconnect_attempt


async def reconnect(sockets: list[FakeWebSocket], timers: FakeTimers) -> None:
    (await start_reconnect_attempt(sockets, timers)).open()
    await flush()


# end function reconnect


class TestPublishesAcrossAReconnect:
    """QUEUE-01: a publish not yet given to a socket waits for the next one."""

    async def test_queues_a_publish_while_reconnecting_and_sends_it_after_reconnect(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        sockets[-1].disconnect()
        assert setup.channel.state == "reconnecting"

        published = publish(setup.channel.segment("chat"), b"x")
        await flush()
        assert not published.done()

        await reconnect(sockets, timers)
        await published
        assert setup.channel.state == "connected"
        assert sockets[-1].sent_frames() == [publish_frame("chat", "generated-1", "x")]

    # end method test_queues_a_publish_while_reconnecting_and_sends_it_after_reconnect

    async def test_sends_publishes_waiting_behind_a_full_writer_in_order_on_reconnect(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        sockets[-1].buffered_amount = FULL_BUFFER
        chat = setup.channel.segment("chat")
        published = [publish(chat, body) for body in (b"a", b"b", b"c")]
        await flush()

        sockets[-1].disconnect()
        await reconnect(sockets, timers)
        await asyncio.gather(*published)

        assert sockets[-1].sent_frames() == [
            publish_frame("chat", "generated-1", "a"),
            publish_frame("chat", "generated-2", "b"),
            publish_frame("chat", "generated-3", "c"),
        ]

    # end method test_sends_publishes_waiting_behind_a_full_writer_in_order_on_reconnect

    async def test_restores_every_subscription_before_any_queued_publish(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup.channel.segment("chat").subscribe()
        setup.channel.segment("news").subscribe()
        setup.channel.segment("lobby").subscribe_presence()
        await flush()

        # Queued before the drop, to a segment the restoration subscribes.
        sockets[-1].buffered_amount = FULL_BUFFER
        before_drop = publish(setup.channel.segment("chat"), b"a")
        await flush()
        sockets[-1].disconnect()
        during_outage = publish(setup.channel.segment("news"), b"b")

        await reconnect(sockets, timers)
        await asyncio.gather(before_drop, during_outage)

        assert sockets[-1].sent_frames() == [
            "@SUB\n$4\nchat\n",
            "@SUB\n$4\nnews\n",
            "@PRES_SUB\n$5\nlobby\n",
            publish_frame("chat", "generated-1", "a"),
            publish_frame("news", "generated-2", "b"),
        ]

    # end method test_restores_every_subscription_before_any_queued_publish

    async def test_refuses_a_publish_while_reconnecting_with_backpressure_when_full(
        self, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup = await establish(
            create_test_channel(timers, publish_queue_size=1), sockets
        )
        sockets[-1].disconnect()
        chat = setup.channel.segment("chat")
        queued = publish(chat, b"a")
        await flush()

        error = await failure_of(publish(chat, b"b"))
        assert isinstance(error, CelerisConnectionError)
        assert (error.code, str(error)) == (
            "Backpressure",
            "The publish queue is full (size 1). Retry once some publishes have "
            "gone out.",
        )

        await reconnect(sockets, timers)
        await queued
        assert sockets[-1].sent_frames() == [publish_frame("chat", "generated-1", "a")]

    # end method test_refuses_a_publish_while_reconnecting_with_backpressure_when_full

    async def test_keeps_the_queue_through_a_failed_attempt_and_sends_it_on_the_next(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        sockets[-1].disconnect()
        published = publish(setup.channel.segment("chat"), b"x")

        (await start_reconnect_attempt(sockets, timers)).fail()
        await flush()
        assert setup.channel.state == "reconnecting"
        assert not published.done()

        await reconnect(sockets, timers)
        await published
        assert sockets[-1].sent_frames() == [publish_frame("chat", "generated-1", "x")]

    # end method test_keeps_the_queue_through_a_failed_attempt_and_sends_it_on_the_next

    async def test_rejects_each_queued_publish_with_the_terminal_error(
        self, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup = await establish(
            create_test_channel(timers, maximum_reconnect_attempts=1), sockets
        )
        errors: list[ChannelError] = []
        setup.channel.events().on_error(errors.append)
        sockets[-1].disconnect()
        chat = setup.channel.segment("chat")
        published = [publish(chat, body) for body in (b"a", b"b")]

        (await start_reconnect_attempt(sockets, timers)).fail()
        await flush()

        assert setup.channel.state == "failed"
        (error,) = errors
        assert isinstance(error, CelerisConnectionError)
        assert error.code == "Transport"

        for pending in published:
            assert await failure_of(pending) is error

    # end method test_rejects_each_queued_publish_with_the_terminal_error

    async def test_rejects_queued_publishes_with_cancelled_on_close_while_reconnecting(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        sockets[-1].disconnect()
        published = publish(setup.channel.segment("chat"), b"x")
        await flush()

        await setup.channel.close()

        error = await failure_of(published)
        assert isinstance(error, CelerisConnectionError)
        assert (error.code, str(error)) == (
            "Cancelled",
            "Channel closed before the publish was sent.",
        )

    # end method test_rejects_queued_publishes_with_cancelled_on_close_while_reconnecting

    async def test_starts_an_explicit_connect_after_failed_with_an_empty_queue(
        self, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup = await establish(
            create_test_channel(timers, maximum_reconnect_attempts=1), sockets
        )
        sockets[-1].disconnect()
        published = publish(setup.channel.segment("chat"), b"x")

        (await start_reconnect_attempt(sockets, timers)).fail()
        await flush()
        assert isinstance(await failure_of(published), CelerisConnectionError)
        assert setup.channel.state == "failed"

        await establish(setup, sockets)
        await timers.advance(1_000)

        assert sockets[-1].sent_frames() == []

    # end method test_starts_an_explicit_connect_after_failed_with_an_empty_queue

    async def test_never_resends_a_publish_the_previous_socket_was_given(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        await setup.channel.segment("chat").publish(b"x")
        assert sockets[-1].sent_frames() == [publish_frame("chat", "generated-1", "x")]

        sockets[-1].disconnect()
        await reconnect(sockets, timers)
        await timers.advance(5_000)

        assert sockets[-1].sent_frames() == []

    # end method test_never_resends_a_publish_the_previous_socket_was_given

    async def test_withdraws_a_publish_queued_while_reconnecting_when_cancelled(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        sockets[-1].disconnect()
        published = publish(setup.channel.segment("chat"), b"x")
        await flush()

        published.cancel()

        assert isinstance(await failure_of(published), asyncio.CancelledError)
        await reconnect(sockets, timers)
        assert sockets[-1].sent_frames() == []

    # end method test_withdraws_a_publish_queued_while_reconnecting_when_cancelled


# end class TestPublishesAcrossAReconnect


async def test_contains_a_listener_that_raises_cancelled_error(
    setup: ChannelSetup, sockets: list[FakeWebSocket]
) -> None:
    # Reading the result of a cancelled future raises CancelledError, which is
    # not an Exception; it is still the listener's own failure.
    errors: list[ChannelError] = []
    delivered: list[str] = []
    setup.channel.events().on_error(errors.append)

    def read_cancelled(payload: bytes, metadata: MessageMetadata) -> None:
        cancelled = asyncio.get_running_loop().create_future()
        cancelled.cancel()
        cancelled.result()

    # end function read_cancelled

    setup.channel.segment("chat").on_message(read_cancelled)
    setup.channel.segment("chat").on_message(
        lambda payload, metadata: delivered.append(metadata.message_id)
    )

    sockets[-1].receive(message_frame("chat", "id-1", "x"))
    sockets[-1].receive(message_frame("chat", "id-2", "x"))

    assert delivered == ["id-1", "id-2"]
    assert [str(error) for error in errors] == [LISTENER_FAILURE] * 2
    assert setup.channel.state == "connected"


# end function test_contains_a_listener_that_raises_cancelled_error


async def test_clears_the_dedup_window_on_an_explicit_connect(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    setup = await establish(create_test_channel(timers), sockets)
    delivered = record_ids(setup.channel.segment("chat"))
    sockets[-1].receive(message_frame("chat", "id-1", "x"))

    # Ten failed reconnects leave the channel failed; connecting again is a
    # fresh start.
    sockets[-1].disconnect()

    for _ in range(10):
        await timers.advance(30_000)
        sockets[-1].fail()

    await flush()
    assert setup.channel.state == "failed"

    await establish(setup, sockets)
    sockets[-1].receive(message_frame("chat", "id-1", "x"))

    assert delivered == ["id-1", "id-1"]


# end function test_clears_the_dedup_window_on_an_explicit_connect


async def test_holds_exactly_1024_ids_in_the_dedup_window(
    setup: ChannelSetup, sockets: list[FakeWebSocket]
) -> None:
    delivered = record_ids(setup.channel.segment("chat"))

    for index in range(1024):
        sockets[-1].receive(message_frame("chat", f"id-{index}", "x"))

    sockets[-1].receive(message_frame("chat", "id-0", "x"))

    assert len(delivered) == 1024


# end function test_holds_exactly_1024_ids_in_the_dedup_window


async def test_reports_a_listener_that_returns_a_coroutine(
    setup: ChannelSetup, sockets: list[FakeWebSocket]
) -> None:
    # Not an async def, so registration accepts it; the coroutine it returns
    # would never run.
    errors: list[ChannelError] = []
    handled: list[bytes] = []
    setup.channel.events().on_error(errors.append)

    async def handle(payload: bytes) -> None:
        handled.append(payload)

    # end function handle

    # Typing refuses it; an untyped caller can still pass it.
    untyped: Any = lambda payload, metadata: handle(payload)  # noqa: E731
    setup.channel.segment("chat").on_message(untyped)

    sockets[-1].receive(message_frame("chat", "id-1", "x"))

    assert handled == []
    assert [str(error) for error in errors] == [LISTENER_FAILURE]


# end function test_reports_a_listener_that_returns_a_coroutine
