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


def bump_buffer(socket: FakeWebSocket) -> None:
    def bump(data: bytes) -> None:
        socket.buffered_amount += 1

    socket.send.side_effect = bump


def record_ids(segment: Segment) -> list[str]:
    delivered: list[str] = []

    def record(payload: bytes, metadata: MessageMetadata) -> None:
        delivered.append(metadata.message_id)

    segment.on_message(record)
    return delivered


def publish(segment: Segment, payload: bytes, **options: Any) -> "asyncio.Task[None]":
    return asyncio.ensure_future(segment.publish(payload, **options))


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

    @pytest.mark.parametrize("identifier", ["", "bad\nid", "bad\rid", "\ud800", None])
    async def test_rejects_invalid_segment_identifiers(
        self, timers: FakeTimers, identifier: Any
    ) -> None:
        with pytest.raises(ConfigurationError, match=r"^Invalid segment ID\. "):
            create_test_channel(timers).channel.segment(identifier)

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

    async def test_never_sends_sub_or_unsub_for_the_default_segment(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        setup.channel.default_segment().subscribe().cancel()

        sockets[-1].send.assert_not_called()


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

    async def test_rejects_offline_publishes_without_queueing(
        self, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        idle = create_test_channel(timers)
        error = await failure_of(publish(idle.channel.default_segment(), b"x"))
        assert isinstance(error, CelerisConnectionError)
        assert error.code == "NotConnected"

        setup = await establish(create_test_channel(timers), sockets)
        segment = setup.channel.segment("chat")
        sockets[-1].disconnect()
        assert setup.channel.state == "reconnecting"

        error = await failure_of(publish(segment, b"x"))
        assert isinstance(error, CelerisConnectionError)
        assert (error.code, str(error)) == (
            "NotConnected",
            "Channel is not connected; it is reconnecting.",
        )

        await setup.channel.close()
        error = await failure_of(publish(segment, b"x"))
        assert isinstance(error, CelerisConnectionError)
        assert error.code == "NotConnected"
        sockets[-1].send.assert_not_called()

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
            "64 publishes are already waiting to be sent. Retry once some have "
            "gone out.",
        )
        assert socket.send.call_count == 64

        socket.buffered_amount = 0
        await timers.advance(50)
        await asyncio.gather(*queued)

        assert socket.send.call_count == 128
        assert setup.channel.state == "connected"

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

    async def test_delivers_to_every_listener_and_preserves_fields(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        seen: list[tuple[bytes, MessageMetadata]] = []

        def record(payload: bytes, metadata: MessageMetadata) -> None:
            seen.append((payload, metadata))

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

    async def test_deduplicates_by_id_recording_ids_even_without_listeners(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        sockets[-1].receive(message_frame("chat", "id-1", "hi"))
        delivered = record_ids(setup.channel.segment("chat"))
        sockets[-1].receive(message_frame("chat", "id-1", "hi"))
        sockets[-1].receive(message_frame("chat", "id-2", "hi"))
        sockets[-1].receive(message_frame("chat", "id-2", "hi"))

        assert delivered == ["id-2"]

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

    async def test_strips_a_leading_byte_order_mark_from_an_error_message(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        errors: list[ChannelError] = []
        setup.channel.events().on_error(errors.append)

        sockets[-1].receive(b"-Err\n+SendError\n$-1\n$6\n\xef\xbb\xbfbad\n$-1\n")

        assert str(errors[0]) == "bad"

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

    async def test_fans_out_nested_arrays_preserving_arrival_order(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        delivered = record_ids(setup.channel.segment("chat"))
        first = "@MSG\n$4\nuser\n$4\nchat\n$4\nal-1\n:1\n$1\na\n"
        second = "@MSG\n$4\nuser\n$4\nchat\n$4\nal-2\n:2\n$1\nb\n"

        sockets[-1].receive(f"*2\n{first}{second}".encode())

        assert delivered == ["al-1", "al-2"]


def rate_limit_frame() -> bytes:
    return error_frame("RateLimitError", "Rate limit exceeded")


# Eight limits in a row, each followed by its resend round: from here on the
# limit is treated as a used-up quota.
async def exhaust_rate_limit(socket: FakeWebSocket, timers: FakeTimers) -> None:
    for _ in range(8):
        socket.receive(rate_limit_frame())
        await timers.advance(31_000)


class TestRateLimitRecovery:
    async def test_pauses_then_resends_recent_subscriptions_before_publishes(
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

    async def test_resends_a_publish_once_and_only_within_the_window(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        chat = setup.channel.segment("chat")
        await chat.publish(b"old")
        setup.clocks.monotonic += 2_001
        await chat.publish(b"new")
        socket.send.reset_mock()

        socket.receive(rate_limit_frame())
        await timers.advance(1_000)

        assert socket.sent_frames() == ["@PUB\n$4\nchat\n$11\ngenerated-2\n$3\nnew\n"]

        socket.send.reset_mock()
        socket.receive(rate_limit_frame())
        await timers.advance(30_000)

        socket.send.assert_not_called()

    async def test_sends_one_command_for_a_subscription_toggled_while_paused(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        sockets[-1].receive(rate_limit_frame())

        setup.channel.segment("chat").subscribe().cancel()
        setup.channel.segment("chat").subscribe()
        await timers.advance(1_000)

        assert sockets[-1].sent_frames() == ["@SUB\n$4\nchat\n"]

    async def test_backs_off_on_consecutive_limits_and_starts_over_after_quiet(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup.clocks.random_value = 1
        socket = sockets[-1]
        setup.channel.segment("chat").subscribe()

        for pause_ms in [1_500, 2_000]:
            socket.send.reset_mock()
            socket.receive(rate_limit_frame())
            await timers.advance(pause_ms - 1)
            socket.send.assert_not_called()

            await timers.advance(1)
            assert socket.sent_frames() == ["@SUB\n$4\nchat\n"]

        setup.clocks.monotonic += 10_000
        setup.channel.segment("lobby").subscribe()
        socket.send.reset_mock()
        socket.receive(rate_limit_frame())
        await timers.advance(1_500)

        assert socket.sent_frames() == ["@SUB\n$5\nlobby\n"]

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

    async def test_rejects_queued_publishes_when_the_connection_drops(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        socket = sockets[-1]
        socket.receive(rate_limit_frame())
        pending = publish(setup.channel.segment("chat"), b"x")
        await flush()

        socket.disconnect()

        error = await failure_of(pending)
        assert isinstance(error, CelerisConnectionError)
        assert (error.code, str(error)) == (
            "NotConnected",
            "Connection lost before the publish was sent; publish again once the "
            "channel reconnects.",
        )
        assert setup.channel.state == "reconnecting"

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

    async def test_resends_dropped_subscriptions_on_a_slow_doubling_probe(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        setup.channel.segment("chat").subscribe()
        await exhaust_rate_limit(socket, timers)

        socket.send.reset_mock()
        socket.receive(rate_limit_frame())
        await timers.advance(59_999)
        socket.send.assert_not_called()

        await timers.advance(1)
        assert socket.sent_frames() == ["@SUB\n$4\nchat\n"]

        socket.send.reset_mock()
        socket.receive(rate_limit_frame())
        await timers.advance(119_999)
        socket.send.assert_not_called()

        await timers.advance(1)
        assert socket.sent_frames() == ["@SUB\n$4\nchat\n"]

    async def test_resends_normally_once_commands_go_a_window_without_a_limit(
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

    async def test_keeps_probing_across_a_reconnect(
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

    async def test_resends_at_most_the_last_64_publishes(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        lobby = setup.channel.segment("lobby")

        for _ in range(70):
            await lobby.publish(b"x")

        socket.send.reset_mock()
        socket.receive(rate_limit_frame())
        await timers.advance(1_000)

        assert socket.send.call_count == 64
        assert socket.sent_frames()[0] == "@PUB\n$5\nlobby\n$11\ngenerated-7\n$1\nx\n"

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

    async def test_resends_subscriptions_restored_on_reconnect_after_a_limit(
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

    setup.channel.segment("chat").on_message(read_cancelled)
    setup.channel.segment("chat").on_message(
        lambda payload, metadata: delivered.append(metadata.message_id)
    )

    sockets[-1].receive(message_frame("chat", "id-1", "x"))
    sockets[-1].receive(message_frame("chat", "id-2", "x"))

    assert delivered == ["id-1", "id-2"]
    assert [str(error) for error in errors] == [LISTENER_FAILURE] * 2
    assert setup.channel.state == "connected"


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


async def test_holds_exactly_1024_ids_in_the_dedup_window(
    setup: ChannelSetup, sockets: list[FakeWebSocket]
) -> None:
    delivered = record_ids(setup.channel.segment("chat"))

    for index in range(1024):
        sockets[-1].receive(message_frame("chat", f"id-{index}", "x"))

    sockets[-1].receive(message_frame("chat", "id-0", "x"))

    assert len(delivered) == 1024


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

    # Typing refuses it; an untyped caller can still pass it.
    untyped: Any = lambda payload, metadata: handle(payload)  # noqa: E731
    setup.channel.segment("chat").on_message(untyped)

    sockets[-1].receive(message_frame("chat", "id-1", "x"))

    assert handled == []
    assert [str(error) for error in errors] == [LISTENER_FAILURE]
