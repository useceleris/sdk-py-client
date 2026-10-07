import asyncio
from typing import Any

import pytest

from tests.helpers.channel import (
    ChannelSetup,
    create_test_channel,
    establish,
    presence_response_frame,
)
from tests.helpers.tasks import failure_of, flush
from tests.helpers.timers import FakeTimers
from tests.helpers.websocket import FakeWebSocket
from useceleris_client import (
    CelerisConnectionError,
    ChannelError,
    ConfigurationError,
    PresenceConnection,
    PresenceEvent,
    PresencePage,
    ServerError,
    ServerNotice,
)

LISTENER_FAILURE = (
    "A listener callback raised an error; the channel caught it and kept running."
)


@pytest.fixture
async def setup(sockets: list[FakeWebSocket], timers: FakeTimers) -> ChannelSetup:
    return await establish(create_test_channel(timers), sockets)


# An error answering the presence query with this request id.
def presence_error_frame(type: str, request_id: str) -> bytes:
    return (
        f"-Err\n+{type}\n+PRES_LIST\n$6\nfailed\n${len(request_id)}\n{request_id}\n"
    ).encode()


def presence_notify_frame(
    segment_id: str,
    token_reference: str,
    connection_id: str,
    joined: bool,
    timestamp: int = 123,
) -> bytes:
    return (
        f"@PRES_NOTIFY\n+{segment_id}\n+{token_reference}\n+{connection_id}\n"
        f";{1 if joined else 0}\n:{timestamp}\n"
    ).encode()


def query(
    setup: ChannelSetup, segment_id: str = "chat", page: int = 1, per_page: int = 25
) -> "asyncio.Task[PresencePage]":
    return asyncio.ensure_future(
        setup.channel.segment(segment_id).presence_list(page=page, per_page=per_page)
    )


class TestPresenceInterests:
    async def test_shares_one_ref_count_across_instances_with_golden_bytes(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        first = setup.channel.segment("chat").subscribe_presence()
        second = setup.channel.segment("chat").subscribe_presence()
        assert sockets[-1].sent_frames() == ["@PRES_SUB\n$4\nchat\n"]

        first.cancel()
        first.cancel()
        assert sockets[-1].sent_frames() == ["@PRES_SUB\n$4\nchat\n"]

        second.cancel()
        assert sockets[-1].sent_frames() == [
            "@PRES_SUB\n$4\nchat\n",
            "@PRES_UNSUB\n$4\nchat\n",
        ]

    async def test_sends_presence_commands_for_the_default_segment_too(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        setup.channel.default_segment().subscribe_presence().cancel()

        assert sockets[-1].sent_frames() == [
            "@PRES_SUB\n$7\ndefault\n",
            "@PRES_UNSUB\n$7\ndefault\n",
        ]

    async def test_sends_unsub_on_message_cancel_while_presence_is_held(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        messages = setup.channel.segment("chat").subscribe()
        presence = setup.channel.segment("chat").subscribe_presence()

        messages.cancel()
        assert sockets[-1].sent_frames() == [
            "@SUB\n$4\nchat\n",
            "@PRES_SUB\n$4\nchat\n",
            "@UNSUB\n$4\nchat\n",
        ]

        presence.cancel()
        assert sockets[-1].sent_frames() == [
            "@SUB\n$4\nchat\n",
            "@PRES_SUB\n$4\nchat\n",
            "@UNSUB\n$4\nchat\n",
            "@PRES_UNSUB\n$4\nchat\n",
        ]

    async def test_flushes_messages_first_then_presence_in_registration_order(
        self, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup = create_test_channel(timers)
        setup.channel.default_segment().subscribe_presence()
        setup.channel.segment("beta").subscribe()
        setup.channel.segment("alpha").subscribe_presence()
        setup.channel.segment("gone").subscribe_presence().cancel()

        await establish(setup, sockets)

        assert sockets[-1].sent_frames() == [
            "@SUB\n$4\nbeta\n",
            "@PRES_SUB\n$7\ndefault\n",
            "@PRES_SUB\n$5\nalpha\n",
        ]

    async def test_rejects_presence_interest_on_a_closed_channel(
        self, timers: FakeTimers
    ) -> None:
        channel = create_test_channel(timers).channel
        await channel.close()

        with pytest.raises(CelerisConnectionError) as caught:
            channel.segment("chat").subscribe_presence()

        assert str(caught.value) == (
            "Channel is closed; create a new one with client.channel()."
        )

    async def test_sends_a_presence_subscription_queued_behind_a_full_writer(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        errors: list[ChannelError] = []
        setup.channel.events().on_error(errors.append)
        socket = sockets[-1]

        def bump(data: bytes) -> None:
            socket.buffered_amount += 1

        socket.send.side_effect = bump

        for _ in range(64):
            await setup.channel.default_segment().publish(b"x")

        setup.channel.segment("chat").subscribe_presence()
        assert socket.send.call_count == 64

        socket.buffered_amount = 0
        await timers.advance(50)

        assert socket.sent_frames()[-1] == "@PRES_SUB\n$4\nchat\n"
        assert setup.channel.state == "connected"
        assert errors == []


class TestPresenceQueries:
    async def test_resolves_a_matching_response_with_its_raw_metadata(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        pending = query(setup)
        await flush()
        assert sockets[-1].sent_frames() == ["@PRES_LIST\n$4\nchat\n;1\n;25\n$1\n1\n"]

        sockets[-1].receive(
            presence_response_frame(
                connections=[
                    ("user", "connection-1", 123),
                    ("user", "connection-2", 456),
                ],
                total=2,
                to=2,
            )
        )

        assert await pending == PresencePage(
            segment_id="chat",
            total=2,
            per_page=25,
            current_page=1,
            from_=1,
            to=2,
            connections=(
                PresenceConnection("user", "connection-1", 123),
                PresenceConnection("user", "connection-2", 456),
            ),
        )
        assert timers.count == 0

    async def test_preserves_past_last_page_metadata(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        pending = query(setup, page=2)
        await flush()

        sockets[-1].receive(
            presence_response_frame(current_page=2, from_=26, to=1, connections=[])
        )

        page = await pending
        assert (page.from_, page.to, page.connections) == (26, 1, ())

    async def test_rejects_overlap_while_the_first_resolves(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        first = query(setup)
        await flush()

        error = await failure_of(query(setup, "other"))
        assert isinstance(error, CelerisConnectionError)
        assert error.code == "OperationInProgress"

        sockets[-1].receive(presence_response_frame())
        assert (await first).segment_id == "chat"

    @pytest.mark.parametrize(
        ("page", "per_page"),
        [(0, 25), (2_147_483_648, 25), (1.5, 25), (True, 25), (1, 0), (1, 101)],
    )
    async def test_rejects_out_of_range_bounds(
        self,
        setup: ChannelSetup,
        sockets: list[FakeWebSocket],
        page: Any,
        per_page: Any,
    ) -> None:
        error = await failure_of(query(setup, page=page, per_page=per_page))

        assert isinstance(error, ConfigurationError)
        sockets[-1].send.assert_not_called()

        recovered = query(setup)
        await flush()
        sockets[-1].receive(presence_response_frame(request_id="2"))
        assert (await recovered).segment_id == "chat"

    async def test_rejects_queries_while_not_connected(
        self, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        idle = create_test_channel(timers)
        error = await failure_of(query(idle))
        assert isinstance(error, CelerisConnectionError)
        assert error.code == "NotConnected"

        setup = await establish(create_test_channel(timers), sockets)
        sockets[-1].disconnect()

        error = await failure_of(query(setup))
        assert isinstance(error, CelerisConnectionError)
        assert error.code == "NotConnected"

    async def test_times_out_without_disturbing_the_connection_and_drops_the_late_reply(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        errors: list[ChannelError] = []
        states: list[str] = []
        setup.channel.events().on_error(errors.append)
        setup.channel.events().on_state_change(states.append)

        pending = query(setup)
        await timers.advance(9_999)
        assert not pending.done()
        await timers.advance(1)

        error = await failure_of(pending)
        assert isinstance(error, CelerisConnectionError)
        assert (error.code, str(error)) == (
            "Timeout",
            "Presence query timed out after 10000 ms.",
        )

        # A late reply carries its own query's request id, so it cannot be
        # mistaken for the next query's (QUERY-01): the connection stays up.
        assert states == []
        sockets[-1].close.assert_not_called()
        assert timers.count == 0

        following = query(setup)
        await flush()
        sockets[-1].receive(presence_response_frame(request_id="1"))
        sockets[-1].receive(presence_error_frame("InternalError", "1"))
        sockets[-1].receive(presence_response_frame(request_id="2", total=7))

        assert (await following).total == 7
        assert errors == []
        assert timers.count == 0

    async def test_cancelling_frees_the_slot_and_stays_connected(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        pending = query(setup)
        await flush()

        pending.cancel()

        assert isinstance(await failure_of(pending), asyncio.CancelledError)
        assert setup.channel.state == "connected"
        assert timers.count == 0

        following = query(setup)
        await flush()
        sockets[-1].receive(presence_response_frame(request_id="1"))
        sockets[-1].receive(presence_response_frame(request_id="2", total=3))
        assert (await following).total == 3

    async def test_matches_a_response_by_request_id_alone(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        sockets[-1].receive(presence_response_frame(request_id="9"))
        assert setup.channel.state == "connected"

        pending = query(setup, page=2, per_page=50)
        await flush()
        sockets[-1].receive(presence_response_frame(request_id="0"))
        sockets[-1].receive(presence_response_frame(request_id="10"))

        # The id alone decides; the other fields are the server's to report.
        sockets[-1].receive(presence_response_frame(request_id="1", current_page=7))
        assert (await pending).current_page == 7
        assert timers.count == 0

    @pytest.mark.parametrize("type", ["InternalError", "PermissionDeniedError"])
    async def test_rejects_at_once_on_an_error_naming_the_query(
        self,
        setup: ChannelSetup,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
        type: str,
    ) -> None:
        errors: list[ChannelError] = []
        setup.channel.events().on_error(errors.append)

        pending = query(setup)
        await flush()
        sockets[-1].receive(presence_error_frame(type, "1"))

        error = await failure_of(pending)
        assert isinstance(error, ServerError)
        assert (error.type, error.sub_type, str(error), error.resource) == (
            type,
            "PRES_LIST",
            "failed",
            "1",
        )
        assert error.code == "Server"

        # Reported once, to the caller; the connection is untouched.
        assert errors == []
        assert setup.channel.state == "connected"
        assert timers.count == 0

        following = query(setup)
        await flush()
        sockets[-1].receive(presence_response_frame(request_id="2"))
        assert (await following).segment_id == "chat"

    async def test_drops_a_presence_query_error_for_any_other_request_id(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        errors: list[ChannelError] = []
        setup.channel.events().on_error(errors.append)

        sockets[-1].receive(presence_error_frame("InternalError", "1"))
        pending = query(setup)
        await flush()
        sockets[-1].receive(presence_error_frame("InternalError", "0"))
        sockets[-1].receive(presence_response_frame(request_id="1"))

        assert (await pending).segment_id == "chat"
        assert errors == []

    async def test_rejects_the_pending_query_on_connection_loss_and_on_close(
        self, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        lost = await establish(create_test_channel(timers), sockets)
        lost_query = query(lost)
        await flush()
        sockets[-1].disconnect()

        error = await failure_of(lost_query)
        assert isinstance(error, CelerisConnectionError)
        assert (error.code, str(error)) == (
            "Transport",
            "Connection lost during the presence query; query again once the "
            "channel reconnects.",
        )
        await lost.channel.close()

        closed = await establish(create_test_channel(timers), sockets)
        closed_query = query(closed)
        await flush()
        await closed.channel.close()

        error = await failure_of(closed_query)
        assert isinstance(error, CelerisConnectionError)
        assert (error.code, str(error)) == (
            "Cancelled",
            "Channel closed while the presence query was pending.",
        )
        assert timers.count == 0

    async def test_rejects_a_query_whose_send_fails_and_stays_connected(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        socket.send.side_effect = [RuntimeError("synthetic-secret"), None]

        error = await failure_of(query(setup))
        assert isinstance(error, CelerisConnectionError)
        assert error.code == "DeliveryUnknown"
        assert setup.channel.state == "connected"

        # The failed send may still have reached the server, so its request id
        # is spent and the next query uses a new one.
        following = query(setup)
        await flush()
        socket.receive(presence_response_frame(request_id="2"))
        assert (await following).segment_id == "chat"
        assert timers.count == 0


class TestNotices:
    async def test_delivers_raw_server_notices_in_order_with_working_disposal(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        seen: list[tuple[str, ServerNotice]] = []
        stop_first = setup.channel.events().on_notice(
            lambda notice: seen.append(("first", notice))
        )
        setup.channel.events().on_notice(lambda notice: seen.append(("second", notice)))

        sockets[-1].receive(b"@SERVER_MSG\n:7\n$6\njoined\n")
        assert seen == [
            ("first", ServerNotice(7, b"joined")),
            ("second", ServerNotice(7, b"joined")),
        ]

        stop_first()
        seen.clear()
        sockets[-1].receive(b"@SERVER_MSG\n:8\n$4\nleft\n")
        assert seen == [("second", ServerNotice(8, b"left"))]

    async def test_contains_throwing_notice_listeners(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        errors: list[ChannelError] = []
        order: list[str] = []
        setup.channel.events().on_error(errors.append)

        def raise_secret(notice: ServerNotice) -> None:
            raise RuntimeError("notice-secret")

        setup.channel.events().on_notice(raise_secret)
        setup.channel.events().on_notice(lambda notice: order.append("after"))

        sockets[-1].receive(b"@SERVER_MSG\n:1\n$0\n\n")

        assert order == ["after"]
        assert len(errors) == 1
        assert str(errors[0]) == LISTENER_FAILURE
        assert setup.channel.state == "connected"

    async def test_delivers_presence_notifications_to_their_own_segment_only(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        chat: list[PresenceEvent] = []
        lobby: list[PresenceEvent] = []
        setup.channel.segment("chat").on_presence(chat.append)
        setup.channel.segment("lobby").on_presence(lobby.append)

        sockets[-1].receive(
            presence_notify_frame("chat", "user", "connection-1", True, 7)
        )
        sockets[-1].receive(
            presence_notify_frame("chat", "user", "connection-1", False, 9)
        )

        assert lobby == []
        assert chat == [
            PresenceEvent("chat", "user", "connection-1", True, 7),
            PresenceEvent("chat", "user", "connection-1", False, 9),
        ]

    async def test_shares_one_presence_listener_set_across_handler_instances(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        seen: list[str] = []
        stop_first = setup.channel.segment("chat").on_presence(
            lambda event: seen.append("first")
        )
        setup.channel.segment("chat").on_presence(lambda event: seen.append("second"))

        sockets[-1].receive(presence_notify_frame("chat", "user", "connection-1", True))
        stop_first()
        sockets[-1].receive(
            presence_notify_frame("chat", "user", "connection-1", False)
        )

        assert seen == ["first", "second", "second"]

    async def test_ignores_a_notification_for_a_segment_with_no_listener(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        errors: list[ChannelError] = []
        setup.channel.events().on_error(errors.append)

        sockets[-1].receive(
            presence_notify_frame("unwatched", "user", "connection-1", True)
        )

        assert errors == []
        assert setup.channel.state == "connected"

    async def test_contains_throwing_presence_listeners(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket]
    ) -> None:
        errors: list[ChannelError] = []
        order: list[str] = []
        setup.channel.events().on_error(errors.append)

        def raise_secret(event: PresenceEvent) -> None:
            raise RuntimeError("presence-secret")

        setup.channel.segment("chat").on_presence(raise_secret)
        setup.channel.segment("chat").on_presence(lambda event: order.append("after"))

        sockets[-1].receive(presence_notify_frame("chat", "user", "connection-1", True))

        assert order == ["after"]
        assert len(errors) == 1
        assert str(errors[0]) == LISTENER_FAILURE
        assert setup.channel.state == "connected"


async def test_never_reuses_a_request_id_across_a_reconnect(
    setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    first = query(setup)
    await flush()
    sockets[-1].receive(presence_response_frame(request_id="1"))
    await first

    sockets[-1].disconnect()
    await timers.advance(0)
    sockets[-1].open()
    await flush()
    assert setup.channel.state == "connected"

    second = query(setup)
    await flush()
    assert sockets[-1].sent_frames()[-1] == "@PRES_LIST\n$4\nchat\n;1\n;25\n$1\n2\n"

    # A reply to the earlier query, however late, answers nothing now.
    sockets[-1].receive(presence_response_frame(request_id="1", total=9))
    sockets[-1].receive(presence_response_frame(request_id="2", total=4))
    assert (await second).total == 4


async def test_still_reconnects_when_a_cancelled_query_meets_a_lost_connection(
    setup: ChannelSetup, sockets: list[FakeWebSocket]
) -> None:
    pending = query(setup)
    await flush()

    # Both land before the query's task resumes.
    pending.cancel()
    sockets[-1].disconnect()

    assert isinstance(await failure_of(pending), asyncio.CancelledError)
    assert setup.channel.state == "reconnecting"
    await setup.channel.close()


async def test_a_late_cancel_never_frees_the_next_querys_slot(
    setup: ChannelSetup, sockets: list[FakeWebSocket]
) -> None:
    first = query(setup)
    await flush()
    second = query(setup)

    # The reply answers the first query; the second takes the slot before the
    # first, cancelled meanwhile, resumes. That cancellation is too late to
    # free anything, least of all the second query's slot.
    sockets[-1].receive(presence_response_frame(request_id="1"))
    first.cancel()
    await flush()
    sockets[-1].receive(presence_response_frame(request_id="2", total=5))

    assert (await second).total == 5
    await failure_of(first)
