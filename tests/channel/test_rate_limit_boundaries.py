import asyncio

import pytest

from tests.helpers.channel import (
    ChannelSetup,
    create_test_channel,
    error_frame,
    establish,
)
from tests.helpers.tasks import failure_of, flush
from tests.helpers.timers import FakeTimers
from tests.helpers.websocket import FakeWebSocket
from useceleris_client import CelerisConnectionError

# The edges of rate-limit recovery (RESEND-01): the suspect window, the streak,
# the quota probe and the quiet span that ends it. Each test moves the
# monotonic clock to an exact boundary.


@pytest.fixture
async def setup(sockets: list[FakeWebSocket], timers: FakeTimers) -> ChannelSetup:
    return await establish(create_test_channel(timers), sockets)


def rate_limit_frame() -> bytes:
    return error_frame("RateLimitError", "Rate limit exceeded")


async def exhaust_rate_limit(socket: FakeWebSocket, timers: FakeTimers) -> None:
    for _ in range(8):
        socket.receive(rate_limit_frame())
        await timers.advance(31_000)


def bump_buffer(socket: FakeWebSocket) -> None:
    def bump(data: bytes) -> None:
        socket.buffered_amount += 1

    socket.send.side_effect = bump


class TestSuspectWindow:
    async def test_resends_a_subscription_sent_exactly_at_its_edge(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup.channel.segment("chat").subscribe()
        setup.clocks.monotonic += 2_000
        setup.channel.segment("lobby").subscribe()
        sockets[-1].send.reset_mock()

        sockets[-1].receive(rate_limit_frame())
        await timers.advance(1_000)

        assert sockets[-1].sent_frames() == ["@SUB\n$4\nchat\n", "@SUB\n$5\nlobby\n"]

    async def test_resends_a_publish_sent_exactly_at_its_edge(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        chat = setup.channel.segment("chat")
        await chat.publish(b"old")
        setup.clocks.monotonic += 2_000
        await chat.publish(b"new")
        sockets[-1].send.reset_mock()

        sockets[-1].receive(rate_limit_frame())
        await timers.advance(1_000)

        assert len(sockets[-1].sent_frames()) == 2

    async def test_puts_resent_publishes_ahead_of_ones_already_waiting(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        lobby = setup.channel.segment("lobby")
        await lobby.publish(b"a")
        # A buffer that never drains fills the 64-command writer.
        socket.buffered_amount = 1

        for _ in range(63):
            await lobby.publish(b"f")

        waiting = asyncio.ensure_future(lobby.publish(b"b"))
        await flush()
        assert not waiting.done()

        socket.receive(rate_limit_frame())
        socket.send.reset_mock()
        socket.buffered_amount = 0
        await timers.advance(1_000)
        await waiting

        assert socket.sent_frames()[-1].endswith("$1\nb\n")


class TestStreak:
    async def streak_case(
        self, setup: ChannelSetup, socket: FakeWebSocket, timers: FakeTimers, at: float
    ) -> None:
        # The first pause is 1.5 s, so the streak runs to 3.5 s: a limit by
        # then waits 2 s, the second step of the backoff.
        setup.clocks.random_value = 1
        socket.receive(rate_limit_frame())
        await timers.advance(1_500)
        setup.clocks.monotonic = at
        setup.channel.segment("lobby").subscribe()
        socket.send.reset_mock()

        socket.receive(rate_limit_frame())
        await timers.advance(1_999)
        socket.send.assert_not_called()

        await timers.advance(1)
        assert socket.sent_frames() == ["@SUB\n$5\nlobby\n"]

    @pytest.mark.parametrize("at", [2_500, 3_500])
    async def test_continues_until_the_streak_ends(
        self,
        setup: ChannelSetup,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
        at: float,
    ) -> None:
        await self.streak_case(setup, sockets[-1], timers, at)

    async def test_survives_a_reconnect(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup.clocks.random_value = 1
        setup.channel.segment("chat").subscribe()

        for _ in range(3):
            sockets[-1].receive(rate_limit_frame())
            await timers.advance(31_000)

        sockets[-1].disconnect()
        await timers.advance(1_000)
        restored = sockets[-1]
        restored.open()
        await flush()
        setup.clocks.monotonic = 2_001
        setup.channel.segment("lobby").subscribe()
        restored.send.reset_mock()

        # The fourth limit in a row: 1 s plus a 4 s backoff step.
        restored.receive(rate_limit_frame())
        await timers.advance(4_999)
        restored.send.assert_not_called()


class TestQuotaProbe:
    async def test_runs_one_probe_at_a_time(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup.channel.segment("chat").subscribe()
        await exhaust_rate_limit(sockets[-1], timers)
        sockets[-1].receive(rate_limit_frame())
        await timers.advance(1_000)
        setup.channel.segment("lobby").subscribe()

        sockets[-1].receive(rate_limit_frame())
        await timers.advance(1_000)

        assert timers.count == 1

    async def test_schedules_no_probe_when_nothing_was_dropped(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        await exhaust_rate_limit(sockets[-1], timers)

        sockets[-1].receive(rate_limit_frame())
        await timers.advance(31_000)

        assert timers.count == 0

    async def test_drops_only_what_the_last_limit_could_concern(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup.channel.segment("chat").subscribe()
        setup.clocks.monotonic += 3_000

        # The subscription went out before any of these limits' windows.
        for _ in range(9):
            sockets[-1].receive(rate_limit_frame())
            await timers.advance(31_000)

        assert timers.count == 0

    async def test_keeps_probing_when_the_probe_itself_is_refused(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        setup.channel.segment("chat").subscribe()
        await exhaust_rate_limit(socket, timers)
        socket.receive(rate_limit_frame())
        setup.clocks.monotonic = 60_000
        await timers.advance(60_000)
        socket.send.reset_mock()

        socket.receive(rate_limit_frame())
        await timers.advance(119_999)

        socket.send.assert_not_called()

    async def test_forgets_dropped_subscriptions_on_a_reconnect(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        setup.channel.segment("chat").subscribe()
        gone = setup.channel.segment("gone").subscribe()
        await exhaust_rate_limit(sockets[-1], timers)
        sockets[-1].receive(rate_limit_frame())
        sockets[-1].disconnect()
        gone.cancel()
        await timers.advance(0)
        restored = sockets[-1]
        restored.open()
        await flush()
        restored.send.reset_mock()

        restored.receive(rate_limit_frame())
        await timers.advance(120_000)

        assert "@UNSUB\n$4\ngone\n" not in restored.sent_frames()


class TestQuietSpan:
    async def test_ends_probing_only_after_more_than_the_span(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        setup.channel.segment("chat").subscribe()
        await exhaust_rate_limit(socket, timers)
        socket.receive(rate_limit_frame())
        await timers.advance(60_000)
        setup.clocks.monotonic = 32_000
        socket.send.reset_mock()

        # Exactly 32 s after the probe's send: still a late report, so the
        # doubled probe re-sends the subscription.
        socket.receive(rate_limit_frame())
        await timers.advance(120_000)

        assert socket.sent_frames() == ["@SUB\n$4\nchat\n"]

    async def test_measures_from_the_first_send_after_a_limit(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        setup.channel.segment("chat").subscribe()
        await exhaust_rate_limit(socket, timers)
        socket.receive(rate_limit_frame())
        await timers.advance(60_000)
        setup.clocks.monotonic = 1_500
        setup.channel.segment("lobby").subscribe()
        setup.clocks.monotonic = 2_500
        setup.channel.segment("other").subscribe()
        socket.send.reset_mock()

        socket.receive(rate_limit_frame())
        await timers.advance(1_000)

        assert socket.sent_frames() == ["@SUB\n$5\nlobby\n", "@SUB\n$5\nother\n"]

    async def test_counts_a_presence_query_as_the_first_send(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        socket = sockets[-1]
        setup.channel.segment("chat").subscribe()
        await exhaust_rate_limit(socket, timers)
        socket.receive(rate_limit_frame())
        await timers.advance(60_000)
        setup.clocks.monotonic = 1_500
        query = asyncio.ensure_future(
            setup.channel.segment("chat").presence_list(page=1, per_page=25)
        )
        await flush()
        setup.clocks.monotonic = 2_500
        setup.channel.segment("other").subscribe()
        socket.send.reset_mock()

        socket.receive(rate_limit_frame())
        await timers.advance(1_000)

        assert socket.sent_frames() == ["@SUB\n$5\nother\n"]
        query.cancel()
        await failure_of(query)


class TestWriter:
    async def test_keeps_one_drain_timer_for_a_full_writer(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        bump_buffer(sockets[-1])
        lobby = setup.channel.default_segment()

        for _ in range(64):
            await lobby.publish(b"x")

        queued = [asyncio.ensure_future(lobby.publish(b"x")) for _ in range(3)]
        await flush()

        assert timers.count == 1
        sockets[-1].buffered_amount = 0
        await timers.advance(50)
        await asyncio.gather(*queued)

    async def test_waits_for_a_buffer_too_full_for_the_next_publish(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        sockets[-1].buffered_amount = 2 * 1024 * 1024 - 1
        pending = asyncio.ensure_future(setup.channel.segment("chat").publish(b"xx"))
        await flush()
        assert not pending.done()

        sockets[-1].buffered_amount = 0
        await timers.advance(50)
        await pending

    async def test_sends_the_next_publish_after_one_fails(
        self, setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
    ) -> None:
        sockets[-1].receive(rate_limit_frame())
        chat = setup.channel.segment("chat")
        first = asyncio.ensure_future(chat.publish(b"1"))
        second = asyncio.ensure_future(chat.publish(b"2"))
        await flush()
        sockets[-1].send.side_effect = [RuntimeError("synthetic"), None]

        await timers.advance(1_000)

        error = await failure_of(first)
        assert isinstance(error, CelerisConnectionError)
        assert error.code == "DeliveryUnknown"
        await second
