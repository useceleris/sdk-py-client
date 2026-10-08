import asyncio

import pytest

from tests.helpers.channel import (
    ChannelSetup,
    create_test_channel,
    establish,
)
from tests.helpers.tasks import flush
from tests.helpers.timers import FakeTimers
from tests.helpers.websocket import FakeWebSocket
from useceleris_client import (
    CelerisConnectionError,
    ChannelError,
    ConfigurationError,
    CredentialRequest,
    Credentials,
    ProtocolError,
    RecoveryEvent,
)


class Attempts:
    def __init__(self, sockets: list[FakeWebSocket], timers: FakeTimers) -> None:
        self.sockets = sockets
        self.timers = timers

    # end method __init__

    async def expect_after(self, delay_ms: float) -> None:
        socket_count = len(self.sockets)

        if delay_ms > 0:
            await self.timers.advance(delay_ms - 1)
            assert len(self.sockets) == socket_count
            await self.timers.advance(1)
        else:
            await self.timers.advance(0)

        await flush()
        assert len(self.sockets) == socket_count + 1

    # end method expect_after

    async def fail_after(self, delay_ms: float) -> None:
        await self.expect_after(delay_ms)
        self.sockets[-1].fail()
        await flush()

    # end method fail_after

    async def succeed_after(self, delay_ms: float) -> None:
        await self.expect_after(delay_ms)
        self.sockets[-1].open()
        await flush()

    # end method succeed_after


# end class Attempts


@pytest.fixture
def attempts(sockets: list[FakeWebSocket], timers: FakeTimers) -> Attempts:
    return Attempts(sockets, timers)


# end function attempts


@pytest.fixture
async def setup(sockets: list[FakeWebSocket], timers: FakeTimers) -> ChannelSetup:
    return await establish(create_test_channel(timers), sockets)


# end function setup


def reconnect_requests(setup: ChannelSetup) -> list[CredentialRequest]:
    return [
        call.args[0]
        for call in setup.credential_provider.call_args_list
        if call.args[0].reason == "reconnect"
    ]


# end function reconnect_requests


async def test_bounds_jittered_delays_per_retry_and_fails_after_ten_retries(
    setup: ChannelSetup,
    attempts: Attempts,
    sockets: list[FakeWebSocket],
    timers: FakeTimers,
) -> None:
    setup.clocks.random_value = 0.5
    errors: list[ChannelError] = []
    states: list[str] = []
    setup.channel.events().on_error(errors.append)
    setup.channel.events().on_state_change(states.append)

    sockets[0].disconnect()
    assert setup.channel.state == "reconnecting"

    for delay in [250, 500, 1_000, 2_000, 4_000, 8_000, 15_000, 15_000, 15_000, 15_000]:
        await attempts.fail_after(delay)

    assert setup.channel.state == "failed"
    assert len(errors) == 1
    assert isinstance(errors[0], CelerisConnectionError)
    assert errors[0].code == "Transport"
    assert states == ["reconnecting", "failed"]
    assert len(reconnect_requests(setup)) == 10
    assert timers.count == 0


# end function test_bounds_jittered_delays_per_retry_and_fails_after_ten_retries


async def test_resets_the_retry_budget_only_after_sixty_seconds_connected(
    setup: ChannelSetup, attempts: Attempts, sockets: list[FakeWebSocket]
) -> None:
    setup.clocks.random_value = 0.5

    sockets[-1].disconnect()

    for delay in [250, 500, 1_000]:
        await attempts.fail_after(delay)

    await attempts.succeed_after(2_000)
    assert setup.channel.state == "connected"

    setup.clocks.monotonic += 1_000
    sockets[-1].disconnect()
    await attempts.succeed_after(2_000)
    assert setup.channel.state == "connected"

    setup.clocks.monotonic += 60_000
    sockets[-1].disconnect()
    await attempts.succeed_after(250)
    assert setup.channel.state == "connected"
    await setup.channel.close()


# end function test_resets_the_retry_budget_only_after_sixty_seconds_connected


async def test_emits_recovery_after_the_connected_state_with_the_attempt_index(
    setup: ChannelSetup, attempts: Attempts, sockets: list[FakeWebSocket]
) -> None:
    log: list[object] = []
    setup.channel.events().on_state_change(lambda state: log.append(f"state:{state}"))
    setup.channel.events().on_recovery(log.append)

    sockets[-1].disconnect()
    await attempts.fail_after(0)
    await attempts.fail_after(0)
    await attempts.succeed_after(0)

    assert log == [
        "state:reconnecting",
        "state:connected",
        RecoveryEvent(retry_index=2, possible_gaps=True, possible_duplicates=True),
    ]
    await setup.channel.close()


# end function test_emits_recovery_after_the_connected_state_with_the_attempt_index


async def test_requests_fresh_credentials_with_the_outage_and_a_capped_lookback(
    setup: ChannelSetup, attempts: Attempts, sockets: list[FakeWebSocket]
) -> None:
    setup.clocks.monotonic = 5_000
    setup.clocks.wall = 1_700_000_100_000

    sockets[-1].disconnect()
    await attempts.fail_after(0)

    setup.clocks.monotonic = 6_500
    # A wall-clock change must not affect the elapsed time.
    setup.clocks.wall = 999
    await attempts.fail_after(0)

    setup.clocks.monotonic = 5_000 + 5_000_000_000
    await attempts.fail_after(0)

    requests = reconnect_requests(setup)
    assert len(requests) == 3
    assert [request.disconnected_at for request in requests] == [1_700_000_100_000] * 3
    assert [request.replay_lookback_ms for request in requests] == [
        5_000,
        6_500,
        4_294_967_295,
    ]
    await setup.channel.close()


# end function test_requests_fresh_credentials_with_the_outage_and_a_capped_lookback


async def test_fails_immediately_on_deterministic_reconnect_errors(
    setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    errors: list[ChannelError] = []
    setup.channel.events().on_error(errors.append)
    setup.credential_provider.return_value = Credentials(payload="", signature="x")

    sockets[-1].disconnect()
    await timers.advance(0)
    await flush()

    assert setup.channel.state == "failed"
    assert len(errors) == 1
    assert isinstance(errors[0], ConfigurationError)
    assert timers.count == 0


# end function test_fails_immediately_on_deterministic_reconnect_errors


async def test_stays_connected_through_protocol_corruption(
    setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    errors: list[ChannelError] = []
    setup.channel.events().on_error(errors.append)

    sockets[-1].receive("text")

    # DECODE-01: the bad frame is dropped and reported; there is nothing to
    # retry because the connection was never lost.
    assert setup.channel.state == "connected"
    assert len(errors) == 1
    assert isinstance(errors[0], ProtocolError)
    sockets[-1].close.assert_not_called()
    assert timers.count == 0


# end function test_stays_connected_through_protocol_corruption


async def test_enters_reconnecting_once_for_a_transport_error_with_trailing_close(
    setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    states: list[str] = []
    setup.channel.events().on_state_change(states.append)

    sockets[-1].fail()

    assert states == ["reconnecting"]
    assert setup.channel.state == "reconnecting"
    assert timers.count == 1
    await setup.channel.close()
    assert timers.count == 0


# end function test_enters_reconnecting_once_for_a_transport_error_with_trailing_close


async def test_stops_reconnecting_when_closed_mid_attempt(
    setup: ChannelSetup,
    attempts: Attempts,
    sockets: list[FakeWebSocket],
    timers: FakeTimers,
) -> None:
    states: list[str] = []

    sockets[-1].disconnect()
    await attempts.expect_after(0)
    setup.channel.events().on_state_change(states.append)

    await setup.channel.close()
    assert states == ["closing", "closed"]

    sockets[-1].open()
    await flush()
    assert setup.channel.state == "closed"
    assert timers.count == 0


# end function test_stops_reconnecting_when_closed_mid_attempt


async def test_does_not_reconnect_when_closed_as_the_retry_timer_fires(
    setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    states: list[str] = []
    setup.channel.events().on_state_change(states.append)
    sockets[-1].disconnect()

    # The attempt starts a loop iteration after its timer fires; the close
    # lands in between.
    (retry,) = timers.pending
    retry.cancel()
    retry.callback()
    await setup.channel.close()
    await flush()

    assert states == ["reconnecting", "closing", "closed"]
    assert len(sockets) == 1
    assert len(reconnect_requests(setup)) == 0


# end function test_does_not_reconnect_when_closed_as_the_retry_timer_fires


async def test_restores_the_retry_budget_on_an_explicit_connect_from_failed(
    setup: ChannelSetup,
    attempts: Attempts,
    sockets: list[FakeWebSocket],
    timers: FakeTimers,
) -> None:
    setup.clocks.random_value = 0.5
    sockets[-1].disconnect()

    for delay in [250, 500, 1_000, 2_000, 4_000, 8_000, 15_000, 15_000, 15_000, 15_000]:
        await attempts.fail_after(delay)

    assert setup.channel.state == "failed"
    await establish(setup, sockets)

    # A fresh budget: the first retry waits the shortest delay again.
    sockets[-1].disconnect()
    await attempts.succeed_after(250)
    assert setup.channel.state == "connected"


# end function test_restores_the_retry_budget_on_an_explicit_connect_from_failed


async def test_retries_a_reconnect_attempt_that_times_out(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    setup = await establish(
        create_test_channel(timers, connect_timeout_ms=1_000), sockets
    )

    async def hang(request: CredentialRequest) -> Credentials:
        await asyncio.get_running_loop().create_future()
        raise AssertionError("unreachable")

    # end function hang

    setup.credential_provider.side_effect = hang
    sockets[-1].disconnect()
    await timers.advance(0)
    await timers.advance(1_000)

    # A deadline is a transient failure: the next retry is scheduled.
    assert setup.channel.state == "reconnecting"
    assert timers.count == 1
    await setup.channel.close()


# end function test_retries_a_reconnect_attempt_that_times_out


async def test_rounds_a_fractional_outage_up_in_the_lookback(
    setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    sockets[-1].disconnect()
    setup.clocks.monotonic = 1_500.5
    await timers.advance(0)

    (request,) = reconnect_requests(setup)
    assert request.replay_lookback_ms == 6_501
    await setup.channel.close()


# end function test_rounds_a_fractional_outage_up_in_the_lookback
