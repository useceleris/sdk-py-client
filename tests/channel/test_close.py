import asyncio

from tests.helpers.channel import (
    TEST_CREDENTIALS,
    create_test_channel,
    establish,
    once,
)
from tests.helpers.tasks import failure_of, flush
from tests.helpers.timers import FakeTimers
from tests.helpers.websocket import FakeWebSocket
from useceleris_client import (
    CelerisConnectionError,
    ChannelError,
    CredentialRequest,
    Credentials,
)


async def test_is_idempotent_and_every_call_waits_for_the_same_close(
    timers: FakeTimers,
) -> None:
    channel = create_test_channel(timers).channel
    states: list[str] = []
    channel.events().on_state_change(states.append)

    await asyncio.gather(channel.close(), channel.close())

    assert channel.state == "closed"
    assert states == ["closing", "closed"]
    await channel.close()
    assert states == ["closing", "closed"]


# end function test_is_idempotent_and_every_call_waits_for_the_same_close


async def test_closes_a_connected_channel_on_the_sockets_close(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    channel = (await establish(create_test_channel(timers), sockets)).channel
    states: list[str] = []
    channel.events().on_state_change(states.append)

    await channel.close()

    assert states == ["closing", "closed"]
    sockets[0].close.assert_called_once()
    assert timers.count == 0


# end function test_closes_a_connected_channel_on_the_sockets_close


async def test_applies_the_five_second_budget_when_the_close_never_arrives(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    channel = (await establish(create_test_channel(timers), sockets)).channel
    sockets[0].close.side_effect = None

    pending = asyncio.ensure_future(channel.close())
    await flush()
    assert channel.state == "closing"

    await timers.advance(4_999)
    assert channel.state == "closing"

    await timers.advance(1)
    await pending
    assert channel.state == "closed"
    assert timers.count == 0


# end function test_applies_the_five_second_budget_when_the_close_never_arrives


async def test_aborts_a_pending_attempt_and_ignores_late_credentials(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    setup = create_test_channel(timers)
    provider_cancelled = asyncio.Event()

    # Ignores its cancellation and returns credentials anyway.
    async def stubborn(request: CredentialRequest) -> Credentials:
        try:
            await asyncio.get_running_loop().create_future()
        except asyncio.CancelledError:
            provider_cancelled.set()

        return TEST_CREDENTIALS

    # end function stubborn

    setup.credential_provider.side_effect = stubborn
    pending = asyncio.ensure_future(setup.channel.connect())
    await flush()

    await setup.channel.close()

    error = await failure_of(pending)
    assert isinstance(error, CelerisConnectionError)
    assert error.code == "Cancelled"
    assert provider_cancelled.is_set()
    assert setup.channel.state == "closed"
    await flush()
    assert sockets == []
    assert timers.count == 0


# end function test_aborts_a_pending_attempt_and_ignores_late_credentials


async def test_clears_the_retry_timer_when_closed_while_reconnecting(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    channel = (await establish(create_test_channel(timers), sockets)).channel

    sockets[0].disconnect()
    assert channel.state == "reconnecting"
    assert timers.count == 1

    await channel.close()
    assert channel.state == "closed"
    assert timers.count == 0


# end function test_clears_the_retry_timer_when_closed_while_reconnecting


async def test_ignores_stale_socket_events_after_close(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    channel = (await establish(create_test_channel(timers), sockets)).channel
    states: list[str] = []
    errors: list[ChannelError] = []
    channel.events().on_state_change(states.append)
    channel.events().on_error(errors.append)
    await channel.close()
    states.clear()

    sockets[0].open()
    sockets[0].fail()
    sockets[0].disconnect()
    sockets[0].receive("text")

    assert states == []
    assert errors == []
    assert channel.state == "closed"
    assert timers.count == 0


# end function test_ignores_stale_socket_events_after_close


async def test_closes_from_every_non_terminal_state(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    idle = create_test_channel(timers)
    await idle.channel.close()
    assert idle.channel.state == "closed"

    failed = create_test_channel(timers)
    failed.credential_provider.side_effect = once(RuntimeError("failure"))
    error = await failure_of(asyncio.ensure_future(failed.channel.connect()))
    assert isinstance(error, CelerisConnectionError)
    assert error.code == "Transport"
    await failed.channel.close()
    assert failed.channel.state == "closed"

    connected = await establish(create_test_channel(timers), sockets)
    await connected.channel.close()
    assert connected.channel.state == "closed"
    assert timers.count == 0


# end function test_closes_from_every_non_terminal_state


async def test_a_cancelled_close_still_completes(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    channel = (await establish(create_test_channel(timers), sockets)).channel
    sockets[0].close.side_effect = None
    first = asyncio.ensure_future(channel.close())
    await flush()

    first.cancel()
    assert isinstance(await failure_of(first), asyncio.CancelledError)

    second = asyncio.ensure_future(channel.close())
    await timers.advance(5_000)
    await second
    assert channel.state == "closed"


# end function test_a_cancelled_close_still_completes


async def test_refuses_a_publish_while_closing_and_writes_nothing(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    channel = (await establish(create_test_channel(timers), sockets)).channel
    sockets[0].close.side_effect = None
    closing = asyncio.ensure_future(channel.close())
    await flush()
    assert channel.state == "closing"

    error = await failure_of(
        asyncio.ensure_future(channel.default_segment().publish(b"x"))
    )
    assert isinstance(error, CelerisConnectionError)
    assert (error.code, str(error)) == (
        "NotConnected",
        "Channel is not connected; it is closing.",
    )
    sockets[0].send.assert_not_called()

    await timers.advance(5_000)
    await closing


# end function test_refuses_a_publish_while_closing_and_writes_nothing
