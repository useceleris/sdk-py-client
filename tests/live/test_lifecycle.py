import asyncio
import time
from dataclasses import dataclass
from typing import Any

import pytest

from tests.live.helpers import (
    DroppingProxy,
    client_id,
    connected_channel,
    next_message,
    sign_credentials,
    signing_secret,
    started,
    unique_channel_reference,
    wait_for,
    websocket_url,
)
from useceleris_client import (
    CelerisConnectionError,
    Channel,
    ChannelError,
    ChannelState,
    CredentialRequest,
    Credentials,
    RecoveryEvent,
    create_client,
)

pytestmark = pytest.mark.live


@dataclass
class Proxied:
    """A channel behind the dropping proxy, with everything it reported."""

    channel: Channel
    requests: list[CredentialRequest]
    states: list[ChannelState]
    errors: list[ChannelError]


# end class Proxied


def proxied_channel(
    proxy: DroppingProxy, opened: list[Channel], label: str, **options: Any
) -> Proxied:
    requests: list[CredentialRequest] = []

    async def provide(request: CredentialRequest) -> Credentials:
        requests.append(request)

        return sign_credentials(client_id(), signing_secret())

    # end function provide

    channel = create_client(
        base_url=proxy.url,
        allow_insecure_loopback=True,
        credential_provider=provide,
        **options,
    ).channel(unique_channel_reference(label))
    opened.append(channel)
    states: list[ChannelState] = []
    channel.events().on_state_change(states.append)
    errors: list[ChannelError] = []
    channel.events().on_error(errors.append)

    return Proxied(channel, requests, states, errors)


# end function proxied_channel


async def until_state(channel: Channel, state: ChannelState, timeout_s: float) -> None:
    if channel.state == state:
        return

    await wait_for(
        channel.events().on_state_change,
        lambda current: current == state,
        f"the {state} state",
        timeout_s,
    )


# end function until_state


async def test_fails_a_connect_at_its_connect_timeout_when_the_server_never_answers(
    proxy: DroppingProxy, opened: list[Channel]
) -> None:
    setup = proxied_channel(
        proxy, opened, "lifecycle-timeout", connect_timeout_ms=2_000
    )
    proxy.blackhole = True

    begun = time.monotonic()

    with pytest.raises(CelerisConnectionError) as caught:
        await setup.channel.connect()

    elapsed = time.monotonic() - begun

    assert (caught.value.code, caught.value.message) == (
        "Timeout",
        "Connection attempt timed out after 2000 ms.",
    )
    assert 1.9 <= elapsed < 4
    assert setup.channel.state == "failed"
    assert setup.states == ["connecting", "failed"]
    # One failure, one report: the caller only (LIFE-02).
    assert setup.errors == []


# end function test_fails_a_connect_at_its_connect_timeout_when_the_server_never_answers


async def test_times_out_at_a_1_ms_connect_timeout_against_the_real_server(
    opened: list[Channel],
) -> None:
    async def provide(request: CredentialRequest) -> Credentials:
        return sign_credentials(client_id(), signing_secret())

    # end function provide

    channel = create_client(
        base_url=websocket_url(),
        allow_insecure_loopback=True,
        connect_timeout_ms=1,
        credential_provider=provide,
    ).channel(unique_channel_reference("lifecycle-1ms"))
    opened.append(channel)

    with pytest.raises(CelerisConnectionError) as caught:
        await channel.connect()

    assert caught.value.code == "Timeout"
    assert channel.state == "failed"


# end function test_times_out_at_a_1_ms_connect_timeout_against_the_real_server


# Cancellation is asyncio's own (LANG-02): cancelling the task that awaits
# connect() abandons the attempt.
async def test_cancels_a_connect_from_its_task(
    proxy: DroppingProxy, opened: list[Channel]
) -> None:
    setup = proxied_channel(proxy, opened, "lifecycle-abort")
    proxy.blackhole = True

    pending = asyncio.ensure_future(setup.channel.connect())
    await asyncio.sleep(0.5)
    pending.cancel()

    with pytest.raises(asyncio.CancelledError):
        await pending

    assert setup.channel.state == "failed"


# end function test_cancels_a_connect_from_its_task


async def test_closes_a_channel_that_is_still_connecting(
    proxy: DroppingProxy, opened: list[Channel]
) -> None:
    setup = proxied_channel(proxy, opened, "lifecycle-close-connecting")
    proxy.blackhole = True

    pending = asyncio.ensure_future(setup.channel.connect())
    await asyncio.sleep(0.5)
    await setup.channel.close()

    with pytest.raises(CelerisConnectionError) as caught:
        await pending

    assert caught.value.code == "Cancelled"
    assert setup.channel.state == "closed"

    with pytest.raises(CelerisConnectionError) as caught:
        await setup.channel.connect()

    assert caught.value.code == "NotConnected"


# end function test_closes_a_channel_that_is_still_connecting


async def test_closes_a_channel_that_is_reconnecting_and_stops_its_retries(
    proxy: DroppingProxy, opened: list[Channel]
) -> None:
    setup = proxied_channel(proxy, opened, "lifecycle-close-reconnecting")
    await setup.channel.connect()
    proxy.refusing = True
    proxy.drop_all()
    await until_state(setup.channel, "reconnecting", 5)
    await asyncio.sleep(1)

    await setup.channel.close()
    requests_at_close = len(setup.requests)
    proxy.refusing = False
    await asyncio.sleep(5)

    assert setup.channel.state == "closed"
    assert len(setup.requests) == requests_at_close


# end function test_closes_a_channel_that_is_reconnecting_and_stops_its_retries


# QUEUE-01: a publish while reconnecting waits (test_reconnect covers it);
# after failed no recovery is in progress, so it is refused.
async def test_refuses_a_publish_after_failed_with_not_connected(
    proxy: DroppingProxy, opened: list[Channel]
) -> None:
    setup = proxied_channel(proxy, opened, "lifecycle-publish-failed")
    proxy.refusing = True

    with pytest.raises(CelerisConnectionError):
        await setup.channel.connect()

    assert setup.channel.state == "failed"

    with pytest.raises(CelerisConnectionError) as caught:
        await setup.channel.segment("chat").publish(b"x")

    assert (caught.value.code, caught.value.message) == (
        "NotConnected",
        "Channel is not connected; it is failed.",
    )


# end function test_refuses_a_publish_after_failed_with_not_connected


async def test_restarts_from_failed_with_a_connect_and_sends_held_subscriptions(
    proxy: DroppingProxy, opened: list[Channel]
) -> None:
    setup = proxied_channel(proxy, opened, "lifecycle-restart")
    chat: list[bytes] = []
    setup.channel.segment("chat").on_message(
        lambda payload, metadata: chat.append(payload)
    )
    setup.channel.segment("chat").subscribe()
    proxy.refusing = True

    with pytest.raises(CelerisConnectionError) as caught:
        await setup.channel.connect()

    assert caught.value.code == "Transport"
    assert setup.channel.state == "failed"

    proxy.refusing = False
    await setup.channel.connect()
    publisher = await connected_channel(
        setup.requests[0].channel_reference, opened=opened
    )
    await asyncio.sleep(1.5)
    arrived = await started(
        next_message(
            setup.channel.segment("chat"),
            lambda message: message.payload == b"after-restart",
            "the delivery after the restart",
        )
    )
    await publisher.segment("chat").publish(b"after-restart")
    await arrived

    assert [request.reason for request in setup.requests] == ["initial", "initial"]
    assert chat == [b"after-restart"]


# end function test_restarts_from_failed_with_a_connect_and_sends_held_subscriptions


@pytest.mark.timeout(240)
async def test_fails_after_ten_failed_reconnect_attempts_and_reports_it(
    proxy: DroppingProxy, opened: list[Channel]
) -> None:
    setup = proxied_channel(proxy, opened, "lifecycle-exhaustion")
    await setup.channel.connect()
    proxy.refusing = True
    proxy.drop_all()

    await until_state(setup.channel, "failed", 200)

    reconnects = [
        request for request in setup.requests if request.reason == "reconnect"
    ]

    assert len(reconnects) == 10
    assert len(setup.errors) == 1
    assert isinstance(setup.errors[0], CelerisConnectionError)
    assert setup.errors[0].code == "Transport"
    assert setup.states[-2:] == ["reconnecting", "failed"]


# end function test_fails_after_ten_failed_reconnect_attempts_and_reports_it


@pytest.mark.timeout(90)
async def test_fails_after_the_configured_maximum_of_2_reconnect_attempts(
    proxy: DroppingProxy, opened: list[Channel]
) -> None:
    setup = proxied_channel(
        proxy, opened, "lifecycle-maximum-attempts", maximum_reconnect_attempts=2
    )
    await setup.channel.connect()
    proxy.refusing = True
    proxy.drop_all()

    await until_state(setup.channel, "failed", 60)

    reconnects = [
        request for request in setup.requests if request.reason == "reconnect"
    ]

    assert len(reconnects) == 2
    assert len(setup.errors) == 1
    assert isinstance(setup.errors[0], CelerisConnectionError)
    assert setup.errors[0].code == "Transport"
    assert setup.states[-2:] == ["reconnecting", "failed"]


# end function test_fails_after_the_configured_maximum_of_2_reconnect_attempts


@pytest.mark.timeout(150)
async def test_resets_the_retry_budget_after_sixty_seconds_connected(
    proxy: DroppingProxy, opened: list[Channel]
) -> None:
    setup = proxied_channel(proxy, opened, "lifecycle-budget")
    recoveries: list[RecoveryEvent] = []
    setup.channel.events().on_recovery(recoveries.append)
    await setup.channel.connect()

    # A first outage uses at least two failed attempts before it recovers.
    proxy.refusing = True
    proxy.drop_all()
    deadline = time.monotonic() + 20

    while len(setup.requests) < 3:
        assert time.monotonic() < deadline, "two failed reconnect attempts"
        await asyncio.sleep(0.1)

    proxy.refusing = False
    await until_state(setup.channel, "connected", 30)

    assert recoveries[-1].retry_index >= 2

    # After sixty seconds connected, the next outage starts a new budget.
    await asyncio.sleep(61)
    second_recovery = await started(
        wait_for(
            setup.channel.events().on_recovery,
            lambda event: True,
            "the second recovery",
            30,
        )
    )
    proxy.drop_all()
    await second_recovery

    assert recoveries[-1].retry_index == 0


# end function test_resets_the_retry_budget_after_sixty_seconds_connected


@pytest.mark.timeout(150)
async def test_keeps_an_idle_connection_open_past_the_servers_60_second_heartbeat(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("lifecycle-idle")
    publisher = await connected_channel(reference, opened=opened)
    receiver = await connected_channel(reference, opened=opened)
    states: list[ChannelState] = []
    receiver.events().on_state_change(states.append)
    receiver.segment("chat").subscribe()

    # The websockets library pings and answers the server's pings; nothing
    # else is sent.
    await asyncio.sleep(95)
    arrived = await started(
        next_message(
            receiver.segment("chat"),
            lambda message: message.payload == b"still-here",
            "the delivery after the idle period",
        )
    )
    await publisher.segment("chat").publish(b"still-here")
    await arrived

    assert states == []
    assert receiver.state == "connected"


# end function test_keeps_an_idle_connection_open_past_the_servers_60_second_heartbeat
