import asyncio
from collections.abc import Callable
from typing import Any

import pytest

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
    ConfigurationError,
    CredentialRequest,
    Credentials,
    create_client,
)


async def provide(request: CredentialRequest) -> Credentials:
    return TEST_CREDENTIALS


# end function provide


async def test_moves_idle_to_connecting_to_connected(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    channel = create_test_channel(timers).channel
    states: list[str] = []
    channel.events().on_state_change(states.append)

    assert channel.state == "idle"
    pending = asyncio.ensure_future(channel.connect())
    await flush()
    assert channel.state == "connecting"

    sockets[0].open()
    await pending

    assert channel.state == "connected"
    assert states == ["connecting", "connected"]


# end function test_moves_idle_to_connecting_to_connected


async def test_rejects_concurrent_connect_with_operation_in_progress(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    channel = create_test_channel(timers).channel
    pending = asyncio.ensure_future(channel.connect())
    await flush()

    with pytest.raises(CelerisConnectionError) as caught:
        await channel.connect()

    assert caught.value.code == "OperationInProgress"
    sockets[0].open()
    await pending

    with pytest.raises(CelerisConnectionError) as caught:
        await channel.connect()

    assert caught.value.code == "OperationInProgress"
    assert (
        str(caught.value) == "connect() was already called; the channel is connected."
    )
    assert channel.state == "connected"


# end function test_rejects_concurrent_connect_with_operation_in_progress


async def test_rejects_connect_after_close_with_not_connected(
    timers: FakeTimers,
) -> None:
    channel = create_test_channel(timers).channel
    await channel.close()

    with pytest.raises(CelerisConnectionError) as caught:
        await channel.connect()

    assert caught.value.code == "NotConnected"
    assert channel.state == "closed"


# end function test_rejects_connect_after_close_with_not_connected


async def test_fails_initial_connect_without_dispatching_on_error(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    setup = create_test_channel(timers)
    errors: list[ChannelError] = []
    setup.channel.events().on_error(errors.append)
    setup.credential_provider.side_effect = RuntimeError("synthetic-secret")

    with pytest.raises(CelerisConnectionError) as caught:
        await setup.channel.connect()

    assert caught.value.code == "Transport"
    assert setup.channel.state == "failed"
    assert errors == []


# end function test_fails_initial_connect_without_dispatching_on_error


async def test_permits_explicit_restart_from_failed(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    setup = create_test_channel(timers)
    setup.credential_provider.side_effect = once(RuntimeError("failure"))

    with pytest.raises(CelerisConnectionError):
        await setup.channel.connect()

    assert setup.channel.state == "failed"
    await establish(setup, sockets)
    assert setup.channel.state == "connected"


# end function test_permits_explicit_restart_from_failed


async def test_cancelling_connect_cancels_the_attempt_and_fails_the_channel(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    setup = create_test_channel(timers)
    provider_cancelled = asyncio.Event()

    async def hang(request: CredentialRequest) -> Credentials:
        try:
            await asyncio.get_running_loop().create_future()
        except asyncio.CancelledError:
            provider_cancelled.set()
            raise

        raise AssertionError("unreachable")

    # end function hang

    setup.credential_provider.side_effect = hang
    pending = asyncio.ensure_future(setup.channel.connect())
    await flush()

    pending.cancel()

    assert isinstance(await failure_of(pending), asyncio.CancelledError)
    await flush()
    assert setup.channel.state == "failed"
    assert provider_cancelled.is_set()
    assert timers.count == 0


# end function test_cancelling_connect_cancels_the_attempt_and_fails_the_channel


async def test_passes_initial_credential_requests_without_outage_fields(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    setup = await establish(create_test_channel(timers), sockets)

    setup.credential_provider.assert_awaited_once_with(
        CredentialRequest(channel_reference="room-1", reason="initial")
    )


# end function test_passes_initial_credential_requests_without_outage_fields


async def test_returns_the_same_handler_from_events(timers: FakeTimers) -> None:
    channel = create_test_channel(timers).channel

    assert channel.events() is channel.events()


# end function test_returns_the_same_handler_from_events


async def test_dispatches_state_listeners_in_order_with_working_disposal(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    channel = create_test_channel(timers).channel
    order: list[str] = []
    dispose_first = channel.events().on_state_change(
        lambda state: order.append("first")
    )
    channel.events().on_state_change(lambda state: order.append("second"))

    connecting = asyncio.ensure_future(channel.connect())
    await flush()
    assert order == ["first", "second"]

    dispose_first()
    dispose_first()
    await channel.close()
    assert order == ["first", "second", "second", "second"]
    assert isinstance(await failure_of(connecting), CelerisConnectionError)


# end function test_dispatches_state_listeners_in_order_with_working_disposal


async def test_skips_a_listener_disposed_mid_dispatch_and_allows_duplicates(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    channel = create_test_channel(timers).channel
    order: list[str] = []
    dispose_second: Callable[[], None] = lambda: None  # noqa: E731

    def first(state: str) -> None:
        order.append("first")
        dispose_second()

    # end function first

    def shared(state: str) -> None:
        order.append("shared")

    # end function shared

    channel.events().on_state_change(first)
    dispose_second = channel.events().on_state_change(
        lambda state: order.append("second")
    )
    channel.events().on_state_change(shared)
    dispose_duplicate = channel.events().on_state_change(shared)

    connecting = asyncio.ensure_future(channel.connect())
    await flush()
    assert order == ["first", "shared", "shared"]

    dispose_duplicate()
    order.clear()
    await channel.close()
    assert order == ["first", "shared", "first", "shared"]
    await failure_of(connecting)


# end function test_skips_a_listener_disposed_mid_dispatch_and_allows_duplicates


async def test_contains_throwing_listeners_and_reports_once_through_on_error(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    channel = create_test_channel(timers).channel
    errors: list[ChannelError] = []
    order: list[str] = []

    def raise_secret(value: object) -> None:
        raise RuntimeError("listener-secret")

    # end function raise_secret

    channel.events().on_error(errors.append)
    channel.events().on_error(raise_secret)
    channel.events().on_state_change(raise_secret)
    channel.events().on_state_change(lambda state: order.append("after"))

    connecting = asyncio.ensure_future(channel.connect())
    await flush()

    assert order == ["after"]
    assert len(errors) == 1
    assert isinstance(errors[0], CelerisConnectionError)
    assert errors[0].code == "Transport"
    assert str(errors[0]) == (
        "A listener callback raised an error; the channel caught it and kept running."
    )
    assert errors[0].__context__ is None
    assert "secret" not in repr(errors[0])
    assert channel.state == "connecting"
    await channel.close()
    await failure_of(connecting)


# end function test_contains_throwing_listeners_and_reports_once_through_on_error


async def test_refuses_coroutine_function_listeners(timers: FakeTimers) -> None:
    channel = create_test_channel(timers).channel

    async def listener(state: str) -> None:
        pass

    # end function listener

    untyped: Any = listener

    with pytest.raises(ConfigurationError) as caught:
        channel.events().on_state_change(untyped)

    assert str(caught.value) == (
        "Invalid listener. It must be a synchronous callable; start a task from it "
        "for asynchronous work."
    )

    with pytest.raises(ConfigurationError):
        channel.default_segment().on_message(untyped)


# end function test_refuses_coroutine_function_listeners


async def test_fails_the_attempt_when_the_socket_closes_as_it_opens(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    channel = create_test_channel(timers).channel
    pending = asyncio.ensure_future(channel.connect())
    await flush()

    # The socket opens and closes before the waiting connect resumes.
    sockets[0].open()
    sockets[0].disconnect()

    error = await failure_of(pending)
    assert isinstance(error, CelerisConnectionError)
    assert (error.code, str(error)) == (
        "Transport",
        "The WebSocket closed as soon as it opened.",
    )
    assert channel.state == "failed"


# end function test_fails_the_attempt_when_the_socket_closes_as_it_opens


async def test_creates_a_fresh_channel_per_call_and_validates_references_eagerly(
    sockets: list[FakeWebSocket],
) -> None:
    client = create_client(base_url="wss://example.test", credential_provider=provide)

    assert client.channel("room-1") is not client.channel("room-1")

    with pytest.raises(ConfigurationError) as caught:
        client.channel("bad ref!")

    assert str(caught.value) == (
        "Invalid channel reference. Must contain only ASCII letters, digits, "
        "hyphens (-) or underscores (_)."
    )

    with pytest.raises(ConfigurationError, match=r"^Invalid channel reference\. Must"):
        client.channel("")

    assert sockets == []


# end function test_creates_a_fresh_channel_per_call_and_validates_references_eagerly


async def test_connects_to_the_built_in_endpoint_when_no_base_url_is_given(
    sockets: list[FakeWebSocket],
) -> None:
    # ENDPOINT-01: consumers do not configure where Celeris lives.
    channel = create_client(credential_provider=provide).channel("room-1")
    pending = asyncio.ensure_future(channel.connect())
    await flush()

    assert len(sockets) == 1
    assert sockets[0].url.startswith("wss://realtime.useceleris.com/channel/room-1?")

    sockets[0].open()
    await pending
    await channel.close()


# end function test_connects_to_the_built_in_endpoint_when_no_base_url_is_given


async def test_rejects_connect_while_reconnecting(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    setup = await establish(create_test_channel(timers), sockets)
    sockets[-1].disconnect()

    with pytest.raises(CelerisConnectionError) as caught:
        await setup.channel.connect()

    assert caught.value.code == "OperationInProgress"
    await setup.channel.close()


# end function test_rejects_connect_while_reconnecting


async def test_closes_a_socket_that_opened_as_the_channel_closed(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    setup = create_test_channel(timers)
    setup.channel.segment("chat").subscribe()
    pending = asyncio.ensure_future(setup.channel.connect())
    await flush()

    # The open resolves the attempt; the close lands before connect resumes.
    sockets[0].open()
    await setup.channel.close()

    error = await failure_of(pending)
    assert isinstance(error, CelerisConnectionError)
    assert error.code == "Cancelled"
    sockets[0].close.assert_called_once()
    sockets[0].send.assert_not_called()


# end function test_closes_a_socket_that_opened_as_the_channel_closed


async def test_fails_an_initial_connect_whose_restoring_write_fails(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    setup = create_test_channel(timers)
    states: list[str] = []
    setup.channel.events().on_state_change(states.append)
    setup.channel.segment("chat").subscribe()
    pending = asyncio.ensure_future(setup.channel.connect())
    await flush()

    sockets[-1].send.side_effect = RuntimeError("synthetic")
    sockets[-1].open()
    await failure_of(pending)

    # No retry: an initial connect fails to the caller.
    assert states == ["connecting", "failed"]
    assert timers.count == 0


# end function test_fails_an_initial_connect_whose_restoring_write_fails


async def test_reports_nothing_received_before_connect_resumes(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    setup = create_test_channel(timers)
    errors: list[ChannelError] = []
    setup.channel.events().on_error(errors.append)
    pending = asyncio.ensure_future(setup.channel.connect())
    await flush()

    sockets[0].open()
    sockets[0].receive("text")
    await pending

    assert errors == []


# end function test_reports_nothing_received_before_connect_resumes


async def test_a_listener_added_during_dispatch_waits_for_the_next_event(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    channel = create_test_channel(timers).channel
    seen: list[str] = []

    def add_another(state: str) -> None:
        channel.events().on_state_change(lambda later: seen.append(later))

    # end function add_another

    channel.events().on_state_change(add_another)
    connecting = asyncio.ensure_future(channel.connect())
    await flush()

    assert seen == []
    await channel.close()
    await failure_of(connecting)
    assert seen[:1] == ["closing"]


# end function test_a_listener_added_during_dispatch_waits_for_the_next_event


async def test_never_nests_error_dispatch(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    setup = await establish(create_test_channel(timers), sockets)
    errors: list[ChannelError] = []

    def report(error: ChannelError) -> None:
        errors.append(error)
        # Reporting this error causes another: a listener fails during it.
        setup.channel.events().on_state_change(fail)
        sockets[-1].disconnect()

    # end function report

    def fail(state: str) -> None:
        raise RuntimeError("synthetic")

    # end function fail

    setup.channel.events().on_error(report)
    sockets[-1].receive(b"-Err\n+SendError\n$-1\n$1\nx\n$-1\n")

    assert len(errors) == 1
    await setup.channel.close()


# end function test_never_nests_error_dispatch
