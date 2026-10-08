"""Listeners that call back into their own channel."""

import asyncio

import pytest

from tests.helpers.channel import (
    ChannelSetup,
    create_test_channel,
    establish,
    message_frame,
    once,
    presence_response_frame,
)
from tests.helpers.tasks import failure_of, flush
from tests.helpers.timers import FakeTimers
from tests.helpers.websocket import FakeWebSocket
from useceleris_client import (
    CelerisConnectionError,
    ChannelError,
    ChannelState,
    MessageMetadata,
    PresencePage,
)


@pytest.fixture
async def setup(sockets: list[FakeWebSocket], timers: FakeTimers) -> ChannelSetup:
    return await establish(create_test_channel(timers), sockets)


# end function setup


async def test_sends_a_publish_from_a_message_listener_after_the_dispatch(
    setup: ChannelSetup, sockets: list[FakeWebSocket]
) -> None:
    publishes: list[asyncio.Task[None]] = []
    sent_during_dispatch: list[int] = []

    def reply(payload: bytes, metadata: MessageMetadata) -> None:
        publishes.append(
            asyncio.ensure_future(setup.channel.segment("chat").publish(b"reply"))
        )

    # end function reply

    def count_sent(payload: bytes, metadata: MessageMetadata) -> None:
        sent_during_dispatch.append(sockets[-1].send.call_count)

    # end function count_sent

    setup.channel.segment("chat").on_message(reply)
    setup.channel.segment("chat").on_message(count_sent)
    sockets[-1].receive(message_frame("chat", "id-1", "x"))

    assert sent_during_dispatch == [0]
    await asyncio.gather(*publishes)
    assert sockets[-1].sent_frames() == [
        "@PUB\n$4\nchat\n$11\ngenerated-1\n$5\nreply\n"
    ]


# end function test_sends_a_publish_from_a_message_listener_after_the_dispatch


async def test_subscribes_and_cancels_from_a_listener(
    setup: ChannelSetup, sockets: list[FakeWebSocket]
) -> None:
    errors: list[ChannelError] = []
    setup.channel.events().on_error(errors.append)
    sports = setup.channel.segment("sports").subscribe()
    later: list[str] = []

    def switch(payload: bytes, metadata: MessageMetadata) -> None:
        setup.channel.segment("news").subscribe()
        sports.cancel()

    # end function switch

    setup.channel.segment("chat").on_message(switch)
    setup.channel.segment("chat").on_message(
        lambda payload, metadata: later.append(metadata.message_id)
    )
    sockets[-1].receive(message_frame("chat", "id-1", "x"))
    await flush()

    assert later == ["id-1"]
    assert errors == []
    assert sockets[-1].sent_frames() == [
        "@SUB\n$6\nsports\n",
        "@SUB\n$4\nnews\n",
        "@UNSUB\n$6\nsports\n",
    ]


# end function test_subscribes_and_cancels_from_a_listener


async def test_closes_from_a_listener_without_raising(
    setup: ChannelSetup, sockets: list[FakeWebSocket]
) -> None:
    errors: list[ChannelError] = []
    setup.channel.events().on_error(errors.append)
    closes: list[asyncio.Task[None]] = []
    delivered: list[str] = []

    def close(payload: bytes, metadata: MessageMetadata) -> None:
        delivered.append(metadata.message_id)

        if not closes:
            closes.append(asyncio.ensure_future(setup.channel.close()))

    # end function close

    setup.channel.segment("chat").on_message(close)
    sockets[-1].receive(message_frame("chat", "id-1", "x"))
    await asyncio.gather(*closes)
    sockets[-1].receive(message_frame("chat", "id-2", "x"))

    assert setup.channel.state == "closed"
    assert delivered == ["id-1"]
    assert errors == []


# end function test_closes_from_a_listener_without_raising


async def test_connects_again_from_a_failed_state_listener(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    setup = create_test_channel(timers)
    setup.credential_provider.side_effect = once(RuntimeError("failure"))
    retries: list[asyncio.Task[None]] = []

    def retry(state: ChannelState) -> None:
        if state == "failed" and not retries:
            retries.append(asyncio.ensure_future(setup.channel.connect()))

    # end function retry

    setup.channel.events().on_state_change(retry)
    error = await failure_of(asyncio.ensure_future(setup.channel.connect()))
    assert isinstance(error, CelerisConnectionError)
    assert error.code == "Transport"

    await flush()
    sockets[-1].open()
    await retries[0]
    assert setup.channel.state == "connected"


# end function test_connects_again_from_a_failed_state_listener


async def test_settles_a_presence_query_from_a_listener_when_its_response_arrives(
    setup: ChannelSetup, sockets: list[FakeWebSocket]
) -> None:
    queries: list[asyncio.Task[PresencePage]] = []

    def ask(payload: bytes, metadata: MessageMetadata) -> None:
        queries.append(
            asyncio.ensure_future(
                setup.channel.segment("chat").presence_list(page=1, per_page=25)
            )
        )

    # end function ask

    setup.channel.segment("chat").on_message(ask)
    sockets[-1].receive(message_frame("chat", "id-1", "x"))
    await flush()
    assert not queries[0].done()

    sockets[-1].receive(presence_response_frame(request_id="1", total=3))
    assert (await queries[0]).total == 3


# end function test_settles_a_presence_query_from_a_listener_when_its_response_arrives
