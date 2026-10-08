import pytest

from tests.helpers.channel import (
    ChannelSetup,
    create_test_channel,
    establish,
    message_frame,
)
from tests.helpers.tasks import flush
from tests.helpers.timers import FakeTimers
from tests.helpers.websocket import FakeWebSocket
from useceleris_client import ChannelError, MessageMetadata


@pytest.fixture
async def setup(sockets: list[FakeWebSocket], timers: FakeTimers) -> ChannelSetup:
    return await establish(create_test_channel(timers), sockets)


# end function setup


async def reconnect(sockets: list[FakeWebSocket], timers: FakeTimers) -> None:
    sockets[-1].disconnect()
    await timers.advance(0)
    await flush()
    sockets[-1].open()
    await flush()


# end function reconnect


def bump_buffer(socket: FakeWebSocket) -> None:
    def bump(data: bytes) -> None:
        socket.buffered_amount += 1

    # end function bump

    socket.send.side_effect = bump


# end function bump_buffer


async def test_restores_current_intent_messages_then_presence_in_order(
    setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    channel = setup.channel
    channel.segment("beta").subscribe()
    channel.segment("alpha").subscribe()
    cancelled_messages = channel.segment("gone").subscribe()
    channel.default_segment().subscribe_presence()
    channel.segment("alpha").subscribe_presence()
    cancelled_presence = channel.segment("brief").subscribe_presence()

    await channel.segment("beta").publish(b"x")
    sockets[-1].disconnect()
    cancelled_messages.cancel()
    cancelled_presence.cancel()

    await timers.advance(0)
    await flush()
    reconnect_socket = sockets[-1]
    reconnect_socket.open()
    await flush()

    assert channel.state == "connected"
    # Interests only, messages before presence, registration order, cancelled
    # intent excluded, no publish resend.
    assert reconnect_socket.sent_frames() == [
        "@SUB\n$4\nbeta\n",
        "@SUB\n$5\nalpha\n",
        "@PRES_SUB\n$7\ndefault\n",
        "@PRES_SUB\n$5\nalpha\n",
    ]


# end function test_restores_current_intent_messages_then_presence_in_order


async def test_sends_restoration_frames_before_the_connected_state_and_recovery(
    setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    setup.channel.segment("chat").subscribe()
    log: list[str] = []
    setup.channel.events().on_state_change(
        lambda state: log.append(f"state:{state} frames:{sockets[-1].send.call_count}")
    )
    setup.channel.events().on_recovery(
        lambda event: log.append(f"recovery:{event.retry_index}")
    )

    await reconnect(sockets, timers)

    assert log == [
        # At "reconnecting" the latest socket is still the old one, carrying
        # only the original SUB; at "connected" the reconnect socket already
        # carries its restoration SUB, before the recovery event fires.
        "state:reconnecting frames:1",
        "state:connected frames:1",
        "recovery:0",
    ]


# end function test_sends_restoration_frames_before_the_connected_state_and_recovery


async def test_keeps_the_default_segment_delivering_without_restoring_it(
    setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    delivered: list[str] = []
    setup.channel.default_segment().subscribe()
    setup.channel.segment("chat").subscribe()

    def record(payload: bytes, metadata: MessageMetadata) -> None:
        delivered.append(metadata.message_id)

    # end function record

    setup.channel.default_segment().on_message(record)

    await reconnect(sockets, timers)
    reconnect_socket = sockets[-1]

    # The server auto-joins "default" on the new connection, so restoring it
    # would be a redundant SUB; a named segment must be rejoined explicitly.
    assert reconnect_socket.sent_frames() == ["@SUB\n$4\nchat\n"]

    # Membership is what matters: the listener still receives.
    reconnect_socket.receive(message_frame("default", "id-1", "a"))
    assert delivered == ["id-1"]


# end function test_keeps_the_default_segment_delivering_without_restoring_it


async def test_absorbs_replayed_duplicates_across_reconnect_while_new_ids_flow(
    setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    delivered: list[str] = []
    setup.channel.segment("chat").subscribe()
    setup.channel.segment("chat").on_message(
        lambda payload, metadata: delivered.append(metadata.message_id)
    )

    sockets[-1].receive(message_frame("chat", "id-1", "a"))
    sockets[-1].receive(message_frame("chat", "id-2", "b"))
    await reconnect(sockets, timers)

    # Replay overlap redelivers old ids; the window absorbs them.
    sockets[-1].receive(message_frame("chat", "id-1", "a"))
    sockets[-1].receive(message_frame("chat", "id-2", "b"))
    sockets[-1].receive(message_frame("chat", "id-3", "c"))

    assert delivered == ["id-1", "id-2", "id-3"]


# end function test_absorbs_replayed_duplicates_across_reconnect_while_new_ids_flow


async def test_restores_more_than_64_subscriptions_as_the_writer_drains(
    setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    for index in range(65):
        setup.channel.segment(f"segment-{index}").subscribe()

    errors: list[ChannelError] = []
    setup.channel.events().on_error(errors.append)

    sockets[-1].disconnect()
    await timers.advance(0)
    await flush()
    reconnect_socket = sockets[-1]
    bump_buffer(reconnect_socket)
    reconnect_socket.open()
    await flush()

    assert setup.channel.state == "connected"
    assert reconnect_socket.send.call_count == 64

    reconnect_socket.buffered_amount = 0
    await timers.advance(50)

    assert reconnect_socket.send.call_count == 65
    assert errors == []


# end function test_restores_more_than_64_subscriptions_as_the_writer_drains


async def test_restores_intent_again_on_a_second_recovery_without_duplicates(
    setup: ChannelSetup, sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    setup.channel.segment("chat").subscribe()
    setup.channel.segment("chat").subscribe_presence()

    await reconnect(sockets, timers)
    first_recovery_socket = sockets[-1]
    await reconnect(sockets, timers)
    second_recovery_socket = sockets[-1]

    expected = ["@SUB\n$4\nchat\n", "@PRES_SUB\n$4\nchat\n"]
    assert first_recovery_socket.sent_frames() == expected
    assert second_recovery_socket.sent_frames() == expected
    assert setup.channel.state == "connected"
    assert timers.count == 0


# end function test_restores_intent_again_on_a_second_recovery_without_duplicates
