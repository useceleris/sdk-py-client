"""Repetition and volume on fake sockets."""

import asyncio

import pytest

from tests.helpers.channel import create_client_channel, create_test_channel, establish
from tests.helpers.tasks import failure_of, flush
from tests.helpers.timers import FakeTimers
from tests.helpers.websocket import FakeWebSocket
from useceleris_client import CelerisConnectionError, Channel


async def test_leaves_nothing_behind_after_50_connect_and_close_cycles(
    sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    channels: list[Channel] = []

    for cycle in range(50):
        setup = create_test_channel(timers)
        channels.append(setup.channel)

        if cycle % 2 == 0:
            await establish(setup, sockets)
            await setup.channel.close()
            continue

        # Closed while the socket is still opening.
        connecting = asyncio.ensure_future(setup.channel.connect())
        await flush()
        await setup.channel.close()
        error = await failure_of(connecting)
        assert isinstance(error, CelerisConnectionError)
        assert error.code == "Cancelled"

    assert [channel.state for channel in channels] == ["closed"] * 50
    assert len(sockets) == 50
    assert [socket.ready_state for socket in sockets] == [FakeWebSocket.CLOSED] * 50
    assert timers.count == 0


async def test_sends_many_queued_publishes_in_order_once_the_writer_drains(
    monkeypatch: pytest.MonkeyPatch, sockets: list[FakeWebSocket], timers: FakeTimers
) -> None:
    setup = await establish(
        create_client_channel(monkeypatch, timers, publish_queue_size=1_000), sockets
    )
    socket = sockets[-1]
    # Any send is refused as backpressure until the buffer empties.
    socket.buffered_amount = 2 * 1024 * 1024
    segments = [setup.channel.segment(name) for name in ("a", "b", "default")]

    queued = [
        asyncio.ensure_future(segments[index % 3].publish(str(index).encode()))
        for index in range(1_000)
    ]
    await timers.advance(0)
    socket.send.assert_not_called()

    socket.buffered_amount = 0
    await timers.advance(50)
    await asyncio.gather(*queued)

    payloads = [frame.split("\n")[-2] for frame in socket.sent_frames()]
    assert payloads == [str(index) for index in range(1_000)]
    assert timers.count == 0
