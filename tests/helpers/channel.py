import asyncio
import itertools
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock

from tests.helpers.tasks import flush
from tests.helpers.timers import FakeTimers
from tests.helpers.websocket import FakeWebSocket
from useceleris_client._channel import Channel, ChannelInternals
from useceleris_client._credential_types import Credentials

TEST_CREDENTIALS = Credentials(payload="payload-1", signature="signature-1")


@dataclass
class ChannelClocks:
    monotonic: float = 0
    wall: int = 1_700_000_000_000
    random_value: float = 0


@dataclass
class ChannelSetup:
    channel: Channel
    credential_provider: AsyncMock
    clocks: ChannelClocks


def create_test_channel(timers: FakeTimers, **overrides: Any) -> ChannelSetup:
    clocks = ChannelClocks()
    generated_message_ids = itertools.count(1)
    credential_provider = AsyncMock(return_value=TEST_CREDENTIALS)
    internals: dict[str, Any] = {
        "base_url": "wss://example.test/",
        "channel_reference": "room-1",
        "allow_insecure_loopback": False,
        "connect_timeout_ms": 15_000,
        "presence_query_timeout_ms": 10_000,
        "credential_provider": credential_provider,
        "clock": lambda: clocks.monotonic,
        "wall_clock": lambda: clocks.wall,
        "random": lambda: clocks.random_value,
        "generate_message_id": lambda: f"generated-{next(generated_message_ids)}",
        "timers": timers,
    }
    internals.update(overrides)

    return ChannelSetup(
        Channel(ChannelInternals(**internals)), credential_provider, clocks
    )


async def establish(setup: ChannelSetup, sockets: list[FakeWebSocket]) -> ChannelSetup:
    pending = asyncio.ensure_future(setup.channel.connect())
    await flush()
    sockets[-1].open()
    await pending

    return setup


def once(*first: object, then: object = TEST_CREDENTIALS) -> Iterator[object]:
    """A mock side effect: these outcomes in turn, then the same one forever."""
    return itertools.chain(first, itertools.repeat(then))


def message_frame(segment_id: str, message_id: str | None, body: str) -> bytes:
    identifier = (
        "$-1\n"
        if message_id is None
        else f"${len(message_id.encode())}\n{message_id}\n"
    )

    return (
        f"@MSG\n$4\nuser\n${len(segment_id.encode())}\n{segment_id}\n"
        f"{identifier}:1\n${len(body.encode())}\n{body}\n"
    ).encode()


# Laid out like the server's ErrorMessage.
def error_frame(
    type: str,
    message: str,
    sub_type: str | None = None,
    resource: str = "$-1\n",
) -> bytes:
    sub_type_field = "$-1\n" if sub_type is None else f"+{sub_type}\n"

    return (
        f"-Err\n+{type}\n{sub_type_field}${len(message.encode())}\n{message}\n"
        + resource
    ).encode()
