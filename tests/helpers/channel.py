import asyncio
import itertools
import random
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock

import pytest

from tests.helpers.tasks import flush
from tests.helpers.timers import FakeTimers
from tests.helpers.websocket import FakeWebSocket
from useceleris_client import create_client
from useceleris_client._channel import Channel, ChannelInternals
from useceleris_client._credential_types import Credentials

TEST_CREDENTIALS = Credentials(payload="payload-1", signature="signature-1")


@dataclass
class ChannelClocks:
    monotonic: float = 0
    wall: int = 1_700_000_000_000
    random_value: float = 0


# end class ChannelClocks


@dataclass
class ChannelSetup:
    channel: Channel
    credential_provider: AsyncMock
    clocks: ChannelClocks


# end class ChannelSetup


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
        "publish_queue_size": 64,
        "deduplication_window_size": 1024,
        "maximum_reconnect_attempts": 10,
        "credential_provider": credential_provider,
        "clock": lambda: clocks.monotonic,
        "wall_clock": lambda: clocks.wall,
        "random": lambda: clocks.random_value,
        "generate_message_id": lambda: f"generated-{next(generated_message_ids)}",
        "timers": timers,
    }
    internals.update(overrides)
    # Mirrors the client's resolution: an omitted reconnect deadline is the
    # connect deadline, overridden or not.
    internals.setdefault("reconnect_timeout_ms", internals["connect_timeout_ms"])

    return ChannelSetup(
        Channel(ChannelInternals(**internals)), credential_provider, clocks
    )


# end function create_test_channel


def create_client_channel(
    monkeypatch: pytest.MonkeyPatch, timers: FakeTimers, **options: Any
) -> ChannelSetup:
    """A channel built through create_client, so the options are validated and
    passed on as a consumer's would be. The client's timers, clocks,
    randomness and message ids are replaced with the test's."""
    clocks = ChannelClocks()
    generated_message_ids = itertools.count(1)
    credential_provider = AsyncMock(return_value=TEST_CREDENTIALS)
    monkeypatch.setattr("useceleris_client._client.LOOP_TIMERS", timers)
    monkeypatch.setattr(
        "useceleris_client._client.monotonic_now", lambda: clocks.monotonic
    )
    monkeypatch.setattr("useceleris_client._client.wall_clock_now", lambda: clocks.wall)
    monkeypatch.setattr(
        "useceleris_client._client.generate_message_id",
        lambda: f"generated-{next(generated_message_ids)}",
    )
    monkeypatch.setattr(random, "random", lambda: clocks.random_value)
    client = create_client(
        **{
            "credential_provider": credential_provider,
            "base_url": "wss://example.test",
            **options,
        }
    )

    return ChannelSetup(client.channel("room-1"), credential_provider, clocks)


# end function create_client_channel


async def establish(setup: ChannelSetup, sockets: list[FakeWebSocket]) -> ChannelSetup:
    pending = asyncio.ensure_future(setup.channel.connect())
    await flush()
    sockets[-1].open()
    await pending

    return setup


# end function establish


def once(*first: object, then: object = TEST_CREDENTIALS) -> Iterator[object]:
    """A mock side effect: these outcomes in turn, then the same one forever."""
    return itertools.chain(first, itertools.repeat(then))


# end function once


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


# end function message_frame


def presence_response_frame(
    *,
    segment_id: str = "chat",
    request_id: str = "1",
    total: int = 1,
    per_page: int = 25,
    current_page: int = 1,
    from_: int = 1,
    to: int = 1,
    connections: Sequence[tuple[str, str, int]] = (("user", "connection-1", 123),),
) -> bytes:
    entries = "".join(
        f"*3\n+{token_reference}\n+{connection_id}\n:{timestamp}\n"
        for token_reference, connection_id, timestamp in connections
    )

    return (
        f"@PRES_LIST_RESPONSE\n+{segment_id}\n${len(request_id)}\n{request_id}\n"
        f";{total}\n;{per_page}\n;{current_page}\n;{from_}\n;{to}\n"
        f"*{len(connections)}\n{entries}"
    ).encode()


# end function presence_response_frame


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


# end function error_frame
