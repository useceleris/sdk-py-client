import asyncio
import base64
import hashlib
import hmac
import itertools
import json
import os
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from useceleris_client import (
    Channel,
    ChannelError,
    Client,
    CredentialRequest,
    Credentials,
    MessageMetadata,
    PresenceEvent,
    Segment,
    ServerNotice,
    create_client,
)

# The SDK gives every publish its own id, which the server delivers as is
# (RESEND-01): 16 random bytes, hex-encoded.
GENERATED_MESSAGE_ID = re.compile("[0-9a-f]{32}")

Value = TypeVar("Value")

_channel_counter = itertools.count(1)


def sign_credentials(client_id: str, signing_secret: str, **claims: Any) -> Credentials:
    """Hand-written from the protocol document, independent of
    useceleris-server: client tests never import server code. Claims use the
    wire's own keys, in the wire's order."""
    wire: dict[str, Any] = {"timestamp": claims.pop("timestamp", now_ms())}

    for key in [
        "reference",
        "channel_references",
        "token_permission",
        "replay",
        "allow_echo",
    ]:
        if key in claims:
            wire[key] = claims.pop(key)

    assert not claims, f"Unknown claims: {sorted(claims)}"

    payload = base64.b64encode(
        json.dumps(wire, separators=(",", ":"), ensure_ascii=False).encode()
    ).decode()
    digest = hmac.new(
        signing_secret.encode(), payload.encode(), hashlib.sha512
    ).hexdigest()
    signature = base64.b64encode(f"{client_id}:{digest}".encode()).decode()

    return Credentials(payload=payload, signature=signature)


def now_ms() -> int:
    return int(time.time() * 1000)


def websocket_url() -> str:
    return os.environ["CELERIS_WS_URL"]


def client_id() -> str:
    return os.environ["CELERIS_CLIENT_ID"]


def signing_secret() -> str:
    return os.environ["CELERIS_SIGNING_SECRET"]


def unique_channel_reference(label: str) -> str:
    return f"pyqual-{label}-{now_ms()}-{next(_channel_counter)}"


def client_with(credentials: Callable[[], Credentials]) -> Client:
    async def provide(request: CredentialRequest) -> Credentials:
        return credentials()

    return create_client(
        base_url=websocket_url(),
        allow_insecure_loopback=True,
        credential_provider=provide,
    )


def qualification_client(**claims: Any) -> Client:
    return client_with(
        lambda: sign_credentials(client_id(), signing_secret(), **dict(claims))
    )


# Positional-only, so a token's own "reference" claim can be passed too.
async def connected_channel(channel_reference: str, /, **claims: Any) -> Channel:
    channel = qualification_client(**claims).channel(channel_reference)
    await channel.connect()

    return channel


async def wait_for(
    register: Callable[[Callable[[Value], None]], Callable[[], None]],
    predicate: Callable[[Value], bool],
    description: str,
    timeout_s: float = 15,
) -> Value:
    arrived: asyncio.Future[Value] = asyncio.get_running_loop().create_future()

    def deliver(value: Value) -> None:
        if not arrived.done() and predicate(value):
            arrived.set_result(value)

    dispose = register(deliver)

    try:
        return await asyncio.wait_for(arrived, timeout_s)
    except asyncio.TimeoutError:
        raise AssertionError(f"Timed out waiting for {description}.") from None
    finally:
        dispose()


@dataclass(frozen=True)
class DeliveredMessage:
    payload: bytes
    metadata: MessageMetadata


def collect(channel: Channel, segment_id: str) -> list[DeliveredMessage]:
    received: list[DeliveredMessage] = []
    channel.segment(segment_id).on_message(
        lambda payload, metadata: received.append(DeliveredMessage(payload, metadata))
    )
    channel.segment(segment_id).subscribe()

    return received


async def next_message(
    segment: Segment,
    predicate: Callable[[DeliveredMessage], bool],
    description: str = "a message delivery",
    timeout_s: float = 15,
) -> DeliveredMessage:
    def register(deliver: Callable[[DeliveredMessage], None]) -> Callable[[], None]:
        return segment.on_message(
            lambda payload, metadata: deliver(DeliveredMessage(payload, metadata))
        )

    return await wait_for(register, predicate, description, timeout_s)


async def next_notice(
    channel: Channel,
    predicate: Callable[[ServerNotice], bool],
    description: str = "a server notice",
    timeout_s: float = 15,
) -> ServerNotice:
    return await wait_for(channel.events().on_notice, predicate, description, timeout_s)


async def next_presence(
    segment: Segment,
    predicate: Callable[[PresenceEvent], bool],
    description: str = "a presence notification",
    timeout_s: float = 15,
) -> PresenceEvent:
    return await wait_for(segment.on_presence, predicate, description, timeout_s)


async def next_error(
    channel: Channel,
    predicate: Callable[[ChannelError], bool],
    description: str = "a channel error",
    timeout_s: float = 15,
) -> ChannelError:
    return await wait_for(channel.events().on_error, predicate, description, timeout_s)
