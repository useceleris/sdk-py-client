import asyncio
import base64
import contextlib
import hashlib
import hmac
import itertools
import json
import os
import re
import time
from collections.abc import Callable, Coroutine
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

    return sign_raw_payload(
        client_id,
        signing_secret,
        json.dumps(wire, separators=(",", ":"), ensure_ascii=False),
    )


# end function sign_credentials


def sign_raw_payload(
    client_id: str, signing_secret: str, payload_text: str
) -> Credentials:
    """Signs any payload text as it is, for claims the keyword form cannot
    express (malformed JSON, missing or wrongly typed fields)."""
    payload = base64.b64encode(payload_text.encode()).decode()
    digest = hmac.new(
        signing_secret.encode(), payload.encode(), hashlib.sha512
    ).hexdigest()
    signature = base64.b64encode(f"{client_id}:{digest}".encode()).decode()

    return Credentials(payload=payload, signature=signature)


# end function sign_raw_payload


def now_ms() -> int:
    return int(time.time() * 1000)


# end function now_ms


def websocket_url() -> str:
    return os.environ["CELERIS_WS_URL"]


# end function websocket_url


# Optional: a gateway that routes to a different server node, used for the
# second connection of the cross-node tests. Without it, those tests skip,
# because two connections through one gateway can share a node.
def peer_websocket_url() -> str:
    return os.environ["CELERIS_WS_URL_PEER"]


# end function peer_websocket_url


def client_id() -> str:
    return os.environ["CELERIS_CLIENT_ID"]


# end function client_id


def signing_secret() -> str:
    return os.environ["CELERIS_SIGNING_SECRET"]


# end function signing_secret


def unique_channel_reference(label: str) -> str:
    return f"pyqual-{label}-{now_ms()}-{next(_channel_counter)}"


# end function unique_channel_reference


def client_with(
    credentials: Callable[[], Credentials], base_url: str | None = None
) -> Client:
    async def provide(request: CredentialRequest) -> Credentials:
        return credentials()

    # end function provide

    return create_client(
        base_url=base_url or websocket_url(),
        allow_insecure_loopback=True,
        credential_provider=provide,
    )


# end function client_with


def qualification_client(base_url: str | None = None, **claims: Any) -> Client:
    return client_with(
        lambda: sign_credentials(client_id(), signing_secret(), **dict(claims)),
        base_url,
    )


# end function qualification_client


# Positional-only, so a token's own "reference" claim can be passed too. A
# channel added to opened is closed by the fixture even when the test fails.
async def connected_channel(
    channel_reference: str,
    /,
    base_url: str | None = None,
    opened: list[Channel] | None = None,
    **claims: Any,
) -> Channel:
    channel = qualification_client(base_url, **claims).channel(channel_reference)

    if opened is not None:
        opened.append(channel)

    await channel.connect()

    return channel


# end function connected_channel


async def started(waiter: Coroutine[Any, Any, Value]) -> asyncio.Future[Value]:
    """Starts a wait so its listener is registered; await the result after the
    action that causes the event."""
    pending = asyncio.ensure_future(waiter)
    await asyncio.sleep(0)

    return pending


# end function started


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

    # end function deliver

    dispose = register(deliver)

    try:
        return await asyncio.wait_for(arrived, timeout_s)
    except asyncio.TimeoutError:
        raise AssertionError(f"Timed out waiting for {description}.") from None
    finally:
        dispose()


# end function wait_for


@dataclass(frozen=True)
class DeliveredMessage:
    payload: bytes
    metadata: MessageMetadata


# end class DeliveredMessage


def collect(channel: Channel, segment_id: str) -> list[DeliveredMessage]:
    received: list[DeliveredMessage] = []
    channel.segment(segment_id).on_message(
        lambda payload, metadata: received.append(DeliveredMessage(payload, metadata))
    )
    channel.segment(segment_id).subscribe()

    return received


# end function collect


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

    # end function register

    return await wait_for(register, predicate, description, timeout_s)


# end function next_message


async def next_notice(
    channel: Channel,
    predicate: Callable[[ServerNotice], bool],
    description: str = "a server notice",
    timeout_s: float = 15,
) -> ServerNotice:
    return await wait_for(channel.events().on_notice, predicate, description, timeout_s)


# end function next_notice


async def next_presence(
    segment: Segment,
    predicate: Callable[[PresenceEvent], bool],
    description: str = "a presence notification",
    timeout_s: float = 15,
) -> PresenceEvent:
    return await wait_for(segment.on_presence, predicate, description, timeout_s)


# end function next_presence


async def next_error(
    channel: Channel,
    predicate: Callable[[ChannelError], bool],
    description: str = "a channel error",
    timeout_s: float = 15,
) -> ChannelError:
    return await wait_for(channel.events().on_error, predicate, description, timeout_s)


# end function next_error


class DroppingProxy:
    """Forwards TCP connections to the realtime service, and can break them:

    - refusing: new connections close at once
    - blackhole: new connections stay open but carry no bytes
    - blackhole_open_links(): open connections stay open but carry no bytes
    - stall_upstream(): the proxy stops reading what clients send, so their
      socket buffers fill
    - drop_all(): every open connection closes, as a network outage would
    """

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.url = ""
        self.refusing = False
        self.blackhole = False
        self._sockets: list[asyncio.StreamWriter] = []
        self._silenced: set[asyncio.StreamWriter] = set()
        self._upstream_flowing = asyncio.Event()
        self._upstream_flowing.set()

    # end method __init__

    async def link(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        if self.refusing:
            writer.transport.abort()
            return

        self._sockets.append(writer)

        if self.blackhole:
            with contextlib.suppress(ConnectionError):
                while await reader.read(65536):
                    pass

            writer.transport.abort()
            return

        upstream_reader, upstream_writer = await asyncio.open_connection(
            self.host, self.port
        )
        self._sockets.append(upstream_writer)
        await asyncio.gather(
            self._pipe(reader, upstream_writer, from_client=True),
            self._pipe(upstream_reader, writer, from_client=False),
            return_exceptions=True,
        )

    # end method link

    async def _pipe(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        from_client: bool,
    ) -> None:
        with contextlib.suppress(ConnectionError):
            while True:
                if from_client:
                    await self._upstream_flowing.wait()

                data = await reader.read(65536)

                if not data:
                    break

                if writer in self._silenced:
                    continue

                writer.write(data)
                await writer.drain()

        writer.transport.abort()

    # end method _pipe

    def blackhole_open_links(self) -> None:
        self._silenced.update(self._sockets)

    # end method blackhole_open_links

    def stall_upstream(self, stalled: bool) -> None:
        if stalled:
            self._upstream_flowing.clear()
        else:
            self._upstream_flowing.set()

    # end method stall_upstream

    def drop_all(self) -> None:
        for socket in self._sockets:
            socket.transport.abort()

        self._sockets.clear()
        self._silenced.clear()

    # end method drop_all


# end class DroppingProxy
