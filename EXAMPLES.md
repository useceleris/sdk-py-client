# useceleris-client: consumer examples

> Every snippet below is checked with `mypy --strict` against the public surface on every `nox` run ([drift test](tests/test_examples.py)), so the document cannot drift from the API. [examples/quickstart.py](examples/quickstart.py) runs against a real Celeris stack in the live suite.

Credentials are always minted by a trusted server. An application that is not trusted with the signing secret (a desktop app, a CLI, a device) fetches short-lived opaque credentials from its own authenticated endpoint. A trusted backend can sign its own with `useceleris-server`.

## The model in three sentences

A `Channel` is **one WebSocket client**: creating another `Channel`, even for the same reference, opens another socket. Every segment of that channel is multiplexed over that single connection, and connecting automatically makes you a member of the `"default"` segment. `Segment` objects are lightweight proxies over the channel connection: create as many as you like for the same segment, they all share the one socket and one interest count, and never own connections of their own.

## Setup

```python
from useceleris_client import CredentialRequest, Credentials, create_client


async def provide_credentials(request: CredentialRequest) -> Credentials:
    # Your authenticated endpoint signs least-privilege credentials for
    # exactly the requested channel. Called fresh for every attempt.
    body = await fetch_credentials(request)  # your HTTP call
    return Credentials(payload=body["payload"], signature=body["signature"])


client = create_client(credential_provider=provide_credentials)
```

The request carries `channel_reference`, `reason` (`"initial"` or `"reconnect"`) and, on reconnect, `disconnected_at` and `replay_lookback_ms`. Send what your endpoint needs; it decides the claims.

`create_client` performs no network work, and neither do `client.channel()`, `channel.segment(id)` and `channel.default_segment()`. Nothing connects until `connect()`. The endpoint is built in; set `base_url` only for a local or self-hosted stack (`ws://` also needs `allow_insecure_loopback=True`).

## Connect and observe the lifecycle

```python
from useceleris_client import ChannelError, ChannelState, RecoveryEvent

channel = client.channel("room-42")  # this object = one WebSocket client


def show_state(state: ChannelState) -> None:
    print("state:", state)


def recovered(event: RecoveryEvent) -> None:
    # Reconnected. Replay may have gaps; duplicates beyond the client's
    # bounded dedup window are possible.
    print("recovered after retry", event.retry_index)


def report(error: ChannelError) -> None:
    # Failures outside any call: socket errors, server error frames, and
    # frames this version could not decode. Only a socket error costs the
    # connection; an undecodable frame is dropped on its own.
    print(error.code, error)


# events() returns the channel's single ChannelEventHandler. Each on_* call
# registers a listener and returns the function that removes it.
events = channel.events()
stop_states = events.on_state_change(show_state)
events.on_recovery(recovered)
events.on_error(report)

await channel.connect()  # opens the socket; you are now in "default"
print(channel.state)  # "connected"

stop_states()  # removes that one listener; calling it again does nothing
```

A lost connection retries automatically with fresh credentials (10 attempts, full jitter). `failed` is not terminal: call `connect()` again. `close()` is terminal and idempotent, and ends every segment object with the channel:

```python
await channel.close()  # at most 5 s; the channel and its segments are done
```

Listeners are called synchronously, in registration order. A listener that raises is contained: the channel reports it through `on_error` and carries on. `async def` listeners are refused; to do asynchronous work, start a task from a plain listener:

```python
import asyncio

from useceleris_client import MessageMetadata

background: set["asyncio.Task[None]"] = set()


async def store(payload: bytes) -> None: ...  # your asynchronous work


def receive(payload: bytes, metadata: MessageMetadata) -> None:
    task = asyncio.get_running_loop().create_task(store(payload))
    background.add(task)
    task.add_done_callback(background.discard)


channel.default_segment().on_message(receive)
```

Tasks started this way are your queue, and bounding it is your choice; the SDK keeps none.

## Segments

```python
from useceleris_client import MessageMetadata, json_payload, read_json, text_payload

# Proxies over the SAME connection: no new sockets here.
lobby = channel.default_segment()  # "default": already a member
chat = channel.segment("chat")


def show(payload: bytes, metadata: MessageMetadata) -> None:
    # Payload first, then who sent it, its id and its timestamp.
    print(metadata.message_id, metadata.token_reference, read_json(payload))


stop_chat = chat.on_message(show)

# Join for messages (sends SUB: a real server-side operation, with its own
# replay cursor).
membership = chat.subscribe()

# Publish to this segment. Returning means the local socket ACCEPTED the
# bytes, not that the server received them. Publishing also joins the
# segment server-side, even without subscribe().
await chat.publish(json_payload({"hello": "world"}))

# The default segment needs no subscribe(): membership came with connect().
lobby.on_message(lambda payload, metadata: print("lobby:", metadata.message_id))
await lobby.publish(text_payload("hello lobby"))

# Tear down. Releasing the last message interest in a named segment sends
# UNSUB, unless a presence interest still holds it; cancelling presence never
# sends UNSUB. The default segment is never left: the client sends no SUB or
# UNSUB for it.
stop_chat()
membership.cancel()  # idempotent
```

Publishing can fail locally:

```python
from useceleris_client import CelerisConnectionError

try:
    await chat.publish(data)
except CelerisConnectionError as error:
    if error.code == "NotConnected":
        ...  # offline: nothing was queued
    elif error.code == "Backpressure":
        ...  # 64 publishes already waiting: slow down
    elif error.code == "DeliveryUnknown":
        ...  # the send failed mid-way: assume neither outcome
```

A permission-denied publish is different: it **returns normally**, and the server's error frame arrives later through `events().on_error`, with no link to the call; the protocol has no acks. Pass `message_id=` to publish with your own id; otherwise one is generated.

## Presence

```python
from useceleris_client import PresenceEvent, ServerNotice, read_text

chat = channel.segment("chat")

# Presence interest. Server-side this ALSO joins the segment for messages;
# cancelling presence does not leave it.
watching = chat.subscribe_presence()

# The default segment is where presence differs from messages: joining on
# connect grants message membership only, so PRES_SUB IS sent here.
lobby_presence = channel.default_segment().subscribe_presence()


def show_presence(event: PresenceEvent) -> None:
    action = "joined" if event.joined else "left"
    print(action, event.token_reference, event.connection_id, event.timestamp)


def show_notice(notice: ServerNotice) -> None:
    # Acks and refusals are untagged prose at the CHANNEL level; never parse
    # prose into events.
    print("notice:", read_text(notice.payload))


stop_presence = chat.on_presence(show_presence)
stop_notices = channel.events().on_notice(show_notice)

# A paginated snapshot: one query in flight per CHANNEL, 10 s deadline. A
# query the server refuses raises a ServerError (sub type "PRES_LIST") at
# once; a timeout never drops the connection.
page = await chat.presence_list(page=1, per_page=50)
print(f"{page.total} connected")

for connection in page.connections:
    print(connection.token_reference, connection.connection_id)

# Past the last page: raw metadata, from_ > to, and no entries.

watching.cancel()
lobby_presence.cancel()
stop_presence()
stop_notices()
```

Two things to know before building on presence events. They are **node-local**: the server fans notifications out only to watchers on the same node, while `presence_list()` aggregates across the cluster, so in a multi-node deployment a watcher can miss a joiner on another node, and reconciling events against a snapshot drifts. And suppression is per connection, not per token: your own other connections appear as joins and leaves.

## Multiple connections

```python
a = client.channel("room-42")
b = client.channel("room-42")  # a SECOND WebSocket client
```

Two channels are fully independent: separate sockets, memberships, presence entries and replay cursors, even under one token. Echo suppression is per connection, so `b` receives what `a` publishes.

## Error handling

Every error the SDK raises or reports is a `CelerisError` with a stable `code`; match on `code`, never on message text. Messages name the field and the rule that failed but never repeat your input, a credential or server text.

```python
from useceleris_client import (
    CelerisConnectionError,
    ChannelError,
    ConfigurationError,
    ServerError,
)

try:
    await channel.connect()
except CelerisConnectionError as error:
    if error.code == "Timeout":
        ...  # credentials and handshake missed the deadline (default 15 s)
    elif error.code == "Transport":
        ...  # network or handshake failure; the message says which
except ConfigurationError:
    ...  # the call is wrong (an invalid option or credential); fix it


# Errors the SERVER sends arrive as ServerError through events().on_error,
# every field as the server sent it. The channel stays connected. A failed
# presence query is the exception: it raises from presence_list() instead.
def watch(error: ChannelError) -> None:
    if not isinstance(error, ServerError):
        return

    if error.type == "PermissionDeniedError":
        ...  # the token lacks access to what it tried
    elif error.type == "MessageSizeLimitError":
        ...  # a publish exceeded your plan's size cap
    elif error.type == "RateLimitError":
        ...  # the client pauses and resends; slow down if it persists

    # e.g. PermissionDeniedError SUB chat Token does not have access ...
    print(error.type, error.sub_type, error.resource, error)


channel.events().on_error(watch)
```

The connection codes are `Timeout`, `Cancelled` (the channel was closed during the call), `Transport`, `NotConnected`, `Backpressure`, `OperationInProgress` and `DeliveryUnknown`. `ConfigurationError` has code `Configuration`, `ProtocolError` has `ProtocolError` (with `field` and `offset`), and `ServerError` has `Server`.

A publish larger than your plan allows **returns normally**; the server rejects it afterwards with a `MessageSizeLimitError` through `on_error`. Anything over 2 MiB can never succeed on any plan, so `publish()` raises a `ConfigurationError` at once.

## Encoding payloads

Payloads are `bytes`. Helpers cover the two common encodings, and one adapter wraps any other serializer.

```python
import time

from useceleris_client import json_payload, read_text, text_payload

await chat.publish(text_payload("hello"))
await chat.publish(json_payload({"body": "hello", "at": time.time()}))

chat.on_message(lambda payload, metadata: print(read_text(payload)))
```

`json_payload` writes compact JSON, as `JSON.stringify` does. `read_json` returns whatever the JSON holds; validate payloads from peers you do not control. Invalid UTF-8, invalid JSON, and values JSON cannot represent (NaN, infinities, sets, bytes, circular structures) raise a `ConfigurationError` that names the problem without repeating the payload.

For protobuf, MessagePack, CBOR or anything else, wrap your serializer once. The SDK bundles none, so you keep your own library and version:

```python
from dataclasses import asdict, dataclass

from useceleris_client import create_payload_codec, json_payload, read_json


@dataclass
class Chat:
    body: str


# Swap these two functions for your serializer's encode and decode, such as
# msgpack.packb / msgpack.unpackb or a protobuf message's
# SerializeToString / FromString.
chat_codec = create_payload_codec(
    encode=lambda value: json_payload(asdict(value)),
    decode=lambda payload: Chat(**read_json(payload)),
)

await chat.publish(chat_codec.encode_payload(Chat(body="hello")))
chat.on_message(lambda payload, metadata: print(chat_codec.read_payload(payload).body))
```

Errors raised by your own `encode` and `decode` propagate unchanged: they are yours, not the SDK's.

## Replay, gaps and duplicates

Reconnects request fresh credentials with a replay lookback covering the outage plus five seconds, and joining a segment replays per the token's replay mode. Replayed messages carry their original ids, and the client deduplicates within a bounded 1024-id window per channel; duplicates beyond it remain possible, which is why every `RecoveryEvent` declares `possible_gaps` and `possible_duplicates`. There is no durable cursor: replay is bounded local recovery, not history.

Restoration treats the default segment the way connecting does. Named segments are rejoined with a fresh SUB on the new socket; the default segment needs none, because the server joins it again on the new connection. A default-segment presence interest *is* re-sent, since presence was never part of that automatic join.

## Timestamps

Timestamps (`MessageMetadata.timestamp`, `PresenceEvent.timestamp`, `PresenceConnection.timestamp` and `ServerNotice.timestamp`) are `int` Unix milliseconds, the exact wire value:

```python
from datetime import datetime, timezone

sent_at = datetime.fromtimestamp(metadata.timestamp / 1000, tz=timezone.utc)
print(sent_at.isoformat(), page.total)
```

## What this API will never do

No offline queue, no automatic resend of publishes beyond rate-limit recovery, no server receipts or acks (the protocol has none), no durable history, no global ordering, no signing in the client. Presence joins and leaves *are* typed, because the wire frame carrying them is.
