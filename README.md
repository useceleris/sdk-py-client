# useceleris-client

[![PyPI](https://img.shields.io/pypi/v/useceleris-client)](https://pypi.org/project/useceleris-client/)
[![Python versions](https://img.shields.io/pypi/pyversions/useceleris-client)](https://pypi.org/project/useceleris-client/)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](https://github.com/useceleris/sdk-py-client/blob/main/LICENSE)

Realtime client for Celeris channels, for Python's asyncio: connection lifecycle with automatic recovery, segment messaging, and presence.

## The model in three sentences

A `Channel` is one WebSocket client: creating another `Channel`, even for the same reference, opens another socket. Every segment of that channel is multiplexed over that single connection, and connecting automatically makes you a member of the `"default"` segment. `Segment` objects are lightweight proxies over the channel connection: create as many as you like for the same segment, they share the socket and one interest count.

## Install

```sh
pip install --pre useceleris-client
```

Python 3.10 to 3.14. The package ships type information (`py.typed`) and depends on `pydantic`, `websockets` and `typing-extensions`. It contains no signing code: a trusted server signs credentials with [`useceleris-server`](https://pypi.org/project/useceleris-server/).

## Quickstart

```python
import asyncio
import json
import urllib.request

from useceleris_client import (
    CredentialRequest,
    Credentials,
    MessageMetadata,
    create_client,
    read_text,
    text_payload,
)


def request_credentials(request: CredentialRequest) -> dict[str, str]:
    # YOUR authenticated endpoint, which signs with useceleris-server.
    http_request = urllib.request.Request(
        "https://your-app.example/api/realtime-credentials",
        data=json.dumps(
            {
                "channel_reference": request.channel_reference,
                "replay_lookback_ms": request.replay_lookback_ms,
            }
        ).encode(),
        headers={"content-type": "application/json", "authorization": "Bearer ..."},
        method="POST",
    )

    with urllib.request.urlopen(http_request, timeout=10) as response:
        body: dict[str, str] = json.load(response)

    return body


async def provide_credentials(request: CredentialRequest) -> Credentials:
    body = await asyncio.to_thread(request_credentials, request)
    return Credentials(payload=body["payload"], signature=body["signature"])


def show(payload: bytes, metadata: MessageMetadata) -> None:
    print(metadata.token_reference, read_text(payload))


async def main() -> None:
    client = create_client(credential_provider=provide_credentials)
    channel = client.channel("room-42")
    chat = channel.segment("chat")
    chat.on_message(show)
    chat.subscribe()

    await channel.connect()
    await chat.publish(text_payload("hello"))
    await asyncio.sleep(30)  # receive for a while
    await channel.close()


asyncio.run(main())
```

Replace the URL and authorization with your own endpoint's. A connection does not receive its own publishes unless its credentials allow echo, so run a second copy to watch messages arrive.

## Credentials

The credential provider is an `async` function from a `CredentialRequest` to `Credentials`. The client calls it for every connection attempt, the first and each reconnect, so credentials are always fresh; never cache them. It runs under the connect deadline, and cancelling the attempt cancels it.

| `CredentialRequest` field | Value                                                                                                  |
| ------------------------- | ------------------------------------------------------------------------------------------------------ |
| `channel_reference`       | The channel being connected                                                                            |
| `reason`                  | `"initial"` or `"reconnect"`                                                                           |
| `disconnected_at`         | On reconnect, when the connection was lost, in Unix milliseconds; otherwise `None`                     |
| `replay_lookback_ms`      | On reconnect, how far back the new connection should replay: the outage plus 5 s; otherwise `None`     |

Send your endpoint what it needs; it alone decides the claims, including whether to honour `replay_lookback_ms`. Return its `payload` and `signature` unchanged: they are opaque, and `repr()` never shows them. A provider that raises fails the attempt with a `Transport` error that never repeats your exception; one that returns anything but `Credentials` holding two non-empty, well-formed strings fails it with a `ConfigurationError`.

Never ship a signing secret in an application. A desktop app, a CLI or a device fetches credentials from its own authenticated endpoint; only a trusted backend signs its own.

## Channels and connection

`create_client()`, `client.channel()` and `channel.segment()` do no network work. Each `client.channel(reference)` call returns a new channel, and a new socket once connected. A reference is 1 to 255 ASCII letters, digits, hyphens or underscores.

| State               | Meaning                                                  |
| ------------------- | -------------------------------------------------------- |
| `idle`              | Created; call `connect()`                                |
| `connecting`        | The first connection attempt is under way                |
| `connected`         | Open: publish, subscribe and query                       |
| `reconnecting`      | The connection dropped; retrying automatically           |
| `failed`            | Connecting or recovery gave up; call `connect()` again   |
| `closing`, `closed` | `close()` was called; the channel is done                |

```python
from useceleris_client import CelerisConnectionError, ChannelState


def show_state(state: ChannelState) -> None:
    print("state:", state)


stop_states = channel.events().on_state_change(show_state)

try:
    await channel.connect()  # returns once the socket is open; "default" is joined
except CelerisConnectionError as error:
    print("connect failed:", error.code)  # the channel is now "failed"

print(channel.state)
await channel.close()  # terminal; waits at most 5 s
stop_states()  # removes that one listener
```

`connect()` raises `OperationInProgress` while the channel is connecting, connected or reconnecting, and `NotConnected` once it is closed. A failed first connect raises to its caller, sets `failed`, and is not retried. `close()` is idempotent and terminal: it stops recovery, fails waiting publishes and a pending presence query with `Cancelled`, and ends every segment object with the channel. Get a new channel from the client to connect again.

## Segments, subscribing and receiving

```python
from useceleris_client import MessageMetadata, read_text


def show(payload: bytes, metadata: MessageMetadata) -> None:
    # The payload first, then who sent it, where, its id and its time.
    print(metadata.token_reference, metadata.segment_id, read_text(payload))
    print(metadata.message_id, metadata.timestamp)


chat = channel.segment("chat")  # a proxy over the channel's socket
stop_chat = chat.on_message(show)  # a local listener; joins nothing by itself
membership = chat.subscribe()  # joins "chat" on the server

lobby = channel.default_segment()  # "default", joined by connect()
stop_lobby = lobby.on_message(show)  # no subscribe() needed

stop_chat()
membership.cancel()  # idempotent
stop_lobby()
```

Interests are counted per segment across every `Segment` object: the first `subscribe()` joins, and the last `cancel()` leaves. The default segment is never left. Subscriptions taken before `connect()` are sent when the channel connects, and every subscription is restored after a reconnect. Read access is checked when the server joins the segment, so a write-only member receives nothing. `metadata.timestamp` is an `int` in Unix milliseconds. A segment id is any non-empty text without CR or LF.

The segment membership on the server controls which messages you receive. A listener does not control it:

- When you hold a `subscribe()` handle, you receive the messages of the segment. A listener alone receives no messages.
- When the connection publishes to a segment, the server also joins the connection to that segment. With read and write access, the connection then receives messages. With write-only access, the connection joins, but it does not receive messages. With read-only access, the server does not accept the publish. To receive messages, call `subscribe()`.
- When you cancel the last handle, the SDK tells the server to unsubscribe. This also stops a join from a publish. A presence subscription does not join or keep a segment.
- The connection always receives the messages of `"default"`.
- After a reconnect, the SDK subscribes again only to the segments that you hold a subscription for. A join from a publish does not continue after a reconnect. To continue to receive the messages of a segment, call `subscribe()`.
- A listener and a subscription are different items. When you remove a listener, your subscriptions do not change. When you cancel a subscription, your listeners do not change.

To receive all messages from all segments, add a channel listener. The segment listeners get each message first, then the channel listeners get it:

```python
from useceleris_client import MessageMetadata, read_text


def show_any(payload: bytes, metadata: MessageMetadata) -> None:
    print(metadata.segment_id, read_text(payload))


remove_channel_listener = channel.events().on_message(show_any)
```

`on_message()` returns a function that removes only this channel listener. When you call `remove_channel_listener()`:

- The other channel listeners continue to receive messages.
- The segment listeners continue to receive messages.
- Your subscriptions do not change, and the SDK does not send a message to the server.

When you call the function again, it has no effect.

## Publishing

```python
import uuid

from useceleris_client import CelerisConnectionError, text_payload

try:
    await chat.publish(text_payload("hello"))
    await chat.publish(data, message_id=str(uuid.uuid4()))  # your own id
except CelerisConnectionError as error:
    if error.code == "NotConnected":
        ...  # not connected, or the connection dropped first: nothing is queued
    elif error.code == "Backpressure":
        ...  # publish_queue_size publishes (64 by default) are already waiting: slow down
    elif error.code == "DeliveryUnknown":
        ...  # the socket failed mid-send: it may or may not have gone out
```

Returning means the local socket accepted the bytes, nothing more: there is no receipt. A server denial (`PermissionDeniedError`) or plan size rejection (`MessageSizeLimitError`) arrives afterwards through `events().on_error`. Publishing joins the segment server-side, without granting read permission. Without `message_id`, a random id is generated. The payload must be `bytes`, and may be empty; a command over 2 MiB encoded, or an invalid id, raises `ConfigurationError` before anything is sent. Cancelling the task awaiting `publish()` withdraws a publish that has not gone out.

## Payloads

Payloads are `bytes`. Helpers cover text and JSON:

```python
from useceleris_client import json_payload, read_json, read_text, text_payload

greeting = text_payload("hello")  # UTF-8 bytes
document = json_payload({"body": "hello", "likes": 3})  # compact JSON bytes

print(read_text(greeting), read_json(document)["likes"])
await chat.publish(document)
```

`json_payload` writes compact JSON, as `JSON.stringify` does. `read_json` returns whatever the JSON holds, typed `Any`: validate what peers send. Invalid UTF-8, invalid JSON, unpaired surrogates and values JSON cannot represent (NaN, infinities, sets, bytes, circular structures) raise `ConfigurationError` without repeating the payload.

For protobuf, MessagePack, CBOR or anything else, wrap your serializer once; the SDK bundles none:

```python
from dataclasses import asdict, dataclass

from useceleris_client import create_payload_codec, json_payload, read_json


@dataclass
class Chat:
    body: str


# Swap in your serializer, such as msgpack.packb / msgpack.unpackb or a
# protobuf message's SerializeToString / FromString.
chat_codec = create_payload_codec(
    encode=lambda value: json_payload(asdict(value)),
    decode=lambda payload: Chat(**read_json(payload)),
)

await chat.publish(chat_codec.encode_payload(Chat(body="hello")))
chat.on_message(lambda payload, metadata: print(chat_codec.read_payload(payload).body))
```

`encode` must return `bytes`. Errors raised by your `encode` and `decode` propagate unchanged.

## Presence

```python
from useceleris_client import PresenceEvent


def show_presence(event: PresenceEvent) -> None:
    action = "joined" if event.joined else "left"
    print(action, event.token_reference, event.connection_id, event.timestamp)


stop_presence = chat.on_presence(show_presence)
watching = chat.subscribe_presence()  # watches joins and leaves; joins nothing

page = await chat.presence_list(page=1, per_page=50)
print(page.total, "connections")

for connection in page.connections:
    print(connection.token_reference, connection.connection_id)

watching.cancel()  # stops the events
stop_presence()
```

`subscribe_presence()` watches joins and leaves without joining: it delivers no messages, and the watcher is not itself announced or listed. The default segment needs it too, since connecting joins it for messages only. Presence describes connections, not people: a user with three connections appears three times, and your own other connections appear as joins and leaves. Events and `presence_list()` both cover every server node serving the channel.

`presence_list()` needs a connection and read permission, not a presence subscription. `page` is 1 to 2147483647 and `per_page` 1 to 100, validated rather than clamped. One query may be in flight per channel; a second raises `OperationInProgress`. A query times out after `presence_query_timeout_ms` (10 s) without dropping the connection, and a refusal raises a `ServerError` with `sub_type` `"PRES_LIST"`. Pages are not an atomic snapshot. To read every page:

```python
from useceleris_client import PresenceConnection, Segment


async def list_connections(segment: Segment) -> list[PresenceConnection]:
    connections: list[PresenceConnection] = []
    page_number = 1

    while True:
        page = await segment.presence_list(page=page_number, per_page=100)
        connections.extend(page.connections)

        if not page.connections or page.to >= page.total:
            return connections

        page_number += 1


everyone = await list_connections(chat)
```

Past the last page, `connections` is empty and `from_ > to`; that is not an error.

## Events and errors

`channel.events()` registers channel-level listeners. Each `on_*` call returns the function that removes it.

| Listener          | Receives                                                                                                            |
| ----------------- | ------------------------------------------------------------------------------------------------------------------- |
| `on_state_change` | Every `ChannelState` change                                                                                         |
| `on_recovery`     | A `RecoveryEvent` after an automatic reconnect                                                                      |
| `on_notice`       | A `ServerNotice`: untagged server prose, such as acknowledgements; never parse it                                   |
| `on_error`        | Failures outside any call: server errors, undecodable frames, a listener that raised, the error that ended recovery |
| `on_message`      | Every message delivery from any segment, after that segment's own listeners                                         |

A call you await raises its own failure rather than reporting it through `on_error`; that includes a presence query the server refuses. Every error is a `CelerisError` with a stable `code`: match on `code`, and on `type` for server errors, never on message text. Messages name what failed and the rule it broke, but never repeat your input, a credential or server text.

| `code`                | Class                    | When                                                                                                                            |
| --------------------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| `Configuration`       | `ConfigurationError`     | An invalid option, argument, payload or listener, or invalid credentials from the provider; fix the call                        |
| `Timeout`             | `CelerisConnectionError` | The connect deadline (credentials and handshake together) or a presence query deadline elapsed                                  |
| `Cancelled`           | `CelerisConnectionError` | The channel was closed during a connect, a waiting publish or a presence query                                                  |
| `Transport`           | `CelerisConnectionError` | A handshake or socket failure, a credential provider that raised, or a listener that raised                                     |
| `NotConnected`        | `CelerisConnectionError` | Publishing or querying while not connected, a waiting publish lost with the connection, or a closed channel                     |
| `Backpressure`        | `CelerisConnectionError` | `publish_queue_size` publishes, 64 by default, are already waiting, or a presence query found the writer full or sending paused |
| `OperationInProgress` | `CelerisConnectionError` | A second `connect()` or `presence_list()` while one is running                                                                  |
| `DeliveryUnknown`     | `CelerisConnectionError` | The socket failed after taking the command: it may or may not have been sent                                                    |
| `ProtocolError`       | `ProtocolError`          | One received frame could not be decoded; it is dropped and the connection stays up. Carries `field` and `offset`                |
| `Server`              | `ServerError`            | An error the server sent                                                                                                        |

A refused handshake is reported as `Transport`: its HTTP status is not available. A `ServerError` carries the server's `type` (`PermissionDeniedError`, `RateLimitError`, `MessageSizeLimitError`, `ParserError`, `SendError`, `InternalError`, or one a newer server adds), `sub_type` (the command it answers, such as `"PUB"` or `"SUB"`, or `None`), its message, and `resource` (what that command names, such as the segment id: a `str`, an `int`, a tuple of these, or `None`). The connection stays up.

```python
from useceleris_client import ChannelError, ServerError


def handle(error: ChannelError) -> None:
    if not isinstance(error, ServerError):
        print(error.code, error)
        return

    if error.type == "PermissionDeniedError":
        ...  # the token lacks access to what error.sub_type tried on error.resource
    elif error.type == "MessageSizeLimitError":
        ...  # a publish exceeded your plan's payload cap
    elif error.type == "RateLimitError":
        ...  # the client pauses and resends; publish less often if it persists
    else:
        ...  # keep a default branch for types a newer server adds

    print(error.type, error.sub_type, error.resource, error)


stop_errors = channel.events().on_error(handle)
```

## Reconnection and recovery

When an open connection drops, the channel moves to `reconnecting` and retries with fresh credentials: up to `maximum_reconnect_attempts` failed attempts (10 by default, 1 to 100), each after a full-jitter delay below 0.5 s × 2ⁿ, capped at 30 s. Each attempt, credentials and handshake, runs under `reconnect_timeout_ms`, which defaults to `connect_timeout_ms`. The budget resets when a connection that lasted at least 60 s drops. Network, handshake and provider failures (`Transport`, `Timeout`) use up the budget; any other failure, such as invalid credentials, ends recovery at once. Either way the final error reaches `on_error` and the channel moves to `failed`.

```python
from useceleris_client import RecoveryEvent


def recovered(event: RecoveryEvent) -> None:
    # possible_gaps and possible_duplicates are always True.
    print("recovered after", event.retry_index, "failed attempts")
    # Reload your application's authoritative state here.


stop_recovery = channel.events().on_recovery(recovered)
```

On success the state becomes `connected`, then `on_recovery` fires. Subscriptions and presence interests are restored. Publishes still waiting when the connection dropped failed with `NotConnected` and are never resent; a pending presence query failed with `Transport`. The reconnect asks for a replay lookback covering the outage plus 5 s, and the `replay` claim your endpoint signs decides what is replayed. Replayed messages keep their original ids, and the client drops ids it has already seen within a 1024-id window per channel; gaps and duplicates beyond it remain possible. The recovery event does not signal that replay has finished.

## Delivery semantics, honestly

- `publish()` returns when the local socket accepted the bytes. There is **no server receipt or ack** anywhere in the protocol; server responses are untagged prose notices, and an error arrives later with no link to the call.
- Every publish carries a message id, yours or a generated one. Receivers drop repeated ids within a bounded window, which is not exactly-once: keep your own idempotency for business operations.
- Lost connections retry automatically. Recovery restores your subscriptions and reports **possible gaps and duplicates**.
- A `RateLimitError` never names the command it dropped, so the client pauses and resends what it sent in the last two seconds: subscriptions first, as their current state, then up to the last 64 publishes, each at most once and with its original id. After eight limits in a row it treats the limit as a used-up quota: it stops resending, and re-sends the subscriptions it dropped on a slow probe until commands go two seconds without a limit. Resends count toward usage.
- Subscriptions and publishes wait for room when the writer is full instead of failing. A subscription change goes out ahead of publishes, but never ahead of a publish to its own segment that was queued before it.
- No offline queue, no durable history, no global ordering.

## Limits and defaults

| What               | Value                                                                                                                  |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------- |
| Outbound command   | 2 MiB encoded, rejected before any write                                                                               |
| Plan payload cap   | 64 KiB free, 128 KiB standard, 512 KiB pro, 1024 KiB prime; enforced by the server                                     |
| Writer bounds      | 64 pending commands / 2 MiB of unsent data; `publish_queue_size` queued publishes, 64 by default, plus resends         |
| Rate-limit pause   | 1 s plus full jitter growing with consecutive limits, at most 31 s                                                     |
| Resends            | last 2 s of commands, at most 64 publishes, each once                                                                  |
| Quota probe        | after 8 limits in a row: dropped subscriptions retried after 1 min, doubling to 1 h                                    |
| Connect deadline   | `connect_timeout_ms`, default 15 s, covering credentials and the handshake                                             |
| Reconnect deadline | `reconnect_timeout_ms` per attempt, default `connect_timeout_ms`, covering credentials and the handshake               |
| Presence query     | one in flight per channel; `presence_query_timeout_ms`, default 10 s; `per_page` at most 100                           |
| Timeout options    | each 1 ms to 15 minutes (900000 ms); larger values raise `ConfigurationError`                                          |
| Reconnect          | `maximum_reconnect_attempts` failed attempts, default 10, 1 to 100; full jitter up to 30 s, reset after 60 s connected |
| Replay lookback    | the outage plus 5 s, requested on reconnect                                                                            |
| Dedup window       | `deduplication_window_size` message ids per channel, default 1024                                                      |
| Close              | at most 5 s                                                                                                            |
| Channel reference  | 1 to 255 ASCII letters, digits, hyphens or underscores                                                                 |

Received messages are never size-checked: they are already in memory when they arrive. A publish over your plan's cap returns locally and is rejected afterwards with a `MessageSizeLimitError`, and it still counts toward your usage.

## asyncio, threads and cancellation

Everything runs on the event loop that called `connect()`; use a channel from that loop's thread only. Cancellation is asyncio's own: cancelling the task awaiting `connect()` abandons the attempt and leaves the channel `failed`, cancelling an awaiting `publish()` withdraws a publish that has not gone out, and cancelling `presence_list()` frees the query slot. `CancelledError` propagates unchanged, so `asyncio.wait_for()` works as usual.

Listeners are plain functions called synchronously, in registration order, as events arrive: a slow listener slows delivery rather than growing a queue. A listener that raises is contained and reported through `on_error`. `async def` listeners are refused with `ConfigurationError`; start a task from a plain listener instead, and keep a reference to it:

```python
import asyncio

from useceleris_client import MessageMetadata

background: set["asyncio.Task[None]"] = set()


async def store(payload: bytes) -> None: ...  # your asynchronous work


def receive(payload: bytes, metadata: MessageMetadata) -> None:
    task = asyncio.get_running_loop().create_task(store(payload))
    background.add(task)
    task.add_done_callback(background.discard)


chat.on_message(receive)
```

Those tasks are your queue, and bounding it is your choice; catch errors inside them, since the SDK contains failures of the listener only. The client silences the `websockets` library's logging for its connections, because the handshake URL it would log carries the credentials.

## Local development and self-hosting

The production endpoint is built in. For a local or self-hosted stack, set `base_url` to its realtime socket endpoint, not your credential endpoint:

```python
import os

from useceleris_client import CredentialRequest, Credentials, create_client


async def provide_credentials(request: CredentialRequest) -> Credentials:
    body = await fetch_credentials(request)  # your local credential endpoint
    return Credentials(payload=body["payload"], signature=body["signature"])


local = create_client(
    credential_provider=provide_credentials,
    base_url=os.environ["CELERIS_WS_URL"],  # such as ws://localhost:<port>
    allow_insecure_loopback=True,  # permits ws:// to a loopback host only
)
```

`base_url` must be an absolute `wss://` URL with no username, password, query string or fragment. `ws://` is accepted only for a loopback host (`localhost` or a loopback IP address) with `allow_insecure_loopback=True`; never set it in production.

## Further documentation

- Python SDK guides: <https://useceleris.com/docs/sdks/python>
- API reference: <https://useceleris.com/docs/api-reference/python-client>
- More walkthroughs: [EXAMPLES.md](https://github.com/useceleris/sdk-py-client/blob/main/EXAMPLES.md), and [examples/quickstart.py](https://github.com/useceleris/sdk-py-client/blob/main/examples/quickstart.py), which runs against a real Celeris stack in the live suite.

## Development

```sh
uv sync
uv run nox
```

`uv sync` creates `.venv` with the package and its development tools at the versions in `uv.lock`. `uv run nox` runs the whole check: lint, `mypy --strict`, the unit suites on every supported Python (which also type-check every Python snippet in this README and in EXAMPLES.md), and the package check (build, install the wheel alone, verify its contents and that it carries no signing facility). `uv run nox -s live` runs the acceptance suites against a real Celeris stack, reading `CELERIS_WS_URL`, `CELERIS_CLIENT_ID` and `CELERIS_SIGNING_SECRET` from a gitignored `.env` or the environment.

Give a runtime dependency a range (`uv add "httpx>=0.28,<1"`) and pin a development tool exactly (`uv add --group dev "coverage==7.10.0"`). `make release` runs the check, builds, and uploads to PyPI from a clean git tree; `make release-test` rehearses on TestPyPI, and `make smoke` installs the published version in a fresh environment.

Read [CONVENTIONS.md](https://github.com/useceleris/sdk-py-client/blob/main/CONVENTIONS.md) before contributing, and [SECURITY.md](https://github.com/useceleris/sdk-py-client/blob/main/SECURITY.md) before reporting a vulnerability.

## License

[Apache 2.0](https://github.com/useceleris/sdk-py-client/blob/main/LICENSE).
