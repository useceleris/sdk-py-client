# useceleris-client

Realtime client for Celeris channels, for Python's asyncio: connection lifecycle with automatic recovery, segment messaging, and presence.

**The model in three sentences.** A `Channel` is one WebSocket client: creating another `Channel`, even for the same reference, opens another socket. Every segment of that channel is multiplexed over that single connection, and connecting automatically makes you a member of the `"default"` segment. `Segment` objects are lightweight proxies over the channel connection: create as many as you like, they share the socket and one interest count.

## Install

```sh
pip install --pre useceleris-client
```

Python 3.10 to 3.14.

## Quickstart

```python
import asyncio

from useceleris_client import (
    CredentialRequest,
    Credentials,
    MessageMetadata,
    create_client,
    read_text,
    text_payload,
)


async def provide_credentials(request: CredentialRequest) -> Credentials:
    # A trusted server signs short-lived credentials; fetch them from YOUR
    # authenticated endpoint. Never ship a signing secret in client code.
    body = await fetch_credentials(request)
    return Credentials(payload=body["payload"], signature=body["signature"])


def show(payload: bytes, metadata: MessageMetadata) -> None:
    print(metadata.message_id, read_text(payload))


async def main() -> None:
    client = create_client(credential_provider=provide_credentials)
    channel = client.channel("room-42")
    chat = channel.segment("chat")
    chat.on_message(show)
    chat.subscribe()

    await channel.connect()
    await chat.publish(text_payload("hello"))
    await asyncio.sleep(1)
    await channel.close()


asyncio.run(main())
```

`fetch_credentials` stands for your own HTTP call. The endpoint is built in; pass `base_url` only for a local or self-hosted stack. Payloads are opaque bytes: `text_payload`/`json_payload` and `read_text`/`read_json` cover the common cases, and `create_payload_codec` wraps any other serializer (protobuf, MessagePack, CBOR) without the package depending on one.

Walkthroughs of lifecycle events, presence, error handling and payloads are in [EXAMPLES.md](EXAMPLES.md); [examples/quickstart.py](examples/quickstart.py) runs against a real Celeris stack in the live suite.

## Delivery semantics, honestly

- `publish()` returns when the local socket accepted the bytes. There is **no server receipt or ack** anywhere in the protocol; server responses are untagged prose notices. Every publish carries a message id, yours or a generated one.
- Lost connections retry automatically (10 attempts, full jitter, fresh credentials, replay lookback). Recovery restores your subscriptions and reports **possible gaps and duplicates**; a bounded 1024-id window deduplicates replayed messages, and duplicates beyond it remain possible.
- A `RateLimitError` never names the command it dropped, so the client pauses and resends what it sent in the last two seconds: subscriptions first, as their current state, then up to the last 64 publishes, each at most once and with its original id so receivers drop a copy that had already arrived. After eight limits in a row the client treats the limit as a used-up quota: it stops resending, and re-sends the subscriptions it dropped on a slow probe (after a minute, doubling to at most an hour) until commands go two seconds without a limit. Resends count toward usage, and a resent subscription can re-announce a presence join.
- Subscriptions and publishes wait for room when the writer is full instead of failing. A subscription change goes out ahead of publishes, but never ahead of a publish to its own segment that was queued before it. A presence query still raises `Backpressure` when the writer is full or sending is paused.
- No offline queue (publishes still waiting when the connection drops fail), no durable history, no global ordering.
- Errors the server sends (`PermissionDeniedError`, `RateLimitError`, `MessageSizeLimitError`, `ParserError`, `SendError`, `InternalError`) arrive through `events().on_error` as a `ServerError` carrying the server's `type`, `sub_type` (the command it answers), message and `resource` (what that command names, such as the segment). A denied or oversized publish still returns normally, since publishing has no receipt. A failed presence query is the exception: its error names the query, so `presence_list()` raises it at once.
- Publishing to a segment joins it server-side; subscribing to presence also joins it for messages.

## Limits and defaults

| What             | Value                                                                                 |
| ---------------- | ------------------------------------------------------------------------------------- |
| Outbound command | 2 MiB encoded, rejected before any write                                              |
| Plan payload cap | enforced by the server per plan; see below                                            |
| Writer bounds    | 64 pending commands / 2 MiB of unsent data; 64 queued publishes, plus resends         |
| Rate-limit pause | 1 s plus full jitter growing with consecutive limits, at most 31 s                    |
| Resends          | last 2 s of commands, at most 64 publishes, each once                                 |
| Quota probe      | after 8 limits in a row: dropped subscriptions retried after 1 min, doubling to 1 h   |
| Connect deadline | `connect_timeout_ms`, default 15 s                                                    |
| Presence query   | one in flight per channel, default 10 s deadline; a timeout never drops the connection |
| Reconnect        | 10 retries, full jitter up to 30 s, reset after 60 s connected                        |
| Dedup window     | 1024 message ids per channel                                                          |

Received messages are never size-checked: they are already in memory when they arrive, so the client processes whatever the server sends. Each plan caps publish payloads: 64 KiB free, 128 KiB standard, 512 KiB pro, 1024 KiB prime. A publish over your plan's cap returns locally and is rejected afterwards with a `MessageSizeLimitError`, and it still counts toward your usage.

## asyncio, threads and cancellation

Everything runs on the event loop that called `connect()`; use a channel from that loop's thread only. Cancellation is asyncio's own: cancelling the task awaiting `connect()` abandons the attempt and leaves the channel `failed`, cancelling an awaiting `publish()` withdraws a publish that has not gone out yet, and cancelling `presence_list()` frees the query slot. `CancelledError` propagates unchanged.

Listeners are plain functions called synchronously, in registration order, as events arrive: a slow listener slows delivery rather than growing a queue. `async def` listeners are refused at registration; start a task from a listener for asynchronous work.

The client turns off the WebSocket library's own logging for its connections, because the library logs the handshake URL, and that URL carries the credentials.

## Development

`uv run nox` runs the whole check: lint, `mypy --strict`, the unit suites on every supported Python, and the package check (build, install the wheel alone, verify its contents and that it carries no signing facility). `uv run nox -s live` runs the acceptance suites against a real Celeris stack; they read `CELERIS_WS_URL`, `CELERIS_CLIENT_ID` and `CELERIS_SIGNING_SECRET` from a gitignored `.env` or the environment.

```sh
uv sync
uv run nox
```

`uv sync` creates `.venv` with the package and its development tools, at the versions in `uv.lock`. `uv add` and `uv remove` change a dependency in `pyproject.toml` and `uv.lock` together: give a runtime dependency a range (`uv add "httpx>=0.28,<1"`) and pin a tool exactly (`uv add --group dev "coverage==7.10.0"`).

`make release` runs that check, builds, and uploads to PyPI from a clean git tree; `make release-test` rehearses on TestPyPI, and `make smoke` installs the published version in a fresh environment.

Read [CONVENTIONS.md](CONVENTIONS.md) before contributing, and [SECURITY.md](SECURITY.md) before reporting a vulnerability.

## License

[Apache 2.0](LICENSE).
