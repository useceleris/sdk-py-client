import asyncio
import time

import pytest

from tests.live.helpers import (
    GENERATED_MESSAGE_ID,
    DeliveredMessage,
    collect,
    connected_channel,
    next_error,
    next_message,
    unique_channel_reference,
)
from useceleris_client import (
    CelerisConnectionError,
    ChannelError,
    MessageMetadata,
    ServerError,
)

pytestmark = pytest.mark.live


def patterned(length: int) -> bytes:
    return bytes(index % 251 for index in range(length))


async def test_delivers_binary_payloads_with_their_ids_and_per_connection_echo() -> (
    None
):
    reference = unique_channel_reference("msg")
    publisher = await connected_channel(reference)
    receiver = await connected_channel(reference)
    publisher_saw = collect(publisher, "chat")
    receiver_saw = collect(receiver, "chat")
    await asyncio.sleep(1.5)

    await publisher.segment("chat").publish("hello-바이너리".encode())

    message = await next_message(
        receiver.segment("chat"), lambda message: True, "cross-connection delivery"
    )
    # REV-01: the id the SDK generated arrives unchanged.
    assert GENERATED_MESSAGE_ID.fullmatch(message.metadata.message_id)
    assert message.payload.decode() == "hello-바이너리"
    assert isinstance(message.metadata.timestamp, int)
    assert message.metadata.timestamp > 0

    # A sibling connection of the same token receives; the publishing
    # connection itself is echo-suppressed.
    await asyncio.sleep(1.5)
    assert len(receiver_saw) >= 1
    assert publisher_saw == []
    await publisher.close()
    await receiver.close()


async def test_echoes_to_the_publisher_when_the_token_allows_echo() -> None:
    channel = await connected_channel(unique_channel_reference("echo"), allow_echo=True)
    saw = collect(channel, "chat")
    await asyncio.sleep(1.5)

    await channel.segment("chat").publish(b"self")
    await next_message(
        channel.segment("chat"),
        lambda message: message.payload == b"self",
        "an echoed publish",
    )

    assert len(saw) >= 1
    await channel.close()


async def test_demuxes_segments_and_stops_delivery_after_unsubscribe() -> None:
    reference = unique_channel_reference("segments")
    publisher = await connected_channel(reference)
    receiver = await connected_channel(reference)
    alpha = collect(receiver, "alpha")
    beta: list[DeliveredMessage] = []
    receiver.segment("beta").on_message(
        lambda payload, metadata: beta.append(DeliveredMessage(payload, metadata))
    )
    beta_membership = receiver.segment("beta").subscribe()
    await asyncio.sleep(1.5)

    await publisher.segment("alpha").publish(b"a")
    await publisher.segment("beta").publish(b"b")
    await next_message(
        receiver.segment("alpha"),
        lambda message: message.payload == b"a",
        "alpha delivery",
    )
    await asyncio.sleep(1.5)

    assert all(message.metadata.segment_id == "alpha" for message in alpha)
    assert all(message.metadata.segment_id == "beta" for message in beta)
    beta_count = len(beta)

    beta_membership.cancel()
    await asyncio.sleep(1.5)
    await publisher.segment("beta").publish(b"late")
    await asyncio.sleep(2.5)

    assert len(beta) == beta_count
    await publisher.close()
    await receiver.close()


async def test_delivers_on_the_default_segment_without_subscribing() -> None:
    reference = unique_channel_reference("default")
    publisher = await connected_channel(reference)
    receiver = await connected_channel(reference)
    seen: list[MessageMetadata] = []
    receiver.default_segment().on_message(
        lambda payload, metadata: seen.append(metadata)
    )
    await asyncio.sleep(1.5)

    await publisher.default_segment().publish(b"lobby")
    await next_message(
        receiver.default_segment(),
        lambda message: message.payload == b"lobby",
        "default-segment delivery",
    )

    assert seen[0].segment_id == "default"
    await publisher.close()
    await receiver.close()


async def test_round_trips_a_large_binary_payload() -> None:
    reference = unique_channel_reference("large")
    publisher = await connected_channel(reference)
    receiver = await connected_channel(reference)
    receiver.segment("bulk").subscribe()
    await asyncio.sleep(1.5)
    payload = patterned(100 * 1024)

    await publisher.segment("bulk").publish(payload)

    message = await next_message(
        receiver.segment("bulk"),
        lambda received: len(received.payload) == len(payload),
        "large payload delivery",
        20,
    )
    assert message.payload == payload
    await publisher.close()
    await receiver.close()


# Needs the qualification app on a plan with message_size_limit_in_kb of at
# least 1024.
async def test_round_trips_a_full_1024_kib_payload() -> None:
    reference = unique_channel_reference("huge")
    publisher = await connected_channel(reference)
    receiver = await connected_channel(reference)
    receiver.segment("bulk").subscribe()
    await asyncio.sleep(1.5)
    payload = patterned(1024 * 1024)

    await publisher.segment("bulk").publish(payload)

    # Framed, this delivery is larger than 1 MiB, the WebSocket library's
    # default inbound limit (LIMIT-01).
    message = await next_message(
        receiver.segment("bulk"),
        lambda received: len(received.payload) == len(payload),
        "the 1024 KiB delivery",
        30,
    )
    assert message.payload == payload
    await publisher.close()
    await receiver.close()


async def test_reports_a_publish_over_the_plan_cap_as_message_size_limit() -> None:
    channel = await connected_channel(unique_channel_reference("oversize"))

    # Over every plan's cap but under the 2 MiB transport ceiling, so it passes
    # the client's check and is accepted locally before the server rejects it.
    await channel.segment("bulk").publish(bytes(1536 * 1024))

    rejection = await next_error(
        channel,
        lambda error: (
            isinstance(error, ServerError) and error.type == "MessageSizeLimitError"
        ),
        "the MessageSizeLimitError frame",
        20,
    )

    assert "Message size limit exceeded" in str(rejection)
    assert channel.state == "connected"
    await channel.close()


async def test_reports_a_read_only_tokens_publish_as_permission_denied() -> None:
    read_only = await connected_channel(
        unique_channel_reference("perm"),
        token_permission={"read": True, "write": False},
    )

    # Publish returns locally; the denial arrives later through on_error.
    await read_only.segment("chat").publish(b"denied")
    denial = await next_error(
        read_only,
        lambda error: (
            isinstance(error, ServerError) and error.type == "PermissionDeniedError"
        ),
        "the PermissionDeniedError frame",
    )

    assert isinstance(denial, ServerError)
    assert str(denial) != ""
    assert (denial.sub_type, denial.resource) == ("PUB", "chat")
    assert read_only.state == "connected"
    await read_only.close()


async def test_keeps_a_write_only_token_publishing_while_receiving_nothing() -> None:
    reference = unique_channel_reference("writeonly")
    write_only = await connected_channel(
        reference, token_permission={"read": False, "write": True}
    )
    reader = await connected_channel(reference)
    writer_saw = collect(write_only, "chat")
    reader.segment("chat").subscribe()
    await asyncio.sleep(1.5)

    await write_only.segment("chat").publish(b"one-way")
    await next_message(
        reader.segment("chat"),
        lambda message: message.payload == b"one-way",
        "delivery to the reader",
    )
    await asyncio.sleep(1.5)

    assert writer_saw == []
    await write_only.close()
    await reader.close()


@pytest.mark.timeout(240)
async def test_recovers_subscriptions_the_server_drops_under_its_rate_limit() -> None:
    reference = unique_channel_reference("limit")
    # Separate token references keep separate per-connection limits.
    subscriber = await connected_channel(reference, reference="limit-subscriber")
    publisher = await connected_channel(reference, reference="limit-publisher")
    rate_limited = False

    def watch(error: ChannelError) -> None:
        nonlocal rate_limited

        if isinstance(error, ServerError) and error.type == "RateLimitError":
            rate_limited = True

    subscriber.events().on_error(watch)

    # The limiter tolerates bursts, so subscriptions go out in growing batches
    # until one trips it; some of them are then dropped, and only recovery can
    # restore them.
    segment_ids: list[str] = []
    delivered: set[str] = set()

    def deliver_to(segment_id: str) -> None:
        subscriber.segment(segment_id).on_message(
            lambda payload, metadata: delivered.add(segment_id)
        )

    while not rate_limited and len(segment_ids) < 2_000:
        for _ in range(250):
            segment_id = f"limit-{len(segment_ids)}"
            segment_ids.append(segment_id)
            deliver_to(segment_id)
            subscriber.segment(segment_id).subscribe()

        await asyncio.sleep(0.5)

    assert rate_limited, "the subscription burst must trip the limit"

    # Publishes to every segment not yet delivered, round after round, until
    # each subscription has recovered. A publish the publisher's own limit
    # drops is simply published again in the next round.
    deadline = time.monotonic() + 150

    while len(delivered) < len(segment_ids) and time.monotonic() < deadline:
        await asyncio.sleep(3)

        for segment_id in segment_ids:
            if segment_id in delivered:
                continue

            try:
                await publisher.segment(segment_id).publish(segment_id.encode())
            except CelerisConnectionError as error:
                # The publisher trips its own limit: sending pauses and the
                # publish queue fills. Back off and let it drain.
                if error.code != "Backpressure":
                    raise

                await asyncio.sleep(2)

            await asyncio.sleep(0.01)

    assert len(delivered) == len(segment_ids)
    await publisher.close()
    await subscriber.close()
