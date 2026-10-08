import asyncio
import time

import pytest

from tests.live.helpers import (
    GENERATED_MESSAGE_ID,
    client_id,
    collect,
    connected_channel,
    next_error,
    next_message,
    now_ms,
    sign_credentials,
    signing_secret,
    started,
    unique_channel_reference,
    websocket_url,
)
from useceleris_client import (
    CelerisConnectionError,
    Channel,
    ChannelError,
    ConfigurationError,
    CredentialRequest,
    Credentials,
    MessageMetadata,
    ServerError,
    create_client,
)

pytestmark = pytest.mark.live


def patterned(length: int) -> bytes:
    return bytes(index % 251 for index in range(length))


# end function patterned


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


# end function test_delivers_binary_payloads_with_their_ids_and_per_connection_echo


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


# end function test_echoes_to_the_publisher_when_the_token_allows_echo


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


# end function test_delivers_on_the_default_segment_without_subscribing


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


# end function test_round_trips_a_large_binary_payload


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


# end function test_round_trips_a_full_1024_kib_payload


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


# end function test_reports_a_publish_over_the_plan_cap_as_message_size_limit


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


# end function test_reports_a_read_only_tokens_publish_as_permission_denied


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


# end function test_keeps_a_write_only_token_publishing_while_receiving_nothing


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

    # end function watch

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

    # end function deliver_to

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


# end function test_recovers_subscriptions_the_server_drops_under_its_rate_limit


async def provide_publisher_credentials(request: CredentialRequest) -> Credentials:
    return sign_credentials(client_id(), signing_secret(), reference="limit-publisher")


# end function provide_publisher_credentials


# L2 (RESEND-01): publishes resent after a rate limit carry their original
# ids, so the receiver's dedup window drops any copy that got through.
@pytest.mark.timeout(120)
async def test_delivers_no_duplicate_under_a_publish_rate_limit(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("limit-publish")
    # Separate token references keep separate per-connection limits.
    receiver = await connected_channel(
        reference, opened=opened, reference="limit-receiver"
    )
    # A queue large enough for the whole burst, so that the publishes reach
    # the server instead of being refused with Backpressure in the client.
    publisher = create_client(
        base_url=websocket_url(),
        allow_insecure_loopback=True,
        publish_queue_size=10_000,
        credential_provider=provide_publisher_credentials,
    ).channel(reference)
    opened.append(publisher)
    await publisher.connect()
    delivered = collect(receiver, "burst")
    await asyncio.sleep(1.5)
    rate_limited = False

    def watch(error: ChannelError) -> None:
        nonlocal rate_limited

        if isinstance(error, ServerError) and error.type == "RateLimitError":
            rate_limited = True

    # end function watch

    publisher.events().on_error(watch)

    # Bursts that double in size until one trips the limit. The ceiling on the
    # total keeps the test finite. A publish the full queue refuses with
    # Backpressure never went out, which is fine.
    published: set[bytes] = set()
    burst_size = 100

    while not rate_limited and len(published) < 5_000:
        burst: list[asyncio.Future[None]] = []

        for _ in range(burst_size):
            body = f"burst-{len(published)}".encode()
            published.add(body)
            burst.append(
                asyncio.ensure_future(publisher.segment("burst").publish(body))
            )

        await asyncio.gather(*burst, return_exceptions=True)
        burst_size *= 2
        await asyncio.sleep(0.2)

    assert rate_limited, "the publish burst must trip the limit"

    await asyncio.sleep(3)
    marker = await started(
        next_message(
            receiver.segment("burst"),
            lambda message: message.payload == b"marker",
            "the marker",
            30,
        )
    )
    published.add(b"marker")
    await publisher.segment("burst").publish(b"marker")
    await marker
    await asyncio.sleep(1.5)

    message_ids = [message.metadata.message_id for message in delivered]
    bodies = [message.payload for message in delivered]

    assert len(set(message_ids)) == len(message_ids)
    assert [body for body in bodies if body not in published] == []
    assert b"marker" in bodies
    assert receiver.state == "connected"
    assert publisher.state == "connected"


# end function test_delivers_no_duplicate_under_a_publish_rate_limit


async def test_delivers_a_custom_message_id_unchanged_and_drops_a_repeat_of_it(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("custom-id")
    publisher = await connected_channel(reference, opened=opened)
    receiver = await connected_channel(reference, opened=opened)
    received = collect(receiver, "chat")
    await asyncio.sleep(1.5)
    message_id = f"order-{now_ms()}"

    await publisher.segment("chat").publish(b"first", message_id=message_id)
    await publisher.segment("chat").publish(b"repeat", message_id=message_id)
    marker = await started(
        next_message(
            receiver.segment("chat"),
            lambda message: message.payload == b"marker",
            "the marker after the repeat",
        )
    )
    await publisher.segment("chat").publish(b"marker")
    await marker

    assert [message.payload for message in received] == [b"first", b"marker"]
    assert received[0].metadata.message_id == message_id


# end function test_delivers_a_custom_message_id_unchanged_and_drops_a_repeat_of_it


async def test_round_trips_an_empty_payload(opened: list[Channel]) -> None:
    reference = unique_channel_reference("empty")
    publisher = await connected_channel(reference, opened=opened)
    receiver = await connected_channel(reference, opened=opened)
    receiver.segment("chat").subscribe()
    await asyncio.sleep(1.5)

    arrived = await started(
        next_message(
            receiver.segment("chat"),
            lambda message: len(message.payload) == 0,
            "the empty payload",
        )
    )
    await publisher.segment("chat").publish(b"")
    message = await arrived

    assert message.payload == b""
    assert GENERATED_MESSAGE_ID.fullmatch(message.metadata.message_id)


# end function test_round_trips_an_empty_payload


async def test_refuses_a_payload_one_byte_over_the_1024_kib_plan_cap(
    opened: list[Channel],
) -> None:
    channel = await connected_channel(
        unique_channel_reference("cap-plus-one"), opened=opened
    )

    rejected = await started(
        next_error(
            channel,
            lambda error: (
                isinstance(error, ServerError) and error.type == "MessageSizeLimitError"
            ),
            "the MessageSizeLimitError frame",
            20,
        )
    )
    await channel.segment("bulk").publish(bytes(1024 * 1024 + 1))
    rejection = await rejected

    assert "size limit = 1024 KB" in str(rejection)
    assert channel.state == "connected"


# end function test_refuses_a_payload_one_byte_over_the_1024_kib_plan_cap


async def test_refuses_a_command_over_2_mib_locally_and_stays_connected(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("local-ceiling")
    publisher = await connected_channel(reference, opened=opened)
    receiver = await connected_channel(reference, opened=opened)
    received = collect(receiver, "bulk")
    await asyncio.sleep(1.5)

    with pytest.raises(ConfigurationError):
        await publisher.segment("bulk").publish(bytes(2 * 1024 * 1024))

    after = await started(
        next_message(
            receiver.segment("bulk"),
            lambda message: message.payload == b"after",
            "the publish after the refusal",
        )
    )
    await publisher.segment("bulk").publish(b"after")
    await after

    assert [message.payload for message in received] == [b"after"]
    assert publisher.state == "connected"


# end function test_refuses_a_command_over_2_mib_locally_and_stays_connected


@pytest.mark.timeout(90)
async def test_delivers_a_paced_burst_of_50_messages_in_publish_order_one_time_each(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("burst")
    publisher = await connected_channel(reference, opened=opened)
    receiver = await connected_channel(reference, opened=opened)
    received = collect(receiver, "chat")
    await asyncio.sleep(1.5)
    bodies = [f"b{index}".encode() for index in range(50)]

    last = await started(
        next_message(
            receiver.segment("chat"),
            lambda message: message.payload == b"b49",
            "the last message of the burst",
            30,
        )
    )

    # Paced below the per-second publish limit.
    for start in range(0, len(bodies), 10):
        for body in bodies[start : start + 10]:
            await publisher.segment("chat").publish(body)

        await asyncio.sleep(1.1)

    await last
    await asyncio.sleep(1.5)

    assert [message.payload for message in received] == bodies
    assert len({message.metadata.message_id for message in received}) == 50


# end function test_delivers_a_paced_burst_of_50_messages_in_publish_order_one_time_each
