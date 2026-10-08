import asyncio
from typing import Any

import pytest

from tests.live.helpers import (
    DroppingProxy,
    client_id,
    connected_channel,
    next_message,
    sign_credentials,
    signing_secret,
    started,
    unique_channel_reference,
    websocket_url,
)
from useceleris_client import (
    CelerisConnectionError,
    Channel,
    CredentialRequest,
    Credentials,
    create_client,
)

pytestmark = pytest.mark.live


async def open_with(
    opened: list[Channel],
    reference: str,
    claims: dict[str, Any] | None = None,
    base_url: str | None = None,
    **options: Any,
) -> Channel:
    """A connected channel whose client has these non-default options."""

    async def provide(request: CredentialRequest) -> Credentials:
        return sign_credentials(client_id(), signing_secret(), **(claims or {}))

    # end function provide

    channel = create_client(
        base_url=base_url or websocket_url(),
        allow_insecure_loopback=True,
        credential_provider=provide,
        **options,
    ).channel(reference)
    opened.append(channel)
    await channel.connect()

    return channel


# end function open_with


async def test_times_out_a_presence_query_at_a_1_ms_timeout_and_stays_connected(
    opened: list[Channel],
) -> None:
    channel = await open_with(
        opened, unique_channel_reference("option-presence"), presence_query_timeout_ms=1
    )

    with pytest.raises(CelerisConnectionError) as caught:
        await channel.segment("room").presence_list(page=1, per_page=10)

    assert (caught.value.code, caught.value.message) == (
        "Timeout",
        "Presence query timed out after 1 ms.",
    )
    assert channel.state == "connected"


# end function test_times_out_a_presence_query_at_a_1_ms_timeout_and_stays_connected


async def test_delivers_replayed_ids_again_beyond_a_deduplication_window_of_1(
    opened: list[Channel],
) -> None:
    reference = unique_channel_reference("option-window")
    publisher = await connected_channel(reference, opened=opened)
    receiver = await open_with(
        opened, reference, {"replay": True}, deduplication_window_size=1
    )
    history: list[bytes] = []
    receiver.segment("history").on_message(
        lambda payload, metadata: history.append(payload)
    )
    first = receiver.segment("history").subscribe()
    await asyncio.sleep(1.5)
    two = await started(
        next_message(
            receiver.segment("history"),
            lambda message: message.payload == b"two",
            "the live delivery of two",
        )
    )
    await publisher.segment("history").publish(b"one")
    await publisher.segment("history").publish(b"two")
    await two

    # The window holds only "two". The re-join replays "one", which pushes
    # "two" out of the window, so the replayed "two" is delivered again too.
    first.cancel()
    await asyncio.sleep(1.5)
    receiver.segment("history").subscribe()
    await asyncio.sleep(4)

    assert history == [b"one", b"two", b"one", b"two"]


# end function test_delivers_replayed_ids_again_beyond_a_deduplication_window_of_1


@pytest.mark.timeout(90)
async def test_refuses_the_second_waiting_publish_with_a_publish_queue_of_1(
    proxy: DroppingProxy, opened: list[Channel]
) -> None:
    channel = await open_with(
        opened,
        unique_channel_reference("option-queue"),
        base_url=proxy.url,
        publish_queue_size=1,
    )

    # The proxy stops reading, so the socket buffer fills and publishes wait.
    proxy.stall_upstream(True)
    payload = bytes(900 * 1024)

    async def outcome() -> str:
        try:
            await channel.segment("bulk").publish(payload)
        except CelerisConnectionError as error:
            return error.message

        return "sent"

    # end function outcome

    outcomes = [asyncio.ensure_future(outcome()) for _ in range(40)]
    await asyncio.sleep(2)
    proxy.stall_upstream(False)
    results = await asyncio.gather(*outcomes)

    assert (
        "The publish queue is full (size 1). Retry once some publishes have gone out."
        in results
    )
    assert results.count("sent") > 0


# end function test_refuses_the_second_waiting_publish_with_a_publish_queue_of_1
