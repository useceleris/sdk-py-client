import pytest

from tests.live.helpers import (
    client_id,
    client_with,
    connected_channel,
    next_notice,
    now_ms,
    qualification_client,
    sign_credentials,
    signing_secret,
    unique_channel_reference,
)
from useceleris_client import CelerisConnectionError, read_text

pytestmark = pytest.mark.live


async def test_connects_with_valid_credentials_and_receives_the_greetings() -> None:
    channel = qualification_client().channel(unique_channel_reference("auth"))
    notices: list[str] = []
    channel.events().on_notice(lambda notice: notices.append(read_text(notice.payload)))

    await channel.connect()
    await next_notice(
        channel,
        lambda notice: len(notices) >= 2,
        "the connect and default-subscribe greetings",
    )

    assert any("Successfully connected" in notice for notice in notices)
    assert any('segment "default"' in notice for notice in notices)
    await channel.close()


async def test_rejects_an_invalid_signature_as_transport() -> None:
    # DEV-02: a refused handshake is never labelled an authorization failure.
    channel = client_with(
        lambda: sign_credentials(client_id(), "wrong-secret")
    ).channel(unique_channel_reference("badsig"))

    with pytest.raises(CelerisConnectionError) as caught:
        await channel.connect()

    assert caught.value.code == "Transport"
    assert channel.state == "failed"


async def test_rejects_an_unknown_client_id() -> None:
    channel = client_with(
        lambda: sign_credentials("no-such-client", signing_secret())
    ).channel(unique_channel_reference("noclient"))

    with pytest.raises(CelerisConnectionError) as caught:
        await channel.connect()

    assert caught.value.code == "Transport"


async def test_rejects_expired_and_future_timestamps_inside_the_observed_window() -> (
    None
):
    reference = unique_channel_reference("window")
    expired = qualification_client(timestamp=now_ms() - 61 * 60 * 1_000).channel(
        reference
    )

    with pytest.raises(CelerisConnectionError) as caught:
        await expired.connect()

    assert caught.value.code == "Transport"

    future = qualification_client(timestamp=now_ms() + 5 * 60 * 1_000).channel(
        reference
    )

    with pytest.raises(CelerisConnectionError) as caught:
        await future.connect()

    assert caught.value.code == "Transport"

    # Documented intent is a 60-second window; the server accepts up to 60
    # minutes (D-001 evidence: recorded, not relied upon).
    stale = qualification_client(timestamp=now_ms() - 59 * 60 * 1_000).channel(
        reference
    )
    await stale.connect()
    await stale.close()


async def test_rejects_a_channel_outside_the_tokens_restriction() -> None:
    channel = qualification_client(channel_references=["some-other-channel"]).channel(
        unique_channel_reference("restricted")
    )

    with pytest.raises(CelerisConnectionError) as caught:
        await channel.connect()

    assert caught.value.code == "Transport"


async def test_accepts_a_channel_inside_the_tokens_restriction() -> None:
    reference = unique_channel_reference("allowed")
    channel = await connected_channel(reference, channel_references=[reference])

    assert channel.state == "connected"
    await channel.close()
