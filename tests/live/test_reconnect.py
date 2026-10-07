import asyncio
from dataclasses import dataclass

import pytest

from tests.live.helpers import (
    DeliveredMessage,
    DroppingProxy,
    client_id,
    connected_channel,
    next_message,
    next_presence,
    sign_credentials,
    signing_secret,
    started,
    unique_channel_reference,
    wait_for,
)
from useceleris_client import (
    Channel,
    CredentialRequest,
    Credentials,
    RecoveryEvent,
    create_client,
)

pytestmark = pytest.mark.live


def received(channel: Channel, segment_id: str) -> list[tuple[bytes, str]]:
    """Payloads and message ids one segment listener receives, in arrival order."""
    deliveries: list[tuple[bytes, str]] = []
    channel.segment(segment_id).on_message(
        lambda payload, metadata: deliveries.append((payload, metadata.message_id))
    )

    return deliveries


def bodies(deliveries: list[tuple[bytes, str]]) -> list[bytes]:
    return [payload for payload, _ in deliveries]


async def arrival(
    channel: Channel, segment_id: str, body: bytes
) -> asyncio.Future[DeliveredMessage]:
    """Starts waiting for a segment listener on the channel to receive this
    payload; await the result after the action that causes the delivery."""
    delivered = asyncio.ensure_future(
        next_message(
            channel.segment(segment_id),
            lambda message: message.payload == body,
            f"{body!r} on {segment_id}",
            30,
        )
    )
    await asyncio.sleep(0)

    return delivered


@dataclass
class Reconnecting:
    """A receiver behind the dropping proxy and a publisher that connects
    directly, so only the receiver has the outage."""

    dropping: DroppingProxy
    receiver: Channel
    publisher: Channel
    requests: list[CredentialRequest]
    recoveries: list[RecoveryEvent]

    async def start_outage(self) -> None:
        """Cuts every connection through the proxy and refuses new ones, so a
        message published now can reach the receiver only by replay."""
        self.dropping.refusing = True
        self.dropping.drop_all()
        await asyncio.sleep(0.5)
        assert self.receiver.state == "reconnecting"

    async def end_outage(self) -> None:
        if self.recoveries:
            self.dropping.refusing = False
            return

        recovered = asyncio.ensure_future(
            wait_for(
                self.receiver.events().on_recovery,
                lambda event: True,
                "the recovery event",
                45,
            )
        )
        await asyncio.sleep(0)
        self.dropping.refusing = False
        await recovered


async def set_up(
    proxy: DroppingProxy,
    opened: list[Channel],
    label: str,
    replay_on_reconnect: bool = True,
) -> Reconnecting:
    """With replay_on_reconnect, the credential provider signs the lookback
    the SDK asks for into the token: the canonical mapping."""
    reference = unique_channel_reference(label)
    requests: list[CredentialRequest] = []

    async def provide(request: CredentialRequest) -> Credentials:
        requests.append(request)

        if replay_on_reconnect and request.reason == "reconnect":
            return sign_credentials(
                client_id(), signing_secret(), replay=request.replay_lookback_ms or 0
            )

        return sign_credentials(client_id(), signing_secret())

    receiver = create_client(
        base_url=proxy.url, allow_insecure_loopback=True, credential_provider=provide
    ).channel(reference)
    opened.append(receiver)
    recoveries: list[RecoveryEvent] = []
    receiver.events().on_recovery(recoveries.append)
    publisher = await connected_channel(reference)
    opened.append(publisher)

    return Reconnecting(proxy, receiver, publisher, requests, recoveries)


# SUB-01, REC-02: an outage recovers with fresh credentials, a replay of what
# it missed and the subscription restored; replayed duplicates are dropped by
# the dedup window.
@pytest.mark.timeout(120)
async def test_recovers_after_an_outage_with_replay_and_restored_subscriptions(
    proxy: DroppingProxy, opened: list[Channel]
) -> None:
    setup = await set_up(proxy, opened, "reconnect")
    receiver, publisher = setup.receiver, setup.publisher
    chat = received(receiver, "chat")
    receiver.segment("chat").subscribe()
    await receiver.connect()
    await asyncio.sleep(1.5)
    before = await arrival(receiver, "chat", b"before")
    await publisher.segment("chat").publish(b"before")
    await before

    await setup.start_outage()
    await publisher.segment("chat").publish(b"during")
    await asyncio.sleep(2)
    during = await arrival(receiver, "chat", b"during")
    await setup.end_outage()
    await during

    assert setup.recoveries[0].possible_gaps
    assert setup.recoveries[0].possible_duplicates
    assert len(setup.requests) > 1

    for request in setup.requests[1:]:
        assert request.reason == "reconnect"
        assert request.disconnected_at is not None
        assert request.disconnected_at > 0
        assert request.replay_lookback_ms is not None
        assert request.replay_lookback_ms >= 5_000

    # The subscription was restored on the new connection.
    after = await arrival(receiver, "chat", b"after")
    await publisher.segment("chat").publish(b"after")
    await after
    await asyncio.sleep(1.5)

    # Replay sent "before" again; the dedup window dropped it.
    assert bodies(chat) == [b"before", b"during", b"after"]
    assert len({message_id for _, message_id in chat}) == 3


@pytest.mark.timeout(120)
async def test_recovers_every_missed_message_on_several_segments_in_order_and_one_time(
    proxy: DroppingProxy, opened: list[Channel]
) -> None:
    setup = await set_up(proxy, opened, "reconnect-segments")
    receiver, publisher = setup.receiver, setup.publisher
    alpha = received(receiver, "alpha")
    beta = received(receiver, "beta")
    lobby = received(receiver, "default")
    channel_wide: list[str] = []
    receiver.events().on_message(
        lambda payload, metadata: channel_wide.append(
            f"{metadata.segment_id}:{payload.decode()}"
        )
    )
    receiver.segment("alpha").subscribe()
    receiver.segment("beta").subscribe()
    await receiver.connect()
    await asyncio.sleep(1.5)
    first_alpha = await arrival(receiver, "alpha", b"a0")
    await publisher.segment("alpha").publish(b"a0")
    await first_alpha

    await setup.start_outage()

    for body in [b"a1", b"a2", b"a3"]:
        await publisher.segment("alpha").publish(body)

    for body in [b"b1", b"b2"]:
        await publisher.segment("beta").publish(body)

    await publisher.segment("default").publish(b"d1")
    await asyncio.sleep(2)
    last_alpha = await arrival(receiver, "alpha", b"a3")
    last_beta = await arrival(receiver, "beta", b"b2")
    last_lobby = await arrival(receiver, "default", b"d1")
    await setup.end_outage()
    await asyncio.gather(last_alpha, last_beta, last_lobby)
    await asyncio.sleep(1.5)

    assert bodies(alpha) == [b"a0", b"a1", b"a2", b"a3"]
    assert bodies(beta) == [b"b1", b"b2"]
    assert bodies(lobby) == [b"d1"]
    assert sorted(channel_wide) == sorted(
        [
            "alpha:a0",
            "alpha:a1",
            "alpha:a2",
            "alpha:a3",
            "beta:b1",
            "beta:b2",
            "default:d1",
        ]
    )


@pytest.mark.timeout(120)
async def test_loses_missed_messages_without_replay_but_restores_the_subscription(
    proxy: DroppingProxy, opened: list[Channel]
) -> None:
    setup = await set_up(proxy, opened, "reconnect-no-replay", False)
    receiver, publisher = setup.receiver, setup.publisher
    chat = received(receiver, "chat")
    receiver.segment("chat").subscribe()
    await receiver.connect()
    await asyncio.sleep(1.5)
    before = await arrival(receiver, "chat", b"before")
    await publisher.segment("chat").publish(b"before")
    await before

    await setup.start_outage()
    await publisher.segment("chat").publish(b"missed")
    await asyncio.sleep(2)
    await setup.end_outage()
    await asyncio.sleep(1.5)

    after = await arrival(receiver, "chat", b"after")
    await publisher.segment("chat").publish(b"after")
    await after
    await asyncio.sleep(1.5)

    # The recovery event declares the gap that this test makes.
    assert setup.recoveries[0].possible_gaps
    assert bodies(chat) == [b"before", b"after"]


@pytest.mark.timeout(120)
async def test_recovers_every_missed_message_after_a_longer_outage_with_failed_attempts(
    proxy: DroppingProxy, opened: list[Channel]
) -> None:
    setup = await set_up(proxy, opened, "reconnect-long")
    receiver, publisher = setup.receiver, setup.publisher
    chat = received(receiver, "chat")
    receiver.segment("chat").subscribe()
    await receiver.connect()
    await asyncio.sleep(1.5)

    await setup.start_outage()
    await publisher.segment("chat").publish(b"m1")
    await asyncio.sleep(3)
    await publisher.segment("chat").publish(b"m2")
    await asyncio.sleep(3)
    await publisher.segment("chat").publish(b"m3")
    await asyncio.sleep(0.5)
    last = await arrival(receiver, "chat", b"m3")
    await setup.end_outage()
    await last
    await asyncio.sleep(1.5)

    # Retries in the first 6.5 s fail (their delays are at most 0.5, 1 and
    # 2 s), each with a fresh credential request and a longer lookback.
    reconnects = [
        request for request in setup.requests if request.reason == "reconnect"
    ]
    assert len(reconnects) >= 4
    lookbacks = [
        request.replay_lookback_ms
        for request in reconnects
        if request.replay_lookback_ms is not None
    ]
    assert len(lookbacks) == len(reconnects)
    assert lookbacks == sorted(lookbacks)
    assert lookbacks[-1] >= 11_000
    assert len({request.disconnected_at for request in reconnects}) == 1
    assert bodies(chat) == [b"m1", b"m2", b"m3"]


@pytest.mark.timeout(120)
async def test_does_not_rejoin_a_segment_that_the_connection_joined_only_by_publishing(
    proxy: DroppingProxy, opened: list[Channel]
) -> None:
    setup = await set_up(proxy, opened, "reconnect-publish-join")
    receiver, publisher = setup.receiver, setup.publisher
    team = received(receiver, "team")
    await receiver.connect()
    await receiver.segment("team").publish(b"joining")
    await asyncio.sleep(1.5)
    before = await arrival(receiver, "team", b"before")
    await publisher.segment("team").publish(b"before")
    await before

    await setup.start_outage()
    await setup.end_outage()
    await asyncio.sleep(1.5)

    await publisher.segment("team").publish(b"after")
    control = await arrival(receiver, "default", b"control")
    await publisher.segment("default").publish(b"control")
    await control
    await asyncio.sleep(2.5)

    assert bodies(team) == [b"before"]


@pytest.mark.timeout(120)
async def test_announces_the_new_connection_and_restores_its_presence_subscription(
    proxy: DroppingProxy, opened: list[Channel]
) -> None:
    setup = await set_up(proxy, opened, "reconnect-presence")
    receiver, publisher = setup.receiver, setup.publisher
    receiver.segment("room").subscribe()
    receiver.segment("room").subscribe_presence()
    publisher.segment("room").subscribe_presence()
    first_join = await started(
        next_presence(
            publisher.segment("room"), lambda event: event.joined, "the first join", 20
        )
    )
    await receiver.connect()
    before = await first_join

    left = await started(
        next_presence(
            publisher.segment("room"),
            lambda event: (
                not event.joined and event.connection_id == before.connection_id
            ),
            "the leave of the old connection",
            20,
        )
    )
    await setup.start_outage()
    await left
    rejoined = await started(
        next_presence(
            publisher.segment("room"),
            lambda event: event.joined and event.connection_id != before.connection_id,
            "the join of the new connection",
            45,
        )
    )
    await setup.end_outage()
    await rejoined

    # The receiver's presence subscription came back with the reconnect.
    actor_join = await started(
        next_presence(
            receiver.segment("room"),
            lambda event: event.joined and event.token_reference == "actor",
            "the actor's join at the restored watcher",
            20,
        )
    )
    actor = await connected_channel(
        setup.requests[0].channel_reference, opened=opened, reference="actor"
    )
    actor.segment("room").subscribe()
    await actor_join
