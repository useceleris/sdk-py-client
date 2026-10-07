"""Every client option, validated and honoured through create_client
(CONFIG-01)."""

import asyncio
from typing import Any

import pytest

from tests.helpers.channel import (
    TEST_CREDENTIALS,
    ChannelSetup,
    create_client_channel,
    establish,
    message_frame,
)
from tests.helpers.tasks import failure_of, flush
from tests.helpers.timers import FakeTimers
from tests.helpers.websocket import FakeWebSocket
from useceleris_client import (
    CelerisConnectionError,
    ChannelError,
    Client,
    ConfigurationError,
    CredentialRequest,
    Credentials,
    MessageMetadata,
    create_client,
)

TIMEOUT_OPTIONS = [
    "connect_timeout_ms",
    "reconnect_timeout_ms",
    "presence_query_timeout_ms",
]

COUNT_OPTIONS = [
    "publish_queue_size",
    "deduplication_window_size",
    "maximum_reconnect_attempts",
]

# Holds the fake writer full: any send is refused as backpressure.
FULL_BUFFER = 2 * 1024 * 1024

LOOPBACK_REFUSAL = (
    "Invalid connection URL. base_url must use wss://, or ws:// for a loopback "
    "host when allow_insecure_loopback is True."
)


async def provide(request: CredentialRequest) -> Credentials:
    return TEST_CREDENTIALS


def refusal(**options: Any) -> str:
    with pytest.raises(ConfigurationError) as caught:
        create_client(**{"credential_provider": provide, **options})

    return str(caught.value)


def record_ids(setup: ChannelSetup) -> list[str]:
    delivered: list[str] = []

    def record(payload: bytes, metadata: MessageMetadata) -> None:
        delivered.append(metadata.message_id)

    setup.channel.segment("chat").on_message(record)
    return delivered


def publish(setup: ChannelSetup, payload: bytes) -> "asyncio.Task[None]":
    return asyncio.ensure_future(setup.channel.default_segment().publish(payload))


def published_payloads(socket: FakeWebSocket) -> list[str]:
    return [frame.split("\n")[-2] for frame in socket.sent_frames()]


async def assert_connect_times_out_at(
    setup: ChannelSetup, timers: FakeTimers, timeout_ms: int
) -> None:
    # The socket never opens.
    pending = asyncio.ensure_future(setup.channel.connect())
    await timers.advance(timeout_ms - 1)
    assert not pending.done()

    await timers.advance(1)
    error = await failure_of(pending)
    assert isinstance(error, CelerisConnectionError)
    assert (error.code, str(error)) == (
        "Timeout",
        f"Connection attempt timed out after {timeout_ms} ms.",
    )


async def assert_reconnects_time_out_at(
    setup: ChannelSetup,
    sockets: list[FakeWebSocket],
    timers: FakeTimers,
    timeout_ms: int,
) -> None:
    errors: list[ChannelError] = []
    setup.channel.events().on_error(errors.append)
    await establish(setup, sockets)

    # Retries start at once (zero jitter), and no reconnect socket opens.
    sockets[-1].disconnect()
    await timers.advance(0)
    attempt = sockets[-1]

    await timers.advance(timeout_ms - 1)
    assert sockets[-1] is attempt
    assert setup.channel.state == "reconnecting"

    await timers.advance(1)
    assert sockets[-1] is not attempt
    attempt.close.assert_called_once()

    for _ in range(9):
        await timers.advance(timeout_ms)

    assert setup.channel.state == "failed"
    assert [(error.code, str(error)) for error in errors] == [
        ("Timeout", f"Connection attempt timed out after {timeout_ms} ms.")
    ]


async def assert_presence_query_times_out_at(
    setup: ChannelSetup,
    sockets: list[FakeWebSocket],
    timers: FakeTimers,
    timeout_ms: int,
) -> None:
    pending = asyncio.ensure_future(
        setup.channel.default_segment().presence_list(page=1, per_page=1)
    )
    await timers.advance(timeout_ms - 1)
    assert not pending.done()

    await timers.advance(1)
    error = await failure_of(pending)
    assert isinstance(error, CelerisConnectionError)
    assert (error.code, str(error)) == (
        "Timeout",
        f"Presence query timed out after {timeout_ms} ms.",
    )
    assert setup.channel.state == "connected"
    sockets[-1].close.assert_not_called()


class TestValidation:
    @pytest.mark.parametrize("option", TIMEOUT_OPTIONS + COUNT_OPTIONS)
    @pytest.mark.parametrize(
        ("value", "rule"),
        [
            (0, "Input should be greater than or equal to 1."),
            (-1, "Input should be greater than or equal to 1."),
            (1.5, "Input should be a valid integer."),
            ("1", "Input should be a valid integer."),
            (True, "Input should be a valid integer."),
        ],
    )
    def test_refuses_a_number_option_naming_it_and_its_rule(
        self, option: str, value: object, rule: str
    ) -> None:
        assert refusal(**{option: value}) == f"Invalid client options. {option}: {rule}"

    @pytest.mark.parametrize(
        "option",
        [
            option
            for option in TIMEOUT_OPTIONS + COUNT_OPTIONS
            if "reconnect" not in option
        ],
    )
    def test_refuses_none_for_every_number_option_but_the_reconnect_timeout(
        self, option: str
    ) -> None:
        assert refusal(**{option: None}) == (
            f"Invalid client options. {option}: Input should be a valid integer."
        )

    @pytest.mark.parametrize("option", TIMEOUT_OPTIONS)
    def test_accepts_a_timeout_from_1_ms_to_15_minutes(self, option: str) -> None:
        for value in (1, 900_000):
            options: dict[str, Any] = {"credential_provider": provide, option: value}
            assert isinstance(create_client(**options), Client)

        assert refusal(**{option: 900_001}) == (
            f"Invalid client options. {option}: Input should be less than or equal "
            "to 900000."
        )

    def test_accepts_maximum_reconnect_attempts_from_1_to_100(self) -> None:
        for value in (1, 100):
            options: dict[str, Any] = {
                "credential_provider": provide,
                "maximum_reconnect_attempts": value,
            }
            assert isinstance(create_client(**options), Client)

        assert refusal(maximum_reconnect_attempts=101) == (
            "Invalid client options. maximum_reconnect_attempts: Input should be "
            "less than or equal to 100."
        )

    @pytest.mark.parametrize("provider", [None, "provider", 1])
    def test_refuses_a_credential_provider_that_is_not_callable(
        self, provider: object
    ) -> None:
        assert refusal(credential_provider=provider) == (
            "Invalid client options. credential_provider: Must be callable."
        )

    @pytest.mark.parametrize(
        ("base_url", "rule"),
        [
            (123, "Input should be a valid string."),
            (None, "Input should be a valid string."),
            (b"wss://example.test", "Input should be a valid string."),
            ("", "Must not be empty."),
        ],
    )
    def test_refuses_a_base_url_of_the_wrong_type_or_empty(
        self, base_url: object, rule: str
    ) -> None:
        assert refusal(base_url=base_url) == (
            f"Invalid client options. base_url: {rule}"
        )

    @pytest.mark.parametrize("allow", ["yes", 1, None])
    def test_refuses_a_loopback_opt_in_that_is_not_a_bool(self, allow: object) -> None:
        assert refusal(allow_insecure_loopback=allow) == (
            "Invalid client options. allow_insecure_loopback: Input should be a "
            "valid boolean."
        )


class TestTimeouts:
    async def test_times_out_a_connect_at_a_custom_connect_timeout(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
    ) -> None:
        setup = create_client_channel(monkeypatch, timers, connect_timeout_ms=5_000)

        await assert_connect_times_out_at(setup, timers, 5_000)
        assert setup.channel.state == "failed"
        assert timers.count == 0

    @pytest.mark.parametrize(
        ("connect_timeout_ms", "reconnect_timeout_ms"),
        [(3_000, 3_000), (2_000, 7_000), (6_000, 3_000)],
        ids=["equal", "longer", "shorter"],
    )
    async def test_times_out_each_reconnect_attempt_at_the_reconnect_timeout(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
        connect_timeout_ms: int,
        reconnect_timeout_ms: int,
    ) -> None:
        setup = create_client_channel(
            monkeypatch,
            timers,
            connect_timeout_ms=connect_timeout_ms,
            reconnect_timeout_ms=reconnect_timeout_ms,
        )

        # The first connect keeps the connect timeout.
        await assert_connect_times_out_at(setup, timers, connect_timeout_ms)
        await assert_reconnects_time_out_at(
            setup, sockets, timers, reconnect_timeout_ms
        )

    async def test_reconnects_follow_a_custom_connect_timeout_when_none_is_set(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
    ) -> None:
        setup = create_client_channel(monkeypatch, timers, connect_timeout_ms=4_000)

        await assert_reconnects_time_out_at(setup, sockets, timers, 4_000)

    async def test_times_out_a_presence_query_at_a_custom_timeout(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
    ) -> None:
        setup = await establish(
            create_client_channel(monkeypatch, timers, presence_query_timeout_ms=2_000),
            sockets,
        )

        await assert_presence_query_times_out_at(setup, sockets, timers, 2_000)

    async def test_applies_the_default_timeouts(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
    ) -> None:
        setup = create_client_channel(monkeypatch, timers)

        await assert_connect_times_out_at(setup, timers, 15_000)
        await assert_reconnects_time_out_at(setup, sockets, timers, 15_000)
        await establish(setup, sockets)
        await assert_presence_query_times_out_at(setup, sockets, timers, 10_000)

    async def test_times_out_at_exactly_15_minutes_at_the_largest_timeouts(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
    ) -> None:
        setup = create_client_channel(
            monkeypatch,
            timers,
            connect_timeout_ms=900_000,
            reconnect_timeout_ms=900_000,
            presence_query_timeout_ms=900_000,
        )

        await assert_connect_times_out_at(setup, timers, 900_000)
        await assert_reconnects_time_out_at(setup, sockets, timers, 900_000)
        await establish(setup, sockets)
        await assert_presence_query_times_out_at(setup, sockets, timers, 900_000)


class TestPublishQueueSize:
    async def test_queues_one_publish_and_refuses_the_next_at_size_1(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
    ) -> None:
        setup = await establish(
            create_client_channel(monkeypatch, timers, publish_queue_size=1), sockets
        )
        socket = sockets[-1]
        socket.buffered_amount = FULL_BUFFER

        first = publish(setup, b"first")
        await timers.advance(0)
        assert not first.done()

        error = await failure_of(publish(setup, b"second"))
        assert isinstance(error, CelerisConnectionError)
        assert (error.code, str(error)) == (
            "Backpressure",
            "The publish queue is full (size 1). Retry once some publishes have "
            "gone out.",
        )
        socket.send.assert_not_called()

        socket.buffered_amount = 0
        await timers.advance(50)
        await first
        assert socket.sent_frames() == [
            "@PUB\n$7\ndefault\n$11\ngenerated-1\n$5\nfirst\n"
        ]

    async def test_queues_64_publishes_by_default(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
    ) -> None:
        setup = await establish(create_client_channel(monkeypatch, timers), sockets)
        socket = sockets[-1]
        socket.buffered_amount = FULL_BUFFER

        queued = [publish(setup, str(index).encode()) for index in range(64)]
        await timers.advance(0)

        error = await failure_of(publish(setup, b"64"))
        assert isinstance(error, CelerisConnectionError)
        assert (error.code, str(error)) == (
            "Backpressure",
            "The publish queue is full (size 64). Retry once some publishes have "
            "gone out.",
        )

        socket.buffered_amount = 0
        await timers.advance(50)
        await asyncio.gather(*queued)
        assert published_payloads(socket) == [str(index) for index in range(64)]


class TestDeduplicationWindowSize:
    async def test_remembers_one_id_at_size_1(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
    ) -> None:
        setup = await establish(
            create_client_channel(monkeypatch, timers, deduplication_window_size=1),
            sockets,
        )
        delivered = record_ids(setup)

        sockets[-1].receive(message_frame("chat", "A", "x"))
        sockets[-1].receive(message_frame("chat", "A", "x"))
        assert delivered == ["A"]

        # B evicts A, so A is new again.
        sockets[-1].receive(message_frame("chat", "B", "x"))
        sockets[-1].receive(message_frame("chat", "A", "x"))
        assert delivered == ["A", "B", "A"]

    async def test_remembers_1024_ids_by_default(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
    ) -> None:
        setup = await establish(create_client_channel(monkeypatch, timers), sockets)
        delivered = record_ids(setup)

        for index in range(1024):
            sockets[-1].receive(message_frame("chat", f"id-{index}", "x"))

        sockets[-1].receive(message_frame("chat", "id-0", "x"))
        assert len(delivered) == 1024

        sockets[-1].receive(message_frame("chat", "id-1024", "x"))
        sockets[-1].receive(message_frame("chat", "id-0", "x"))
        assert delivered[-2:] == ["id-1024", "id-0"]


class TestLoopbackOptIn:
    def test_refuses_ws_to_localhost_unless_opted_in(self) -> None:
        assert refusal(base_url="ws://localhost:8080") == LOOPBACK_REFUSAL
        assert (
            refusal(base_url="ws://localhost:8080", allow_insecure_loopback=False)
            == LOOPBACK_REFUSAL
        )

    @pytest.mark.parametrize("allow", [False, True])
    def test_always_refuses_ws_to_a_host_that_is_not_loopback(
        self, allow: bool
    ) -> None:
        assert (
            refusal(base_url="ws://example.test", allow_insecure_loopback=allow)
            == LOOPBACK_REFUSAL
        )

    async def test_connects_over_ws_to_localhost_when_opted_in(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
    ) -> None:
        setup = await establish(
            create_client_channel(
                monkeypatch,
                timers,
                base_url="ws://localhost:8080",
                allow_insecure_loopback=True,
            ),
            sockets,
        )

        assert setup.channel.state == "connected"
        assert sockets[0].url.startswith("ws://localhost:8080/channel/room-1?")
        await setup.channel.close()


class ReconnectAttempts:
    """A channel built through create_client, connected and then dropped, so
    the first reconnect attempt is scheduled. Randomness is 0, so every retry
    delay is zero."""

    def __init__(
        self,
        setup: ChannelSetup,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
    ) -> None:
        self.setup = setup
        self.sockets = sockets
        self.timers = timers
        self.errors: list[ChannelError] = []
        self.states: list[str] = []
        setup.channel.events().on_error(self.errors.append)
        setup.channel.events().on_state_change(self.states.append)

    @classmethod
    async def connect_and_drop(
        cls,
        monkeypatch: pytest.MonkeyPatch,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
        **options: Any,
    ) -> "ReconnectAttempts":
        attempts = cls(
            create_client_channel(monkeypatch, timers, **options), sockets, timers
        )
        await establish(attempts.setup, sockets)
        sockets[-1].disconnect()
        return attempts

    def reconnect_requests(self) -> int:
        return sum(
            1
            for call in self.setup.credential_provider.call_args_list
            if call.args[0].reason == "reconnect"
        )

    async def start_scheduled_attempt(self) -> None:
        socket_count = len(self.sockets)
        await self.timers.advance(0)
        assert len(self.sockets) == socket_count + 1

    async def fail_scheduled_attempts(self, count: int) -> None:
        for _ in range(count):
            await self.start_scheduled_attempt()
            self.sockets[-1].fail()
            await flush()

    async def succeed_scheduled_attempt(self) -> None:
        await self.start_scheduled_attempt()
        self.sockets[-1].open()
        await flush()

    def assert_failed_once(self) -> None:
        assert self.setup.channel.state == "failed"
        (error,) = self.errors
        assert isinstance(error, CelerisConnectionError)
        assert error.code == "Transport"
        assert self.states[-2:] == ["reconnecting", "failed"]
        assert self.timers.count == 0


class TestMaximumReconnectAttempts:
    async def test_fails_on_the_first_failed_attempt_with_a_maximum_of_1(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
    ) -> None:
        attempts = await ReconnectAttempts.connect_and_drop(
            monkeypatch, sockets, timers, maximum_reconnect_attempts=1
        )

        await attempts.fail_scheduled_attempts(1)

        attempts.assert_failed_once()
        assert attempts.reconnect_requests() == 1

    async def test_fails_after_exactly_3_failed_attempts_with_a_maximum_of_3(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
    ) -> None:
        attempts = await ReconnectAttempts.connect_and_drop(
            monkeypatch, sockets, timers, maximum_reconnect_attempts=3
        )

        await attempts.fail_scheduled_attempts(2)
        assert attempts.setup.channel.state == "reconnecting"
        assert timers.count == 1

        await attempts.fail_scheduled_attempts(1)
        attempts.assert_failed_once()
        assert attempts.reconnect_requests() == 3

    async def test_fails_after_10_failed_attempts_by_default(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
    ) -> None:
        attempts = await ReconnectAttempts.connect_and_drop(
            monkeypatch, sockets, timers
        )

        await attempts.fail_scheduled_attempts(9)
        assert attempts.setup.channel.state == "reconnecting"

        await attempts.fail_scheduled_attempts(1)
        attempts.assert_failed_once()
        assert attempts.reconnect_requests() == 10

    async def test_keeps_reconnecting_past_10_failed_attempts_with_a_maximum_of_100(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
    ) -> None:
        attempts = await ReconnectAttempts.connect_and_drop(
            monkeypatch, sockets, timers, maximum_reconnect_attempts=100
        )

        await attempts.fail_scheduled_attempts(10)
        assert attempts.setup.channel.state == "reconnecting"
        assert attempts.errors == []

        await attempts.start_scheduled_attempt()
        assert attempts.reconnect_requests() == 11
        await attempts.setup.channel.close()

    async def test_keeps_spent_attempts_for_an_outage_within_sixty_seconds_of_recovery(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
    ) -> None:
        attempts = await ReconnectAttempts.connect_and_drop(
            monkeypatch, sockets, timers, maximum_reconnect_attempts=2
        )

        await attempts.fail_scheduled_attempts(1)
        await attempts.succeed_scheduled_attempt()
        assert attempts.setup.channel.state == "connected"

        attempts.setup.clocks.monotonic += 59_999
        sockets[-1].disconnect()
        await attempts.fail_scheduled_attempts(1)

        attempts.assert_failed_once()
        assert attempts.reconnect_requests() == 3

    async def test_allows_the_full_maximum_again_after_sixty_seconds_connected(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sockets: list[FakeWebSocket],
        timers: FakeTimers,
    ) -> None:
        attempts = await ReconnectAttempts.connect_and_drop(
            monkeypatch, sockets, timers, maximum_reconnect_attempts=2
        )

        await attempts.fail_scheduled_attempts(1)
        await attempts.succeed_scheduled_attempt()
        assert attempts.setup.channel.state == "connected"

        attempts.setup.clocks.monotonic += 60_000
        sockets[-1].disconnect()
        await attempts.fail_scheduled_attempts(1)
        assert attempts.setup.channel.state == "reconnecting"

        await attempts.fail_scheduled_attempts(1)
        attempts.assert_failed_once()
        assert attempts.reconnect_requests() == 4
