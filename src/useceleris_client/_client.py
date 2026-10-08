import random
from typing import Annotated

from pydantic import Field, TypeAdapter
from typing_extensions import TypedDict

from useceleris_client._channel import Channel, ChannelInternals
from useceleris_client._connection_url import validate_base_url
from useceleris_client._constants import (
    DEDUP_WINDOW_SIZE,
    DEFAULT_BASE_URL,
    DEFAULT_CONNECT_TIMEOUT_MS,
    DEFAULT_MAXIMUM_RECONNECT_ATTEMPTS,
    DEFAULT_PRESENCE_QUERY_TIMEOUT_MS,
    MAXIMUM_PENDING_COMMANDS,
    MAXIMUM_RECONNECT_ATTEMPTS_CEILING,
    MAXIMUM_TIMEOUT_MS,
)
from useceleris_client._credential_types import CredentialProvider
from useceleris_client._credentials import CHANNEL_REFERENCE, BaseUrl
from useceleris_client._errors import ConfigurationError
from useceleris_client._message_id import generate_message_id
from useceleris_client._parse_error import validate_input
from useceleris_client._reconnect import monotonic_now, wall_clock_now
from useceleris_client._timers import LOOP_TIMERS

_TimeoutMs = Annotated[int, Field(ge=1, le=MAXIMUM_TIMEOUT_MS)]


class _ClientOptions(TypedDict):
    base_url: BaseUrl
    allow_insecure_loopback: bool
    connect_timeout_ms: _TimeoutMs
    # None means each reconnect attempt keeps the connect deadline.
    reconnect_timeout_ms: _TimeoutMs | None
    presence_query_timeout_ms: _TimeoutMs
    publish_queue_size: Annotated[int, Field(ge=1)]
    deduplication_window_size: Annotated[int, Field(ge=1)]
    maximum_reconnect_attempts: Annotated[
        int, Field(ge=1, le=MAXIMUM_RECONNECT_ATTEMPTS_CEILING)
    ]


# end class _ClientOptions


_CLIENT_OPTIONS = TypeAdapter(_ClientOptions)


class Client:
    def __init__(
        self,
        *,
        credential_provider: CredentialProvider,
        base_url: str = DEFAULT_BASE_URL,
        allow_insecure_loopback: bool = False,
        connect_timeout_ms: int = DEFAULT_CONNECT_TIMEOUT_MS,
        reconnect_timeout_ms: int | None = None,
        presence_query_timeout_ms: int = DEFAULT_PRESENCE_QUERY_TIMEOUT_MS,
        publish_queue_size: int = MAXIMUM_PENDING_COMMANDS,
        deduplication_window_size: int = DEDUP_WINDOW_SIZE,
        maximum_reconnect_attempts: int = DEFAULT_MAXIMUM_RECONNECT_ATTEMPTS,
    ) -> None:
        if not callable(credential_provider):
            raise ConfigurationError(
                "Invalid client options. credential_provider: Must be callable."
            )

        options = validate_input(
            _CLIENT_OPTIONS,
            {
                "base_url": base_url,
                "allow_insecure_loopback": allow_insecure_loopback,
                "connect_timeout_ms": connect_timeout_ms,
                "reconnect_timeout_ms": reconnect_timeout_ms,
                "presence_query_timeout_ms": presence_query_timeout_ms,
                "publish_queue_size": publish_queue_size,
                "deduplication_window_size": deduplication_window_size,
                "maximum_reconnect_attempts": maximum_reconnect_attempts,
            },
            "client options",
        )
        validate_base_url(options["base_url"], options["allow_insecure_loopback"])

        self._options = options
        self._credential_provider = credential_provider

    # end method __init__

    def channel(self, reference: str) -> Channel:
        """Side-effect free: each call is a new channel, and a new socket once
        connected."""
        channel_reference = validate_input(
            CHANNEL_REFERENCE, reference, "channel reference"
        )
        reconnect_timeout_ms = self._options["reconnect_timeout_ms"]

        if reconnect_timeout_ms is None:
            reconnect_timeout_ms = self._options["connect_timeout_ms"]

        return Channel(
            ChannelInternals(
                base_url=self._options["base_url"],
                channel_reference=channel_reference,
                allow_insecure_loopback=self._options["allow_insecure_loopback"],
                connect_timeout_ms=self._options["connect_timeout_ms"],
                reconnect_timeout_ms=reconnect_timeout_ms,
                presence_query_timeout_ms=self._options["presence_query_timeout_ms"],
                publish_queue_size=self._options["publish_queue_size"],
                deduplication_window_size=self._options["deduplication_window_size"],
                maximum_reconnect_attempts=self._options["maximum_reconnect_attempts"],
                credential_provider=self._credential_provider,
                clock=monotonic_now,
                wall_clock=wall_clock_now,
                random=random.random,
                generate_message_id=generate_message_id,
                timers=LOOP_TIMERS,
            )
        )

    # end method channel


# end class Client


def create_client(
    *,
    credential_provider: CredentialProvider,
    base_url: str = DEFAULT_BASE_URL,
    allow_insecure_loopback: bool = False,
    connect_timeout_ms: int = DEFAULT_CONNECT_TIMEOUT_MS,
    reconnect_timeout_ms: int | None = None,
    presence_query_timeout_ms: int = DEFAULT_PRESENCE_QUERY_TIMEOUT_MS,
    publish_queue_size: int = MAXIMUM_PENDING_COMMANDS,
    deduplication_window_size: int = DEDUP_WINDOW_SIZE,
    maximum_reconnect_attempts: int = DEFAULT_MAXIMUM_RECONNECT_ATTEMPTS,
) -> Client:
    """Validates every option at once and raises ConfigurationError naming the
    field and the rule it broke (CONFIG-01). Timeouts are 1 ms to 15 minutes,
    counts at least 1. maximum_reconnect_attempts, 1 to 100 and 10 by default,
    is how many failed reconnect attempts end recovery in failed."""
    return Client(
        credential_provider=credential_provider,
        base_url=base_url,
        allow_insecure_loopback=allow_insecure_loopback,
        connect_timeout_ms=connect_timeout_ms,
        reconnect_timeout_ms=reconnect_timeout_ms,
        presence_query_timeout_ms=presence_query_timeout_ms,
        publish_queue_size=publish_queue_size,
        deduplication_window_size=deduplication_window_size,
        maximum_reconnect_attempts=maximum_reconnect_attempts,
    )


# end function create_client
