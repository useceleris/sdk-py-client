import random
from typing import Annotated

from pydantic import Field, TypeAdapter
from typing_extensions import TypedDict

from useceleris_client._channel import Channel, ChannelInternals
from useceleris_client._connection_url import validate_base_url
from useceleris_client._constants import (
    DEFAULT_BASE_URL,
    DEFAULT_CONNECT_TIMEOUT_MS,
    DEFAULT_PRESENCE_QUERY_TIMEOUT_MS,
)
from useceleris_client._credential_types import CredentialProvider
from useceleris_client._credentials import CHANNEL_REFERENCE, BaseUrl
from useceleris_client._errors import ConfigurationError
from useceleris_client._message_id import generate_message_id
from useceleris_client._parse_error import validate_input
from useceleris_client._reconnect import monotonic_now, wall_clock_now
from useceleris_client._timers import LOOP_TIMERS


class _ClientOptions(TypedDict):
    base_url: BaseUrl
    allow_insecure_loopback: bool
    connect_timeout_ms: Annotated[int, Field(ge=1)]
    presence_query_timeout_ms: Annotated[int, Field(ge=1)]


_CLIENT_OPTIONS = TypeAdapter(_ClientOptions)


class Client:
    def __init__(
        self,
        *,
        credential_provider: CredentialProvider,
        base_url: str = DEFAULT_BASE_URL,
        allow_insecure_loopback: bool = False,
        connect_timeout_ms: int = DEFAULT_CONNECT_TIMEOUT_MS,
        presence_query_timeout_ms: int = DEFAULT_PRESENCE_QUERY_TIMEOUT_MS,
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
                "presence_query_timeout_ms": presence_query_timeout_ms,
            },
            "client options",
        )
        validate_base_url(options["base_url"], options["allow_insecure_loopback"])

        self._options = options
        self._credential_provider = credential_provider

    def channel(self, reference: str) -> Channel:
        """Side-effect free: each call is a new channel, and a new socket once
        connected."""
        channel_reference = validate_input(
            CHANNEL_REFERENCE, reference, "channel reference"
        )

        return Channel(
            ChannelInternals(
                base_url=self._options["base_url"],
                channel_reference=channel_reference,
                allow_insecure_loopback=self._options["allow_insecure_loopback"],
                connect_timeout_ms=self._options["connect_timeout_ms"],
                presence_query_timeout_ms=self._options["presence_query_timeout_ms"],
                credential_provider=self._credential_provider,
                clock=monotonic_now,
                wall_clock=wall_clock_now,
                random=random.random,
                generate_message_id=generate_message_id,
                timers=LOOP_TIMERS,
            )
        )


def create_client(
    *,
    credential_provider: CredentialProvider,
    base_url: str = DEFAULT_BASE_URL,
    allow_insecure_loopback: bool = False,
    connect_timeout_ms: int = DEFAULT_CONNECT_TIMEOUT_MS,
    presence_query_timeout_ms: int = DEFAULT_PRESENCE_QUERY_TIMEOUT_MS,
) -> Client:
    return Client(
        credential_provider=credential_provider,
        base_url=base_url,
        allow_insecure_loopback=allow_insecure_loopback,
        connect_timeout_ms=connect_timeout_ms,
        presence_query_timeout_ms=presence_query_timeout_ms,
    )
