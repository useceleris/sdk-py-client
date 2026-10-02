import re
from typing import Annotated, Literal, TypeAlias

from pydantic import AfterValidator, Field, TypeAdapter
from pydantic_core import PydanticCustomError
from typing_extensions import NotRequired, TypedDict

from useceleris_client._constants import (
    MAXIMUM_CHANNEL_REFERENCE_LENGTH,
    REPLAY_LOOKBACK_CAP_MS,
)
from useceleris_client._credential_types import Credentials
from useceleris_client._errors import ConfigurationError
from useceleris_client._parse_error import validate_input

_SURROGATE = re.compile("[\ud800-\udfff]")

_CHANNEL_REFERENCE = re.compile("[A-Za-z0-9_-]+")


def _check_credential_value(value: str) -> str:
    if not value:
        raise PydanticCustomError("credential", "Must not be empty")

    if _SURROGATE.search(value):
        raise PydanticCustomError(
            "credential", "Must not contain unpaired UTF-16 surrogates"
        )

    return value


def _check_channel_reference(value: str) -> str:
    if not value:
        raise PydanticCustomError("channel_reference", "Must not be empty")

    if len(value) > MAXIMUM_CHANNEL_REFERENCE_LENGTH:
        raise PydanticCustomError(
            "channel_reference",
            f"Must be at most {MAXIMUM_CHANNEL_REFERENCE_LENGTH} characters",
        )

    if _CHANNEL_REFERENCE.fullmatch(value) is None:
        raise PydanticCustomError(
            "channel_reference",
            "Must contain only ASCII letters, digits, hyphens (-) or underscores (_)",
        )

    return value


def _check_base_url(value: str) -> str:
    if not value:
        raise PydanticCustomError("base_url", "Must not be empty")

    return value


ChannelReference: TypeAlias = Annotated[str, AfterValidator(_check_channel_reference)]

CHANNEL_REFERENCE = TypeAdapter(ChannelReference)

BaseUrl: TypeAlias = Annotated[str, AfterValidator(_check_base_url)]


class _CredentialFields(TypedDict):
    payload: Annotated[str, AfterValidator(_check_credential_value)]
    signature: Annotated[str, AfterValidator(_check_credential_value)]


_CREDENTIAL_FIELDS = TypeAdapter(_CredentialFields)


def get_safe_parsed_credentials(credentials: object) -> Credentials:
    if not isinstance(credentials, Credentials):
        raise ConfigurationError(
            "Invalid credentials. The provider must return a Credentials instance."
        )

    fields = validate_input(
        _CREDENTIAL_FIELDS,
        {"payload": credentials.payload, "signature": credentials.signature},
        "credentials",
    )
    return Credentials(payload=fields["payload"], signature=fields["signature"])


class InitialRecovery(TypedDict):
    reason: Literal["initial"]


class ReconnectRecovery(TypedDict):
    reason: Literal["reconnect"]
    disconnected_at: Annotated[int, Field(ge=0)]
    replay_lookback_ms: Annotated[int, Field(ge=0, le=REPLAY_LOOKBACK_CAP_MS)]


Recovery: TypeAlias = Annotated[
    InitialRecovery | ReconnectRecovery, Field(discriminator="reason")
]


class ConnectionConfiguration(TypedDict):
    base_url: BaseUrl
    channel_reference: ChannelReference
    allow_insecure_loopback: NotRequired[bool]
    recovery: NotRequired[Recovery]


CONNECTION_CONFIGURATION = TypeAdapter(ConnectionConfiguration)


def get_safe_parsed_connection_configuration(
    configuration: ConnectionConfiguration,
) -> ConnectionConfiguration:
    return validate_input(
        CONNECTION_CONFIGURATION, configuration, "connection configuration"
    )
