"""Celeris realtime client."""

from useceleris_client._channel import (
    Channel,
    ChannelError,
    ChannelEventHandler,
    ChannelState,
    MessageListener,
    MessageMetadata,
    PresenceEvent,
    PresencePage,
    RecoveryEvent,
    ServerNotice,
    Subscription,
)
from useceleris_client._client import Client, create_client
from useceleris_client._credential_types import (
    CredentialProvider,
    CredentialRequest,
    Credentials,
)
from useceleris_client._errors import (
    CelerisConnectionError,
    CelerisError,
    ConfigurationError,
    ConnectionErrorCode,
    ProtocolError,
    ServerError,
    ServerErrorResource,
    ServerErrorType,
)
from useceleris_client._messages import PresenceConnection
from useceleris_client._payload import (
    PayloadCodec,
    create_payload_codec,
    json_payload,
    read_json,
    read_text,
    text_payload,
)
from useceleris_client._segment import Segment

__all__ = [
    "CelerisConnectionError",
    "CelerisError",
    "Channel",
    "ChannelError",
    "ChannelEventHandler",
    "ChannelState",
    "Client",
    "ConfigurationError",
    "ConnectionErrorCode",
    "CredentialProvider",
    "CredentialRequest",
    "Credentials",
    "MessageListener",
    "MessageMetadata",
    "PayloadCodec",
    "PresenceConnection",
    "PresenceEvent",
    "PresencePage",
    "ProtocolError",
    "RecoveryEvent",
    "Segment",
    "ServerError",
    "ServerErrorResource",
    "ServerErrorType",
    "ServerNotice",
    "Subscription",
    "create_client",
    "create_payload_codec",
    "json_payload",
    "read_json",
    "read_text",
    "text_payload",
]
