import subprocess
import sys

import useceleris_client

# The public surface, written down in this one place.
PUBLIC_NAMES = [
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


def test_exports_exactly_the_public_surface() -> None:
    assert sorted(useceleris_client.__all__) == PUBLIC_NAMES

    for name in PUBLIC_NAMES:
        assert hasattr(useceleris_client, name), name


def test_every_error_shares_one_root() -> None:
    for error in [
        useceleris_client.ConfigurationError,
        useceleris_client.CelerisConnectionError,
        useceleris_client.ProtocolError,
        useceleris_client.ServerError,
    ]:
        assert issubclass(error, useceleris_client.CelerisError)


def test_credentials_never_appear_in_their_repr() -> None:
    credentials = useceleris_client.Credentials(
        payload="synthetic-payload", signature="synthetic-signature"
    )

    assert "synthetic" not in repr(credentials)


IMPORT_PROBE = """
import socket
import threading

created = []
original = socket.socket.__init__


def record(self, *args, **kwargs):
    created.append(args)
    original(self, *args, **kwargs)


socket.socket.__init__ = record

import useceleris_client

assert created == [], "importing opened a socket"
assert threading.active_count() == 1, "importing started a thread"
"""


def test_importing_opens_no_socket_and_starts_no_work() -> None:
    subprocess.run([sys.executable, "-c", IMPORT_PROBE], check=True)
