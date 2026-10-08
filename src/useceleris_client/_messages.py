from dataclasses import dataclass
from typing import TypeAlias

from useceleris_client._errors import ServerErrorResource


@dataclass(frozen=True)
class PresenceConnection:
    token_reference: str
    connection_id: str
    timestamp: int


# end class PresenceConnection


# What the decoder produces from one server frame, one class per command.


@dataclass(frozen=True)
class MessageFrame:
    """MSG: a message published to a segment."""

    token_reference: str
    segment_id: str
    message_id: str | None
    timestamp: int
    payload: bytes


# end class MessageFrame


@dataclass(frozen=True)
class NoticeFrame:
    """SERVER_MSG: untagged prose from the server."""

    timestamp: int
    payload: bytes


# end class NoticeFrame


@dataclass(frozen=True)
class PresenceListFrame:
    """PRES_LIST_RESPONSE: one page of a presence query."""

    segment_id: str
    request_id: str
    total: int
    per_page: int
    current_page: int
    from_: int
    to: int
    connections: tuple[PresenceConnection, ...]


# end class PresenceListFrame


@dataclass(frozen=True)
class PresenceNotifyFrame:
    """PRES_NOTIFY: one connection joining or leaving one segment."""

    segment_id: str
    token_reference: str
    connection_id: str
    joined: bool
    timestamp: int


# end class PresenceNotifyFrame


@dataclass(frozen=True)
class ErrorFrame:
    """-Err: the server's error frame (ERR-01)."""

    type: str
    sub_type: str | None
    message: bytes
    resource: ServerErrorResource


# end class ErrorFrame


@dataclass(frozen=True)
class ArrayFrame:
    messages: tuple["ServerMessage", ...]


# end class ArrayFrame


@dataclass(frozen=True)
class IgnoredFrame:
    """A command this version does not know. Skipped, never surfaced
    (DECODE-01)."""


# end class IgnoredFrame


ServerMessage: TypeAlias = (
    MessageFrame
    | NoticeFrame
    | PresenceListFrame
    | PresenceNotifyFrame
    | ErrorFrame
    | ArrayFrame
    | IgnoredFrame
)
