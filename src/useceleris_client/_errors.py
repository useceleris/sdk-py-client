from typing import Literal, TypeAlias, Union

ConnectionErrorCode: TypeAlias = Literal[
    "Timeout",
    "Cancelled",
    "Transport",
    "NotConnected",
    "Backpressure",
    "OperationInProgress",
    "DeliveryUnknown",
]

# The server's own error types, one per RealtimeError variant (ERR-01). The
# type a ServerError carries stays a plain str, open to types a newer server
# may add.
ServerErrorType: TypeAlias = Literal[
    "ParserError",
    "SendError",
    "PermissionDeniedError",
    "RateLimitError",
    "MessageSizeLimitError",
    "InternalError",
]

# Whatever an error's type and sub type define it to carry, such as the
# segment a denial refers to.
ServerErrorResource: TypeAlias = Union[  # noqa: UP007 - a recursive alias
    str, int, tuple["ServerErrorResource", ...], None
]


class CelerisError(Exception):
    """Every error this package raises or reports. ``code`` names the category."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class ConfigurationError(CelerisError):
    def __init__(self, message: str = "Invalid client command.") -> None:
        super().__init__("Configuration", message)


class ProtocolError(CelerisError):
    def __init__(self, message: str, field: str, offset: int) -> None:
        # The field is a name this package chose and the offset a byte
        # position, so neither repeats what the server sent.
        super().__init__(
            "ProtocolError", f"{message} Field: {field}, byte offset {offset}."
        )
        self.field = field
        self.offset = offset


class CelerisConnectionError(CelerisError):
    # Prefixed so it does not shadow the built-in ConnectionError.
    code: ConnectionErrorCode

    def __init__(self, code: ConnectionErrorCode, message: str) -> None:
        super().__init__(code, message)


class ServerError(CelerisError):
    """An error frame from the server, every field exactly as sent."""

    def __init__(
        self,
        type: str,
        # The command the error answers, e.g. "PRES_LIST"; None when none.
        sub_type: str | None,
        message: str,
        resource: ServerErrorResource,
    ) -> None:
        super().__init__("Server", message)
        self.type = type
        self.sub_type = sub_type
        self.resource = resource
