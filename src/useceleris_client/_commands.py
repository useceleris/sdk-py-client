import re
from typing import Annotated, Literal, TypeAlias

from pydantic import AfterValidator, Field, TypeAdapter
from pydantic_core import PydanticCustomError
from typing_extensions import NotRequired, TypedDict

from useceleris_client._constants import (
    MAXIMUM_INTEGER32,
    MAXIMUM_PRESENCE_PAGE_SIZE,
)
from useceleris_client._parse_error import validate_input

# A Python str holds surrogate code points only unpaired, so any surrogate is
# ill-formed text that UTF-8 cannot encode.
_FORBIDDEN_IDENTIFIER_CHARACTERS = re.compile("[\r\n\ud800-\udfff]")


def _check_identifier(value: str) -> str:
    if not value:
        raise PydanticCustomError("identifier", "Must not be empty")

    if _FORBIDDEN_IDENTIFIER_CHARACTERS.search(value):
        raise PydanticCustomError(
            "identifier", "Must not contain CR, LF or unpaired UTF-16 surrogates"
        )

    return value


Identifier: TypeAlias = Annotated[str, AfterValidator(_check_identifier)]

IDENTIFIER = TypeAdapter(Identifier)


class PublishCommand(TypedDict):
    command: Literal["PUB"]
    segment_id: Identifier
    message_id: NotRequired[Identifier]
    payload: bytes


class InterestCommand(TypedDict):
    command: Literal["SUB", "UNSUB", "PRES_SUB", "PRES_UNSUB"]
    segment_id: Identifier


class PresenceListCommand(TypedDict):
    command: Literal["PRES_LIST"]
    segment_id: Identifier
    page: Annotated[int, Field(ge=1, le=MAXIMUM_INTEGER32)]
    per_page: Annotated[int, Field(ge=1, le=MAXIMUM_PRESENCE_PAGE_SIZE)]
    request_id: Identifier


ClientCommand: TypeAlias = PublishCommand | InterestCommand | PresenceListCommand


class _CommandEnvelope(TypedDict):
    command: Literal["PUB", "SUB", "UNSUB", "PRES_SUB", "PRES_UNSUB", "PRES_LIST"]


# The command name is checked on its own first, so a failure in one command's
# fields is reported against that command's layout alone.
_COMMAND_ENVELOPE = TypeAdapter(_CommandEnvelope)

_PUBLISH_COMMAND = TypeAdapter(PublishCommand)

_INTEREST_COMMAND = TypeAdapter(InterestCommand)

_PRESENCE_LIST_COMMAND = TypeAdapter(PresenceListCommand)


def parse_client_command(command: object) -> ClientCommand:
    """Validates once and strips unknown keys."""
    envelope = validate_input(_COMMAND_ENVELOPE, command, "command")

    if envelope["command"] == "PUB":
        return validate_input(_PUBLISH_COMMAND, command, "command")

    if envelope["command"] == "PRES_LIST":
        return validate_input(_PRESENCE_LIST_COMMAND, command, "command")

    return validate_input(_INTEREST_COMMAND, command, "command")
