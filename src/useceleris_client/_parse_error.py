from typing import TypeVar

from pydantic import TypeAdapter, ValidationError

from useceleris_client._errors import ConfigurationError

ParsedValue = TypeVar("ParsedValue")


def validate_input(
    adapter: TypeAdapter[ParsedValue], value: object, subject: str
) -> ParsedValue:
    """Validates strictly: nothing is coerced, so True is not an int and 1.0
    is not an int."""
    try:
        return adapter.validate_python(value, strict=True)
    except ValidationError as error:
        description = describe_parse_error(subject, error)

    # Raised outside the handler, so the validation error, which holds the
    # input, is not chained to it as context.
    raise ConfigurationError(description)


def describe_parse_error(subject: str, error: ValidationError) -> str:
    """Names every field that failed and the rule it broke. Pydantic's messages
    for the rules used here state the expected type, format or bound, never
    the value."""
    failures = []

    for issue in error.errors(
        include_url=False, include_input=False, include_context=False
    ):
        path = _describe_path(issue["loc"])
        rule = issue["msg"]
        failures.append(f"{path}: {rule}." if path else f"{rule}.")

    return f"Invalid {subject}. {' '.join(failures)}"


def _describe_path(location: tuple[int | str, ...]) -> str:
    path = ""

    for key in location:
        if isinstance(key, int):
            path += f"[{key}]"
        else:
            path += f".{key}" if path else key

    return path
