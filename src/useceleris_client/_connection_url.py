import ipaddress
from urllib.parse import SplitResult, urlencode, urlsplit, urlunsplit

from useceleris_client._credential_types import Credentials
from useceleris_client._errors import ConfigurationError


def validate_base_url(base_url: str, allow_insecure_loopback: bool) -> SplitResult:
    url: SplitResult | None

    try:
        url = urlsplit(base_url)
        # Reading the port parses it, so an invalid one fails here.
        url.port  # noqa: B018
    except ValueError:
        url = None

    # Raised outside the handler: the parse error can quote the URL.
    if url is None or not url.scheme or not url.hostname:
        raise ConfigurationError(
            "Invalid connection URL. base_url is not an absolute URL."
        )

    if url.username is not None or url.password is not None:
        raise ConfigurationError(
            "Invalid connection URL. base_url must not contain a username or password."
        )

    if "?" in base_url or "#" in base_url:
        raise ConfigurationError(
            "Invalid connection URL. base_url must not contain a query string or "
            "fragment."
        )

    if not (
        url.scheme == "wss"
        or (
            url.scheme == "ws"
            and allow_insecure_loopback
            and _is_loopback(url.hostname)
        )
    ):
        raise ConfigurationError(
            "Invalid connection URL. base_url must use wss://, or ws:// for a "
            "loopback host when allow_insecure_loopback is True."
        )

    return url


def _is_loopback(hostname: str) -> bool:
    if hostname == "localhost":
        return True

    try:
        return ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        return False


def create_credential_url(
    base_url: SplitResult, channel_reference: str, credentials: Credentials
) -> str:
    path = f"{base_url.path.rstrip('/')}/channel/{channel_reference}"
    query = urlencode(
        {"payload": credentials.payload, "signature": credentials.signature}
    )

    return urlunsplit((base_url.scheme, base_url.netloc, path, query, ""))
