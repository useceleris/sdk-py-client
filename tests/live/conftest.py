import asyncio
import os
import socket
from collections.abc import AsyncIterator
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from tests.live.helpers import DroppingProxy, websocket_url
from useceleris_client import Channel

ENVIRONMENT_FILE = Path(__file__).resolve().parents[2] / ".env"

REQUIRED = ["CELERIS_WS_URL", "CELERIS_CLIENT_ID", "CELERIS_SIGNING_SECRET"]


def load_environment() -> None:
    """Reads KEY=VALUE lines from the repository's .env, if there is one,
    without replacing variables already set."""
    if not ENVIRONMENT_FILE.exists():
        return

    for line in ENVIRONMENT_FILE.read_text().splitlines():
        line = line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


@pytest.fixture(scope="session", autouse=True)
def realtime() -> str:
    """Fails the run at once when the target cannot be used, instead of
    letting every test wait out its connect deadline."""
    load_environment()
    missing = [name for name in REQUIRED if not os.environ.get(name)]

    if missing:
        pytest.exit(
            f"{', '.join(missing)} not set. Copy the three CELERIS_* values into "
            "a local .env (gitignored): CELERIS_WS_URL, CELERIS_CLIENT_ID and "
            "CELERIS_SIGNING_SECRET. Any stack works.",
            returncode=1,
        )

    url = urlsplit(os.environ["CELERIS_WS_URL"])
    host = url.hostname or "localhost"
    port = url.port or (443 if url.scheme == "wss" else 80)

    try:
        socket.create_connection((host, port), timeout=5).close()
    except OSError:
        pytest.exit(
            f"The realtime service at {url.scheme}://{host}:{port} is not "
            "reachable. Start the stack, or point CELERIS_WS_URL elsewhere.",
            returncode=1,
        )

    return os.environ["CELERIS_WS_URL"]


@pytest.fixture
async def proxy() -> AsyncIterator[DroppingProxy]:
    """A dropping proxy in front of CELERIS_WS_URL, at its own url."""
    target = urlsplit(websocket_url())
    dropping = DroppingProxy(target.hostname or "localhost", target.port or 80)
    server = await asyncio.start_server(dropping.link, "127.0.0.1", 0)
    dropping.url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"

    async with server:
        yield dropping
        dropping.stall_upstream(False)
        dropping.drop_all()


@pytest.fixture
async def opened() -> AsyncIterator[list[Channel]]:
    """Every channel a test opens, closed even when the test fails."""
    channels: list[Channel] = []
    yield channels

    for channel in channels:
        await channel.close()
