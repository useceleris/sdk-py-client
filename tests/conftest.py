import pytest

from tests.helpers.timers import FakeTimers
from tests.helpers.websocket import FakeWebSocket
from useceleris_client import _connection


@pytest.fixture
def sockets(monkeypatch: pytest.MonkeyPatch) -> list[FakeWebSocket]:
    """Every socket the package creates during the test, in order."""
    created: list[FakeWebSocket] = []

    class RecordedWebSocket(FakeWebSocket):
        def __init__(self, url: str) -> None:
            super().__init__(url)
            created.append(self)

    monkeypatch.setattr(_connection, "WebSocket", RecordedWebSocket)
    return created


@pytest.fixture
def timers() -> FakeTimers:
    return FakeTimers()
