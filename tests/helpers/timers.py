import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field

from tests.helpers.tasks import flush


@dataclass(eq=False)
class FakeTimer:
    due: float
    sequence: int
    callback: Callable[[], None]
    owner: "FakeTimers" = field(repr=False)

    def cancel(self) -> None:
        if self in self.owner.pending:
            self.owner.pending.remove(self)


class FakeTimers:
    """Virtual time for the package's timers. advance() fires each due timer
    in order, letting the loop run after each one."""

    def __init__(self) -> None:
        self.now = 0.0
        self.pending: list[FakeTimer] = []
        self._sequence = 0

    def call_later(self, delay_ms: float, callback: Callable[[], None]) -> FakeTimer:
        self._sequence += 1
        timer = FakeTimer(self.now + max(delay_ms, 0), self._sequence, callback, self)
        self.pending.append(timer)
        return timer

    @property
    def count(self) -> int:
        return len(self.pending)

    async def advance(self, milliseconds: float) -> None:
        target = self.now + milliseconds

        while True:
            await flush()
            due = [timer for timer in self.pending if timer.due <= target]

            if not due:
                break

            timer = min(due, key=lambda candidate: (candidate.due, candidate.sequence))
            self.pending.remove(timer)
            self.now = timer.due
            timer.callback()

        self.now = target
        await flush()

    async def sleep(self, milliseconds: float) -> None:
        """For test doubles that need to take virtual time."""
        woken = asyncio.get_running_loop().create_future()
        self.call_later(milliseconds, lambda: woken.set_result(None))
        await woken
