import asyncio
from collections.abc import Callable
from typing import Protocol


class Timer(Protocol):
    def cancel(self) -> None: ...

    # end method cancel


# end class Timer


class Timers(Protocol):
    """Every timer the package sets goes through one of these, so tests can
    drive time."""

    def call_later(self, delay_ms: float, callback: Callable[[], None]) -> Timer: ...

    # end method call_later


# end class Timers


class LoopTimers:
    def call_later(self, delay_ms: float, callback: Callable[[], None]) -> Timer:
        return asyncio.get_running_loop().call_later(delay_ms / 1000, callback)

    # end method call_later


# end class LoopTimers


LOOP_TIMERS = LoopTimers()
