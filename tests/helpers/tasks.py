import asyncio
from typing import Any


async def flush(times: int = 20) -> None:
    """Lets every task that is ready run, and the tasks they wake."""
    for _ in range(times):
        await asyncio.sleep(0)


async def failure_of(awaitable: "asyncio.Future[Any]") -> BaseException:
    try:
        await awaitable
    except BaseException as error:
        return error

    raise AssertionError("Expected a failure")
