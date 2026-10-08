import math
import time
from collections.abc import Callable

from useceleris_client._constants import (
    REPLAY_LOOKBACK_CAP_MS,
    REPLAY_OVERLAP_MS,
    RETRY_BASE_DELAY_MS,
    RETRY_DELAY_CAP_MS,
)


def compute_retry_delay_ms(retry_index: int, random: Callable[[], float]) -> float:
    ceiling: float = min(RETRY_DELAY_CAP_MS, RETRY_BASE_DELAY_MS * 2**retry_index)

    return random() * ceiling


# end function compute_retry_delay_ms


def compute_replay_lookback_ms(elapsed_outage_ms: float) -> int:
    return min(math.ceil(elapsed_outage_ms) + REPLAY_OVERLAP_MS, REPLAY_LOOKBACK_CAP_MS)


# end function compute_replay_lookback_ms


def monotonic_now() -> float:
    return time.monotonic() * 1000


# end function monotonic_now


def wall_clock_now() -> int:
    return time.time_ns() // 1_000_000


# end function wall_clock_now
