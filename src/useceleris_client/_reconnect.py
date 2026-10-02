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


def compute_replay_lookback_ms(elapsed_outage_ms: float) -> int:
    return min(math.ceil(elapsed_outage_ms) + REPLAY_OVERLAP_MS, REPLAY_LOOKBACK_CAP_MS)


def monotonic_now() -> float:
    return time.monotonic() * 1000


def wall_clock_now() -> int:
    return time.time_ns() // 1_000_000
