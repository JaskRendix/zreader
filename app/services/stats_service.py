from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class StatsSnapshot:
    """
    Immutable point-in-time snapshot of processing statistics.
    Safe to serialise and return in API responses.
    """

    uptime_seconds: float
    lines_total: int
    lines_valid: int
    lines_invalid: int
    lines_filtered_out: int
    lines_emitted: int
    throughput_lps: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class StatsService:
    """
    Lightweight in-memory statistics collector for NDJSON processing.

    All increment operations are single Python statements so they are
    effectively atomic under the GIL — safe for concurrent async tasks
    running in one event loop. Not safe across multiple processes or
    threads without an explicit lock.

    Usage
    -----
    Instantiate once at application startup (e.g. as a FastAPI dependency
    with lifespan scope) and inject into routes that need it. Call reset()
    between requests if per-request stats are needed, or keep a single
    long-lived instance for server-lifetime aggregates.
    """

    def __init__(self) -> None:
        self._start_time: float = time.monotonic()
        self.lines_total: int = 0
        self.lines_valid: int = 0
        self.lines_invalid: int = 0
        self.lines_filtered_out: int = 0
        self.lines_emitted: int = 0

    def inc_total(self, n: int = 1) -> None:
        self.lines_total += n

    def inc_valid(self, n: int = 1) -> None:
        self.lines_valid += n

    def inc_invalid(self, n: int = 1) -> None:
        self.lines_invalid += n

    def inc_filtered_out(self, n: int = 1) -> None:
        self.lines_filtered_out += n

    def inc_emitted(self, n: int = 1) -> None:
        self.lines_emitted += n

    def uptime(self) -> float:
        """Seconds since this StatsService instance was created."""
        return time.monotonic() - self._start_time

    def throughput_lps(self) -> float:
        """Emitted lines per second since instantiation."""
        elapsed = self.uptime()
        return self.lines_emitted / elapsed if elapsed > 0 else 0.0

    def snapshot(self) -> StatsSnapshot:
        """
        Return an immutable point-in-time snapshot of all metrics.
        Use this for API responses instead of exposing mutable state.
        """
        return StatsSnapshot(
            uptime_seconds=round(self.uptime(), 3),
            lines_total=self.lines_total,
            lines_valid=self.lines_valid,
            lines_invalid=self.lines_invalid,
            lines_filtered_out=self.lines_filtered_out,
            lines_emitted=self.lines_emitted,
            throughput_lps=round(self.throughput_lps(), 2),
        )

    def reset(self) -> None:
        """
        Reset all counters and restart the uptime clock.
        Useful for per-request stats when the service is reused.
        """
        self._start_time = time.monotonic()
        self.lines_total = 0
        self.lines_valid = 0
        self.lines_invalid = 0
        self.lines_filtered_out = 0
        self.lines_emitted = 0
