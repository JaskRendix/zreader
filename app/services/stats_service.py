from __future__ import annotations

import time


class StatsService:
    """
    Lightweight in-memory statistics collector for NDJSON processing.
    Thread-safe enough for async workloads due to GIL and atomic ops.
    """

    def __init__(self) -> None:
        self.start_time = time.time()

        self.lines_total = 0
        self.lines_valid = 0
        self.lines_invalid = 0
        self.lines_filtered_out = 0
        self.lines_emitted = 0

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
        return time.time() - self.start_time

    def throughput_lps(self) -> float:
        """
        Lines per second (emitted).
        """
        elapsed = self.uptime()
        if elapsed <= 0:
            return 0.0
        return self.lines_emitted / elapsed
