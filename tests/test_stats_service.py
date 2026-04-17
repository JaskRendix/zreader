import time

from app.services.stats_service import StatsService


def test_stats_service_counts_and_throughput():
    s = StatsService()

    s.inc_total(3)
    s.inc_valid(2)
    s.inc_invalid(1)
    s.inc_filtered_out(1)
    s.inc_emitted(2)

    assert s.lines_total == 3
    assert s.lines_valid == 2
    assert s.lines_invalid == 1
    assert s.lines_filtered_out == 1
    assert s.lines_emitted == 2

    time.sleep(0.01)
    assert s.throughput_lps() > 0
