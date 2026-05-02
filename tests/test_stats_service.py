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


def test_snapshot_is_immutable():
    s = StatsService()
    s.inc_total(10)

    snap = s.snapshot()
    s.inc_total(5)

    assert snap.lines_total == 10
    assert s.lines_total == 15


def test_reset_resets_counters_and_uptime():
    s = StatsService()
    s.inc_total(5)
    time.sleep(0.01)

    before = s.uptime()
    s.reset()

    assert s.lines_total == 0
    assert s.lines_valid == 0
    assert s.lines_invalid == 0
    assert s.lines_filtered_out == 0
    assert s.lines_emitted == 0

    assert s.uptime() < before


def test_throughput_zero_elapsed():
    s = StatsService()
    s.inc_emitted(10)

    # No sleep → elapsed ~0
    assert s.throughput_lps() >= 0


def test_throughput_non_negative_and_stable():
    s = StatsService()
    s.inc_emitted(10)
    t1 = s.throughput_lps()

    time.sleep(0.02)
    s.inc_emitted(10)
    t2 = s.throughput_lps()

    # Throughput must always be >= 0
    assert t1 >= 0
    assert t2 >= 0

    # Throughput should not be NaN or inf
    assert t1 == t1
    assert t2 == t2


def test_throughput_decreases_over_time_if_rate_is_constant():
    s = StatsService()
    s.inc_emitted(10)
    t1 = s.throughput_lps()

    time.sleep(0.02)
    # Emit at the same rate → throughput should drop
    s.inc_emitted(10)
    t2 = s.throughput_lps()

    assert t2 < t1


def test_snapshot_rounding():
    s = StatsService()
    s.inc_emitted(100)
    time.sleep(0.01)

    snap = s.snapshot()

    # uptime rounded to 3 decimals
    assert len(str(snap.uptime_seconds).split(".")[-1]) <= 3

    # throughput rounded to 2 decimals
    assert len(str(snap.throughput_lps).split(".")[-1]) <= 2


def test_large_counters():
    s = StatsService()
    big = 10_000_000

    s.inc_total(big)
    s.inc_valid(big)
    s.inc_invalid(big)
    s.inc_filtered_out(big)
    s.inc_emitted(big)

    snap = s.snapshot()

    assert snap.lines_total == big
    assert snap.lines_valid == big
    assert snap.lines_invalid == big
    assert snap.lines_filtered_out == big
    assert snap.lines_emitted == big
