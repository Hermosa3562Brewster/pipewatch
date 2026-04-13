"""Tests for pipewatch.pipeline_throughput."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest

from pipewatch.history import RunRecord
from pipewatch.pipeline_throughput import (
    ThroughputStats,
    compute_throughput,
    compute_throughput_map,
)


def _make_record(
    status: str = "success",
    duration: Optional[float] = 10.0,
    records: Optional[int] = 100,
) -> RunRecord:
    return RunRecord(
        pipeline_name="test",
        status=status,
        started_at=datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat(),
        finished_at=datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat(),
        duration_seconds=duration,
        records_processed=records,
        error_message=None,
    )


def test_empty_records_returns_zero_stats():
    stats = compute_throughput("pipe", [])
    assert stats.pipeline_name == "pipe"
    assert stats.total_records == 0
    assert stats.avg_rps == 0.0
    assert stats.run_count == 0


def test_single_successful_run():
    stats = compute_throughput("pipe", [_make_record(duration=10.0, records=200)])
    assert stats.run_count == 1
    assert stats.total_records == 200
    assert stats.avg_rps == pytest.approx(20.0)


def test_failed_runs_are_excluded():
    records = [
        _make_record(status="failure", duration=5.0, records=50),
        _make_record(status="success", duration=10.0, records=100),
    ]
    stats = compute_throughput("pipe", records)
    assert stats.run_count == 1
    assert stats.total_records == 100


def test_zero_duration_run_excluded():
    stats = compute_throughput("pipe", [_make_record(duration=0.0, records=100)])
    assert stats.run_count == 0
    assert stats.avg_rps == 0.0


def test_zero_records_run_excluded():
    stats = compute_throughput("pipe", [_make_record(duration=10.0, records=0)])
    assert stats.run_count == 0


def test_min_max_rps():
    records = [
        _make_record(duration=10.0, records=100),  # 10 rps
        _make_record(duration=5.0, records=100),   # 20 rps
        _make_record(duration=20.0, records=100),  # 5 rps
    ]
    stats = compute_throughput("pipe", records)
    assert stats.min_rps == pytest.approx(5.0)
    assert stats.max_rps == pytest.approx(20.0)


def test_avg_records_per_run():
    records = [
        _make_record(duration=10.0, records=100),
        _make_record(duration=10.0, records=300),
    ]
    stats = compute_throughput("pipe", records)
    assert stats.avg_records_per_run == pytest.approx(200.0)


def test_p95_rps_single_sample():
    stats = compute_throughput("pipe", [_make_record(duration=10.0, records=100)])
    assert stats.p95_rps == pytest.approx(10.0)


def test_p95_rps_empty():
    stats = compute_throughput("pipe", [])
    assert stats.p95_rps == 0.0


def test_compute_throughput_map():
    records_map = {
        "alpha": [_make_record(duration=10.0, records=100)],
        "beta": [_make_record(duration=5.0, records=50)],
    }
    result = compute_throughput_map(records_map)
    assert set(result.keys()) == {"alpha", "beta"}
    assert result["alpha"].avg_rps == pytest.approx(10.0)
    assert result["beta"].avg_rps == pytest.approx(10.0)
