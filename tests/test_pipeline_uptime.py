"""Tests for pipewatch.pipeline_uptime."""

from datetime import datetime, timezone, timedelta
from typing import List

import pytest

from pipewatch.history import RunRecord
from pipewatch.pipeline_uptime import (
    UptimeReport,
    compute_uptime,
    compute_uptime_map,
    _longest_gap,
)


def _make_record(status: str, offset_seconds: int = 0) -> RunRecord:
    base = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    started = base + timedelta(seconds=offset_seconds)
    return RunRecord(
        pipeline_name="pipe",
        status=status,
        started_at=started,
        finished_at=started + timedelta(seconds=10),
        duration_seconds=10.0,
        successes=1 if status == "success" else 0,
        failures=0 if status == "success" else 1,
    )


def test_compute_uptime_empty_records():
    report = compute_uptime("pipe", [])
    assert report.total_runs == 0
    assert report.uptime_pct == 0.0
    assert report.first_seen is None
    assert report.last_seen is None


def test_compute_uptime_all_success():
    records = [_make_record("success", i * 60) for i in range(5)]
    report = compute_uptime("pipe", records)
    assert report.total_runs == 5
    assert report.successful_runs == 5
    assert report.failed_runs == 0
    assert report.uptime_pct == 100.0


def test_compute_uptime_all_failure():
    records = [_make_record("failure", i * 60) for i in range(4)]
    report = compute_uptime("pipe", records)
    assert report.uptime_pct == 0.0
    assert report.failed_runs == 4


def test_compute_uptime_mixed():
    records = [
        _make_record("success", 0),
        _make_record("success", 60),
        _make_record("failure", 120),
        _make_record("success", 180),
    ]
    report = compute_uptime("pipe", records)
    assert report.total_runs == 4
    assert report.successful_runs == 3
    assert report.failed_runs == 1
    assert report.uptime_pct == 75.0


def test_compute_uptime_first_and_last_seen():
    records = [_make_record("success", i * 100) for i in range(3)]
    report = compute_uptime("pipe", records)
    assert report.first_seen is not None
    assert report.last_seen is not None
    assert report.last_seen > report.first_seen


def test_longest_gap_single_record():
    records = [_make_record("success", 0)]
    assert _longest_gap(records) == 0.0


def test_longest_gap_multiple_records():
    records = [
        _make_record("success", 0),
        _make_record("success", 60),
        _make_record("success", 300),
    ]
    gap = _longest_gap(records)
    assert gap == 240.0


def test_grade_a():
    report = compute_uptime("p", [_make_record("success", i) for i in range(100)])
    assert report.grade() == "A"


def test_grade_f():
    records = [_make_record("failure", i * 10) for i in range(10)]
    report = compute_uptime("p", records)
    assert report.grade() == "F"


def test_compute_uptime_map():
    records_map = {
        "alpha": [_make_record("success", i * 60) for i in range(3)],
        "beta": [_make_record("failure", i * 60) for i in range(3)],
    }
    result = compute_uptime_map(records_map)
    assert "alpha" in result
    assert "beta" in result
    assert result["alpha"].uptime_pct == 100.0
    assert result["beta"].uptime_pct == 0.0
