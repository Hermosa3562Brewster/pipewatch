"""Tests for pipewatch.pipeline_window."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipewatch.history import RunRecord
from pipewatch.pipeline_window import WindowTracker, WindowStats


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_record(
    name: str,
    status: str = "success",
    seconds_ago: float = 10.0,
    duration: float = 1.0,
) -> RunRecord:
    started = datetime.utcnow() - timedelta(seconds=seconds_ago)
    finished = started + timedelta(seconds=duration)
    return RunRecord(
        pipeline_name=name,
        status=status,
        started_at=started,
        finished_at=finished,
        duration_seconds=duration,
    )


@pytest.fixture
def tracker() -> WindowTracker:
    return WindowTracker(window_seconds=60)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_stats_returns_none_for_unknown_pipeline(tracker):
    assert tracker.stats("ghost") is None


def test_record_and_retrieve(tracker):
    tracker.record(_make_record("pipe_a"))
    stats = tracker.stats("pipe_a")
    assert stats is not None
    assert stats.total_runs == 1


def test_success_and_failure_counts(tracker):
    tracker.record(_make_record("p", status="success"))
    tracker.record(_make_record("p", status="success"))
    tracker.record(_make_record("p", status="failure"))
    stats = tracker.stats("p")
    assert stats.successes == 2
    assert stats.failures == 1


def test_error_rate_calculation(tracker):
    for _ in range(3):
        tracker.record(_make_record("p", status="success"))
    tracker.record(_make_record("p", status="failure"))
    stats = tracker.stats("p")
    assert abs(stats.error_rate - 0.25) < 1e-9


def test_avg_duration(tracker):
    tracker.record(_make_record("p", duration=2.0))
    tracker.record(_make_record("p", duration=4.0))
    stats = tracker.stats("p")
    assert abs(stats.avg_duration - 3.0) < 1e-9


def test_old_records_are_evicted():
    tracker = WindowTracker(window_seconds=30)
    old = _make_record("p", seconds_ago=120, duration=1.0)
    recent = _make_record("p", seconds_ago=5, duration=1.0)
    tracker.record(old)
    tracker.record(recent)
    stats = tracker.stats("p")
    assert stats.total_runs == 1


def test_all_stats_returns_all_pipelines(tracker):
    tracker.record(_make_record("alpha"))
    tracker.record(_make_record("beta"))
    names = {s.pipeline for s in tracker.all_stats()}
    assert names == {"alpha", "beta"}


def test_summary_contains_pipeline_name(tracker):
    tracker.record(_make_record("my_pipe"))
    stats = tracker.stats("my_pipe")
    assert "my_pipe" in stats.summary()


def test_pipelines_lists_tracked_names(tracker):
    tracker.record(_make_record("x"))
    tracker.record(_make_record("y"))
    assert set(tracker.pipelines()) == {"x", "y"}
