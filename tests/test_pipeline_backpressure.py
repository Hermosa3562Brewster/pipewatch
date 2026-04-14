"""Tests for pipeline_backpressure and backpressure_display."""
from __future__ import annotations

import pytest

from pipewatch.pipeline_backpressure import (
    BackpressureReading,
    BackpressureStats,
    BackpressureTracker,
)
from pipewatch.backpressure_display import (
    render_backpressure_table,
    render_backpressure_summary,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tracker() -> BackpressureTracker:
    return BackpressureTracker(max_readings=10)


# ---------------------------------------------------------------------------
# BackpressureReading
# ---------------------------------------------------------------------------

def test_reading_to_dict_keys():
    t = BackpressureTracker()
    r = t.record("pipe-a", queue_depth=5, lag_seconds=12.3)
    d = r.to_dict()
    assert set(d.keys()) == {"pipeline", "queue_depth", "lag_seconds", "recorded_at"}


def test_reading_from_dict_roundtrip():
    t = BackpressureTracker()
    r = t.record("pipe-a", queue_depth=5, lag_seconds=12.3)
    restored = BackpressureReading.from_dict(r.to_dict())
    assert restored.pipeline == r.pipeline
    assert restored.queue_depth == r.queue_depth
    assert abs(restored.lag_seconds - r.lag_seconds) < 1e-6


# ---------------------------------------------------------------------------
# BackpressureTracker
# ---------------------------------------------------------------------------

def test_record_returns_reading(tracker):
    r = tracker.record("pipe-a", queue_depth=3, lag_seconds=5.0)
    assert isinstance(r, BackpressureReading)
    assert r.pipeline == "pipe-a"


def test_stats_empty_pipeline(tracker):
    s = tracker.stats("unknown")
    assert s.avg_queue_depth == 0.0
    assert s.max_lag_seconds == 0.0
    assert s.latest is None


def test_stats_avg_queue_depth(tracker):
    tracker.record("p", queue_depth=4, lag_seconds=1.0)
    tracker.record("p", queue_depth=8, lag_seconds=2.0)
    s = tracker.stats("p")
    assert s.avg_queue_depth == pytest.approx(6.0)


def test_stats_max_lag(tracker):
    tracker.record("p", queue_depth=1, lag_seconds=10.0)
    tracker.record("p", queue_depth=1, lag_seconds=30.0)
    assert tracker.stats("p").max_lag_seconds == pytest.approx(30.0)


def test_max_readings_cap(tracker):
    for i in range(15):
        tracker.record("p", queue_depth=i, lag_seconds=0.0)
    assert len(tracker.stats("p").readings) == 10


def test_is_under_pressure_true(tracker):
    # build a stable baseline then spike
    for _ in range(5):
        tracker.record("p", queue_depth=2, lag_seconds=0.0)
    tracker.record("p", queue_depth=20, lag_seconds=0.0)  # spike
    assert tracker.stats("p").is_under_pressure is True


def test_is_under_pressure_false_when_depth_drops(tracker):
    for _ in range(5):
        tracker.record("p", queue_depth=20, lag_seconds=0.0)
    tracker.record("p", queue_depth=1, lag_seconds=0.0)  # drop
    assert tracker.stats("p").is_under_pressure is False


def test_clear_removes_pipeline(tracker):
    tracker.record("p", queue_depth=1, lag_seconds=0.0)
    tracker.clear("p")
    assert "p" not in tracker.all_pipelines()


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def test_render_table_empty():
    t = BackpressureTracker()
    assert "No back-pressure" in render_backpressure_table(t)


def test_render_table_contains_pipeline_name(tracker):
    tracker.record("my-pipeline", queue_depth=3, lag_seconds=5.0)
    assert "my-pipeline" in render_backpressure_table(tracker)


def test_render_summary_shows_totals(tracker):
    tracker.record("a", queue_depth=1, lag_seconds=0.0)
    tracker.record("b", queue_depth=1, lag_seconds=0.0)
    out = render_backpressure_summary(tracker)
    assert "2" in out
