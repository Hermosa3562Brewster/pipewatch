"""Tests for pipeline capacity tracking and display."""
import pytest
from pipewatch.pipeline_capacity import (
    CapacityConfig, CapacityReading, CapacityTracker, CapacityStatus,
)
from pipewatch.capacity_display import render_capacity_table, render_capacity_summary


@pytest.fixture
def tracker():
    return CapacityTracker()


def test_reading_to_dict_keys():
    t = CapacityTracker()
    r = t.record("pipe", 100, 128.0, 45.0)
    assert set(r.to_dict()) == {"pipeline", "queue_depth", "memory_mb", "cpu_pct", "recorded_at"}


def test_reading_from_dict_roundtrip():
    t = CapacityTracker()
    r = t.record("pipe", 200, 256.5, 70.0)
    r2 = CapacityReading.from_dict(r.to_dict())
    assert r2.pipeline == r.pipeline
    assert r2.queue_depth == r.queue_depth
    assert r2.memory_mb == r.memory_mb
    assert r2.cpu_pct == r.cpu_pct


def test_latest_returns_none_for_unknown(tracker):
    assert tracker.latest("unknown") is None


def test_record_returns_reading(tracker):
    r = tracker.record("p1", 50, 64.0, 30.0)
    assert isinstance(r, CapacityReading)
    assert r.pipeline == "p1"


def test_latest_returns_most_recent(tracker):
    tracker.record("p1", 10, 10.0, 5.0)
    tracker.record("p1", 20, 20.0, 10.0)
    assert tracker.latest("p1").queue_depth == 20


def test_status_returns_none_without_reading(tracker):
    assert tracker.status("nope") is None


def test_status_uses_default_config(tracker):
    tracker.record("p1", 500, 256.0, 45.0)
    st = tracker.status("p1")
    assert st.config.max_queue_depth == 1000


def test_status_uses_custom_config(tracker):
    tracker.configure("p1", CapacityConfig(max_queue_depth=200))
    tracker.record("p1", 100, 64.0, 20.0)
    st = tracker.status("p1")
    assert st.queue_pct == pytest.approx(50.0)


def test_is_overloaded_when_queue_full(tracker):
    tracker.configure("p1", CapacityConfig(max_queue_depth=100))
    tracker.record("p1", 100, 64.0, 20.0)
    assert tracker.status("p1").is_overloaded


def test_is_not_overloaded_within_limits(tracker):
    tracker.configure("p1", CapacityConfig(max_queue_depth=1000, max_memory_mb=512.0, max_cpu_pct=90.0))
    tracker.record("p1", 100, 64.0, 20.0)
    assert not tracker.status("p1").is_overloaded


def test_is_overloaded_when_cpu_exceeds(tracker):
    tracker.configure("p1", CapacityConfig(max_cpu_pct=80.0))
    tracker.record("p1", 10, 64.0, 95.0)
    assert tracker.status("p1").is_overloaded


def test_summary_contains_pipeline_name(tracker):
    tracker.record("pipe_a", 100, 128.0, 50.0)
    st = tracker.status("pipe_a")
    assert "pipe_a" in st.summary()


def test_render_table_empty(tracker):
    assert "No capacity data" in render_capacity_table(tracker)


def test_render_table_contains_pipeline_name(tracker):
    tracker.record("my_pipe", 100, 64.0, 30.0)
    assert "my_pipe" in render_capacity_table(tracker)


def test_render_summary_shows_total(tracker):
    tracker.record("p1", 10, 10.0, 5.0)
    tracker.record("p2", 20, 20.0, 10.0)
    out = render_capacity_summary(tracker)
    assert "2" in out


def test_render_summary_shows_overloaded_count(tracker):
    tracker.configure("p1", CapacityConfig(max_queue_depth=10))
    tracker.record("p1", 10, 10.0, 5.0)
    out = render_capacity_summary(tracker)
    assert "1" in out
