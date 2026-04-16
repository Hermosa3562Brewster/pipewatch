"""Tests for pipeline concurrency tracking."""
import pytest
from pipewatch.pipeline_concurrency import ConcurrencyTracker, ConcurrencyConfig, ConcurrencyStatus


@pytest.fixture
def tracker():
    return ConcurrencyTracker()


def test_configure_registers_pipeline(tracker):
    tracker.configure("etl", max_slots=3)
    status = tracker.status("etl")
    assert status is not None
    assert status.max_slots == 3


def test_status_returns_none_for_unknown(tracker):
    assert tracker.status("ghost") is None


def test_acquire_unconfigured_always_allowed(tracker):
    assert tracker.acquire("unknown") is True


def test_acquire_within_limit_returns_true(tracker):
    tracker.configure("etl", max_slots=2)
    assert tracker.acquire("etl") is True
    assert tracker.acquire("etl") is True


def test_acquire_beyond_limit_returns_false(tracker):
    tracker.configure("etl", max_slots=1)
    tracker.acquire("etl")
    assert tracker.acquire("etl") is False


def test_rejected_count_increments(tracker):
    tracker.configure("etl", max_slots=1)
    tracker.acquire("etl")
    tracker.acquire("etl")
    status = tracker.status("etl")
    assert status.total_rejected == 1


def test_acquired_count_increments(tracker):
    tracker.configure("etl", max_slots=2)
    tracker.acquire("etl")
    tracker.acquire("etl")
    status = tracker.status("etl")
    assert status.total_acquired == 2


def test_release_decrements_active(tracker):
    tracker.configure("etl", max_slots=1)
    tracker.acquire("etl")
    tracker.release("etl")
    status = tracker.status("etl")
    assert status.active_slots == 0


def test_release_below_zero_is_safe(tracker):
    tracker.configure("etl", max_slots=1)
    tracker.release("etl")  # never acquired
    assert tracker.status("etl").active_slots == 0


def test_is_saturated_when_full(tracker):
    tracker.configure("etl", max_slots=1)
    tracker.acquire("etl")
    assert tracker.status("etl").is_saturated is True


def test_is_not_saturated_when_slot_free(tracker):
    tracker.configure("etl", max_slots=2)
    tracker.acquire("etl")
    assert tracker.status("etl").is_saturated is False


def test_utilisation_pct(tracker):
    tracker.configure("etl", max_slots=4)
    tracker.acquire("etl")
    tracker.acquire("etl")
    assert tracker.status("etl").utilisation_pct == 50.0


def test_summary_contains_pipeline_name(tracker):
    tracker.configure("etl", max_slots=2)
    tracker.acquire("etl")
    assert "etl" in tracker.status("etl").summary()


def test_all_statuses_returns_all(tracker):
    tracker.configure("a", max_slots=1)
    tracker.configure("b", max_slots=2)
    statuses = tracker.all_statuses()
    assert set(statuses.keys()) == {"a", "b"}
