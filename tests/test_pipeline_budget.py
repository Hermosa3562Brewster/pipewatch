"""Tests for pipewatch.pipeline_budget."""
from datetime import datetime, timedelta

import pytest

from pipewatch.pipeline_budget import BudgetConfig, BudgetStatus, BudgetTracker


@pytest.fixture
def tracker() -> BudgetTracker:
    return BudgetTracker()


def _now() -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0)


def test_budget_config_window_timedelta():
    cfg = BudgetConfig(max_runs=10, window_seconds=3600)
    assert cfg.window() == timedelta(seconds=3600)


def test_status_returns_none_for_unregistered(tracker):
    assert tracker.status("pipe") is None


def test_is_exceeded_false_for_unregistered(tracker):
    assert tracker.is_exceeded("pipe") is False


def test_configure_registers_pipeline(tracker):
    tracker.configure("pipe", max_runs=5, window_seconds=60)
    s = tracker.status("pipe", _now())
    assert s is not None
    assert s.pipeline == "pipe"


def test_no_runs_not_exceeded(tracker):
    tracker.configure("pipe", max_runs=3, window_seconds=60)
    assert not tracker.is_exceeded("pipe", _now())


def test_runs_within_limit_not_exceeded(tracker):
    tracker.configure("pipe", max_runs=3, window_seconds=60)
    now = _now()
    tracker.record_run("pipe", at=now - timedelta(seconds=10))
    tracker.record_run("pipe", at=now - timedelta(seconds=5))
    assert not tracker.is_exceeded("pipe", now)


def test_runs_at_limit_is_exceeded(tracker):
    tracker.configure("pipe", max_runs=2, window_seconds=60)
    now = _now()
    tracker.record_run("pipe", at=now - timedelta(seconds=10))
    tracker.record_run("pipe", at=now - timedelta(seconds=5))
    assert tracker.is_exceeded("pipe", now)


def test_old_runs_outside_window_excluded(tracker):
    tracker.configure("pipe", max_runs=2, window_seconds=60)
    now = _now()
    tracker.record_run("pipe", at=now - timedelta(seconds=120))
    tracker.record_run("pipe", at=now - timedelta(seconds=90))
    assert not tracker.is_exceeded("pipe", now)


def test_remaining_decreases_with_runs(tracker):
    tracker.configure("pipe", max_runs=5, window_seconds=60)
    now = _now()
    tracker.record_run("pipe", at=now - timedelta(seconds=5))
    tracker.record_run("pipe", at=now - timedelta(seconds=3))
    s = tracker.status("pipe", now)
    assert s.remaining() == 3


def test_remaining_never_negative(tracker):
    tracker.configure("pipe", max_runs=1, window_seconds=60)
    now = _now()
    tracker.record_run("pipe", at=now - timedelta(seconds=5))
    tracker.record_run("pipe", at=now - timedelta(seconds=3))
    s = tracker.status("pipe", now)
    assert s.remaining() == 0


def test_utilisation_pct_zero_when_no_runs(tracker):
    tracker.configure("pipe", max_runs=10, window_seconds=60)
    s = tracker.status("pipe", _now())
    assert s.utilisation_pct() == 0.0


def test_utilisation_pct_capped_at_100(tracker):
    tracker.configure("pipe", max_runs=1, window_seconds=60)
    now = _now()
    for _ in range(5):
        tracker.record_run("pipe", at=now - timedelta(seconds=1))
    s = tracker.status("pipe", now)
    assert s.utilisation_pct() == 100.0


def test_summary_contains_pipeline_name(tracker):
    tracker.configure("alpha", max_runs=10, window_seconds=300)
    s = tracker.status("alpha", _now())
    assert "alpha" in s.summary()


def test_summary_shows_exceeded_state(tracker):
    tracker.configure("alpha", max_runs=1, window_seconds=60)
    now = _now()
    tracker.record_run("alpha", at=now - timedelta(seconds=5))
    tracker.record_run("alpha", at=now - timedelta(seconds=2))
    s = tracker.status("alpha", now)
    assert "EXCEEDED" in s.summary()


def test_all_statuses_returns_all_configured(tracker):
    tracker.configure("a", max_runs=5, window_seconds=60)
    tracker.configure("b", max_runs=3, window_seconds=120)
    statuses = tracker.all_statuses(_now())
    names = {s.pipeline for s in statuses}
    assert names == {"a", "b"}
