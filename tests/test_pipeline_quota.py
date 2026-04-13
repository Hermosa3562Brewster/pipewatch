"""Tests for pipewatch.pipeline_quota."""

from datetime import datetime, timedelta

import pytest

from pipewatch.pipeline_quota import QuotaConfig, QuotaTracker


@pytest.fixture()
def tracker() -> QuotaTracker:
    return QuotaTracker()


def _now() -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0)


def test_quota_config_window_timedelta():
    cfg = QuotaConfig(max_runs=10, window_seconds=300)
    assert cfg.window == timedelta(seconds=300)


def test_status_returns_none_for_unregistered_pipeline(tracker):
    assert tracker.status("unknown") is None


def test_is_exceeded_false_for_unregistered(tracker):
    assert tracker.is_exceeded("ghost") is False


def test_no_runs_not_exceeded(tracker):
    tracker.set_quota("pipe", max_runs=5)
    s = tracker.status("pipe", now=_now())
    assert s is not None
    assert s.runs_in_window == 0
    assert not s.exceeded
    assert s.remaining == 5


def test_runs_within_quota_not_exceeded(tracker):
    now = _now()
    tracker.set_quota("pipe", max_runs=3, window_seconds=60)
    for i in range(3):
        tracker.record_run("pipe", at=now - timedelta(seconds=i * 10))
    s = tracker.status("pipe", now=now)
    assert s.runs_in_window == 3
    assert not s.exceeded
    assert s.remaining == 0


def test_runs_exceed_quota(tracker):
    now = _now()
    tracker.set_quota("pipe", max_runs=2, window_seconds=60)
    for i in range(4):
        tracker.record_run("pipe", at=now - timedelta(seconds=i * 5))
    s = tracker.status("pipe", now=now)
    assert s.exceeded
    assert s.runs_in_window == 4
    assert s.remaining == 0


def test_old_runs_outside_window_are_pruned(tracker):
    now = _now()
    tracker.set_quota("pipe", max_runs=5, window_seconds=60)
    # 3 old runs outside the window
    for i in range(3):
        tracker.record_run("pipe", at=now - timedelta(seconds=120 + i * 10))
    # 1 recent run inside the window
    tracker.record_run("pipe", at=now - timedelta(seconds=30))
    s = tracker.status("pipe", now=now)
    assert s.runs_in_window == 1
    assert not s.exceeded


def test_utilisation_pct(tracker):
    now = _now()
    tracker.set_quota("pipe", max_runs=4, window_seconds=60)
    tracker.record_run("pipe", at=now - timedelta(seconds=10))
    tracker.record_run("pipe", at=now - timedelta(seconds=20))
    s = tracker.status("pipe", now=now)
    assert s.utilisation_pct == 50.0


def test_all_statuses_returns_one_per_registered_pipeline(tracker):
    tracker.set_quota("a", max_runs=10)
    tracker.set_quota("b", max_runs=5)
    statuses = tracker.all_statuses(now=_now())
    names = {s.pipeline for s in statuses}
    assert names == {"a", "b"}


def test_is_exceeded_true_when_over_quota(tracker):
    now = _now()
    tracker.set_quota("pipe", max_runs=1, window_seconds=60)
    tracker.record_run("pipe", at=now - timedelta(seconds=5))
    tracker.record_run("pipe", at=now - timedelta(seconds=10))
    assert tracker.is_exceeded("pipe", now=now)
