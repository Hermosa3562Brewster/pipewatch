"""Tests for pipewatch.pipeline_soak and pipewatch.soak_display."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipewatch.pipeline_soak import SoakConfig, SoakReport, SoakTracker
from pipewatch.soak_display import render_soak_table, render_soak_summary


@pytest.fixture()
def tracker() -> SoakTracker:
    return SoakTracker()


def _now() -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0)


# --- SoakConfig ---

def test_soak_config_window_timedelta():
    cfg = SoakConfig(duration_seconds=300)
    assert cfg.window == timedelta(seconds=300)


# --- SoakTracker.check ---

def test_check_unconfigured_returns_none(tracker):
    assert tracker.check("pipe_x") is None


def test_check_no_records_fails(tracker):
    tracker.configure("p", duration_seconds=60, min_runs=2)
    report = tracker.check("p", now=_now())
    assert report is not None
    assert report.passed is False
    assert report.runs_in_window == 0


def test_check_all_successes_within_window_passes(tracker):
    now = _now()
    tracker.configure("p", duration_seconds=120, min_runs=3)
    for i in range(3):
        tracker.record("p", success=True, ts=now - timedelta(seconds=60 - i * 10))
    report = tracker.check("p", now=now)
    assert report.passed is True
    assert report.runs_in_window == 3
    assert report.failures_in_window == 0


def test_check_single_failure_fails(tracker):
    now = _now()
    tracker.configure("p", duration_seconds=120, min_runs=2)
    tracker.record("p", success=True, ts=now - timedelta(seconds=50))
    tracker.record("p", success=False, ts=now - timedelta(seconds=30))
    tracker.record("p", success=True, ts=now - timedelta(seconds=10))
    report = tracker.check("p", now=now)
    assert report.passed is False
    assert report.failures_in_window == 1


def test_records_outside_window_excluded(tracker):
    now = _now()
    tracker.configure("p", duration_seconds=60, min_runs=2)
    # old record outside window
    tracker.record("p", success=False, ts=now - timedelta(seconds=120))
    # fresh records inside window
    tracker.record("p", success=True, ts=now - timedelta(seconds=30))
    tracker.record("p", success=True, ts=now - timedelta(seconds=10))
    report = tracker.check("p", now=now)
    assert report.runs_in_window == 2
    assert report.failures_in_window == 0
    assert report.passed is True


def test_min_runs_not_met_fails(tracker):
    now = _now()
    tracker.configure("p", duration_seconds=120, min_runs=5)
    tracker.record("p", success=True, ts=now - timedelta(seconds=10))
    report = tracker.check("p", now=now)
    assert report.passed is False
    assert report.runs_in_window == 1


def test_error_rate_calculation(tracker):
    now = _now()
    tracker.configure("p", duration_seconds=120, min_runs=1)
    tracker.record("p", success=True, ts=now - timedelta(seconds=20))
    tracker.record("p", success=False, ts=now - timedelta(seconds=10))
    report = tracker.check("p", now=now)
    assert abs(report.error_rate - 0.5) < 1e-6


def test_check_all_returns_all_configured(tracker):
    tracker.configure("a", duration_seconds=60)
    tracker.configure("b", duration_seconds=60)
    result = tracker.check_all(now=_now())
    assert set(result.keys()) == {"a", "b"}


def test_summary_contains_pipeline_and_status(tracker):
    now = _now()
    tracker.configure("pipe", duration_seconds=60, min_runs=2)
    tracker.record("pipe", success=True, ts=now - timedelta(seconds=20))
    tracker.record("pipe", success=True, ts=now - timedelta(seconds=10))
    report = tracker.check("pipe", now=now)
    s = report.summary()
    assert "pipe" in s
    assert "PASS" in s


# --- Display ---

def test_render_soak_table_empty():
    result = render_soak_table({})
    assert "No soak" in result


def test_render_soak_table_contains_pipeline_name(tracker):
    now = _now()
    tracker.configure("my_pipeline", duration_seconds=60, min_runs=1)
    tracker.record("my_pipeline", success=True, ts=now - timedelta(seconds=10))
    reports = tracker.check_all(now=now)
    table = render_soak_table(reports)
    assert "my_pipeline" in table


def test_render_soak_summary_shows_totals(tracker):
    now = _now()
    tracker.configure("a", duration_seconds=60, min_runs=1)
    tracker.configure("b", duration_seconds=60, min_runs=1)
    tracker.record("a", success=True, ts=now - timedelta(seconds=5))
    reports = tracker.check_all(now=now)
    summary = render_soak_summary(reports)
    assert "Total" in summary
    assert "2" in summary


def test_render_soak_summary_empty():
    result = render_soak_summary({})
    assert "No soak" in result
