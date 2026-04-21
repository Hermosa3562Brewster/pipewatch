"""Tests for pipewatch.pipeline_stale."""
from datetime import datetime, timedelta

import pytest

from pipewatch.pipeline_stale import StalenessConfig, StalenessReport, StalenessTracker


@pytest.fixture()
def tracker() -> StalenessTracker:
    return StalenessTracker()


def _now() -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0)


# ---------------------------------------------------------------------------
# StalenessConfig
# ---------------------------------------------------------------------------

def test_staleness_config_max_age_timedelta():
    cfg = StalenessConfig(pipeline="p", max_age_seconds=300)
    assert cfg.max_age == timedelta(seconds=300)


# ---------------------------------------------------------------------------
# check returns None for unconfigured pipeline
# ---------------------------------------------------------------------------

def test_check_unconfigured_returns_none(tracker):
    assert tracker.check("unknown") is None


# ---------------------------------------------------------------------------
# Never-seen pipeline is stale
# ---------------------------------------------------------------------------

def test_check_never_seen_is_stale(tracker):
    tracker.configure("pipe", max_age_seconds=60)
    report = tracker.check("pipe", now=_now())
    assert report is not None
    assert report.is_stale is True
    assert report.last_seen is None
    assert report.age_seconds is None


# ---------------------------------------------------------------------------
# Fresh heartbeat is not stale
# ---------------------------------------------------------------------------

def test_heartbeat_within_threshold_not_stale(tracker):
    tracker.configure("pipe", max_age_seconds=60)
    now = _now()
    tracker.heartbeat("pipe", at=now - timedelta(seconds=30))
    report = tracker.check("pipe", now=now)
    assert report is not None
    assert report.is_stale is False
    assert abs(report.age_seconds - 30.0) < 0.01


# ---------------------------------------------------------------------------
# Heartbeat older than threshold is stale
# ---------------------------------------------------------------------------

def test_heartbeat_exceeds_threshold_is_stale(tracker):
    tracker.configure("pipe", max_age_seconds=60)
    now = _now()
    tracker.heartbeat("pipe", at=now - timedelta(seconds=90))
    report = tracker.check("pipe", now=now)
    assert report is not None
    assert report.is_stale is True


# ---------------------------------------------------------------------------
# check_all returns one report per configured pipeline
# ---------------------------------------------------------------------------

def test_check_all_returns_all_pipelines(tracker):
    tracker.configure("a", max_age_seconds=60)
    tracker.configure("b", max_age_seconds=120)
    reports = tracker.check_all(now=_now())
    names = {r.pipeline for r in reports}
    assert names == {"a", "b"}


# ---------------------------------------------------------------------------
# stale_pipelines filters correctly
# ---------------------------------------------------------------------------

def test_stale_pipelines_filters_fresh(tracker):
    now = _now()
    tracker.configure("fresh", max_age_seconds=60)
    tracker.configure("stale", max_age_seconds=60)
    tracker.heartbeat("fresh", at=now - timedelta(seconds=10))
    # "stale" never had a heartbeat
    stale = tracker.stale_pipelines(now=now)
    assert len(stale) == 1
    assert stale[0].pipeline == "stale"


# ---------------------------------------------------------------------------
# StalenessReport.summary
# ---------------------------------------------------------------------------

def test_summary_never_seen():
    r = StalenessReport(
        pipeline="pipe", last_seen=None, max_age_seconds=60,
        is_stale=True, age_seconds=None
    )
    assert "never seen" in r.summary()
    assert "pipe" in r.summary()


def test_summary_stale():
    r = StalenessReport(
        pipeline="pipe", last_seen=_now(), max_age_seconds=60,
        is_stale=True, age_seconds=90.0
    )
    assert "STALE" in r.summary()
    assert "90.0" in r.summary()


def test_summary_fresh():
    r = StalenessReport(
        pipeline="pipe", last_seen=_now(), max_age_seconds=60,
        is_stale=False, age_seconds=20.0
    )
    assert "fresh" in r.summary()
