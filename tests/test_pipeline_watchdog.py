"""Tests for pipewatch.pipeline_watchdog."""
from datetime import datetime, timedelta

import pytest

from pipewatch.pipeline_watchdog import PipelineWatchdog, WatchdogConfig


NOW = datetime(2024, 6, 1, 12, 0, 0)


@pytest.fixture
def dog() -> PipelineWatchdog:
    return PipelineWatchdog()


def test_configure_registers_pipeline(dog):
    dog.configure("etl", 300)
    assert "etl" in dog.configured_pipelines()


def test_check_unconfigured_returns_none(dog):
    assert dog.check("ghost", now=NOW) is None


def test_check_never_run_is_stale(dog):
    dog.configure("etl", 300)
    report = dog.check("etl", now=NOW)
    assert report is not None
    assert report.is_stale is True
    assert report.last_started_at is None


def test_heartbeat_within_threshold_not_stale(dog):
    dog.configure("etl", 300)
    dog.heartbeat("etl", at=NOW - timedelta(seconds=100))
    report = dog.check("etl", now=NOW)
    assert report.is_stale is False
    assert report.silence_seconds == pytest.approx(100, abs=1)


def test_heartbeat_outside_threshold_is_stale(dog):
    dog.configure("etl", 300)
    dog.heartbeat("etl", at=NOW - timedelta(seconds=400))
    report = dog.check("etl", now=NOW)
    assert report.is_stale is True


def test_check_all_returns_all_configured(dog):
    dog.configure("a", 60)
    dog.configure("b", 120)
    dog.heartbeat("a", at=NOW - timedelta(seconds=10))
    reports = dog.check_all(now=NOW)
    assert len(reports) == 2
    names = {r.pipeline for r in reports}
    assert names == {"a", "b"}


def test_stale_pipelines_filters_correctly(dog):
    dog.configure("ok", 300)
    dog.configure("stale", 300)
    dog.heartbeat("ok", at=NOW - timedelta(seconds=50))
    # "stale" never received a heartbeat
    stale = dog.stale_pipelines(now=NOW)
    assert len(stale) == 1
    assert stale[0].pipeline == "stale"


def test_summary_contains_pipeline_name(dog):
    dog.configure("etl", 300)
    dog.heartbeat("etl", at=NOW - timedelta(seconds=50))
    report = dog.check("etl", now=NOW)
    assert "etl" in report.summary()


def test_summary_stale_label(dog):
    dog.configure("etl", 300)
    report = dog.check("etl", now=NOW)
    assert "STALE" in report.summary()


def test_summary_ok_label(dog):
    dog.configure("etl", 300)
    dog.heartbeat("etl", at=NOW - timedelta(seconds=10))
    report = dog.check("etl", now=NOW)
    assert "OK" in report.summary()


def test_watchdog_config_max_silence_timedelta():
    cfg = WatchdogConfig(max_silence_seconds=120)
    assert cfg.max_silence.total_seconds() == 120
