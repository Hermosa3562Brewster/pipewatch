"""Tests for pipewatch.pipeline_throttle."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipewatch.pipeline_throttle import PipelineThrottleManager, ThrottleConfig, ThrottleStatus


@pytest.fixture
def manager() -> PipelineThrottleManager:
    return PipelineThrottleManager()


def _now() -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0)


def test_unconfigured_pipeline_is_always_allowed(manager):
    assert manager.is_allowed("pipe_a") is True


def test_configured_pipeline_allowed_before_first_run(manager):
    manager.configure("pipe_a", min_interval_seconds=30)
    assert manager.is_allowed("pipe_a") is True


def test_pipeline_throttled_immediately_after_run(manager):
    now = _now()
    manager.configure("pipe_a", min_interval_seconds=60)
    manager.record_run("pipe_a", at=now)
    assert manager.is_allowed("pipe_a", now=now) is False


def test_pipeline_allowed_after_interval(manager):
    now = _now()
    manager.configure("pipe_a", min_interval_seconds=60)
    manager.record_run("pipe_a", at=now)
    later = now + timedelta(seconds=61)
    assert manager.is_allowed("pipe_a", now=later) is True


def test_pipeline_not_allowed_before_interval_expires(manager):
    now = _now()
    manager.configure("pipe_a", min_interval_seconds=60)
    manager.record_run("pipe_a", at=now)
    almost = now + timedelta(seconds=59)
    assert manager.is_allowed("pipe_a", now=almost) is False


def test_disabled_throttle_always_allows(manager):
    now = _now()
    manager.configure("pipe_a", min_interval_seconds=3600, enabled=False)
    manager.record_run("pipe_a", at=now)
    assert manager.is_allowed("pipe_a", now=now) is True


def test_seconds_until_allowed_zero_when_ready(manager):
    now = _now()
    manager.configure("pipe_a", min_interval_seconds=10)
    manager.record_run("pipe_a", at=now - timedelta(seconds=15))
    assert manager.seconds_until_allowed("pipe_a", now=now) == 0.0


def test_seconds_until_allowed_positive_when_throttled(manager):
    now = _now()
    manager.configure("pipe_a", min_interval_seconds=60)
    manager.record_run("pipe_a", at=now - timedelta(seconds=20))
    remaining = manager.seconds_until_allowed("pipe_a", now=now)
    assert abs(remaining - 40.0) < 0.01


def test_status_returns_throttle_status_object(manager):
    now = _now()
    manager.configure("pipe_a", min_interval_seconds=60)
    manager.record_run("pipe_a", at=now)
    st = manager.status("pipe_a", now=now)
    assert isinstance(st, ThrottleStatus)
    assert st.pipeline == "pipe_a"
    assert st.is_throttled is True


def test_status_summary_allowed(manager):
    now = _now()
    manager.configure("pipe_a", min_interval_seconds=10)
    manager.record_run("pipe_a", at=now - timedelta(seconds=20))
    st = manager.status("pipe_a", now=now)
    assert "allowed" in st.summary()


def test_status_summary_throttled(manager):
    now = _now()
    manager.configure("pipe_a", min_interval_seconds=60)
    manager.record_run("pipe_a", at=now)
    st = manager.status("pipe_a", now=now)
    assert "throttled" in st.summary()


def test_all_statuses_returns_all_pipelines(manager):
    now = _now()
    manager.configure("alpha", min_interval_seconds=30)
    manager.configure("beta", min_interval_seconds=60)
    manager.record_run("alpha", at=now)
    statuses = manager.all_statuses(now=now)
    assert "alpha" in statuses
    assert "beta" in statuses


def test_throttle_config_min_interval_timedelta():
    cfg = ThrottleConfig(min_interval_seconds=45)
    assert cfg.min_interval == timedelta(seconds=45)
