"""Tests for pipewatch.pipeline_cooldown."""

from datetime import datetime, timedelta

import pytest

from pipewatch.pipeline_cooldown import CooldownConfig, CooldownManager


@pytest.fixture
def manager() -> CooldownManager:
    return CooldownManager()


def _now() -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0)


def test_cooldown_config_window_timedelta():
    cfg = CooldownConfig(duration_seconds=120.0)
    assert cfg.window == timedelta(seconds=120)


def test_status_returns_none_for_unconfigured(manager):
    assert manager.status("pipe_x") is None


def test_is_in_cooldown_false_for_unconfigured(manager):
    assert manager.is_in_cooldown("pipe_x") is False


def test_no_failure_recorded_not_in_cooldown(manager):
    manager.configure("alpha", duration_seconds=30.0)
    s = manager.status("alpha", now=_now())
    assert s is not None
    assert s.in_cooldown is False
    assert s.seconds_remaining == 0.0
    assert s.last_failure is None


def test_record_failure_puts_pipeline_in_cooldown(manager):
    manager.configure("alpha", duration_seconds=60.0)
    manager.record_failure("alpha", at=_now())
    s = manager.status("alpha", now=_now())
    assert s.in_cooldown is True
    assert s.seconds_remaining == pytest.approx(60.0)


def test_cooldown_expires_after_duration(manager):
    manager.configure("alpha", duration_seconds=60.0)
    failure_time = _now()
    manager.record_failure("alpha", at=failure_time)
    after = failure_time + timedelta(seconds=61)
    s = manager.status("alpha", now=after)
    assert s.in_cooldown is False
    assert s.seconds_remaining == 0.0


def test_seconds_remaining_decreases_over_time(manager):
    manager.configure("beta", duration_seconds=100.0)
    failure_time = _now()
    manager.record_failure("beta", at=failure_time)
    s = manager.status("beta", now=failure_time + timedelta(seconds=40))
    assert s.seconds_remaining == pytest.approx(60.0)


def test_eligible_at_is_set_correctly(manager):
    manager.configure("gamma", duration_seconds=30.0)
    failure_time = _now()
    manager.record_failure("gamma", at=failure_time)
    s = manager.status("gamma", now=failure_time)
    assert s.eligible_at == failure_time + timedelta(seconds=30)


def test_clear_removes_cooldown(manager):
    manager.configure("delta", duration_seconds=60.0)
    manager.record_failure("delta", at=_now())
    manager.clear("delta")
    s = manager.status("delta", now=_now())
    assert s.in_cooldown is False


def test_record_failure_auto_configures_with_defaults(manager):
    manager.record_failure("auto", at=_now())
    s = manager.status("auto", now=_now())
    assert s is not None
    assert s.in_cooldown is True


def test_summary_ready(manager):
    manager.configure("pipe", duration_seconds=10.0)
    s = manager.status("pipe", now=_now())
    assert "ready" in s.summary()


def test_summary_cooling_down(manager):
    manager.configure("pipe", duration_seconds=60.0)
    manager.record_failure("pipe", at=_now())
    s = manager.status("pipe", now=_now())
    assert "cooling down" in s.summary()
    assert "pipe" in s.summary()


def test_all_pipelines_returns_sorted(manager):
    manager.configure("zebra")
    manager.configure("alpha")
    manager.configure("mango")
    assert manager.all_pipelines() == ["alpha", "mango", "zebra"]
