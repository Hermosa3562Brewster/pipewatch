"""Tests for pipewatch.rate_limiter."""

import time
import pytest
from unittest.mock import patch
from datetime import datetime, timedelta

from pipewatch.rate_limiter import RateLimiter, RateLimitEntry


@pytest.fixture
def limiter():
    return RateLimiter(default_interval_seconds=5.0)


def test_new_key_is_always_allowed(limiter):
    assert limiter.is_allowed("pipeline.errors") is True


def test_after_record_key_is_blocked(limiter):
    limiter.record("pipeline.errors")
    assert limiter.is_allowed("pipeline.errors") is False


def test_key_allowed_after_interval_passes(limiter):
    past = datetime.utcnow() - timedelta(seconds=10)
    limiter._entries["pipeline.errors"] = RateLimitEntry(last_fired=past, fire_count=1)
    assert limiter.is_allowed("pipeline.errors") is True


def test_try_acquire_returns_true_and_records(limiter):
    result = limiter.try_acquire("alert.key")
    assert result is True
    assert limiter.fire_count("alert.key") == 1


def test_try_acquire_returns_false_when_blocked(limiter):
    limiter.record("alert.key")
    result = limiter.try_acquire("alert.key")
    assert result is False
    assert limiter.fire_count("alert.key") == 1  # not incremented


def test_fire_count_increments_on_each_record(limiter):
    key = "test.key"
    for _ in range(3):
        past = datetime.utcnow() - timedelta(seconds=10)
        limiter._entries[key] = RateLimitEntry(last_fired=past, fire_count=limiter.fire_count(key))
        limiter.record(key)
    assert limiter.fire_count(key) == 3


def test_reset_clears_state(limiter):
    limiter.record("x")
    limiter.reset("x")
    assert limiter.is_allowed("x") is True
    assert limiter.fire_count("x") == 0


def test_custom_interval_overrides_default(limiter):
    limiter.set_interval("fast.key", 0.0)
    limiter.record("fast.key")
    assert limiter.is_allowed("fast.key") is True


def test_time_until_next_returns_none_for_unknown_key(limiter):
    assert limiter.time_until_next("unknown") is None


def test_time_until_next_returns_positive_when_blocked(limiter):
    limiter.record("blocked.key")
    remaining = limiter.time_until_next("blocked.key")
    assert remaining is not None
    assert remaining > 0


def test_time_until_next_returns_none_when_allowed(limiter):
    past = datetime.utcnow() - timedelta(seconds=100)
    limiter._entries["old.key"] = RateLimitEntry(last_fired=past, fire_count=1)
    assert limiter.time_until_next("old.key") is None


def test_independent_keys_do_not_interfere(limiter):
    limiter.record("key.a")
    assert limiter.is_allowed("key.b") is True


def test_reset_nonexistent_key_does_not_raise(limiter):
    """Resetting a key that was never recorded should be a no-op."""
    try:
        limiter.reset("never.recorded")
    except Exception as exc:
        pytest.fail(f"reset() raised unexpectedly: {exc}")
    assert limiter.is_allowed("never.recorded") is True
    assert limiter.fire_count("never.recorded") == 0
