"""Tests for pipewatch.throttled_notifier."""

import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from pipewatch.notify import AlertEvent, InMemoryNotifier
from pipewatch.rate_limiter import RateLimiter, RateLimitEntry
from pipewatch.throttled_notifier import ThrottledNotifier


def _make_event(pipeline="etl", rule="high_errors", message="Error rate high"):
    return AlertEvent(pipeline_name=pipeline, rule_name=rule, message=message)


@pytest.fixture
def inner():
    return InMemoryNotifier()


@pytest.fixture
def throttled(inner):
    return ThrottledNotifier(inner, default_interval_seconds=60.0)


def test_first_event_is_forwarded(throttled, inner):
    event = _make_event()
    throttled(event)
    assert len(inner.events) == 1


def test_second_event_within_interval_is_suppressed(throttled, inner):
    event = _make_event()
    throttled(event)
    throttled(event)
    assert len(inner.events) == 1
    assert throttled.suppressed_count == 1


def test_event_forwarded_after_interval_expires(throttled, inner):
    event = _make_event()
    key = f"{event.pipeline_name}::{event.rule_name}"
    throttled(event)
    # Manually expire the entry
    throttled.rate_limiter._entries[key].last_fired = datetime.utcnow() - timedelta(seconds=120)
    throttled(event)
    assert len(inner.events) == 2


def test_different_rules_throttled_independently(throttled, inner):
    e1 = _make_event(rule="high_errors")
    e2 = _make_event(rule="low_throughput")
    throttled(e1)
    throttled(e2)
    assert len(inner.events) == 2


def test_different_pipelines_throttled_independently(throttled, inner):
    e1 = _make_event(pipeline="etl_a")
    e2 = _make_event(pipeline="etl_b")
    throttled(e1)
    throttled(e2)
    assert len(inner.events) == 2


def test_suppressed_count_accumulates(throttled, inner):
    event = _make_event()
    throttled(event)
    throttled(event)
    throttled(event)
    assert throttled.suppressed_count == 2


def test_set_interval_custom_zero_allows_repeated(inner):
    t = ThrottledNotifier(inner, default_interval_seconds=60.0)
    t.set_interval("etl", "high_errors", 0.0)
    event = _make_event()
    t(event)
    t(event)
    assert len(inner.events) == 2


def test_reset_clears_throttle_state(throttled, inner):
    event = _make_event()
    throttled(event)
    throttled.reset(event.pipeline_name, event.rule_name)
    throttled(event)
    assert len(inner.events) == 2


def test_mock_inner_called_on_allowed(inner):
    mock_fn = MagicMock()
    t = ThrottledNotifier(mock_fn, default_interval_seconds=60.0)
    event = _make_event()
    t(event)
    mock_fn.assert_called_once_with(event)


def test_mock_inner_not_called_when_throttled():
    mock_fn = MagicMock()
    t = ThrottledNotifier(mock_fn, default_interval_seconds=60.0)
    event = _make_event()
    t(event)
    t(event)
    assert mock_fn.call_count == 1
