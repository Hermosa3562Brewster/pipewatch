"""Tests for pipeline_circuit_breaker module."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipewatch.pipeline_circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerRegistry,
    STATE_CLOSED,
    STATE_OPEN,
    STATE_HALF_OPEN,
)


@pytest.fixture
def cb() -> CircuitBreaker:
    return CircuitBreaker(pipeline="etl", failure_threshold=3, recovery_timeout=60)


def test_initial_state_is_closed(cb):
    assert cb.state == STATE_CLOSED


def test_allow_run_when_closed(cb):
    assert cb.allow_run() is True


def test_failure_increments_count(cb):
    cb.record_failure()
    assert cb.failure_count == 1


def test_trips_after_threshold(cb):
    for _ in range(3):
        cb.record_failure()
    assert cb.state == STATE_OPEN


def test_tripped_at_set_when_open(cb):
    for _ in range(3):
        cb.record_failure()
    assert cb.tripped_at is not None


def test_open_circuit_blocks_run(cb):
    for _ in range(3):
        cb.record_failure()
    assert cb.allow_run() is False


def test_open_transitions_to_half_open_after_timeout(cb):
    for _ in range(3):
        cb.record_failure()
    cb.tripped_at = datetime.utcnow() - timedelta(seconds=61)
    assert cb.allow_run() is True
    assert cb.state == STATE_HALF_OPEN


def test_half_open_success_closes_circuit(cb):
    cb.state = STATE_HALF_OPEN
    cb.record_success()
    assert cb.state == STATE_CLOSED
    assert cb.failure_count == 0


def test_half_open_failure_reopens_circuit(cb):
    cb.state = STATE_HALF_OPEN
    cb.record_failure()
    assert cb.state == STATE_OPEN


def test_success_in_closed_resets_count(cb):
    cb.record_failure()
    cb.record_failure()
    cb.record_success()
    assert cb.failure_count == 0


def test_status_summary_contains_pipeline(cb):
    assert "etl" in cb.status_summary()


def test_status_summary_contains_state(cb):
    assert STATE_CLOSED in cb.status_summary()


# --- Registry ---

@pytest.fixture
def registry() -> CircuitBreakerRegistry:
    return CircuitBreakerRegistry(failure_threshold=2, recovery_timeout=30)


def test_registry_creates_breaker_on_first_get(registry):
    cb = registry.get("pipe_a")
    assert cb.pipeline == "pipe_a"


def test_registry_returns_same_instance(registry):
    assert registry.get("pipe_a") is registry.get("pipe_a")


def test_registry_all_returns_all(registry):
    registry.get("pipe_a")
    registry.get("pipe_b")
    assert set(registry.all().keys()) == {"pipe_a", "pipe_b"}


def test_registry_reset_closes_open_breaker(registry):
    cb = registry.get("pipe_a")
    cb.state = STATE_OPEN
    cb.failure_count = 5
    registry.reset("pipe_a")
    assert cb.state == STATE_CLOSED
    assert cb.failure_count == 0
