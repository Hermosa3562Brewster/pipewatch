"""Tests for circuit_breaker_display module."""

from __future__ import annotations

from pipewatch.pipeline_circuit_breaker import (
    CircuitBreakerRegistry,
    STATE_OPEN,
    STATE_HALF_OPEN,
)
from pipewatch.circuit_breaker_display import (
    render_circuit_breaker_table,
    render_circuit_breaker_summary,
)


def _make_registry() -> CircuitBreakerRegistry:
    reg = CircuitBreakerRegistry(failure_threshold=3, recovery_timeout=60)
    reg.get("alpha")
    reg.get("beta")
    return reg


def test_empty_registry_returns_no_breakers_message():
    reg = CircuitBreakerRegistry()
    result = render_circuit_breaker_table(reg)
    assert "No circuit breakers" in result


def test_table_contains_pipeline_names():
    reg = _make_registry()
    result = render_circuit_breaker_table(reg)
    assert "alpha" in result
    assert "beta" in result


def test_table_contains_header():
    reg = _make_registry()
    result = render_circuit_breaker_table(reg)
    assert "Pipeline" in result
    assert "State" in result


def test_table_shows_failure_count():
    reg = _make_registry()
    cb = reg.get("alpha")
    cb.record_failure()
    result = render_circuit_breaker_table(reg)
    assert "1" in result


def test_table_shows_tripped_at_when_open():
    reg = _make_registry()
    cb = reg.get("alpha")
    for _ in range(3):
        cb.record_failure()
    result = render_circuit_breaker_table(reg)
    assert cb.tripped_at.isoformat()[:10] in result


def test_summary_contains_header():
    reg = _make_registry()
    result = render_circuit_breaker_summary(reg)
    assert "Circuit Breaker Summary" in result


def test_summary_counts_total():
    reg = _make_registry()
    result = render_circuit_breaker_summary(reg)
    assert "2" in result


def test_summary_counts_open():
    reg = _make_registry()
    cb = reg.get("alpha")
    for _ in range(3):
        cb.record_failure()
    result = render_circuit_breaker_summary(reg)
    assert "Open" in result


def test_summary_counts_half_open():
    reg = _make_registry()
    cb = reg.get("beta")
    cb.state = STATE_HALF_OPEN
    result = render_circuit_breaker_summary(reg)
    assert "Half" in result
