"""Tests for pipewatch.pipeline_health."""

import pytest

from pipewatch.metrics import PipelineMetrics
from pipewatch.pipeline_health import (
    HealthReport,
    _grade,
    _status,
    compute_health,
    compute_health_map,
)


def _make_metrics(name: str, successes: int, failures: int) -> PipelineMetrics:
    m = PipelineMetrics(name=name)
    m.success_count = successes
    m.failure_count = failures
    return m


# --- _grade ---

def test_grade_a():
    assert _grade(0.95) == "A"

def test_grade_b():
    assert _grade(0.80) == "B"

def test_grade_c():
    assert _grade(0.60) == "C"

def test_grade_d():
    assert _grade(0.40) == "D"

def test_grade_f():
    assert _grade(0.20) == "F"


# --- _status ---

def test_status_unknown_when_no_runs():
    assert _status(1.0, 0) == "unknown"

def test_status_healthy():
    assert _status(0.90, 10) == "healthy"

def test_status_degraded():
    assert _status(0.55, 10) == "degraded"

def test_status_critical():
    assert _status(0.10, 10) == "critical"


# --- compute_health ---

def test_compute_health_no_runs():
    m = _make_metrics("pipe", 0, 0)
    r = compute_health(m)
    assert r.status == "unknown"
    assert r.score == 1.0
    assert r.note is not None

def test_compute_health_all_success():
    m = _make_metrics("pipe", 100, 0)
    r = compute_health(m)
    assert r.grade == "A"
    assert r.error_rate == 0.0
    assert r.status == "healthy"

def test_compute_health_high_failure():
    m = _make_metrics("pipe", 10, 90)
    r = compute_health(m)
    assert r.grade == "F"
    assert r.status == "critical"
    assert r.error_rate == pytest.approx(0.90, rel=1e-3)

def test_compute_health_returns_health_report_instance():
    m = _make_metrics("alpha", 50, 10)
    r = compute_health(m)
    assert isinstance(r, HealthReport)
    assert r.name == "alpha"
    assert r.total_runs == 60


# --- compute_health_map ---

def test_compute_health_map_keys_match():
    metrics_map = {
        "a": _make_metrics("a", 10, 0),
        "b": _make_metrics("b", 5, 5),
    }
    result = compute_health_map(metrics_map)
    assert set(result.keys()) == {"a", "b"}

def test_compute_health_map_values_are_reports():
    metrics_map = {"x": _make_metrics("x", 20, 2)}
    result = compute_health_map(metrics_map)
    assert isinstance(result["x"], HealthReport)
