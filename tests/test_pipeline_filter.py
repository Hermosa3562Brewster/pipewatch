"""Tests for pipewatch.pipeline_filter and pipewatch.filtered_display."""

from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetrics
from pipewatch.pipeline_filter import (
    apply_filters,
    filter_by_error_rate,
    filter_by_name,
    filter_by_predicate,
    filter_by_status,
)
from pipewatch.filtered_display import render_filtered_view


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_metrics(status: str = "idle", successes: int = 0, failures: int = 0) -> PipelineMetrics:
    m = PipelineMetrics()
    m.status = status
    m.successes = successes
    m.failures = failures
    return m


@pytest.fixture()
def sample_map():
    return {
        "alpha": _make_metrics(status="running", successes=10, failures=0),
        "beta": _make_metrics(status="failed", successes=5, failures=5),
        "gamma": _make_metrics(status="idle", successes=0, failures=0),
    }


# ---------------------------------------------------------------------------
# filter_by_name
# ---------------------------------------------------------------------------

def test_filter_by_name_returns_only_requested(sample_map):
    result = filter_by_name(sample_map, ["alpha", "gamma"])
    assert set(result.keys()) == {"alpha", "gamma"}


def test_filter_by_name_unknown_name_excluded(sample_map):
    result = filter_by_name(sample_map, ["unknown"])
    assert result == {}


# ---------------------------------------------------------------------------
# filter_by_status
# ---------------------------------------------------------------------------

def test_filter_by_status_running(sample_map):
    result = filter_by_status(sample_map, "running")
    assert list(result.keys()) == ["alpha"]


def test_filter_by_status_no_match(sample_map):
    result = filter_by_status(sample_map, "completed")
    assert result == {}


# ---------------------------------------------------------------------------
# filter_by_error_rate
# ---------------------------------------------------------------------------

def test_filter_by_error_rate_high_only(sample_map):
    # beta has 50% error rate; alpha and gamma have 0%
    result = filter_by_error_rate(sample_map, min_rate=0.4)
    assert set(result.keys()) == {"beta"}


def test_filter_by_error_rate_zero_only(sample_map):
    result = filter_by_error_rate(sample_map, max_rate=0.0)
    assert "beta" not in result
    assert "alpha" in result
    assert "gamma" in result


# ---------------------------------------------------------------------------
# filter_by_predicate
# ---------------------------------------------------------------------------

def test_filter_by_predicate_custom(sample_map):
    result = filter_by_predicate(sample_map, lambda name, m: name.startswith("a"))
    assert list(result.keys()) == ["alpha"]


# ---------------------------------------------------------------------------
# apply_filters
# ---------------------------------------------------------------------------

def test_apply_filters_no_filters_returns_all(sample_map):
    result = apply_filters(sample_map)
    assert set(result.keys()) == {"alpha", "beta", "gamma"}


def test_apply_filters_combined(sample_map):
    result = apply_filters(sample_map, names=["alpha", "beta"], max_error_rate=0.3)
    # beta has 50% error rate → excluded; alpha has 0% → included
    assert set(result.keys()) == {"alpha"}


# ---------------------------------------------------------------------------
# render_filtered_view
# ---------------------------------------------------------------------------

def test_render_filtered_view_no_match(sample_map):
    output = render_filtered_view(sample_map, status="completed")
    assert "No pipelines match" in output


def test_render_filtered_view_shows_count(sample_map):
    output = render_filtered_view(sample_map, status="running")
    assert "1 / 3" in output


def test_render_filtered_view_contains_pipeline_name(sample_map):
    output = render_filtered_view(sample_map, names=["beta"])
    assert "beta" in output
