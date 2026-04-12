"""Tests for pipewatch.aggregator module."""

import pytest
from pipewatch.metrics import PipelineMetrics
from pipewatch.aggregator import (
    aggregate,
    top_failing,
    healthy_pipelines,
    AggregatedStats,
)


def _make_metrics(name: str, successes: int, failures: int, status: str = "idle") -> PipelineMetrics:
    m = PipelineMetrics(name=name)
    m.success_count = successes
    m.failure_count = failures
    m.status = status
    return m


@pytest.fixture
def sample_map():
    return {
        "etl_a": _make_metrics("etl_a", 90, 10, status="running"),
        "etl_b": _make_metrics("etl_b", 50, 50, status="failed"),
        "etl_c": _make_metrics("etl_c", 100, 0, status="idle"),
    }


def test_aggregate_total_pipelines(sample_map):
    stats = aggregate(sample_map)
    assert stats.total_pipelines == 3


def test_aggregate_counts_active_and_failed(sample_map):
    stats = aggregate(sample_map)
    assert stats.active_pipelines == 1
    assert stats.failed_pipelines == 1


def test_aggregate_totals_successes_and_failures(sample_map):
    stats = aggregate(sample_map)
    assert stats.total_successes == 240
    assert stats.total_failures == 60


def test_aggregate_overall_error_rate(sample_map):
    stats = aggregate(sample_map)
    assert stats.overall_error_rate == pytest.approx(60 / 300, rel=1e-4)


def test_aggregate_health_score(sample_map):
    stats = aggregate(sample_map)
    assert 0.0 <= stats.health_score <= 1.0
    assert stats.health_score == pytest.approx(1.0 - stats.overall_error_rate, rel=1e-4)


def test_aggregate_empty_map():
    stats = aggregate({})
    assert stats.total_pipelines == 0
    assert stats.health_score == 1.0
    assert stats.overall_error_rate == 0.0


def test_aggregate_pipeline_names(sample_map):
    stats = aggregate(sample_map)
    assert set(stats.pipeline_names) == {"etl_a", "etl_b", "etl_c"}


def test_top_failing_returns_ordered(sample_map):
    result = top_failing(sample_map, n=2)
    assert result[0] == "etl_b"  # 50 failures
    assert result[1] == "etl_a"  # 10 failures
    assert len(result) == 2


def test_top_failing_empty_map():
    result = top_failing({}, n=3)
    assert result == []


def test_healthy_pipelines_filters_by_threshold(sample_map):
    result = healthy_pipelines(sample_map, threshold=0.15)
    assert "etl_a" in result   # 10% error rate
    assert "etl_c" in result   # 0% error rate
    assert "etl_b" not in result  # 50% error rate


def test_healthy_pipelines_all_healthy():
    m = {"p": _make_metrics("p", 100, 0)}
    assert healthy_pipelines(m) == ["p"]


def test_healthy_pipelines_none_healthy():
    m = {"p": _make_metrics("p", 0, 100)}
    assert healthy_pipelines(m, threshold=0.05) == []
