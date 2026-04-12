"""Tests for pipewatch.display_aggregated module."""

import pytest
from pipewatch.metrics import PipelineMetrics
from pipewatch.display_aggregated import (
    render_aggregated_summary,
    render_aggregated_table,
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


def test_render_summary_contains_header(sample_map):
    output = render_aggregated_summary(sample_map)
    assert "Aggregated Pipeline Summary" in output


def test_render_summary_shows_total_pipelines(sample_map):
    output = render_aggregated_summary(sample_map)
    assert "Total Pipelines  : 3" in output


def test_render_summary_shows_active_and_failed(sample_map):
    output = render_aggregated_summary(sample_map)
    assert "Active           : 1" in output
    assert "Failed           : 1" in output


def test_render_summary_shows_totals(sample_map):
    output = render_aggregated_summary(sample_map)
    assert "Total Successes  : 240" in output
    assert "Total Failures   : 60" in output


def test_render_summary_shows_health_score(sample_map):
    output = render_aggregated_summary(sample_map)
    assert "Health Score" in output


def test_render_summary_shows_top_failing(sample_map):
    output = render_aggregated_summary(sample_map)
    assert "Top Failing" in output
    assert "etl_b" in output


def test_render_summary_empty_map():
    output = render_aggregated_summary({})
    assert "No pipelines registered" in output


def test_render_table_contains_pipeline_names(sample_map):
    output = render_aggregated_table(sample_map)
    assert "etl_a" in output
    assert "etl_b" in output
    assert "etl_c" in output


def test_render_table_contains_header_columns(sample_map):
    output = render_aggregated_table(sample_map)
    assert "Pipeline" in output
    assert "Status" in output
    assert "Successes" in output
    assert "Failures" in output
    assert "Err Rate" in output


def test_render_table_empty_map():
    output = render_aggregated_table({})
    assert "No pipelines to display" in output


def test_render_table_sorted_output(sample_map):
    output = render_aggregated_table(sample_map)
    lines = [l for l in output.splitlines() if l.startswith("etl_")]
    names = [l.split()[0] for l in lines]
    assert names == sorted(names)
