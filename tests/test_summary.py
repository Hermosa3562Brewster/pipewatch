"""Tests for pipewatch/summary.py"""

import pytest
from pipewatch.history import PipelineHistory, RunRecord, save_history
from pipewatch.summary import (
    _trend_arrow,
    format_duration,
    render_history_summary,
    render_all_summaries,
)


def _make_record(name="pipe", error_rate=0.0, duration=120.0, status="completed"):
    return RunRecord(
        pipeline_name=name,
        started_at="2024-03-01T10:00:00",
        completed_at="2024-03-01T10:02:00",
        status=status,
        total_records=500,
        success_count=490,
        failure_count=10,
        error_rate=error_rate,
        duration_seconds=duration,
    )


def test_trend_arrow_up():
    assert _trend_arrow([1.0, 2.0]) == "↑ "


def test_trend_arrow_down():
    assert _trend_arrow([3.0, 1.0]) == "↓ "


def test_trend_arrow_flat():
    assert _trend_arrow([2.0, 2.0]) == "→ "


def test_trend_arrow_single_value():
    assert _trend_arrow([5.0]) == "  "


def test_format_duration_seconds_only():
    assert format_duration(45.0) == "45s"


def test_format_duration_minutes_and_seconds():
    assert format_duration(125.0) == "2m 05s"


def test_format_duration_none():
    assert format_duration(None) == "N/A"


def test_render_history_summary_no_runs():
    h = PipelineHistory(pipeline_name="empty_pipe")
    result = render_history_summary(h)
    assert "No history" in result
    assert "empty_pipe" in result


def test_render_history_summary_contains_pipeline_name():
    h = PipelineHistory(pipeline_name="etl_sales")
    h.add_run(_make_record("etl_sales", error_rate=1.5))
    result = render_history_summary(h)
    assert "etl_sales" in result


def test_render_history_summary_shows_stats():
    h = PipelineHistory(pipeline_name="etl_sales")
    h.add_run(_make_record("etl_sales", error_rate=2.0, duration=60.0))
    h.add_run(_make_record("etl_sales", error_rate=4.0, duration=90.0))
    result = render_history_summary(h)
    assert "3.00%" in result  # avg error rate
    assert "1m 15s" in result  # avg duration 75s
    assert "2 runs" in result or "Last 2" in result


def test_render_all_summaries_no_file(tmp_path):
    missing = str(tmp_path / "no_file.json")
    result = render_all_summaries(path=missing)
    assert "No pipeline history" in result


def test_render_all_summaries_multiple_pipelines(tmp_path):
    path = str(tmp_path / "hist.json")
    h1 = PipelineHistory(pipeline_name="alpha")
    h1.add_run(_make_record("alpha"))
    h2 = PipelineHistory(pipeline_name="beta")
    h2.add_run(_make_record("beta"))
    save_history({"alpha": h1, "beta": h2}, path)

    result = render_all_summaries(path=path)
    assert "alpha" in result
    assert "beta" in result
    assert "---" in result  # separator
