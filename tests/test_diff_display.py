"""Tests for pipewatch.diff_display."""

from __future__ import annotations

from pipewatch.pipeline_diff import PipelineDiff
from pipewatch.diff_display import render_diff_table


def _make_diff(
    name: str = "pipe",
    success_delta: int = 0,
    failure_delta: int = 0,
    error_rate_delta: float = 0.0,
    status_changed: bool = False,
    prev_status: str | None = "running",
    curr_status: str | None = "running",
) -> PipelineDiff:
    return PipelineDiff(
        name=name,
        success_delta=success_delta,
        failure_delta=failure_delta,
        error_rate_delta=error_rate_delta,
        status_changed=status_changed,
        prev_status=prev_status,
        curr_status=curr_status,
    )


def test_empty_diffs_returns_no_changes_message():
    output = render_diff_table([])
    assert "No pipeline changes" in output


def test_render_contains_pipeline_name():
    diffs = [_make_diff(name="my-pipeline")]
    output = render_diff_table(diffs)
    assert "my-pipeline" in output


def test_render_contains_header():
    diffs = [_make_diff()]
    output = render_diff_table(diffs)
    assert "Pipeline" in output
    assert "Success" in output
    assert "Failure" in output


def test_render_shows_degraded_count():
    diffs = [
        _make_diff("a", error_rate_delta=0.1),
        _make_diff("b", error_rate_delta=0.0),
    ]
    output = render_diff_table(diffs)
    assert "1 degraded" in output


def test_render_shows_improved_count():
    diffs = [
        _make_diff("a", error_rate_delta=-0.05),
    ]
    output = render_diff_table(diffs)
    assert "1 improved" in output


def test_render_shows_total_count():
    diffs = [_make_diff("a"), _make_diff("b"), _make_diff("c")]
    output = render_diff_table(diffs)
    assert "3 total" in output


def test_render_status_change_shown():
    diff = _make_diff(
        name="flaky",
        status_changed=True,
        prev_status="running",
        curr_status="failed",
        error_rate_delta=0.2,
    )
    output = render_diff_table([diff])
    assert "running" in output
    assert "failed" in output


def test_render_no_status_change_no_arrow():
    diff = _make_diff(name="stable", status_changed=False)
    output = render_diff_table([diff])
    assert "→" not in output
