"""Tests for pipewatch.retry_display."""
from __future__ import annotations

import pytest

from pipewatch.pipeline_retry import RetryTracker
from pipewatch.retry_display import (
    render_retry_detail,
    render_retry_summary,
    render_retry_table,
)


@pytest.fixture
def tracker() -> RetryTracker:
    t = RetryTracker()
    t.record("etl_alpha", attempt=1, reason="timeout", succeeded=False)
    t.record("etl_alpha", attempt=2, reason="timeout", succeeded=True)
    t.record("etl_beta", attempt=1, reason="oom", succeeded=False)
    return t


def test_render_table_contains_pipeline_names(tracker):
    out = render_retry_table(tracker)
    assert "etl_alpha" in out
    assert "etl_beta" in out


def test_render_table_shows_retry_counts(tracker):
    out = render_retry_table(tracker)
    # etl_alpha has 2 retries
    assert "2" in out


def test_render_table_empty_tracker():
    out = render_retry_table(RetryTracker())
    assert "No retry data" in out


def test_render_detail_contains_attempts(tracker):
    out = render_retry_detail(tracker, "etl_alpha")
    assert "attempt=1" in out
    assert "attempt=2" in out


def test_render_detail_contains_reason(tracker):
    out = render_retry_detail(tracker, "etl_alpha")
    assert "timeout" in out


def test_render_detail_shows_status(tracker):
    out = render_retry_detail(tracker, "etl_alpha")
    assert "OK" in out
    assert "FAIL" in out


def test_render_detail_unknown_pipeline():
    t = RetryTracker()
    out = render_retry_detail(t, "ghost")
    assert "No retry records" in out


def test_render_summary_contains_header(tracker):
    out = render_retry_summary(tracker)
    assert "Retry Summary" in out


def test_render_summary_shows_pipeline_count(tracker):
    out = render_retry_summary(tracker)
    assert "2" in out


def test_render_summary_shows_most_retried(tracker):
    out = render_retry_summary(tracker)
    assert "etl_alpha" in out


def test_render_summary_empty_tracker():
    out = render_retry_summary(RetryTracker())
    assert "0" in out
