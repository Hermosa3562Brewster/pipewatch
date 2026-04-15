"""Tests for pipewatch.deadletter_display."""
from __future__ import annotations

import pytest

from pipewatch.pipeline_deadletter import DeadLetterEntry, DeadLetterQueue
from pipewatch.deadletter_display import (
    render_deadletter_table,
    render_deadletter_detail,
    render_deadletter_summary,
)


def _entry(pipeline: str = "pipe_a", reason: str = "timeout",
           payload: str = "{}", retryable: bool = True) -> DeadLetterEntry:
    return DeadLetterEntry(pipeline=pipeline, reason=reason,
                           payload=payload, retryable=retryable)


@pytest.fixture
def queue() -> DeadLetterQueue:
    q = DeadLetterQueue()
    q.push(_entry("pipe_a", reason="connection refused", retryable=True))
    q.push(_entry("pipe_a", reason="schema error", retryable=False))
    q.push(_entry("pipe_b", reason="timeout", retryable=True))
    return q


def test_render_table_empty_queue():
    result = render_deadletter_table(DeadLetterQueue())
    assert "No dead-letter" in result


def test_render_table_contains_pipeline_names(queue):
    result = render_deadletter_table(queue)
    assert "pipe_a" in result
    assert "pipe_b" in result


def test_render_table_contains_header(queue):
    result = render_deadletter_table(queue)
    assert "Pipeline" in result
    assert "Total" in result


def test_render_table_filter_by_pipeline(queue):
    result = render_deadletter_table(queue, pipeline="pipe_a")
    assert "pipe_a" in result
    assert "pipe_b" not in result


def test_render_table_filter_nonexistent_pipeline(queue):
    """Filtering by a pipeline that has no entries should show the empty message."""
    result = render_deadletter_table(queue, pipeline="ghost")
    assert "No dead-letter" in result


def test_render_detail_contains_entries(queue):
    result = render_deadletter_detail(queue, "pipe_a")
    assert "pipe_a" in result
    assert "connection refused" in result


def test_render_detail_empty_pipeline():
    result = render_deadletter_detail(DeadLetterQueue(), "ghost")
    assert "No dead-letter" in result


def test_render_summary_contains_totals(queue):
    result = render_deadletter_summary(queue)
    assert "Total entries" in result
    assert "3" in result


def test_render_summary_shows_retryable_count(queue):
    result = render_deadletter_summary(queue)
    assert "Retryable" in result


def test_render_summary_empty_queue():
    """Summary for an empty queue should report zero entries."""
    result = render_deadletter_summary(DeadLetterQueue())
    assert "Total entries" in result
    assert "0" in result
