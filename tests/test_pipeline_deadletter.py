"""Tests for pipewatch.pipeline_deadletter."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.pipeline_deadletter import DeadLetterEntry, DeadLetterQueue


def _make_entry(pipeline: str = "pipe_a", reason: str = "timeout",
                payload: str = "{}", retryable: bool = True) -> DeadLetterEntry:
    return DeadLetterEntry(pipeline=pipeline, reason=reason,
                           payload=payload, retryable=retryable)


@pytest.fixture
def queue() -> DeadLetterQueue:
    return DeadLetterQueue(max_per_pipeline=5)


def test_to_dict_keys():
    e = _make_entry()
    assert set(e.to_dict().keys()) == {"pipeline", "reason", "payload", "failed_at", "retryable"}


def test_from_dict_roundtrip():
    e = _make_entry(pipeline="x", reason="oops", payload="data", retryable=False)
    assert DeadLetterEntry.from_dict(e.to_dict()).pipeline == "x"
    assert DeadLetterEntry.from_dict(e.to_dict()).retryable is False


def test_summary_contains_pipeline_and_reason():
    e = _make_entry(pipeline="pipe_a", reason="connection refused")
    s = e.summary()
    assert "pipe_a" in s
    assert "connection refused" in s


def test_summary_retryable_tag():
    assert "[retryable]" in _make_entry(retryable=True).summary()
    assert "[dead]" in _make_entry(retryable=False).summary()


def test_push_and_get(queue):
    queue.push(_make_entry("pipe_a"))
    assert len(queue.get("pipe_a")) == 1


def test_get_unknown_returns_empty(queue):
    assert queue.get("unknown") == []


def test_get_retryable_filters(queue):
    queue.push(_make_entry("pipe_a", retryable=True))
    queue.push(_make_entry("pipe_a", retryable=False))
    assert len(queue.get_retryable("pipe_a")) == 1


def test_purge_removes_entries(queue):
    queue.push(_make_entry("pipe_a"))
    queue.push(_make_entry("pipe_a"))
    removed = queue.purge("pipe_a")
    assert removed == 2
    assert queue.get("pipe_a") == []


def test_purge_unknown_returns_zero(queue):
    assert queue.purge("ghost") == 0


def test_max_per_pipeline_cap(queue):
    for _ in range(10):
        queue.push(_make_entry("pipe_a"))
    assert len(queue.get("pipe_a")) == 5


def test_total_count(queue):
    queue.push(_make_entry("pipe_a"))
    queue.push(_make_entry("pipe_b"))
    assert queue.total_count() == 2


def test_all_pipelines(queue):
    queue.push(_make_entry("pipe_a"))
    queue.push(_make_entry("pipe_b"))
    assert set(queue.all_pipelines()) == {"pipe_a", "pipe_b"}


def test_counts(queue):
    queue.push(_make_entry("pipe_a"))
    queue.push(_make_entry("pipe_a"))
    queue.push(_make_entry("pipe_b"))
    c = queue.counts()
    assert c["pipe_a"] == 2
    assert c["pipe_b"] == 1
