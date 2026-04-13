"""Tests for pipewatch.pipeline_retry."""
from __future__ import annotations

import pytest
from datetime import datetime

from pipewatch.pipeline_retry import RetryRecord, RetryTracker


@pytest.fixture
def tracker() -> RetryTracker:
    return RetryTracker()


def test_record_returns_retry_record(tracker):
    rec = tracker.record("etl_a", attempt=1, reason="timeout")
    assert isinstance(rec, RetryRecord)
    assert rec.pipeline_name == "etl_a"
    assert rec.attempt == 1
    assert rec.reason == "timeout"


def test_record_stores_entry(tracker):
    tracker.record("etl_a", attempt=1)
    assert tracker.total_retries("etl_a") == 1


def test_get_returns_all_records(tracker):
    tracker.record("etl_a", attempt=1)
    tracker.record("etl_a", attempt=2)
    records = tracker.get("etl_a")
    assert len(records) == 2


def test_get_last_n(tracker):
    for i in range(5):
        tracker.record("etl_a", attempt=i + 1)
    records = tracker.get("etl_a", last_n=3)
    assert len(records) == 3
    assert records[-1].attempt == 5


def test_max_attempt(tracker):
    tracker.record("etl_a", attempt=1)
    tracker.record("etl_a", attempt=3)
    tracker.record("etl_a", attempt=2)
    assert tracker.max_attempt("etl_a") == 3


def test_max_attempt_empty_returns_zero(tracker):
    assert tracker.max_attempt("unknown") == 0


def test_success_rate_all_succeeded(tracker):
    tracker.record("etl_b", attempt=1, succeeded=True)
    tracker.record("etl_b", attempt=2, succeeded=True)
    assert tracker.success_rate("etl_b") == 1.0


def test_success_rate_none_succeeded(tracker):
    tracker.record("etl_b", attempt=1, succeeded=False)
    tracker.record("etl_b", attempt=2, succeeded=False)
    assert tracker.success_rate("etl_b") == 0.0


def test_success_rate_mixed(tracker):
    tracker.record("etl_c", attempt=1, succeeded=True)
    tracker.record("etl_c", attempt=2, succeeded=False)
    assert tracker.success_rate("etl_c") == pytest.approx(0.5)


def test_success_rate_empty_returns_zero(tracker):
    assert tracker.success_rate("ghost") == 0.0


def test_clear_removes_records(tracker):
    tracker.record("etl_a", attempt=1)
    tracker.clear("etl_a")
    assert tracker.total_retries("etl_a") == 0


def test_max_per_pipeline_cap():
    t = RetryTracker(max_per_pipeline=3)
    for i in range(6):
        t.record("etl_x", attempt=i + 1)
    assert t.total_retries("etl_x") == 3
    assert t.get("etl_x")[0].attempt == 4


def test_all_pipeline_names(tracker):
    tracker.record("a", attempt=1)
    tracker.record("b", attempt=1)
    names = tracker.all_pipeline_names()
    assert set(names) == {"a", "b"}


def test_to_dict_and_from_dict_roundtrip(tracker):
    rec = tracker.record("etl_a", attempt=2, reason="oom", succeeded=True)
    d = rec.to_dict()
    restored = RetryRecord.from_dict(d)
    assert restored.pipeline_name == rec.pipeline_name
    assert restored.attempt == rec.attempt
    assert restored.reason == rec.reason
    assert restored.succeeded == rec.succeeded
    assert restored.timestamp == rec.timestamp
