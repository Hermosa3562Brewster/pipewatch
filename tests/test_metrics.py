"""Unit tests for pipewatch.metrics.PipelineMetrics."""

import time
import pytest
from pipewatch.metrics import PipelineMetrics


def test_initial_state():
    m = PipelineMetrics(name="test-pipe")
    assert m.records_processed == 0
    assert m.records_failed == 0
    assert m.bytes_transferred == 0
    assert m.status == "idle"
    assert m.error_rate == 0.0
    assert m.throughput == 0.0


def test_start_sets_status_and_timestamps():
    m = PipelineMetrics(name="test-pipe")
    m.start()
    assert m.status == "running"
    assert m.started_at is not None
    assert m.last_updated is not None


def test_record_success_updates_counters():
    m = PipelineMetrics(name="test-pipe")
    m.start()
    m.record_success(count=100, bytes_count=4096)
    assert m.records_processed == 100
    assert m.bytes_transferred == 4096
    assert m.status == "running"


def test_record_failure_updates_counters():
    m = PipelineMetrics(name="test-pipe")
    m.start()
    m.record_failure(count=5)
    assert m.records_failed == 5
    assert m.status == "error"


def test_error_rate_calculation():
    m = PipelineMetrics(name="test-pipe")
    m.start()
    m.record_success(count=90)
    m.record_failure(count=10)
    assert m.error_rate == pytest.approx(0.1)


def test_error_rate_zero_when_no_records():
    m = PipelineMetrics(name="test-pipe")
    assert m.error_rate == 0.0


def test_throughput_positive_after_records():
    m = PipelineMetrics(name="test-pipe")
    m.start()
    time.sleep(0.05)
    m.record_success(count=50)
    assert m.throughput > 0.0


def test_complete_sets_status():
    m = PipelineMetrics(name="test-pipe")
    m.start()
    m.record_success(count=10)
    m.complete()
    assert m.status == "completed"


def test_to_dict_contains_expected_keys():
    m = PipelineMetrics(name="test-pipe")
    m.start()
    m.record_success(count=20, bytes_count=512)
    d = m.to_dict()
    expected_keys = {
        "name", "status", "records_processed", "records_failed",
        "bytes_transferred", "error_rate", "throughput_rps",
        "started_at", "last_updated",
    }
    assert expected_keys == set(d.keys())
    assert d["name"] == "test-pipe"
    assert d["records_processed"] == 20
