"""Tests for pipewatch/history.py"""

import json
import os
import pytest

from pipewatch.history import (
    RunRecord,
    PipelineHistory,
    load_history,
    save_history,
    record_run,
    DEFAULT_HISTORY_FILE,
)
from pipewatch.metrics import PipelineMetrics


@pytest.fixture
def tmp_history_file(tmp_path):
    return str(tmp_path / "test_history.json")


@pytest.fixture
def sample_record():
    return RunRecord(
        pipeline_name="etl_daily",
        started_at="2024-01-15T08:00:00",
        completed_at="2024-01-15T08:05:30",
        status="completed",
        total_records=1000,
        success_count=980,
        failure_count=20,
        error_rate=2.0,
        duration_seconds=330.0,
    )


def test_run_record_to_dict(sample_record):
    d = sample_record.to_dict()
    assert d["pipeline_name"] == "etl_daily"
    assert d["error_rate"] == 2.0
    assert d["duration_seconds"] == 330.0


def test_run_record_from_dict(sample_record):
    d = sample_record.to_dict()
    restored = RunRecord.from_dict(d)
    assert restored.pipeline_name == sample_record.pipeline_name
    assert restored.status == sample_record.status


def test_pipeline_history_add_and_last_n(sample_record):
    history = PipelineHistory(pipeline_name="etl_daily")
    for _ in range(15):
        history.add_run(sample_record)
    assert len(history.runs) == 15
    assert len(history.last_n_runs(10)) == 10


def test_pipeline_history_average_error_rate(sample_record):
    history = PipelineHistory(pipeline_name="etl_daily")
    history.add_run(sample_record)  # error_rate=2.0
    r2 = RunRecord(**{**sample_record.to_dict(), "error_rate": 4.0})
    history.add_run(r2)
    assert history.average_error_rate() == pytest.approx(3.0)


def test_pipeline_history_average_duration(sample_record):
    history = PipelineHistory(pipeline_name="etl_daily")
    history.add_run(sample_record)  # 330s
    r2 = RunRecord(**{**sample_record.to_dict(), "duration_seconds": 270.0})
    history.add_run(r2)
    assert history.average_duration() == pytest.approx(300.0)


def test_average_error_rate_empty():
    history = PipelineHistory(pipeline_name="empty")
    assert history.average_error_rate() == 0.0


def test_average_duration_none_when_no_durations():
    history = PipelineHistory(pipeline_name="empty")
    assert history.average_duration() is None


def test_save_and_load_history(tmp_history_file, sample_record):
    history = PipelineHistory(pipeline_name="etl_daily")
    history.add_run(sample_record)
    save_history({"etl_daily": history}, tmp_history_file)

    loaded = load_history(tmp_history_file)
    assert "etl_daily" in loaded
    assert len(loaded["etl_daily"].runs) == 1
    assert loaded["etl_daily"].runs[0].status == "completed"


def test_load_history_missing_file(tmp_history_file):
    result = load_history(tmp_history_file)
    assert result == {}


def test_record_run_appends_entry(tmp_history_file):
    m = PipelineMetrics(pipeline_name="etl_test")
    m.start()
    for _ in range(10):
        m.record_success()
    m.record_failure("some error")
    m.complete()

    record_run(m, tmp_history_file)
    histories = load_history(tmp_history_file)
    assert "etl_test" in histories
    assert len(histories["etl_test"].runs) == 1
    run = histories["etl_test"].runs[0]
    assert run.success_count == 10
    assert run.failure_count == 1
    assert run.status == "completed"
