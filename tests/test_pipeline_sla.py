"""Tests for pipewatch.pipeline_sla."""
import pytest
from datetime import datetime

from pipewatch.history import RunRecord
from pipewatch.pipeline_sla import (
    SLAConfig,
    SLAReport,
    compute_sla,
    compute_sla_map,
    _error_rate,
    _avg_duration,
)


def _make_record(status: str, duration: float = 10.0) -> RunRecord:
    return RunRecord(
        pipeline_name="pipe",
        status=status,
        started_at=datetime(2024, 1, 1, 12, 0, 0),
        ended_at=datetime(2024, 1, 1, 12, 0, 10),
        duration_seconds=duration,
        success_count=1 if status == "success" else 0,
        failure_count=0 if status == "success" else 1,
    )


def test_error_rate_empty():
    assert _error_rate([]) == 0.0


def test_error_rate_all_success():
    records = [_make_record("success")] * 4
    assert _error_rate(records) == 0.0


def test_error_rate_all_failure():
    records = [_make_record("failed")] * 3
    assert _error_rate(records) == 1.0


def test_error_rate_mixed():
    records = [_make_record("success")] * 3 + [_make_record("failed")]
    assert _error_rate(records) == pytest.approx(0.25)


def test_avg_duration_empty():
    assert _avg_duration([]) == 0.0


def test_avg_duration_single():
    assert _avg_duration([_make_record("success", 42.0)]) == pytest.approx(42.0)


def test_avg_duration_multiple():
    records = [_make_record("success", d) for d in [10.0, 20.0, 30.0]]
    assert _avg_duration(records) == pytest.approx(20.0)


def test_compute_sla_compliant():
    records = [_make_record("success", 10.0)] * 10
    report = compute_sla("pipe_a", records)
    assert report.compliant is True
    assert report.violations == []
    assert report.grade == "OK"


def test_compute_sla_error_rate_violation():
    records = [_make_record("failed")] * 5 + [_make_record("success")] * 5
    cfg = SLAConfig(max_error_rate=0.1)
    report = compute_sla("pipe_b", records, cfg)
    assert report.compliant is False
    assert any("error_rate" in v for v in report.violations)


def test_compute_sla_duration_violation():
    records = [_make_record("success", 600.0)] * 5
    cfg = SLAConfig(max_duration_seconds=300.0)
    report = compute_sla("pipe_c", records, cfg)
    assert report.compliant is False
    assert any("avg_duration" in v for v in report.violations)


def test_compute_sla_min_success_violation():
    records = [_make_record("success")] * 2
    cfg = SLAConfig(min_success_count=10)
    report = compute_sla("pipe_d", records, cfg)
    assert report.compliant is False
    assert any("success_count" in v for v in report.violations)


def test_compute_sla_grade_warn_single_violation():
    records = [_make_record("failed")] * 10
    cfg = SLAConfig(max_error_rate=0.01, max_duration_seconds=9999)
    report = compute_sla("pipe_e", records, cfg)
    assert report.grade == "WARN"


def test_compute_sla_grade_breach_multiple_violations():
    records = [_make_record("failed", 999.0)] * 10
    cfg = SLAConfig(max_error_rate=0.01, max_duration_seconds=10.0)
    report = compute_sla("pipe_f", records, cfg)
    assert report.grade == "BREACH"
    assert len(report.violations) >= 2


def test_compute_sla_map_returns_all_pipelines():
    records_map = {
        "alpha": [_make_record("success")] * 5,
        "beta": [_make_record("failed")] * 5,
    }
    result = compute_sla_map(records_map)
    assert set(result.keys()) == {"alpha", "beta"}


def test_compute_sla_map_uses_per_pipeline_config():
    records_map = {"alpha": [_make_record("failed")] * 10}
    configs = {"alpha": SLAConfig(max_error_rate=0.99)}
    result = compute_sla_map(records_map, configs)
    assert result["alpha"].compliant is True
