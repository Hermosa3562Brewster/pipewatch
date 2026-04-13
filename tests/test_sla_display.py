"""Tests for pipewatch.sla_display."""
import pytest

from pipewatch.pipeline_sla import SLAReport
from pipewatch.sla_display import (
    render_sla_table,
    render_sla_violations,
    render_sla_summary,
)


def _make_report(
    name: str = "pipe",
    compliant: bool = True,
    error_rate: float = 0.0,
    avg_duration: float = 10.0,
    total_runs: int = 10,
    violations: list = None,
) -> SLAReport:
    return SLAReport(
        pipeline_name=name,
        compliant=compliant,
        error_rate=error_rate,
        avg_duration=avg_duration,
        total_runs=total_runs,
        violations=violations or [],
    )


def test_render_table_empty():
    result = render_sla_table([])
    assert "No SLA" in result


def test_render_table_contains_pipeline_name():
    report = _make_report(name="my_pipeline")
    result = render_sla_table([report])
    assert "my_pipeline" in result


def test_render_table_contains_grade():
    report = _make_report(compliant=True)
    result = render_sla_table([report])
    assert "OK" in result


def test_render_table_shows_error_rate():
    report = _make_report(error_rate=0.12)
    result = render_sla_table([report])
    assert "12.00%" in result


def test_render_table_shows_avg_duration():
    report = _make_report(avg_duration=55.5)
    result = render_sla_table([report])
    assert "55.5" in result


def test_render_table_shows_total_runs():
    report = _make_report(total_runs=42)
    result = render_sla_table([report])
    assert "42" in result


def test_render_violations_all_compliant():
    reports = [_make_report(compliant=True)]
    result = render_sla_violations(reports)
    assert "within SLA" in result


def test_render_violations_shows_pipeline_name():
    report = _make_report(
        name="broken_pipe",
        compliant=False,
        violations=["error_rate 50.00% exceeds limit 5.00%"],
    )
    result = render_sla_violations([report])
    assert "broken_pipe" in result


def test_render_violations_shows_violation_text():
    report = _make_report(
        compliant=False,
        violations=["error_rate 50.00% exceeds limit 5.00%"],
    )
    result = render_sla_violations([report])
    assert "error_rate" in result


def test_render_summary_shows_counts():
    reports = [
        _make_report(name="a", compliant=True),
        _make_report(name="b", compliant=False, violations=["x"]),
    ]
    result = render_sla_summary(reports)
    assert "1/2" in result


def test_render_summary_empty():
    result = render_sla_summary([])
    assert "0/0" in result


def test_render_summary_shows_breach_count():
    reports = [
        _make_report(name="a", compliant=False, violations=["v1"]),
        _make_report(name="b", compliant=False, violations=["v2"]),
    ]
    result = render_sla_summary(reports)
    assert "2 breach" in result
