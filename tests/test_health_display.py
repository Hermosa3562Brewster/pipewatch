"""Tests for pipewatch.health_display."""

from pipewatch.pipeline_health import HealthReport
from pipewatch.health_display import (
    render_health_table,
    render_health_summary,
)


def _make_report(
    name: str,
    score: float,
    grade: str,
    status: str,
    error_rate: float = 0.0,
    total_runs: int = 10,
    note: str | None = None,
) -> HealthReport:
    return HealthReport(
        name=name,
        score=score,
        grade=grade,
        status=status,
        error_rate=error_rate,
        total_runs=total_runs,
        note=note,
    )


# --- render_health_table ---

def test_render_table_empty():
    result = render_health_table([])
    assert "No pipeline" in result

def test_render_table_contains_pipeline_name():
    r = _make_report("my_pipeline", 0.95, "A", "healthy")
    result = render_health_table([r])
    assert "my_pipeline" in result

def test_render_table_contains_grade():
    r = _make_report("pipe", 0.95, "A", "healthy")
    result = render_health_table([r])
    assert "A" in result

def test_render_table_contains_status():
    r = _make_report("pipe", 0.30, "F", "critical", error_rate=0.70)
    result = render_health_table([r])
    assert "critical" in result

def test_render_table_shows_note():
    r = _make_report("pipe", 1.0, "A", "unknown", total_runs=0, note="No runs recorded yet.")
    result = render_health_table([r])
    assert "No runs recorded yet." in result

def test_render_table_multiple_pipelines():
    reports = [
        _make_report("alpha", 0.95, "A", "healthy"),
        _make_report("beta", 0.40, "D", "degraded", error_rate=0.60),
    ]
    result = render_health_table(reports)
    assert "alpha" in result
    assert "beta" in result


# --- render_health_summary ---

def test_render_summary_empty():
    result = render_health_summary([])
    assert "no data" in result

def test_render_summary_counts_healthy():
    reports = [
        _make_report("a", 0.95, "A", "healthy"),
        _make_report("b", 0.95, "A", "healthy"),
        _make_report("c", 0.20, "F", "critical"),
    ]
    result = render_health_summary(reports)
    assert "healthy: 2" in result
    assert "critical: 1" in result

def test_render_summary_shows_avg_score():
    reports = [
        _make_report("a", 1.0, "A", "healthy"),
        _make_report("b", 0.0, "F", "critical"),
    ]
    result = render_health_summary(reports)
    assert "50.00%" in result
