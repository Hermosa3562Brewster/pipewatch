"""Tests for pipeline_scorecard and scorecard_display."""
import pytest

from pipewatch.pipeline_scorecard import (
    ScorecardBuilder,
    ScorecardDimension,
    ScorecardReport,
    compute_scorecard_map,
)
from pipewatch.scorecard_display import (
    render_scorecard_detail,
    render_scorecard_summary,
    render_scorecard_table,
)


# ---------------------------------------------------------------------------
# ScorecardReport
# ---------------------------------------------------------------------------

def _make_report(pipeline: str = "pipe-a") -> ScorecardReport:
    return (
        ScorecardBuilder(pipeline)
        .add("error_rate", 80.0, weight=2.0, note="ok")
        .add("latency", 60.0, weight=1.0)
        .build()
    )


def test_weighted_score_correct():
    report = _make_report()
    # (80*2 + 60*1) / 3 = 220/3 ≈ 73.33
    assert abs(report.weighted_score - (220 / 3)) < 0.01


def test_grade_b_for_mid_score():
    report = _make_report()
    assert report.grade == "B"


def test_grade_a_for_high_score():
    r = ScorecardBuilder("p").add("x", 95.0).build()
    assert r.grade == "A"


def test_grade_f_for_low_score():
    r = ScorecardBuilder("p").add("x", 20.0).build()
    assert r.grade == "F"


def test_empty_dimensions_score_zero():
    r = ScorecardReport(pipeline="empty")
    assert r.weighted_score == 0.0
    assert r.grade == "F"


def test_score_clamped_above_100():
    r = ScorecardBuilder("p").add("x", 150.0).build()
    assert r.dimensions[0].score == 100.0


def test_score_clamped_below_zero():
    r = ScorecardBuilder("p").add("x", -10.0).build()
    assert r.dimensions[0].score == 0.0


def test_to_dict_keys():
    report = _make_report()
    d = report.to_dict()
    assert set(d.keys()) == {"pipeline", "weighted_score", "grade", "dimensions"}


def test_to_dict_dimensions_count():
    report = _make_report()
    assert len(report.to_dict()["dimensions"]) == 2


# ---------------------------------------------------------------------------
# compute_scorecard_map
# ---------------------------------------------------------------------------

def test_compute_scorecard_map_returns_all_pipelines():
    def dim_fn(name):
        return [ScorecardDimension(name="x", score=70.0)]

    result = compute_scorecard_map(["a", "b", "c"], dim_fn)
    assert set(result.keys()) == {"a", "b", "c"}


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def test_render_table_contains_pipeline_name():
    reports = {"pipe-a": _make_report("pipe-a")}
    out = render_scorecard_table(reports)
    assert "pipe-a" in out


def test_render_table_empty():
    assert render_scorecard_table({}) == "No scorecard data available."


def test_render_detail_contains_dimension_names():
    report = _make_report()
    out = render_scorecard_detail(report)
    assert "error_rate" in out
    assert "latency" in out


def test_render_detail_contains_note():
    report = _make_report()
    out = render_scorecard_detail(report)
    assert "ok" in out


def test_render_summary_shows_pipeline_count():
    reports = {"a": _make_report("a"), "b": _make_report("b")}
    out = render_scorecard_summary(reports)
    assert "2" in out


def test_render_summary_empty():
    assert render_scorecard_summary({}) == "No scorecard data."
