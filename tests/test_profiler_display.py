"""Tests for pipewatch.profiler_display."""
from pipewatch.pipeline_profiler import PipelineProfile, StageProfile
from pipewatch.profiler_display import (
    render_profile_table,
    render_profile_summary,
)


def _make_profile(name: str, durations: dict) -> PipelineProfile:
    pp = PipelineProfile(pipeline_name=name)
    t = 0.0
    for stage_name, dur in durations.items():
        pp.stages.append(
            StageProfile(name=stage_name, duration_s=dur, started_at=t, ended_at=t + dur)
        )
        t += dur
    return pp


def test_render_table_contains_pipeline_name():
    pp = _make_profile("my_etl", {"extract": 1.0})
    output = render_profile_table(pp)
    assert "my_etl" in output


def test_render_table_contains_stage_names():
    pp = _make_profile("pipe", {"extract": 1.0, "load": 2.0})
    output = render_profile_table(pp)
    assert "extract" in output
    assert "load" in output


def test_render_table_marks_slowest():
    pp = _make_profile("pipe", {"fast": 0.1, "slow": 5.0})
    output = render_profile_table(pp)
    assert "slowest" in output
    assert "slow" in output


def test_render_table_empty_stages():
    pp = PipelineProfile(pipeline_name="empty")
    output = render_profile_table(pp)
    assert "No stages" in output


def test_render_table_shows_total_duration():
    pp = _make_profile("pipe", {"a": 1.0, "b": 2.0})
    output = render_profile_table(pp)
    assert "3.000" in output


def test_render_summary_contains_pipeline_names():
    profiles = {
        "pipe_a": _make_profile("pipe_a", {"x": 1.0}),
        "pipe_b": _make_profile("pipe_b", {"y": 2.0}),
    }
    output = render_profile_summary(profiles)
    assert "pipe_a" in output
    assert "pipe_b" in output


def test_render_summary_empty():
    output = render_profile_summary({})
    assert "No profile" in output


def test_render_summary_shows_slowest_stage():
    profiles = {"p": _make_profile("p", {"fast": 0.5, "slow": 3.0})}
    output = render_profile_summary(profiles)
    assert "slow" in output
