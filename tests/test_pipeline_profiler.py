"""Tests for pipewatch.pipeline_profiler."""
import time

import pytest

from pipewatch.pipeline_profiler import (
    Profiler,
    PipelineProfile,
    StageProfile,
)


# ---------------------------------------------------------------------------
# StageProfile
# ---------------------------------------------------------------------------

def test_stage_profile_to_dict_keys():
    sp = StageProfile(name="extract", duration_s=1.5, started_at=0.0, ended_at=1.5)
    assert set(sp.to_dict().keys()) == {"name", "duration_s", "started_at", "ended_at"}


def test_stage_profile_roundtrip():
    sp = StageProfile(name="transform", duration_s=2.0, started_at=1.0, ended_at=3.0)
    assert StageProfile.from_dict(sp.to_dict()) == sp


# ---------------------------------------------------------------------------
# PipelineProfile helpers
# ---------------------------------------------------------------------------

def _make_profile(durations: dict) -> PipelineProfile:
    pp = PipelineProfile(pipeline_name="test_pipe")
    t = 0.0
    for name, dur in durations.items():
        pp.stages.append(StageProfile(name=name, duration_s=dur, started_at=t, ended_at=t + dur))
        t += dur
    return pp


def test_total_duration_sums_stages():
    pp = _make_profile({"extract": 1.0, "transform": 2.0, "load": 0.5})
    assert pp.total_duration() == pytest.approx(3.5)


def test_total_duration_empty():
    pp = PipelineProfile(pipeline_name="empty")
    assert pp.total_duration() == 0.0


def test_slowest_stage_returns_max():
    pp = _make_profile({"extract": 1.0, "transform": 5.0, "load": 0.5})
    assert pp.slowest_stage().name == "transform"


def test_slowest_stage_empty_returns_none():
    pp = PipelineProfile(pipeline_name="empty")
    assert pp.slowest_stage() is None


def test_stage_share_sums_to_one():
    pp = _make_profile({"a": 1.0, "b": 3.0})
    shares = pp.stage_share()
    assert sum(shares.values()) == pytest.approx(1.0)


def test_stage_share_zero_total():
    pp = _make_profile({"a": 0.0, "b": 0.0})
    shares = pp.stage_share()
    assert all(v == 0.0 for v in shares.values())


# ---------------------------------------------------------------------------
# Profiler
# ---------------------------------------------------------------------------

def test_profiler_records_stages():
    p = Profiler("my_pipe")
    p.begin_stage("extract")
    time.sleep(0.01)
    p.end_stage()
    profile = p.finish()
    assert len(profile.stages) == 1
    assert profile.stages[0].name == "extract"
    assert profile.stages[0].duration_s >= 0.01


def test_profiler_multiple_stages():
    p = Profiler("pipe")
    for stage in ("extract", "transform", "load"):
        p.begin_stage(stage)
        p.end_stage()
    profile = p.finish()
    assert [s.name for s in profile.stages] == ["extract", "transform", "load"]


def test_profiler_begin_without_end_auto_closes():
    """Calling begin_stage again should auto-close the previous stage."""
    p = Profiler("pipe")
    p.begin_stage("extract")
    p.begin_stage("transform")  # should close 'extract'
    profile = p.finish()
    names = [s.name for s in profile.stages]
    assert "extract" in names
    assert "transform" in names


def test_profiler_finish_returns_profile():
    p = Profiler("pipe")
    p.begin_stage("only")
    result = p.finish()
    assert isinstance(result, PipelineProfile)
    assert result.pipeline_name == "pipe"
