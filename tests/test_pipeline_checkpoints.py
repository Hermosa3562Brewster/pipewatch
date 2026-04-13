"""Tests for pipeline_checkpoints and checkpoint_display modules."""

from __future__ import annotations

import time
import pytest

from pipewatch.pipeline_checkpoints import Checkpoint, CheckpointTracker, CheckpointRegistry
from pipewatch.checkpoint_display import (
    render_checkpoint_list,
    render_checkpoint_summary,
    _fmt_duration,
)


# ---------------------------------------------------------------------------
# Checkpoint dataclass
# ---------------------------------------------------------------------------

def test_checkpoint_to_dict_keys():
    cp = Checkpoint(name="extract", reached_at=1000.0, duration_ms=42.5, metadata={"rows": "100"})
    d = cp.to_dict()
    assert set(d.keys()) == {"name", "reached_at", "duration_ms", "metadata"}


def test_checkpoint_from_dict_roundtrip():
    cp = Checkpoint(name="transform", reached_at=2000.0, duration_ms=10.0, metadata={})
    restored = Checkpoint.from_dict(cp.to_dict())
    assert restored.name == cp.name
    assert restored.reached_at == cp.reached_at
    assert restored.duration_ms == cp.duration_ms


def test_checkpoint_from_dict_missing_duration():
    data = {"name": "load", "reached_at": 3000.0}
    cp = Checkpoint.from_dict(data)
    assert cp.duration_ms is None
    assert cp.metadata == {}


# ---------------------------------------------------------------------------
# CheckpointTracker
# ---------------------------------------------------------------------------

def test_reach_records_checkpoint():
    tracker = CheckpointTracker("pipe1")
    cp = tracker.reach("step_a")
    assert cp.name == "step_a"
    assert tracker.latest().name == "step_a"


def test_start_and_reach_computes_duration():
    tracker = CheckpointTracker("pipe1")
    tracker.start_checkpoint("step_b")
    time.sleep(0.01)
    cp = tracker.reach("step_b")
    assert cp.duration_ms is not None
    assert cp.duration_ms >= 5.0  # at least 5 ms


def test_reach_without_start_has_no_duration():
    tracker = CheckpointTracker("pipe1")
    cp = tracker.reach("step_c")
    assert cp.duration_ms is None


def test_last_n_returns_correct_count():
    tracker = CheckpointTracker("pipe1")
    for i in range(8):
        tracker.reach(f"step_{i}")
    assert len(tracker.last_n(5)) == 5


def test_max_checkpoints_cap():
    tracker = CheckpointTracker("pipe1", max_checkpoints=5)
    for i in range(10):
        tracker.reach(f"step_{i}")
    assert len(tracker.last_n(100)) == 5


def test_clear_removes_all():
    tracker = CheckpointTracker("pipe1")
    tracker.reach("step_a")
    tracker.clear()
    assert tracker.latest() is None
    assert tracker.names() == []


def test_names_returns_ordered_names():
    tracker = CheckpointTracker("pipe1")
    for name in ["extract", "transform", "load"]:
        tracker.reach(name)
    assert tracker.names() == ["extract", "transform", "load"]


# ---------------------------------------------------------------------------
# CheckpointRegistry
# ---------------------------------------------------------------------------

def test_registry_creates_tracker_on_demand():
    reg = CheckpointRegistry()
    t = reg.tracker("alpha")
    assert t.pipeline_name == "alpha"


def test_registry_returns_same_tracker_instance():
    reg = CheckpointRegistry()
    assert reg.tracker("beta") is reg.tracker("beta")


def test_registry_remove():
    reg = CheckpointRegistry()
    reg.tracker("gamma").reach("s1")
    reg.remove("gamma")
    assert "gamma" not in reg.all_pipeline_names()


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def test_fmt_duration_none():
    assert _fmt_duration(None) == "—"


def test_fmt_duration_ms():
    result = _fmt_duration(123.4)
    assert "ms" in result


def test_fmt_duration_seconds():
    result = _fmt_duration(2500.0)
    assert "s" in result


def test_render_checkpoint_list_contains_name():
    tracker = CheckpointTracker("my_pipe")
    tracker.reach("extract", metadata={"rows": "50"})
    out = render_checkpoint_list(tracker)
    assert "my_pipe" in out
    assert "extract" in out


def test_render_checkpoint_list_empty():
    tracker = CheckpointTracker("empty_pipe")
    out = render_checkpoint_list(tracker)
    assert "no checkpoints" in out


def test_render_checkpoint_summary_contains_pipeline():
    reg = CheckpointRegistry()
    reg.tracker("pipe_x").reach("done")
    out = render_checkpoint_summary(reg)
    assert "pipe_x" in out
    assert "done" in out


def test_render_checkpoint_summary_empty():
    reg = CheckpointRegistry()
    out = render_checkpoint_summary(reg)
    assert "No checkpoint" in out
