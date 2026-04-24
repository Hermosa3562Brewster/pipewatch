"""Tests for pipeline embargo windows."""
from __future__ import annotations

from datetime import datetime, time

import pytest

from pipewatch.pipeline_embargo import EmbargoManager, EmbargoWindow


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def manager() -> EmbargoManager:
    return EmbargoManager()


def _window(pipeline: str = "etl", start: str = "02:00", end: str = "04:00", **kw) -> EmbargoWindow:
    return EmbargoWindow(
        pipeline=pipeline,
        start_time=time.fromisoformat(start),
        end_time=time.fromisoformat(end),
        **kw,
    )


# ---------------------------------------------------------------------------
# EmbargoWindow.is_active
# ---------------------------------------------------------------------------

def test_is_active_inside_window():
    w = _window(start="02:00", end="04:00")
    assert w.is_active(datetime(2024, 1, 1, 3, 0)) is True


def test_is_active_outside_window():
    w = _window(start="02:00", end="04:00")
    assert w.is_active(datetime(2024, 1, 1, 5, 0)) is False


def test_is_active_overnight_inside():
    w = _window(start="22:00", end="06:00")
    assert w.is_active(datetime(2024, 1, 1, 23, 30)) is True


def test_is_active_overnight_early_morning():
    w = _window(start="22:00", end="06:00")
    assert w.is_active(datetime(2024, 1, 1, 3, 0)) is True


def test_is_active_overnight_outside():
    w = _window(start="22:00", end="06:00")
    assert w.is_active(datetime(2024, 1, 1, 12, 0)) is False


def test_disabled_window_never_active():
    w = _window(start="00:00", end="23:59", enabled=False)
    assert w.is_active(datetime(2024, 1, 1, 12, 0)) is False


# ---------------------------------------------------------------------------
# to_dict / from_dict
# ---------------------------------------------------------------------------

def test_to_dict_keys():
    w = _window(reason="maintenance")
    d = w.to_dict()
    assert set(d.keys()) == {"pipeline", "start_time", "end_time", "reason", "enabled"}


def test_from_dict_roundtrip():
    w = _window(reason="deploy freeze")
    w2 = EmbargoWindow.from_dict(w.to_dict())
    assert w2.pipeline == w.pipeline
    assert w2.start_time == w.start_time
    assert w2.end_time == w.end_time
    assert w2.reason == w.reason
    assert w2.enabled == w.enabled


def test_summary_contains_pipeline():
    w = _window(pipeline="loader", reason="freeze")
    assert "loader" in w.summary()
    assert "freeze" in w.summary()


# ---------------------------------------------------------------------------
# EmbargoManager
# ---------------------------------------------------------------------------

def test_add_and_windows_for(manager):
    manager.add(_window("pipe_a"))
    assert len(manager.windows_for("pipe_a")) == 1


def test_is_embargoed_true(manager):
    manager.add(_window("pipe_a", start="02:00", end="04:00"))
    assert manager.is_embargoed("pipe_a", datetime(2024, 1, 1, 3, 0)) is True


def test_is_embargoed_false(manager):
    manager.add(_window("pipe_a", start="02:00", end="04:00"))
    assert manager.is_embargoed("pipe_a", datetime(2024, 1, 1, 10, 0)) is False


def test_is_embargoed_unknown_pipeline(manager):
    assert manager.is_embargoed("ghost") is False


def test_remove_window(manager):
    manager.add(_window("pipe_b"))
    manager.add(_window("pipe_b", start="10:00", end="12:00"))
    manager.remove("pipe_b", 0)
    assert len(manager.windows_for("pipe_b")) == 1


def test_remove_invalid_index_does_not_raise(manager):
    manager.add(_window("pipe_c"))
    manager.remove("pipe_c", 99)  # should not raise
    assert len(manager.windows_for("pipe_c")) == 1


def test_all_pipelines(manager):
    manager.add(_window("alpha"))
    manager.add(_window("beta"))
    assert set(manager.all_pipelines()) == {"alpha", "beta"}
