"""Tests for pipewatch.pause_display."""
from __future__ import annotations

import pytest

from pipewatch.pipeline_pause import PauseManager
from pipewatch.pause_display import (
    render_pause_summary,
    render_pause_table,
)


@pytest.fixture
def manager() -> PauseManager:
    return PauseManager()


def test_render_table_contains_pipeline_name(manager):
    manager.pause("etl_a")
    result = render_pause_table(manager, ["etl_a"])
    assert "etl_a" in result


def test_render_table_shows_paused_status(manager):
    manager.pause("etl_a")
    result = render_pause_table(manager, ["etl_a"])
    assert "PAUSED" in result


def test_render_table_shows_active_status_after_resume(manager):
    manager.pause("etl_a")
    manager.resume("etl_a")
    result = render_pause_table(manager, ["etl_a"])
    assert "active" in result


def test_render_table_no_history_shows_active(manager):
    result = render_pause_table(manager, ["etl_z"])
    assert "etl_z" in result
    assert "active" in result


def test_render_table_shows_reason(manager):
    manager.pause("etl_a", reason="scheduled maintenance")
    result = render_pause_table(manager, ["etl_a"])
    assert "scheduled maintenance" in result


def test_render_summary_header(manager):
    result = render_pause_summary(manager)
    assert "Pause Summary" in result


def test_render_summary_shows_paused_count(manager):
    manager.pause("etl_a")
    manager.pause("etl_b")
    result = render_pause_summary(manager)
    assert "2" in result


def test_render_summary_lists_paused_pipelines(manager):
    manager.pause("etl_a", reason="deploy")
    result = render_pause_summary(manager)
    assert "etl_a" in result


def test_render_summary_zero_when_none_paused(manager):
    result = render_pause_summary(manager)
    assert "0" in result
