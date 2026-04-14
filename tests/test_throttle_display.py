"""Tests for pipewatch.throttle_display."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipewatch.pipeline_throttle import PipelineThrottleManager
from pipewatch.throttle_display import render_throttle_table, render_throttle_summary


@pytest.fixture
def manager() -> PipelineThrottleManager:
    mgr = PipelineThrottleManager()
    now = datetime(2024, 6, 1, 12, 0, 0)
    mgr.configure("pipe_alpha", min_interval_seconds=60)
    mgr.configure("pipe_beta", min_interval_seconds=30)
    mgr.record_run("pipe_alpha", at=now)
    mgr.record_run("pipe_beta", at=now - timedelta(seconds=60))
    return mgr


def test_render_table_empty_manager():
    mgr = PipelineThrottleManager()
    result = render_throttle_table(mgr)
    assert "No throttle" in result


def test_render_table_contains_pipeline_names(manager):
    result = render_throttle_table(manager)
    assert "pipe_alpha" in result
    assert "pipe_beta" in result


def test_render_table_contains_header(manager):
    result = render_throttle_table(manager)
    assert "Pipeline" in result
    assert "Status" in result


def test_render_table_shows_throttled(manager):
    now = datetime(2024, 6, 1, 12, 0, 0)
    result = render_throttle_table(manager)
    assert "THROTTLED" in result


def test_render_table_shows_allowed(manager):
    result = render_throttle_table(manager)
    assert "ALLOWED" in result


def test_render_summary_contains_header(manager):
    result = render_throttle_summary(manager)
    assert "Throttle Summary" in result


def test_render_summary_shows_total(manager):
    result = render_throttle_summary(manager)
    assert "2" in result


def test_render_summary_shows_throttled_count(manager):
    result = render_throttle_summary(manager)
    assert "Throttled" in result
