"""Tests for pipeline_tags and tag_display modules."""

from __future__ import annotations

import pytest

from pipewatch.pipeline_tags import TagRegistry
from pipewatch.tag_display import (
    render_tag_summary,
    render_pipeline_tags,
    render_tagged_metrics,
)
from pipewatch.metrics import PipelineMetrics


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_metrics(name: str, status: str = "idle") -> PipelineMetrics:
    m = PipelineMetrics(name=name)
    m.status = status
    return m


@pytest.fixture()
def registry() -> TagRegistry:
    return TagRegistry()


# ---------------------------------------------------------------------------
# TagRegistry
# ---------------------------------------------------------------------------

def test_tag_associates_pipeline_with_tag(registry):
    registry.tag("etl_sales", "sales", "nightly")
    assert "sales" in registry.tags_for("etl_sales")
    assert "nightly" in registry.tags_for("etl_sales")


def test_pipelines_for_tag_returns_correct_set(registry):
    registry.tag("etl_sales", "sales")
    registry.tag("etl_marketing", "sales")
    assert registry.pipelines_for_tag("sales") == {"etl_sales", "etl_marketing"}


def test_untag_removes_association(registry):
    registry.tag("etl_sales", "sales", "nightly")
    registry.untag("etl_sales", "sales")
    assert "sales" not in registry.tags_for("etl_sales")
    assert "nightly" in registry.tags_for("etl_sales")


def test_all_tags_sorted(registry):
    registry.tag("p1", "zebra", "alpha")
    assert registry.all_tags() == ["alpha", "zebra"]


def test_remove_pipeline_clears_all_tags(registry):
    registry.tag("etl_sales", "sales", "nightly")
    registry.remove_pipeline("etl_sales")
    assert registry.tags_for("etl_sales") == set()
    assert "etl_sales" not in registry.pipelines_for_tag("sales")


def test_filter_by_tags_any_match(registry):
    registry.tag("p1", "sales")
    registry.tag("p2", "marketing")
    registry.tag("p3", "sales", "marketing")
    metrics_map = {n: _make_metrics(n) for n in ["p1", "p2", "p3"]}
    result = registry.filter_by_tags(metrics_map, ["sales"])
    assert set(result.keys()) == {"p1", "p3"}


def test_filter_by_tags_match_all(registry):
    registry.tag("p1", "sales")
    registry.tag("p2", "sales", "nightly")
    metrics_map = {n: _make_metrics(n) for n in ["p1", "p2"]}
    result = registry.filter_by_tags(metrics_map, ["sales", "nightly"], match_all=True)
    assert set(result.keys()) == {"p2"}


def test_filter_by_tags_no_match_returns_empty(registry):
    registry.tag("p1", "sales")
    metrics_map = {"p1": _make_metrics("p1")}
    result = registry.filter_by_tags(metrics_map, ["unknown_tag"])
    assert result == {}


# ---------------------------------------------------------------------------
# tag_display
# ---------------------------------------------------------------------------

def test_render_tag_summary_no_tags(registry):
    output = render_tag_summary(registry)
    assert "No tags" in output


def test_render_tag_summary_shows_tag_and_pipeline(registry):
    registry.tag("etl_sales", "sales")
    output = render_tag_summary(registry)
    assert "sales" in output
    assert "etl_sales" in output


def test_render_pipeline_tags_no_tags(registry):
    output = render_pipeline_tags("etl_sales", registry)
    assert "no tags" in output


def test_render_pipeline_tags_with_tags(registry):
    registry.tag("etl_sales", "nightly", "sales")
    output = render_pipeline_tags("etl_sales", registry)
    assert "#nightly" in output
    assert "#sales" in output


def test_render_tagged_metrics_unknown_tag(registry):
    output = render_tagged_metrics({}, registry, "ghost")
    assert "No pipelines" in output


def test_render_tagged_metrics_shows_pipeline_info(registry):
    registry.tag("p1", "sales")
    m = _make_metrics("p1", status="running")
    output = render_tagged_metrics({"p1": m}, registry, "sales")
    assert "p1" in output
    assert "running" in output
