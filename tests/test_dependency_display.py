"""Tests for pipewatch.dependency_display."""

import pytest
from pipewatch.pipeline_dependencies import DependencyGraph
from pipewatch.dependency_display import (
    render_dependency_table,
    render_dependency_tree,
    render_impact_summary,
)


@pytest.fixture
def graph() -> DependencyGraph:
    g = DependencyGraph()
    g.add_dependency("transform", "ingest")
    g.add_dependency("load", "transform")
    return g


def test_render_table_contains_pipeline_names(graph):
    out = render_dependency_table(graph)
    assert "ingest" in out
    assert "transform" in out
    assert "load" in out


def test_render_table_shows_upstream(graph):
    out = render_dependency_table(graph)
    assert "ingest" in out


def test_render_table_shows_downstream(graph):
    out = render_dependency_table(graph)
    assert "load" in out


def test_render_table_empty_graph():
    out = render_dependency_table(DependencyGraph())
    assert "No dependencies" in out


def test_render_tree_contains_root(graph):
    out = render_dependency_tree(graph, "ingest")
    assert "ingest" in out


def test_render_tree_contains_child(graph):
    out = render_dependency_tree(graph, "ingest")
    assert "transform" in out


def test_render_tree_contains_grandchild(graph):
    out = render_dependency_tree(graph, "ingest")
    assert "load" in out


def test_render_impact_summary_lists_impacted(graph):
    out = render_impact_summary(graph, "ingest")
    assert "transform" in out
    assert "load" in out


def test_render_impact_summary_no_downstream(graph):
    out = render_impact_summary(graph, "load")
    assert "no downstream" in out.lower()


def test_render_impact_summary_shows_count(graph):
    out = render_impact_summary(graph, "ingest")
    assert "2" in out
