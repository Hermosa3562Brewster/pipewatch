"""Tests for pipewatch.pipeline_dependencies."""

import pytest
from pipewatch.pipeline_dependencies import DependencyGraph


@pytest.fixture
def graph() -> DependencyGraph:
    return DependencyGraph()


def test_add_dependency_registers_upstream(graph):
    graph.add_dependency("b", "a")
    assert "a" in graph.upstream("b")


def test_add_dependency_registers_downstream(graph):
    graph.add_dependency("b", "a")
    assert "b" in graph.downstream("a")


def test_remove_dependency(graph):
    graph.add_dependency("b", "a")
    graph.remove_dependency("b", "a")
    assert graph.upstream("b") == []
    assert graph.downstream("a") == []


def test_all_pipelines_returns_all_nodes(graph):
    graph.add_dependency("b", "a")
    graph.add_dependency("c", "b")
    assert set(graph.all_pipelines()) == {"a", "b", "c"}


def test_upstream_unknown_pipeline_returns_empty(graph):
    assert graph.upstream("ghost") == []


def test_downstream_unknown_pipeline_returns_empty(graph):
    assert graph.downstream("ghost") == []


def test_transitive_upstream(graph):
    graph.add_dependency("c", "b")
    graph.add_dependency("b", "a")
    result = graph.transitive_upstream("c")
    assert "a" in result
    assert "b" in result


def test_transitive_upstream_direct_only(graph):
    graph.add_dependency("b", "a")
    result = graph.transitive_upstream("b")
    assert result == ["a"]


def test_has_cycle_detects_cycle(graph):
    graph.add_dependency("b", "a")
    graph.add_dependency("c", "b")
    graph.add_dependency("a", "c")  # creates a -> b -> c -> a
    assert graph.has_cycle() is True


def test_has_cycle_no_cycle(graph):
    graph.add_dependency("b", "a")
    graph.add_dependency("c", "b")
    assert graph.has_cycle() is False


def test_impact_set_direct(graph):
    graph.add_dependency("b", "a")
    assert "b" in graph.impact_set("a")


def test_impact_set_transitive(graph):
    graph.add_dependency("b", "a")
    graph.add_dependency("c", "b")
    impact = graph.impact_set("a")
    assert "b" in impact
    assert "c" in impact


def test_impact_set_no_downstream(graph):
    graph.add_dependency("b", "a")
    assert graph.impact_set("b") == []


def test_multiple_upstreams(graph):
    graph.add_dependency("c", "a")
    graph.add_dependency("c", "b")
    assert sorted(graph.upstream("c")) == ["a", "b"]
