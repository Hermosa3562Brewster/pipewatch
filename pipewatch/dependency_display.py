"""Render pipeline dependency information to the terminal."""

from __future__ import annotations
from typing import Optional
from pipewatch.pipeline_dependencies import DependencyGraph


def render_dependency_tree(graph: DependencyGraph, root: str, indent: int = 0) -> str:
    """Render a simple ASCII tree of downstream dependents from *root*."""
    lines = []
    prefix = "  " * indent
    marker = "└─ " if indent > 0 else ""
    lines.append(f"{prefix}{marker}{root}")
    for child in graph.downstream(root):
        lines.append(render_dependency_tree(graph, child, indent + 1))
    return "\n".join(lines)


def render_dependency_table(graph: DependencyGraph) -> str:
    """Render a table showing upstream/downstream for every pipeline."""
    pipelines = graph.all_pipelines()
    if not pipelines:
        return "No dependencies registered."

    col_w = max(len(p) for p in pipelines)
    col_w = max(col_w, 12)
    header = f"{'Pipeline':<{col_w}}  {'Upstream':<30}  {'Downstream'}"
    sep = "-" * len(header)
    rows = [header, sep]
    for name in pipelines:
        up = ", ".join(graph.upstream(name)) or "—"
        down = ", ".join(graph.downstream(name)) or "—"
        rows.append(f"{name:<{col_w}}  {up:<30}  {down}")
    return "\n".join(rows)


def render_impact_summary(graph: DependencyGraph, pipeline: str) -> str:
    """Describe the blast radius if *pipeline* were to fail."""
    impacted = graph.impact_set(pipeline)
    if not impacted:
        return f"[{pipeline}] has no downstream dependents."
    lines = [f"[{pipeline}] failure would impact {len(impacted)} pipeline(s):"]
    for p in impacted:
        lines.append(f"  • {p}")
    return "\n".join(lines)


def print_dependency_table(graph: DependencyGraph) -> None:
    print(render_dependency_table(graph))


def print_impact_summary(graph: DependencyGraph, pipeline: str) -> None:
    print(render_impact_summary(graph, pipeline))
