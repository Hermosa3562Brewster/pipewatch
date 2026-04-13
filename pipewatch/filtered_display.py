"""Render a filtered view of pipeline metrics in the terminal."""

from __future__ import annotations

from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetrics
from pipewatch.pipeline_filter import apply_filters
from pipewatch.display import render_metrics


SEPARATOR = "-" * 50


def render_filtered_view(
    metrics_map: Dict[str, PipelineMetrics],
    names: Optional[List[str]] = None,
    status: Optional[str] = None,
    max_error_rate: Optional[float] = None,
) -> str:
    """Return a multi-pipeline report limited to pipelines matching filters."""
    filtered = apply_filters(
        metrics_map,
        names=names,
        status=status,
        max_error_rate=max_error_rate,
    )

    if not filtered:
        return "[pipewatch] No pipelines match the current filter criteria.\n"

    lines: List[str] = [
        f"[pipewatch] Showing {len(filtered)} / {len(metrics_map)} pipeline(s)",
        SEPARATOR,
    ]

    for name, m in sorted(filtered.items()):
        lines.append(render_metrics(name, m))
        lines.append(SEPARATOR)

    return "\n".join(lines) + "\n"


def print_filtered_view(
    metrics_map: Dict[str, PipelineMetrics],
    names: Optional[List[str]] = None,
    status: Optional[str] = None,
    max_error_rate: Optional[float] = None,
) -> None:
    """Print the filtered view directly to stdout."""
    print(render_filtered_view(metrics_map, names=names, status=status, max_error_rate=max_error_rate), end="")
