"""Rendering helpers for pipeline labels."""
from __future__ import annotations
from typing import Dict, Iterable

from pipewatch.pipeline_labels import LabelRegistry


def _format_labels(labels: Dict[str, str]) -> str:
    if not labels:
        return "(none)"
    return "  ".join(f"{k}={v}" for k, v in sorted(labels.items()))


def render_labels_for_pipeline(registry: LabelRegistry, pipeline: str) -> str:
    """Return a single-pipeline label block."""
    labels = registry.labels_for(pipeline)
    lines = [
        f"Labels for [{pipeline}]",
        "-" * 40,
    ]
    if not labels:
        lines.append("  No labels set.")
    else:
        for k, v in sorted(labels.items()):
            lines.append(f"  {k:<20} {v}")
    return "\n".join(lines)


def render_labels_table(
    registry: LabelRegistry, pipelines: Iterable[str]
) -> str:
    """Render a table showing all labels for each pipeline."""
    pipelines = list(pipelines)
    if not pipelines:
        return "No pipelines."

    all_keys = registry.all_keys()
    col_width = max((len(k) for k in all_keys), default=5)
    name_width = max(len(p) for p in pipelines)

    header_keys = "  ".join(k.ljust(col_width) for k in all_keys) if all_keys else "(no labels)"
    lines = [
        f"{'Pipeline':<{name_width}}  {header_keys}",
        "-" * (name_width + 2 + max(len(header_keys), 10)),
    ]
    for pipeline in sorted(pipelines):
        labels = registry.labels_for(pipeline)
        values = "  ".join(
            labels.get(k, "-").ljust(col_width) for k in all_keys
        ) if all_keys else "(none)"
        lines.append(f"{pipeline:<{name_width}}  {values}")
    return "\n".join(lines)


def render_label_summary(registry: LabelRegistry, pipelines: Iterable[str]) -> str:
    """One-line summary of label coverage."""
    pipelines = list(pipelines)
    total = len(pipelines)
    labelled = sum(1 for p in pipelines if registry.labels_for(p))
    keys = registry.all_keys()
    return (
        f"Label summary: {labelled}/{total} pipelines labelled  "
        f"|  Distinct keys: {len(keys)}  ({', '.join(keys) or 'none'})"
    )


def print_labels(registry: LabelRegistry, pipelines: Iterable[str]) -> None:
    pipelines = list(pipelines)
    print(render_label_summary(registry, pipelines))
    print()
    print(render_labels_table(registry, pipelines))
