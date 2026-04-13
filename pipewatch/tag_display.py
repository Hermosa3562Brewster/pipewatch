"""Display helpers for pipeline tag information."""

from __future__ import annotations

from typing import Dict

from pipewatch.metrics import PipelineMetrics
from pipewatch.pipeline_tags import TagRegistry


def render_tag_summary(registry: TagRegistry) -> str:
    """Render a summary table of all tags and the pipelines they cover."""
    tags = registry.all_tags()
    if not tags:
        return "No tags registered.\n"

    lines = ["Tag Summary", "=" * 40]
    for tag in tags:
        pipelines = sorted(registry.pipelines_for_tag(tag))
        lines.append(f"  [{tag}]  ->  {', '.join(pipelines) if pipelines else '(none)'}")
    return "\n".join(lines) + "\n"


def render_pipeline_tags(
    pipeline_name: str, registry: TagRegistry
) -> str:
    """Render the tags associated with a single pipeline."""
    tags = sorted(registry.tags_for(pipeline_name))
    if not tags:
        return f"{pipeline_name}: (no tags)\n"
    return f"{pipeline_name}: {' '.join(f'#{t}' for t in tags)}\n"


def render_tagged_metrics(
    metrics_map: Dict[str, PipelineMetrics],
    registry: TagRegistry,
    tag: str,
) -> str:
    """Render a simple view of pipelines that carry a specific tag."""
    pipelines = sorted(registry.pipelines_for_tag(tag))
    if not pipelines:
        return f"No pipelines tagged '{tag}'.\n"

    lines = [f"Pipelines tagged #{tag}", "-" * 36]
    for name in pipelines:
        m = metrics_map.get(name)
        if m is None:
            lines.append(f"  {name:<20} (no metrics)")
        else:
            err = f"{m.error_rate * 100:.1f}%" if m.total_runs else "n/a"
            lines.append(
                f"  {name:<20} status={m.status:<10} error_rate={err}"
            )
    return "\n".join(lines) + "\n"


def print_tag_summary(registry: TagRegistry) -> None:
    print(render_tag_summary(registry))
