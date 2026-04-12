"""Rich-text rendering for aggregated pipeline statistics."""

from typing import Dict
from pipewatch.metrics import PipelineMetrics
from pipewatch.aggregator import aggregate, top_failing, healthy_pipelines


def _health_badge(score: float) -> str:
    if score >= 0.95:
        return "[bold green]HEALTHY[/bold green]"
    elif score >= 0.75:
        return "[bold yellow]DEGRADED[/bold yellow]"
    else:
        return "[bold red]UNHEALTHY[/bold red]"


def render_aggregated_summary(metrics_map: Dict[str, PipelineMetrics]) -> str:
    """Render a multi-line summary of aggregated pipeline statistics."""
    if not metrics_map:
        return "No pipelines registered."

    stats = aggregate(metrics_map)
    lines = []

    lines.append("=== Aggregated Pipeline Summary ===")
    lines.append(f"Total Pipelines  : {stats.total_pipelines}")
    lines.append(f"Active           : {stats.active_pipelines}")
    lines.append(f"Failed           : {stats.failed_pipelines}")
    lines.append(f"Total Successes  : {stats.total_successes}")
    lines.append(f"Total Failures   : {stats.total_failures}")
    lines.append(f"Avg Error Rate   : {stats.avg_error_rate:.2%}")
    lines.append(f"Avg Throughput   : {stats.avg_throughput:.2f} rec/s")
    lines.append(f"Overall Err Rate : {stats.overall_error_rate:.2%}")
    lines.append(f"Health Score     : {stats.health_score:.2%}")

    top = top_failing(metrics_map, n=3)
    if top:
        lines.append(f"Top Failing      : {', '.join(top)}")

    healthy = healthy_pipelines(metrics_map)
    lines.append(f"Healthy Pipelines: {len(healthy)}/{stats.total_pipelines}")

    return "\n".join(lines)


def render_aggregated_table(metrics_map: Dict[str, PipelineMetrics]) -> str:
    """Render a simple table of per-pipeline stats."""
    if not metrics_map:
        return "No pipelines to display."

    header = f"{'Pipeline':<20} {'Status':<10} {'Successes':>10} {'Failures':>10} {'Err Rate':>10}"
    separator = "-" * len(header)
    rows = [header, separator]

    for name, m in sorted(metrics_map.items()):
        rows.append(
            f"{name:<20} {m.status:<10} {m.success_count:>10} "
            f"{m.failure_count:>10} {m.error_rate:>9.2%}"
        )

    return "\n".join(rows)
