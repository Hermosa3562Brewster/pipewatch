"""CLI display utilities for pipewatch — renders pipeline metrics and alerts to the terminal."""

from typing import List
from pipewatch.metrics import PipelineMetrics


SEP = "-" * 50


def _status_badge(status: str) -> str:
    badges = {
        "idle": "\u25cb IDLE",
        "running": "\u25b6 RUNNING",
        "completed": "\u2714 COMPLETED",
        "failed": "\u2718 FAILED",
    }
    return badges.get(status, status.upper())


def render_metrics(metrics: PipelineMetrics) -> str:
    """Return a formatted string summary of the given pipeline metrics."""
    lines = [
        SEP,
        f"  Pipeline : {metrics.pipeline_name}",
        f"  Status   : {_status_badge(metrics.status)}",
        f"  Success  : {metrics.success_count}",
        f"  Failures : {metrics.failure_count}",
        f"  Total    : {metrics.total_count}",
        f"  Error %  : {metrics.error_rate * 100:.1f}%",
        f"  Throughput: {metrics.throughput:.2f} rec/s",
    ]
    if metrics.duration is not None:
        lines.append(f"  Duration : {metrics.duration:.2f}s")
    if metrics.last_error:
        lines.append(f"  Last Err : {metrics.last_error}")
    lines.append(SEP)
    return "\n".join(lines)


def render_alerts(alert_messages: List[str]) -> str:
    """Return a formatted string for a list of alert messages."""
    if not alert_messages:
        return "  No active alerts."
    lines = ["  !! Active Alerts !!"]
    for msg in alert_messages:
        lines.append(f"  {msg}")
    return "\n".join(lines)


def render_dashboard(metrics: PipelineMetrics, alert_messages: List[str]) -> str:
    """Combine metrics and alerts into a single dashboard string."""
    return "\n".join([
        render_metrics(metrics),
        render_alerts(alert_messages),
        SEP,
    ])
