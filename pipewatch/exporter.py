"""Export pipeline metrics to various formats (JSON, CSV)."""

import csv
import json
import io
from datetime import datetime
from typing import List, Optional

from pipewatch.metrics import PipelineMetrics


def metrics_to_dict(metrics: PipelineMetrics) -> dict:
    """Convert a PipelineMetrics instance to a serializable dictionary."""
    return {
        "name": metrics.name,
        "status": metrics.status,
        "total_records": metrics.total_records,
        "success_count": metrics.success_count,
        "failure_count": metrics.failure_count,
        "error_rate": round(metrics.error_rate, 4),
        "avg_latency_ms": round(metrics.avg_latency_ms, 2) if metrics.avg_latency_ms else None,
        "started_at": metrics.started_at.isoformat() if metrics.started_at else None,
        "completed_at": metrics.completed_at.isoformat() if metrics.completed_at else None,
        "exported_at": datetime.utcnow().isoformat(),
    }


def export_json(metrics_list: List[PipelineMetrics], indent: int = 2) -> str:
    """Export a list of PipelineMetrics instances to a JSON string."""
    data = [metrics_to_dict(m) for m in metrics_list]
    return json.dumps(data, indent=indent)


def export_csv(metrics_list: List[PipelineMetrics]) -> str:
    """Export a list of PipelineMetrics instances to a CSV string."""
    if not metrics_list:
        return ""

    fieldnames = [
        "name", "status", "total_records", "success_count",
        "failure_count", "error_rate", "avg_latency_ms",
        "started_at", "completed_at", "exported_at",
    ]

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for m in metrics_list:
        writer.writerow(metrics_to_dict(m))

    return output.getvalue()


def export_to_file(
    metrics_list: List[PipelineMetrics],
    filepath: str,
    fmt: str = "json",
) -> None:
    """Write exported metrics to a file. fmt must be 'json' or 'csv'."""
    if fmt == "json":
        content = export_json(metrics_list)
    elif fmt == "csv":
        content = export_csv(metrics_list)
    else:
        raise ValueError(f"Unsupported export format: '{fmt}'. Use 'json' or 'csv'.")

    with open(filepath, "w", encoding="utf-8") as fh:
        fh.write(content)


def export_summary(metrics_list: List[PipelineMetrics]) -> dict:
    """Return an aggregate summary across all provided PipelineMetrics instances.

    The summary includes total pipeline count, combined record counts, overall
    error rate, and the number of pipelines in each status.
    """
    if not metrics_list:
        return {}

    total_records = sum(m.total_records for m in metrics_list)
    total_success = sum(m.success_count for m in metrics_list)
    total_failure = sum(m.failure_count for m in metrics_list)
    overall_error_rate = round(total_failure / total_records, 4) if total_records else 0.0

    status_counts: dict = {}
    for m in metrics_list:
        status_counts[m.status] = status_counts.get(m.status, 0) + 1

    return {
        "pipeline_count": len(metrics_list),
        "total_records": total_records,
        "total_success": total_success,
        "total_failure": total_failure,
        "overall_error_rate": overall_error_rate,
        "status_counts": status_counts,
        "summarized_at": datetime.utcnow().isoformat(),
    }
