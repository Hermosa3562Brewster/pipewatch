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
