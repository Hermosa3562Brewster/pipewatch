"""Filtering utilities for selecting pipelines by name, status, or health."""

from __future__ import annotations

from typing import Callable, Dict, List, Optional

from pipewatch.metrics import PipelineMetrics


def filter_by_name(
    metrics_map: Dict[str, PipelineMetrics],
    names: List[str],
) -> Dict[str, PipelineMetrics]:
    """Return only pipelines whose names are in *names*."""
    return {k: v for k, v in metrics_map.items() if k in names}


def filter_by_status(
    metrics_map: Dict[str, PipelineMetrics],
    status: str,
) -> Dict[str, PipelineMetrics]:
    """Return only pipelines whose current status matches *status*.

    Common statuses: 'idle', 'running', 'failed', 'completed'.
    """
    return {k: v for k, v in metrics_map.items() if v.status == status}


def filter_by_error_rate(
    metrics_map: Dict[str, PipelineMetrics],
    min_rate: float = 0.0,
    max_rate: float = 1.0,
) -> Dict[str, PipelineMetrics]:
    """Return pipelines whose error rate falls within [min_rate, max_rate]."""
    result = {}
    for name, m in metrics_map.items():
        rate = m.error_rate()
        if min_rate <= rate <= max_rate:
            result[name] = m
    return result


def filter_by_predicate(
    metrics_map: Dict[str, PipelineMetrics],
    predicate: Callable[[str, PipelineMetrics], bool],
) -> Dict[str, PipelineMetrics]:
    """Return pipelines for which *predicate(name, metrics)* is True."""
    return {k: v for k, v in metrics_map.items() if predicate(k, v)}


def apply_filters(
    metrics_map: Dict[str, PipelineMetrics],
    names: Optional[List[str]] = None,
    status: Optional[str] = None,
    max_error_rate: Optional[float] = None,
) -> Dict[str, PipelineMetrics]:
    """Convenience wrapper that chains multiple optional filters."""
    result = dict(metrics_map)
    if names:
        result = filter_by_name(result, names)
    if status is not None:
        result = filter_by_status(result, status)
    if max_error_rate is not None:
        result = filter_by_error_rate(result, max_rate=max_error_rate)
    return result
