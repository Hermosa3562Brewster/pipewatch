"""Sorting utilities for pipeline metric maps."""

from typing import Dict, Callable, List, Tuple
from pipewatch.metrics import PipelineMetrics


SortKey = Callable[[PipelineMetrics], float]

_SORT_KEYS: Dict[str, SortKey] = {
    "name": lambda m: 0.0,  # handled separately as string sort
    "error_rate": lambda m: m.error_rate(),
    "successes": lambda m: float(m.success_count),
    "failures": lambda m: float(m.failure_count),
    "throughput": lambda m: float(m.success_count + m.failure_count),
    "duration": lambda m: m.avg_duration if m.avg_duration is not None else 0.0,
}


def available_sort_keys() -> List[str]:
    """Return the list of valid sort key names."""
    return list(_SORT_KEYS.keys())


def sort_pipelines(
    metrics_map: Dict[str, PipelineMetrics],
    key: str = "name",
    reverse: bool = False,
) -> List[Tuple[str, PipelineMetrics]]:
    """Sort a pipeline metrics map by the given key.

    Args:
        metrics_map: Mapping of pipeline name -> PipelineMetrics.
        key: One of the available sort keys (see available_sort_keys()).
        reverse: If True, sort in descending order.

    Returns:
        A list of (name, metrics) tuples in sorted order.

    Raises:
        ValueError: If the key is not recognised.
    """
    if key not in _SORT_KEYS:
        raise ValueError(
            f"Unknown sort key '{key}'. Valid keys: {available_sort_keys()}"
        )

    items = list(metrics_map.items())

    if key == "name":
        items.sort(key=lambda pair: pair[0].lower(), reverse=reverse)
    else:
        sort_fn = _SORT_KEYS[key]
        items.sort(key=lambda pair: sort_fn(pair[1]), reverse=reverse)

    return items


def top_n(
    metrics_map: Dict[str, PipelineMetrics],
    n: int,
    key: str = "error_rate",
    reverse: bool = True,
) -> List[Tuple[str, PipelineMetrics]]:
    """Return the top-n pipelines sorted by the given key.

    Defaults to highest error_rate first.
    """
    sorted_items = sort_pipelines(metrics_map, key=key, reverse=reverse)
    return sorted_items[:n]
