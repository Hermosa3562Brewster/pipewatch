"""Registry for tracking and managing multiple pipeline metrics instances."""

from __future__ import annotations

from typing import Dict, Iterator, List, Optional

from pipewatch.metrics import PipelineMetrics


class PipelineRegistry:
    """Central registry that holds PipelineMetrics instances by name."""

    def __init__(self) -> None:
        self._pipelines: Dict[str, PipelineMetrics] = {}

    def register(self, name: str, interval: float = 60.0) -> PipelineMetrics:
        """Register a new pipeline, or return existing one if already registered."""
        if name not in self._pipelines:
            self._pipelines[name] = PipelineMetrics(name=name, interval=interval)
        return self._pipelines[name]

    def get(self, name: str) -> Optional[PipelineMetrics]:
        """Return the PipelineMetrics for *name*, or None if not found."""
        return self._pipelines.get(name)

    def remove(self, name: str) -> bool:
        """Remove a pipeline from the registry. Returns True if it existed."""
        if name in self._pipelines:
            del self._pipelines[name]
            return True
        return False

    def names(self) -> List[str]:
        """Return a sorted list of registered pipeline names."""
        return sorted(self._pipelines.keys())

    def all(self) -> Dict[str, PipelineMetrics]:
        """Return a shallow copy of the internal pipeline map."""
        return dict(self._pipelines)

    def active(self) -> List[PipelineMetrics]:
        """Return pipelines whose status is 'running'."""
        return [p for p in self._pipelines.values() if p.status == "running"]

    def failed(self) -> List[PipelineMetrics]:
        """Return pipelines whose status is 'failed'."""
        return [p for p in self._pipelines.values() if p.status == "failed"]

    def clear(self) -> None:
        """Remove all registered pipelines."""
        self._pipelines.clear()

    def __len__(self) -> int:
        return len(self._pipelines)

    def __iter__(self) -> Iterator[PipelineMetrics]:
        return iter(self._pipelines.values())

    def __contains__(self, name: str) -> bool:
        return name in self._pipelines


# Module-level default registry
_default_registry: PipelineRegistry = PipelineRegistry()


def get_default_registry() -> PipelineRegistry:
    """Return the module-level default registry."""
    return _default_registry
