"""Key-value label support for pipelines (e.g. env=prod, team=data)."""
from __future__ import annotations
from collections import defaultdict
from typing import Dict, Iterable, Optional


class LabelRegistry:
    """Stores arbitrary key=value labels per pipeline."""

    def __init__(self) -> None:
        self._labels: Dict[str, Dict[str, str]] = defaultdict(dict)

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def set(self, pipeline: str, key: str, value: str) -> None:
        """Set a label on a pipeline."""
        self._labels[pipeline][key] = value

    def remove(self, pipeline: str, key: str) -> bool:
        """Remove a label key from a pipeline. Returns True if it existed."""
        return self._labels[pipeline].pop(key, None) is not None

    def clear(self, pipeline: str) -> None:
        """Remove all labels from a pipeline."""
        self._labels.pop(pipeline, None)

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get(self, pipeline: str, key: str) -> Optional[str]:
        """Return the value of a label, or None if absent."""
        return self._labels.get(pipeline, {}).get(key)

    def labels_for(self, pipeline: str) -> Dict[str, str]:
        """Return all labels for a pipeline (empty dict if none)."""
        return dict(self._labels.get(pipeline, {}))

    def pipelines_with_label(self, key: str, value: Optional[str] = None) -> list:
        """Return pipelines that have *key* (optionally matching *value*)."""
        result = []
        for pipeline, labels in self._labels.items():
            if key in labels:
                if value is None or labels[key] == value:
                    result.append(pipeline)
        return sorted(result)

    def all_keys(self) -> list:
        """Return the sorted union of all label keys across all pipelines."""
        keys: set = set()
        for labels in self._labels.values():
            keys.update(labels.keys())
        return sorted(keys)

    def filter_by_labels(
        self, requirements: Dict[str, str], names: Iterable[str]
    ) -> list:
        """Return subset of *names* whose labels satisfy all *requirements*."""
        result = []
        for name in names:
            labels = self._labels.get(name, {})
            if all(labels.get(k) == v for k, v in requirements.items()):
                result.append(name)
        return result
