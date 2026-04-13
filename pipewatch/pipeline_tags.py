"""Tag-based grouping and filtering for pipelines."""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, List, Set

from pipewatch.metrics import PipelineMetrics


class TagRegistry:
    """Maintains a mapping of tags to pipeline names."""

    def __init__(self) -> None:
        self._tags: Dict[str, Set[str]] = defaultdict(set)  # tag -> pipeline names
        self._pipeline_tags: Dict[str, Set[str]] = defaultdict(set)  # name -> tags

    def tag(self, pipeline_name: str, *tags: str) -> None:
        """Associate one or more tags with a pipeline."""
        for tag in tags:
            self._tags[tag].add(pipeline_name)
            self._pipeline_tags[pipeline_name].add(tag)

    def untag(self, pipeline_name: str, *tags: str) -> None:
        """Remove one or more tags from a pipeline."""
        for tag in tags:
            self._tags[tag].discard(pipeline_name)
            self._pipeline_tags[pipeline_name].discard(tag)

    def tags_for(self, pipeline_name: str) -> Set[str]:
        """Return all tags associated with a pipeline."""
        return set(self._pipeline_tags.get(pipeline_name, set()))

    def pipelines_for_tag(self, tag: str) -> Set[str]:
        """Return all pipeline names associated with a tag."""
        return set(self._tags.get(tag, set()))

    def all_tags(self) -> List[str]:
        """Return a sorted list of all known tags."""
        return sorted(self._tags.keys())

    def filter_by_tags(
        self,
        metrics_map: Dict[str, PipelineMetrics],
        tags: Iterable[str],
        match_all: bool = False,
    ) -> Dict[str, PipelineMetrics]:
        """Return pipelines whose tags overlap (or fully match) the given tags.

        Args:
            metrics_map: Mapping of pipeline name -> PipelineMetrics.
            tags: Tags to filter by.
            match_all: If True, pipeline must have ALL given tags; otherwise ANY.
        """
        tag_set = set(tags)
        result: Dict[str, PipelineMetrics] = {}
        for name, metrics in metrics_map.items():
            pipeline_tags = self.tags_for(name)
            if match_all:
                if tag_set.issubset(pipeline_tags):
                    result[name] = metrics
            else:
                if tag_set & pipeline_tags:
                    result[name] = metrics
        return result

    def remove_pipeline(self, pipeline_name: str) -> None:
        """Remove a pipeline and all its tag associations."""
        for tag in list(self._pipeline_tags.get(pipeline_name, [])):
            self._tags[tag].discard(pipeline_name)
        self._pipeline_tags.pop(pipeline_name, None)
