"""Track dependencies between pipelines (upstream/downstream relationships)."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set


@dataclass
class DependencyGraph:
    """Directed graph of pipeline dependencies."""
    _upstream: Dict[str, Set[str]] = field(default_factory=dict)    # name -> set of upstream names
    _downstream: Dict[str, Set[str]] = field(default_factory=dict)  # name -> set of downstream names

    def add_dependency(self, pipeline: str, depends_on: str) -> None:
        """Register that *pipeline* depends on *depends_on*."""
        self._upstream.setdefault(pipeline, set()).add(depends_on)
        self._downstream.setdefault(depends_on, set()).add(pipeline)
        # Ensure both nodes exist in both maps
        self._upstream.setdefault(depends_on, set())
        self._downstream.setdefault(pipeline, set())

    def remove_dependency(self, pipeline: str, depends_on: str) -> None:
        """Remove a specific dependency edge."""
        self._upstream.get(pipeline, set()).discard(depends_on)
        self._downstream.get(depends_on, set()).discard(pipeline)

    def upstream(self, pipeline: str) -> List[str]:
        """Return direct upstream dependencies of *pipeline*."""
        return sorted(self._upstream.get(pipeline, set()))

    def downstream(self, pipeline: str) -> List[str]:
        """Return direct downstream dependents of *pipeline*."""
        return sorted(self._downstream.get(pipeline, set()))

    def all_pipelines(self) -> List[str]:
        """Return all known pipeline names."""
        names: Set[str] = set(self._upstream) | set(self._downstream)
        return sorted(names)

    def transitive_upstream(self, pipeline: str) -> List[str]:
        """Return all transitive upstream dependencies (BFS)."""
        visited: Set[str] = set()
        queue = list(self._upstream.get(pipeline, set()))
        while queue:
            node = queue.pop(0)
            if node not in visited:
                visited.add(node)
                queue.extend(self._upstream.get(node, set()) - visited)
        return sorted(visited)

    def has_cycle(self) -> bool:
        """Return True if the graph contains a cycle (DFS)."""
        WHITE, GRAY, BLACK = 0, 1, 2
        color: Dict[str, int] = {n: WHITE for n in self.all_pipelines()}

        def dfs(node: str) -> bool:
            color[node] = GRAY
            for nbr in self._downstream.get(node, set()):
                if color.get(nbr, WHITE) == GRAY:
                    return True
                if color.get(nbr, WHITE) == WHITE and dfs(nbr):
                    return True
            color[node] = BLACK
            return False

        return any(dfs(n) for n in self.all_pipelines() if color[n] == WHITE)

    def impact_set(self, pipeline: str) -> List[str]:
        """Return all pipelines impacted if *pipeline* fails (transitive downstream)."""
        visited: Set[str] = set()
        queue = list(self._downstream.get(pipeline, set()))
        while queue:
            node = queue.pop(0)
            if node not in visited:
                visited.add(node)
                queue.extend(self._downstream.get(node, set()) - visited)
        return sorted(visited)
