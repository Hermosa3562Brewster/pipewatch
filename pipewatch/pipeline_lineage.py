"""Track data lineage between pipelines (inputs/outputs/datasets)."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set


@dataclass
class LineageNode:
    name: str
    inputs: List[str] = field(default_factory=list)
    outputs: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"name": self.name, "inputs": self.inputs, "outputs": self.outputs}

    @classmethod
    def from_dict(cls, d: dict) -> "LineageNode":
        return cls(name=d["name"], inputs=d.get("inputs", []), outputs=d.get("outputs", []))

    def summary(self) -> str:
        return f"{self.name}: {len(self.inputs)} input(s), {len(self.outputs)} output(s)"


class LineageRegistry:
    def __init__(self) -> None:
        self._nodes: Dict[str, LineageNode] = {}

    def register(self, pipeline: str, inputs: Optional[List[str]] = None, outputs: Optional[List[str]] = None) -> LineageNode:
        node = self._nodes.setdefault(pipeline, LineageNode(name=pipeline))
        if inputs:
            for ds in inputs:
                if ds not in node.inputs:
                    node.inputs.append(ds)
        if outputs:
            for ds in outputs:
                if ds not in node.outputs:
                    node.outputs.append(ds)
        return node

    def get(self, pipeline: str) -> Optional[LineageNode]:
        return self._nodes.get(pipeline)

    def all_pipelines(self) -> List[str]:
        return list(self._nodes.keys())

    def producers_of(self, dataset: str) -> List[str]:
        return [n.name for n in self._nodes.values() if dataset in n.outputs]

    def consumers_of(self, dataset: str) -> List[str]:
        return [n.name for n in self._nodes.values() if dataset in n.inputs]

    def all_datasets(self) -> Set[str]:
        ds: Set[str] = set()
        for n in self._nodes.values():
            ds.update(n.inputs)
            ds.update(n.outputs)
        return ds

    def remove(self, pipeline: str) -> None:
        self._nodes.pop(pipeline, None)
