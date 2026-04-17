"""Pipeline metadata: arbitrary key-value store per pipeline."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional


@dataclass
class MetadataEntry:
    key: str
    value: Any
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "value": self.value,
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "MetadataEntry":
        return cls(
            key=d["key"],
            value=d["value"],
            updated_at=datetime.fromisoformat(d["updated_at"]),
        )


class MetadataStore:
    def __init__(self) -> None:
        self._store: Dict[str, Dict[str, MetadataEntry]] = {}

    def set(self, pipeline: str, key: str, value: Any) -> MetadataEntry:
        self._store.setdefault(pipeline, {})
        entry = MetadataEntry(key=key, value=value)
        self._store[pipeline][key] = entry
        return entry

    def get(self, pipeline: str, key: str) -> Optional[Any]:
        return self._store.get(pipeline, {}).get(key, None) and self._store[pipeline][key].value

    def get_entry(self, pipeline: str, key: str) -> Optional[MetadataEntry]:
        return self._store.get(pipeline, {}).get(key)

    def all_for(self, pipeline: str) -> Dict[str, MetadataEntry]:
        return dict(self._store.get(pipeline, {}))

    def remove(self, pipeline: str, key: str) -> bool:
        if pipeline in self._store and key in self._store[pipeline]:
            del self._store[pipeline][key]
            return True
        return False

    def clear(self, pipeline: str) -> None:
        self._store.pop(pipeline, None)

    def all_pipelines(self) -> list:
        return list(self._store.keys())
