"""Audit log for pipeline configuration and state changes."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


_VALID_ACTIONS = {"created", "updated", "deleted", "enabled", "disabled", "reset"}


@dataclass
class AuditEntry:
    pipeline: str
    action: str
    actor: str
    detail: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "action": self.action,
            "actor": self.actor,
            "detail": self.detail,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AuditEntry":
        return cls(
            pipeline=data["pipeline"],
            action=data["action"],
            actor=data["actor"],
            detail=data["detail"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            metadata=data.get("metadata", {}),
        )

    def summary(self) -> str:
        ts = self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        return f"[{ts}] {self.actor} {self.action} '{self.pipeline}': {self.detail}"


class AuditLog:
    def __init__(self, max_per_pipeline: int = 200) -> None:
        self._entries: Dict[str, List[AuditEntry]] = {}
        self._max = max_per_pipeline

    def record(
        self,
        pipeline: str,
        action: str,
        actor: str,
        detail: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AuditEntry:
        if action not in _VALID_ACTIONS:
            raise ValueError(f"Unknown audit action '{action}'. Valid: {_VALID_ACTIONS}")
        entry = AuditEntry(
            pipeline=pipeline,
            action=action,
            actor=actor,
            detail=detail,
            metadata=metadata or {},
        )
        bucket = self._entries.setdefault(pipeline, [])
        bucket.append(entry)
        if len(bucket) > self._max:
            bucket.pop(0)
        return entry

    def get(self, pipeline: str, last_n: Optional[int] = None) -> List[AuditEntry]:
        entries = self._entries.get(pipeline, [])
        if last_n is not None:
            return entries[-last_n:]
        return list(entries)

    def all_entries(self) -> List[AuditEntry]:
        result = []
        for entries in self._entries.values():
            result.extend(entries)
        result.sort(key=lambda e: e.timestamp)
        return result

    def pipelines(self) -> List[str]:
        return list(self._entries.keys())

    def clear(self, pipeline: str) -> None:
        self._entries.pop(pipeline, None)
