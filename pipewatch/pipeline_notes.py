"""Attach and retrieve operator notes/annotations on pipelines."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional


@dataclass
class PipelineNote:
    pipeline: str
    message: str
    author: str = "operator"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "message": self.message,
            "author": self.author,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PipelineNote":
        return cls(
            pipeline=data["pipeline"],
            message=data["message"],
            author=data.get("author", "operator"),
            created_at=datetime.fromisoformat(data["created_at"]),
        )


class NoteBook:
    """Stores notes per pipeline, with optional max-per-pipeline cap."""

    def __init__(self, max_per_pipeline: int = 50) -> None:
        self._notes: Dict[str, List[PipelineNote]] = {}
        self.max_per_pipeline = max_per_pipeline

    def add(self, pipeline: str, message: str, author: str = "operator") -> PipelineNote:
        note = PipelineNote(pipeline=pipeline, message=message, author=author)
        bucket = self._notes.setdefault(pipeline, [])
        bucket.append(note)
        if len(bucket) > self.max_per_pipeline:
            bucket.pop(0)
        return note

    def get(self, pipeline: str, last_n: Optional[int] = None) -> List[PipelineNote]:
        notes = self._notes.get(pipeline, [])
        if last_n is not None:
            return notes[-last_n:]
        return list(notes)

    def all_pipelines(self) -> List[str]:
        return [p for p, notes in self._notes.items() if notes]

    def clear(self, pipeline: str) -> None:
        self._notes.pop(pipeline, None)

    def total_count(self, pipeline: str) -> int:
        return len(self._notes.get(pipeline, []))
