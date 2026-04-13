"""Runbook entries for pipelines — attach remediation notes to alert conditions."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional


@dataclass
class RunbookEntry:
    pipeline: str
    title: str
    condition: str
    steps: List[str]
    author: str = "unknown"
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "title": self.title,
            "condition": self.condition,
            "steps": list(self.steps),
            "author": self.author,
            "created_at": self.created_at,
        }

    @staticmethod
    def from_dict(data: dict) -> "RunbookEntry":
        return RunbookEntry(
            pipeline=data["pipeline"],
            title=data["title"],
            condition=data["condition"],
            steps=list(data.get("steps", [])),
            author=data.get("author", "unknown"),
            created_at=data.get("created_at", ""),
        )

    def summary(self) -> str:
        return f"[{self.pipeline}] {self.title} — {self.condition}"


class Runbook:
    """Store and retrieve runbook entries keyed by pipeline name."""

    def __init__(self) -> None:
        self._entries: Dict[str, List[RunbookEntry]] = {}

    def add(self, entry: RunbookEntry) -> RunbookEntry:
        self._entries.setdefault(entry.pipeline, []).append(entry)
        return entry

    def get(self, pipeline: str) -> List[RunbookEntry]:
        return list(self._entries.get(pipeline, []))

    def remove(self, pipeline: str, title: str) -> bool:
        entries = self._entries.get(pipeline, [])
        before = len(entries)
        self._entries[pipeline] = [e for e in entries if e.title != title]
        return len(self._entries[pipeline]) < before

    def all_pipelines(self) -> List[str]:
        return sorted(self._entries.keys())

    def find_by_condition(self, keyword: str) -> List[RunbookEntry]:
        keyword_lower = keyword.lower()
        results: List[RunbookEntry] = []
        for entries in self._entries.values():
            for e in entries:
                if keyword_lower in e.condition.lower():
                    results.append(e)
        return results

    def total_entries(self) -> int:
        return sum(len(v) for v in self._entries.values())
