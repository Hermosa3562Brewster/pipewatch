"""Pipeline ownership registry — track which team or person owns each pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class OwnerRecord:
    pipeline: str
    owner: str
    team: Optional[str] = None
    contact: Optional[str] = None  # e.g. email or Slack handle

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "owner": self.owner,
            "team": self.team,
            "contact": self.contact,
        }

    @staticmethod
    def from_dict(data: Dict) -> "OwnerRecord":
        return OwnerRecord(
            pipeline=data["pipeline"],
            owner=data["owner"],
            team=data.get("team"),
            contact=data.get("contact"),
        )

    def summary(self) -> str:
        parts = [f"{self.pipeline} → {self.owner}"]
        if self.team:
            parts.append(f"({self.team})")
        if self.contact:
            parts.append(f"<{self.contact}>")
        return " ".join(parts)


class OwnershipRegistry:
    """Maps pipeline names to their OwnerRecord."""

    def __init__(self) -> None:
        self._records: Dict[str, OwnerRecord] = {}

    def set(self, pipeline: str, owner: str, team: Optional[str] = None,
            contact: Optional[str] = None) -> OwnerRecord:
        record = OwnerRecord(pipeline=pipeline, owner=owner, team=team, contact=contact)
        self._records[pipeline] = record
        return record

    def get(self, pipeline: str) -> Optional[OwnerRecord]:
        return self._records.get(pipeline)

    def remove(self, pipeline: str) -> bool:
        if pipeline in self._records:
            del self._records[pipeline]
            return True
        return False

    def all_records(self) -> List[OwnerRecord]:
        return list(self._records.values())

    def pipelines_for_owner(self, owner: str) -> List[str]:
        return [r.pipeline for r in self._records.values() if r.owner == owner]

    def pipelines_for_team(self, team: str) -> List[str]:
        return [r.pipeline for r in self._records.values() if r.team == team]

    def __len__(self) -> int:
        return len(self._records)
