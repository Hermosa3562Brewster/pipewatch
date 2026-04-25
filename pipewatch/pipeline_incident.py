"""Pipeline incident tracking — open, resolve, and query incidents."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

VALID_SEVERITIES = ("low", "medium", "high", "critical")
VALID_STATUSES = ("open", "resolved")


@dataclass
class Incident:
    incident_id: str
    pipeline: str
    severity: str
    description: str
    status: str
    opened_at: datetime
    resolved_at: Optional[datetime] = None
    notes: str = ""

    def to_dict(self) -> dict:
        return {
            "incident_id": self.incident_id,
            "pipeline": self.pipeline,
            "severity": self.severity,
            "description": self.description,
            "status": self.status,
            "opened_at": self.opened_at.isoformat(),
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Incident":
        return cls(
            incident_id=data["incident_id"],
            pipeline=data["pipeline"],
            severity=data["severity"],
            description=data["description"],
            status=data["status"],
            opened_at=datetime.fromisoformat(data["opened_at"]),
            resolved_at=datetime.fromisoformat(data["resolved_at"]) if data.get("resolved_at") else None,
            notes=data.get("notes", ""),
        )

    def summary(self) -> str:
        resolved = f" resolved={self.resolved_at.isoformat()}" if self.resolved_at else ""
        return f"[{self.severity.upper()}] {self.pipeline}: {self.description} ({self.status}){resolved}"

    def duration_seconds(self) -> Optional[float]:
        if self.resolved_at is None:
            return None
        return (self.resolved_at - self.opened_at).total_seconds()


class IncidentLog:
    def __init__(self, max_per_pipeline: int = 100) -> None:
        self._incidents: Dict[str, List[Incident]] = {}
        self._max = max_per_pipeline

    def open(self, pipeline: str, severity: str, description: str, notes: str = "") -> Incident:
        if severity not in VALID_SEVERITIES:
            raise ValueError(f"Invalid severity '{severity}'. Choose from {VALID_SEVERITIES}.")
        incident = Incident(
            incident_id=uuid.uuid4().hex[:12],
            pipeline=pipeline,
            severity=severity,
            description=description,
            status="open",
            opened_at=datetime.now(timezone.utc),
            notes=notes,
        )
        bucket = self._incidents.setdefault(pipeline, [])
        bucket.append(incident)
        if len(bucket) > self._max:
            bucket.pop(0)
        return incident

    def resolve(self, incident_id: str, notes: str = "") -> Optional[Incident]:
        for bucket in self._incidents.values():
            for inc in bucket:
                if inc.incident_id == incident_id:
                    inc.status = "resolved"
                    inc.resolved_at = datetime.now(timezone.utc)
                    if notes:
                        inc.notes = notes
                    return inc
        return None

    def get(self, pipeline: str, status: Optional[str] = None) -> List[Incident]:
        incidents = list(self._incidents.get(pipeline, []))
        if status is not None:
            incidents = [i for i in incidents if i.status == status]
        return incidents

    def all_open(self) -> List[Incident]:
        result = []
        for bucket in self._incidents.values():
            result.extend(i for i in bucket if i.status == "open")
        return result

    def all_pipelines(self) -> List[str]:
        return list(self._incidents.keys())
