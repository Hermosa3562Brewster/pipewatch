"""Display helpers for pipeline ownership information."""

from __future__ import annotations

from typing import Optional

from pipewatch.pipeline_ownership import OwnershipRegistry

_COL_PIPELINE = 22
_COL_OWNER = 18
_COL_TEAM = 16
_COL_CONTACT = 24


def _row(pipeline: str, owner: str, team: str, contact: str) -> str:
    return (
        f"  {pipeline:<{_COL_PIPELINE}}"
        f"{owner:<{_COL_OWNER}}"
        f"{team:<{_COL_TEAM}}"
        f"{contact:<{_COL_CONTACT}}"
    )


def render_ownership_table(registry: OwnershipRegistry) -> str:
    records = registry.all_records()
    if not records:
        return "  (no ownership records registered)"

    header = _row("PIPELINE", "OWNER", "TEAM", "CONTACT")
    sep = "  " + "-" * (_COL_PIPELINE + _COL_OWNER + _COL_TEAM + _COL_CONTACT)
    lines = ["\nPipeline Ownership", sep, header, sep]
    for r in sorted(records, key=lambda x: x.pipeline):
        lines.append(_row(
            r.pipeline,
            r.owner,
            r.team or "-",
            r.contact or "-",
        ))
    lines.append(sep)
    return "\n".join(lines)


def render_ownership_summary(registry: OwnershipRegistry) -> str:
    records = registry.all_records()
    total = len(records)
    teams = {r.team for r in records if r.team}
    owners = {r.owner for r in records}
    lines = [
        "\nOwnership Summary",
        f"  Total pipelines with owners : {total}",
        f"  Unique owners               : {len(owners)}",
        f"  Unique teams                : {len(teams)}",
    ]
    return "\n".join(lines)


def render_owner_detail(registry: OwnershipRegistry, owner: str) -> str:
    pipelines = registry.pipelines_for_owner(owner)
    if not pipelines:
No pipelines found for owner '{owner}'."
    lines = [f"\nPipelines owned by '{owner}':"] + [f"  - {p}" for p in sorted(pipelines)]
    return "\n".join(lines)


def print_ownership(registry: OwnershipRegistry, owner: Optional[str] = None) -> None:
    if owner:
        print(render_owner_detail(registry, owner))
    else:
        print(render_ownership_table(registry))
        print(render_ownership_summary(registry))
