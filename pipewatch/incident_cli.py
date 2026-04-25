"""CLI sub-commands for managing pipeline incidents."""
from __future__ import annotations

import argparse

from pipewatch.pipeline_incident import IncidentLog
from pipewatch.incident_display import print_incidents, render_incident_summary

_log = IncidentLog()


def get_log() -> IncidentLog:
    return _log


def cmd_incident_open(args: argparse.Namespace) -> None:
    log = get_log()
    inc = log.open(
        pipeline=args.pipeline,
        severity=args.severity,
        description=args.description,
        notes=args.notes or "",
    )
    print(f"Opened incident {inc.incident_id} [{inc.severity.upper()}] for '{inc.pipeline}'.")


def cmd_incident_resolve(args: argparse.Namespace) -> None:
    log = get_log()
    inc = log.resolve(args.incident_id, notes=args.notes or "")
    if inc:
        print(f"Resolved incident {inc.incident_id} for '{inc.pipeline}'.")
    else:
        print(f"Incident '{args.incident_id}' not found.")


def cmd_incident_list(args: argparse.Namespace) -> None:
    log = get_log()
    print_incidents(log, pipeline=args.pipeline, status=args.status)


def cmd_incident_summary(args: argparse.Namespace) -> None:
    print(render_incident_summary(get_log()))


def build_incident_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("incident", help="Manage pipeline incidents")
    sub = p.add_subparsers(dest="incident_cmd")

    p_open = sub.add_parser("open", help="Open a new incident")
    p_open.add_argument("pipeline")
    p_open.add_argument("severity", choices=["low", "medium", "high", "critical"])
    p_open.add_argument("description")
    p_open.add_argument("--notes", default="")
    p_open.set_defaults(func=cmd_incident_open)

    p_res = sub.add_parser("resolve", help="Resolve an incident")
    p_res.add_argument("incident_id")
    p_res.add_argument("--notes", default="")
    p_res.set_defaults(func=cmd_incident_resolve)

    p_list = sub.add_parser("list", help="List incidents")
    p_list.add_argument("--pipeline", default=None)
    p_list.add_argument("--status", choices=["open", "resolved"], default=None)
    p_list.set_defaults(func=cmd_incident_list)

    p_sum = sub.add_parser("summary", help="Show incident summary")
    p_sum.set_defaults(func=cmd_incident_summary)
