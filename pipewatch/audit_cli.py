"""CLI commands for the pipeline audit log."""
from __future__ import annotations

import argparse

from pipewatch.pipeline_audit import AuditLog
from pipewatch.audit_display import print_audit, render_audit_for_pipeline

_GLOBAL_LOG = AuditLog()


def get_log() -> AuditLog:
    return _GLOBAL_LOG


def cmd_audit_record(args: argparse.Namespace) -> None:
    log = get_log()
    entry = log.record(
        pipeline=args.pipeline,
        action=args.action,
        actor=args.actor,
        detail=args.detail or "",
    )
    print(entry.summary())


def cmd_audit_list(args: argparse.Namespace) -> None:
    log = get_log()
    last_n = args.last if hasattr(args, "last") and args.last else None
    pipeline = args.pipeline if hasattr(args, "pipeline") and args.pipeline else None
    print_audit(log, pipeline=pipeline, last_n=last_n)


def cmd_audit_clear(args: argparse.Namespace) -> None:
    log = get_log()
    log.clear(args.pipeline)
    print(f"Audit log cleared for pipeline '{args.pipeline}'.")


def build_audit_parser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("audit", help="Pipeline audit log commands")
    sub = p.add_subparsers(dest="audit_cmd")

    rec = sub.add_parser("record", help="Record an audit event")
    rec.add_argument("pipeline")
    rec.add_argument("action", choices=["created", "updated", "deleted", "enabled", "disabled", "reset"])
    rec.add_argument("actor")
    rec.add_argument("--detail", default="")
    rec.set_defaults(func=cmd_audit_record)

    lst = sub.add_parser("list", help="List audit entries")
    lst.add_argument("--pipeline", default=None)
    lst.add_argument("--last", type=int, default=None)
    lst.set_defaults(func=cmd_audit_list)

    clr = sub.add_parser("clear", help="Clear audit log for a pipeline")
    clr.add_argument("pipeline")
    clr.set_defaults(func=cmd_audit_clear)
