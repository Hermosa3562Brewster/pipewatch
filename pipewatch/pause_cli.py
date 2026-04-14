"""CLI commands for pausing and resuming pipelines."""
from __future__ import annotations

import argparse

from pipewatch.pipeline_pause import PauseManager
from pipewatch.pause_display import print_pause_status

_manager = PauseManager()


def get_manager() -> PauseManager:
    return _manager


def cmd_pause(args: argparse.Namespace) -> None:
    manager = get_manager()
    try:
        rec = manager.pause(args.pipeline, reason=args.reason or "")
        print(f"[paused] {args.pipeline} at {rec.paused_at.strftime('%H:%M:%S')}")
        if rec.reason:
            print(f"  Reason: {rec.reason}")
    except ValueError as exc:
        print(f"[error] {exc}")


def cmd_resume(args: argparse.Namespace) -> None:
    manager = get_manager()
    try:
        rec = manager.resume(args.pipeline)
        dur = rec.duration_seconds()
        print(f"[resumed] {args.pipeline} after {dur:.1f}s")
    except ValueError as exc:
        print(f"[error] {exc}")


def cmd_pause_list(args: argparse.Namespace) -> None:
    manager = get_manager()
    pipelines = list(manager._records.keys())
    if not pipelines:
        print("No pause history recorded.")
        return
    print_pause_status(manager, pipelines)


def build_pause_parser(subparsers) -> None:
    p_pause = subparsers.add_parser("pause", help="Pause a pipeline")
    p_pause.add_argument("pipeline", help="Pipeline name")
    p_pause.add_argument("--reason", default="", help="Reason for pausing")
    p_pause.set_defaults(func=cmd_pause)

    p_resume = subparsers.add_parser("resume", help="Resume a paused pipeline")
    p_resume.add_argument("pipeline", help="Pipeline name")
    p_resume.set_defaults(func=cmd_resume)

    p_list = subparsers.add_parser("pause-list", help="List pause history")
    p_list.set_defaults(func=cmd_pause_list)
