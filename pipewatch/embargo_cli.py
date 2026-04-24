"""CLI commands for managing pipeline embargo windows."""
from __future__ import annotations

import argparse
from datetime import time

from pipewatch.pipeline_embargo import EmbargoManager, EmbargoWindow
from pipewatch.embargo_display import print_embargo

_manager = EmbargoManager()


def get_manager() -> EmbargoManager:
    return _manager


def cmd_embargo_add(args: argparse.Namespace) -> None:
    start = time.fromisoformat(args.start)
    end = time.fromisoformat(args.end)
    window = EmbargoWindow(
        pipeline=args.pipeline,
        start_time=start,
        end_time=end,
        reason=args.reason or "",
    )
    get_manager().add(window)
    print(f"Embargo window added for '{args.pipeline}': {window.summary()}")


def cmd_embargo_remove(args: argparse.Namespace) -> None:
    get_manager().remove(args.pipeline, args.index)
    print(f"Removed embargo window {args.index} for '{args.pipeline}'.")


def cmd_embargo_list(args: argparse.Namespace) -> None:
    print_embargo(get_manager())


def cmd_embargo_check(args: argparse.Namespace) -> None:
    mgr = get_manager()
    if mgr.is_embargoed(args.pipeline):
        print(f"'{args.pipeline}' is currently EMBARGOED.")
    else:
        print(f"'{args.pipeline}' is NOT embargoed — safe to run.")


def build_embargo_parser(sub: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p_add = sub.add_parser("embargo-add", help="Add an embargo window")
    p_add.add_argument("pipeline")
    p_add.add_argument("--start", required=True, help="HH:MM start time (UTC)")
    p_add.add_argument("--end", required=True, help="HH:MM end time (UTC)")
    p_add.add_argument("--reason", default="")
    p_add.set_defaults(func=cmd_embargo_add)

    p_rm = sub.add_parser("embargo-remove", help="Remove an embargo window by index")
    p_rm.add_argument("pipeline")
    p_rm.add_argument("index", type=int)
    p_rm.set_defaults(func=cmd_embargo_remove)

    p_ls = sub.add_parser("embargo-list", help="List all embargo windows")
    p_ls.set_defaults(func=cmd_embargo_list)

    p_chk = sub.add_parser("embargo-check", help="Check if a pipeline is embargoed now")
    p_chk.add_argument("pipeline")
    p_chk.set_defaults(func=cmd_embargo_check)
