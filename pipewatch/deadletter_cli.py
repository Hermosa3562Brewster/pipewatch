"""CLI sub-commands for the dead-letter queue."""
from __future__ import annotations

import argparse

from pipewatch.pipeline_deadletter import DeadLetterEntry, DeadLetterQueue
from pipewatch.deadletter_display import (
    print_deadletter,
    render_deadletter_detail,
    render_deadletter_summary,
)

_QUEUE: DeadLetterQueue = DeadLetterQueue()


def get_queue() -> DeadLetterQueue:
    return _QUEUE


def cmd_dl_list(args: argparse.Namespace) -> None:
    queue = get_queue()
    pipeline = getattr(args, "pipeline", None)
    print_deadletter(queue, pipeline=pipeline)


def cmd_dl_detail(args: argparse.Namespace) -> None:
    queue = get_queue()
    print(render_deadletter_detail(queue, args.pipeline))


def cmd_dl_purge(args: argparse.Namespace) -> None:
    queue = get_queue()
    removed = queue.purge(args.pipeline)
    print(f"Purged {removed} entries from '{args.pipeline}'.")


def cmd_dl_summary(args: argparse.Namespace) -> None:
    queue = get_queue()
    print(render_deadletter_summary(queue))


def build_deadletter_parser(subparsers: argparse._SubParsersAction) -> None:
    dl = subparsers.add_parser("deadletter", help="Manage the dead-letter queue")
    dl_sub = dl.add_subparsers(dest="dl_cmd")

    p_list = dl_sub.add_parser("list", help="List dead-letter entries")
    p_list.add_argument("--pipeline", default=None, help="Filter by pipeline name")
    p_list.set_defaults(func=cmd_dl_list)

    p_detail = dl_sub.add_parser("detail", help="Show entry details for a pipeline")
    p_detail.add_argument("pipeline", help="Pipeline name")
    p_detail.set_defaults(func=cmd_dl_detail)

    p_purge = dl_sub.add_parser("purge", help="Purge entries for a pipeline")
    p_purge.add_argument("pipeline", help="Pipeline name")
    p_purge.set_defaults(func=cmd_dl_purge)

    p_summary = dl_sub.add_parser("summary", help="Show dead-letter summary")
    p_summary.set_defaults(func=cmd_dl_summary)
