"""CLI commands for pipeline metadata management."""
from __future__ import annotations
import argparse
from pipewatch.pipeline_metadata import MetadataStore
from pipewatch.metadata_display import print_metadata

_store = MetadataStore()


def get_store() -> MetadataStore:
    return _store


def cmd_meta_set(args: argparse.Namespace) -> None:
    store = get_store()
    store.set(args.pipeline, args.key, args.value)
    print(f"Set [{args.pipeline}] {args.key} = {args.value}")


def cmd_meta_get(args: argparse.Namespace) -> None:
    store = get_store()
    entry = store.get_entry(args.pipeline, args.key)
    if entry is None:
        print(f"No metadata for [{args.pipeline}] key '{args.key}'.")
    else:
        print(f"[{args.pipeline}] {args.key} = {entry.value}")


def cmd_meta_remove(args: argparse.Namespace) -> None:
    store = get_store()
    removed = store.remove(args.pipeline, args.key)
    if removed:
        print(f"Removed [{args.pipeline}] {args.key}.")
    else:
        print(f"Key '{args.key}' not found for pipeline '{args.pipeline}'.")


def cmd_meta_list(args: argparse.Namespace) -> None:
    store = get_store()
    print_metadata(store, pipeline=getattr(args, "pipeline", None))


def build_metadata_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("meta", help="Manage pipeline metadata")
    sub = p.add_subparsers(dest="meta_cmd")

    ps = sub.add_parser("set", help="Set a metadata key")
    ps.add_argument("pipeline"); ps.add_argument("key"); ps.add_argument("value")
    ps.set_defaults(func=cmd_meta_set)

    pg = sub.add_parser("get", help="Get a metadata key")
    pg.add_argument("pipeline"); pg.add_argument("key")
    pg.set_defaults(func=cmd_meta_get)

    pr = sub.add_parser("remove", help="Remove a metadata key")
    pr.add_argument("pipeline"); pr.add_argument("key")
    pr.set_defaults(func=cmd_meta_remove)

    pl = sub.add_parser("list", help="List metadata")
    pl.add_argument("pipeline", nargs="?", default=None)
    pl.set_defaults(func=cmd_meta_list)
