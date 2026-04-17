"""Display helpers for pipeline metadata."""
from __future__ import annotations
from pipewatch.pipeline_metadata import MetadataStore


def render_metadata_for_pipeline(store: MetadataStore, pipeline: str) -> str:
    entries = store.all_for(pipeline)
    if not entries:
        return f"[{pipeline}] No metadata stored."
    lines = [f"Metadata — {pipeline}", "-" * 40]
    for key, entry in sorted(entries.items()):
        ts = entry.updated_at.strftime("%Y-%m-%d %H:%M:%S")
        lines.append(f"  {key:<24} {str(entry.value):<20} (updated {ts})")
    return "\n".join(lines)


def render_metadata_table(store: MetadataStore) -> str:
    pipelines = store.all_pipelines()
    if not pipelines:
        return "No metadata entries found."
    lines = [f"{'Pipeline':<20} {'Key':<24} {'Value':<20} {'Updated'}",
             "-" * 80]
    for pipeline in sorted(pipelines):
        for key, entry in sorted(store.all_for(pipeline).items()):
            ts = entry.updated_at.strftime("%Y-%m-%d %H:%M:%S")
            lines.append(
                f"{pipeline:<20} {key:<24} {str(entry.value):<20} {ts}"
            )
    return "\n".join(lines)


def render_metadata_summary(store: MetadataStore) -> str:
    pipelines = store.all_pipelines()
    total = sum(len(store.all_for(p)) for p in pipelines)
    return f"Metadata: {len(pipelines)} pipeline(s), {total} total key(s)."


def print_metadata(store: MetadataStore, pipeline: str | None = None) -> None:
    if pipeline:
        print(render_metadata_for_pipeline(store, pipeline))
    else:
        print(render_metadata_table(store))
        print(render_metadata_summary(store))
