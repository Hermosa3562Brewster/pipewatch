"""Display helpers for pipeline lineage."""
from __future__ import annotations
from pipewatch.pipeline_lineage import LineageRegistry


def render_lineage_table(registry: LineageRegistry) -> str:
    pipelines = registry.all_pipelines()
    if not pipelines:
        return "No lineage data registered."
    lines = [f"{'Pipeline':<24} {'Inputs':<30} {'Outputs':<30}"]
    lines.append("-" * 84)
    for name in sorted(pipelines):
        node = registry.get(name)
        ins = ", ".join(node.inputs) if node.inputs else "-"
        outs = ", ".join(node.outputs) if node.outputs else "-"
        lines.append(f"{name:<24} {ins:<30} {outs:<30}")
    return "\n".join(lines)


def render_dataset_map(registry: LineageRegistry) -> str:
    datasets = sorted(registry.all_datasets())
    if not datasets:
        return "No datasets found."
    lines = [f"{'Dataset':<30} {'Producers':<28} {'Consumers':<28}"]
    lines.append("-" * 86)
    for ds in datasets:
        producers = ", ".join(registry.producers_of(ds)) or "-"
        consumers = ", ".join(registry.consumers_of(ds)) or "-"
        lines.append(f"{ds:<30} {producers:<28} {consumers:<28}")
    return "\n".join(lines)


def render_lineage_summary(registry: LineageRegistry) -> str:
    pipelines = registry.all_pipelines()
    datasets = registry.all_datasets()
    return (
        f"Lineage Summary\n"
        f"  Pipelines : {len(pipelines)}\n"
        f"  Datasets  : {len(datasets)}"
    )


def print_lineage(registry: LineageRegistry) -> None:
    print(render_lineage_summary(registry))
    print()
    print(render_lineage_table(registry))
    print()
    print(render_dataset_map(registry))
