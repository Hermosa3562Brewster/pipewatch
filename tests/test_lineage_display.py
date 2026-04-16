import pytest
from pipewatch.pipeline_lineage import LineageRegistry
from pipewatch.lineage_display import (
    render_lineage_table,
    render_dataset_map,
    render_lineage_summary,
)


@pytest.fixture
def registry():
    reg = LineageRegistry()
    reg.register("ingest", inputs=["raw_events"], outputs=["events_clean"])
    reg.register("transform", inputs=["events_clean"], outputs=["events_agg"])
    return reg


def test_render_table_contains_pipeline_names(registry):
    out = render_lineage_table(registry)
    assert "ingest" in out
    assert "transform" in out


def test_render_table_contains_header(registry):
    out = render_lineage_table(registry)
    assert "Pipeline" in out
    assert "Inputs" in out
    assert "Outputs" in out


def test_render_table_shows_datasets(registry):
    out = render_lineage_table(registry)
    assert "raw_events" in out
    assert "events_clean" in out


def test_render_table_empty():
    out = render_lineage_table(LineageRegistry())
    assert "No lineage" in out


def test_render_dataset_map_contains_datasets(registry):
    out = render_dataset_map(registry)
    assert "raw_events" in out
    assert "events_agg" in out


def test_render_dataset_map_shows_producers(registry):
    out = render_dataset_map(registry)
    assert "ingest" in out


def test_render_dataset_map_empty():
    out = render_dataset_map(LineageRegistry())
    assert "No datasets" in out


def test_render_summary_counts(registry):
    out = render_lineage_summary(registry)
    assert "2" in out  # 2 pipelines
    assert "Pipelines" in out
    assert "Datasets" in out
