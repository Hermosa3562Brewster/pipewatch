import pytest
from pipewatch.pipeline_lineage import LineageNode, LineageRegistry


@pytest.fixture
def registry():
    return LineageRegistry()


def test_to_dict_keys():
    node = LineageNode(name="etl", inputs=["raw"], outputs=["clean"])
    assert set(node.to_dict().keys()) == {"name", "inputs", "outputs"}


def test_from_dict_roundtrip():
    node = LineageNode(name="etl", inputs=["raw"], outputs=["clean"])
    assert LineageNode.from_dict(node.to_dict()).name == "etl"


def test_summary_contains_name():
    node = LineageNode(name="etl", inputs=["a", "b"], outputs=["c"])
    assert "etl" in node.summary()
    assert "2" in node.summary()


def test_register_creates_node(registry):
    node = registry.register("pipe_a", inputs=["ds1"], outputs=["ds2"])
    assert node.name == "pipe_a"
    assert "ds1" in node.inputs
    assert "ds2" in node.outputs


def test_register_same_pipeline_accumulates(registry):
    registry.register("pipe_a", inputs=["ds1"])
    registry.register("pipe_a", inputs=["ds2"])
    node = registry.get("pipe_a")
    assert "ds1" in node.inputs
    assert "ds2" in node.inputs


def test_register_no_duplicates(registry):
    registry.register("pipe_a", inputs=["ds1"])
    registry.register("pipe_a", inputs=["ds1"])
    assert registry.get("pipe_a").inputs.count("ds1") == 1


def test_get_returns_none_for_unknown(registry):
    assert registry.get("ghost") is None


def test_all_pipelines(registry):
    registry.register("a")
    registry.register("b")
    assert set(registry.all_pipelines()) == {"a", "b"}


def test_producers_of(registry):
    registry.register("pipe_a", outputs=["ds1"])
    registry.register("pipe_b", outputs=["ds2"])
    assert registry.producers_of("ds1") == ["pipe_a"]


def test_consumers_of(registry):
    registry.register("pipe_a", inputs=["ds1"])
    registry.register("pipe_b", inputs=["ds1"])
    assert set(registry.consumers_of("ds1")) == {"pipe_a", "pipe_b"}


def test_all_datasets(registry):
    registry.register("pipe_a", inputs=["raw"], outputs=["clean"])
    assert {"raw", "clean"}.issubset(registry.all_datasets())


def test_remove(registry):
    registry.register("pipe_a")
    registry.remove("pipe_a")
    assert registry.get("pipe_a") is None


def test_remove_nonexistent_no_error(registry):
    registry.remove("ghost")  # should not raise
