"""Tests for pipewatch.pipeline_registry."""

import pytest

from pipewatch.pipeline_registry import PipelineRegistry, get_default_registry
from pipewatch.metrics import PipelineMetrics


@pytest.fixture
def registry() -> PipelineRegistry:
    return PipelineRegistry()


def test_register_creates_pipeline(registry):
    pm = registry.register("etl_orders")
    assert isinstance(pm, PipelineMetrics)
    assert pm.name == "etl_orders"


def test_register_same_name_returns_same_instance(registry):
    pm1 = registry.register("etl_orders")
    pm2 = registry.register("etl_orders")
    assert pm1 is pm2


def test_register_respects_custom_interval(registry):
    pm = registry.register("etl_users", interval=120.0)
    assert pm.interval == 120.0


def test_get_returns_none_for_unknown(registry):
    assert registry.get("nonexistent") is None


def test_get_returns_registered_pipeline(registry):
    registry.register("etl_orders")
    pm = registry.get("etl_orders")
    assert pm is not None
    assert pm.name == "etl_orders"


def test_remove_existing_pipeline(registry):
    registry.register("etl_orders")
    result = registry.remove("etl_orders")
    assert result is True
    assert registry.get("etl_orders") is None


def test_remove_nonexistent_returns_false(registry):
    result = registry.remove("ghost")
    assert result is False


def test_names_returns_sorted_list(registry):
    registry.register("zebra")
    registry.register("alpha")
    registry.register("middle")
    assert registry.names() == ["alpha", "middle", "zebra"]


def test_all_returns_copy(registry):
    registry.register("etl_orders")
    snapshot = registry.all()
    snapshot["injected"] = None  # type: ignore
    assert "injected" not in registry


def test_len(registry):
    assert len(registry) == 0
    registry.register("a")
    registry.register("b")
    assert len(registry) == 2


def test_contains(registry):
    registry.register("etl_orders")
    assert "etl_orders" in registry
    assert "other" not in registry


def test_iter_yields_metrics(registry):
    registry.register("a")
    registry.register("b")
    items = list(registry)
    assert len(items) == 2
    assert all(isinstance(p, PipelineMetrics) for p in items)


def test_active_and_failed(registry):
    pm_run = registry.register("running_pipe")
    pm_run.start()

    pm_fail = registry.register("failed_pipe")
    pm_fail.start()
    pm_fail.record_failure("boom")

    registry.register("idle_pipe")  # never started

    assert pm_run in registry.active()
    assert pm_fail in registry.failed()
    assert len(registry.active()) == 1
    assert len(registry.failed()) == 1


def test_clear_removes_all(registry):
    registry.register("a")
    registry.register("b")
    registry.clear()
    assert len(registry) == 0


def test_get_default_registry_is_singleton():
    r1 = get_default_registry()
    r2 = get_default_registry()
    assert r1 is r2
