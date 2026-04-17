"""Tests for pipeline_metadata module."""
import pytest
from pipewatch.pipeline_metadata import MetadataEntry, MetadataStore


@pytest.fixture
def store():
    return MetadataStore()


def test_set_and_get_value(store):
    store.set("etl_a", "owner", "alice")
    assert store.get("etl_a", "owner") == "alice"


def test_get_missing_key_returns_none(store):
    assert store.get("etl_a", "missing") is None


def test_get_missing_pipeline_returns_none(store):
    assert store.get("unknown", "key") is None


def test_set_returns_metadata_entry(store):
    entry = store.set("etl_a", "env", "prod")
    assert isinstance(entry, MetadataEntry)
    assert entry.key == "env"
    assert entry.value == "prod"


def test_overwrite_updates_value(store):
    store.set("etl_a", "env", "staging")
    store.set("etl_a", "env", "prod")
    assert store.get("etl_a", "env") == "prod"


def test_all_for_returns_entries(store):
    store.set("etl_a", "k1", "v1")
    store.set("etl_a", "k2", 42)
    result = store.all_for("etl_a")
    assert set(result.keys()) == {"k1", "k2"}


def test_all_for_unknown_pipeline_empty(store):
    assert store.all_for("ghost") == {}


def test_remove_existing_key(store):
    store.set("etl_a", "key", "val")
    removed = store.remove("etl_a", "key")
    assert removed is True
    assert store.get("etl_a", "key") is None


def test_remove_missing_key_returns_false(store):
    assert store.remove("etl_a", "nope") is False


def test_clear_removes_all_keys(store):
    store.set("etl_a", "k1", 1)
    store.set("etl_a", "k2", 2)
    store.clear("etl_a")
    assert store.all_for("etl_a") == {}


def test_all_pipelines_lists_names(store):
    store.set("alpha", "x", 1)
    store.set("beta", "y", 2)
    assert set(store.all_pipelines()) == {"alpha", "beta"}


def test_entry_to_dict_keys(store):
    entry = store.set("p", "k", "v")
    d = entry.to_dict()
    assert set(d.keys()) == {"key", "value", "updated_at"}


def test_entry_from_dict_roundtrip(store):
    entry = store.set("p", "region", "eu-west")
    d = entry.to_dict()
    restored = MetadataEntry.from_dict(d)
    assert restored.key == entry.key
    assert restored.value == entry.value
    assert restored.updated_at == entry.updated_at
