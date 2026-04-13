"""Tests for pipeline ownership registry and display."""

import pytest

from pipewatch.pipeline_ownership import OwnerRecord, OwnershipRegistry
from pipewatch.ownership_display import (
    render_ownership_table,
    render_ownership_summary,
    render_owner_detail,
)


@pytest.fixture
def registry() -> OwnershipRegistry:
    reg = OwnershipRegistry()
    reg.set("ingest", "alice", team="data-eng", contact="alice@example.com")
    reg.set("transform", "bob", team="data-eng", contact="@bob")
    reg.set("export", "carol", team="platform")
    return reg


# --- OwnerRecord ---

def test_to_dict_keys():
    r = OwnerRecord(pipeline="p", owner="alice", team="eng", contact="a@b.com")
    assert set(r.to_dict().keys()) == {"pipeline", "owner", "team", "contact"}


def test_from_dict_roundtrip():
    data = {"pipeline": "p", "owner": "alice", "team": "eng", "contact": "a@b.com"}
    r = OwnerRecord.from_dict(data)
    assert r.pipeline == "p"
    assert r.owner == "alice"
    assert r.team == "eng"
    assert r.contact == "a@b.com"


def test_from_dict_optional_fields_default_none():
    r = OwnerRecord.from_dict({"pipeline": "p", "owner": "alice"})
    assert r.team is None
    assert r.contact is None


def test_summary_contains_pipeline_and_owner():
    r = OwnerRecord(pipeline="ingest", owner="alice")
    s = r.summary()
    assert "ingest" in s
    assert "alice" in s


def test_summary_includes_team_and_contact():
    r = OwnerRecord(pipeline="p", owner="alice", team="eng", contact="@alice")
    s = r.summary()
    assert "eng" in s
    assert "@alice" in s


# --- OwnershipRegistry ---

def test_set_and_get(registry):
    r = registry.get("ingest")
    assert r is not None
    assert r.owner == "alice"


def test_get_missing_returns_none(registry):
    assert registry.get("nonexistent") is None


def test_remove_existing(registry):
    assert registry.remove("ingest") is True
    assert registry.get("ingest") is None


def test_remove_missing_returns_false(registry):
    assert registry.remove("ghost") is False


def test_all_records_length(registry):
    assert len(registry.all_records()) == 3


def test_pipelines_for_owner(registry):
    pipelines = registry.pipelines_for_owner("alice")
    assert "ingest" in pipelines
    assert "transform" not in pipelines


def test_pipelines_for_team(registry):
    pipelines = registry.pipelines_for_team("data-eng")
    assert set(pipelines) == {"ingest", "transform"}


def test_len(registry):
    assert len(registry) == 3


# --- Display ---

def test_render_table_contains_pipeline_names(registry):
    out = render_ownership_table(registry)
    assert "ingest" in out
    assert "transform" in out
    assert "export" in out


def test_render_table_contains_owners(registry):
    out = render_ownership_table(registry)
    assert "alice" in out
    assert "bob" in out


def test_render_table_empty():
    out = render_ownership_table(OwnershipRegistry())
    assert "no ownership" in out.lower()


def test_render_summary_counts(registry):
    out = render_ownership_summary(registry)
    assert "3" in out  # total pipelines


def test_render_owner_detail(registry):
    out = render_owner_detail(registry, "alice")
    assert "ingest" in out


def test_render_owner_detail_unknown():
    out = render_owner_detail(OwnershipRegistry(), "ghost")
    assert "ghost" in out
    assert "No pipelines" in out
