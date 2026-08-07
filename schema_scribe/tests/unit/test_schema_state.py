"""
Unit tests for the schema state sidecar service.

The snapshot is the persisted baseline that the change classifier (Slice 5)
and `db --check` (Slice 6) consume: a pure function of the catalog dict,
saved atomically next to the output file, and loaded without ever raising.
"""

import pytest

from schema_scribe.services.schema_state import SchemaState


def test_roundtrip_snapshot(tmp_path):
    path = tmp_path / "catalog.md.schema-state.json"
    catalog = {"tables": [{"name": "t", "columns": [
        {"name": "id", "type": "INTEGER", "is_pk": True},
        {"name": "v", "type": "TEXT", "is_pk": False},
    ]}], "views": [], "foreign_keys": []}
    snap = SchemaState.snapshot(catalog)
    SchemaState.save(snap, str(path))
    loaded = SchemaState.load(str(path))
    assert loaded["tables"]["t"]["columns"] == {"id": "INTEGER", "v": "TEXT"}
    assert loaded["tables"]["t"]["pk"] == ["id"]


def test_load_missing_or_corrupt_returns_none(tmp_path):
    assert SchemaState.load(str(tmp_path / "nope.json")) is None
    bad = tmp_path / "bad.json"
    bad.write_text("{not json")
    assert SchemaState.load(str(bad)) is None


@pytest.mark.parametrize(
    "content",
    ["{}", "[]", '{"tables": []}', '{"views": []}'],
)
def test_load_structurally_corrupt_returns_none(tmp_path, content):
    """
    A sidecar that parses but is not the minimal snapshot shape (a dict
    with a dict-valued 'tables' key) is treated as missing/corrupt: load()
    returns None instead of handing check() a shape it will KeyError on.
    """
    sidecar = tmp_path / "catalog.md.schema-state.json"
    sidecar.write_text(content)
    assert SchemaState.load(str(sidecar)) is None


def test_load_valid_empty_snapshot_is_not_corrupt(tmp_path):
    """
    A genuinely empty baseline ({'tables': {}}) is a valid snapshot, not
    corruption: it is what a fresh run against an empty schema saves.
    """
    sidecar = tmp_path / "catalog.md.schema-state.json"
    sidecar.write_text('{"tables": {}, "views": [], "generated_at": "x"}')
    loaded = SchemaState.load(str(sidecar))
    assert loaded is not None
    assert loaded["tables"] == {}


def test_classify_categories():
    prev = SchemaState.snapshot({"tables": [
        {"name": "keep", "columns": [{"name": "c", "type": "INTEGER", "is_pk": True}]},
        {"name": "drop", "columns": []},
    ], "views": [], "foreign_keys": []})
    curr = SchemaState.snapshot({"tables": [
        {"name": "keep", "columns": [{"name": "c", "type": "TEXT", "is_pk": True}]},
        {"name": "new", "columns": []},
    ], "views": [], "foreign_keys": []})
    r = SchemaState.classify(prev, curr)
    assert r["added"] == ["new"]
    assert r["removed"] == ["drop"]
    assert r["structurally_changed"] == ["keep"]


def test_classify_without_previous_baseline_marks_all_added():
    curr = SchemaState.snapshot({"tables": [{"name": "t", "columns": []}], "views": [], "foreign_keys": []})
    r = SchemaState.classify(None, curr)
    assert r["added"] == ["t"]


def test_classify_view_added_is_included_in_added():
    """
    Views are catalog objects: a view added since the baseline must appear
    in the 'added' list (views carry no structure in the snapshot, so they
    can never be structurally_changed).
    """
    def snap(views):
        return SchemaState.snapshot({"tables": [{"name": "t", "columns": []}], "views": views, "foreign_keys": []})
    r = SchemaState.classify(snap([]), snap([{"name": "v_report", "definition": "SELECT 1"}]))
    assert r["added"] == ["v_report"]
    assert r["removed"] == []
    assert r["structurally_changed"] == []


def test_classify_view_removed_is_included_in_removed():
    def snap(views):
        return SchemaState.snapshot({"tables": [{"name": "t", "columns": []}], "views": views, "foreign_keys": []})
    r = SchemaState.classify(snap([{"name": "v_report", "definition": "SELECT 1"}]), snap([]))
    assert r["removed"] == ["v_report"]
    assert r["added"] == []
    assert r["structurally_changed"] == []


def test_classify_views_mixed_with_tables_stays_sorted():
    def snap(views):
        return SchemaState.snapshot({"tables": [{"name": "b", "columns": []}], "views": views, "foreign_keys": []})
    r = SchemaState.classify(
        snap([{"name": "v_old", "definition": "SELECT 1"}]),
        snap([{"name": "v_new", "definition": "SELECT 2"}]),
    )
    assert r["added"] == ["v_new"]
    assert r["removed"] == ["v_old"]


def test_classify_unchanged_tables_appear_nowhere():
    catalog = {"tables": [
        {"name": "t", "columns": [
            {"name": "id", "type": "INTEGER", "is_pk": True},
            {"name": "v", "type": "TEXT", "is_pk": False},
        ]},
    ], "views": [], "foreign_keys": []}
    r = SchemaState.classify(SchemaState.snapshot(catalog), SchemaState.snapshot(catalog))
    assert r == {"added": [], "removed": [], "structurally_changed": []}


def test_classify_returns_sorted_lists():
    prev = SchemaState.snapshot({"tables": [
        {"name": "b", "columns": []},
        {"name": "a", "columns": []},
    ], "views": [], "foreign_keys": []})
    curr = SchemaState.snapshot({"tables": [
        {"name": "d", "columns": []},
        {"name": "c", "columns": []},
    ], "views": [], "foreign_keys": []})
    r = SchemaState.classify(prev, curr)
    assert r["added"] == ["c", "d"]
    assert r["removed"] == ["a", "b"]


def test_classify_column_removed_is_structural():
    def snap(cols):
        return SchemaState.snapshot({"tables": [
            {"name": "t", "columns": cols},
        ], "views": [], "foreign_keys": []})
    prev = snap([{"name": "a", "type": "TEXT", "is_pk": False},
                 {"name": "b", "type": "TEXT", "is_pk": False}])
    curr = snap([{"name": "a", "type": "TEXT", "is_pk": False}])
    r = SchemaState.classify(prev, curr)
    assert r["structurally_changed"] == ["t"]


def test_classify_pk_change_is_structural():
    def snap(is_pk):
        return SchemaState.snapshot({"tables": [
            {"name": "t", "columns": [
                {"name": "id", "type": "INTEGER", "is_pk": is_pk},
            ]},
        ], "views": [], "foreign_keys": []})
    r = SchemaState.classify(snap(False), snap(True))
    assert r["structurally_changed"] == ["t"]


def test_classify_fk_target_change_is_structural():
    def snap(target_table):
        return SchemaState.snapshot({"tables": [
            {"name": "orders", "columns": [
                {"name": "user_id", "type": "INTEGER", "is_pk": False},
            ]},
        ], "views": [], "foreign_keys": [
            {"source_table": "orders", "source_column": "user_id",
             "target_table": target_table, "target_column": "id"},
        ]})
    r = SchemaState.classify(snap("users"), snap("accounts"))
    assert r["added"] == []
    assert r["removed"] == []
    assert r["structurally_changed"] == ["orders"]
