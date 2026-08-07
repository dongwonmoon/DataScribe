"""
Unit tests for the schema state sidecar service.

The snapshot is the persisted baseline that the change classifier (Slice 5)
and `db --check` (Slice 6) consume: a pure function of the catalog dict,
saved atomically next to the output file, and loaded without ever raising.
"""

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
