import sqlite3

import pytest

from schema_scribe.tests.fixtures import db_fixtures


@pytest.mark.parametrize("variant", db_fixtures.VARIANT_NAMES)
def test_fixture_builds_deterministically(tmp_path, variant):
    p1 = tmp_path / f"{variant}-1.db"
    p2 = tmp_path / f"{variant}-2.db"
    db_fixtures.build_sqlite(str(p1), variant)
    db_fixtures.build_sqlite(str(p2), variant)
    assert p1.read_bytes() == p2.read_bytes()  # deterministic
    conn = sqlite3.connect(str(p1))
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    conn.close()
    if variant.startswith("legacy"):
        assert len(tables) >= 100
    else:
        assert len(tables) >= 3


@pytest.mark.parametrize("variant", db_fixtures.VARIANT_NAMES)
def test_fixture_foreign_keys_resolve(tmp_path, variant):
    """Every declared FK parent must exist, and FK density must match the variant."""
    path = tmp_path / f"{variant}.db"
    db_fixtures.build_sqlite(str(path), variant)
    conn = sqlite3.connect(str(path))
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    fk_count = 0
    missing = []
    for table in tables:
        for row in conn.execute(f"PRAGMA foreign_key_list('{table}')"):
            fk_count += 1
            if row[2] not in tables:
                missing.append((table, row[2]))
    conn.close()
    assert not missing
    if variant == "legacy-rich":
        assert fk_count > 100
    elif variant == "legacy-poor":
        assert fk_count <= 5
    elif variant == "legacy-conventions":
        assert fk_count > 50


def test_clean_fixture_seed_produces_meaningful_profiles(tmp_path):
    """The seeded clean fixture must produce non-degenerate profile stats:
    emails unique with a NULL (null_ratio > 0), names NOT unique (duplicate
    'Min Park'), so the eval measures real prompt behavior, not the
    empty-table hardcode (is_unique=True for every column).
    """
    from schema_scribe.components.db_connectors.sqlite_connector import SQLiteConnector

    db_path = tmp_path / "clean.db"
    db_fixtures.build_sqlite(str(db_path), "clean")
    connector = SQLiteConnector()
    connector.connect({"path": str(db_path)})

    email_profile = connector.get_column_profile("users", "email")
    name_profile = connector.get_column_profile("users", "name")
    assert email_profile["is_unique"] is True
    assert email_profile["null_ratio"] == 0.0
    assert name_profile["is_unique"] is False
    assert name_profile["null_ratio"] > 0

    users = connector.get_tables()
    connector.close()
    assert len(users) >= 3
