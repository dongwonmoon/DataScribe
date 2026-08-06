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
