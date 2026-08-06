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
