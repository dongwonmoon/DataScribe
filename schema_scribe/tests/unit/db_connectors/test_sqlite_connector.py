"""
Unit tests for the SQLiteConnector.
"""

import sqlite3

import pytest

from schema_scribe.components.db_connectors import SQLiteConnector


def test_sqlite_connector_integration(sqlite_db):
    """
    Tests the full lifecycle of the SQLiteConnector with a real temp database.
    This acts as an integration test for the file-based connector.
    """
    connector = SQLiteConnector()
    connector.connect({"path": sqlite_db})

    tables = connector.get_tables()
    assert "users" in tables
    assert "products" in tables

    columns = connector.get_columns("users")
    # The exact type might vary, so we don't check it here for simplicity
    col_names = [c["name"] for c in columns]
    assert "id" in col_names
    assert "name" in col_names
    assert "email" in col_names

    views = connector.get_views()
    assert views[0]["name"] == "user_orders"
    assert "SELECT" in views[0]["definition"]

    fks = connector.get_foreign_keys()
    assert len(fks) == 2
    # Check for presence of FKs regardless of order
    expected_fk1 = {
        "source_table": "orders",
        "target_table": "users",
        "source_column": "user_id",
        "target_column": "id",
    }
    expected_fk2 = {
        "source_table": "orders",
        "target_table": "products",
        "source_column": "product_id",
        "target_column": "id",
    }
    assert expected_fk1 in fks or expected_fk2 in fks

    connector.close()
    assert connector.connection is None


def test_sqlite_connector_profiling(sqlite_db_with_data):
    """
    Tests the get_column_profile method on SQLiteConnector with predictable data.
    """
    connector = SQLiteConnector()
    connector.connect({"path": sqlite_db_with_data})

    # Test 1: 'id' column (PK)
    # 5 total, 0 null, 5 distinct -> unique
    stats_id = connector.get_column_profile("profile_test", "id")
    assert stats_id == {
        "null_ratio": 0.0,
        "distinct_count": 5,
        "is_unique": True,
    }

    # Test 2: 'nullable_col'
    # 5 total, 2 null, 2 distinct -> not unique
    stats_nullable = connector.get_column_profile(
        "profile_test", "nullable_col"
    )
    assert stats_nullable == {
        "null_ratio": 0.4,  # 2 / 5
        "distinct_count": 2,
        "is_unique": False,
    }

    # Test 3: 'category_col' (low cardinality)
    # 5 total, 0 null, 2 distinct -> not unique
    stats_category = connector.get_column_profile(
        "profile_test", "category_col"
    )
    assert stats_category == {
        "null_ratio": 0.0,
        "distinct_count": 2,
        "is_unique": False,
    }

    connector.close()


def test_profile_returns_none_on_query_failure(mocker):
    """
    Verifies get_column_profile returns None values (not "N/A" strings)
    when the profiling query fails.
    """
    connector = SQLiteConnector()
    connector.cursor = mocker.MagicMock()
    connector.cursor.execute.side_effect = sqlite3.Error("boom")
    profile = connector.get_column_profile("t", "c")
    assert profile["null_ratio"] is None
    assert profile["distinct_count"] is None
    assert profile["is_unique"] is None


def test_connection_is_read_only(tmp_path):
    db_path = tmp_path / "ro.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    conn.commit()
    conn.close()
    connector = SQLiteConnector()
    connector.connect({"path": str(db_path)})
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        connector.connection.execute("CREATE TABLE t2 (id INTEGER)")
    connector.close()


def test_composite_pk_all_columns_marked(tmp_path):
    db_path = tmp_path / "pk.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a, b))")
    conn.commit()
    conn.close()
    connector = SQLiteConnector()
    connector.connect({"path": str(db_path)})
    cols = {c["name"]: c for c in connector.get_columns("t")}
    assert cols["a"]["is_pk"] is True
    assert cols["b"]["is_pk"] is True
    connector.close()


def test_quoted_table_and_column_names_roundtrip(tmp_path):
    """
    Regression lock (issue #3 finding #1): a table named `tab'le` and a column
    named `col"x` must not break get_columns / get_foreign_keys /
    get_column_profile. Before quoting, PRAGMA table_info('tab'le') raised
    sqlite3.OperationalError: near "le": syntax error.
    """
    db_path = tmp_path / "quoted.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        'CREATE TABLE "tab\'le" ("col""x" INTEGER PRIMARY KEY, normal TEXT)'
    )
    conn.execute(
        'CREATE TABLE parent (id INTEGER PRIMARY KEY)'
    )
    conn.execute(
        'ALTER TABLE "tab\'le" ADD COLUMN parent_id INTEGER '
        'REFERENCES parent(id)'
    )
    conn.executemany(
        'INSERT INTO "tab\'le" ("col""x", normal, parent_id) VALUES (?, ?, ?)',
        [(1, "a", 1), (2, "b", 1), (3, None, 1)],
    )
    conn.execute("INSERT INTO parent (id) VALUES (1)")
    conn.commit()
    conn.close()

    connector = SQLiteConnector()
    connector.connect({"path": str(db_path)})

    cols = connector.get_columns("tab'le")
    col_by_name = {c["name"]: c for c in cols}
    assert set(col_by_name) == {'col"x', "normal", "parent_id"}
    assert col_by_name['col"x']["is_pk"] is True

    prof = connector.get_column_profile("tab'le", 'col"x')
    assert prof == {
        "null_ratio": 0.0,
        "distinct_count": 3,
        "is_unique": True,
    }

    prof_null = connector.get_column_profile("tab'le", "normal")
    assert prof_null == {
        "null_ratio": round(1 / 3, 2),
        "distinct_count": 2,
        "is_unique": False,
    }

    fks = connector.get_foreign_keys()
    assert {
        "source_table": "tab'le",
        "source_column": "parent_id",
        "target_table": "parent",
        "target_column": "id",
    } in fks

    connector.close()
