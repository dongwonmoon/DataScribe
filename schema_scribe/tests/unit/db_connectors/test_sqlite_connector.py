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
