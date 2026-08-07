"""
Unit tests for the SqlBaseConnector.
"""

from unittest.mock import MagicMock
from typing import Dict, Any

from schema_scribe.components.db_connectors import SqlBaseConnector


def test_sql_base_connector_profiling_logic():
    """
    Tests the profiling logic in SqlBaseConnector using a mock cursor.
    """

    class DummySqlConnector(SqlBaseConnector):
        def connect(self, db_params: Dict[str, Any]):
            """Mocked implementation of the abstract method."""
            pass

    connector = DummySqlConnector()
    # Mock the cursor to avoid needing a real connection
    connector.cursor = MagicMock()
    connector.schema_name = "public"  # Set required property

    # Mock the return value of fetchone(): (total_count, null_count, distinct_count)
    connector.cursor.fetchone.return_value = (
        100,
        10,
        90,
    )  # 10% nulls, not unique

    stats = connector.get_column_profile("test_table", "test_column")

    # Verify the correct SQL was executed
    expected_query = f"""
        SELECT
            COUNT(*) AS total_count,
            SUM(CASE WHEN "test_column" IS NULL THEN 1 ELSE 0 END) AS null_count,
            COUNT(DISTINCT "test_column") AS distinct_count
        FROM "public"."test_table"
        """
    connector.cursor.execute.assert_called_once_with(expected_query)

    # Verify the stats were calculated correctly
    assert stats == {
        "null_ratio": 0.1,  # 10 / 100
        "distinct_count": 90,
        "is_unique": False,  # 90 != 100
    }

    # Test 'is_unique' logic (distinct = total AND nulls = 0)
    connector.cursor.fetchone.return_value = (100, 0, 100)
    stats_unique = connector.get_column_profile("test_table", "unique_col")
    assert stats_unique["is_unique"] is True

    # Test 'is_unique' logic (distinct = total BUT has nulls)
    connector.cursor.fetchone.return_value = (100, 1, 100)
    stats_unique_null = connector.get_column_profile(
        "test_table", "unique_col_null"
    )
    assert stats_unique_null["is_unique"] is False  # Fails because of null


def test_get_columns_is_pk_is_bool():
    """
    Verifies get_columns coerces the is_pk flag to a bool so callers
    receive True/False rather than the raw 0/1 integer.
    """

    class DummySqlConnector(SqlBaseConnector):
        def connect(self, db_params: Dict[str, Any]):
            """Mocked implementation of the abstract method."""
            pass

    connector = DummySqlConnector()
    connector.cursor = MagicMock()
    connector.schema_name = "public"

    connector.cursor.fetchall.return_value = [
        ("pk_col", "integer", "NO", 1),
        ("plain_col", "text", "YES", 0),
    ]

    columns = connector.get_columns("test_table")

    assert columns[0]["is_pk"] is True
    assert columns[1]["is_pk"] is False


def test_get_foreign_keys_composite_pairs_by_ordinal():
    """
    Regression lock: a composite FK on (a, b) -> (x, y) must yield exactly
    two dicts paired by ordinal position (a->x, b->y) — no cross-product
    (a->y, b->x), no dropped columns.
    """

    class DummySqlConnector(SqlBaseConnector):
        def connect(self, db_params: Dict[str, Any]):
            """Mocked implementation of the abstract method."""
            pass

    connector = DummySqlConnector()
    connector.cursor = MagicMock()
    connector.schema_name = "public"

    # Rows the corrected query returns for a composite FK: one row per
    # paired column, already paired by ordinal inside the join.
    connector.cursor.fetchall.return_value = [
        ("child", "a", "parent", "x"),
        ("child", "b", "parent", "y"),
    ]

    fks = connector.get_foreign_keys()

    assert fks == [
        {
            "source_table": "child",
            "source_column": "a",
            "target_table": "parent",
            "target_column": "x",
        },
        {
            "source_table": "child",
            "source_column": "b",
            "target_table": "parent",
            "target_column": "y",
        },
    ]
    # The pairing must happen in the query itself (the cross-product bug
    # lived in the unpaired constraint_name-only join).
    query = connector.cursor.execute.call_args[0][0]
    assert "position_in_unique_constraint" in query
    assert "ordinal_position" in query


def test_profile_query_quotes_identifiers_not_interpolation():
    """
    Regression lock (issue #3 finding #1): the profile query must quote-double
    identifiers (ANSI) instead of raw-interpolating them. A name containing a
    double quote would otherwise terminate the identifier and inject SQL.
    """

    class DummySqlConnector(SqlBaseConnector):
        def connect(self, db_params: Dict[str, Any]):
            """Mocked implementation of the abstract method."""
            pass

    connector = DummySqlConnector()
    connector.cursor = MagicMock()
    connector.schema_name = 'weird"schema'
    connector.cursor.fetchone.return_value = (10, 0, 10)

    connector.get_column_profile('weird"table', 'weird"name')

    query = connector.cursor.execute.call_args[0][0]
    assert '"weird""schema"."weird""table"' in query
    assert '"weird""name" IS NULL' in query
    assert 'COUNT(DISTINCT "weird""name")' in query
    assert '"weird"schema"' not in query
    assert '"weird"table"' not in query
    assert '"weird"name"' not in query
