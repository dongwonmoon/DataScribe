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
