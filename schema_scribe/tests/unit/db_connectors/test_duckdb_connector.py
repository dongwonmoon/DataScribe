"""
Unit tests for the DuckDBConnector.
"""

import pytest

from unittest.mock import patch, MagicMock, call

from schema_scribe.components.db_connectors import DuckDBConnector


@pytest.fixture
def mock_duckdb_lib(mocker):
    """
    Mocks the 'duckdb' library import within the duckdb_connector module.
    Returns the mock object for manipulation in tests.
    """
    # Patch the 'duckdb' import *where it is used*
    mock_lib = mocker.patch(
        "schema_scribe.components.db_connectors.duckdb_connector.duckdb"
    )

    # Mock the return values for connect().cursor() chain
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    mock_lib.connect.return_value = mock_connection

    # Store cursor for assertions
    mock_lib.mock_cursor = mock_cursor
    return mock_lib


def test_duckdb_connect_db_file(mock_duckdb_lib: MagicMock):
    """Tests that DuckDBConnector connects to a .db file correctly."""
    connector = DuckDBConnector()
    connector.connect({"path": "test.db"})

    # Should connect directly to the file in read-only mode
    mock_duckdb_lib.connect.assert_called_once_with(
        database="test.db", read_only=True
    )
    assert connector.is_directory_scan is False


def test_duckdb_connect_directory_path(mock_duckdb_lib: MagicMock):
    """Tests that DuckDBConnector connects to in-memory for a directory path."""
    connector = DuckDBConnector()
    connector.connect({"path": "./local_data/"})

    # Should connect to in-memory DB
    mock_duckdb_lib.connect.assert_called_once_with(
        database=":memory:", read_only=False
    )
    assert connector.is_directory_scan is True


def test_duckdb_connect_s3_path(mock_duckdb_lib: MagicMock):
    """Tests that DuckDBConnector installs httpfs for S3 paths."""
    connector = DuckDBConnector()
    connector.connect({"path": "s3://my-bucket/data/"})

    # Should connect to in-memory DB
    mock_duckdb_lib.connect.assert_called_once_with(
        database=":memory:", read_only=False
    )
    # Should install httpfs
    mock_duckdb_lib.mock_cursor.execute.assert_called_once_with(
        "INSTALL httpfs; LOAD httpfs;"
    )
    assert connector.is_directory_scan is True
    assert connector.is_s3 is True


def test_duckdb_get_tables_db_file(mock_duckdb_lib: MagicMock):
    """Tests get_tables for a persistent .db file."""
    mock_duckdb_lib.mock_cursor.fetchall.return_value = [
        ("table1",),
        ("view1",),
    ]

    connector = DuckDBConnector()
    connector.connect({"path": "analytics.db"})  # .db file
    tables = connector.get_tables()

    assert tables == ["table1", "view1"]
    mock_duckdb_lib.mock_cursor.execute.assert_called_once_with(
        "SHOW ALL TABLES;"
    )


def test_duckdb_get_tables_directory_scan(mock_duckdb_lib: MagicMock):
    """Tests get_tables for a local directory scan."""
    mock_duckdb_lib.mock_cursor.fetchall.return_value = [
        ("users.parquet",),
        ("orders.csv",),
    ]

    connector = DuckDBConnector()
    connector.connect({"path": "./local_data/"})
    tables = connector.get_tables()

    assert tables == ["users.parquet", "orders.csv"]
    # Should use 'glob' and wildcard
    expected_query = "SELECT basename(file_name) FROM glob('./local_data/*.*')"
    mock_duckdb_lib.mock_cursor.execute.assert_called_once_with(expected_query)


def test_duckdb_get_tables_s3_scan(mock_duckdb_lib: MagicMock):
    """Tests get_tables for an S3 directory scan."""
    mock_duckdb_lib.mock_cursor.fetchall.return_value = [("s3_file.parquet",)]

    connector = DuckDBConnector()
    connector.connect({"path": "s3://my-bucket/data/"})
    tables = connector.get_tables()

    assert tables == ["s3_file.parquet"]
    # Should use 's3_glob'
    expected_query = (
        "SELECT basename(file_name) FROM s3_glob('s3://my-bucket/data/*.*')"
    )
    # Call list includes httpfs install + this glob query
    assert mock_duckdb_lib.mock_cursor.execute.call_args_list[1] == call(
        expected_query
    )


def test_duckdb_get_columns_file_scan(mock_duckdb_lib: MagicMock):
    """Tests get_columns constructs the correct read_auto query for files."""
    mock_duckdb_lib.mock_cursor.fetchall.return_value = [
        ("id", "INTEGER"),
        ("email", "VARCHAR"),
    ]

    connector = DuckDBConnector()
    connector.connect({"path": "s3://my-bucket/data/"})  # Directory scan mode
    columns = connector.get_columns(
        "users.parquet"
    )  # table_name is just the file name

    assert columns == [
        {"name": "id", "type": "INTEGER"},
        {"name": "email", "type": "VARCHAR"},
    ]
    # Should build the full path
    expected_query = "DESCRIBE SELECT * FROM read_auto('s3://my-bucket/data/users.parquet', SAMPLE_SIZE=50000);"
    # httpfs call is [0], this is [1]
    assert mock_duckdb_lib.mock_cursor.execute.call_args_list[1] == call(
        expected_query
    )


def test_duckdb_get_column_profile_file_scan(mock_duckdb_lib: MagicMock):
    """Tests get_column_profile constructs the correct subquery for files."""
    mock_duckdb_lib.mock_cursor.fetchone.return_value = (
        100,
        10,
        90,
    )  # total, null, distinct

    connector = DuckDBConnector()
    connector.connect({"path": "./local_data/"})  # Directory scan mode
    stats = connector.get_column_profile("orders.csv", "order_status")

    # Should calculate stats correctly
    assert stats == {
        "total_count": 100,
        "null_ratio": 0.1,
        "distinct_count": 90,
        "is_unique": False,
    }

    # Should build the correct query with a read_auto() subquery
    expected_query = """
        SELECT
            COUNT(*) AS total_count,
            SUM(CASE WHEN "order_status" IS NULL THEN 1 ELSE 0 END) AS null_count,
            COUNT(DISTINCT "order_status") AS distinct_count
        FROM (SELECT * FROM read_auto('./local_data/orders.csv')) t
        """
    # Normalize whitespace for comparison
    normalized_expected = " ".join(expected_query.split())
    normalized_actual = " ".join(
        mock_duckdb_lib.mock_cursor.execute.call_args[0][0].split()
    )

    assert normalized_actual == normalized_expected


def test_duckdb_get_views_file_scan(mock_duckdb_lib: MagicMock):
    """Tests that get_views returns an empty list for file scans."""
    connector = DuckDBConnector()
    connector.connect({"path": "s3://my-bucket/data/"})
    views = connector.get_views()

    assert views == []
    # Should not execute any query (except httpfs)
    assert mock_duckdb_lib.mock_cursor.execute.call_count == 1
