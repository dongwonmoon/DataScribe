"""
Unit tests for the DuckDBConnector.
"""

import duckdb
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


def test_file_connection_is_read_only(mock_duckdb_lib: MagicMock):
    """Regression lock: persistent .db/.duckdb file connections are read-only."""
    connector = DuckDBConnector()
    connector.connect({"path": "/tmp/fixture.duckdb"})
    kwargs = mock_duckdb_lib.connect.call_args.kwargs
    assert kwargs.get("read_only") is True


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
        ("id", "INTEGER", "NO", "PRI", None, None),
        ("email", "VARCHAR", "YES", "NO", None, None),
    ]

    connector = DuckDBConnector()
    connector.connect({"path": "s3://my-bucket/data/"})  # Directory scan mode
    columns = connector.get_columns(
        "users.parquet"
    )  # table_name is just the file name

    assert columns == [
        {
            "name": "id",
            "type": "INTEGER",
            "description": "",
            "is_nullable": False,
            "is_pk": True,
        },
        {
            "name": "email",
            "type": "VARCHAR",
            "description": "",
            "is_nullable": True,
            "is_pk": False,
        },
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


def test_duckdb_get_foreign_keys_composite_pairs_by_ordinal(
    mock_duckdb_lib: MagicMock,
):
    """
    Regression lock: a composite FK on (a, b) -> (x, y) must come back as two
    dicts paired element-wise from the constraint column arrays — not just the
    first column pair.
    """
    # Real duckdb_constraints() row shape for a composite FK:
    # (source_table, constraint_column_names, referenced_table,
    #  referenced_column_names).
    mock_duckdb_lib.mock_cursor.fetchall.return_value = [
        ("child", ["a", "b"], "parent", ["x", "y"]),
    ]

    connector = DuckDBConnector()
    connector.connect({"path": "analytics.db"})
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


def test_is_pk_detected_from_describe(tmp_path):
    """
    Regression lock: real DuckDB DESCRIBE reports PK columns as 'PRI' in the
    key slot (probe on duckdb 1.5.5), so is_pk works without changes.
    """
    db = duckdb.connect(str(tmp_path / "pk.db"))
    db.execute(
        "CREATE TABLE t (a INTEGER, b INTEGER, c VARCHAR, PRIMARY KEY (a, b))"
    )
    db.close()

    connector = DuckDBConnector()
    connector.connect({"path": str(tmp_path / "pk.db")})
    cols = {col["name"]: col for col in connector.get_columns("t")}

    assert cols["a"]["is_pk"] is True
    assert cols["b"]["is_pk"] is True
    assert cols["c"]["is_pk"] is False
    connector.close()


def test_get_columns_db_mode_quotes_identifier(mock_duckdb_lib: MagicMock):
    """
    Regression lock (issue #3 finding #1): DESCRIBE must quote-double the
    identifier, not raw-interpolate it.
    """
    connector = DuckDBConnector()
    connector.connect({"path": "analytics.db"})
    connector.get_columns('tab"le')

    expected = 'DESCRIBE "tab""le";'
    assert mock_duckdb_lib.mock_cursor.execute.call_args[0][0] == expected


def test_get_columns_file_mode_quotes_read_auto_path(mock_duckdb_lib: MagicMock):
    """
    Regression lock: the read_auto path is a string literal and must have
    embedded single quotes doubled.
    """
    connector = DuckDBConnector()
    connector.connect({"path": "./data/"})
    connector.get_columns("order's.csv")

    expected = "DESCRIBE SELECT * FROM read_auto('./data/order''s.csv', SAMPLE_SIZE=50000);"
    assert mock_duckdb_lib.mock_cursor.execute.call_args[0][0] == expected


def test_get_tables_directory_scan_quotes_glob_path(mock_duckdb_lib: MagicMock):
    """
    Regression lock: the glob path is a string literal and must have embedded
    single quotes doubled.
    """
    connector = DuckDBConnector()
    connector.connect({"path": "./da'ta/"})
    connector.get_tables()

    expected = "SELECT basename(file_name) FROM glob('./da''ta/*.*')"
    assert mock_duckdb_lib.mock_cursor.execute.call_args[0][0] == expected


def test_get_column_profile_file_mode_quotes_literal(mock_duckdb_lib: MagicMock):
    """
    Regression lock: the read_auto path in the profile subquery is a literal
    and the column name is an identifier; both must be escaped.
    """
    mock_duckdb_lib.mock_cursor.fetchone.return_value = (5, 0, 5)

    connector = DuckDBConnector()
    connector.connect({"path": "./data/"})
    connector.get_column_profile("order's.csv", 'col"x')

    expected = """
        SELECT
            COUNT(*) AS total_count,
            SUM(CASE WHEN "col""x" IS NULL THEN 1 ELSE 0 END) AS null_count,
            COUNT(DISTINCT "col""x") AS distinct_count
        FROM (SELECT * FROM read_auto('./data/order''s.csv')) t
        """
    normalized_expected = " ".join(expected.split())
    normalized_actual = " ".join(
        mock_duckdb_lib.mock_cursor.execute.call_args[0][0].split()
    )
    assert normalized_actual == normalized_expected
