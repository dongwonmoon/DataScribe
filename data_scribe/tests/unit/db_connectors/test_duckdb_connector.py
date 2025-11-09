"""
Unit tests for the DuckDBConnector.
"""

from unittest.mock import patch, MagicMock

from data_scribe.components.db_connectors import DuckDBConnector


@patch("data_scribe.components.db_connectors.duckdb_connector.duckdb")
def test_duckdb_connector_connect_to_db_file(mock_duckdb):
    """Tests that DuckDBConnector connects to a .db file correctly."""
    connector = DuckDBConnector()
    db_params = {"path": "test.db"}
    connector.connect(db_params)
    mock_duckdb.connect.assert_called_once_with(
        database="test.db", read_only=True
    )


@patch("data_scribe.components.db_connectors.duckdb_connector.duckdb")
def test_duckdb_connector_connect_to_other_file(mock_duckdb):
    """Tests that DuckDBConnector connects to in-memory for other file types."""
    connector = DuckDBConnector()
    db_params = {"path": "data.parquet"}
    connector.connect(db_params)
    mock_duckdb.connect.assert_called_once_with(
        database=":memory:", read_only=False
    )


@patch("data_scribe.components.db_connectors.duckdb_connector.duckdb")
def test_duckdb_get_tables_from_db_file(mock_duckdb):
    """Tests that DuckDBConnector's get_tables queries for tables from a db file."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("table1",), ("table2",)]
    mock_duckdb.connect.return_value.cursor.return_value = mock_cursor

    connector = DuckDBConnector()
    connector.connect({"path": "analytics.db"})  # .db file path
    tables = connector.get_tables()

    assert tables == ["table1", "table2"]
    mock_cursor.execute.assert_called_once_with("SHOW ALL TABLES;")


@patch("data_scribe.components.db_connectors.duckdb_connector.duckdb")
def test_duckdb_get_tables_from_pattern(mock_duckdb):
    """Tests that DuckDBConnector's get_tables returns the pattern for non-db files."""
    connector = DuckDBConnector()
    connector.connect({"path": "data/*.csv"})  # non-.db file path
    tables = connector.get_tables()
    assert tables == ["data/*.csv"]
