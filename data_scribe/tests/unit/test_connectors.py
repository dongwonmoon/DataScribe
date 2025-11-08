"""
Unit tests for the database connectors.

This test suite verifies that the database connector classes in
`data_scribe.components.db_connectors` correctly handle connections,
execute the expected SQL queries, and parse the results.

For non-file-based databases (Postgres, MariaDB, etc.), the underlying
database driver libraries are mocked to avoid requiring live database
instances during testing.
"""

import pytest
from unittest.mock import patch, MagicMock
import psycopg2

from data_scribe.components.db_connectors import (
    SQLiteConnector,
    PostgresConnector,
    MariaDBConnector,
    DuckDBConnector,
    SnowflakeConnector,
)
from data_scribe.core.exceptions import ConnectorError


# --- SQLiteConnector Tests (using a real temporary database) ---

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
        "from_table": "orders", "to_table": "users",
        "from_column": "user_id", "to_column": "id"
    }
    expected_fk2 = {
        "from_table": "orders", "to_table": "products",
        "from_column": "product_id", "to_column": "id"
    }
    assert expected_fk1 in fks or expected_fk2 in fks

    connector.close()
    assert connector.connection is None


# --- PostgresConnector Tests (mocking psycopg2) ---

@patch("data_scribe.components.db_connectors.postgres_connector.psycopg2")
def test_postgres_connector_connect(mock_psycopg2):
    """Tests that PostgresConnector calls psycopg2.connect with correct params."""
    connector = PostgresConnector()
    db_params = {
        "host": "localhost", "port": 5432, "user": "admin",
        "password": "pw", "dbname": "testdb", "schema": "public",
    }
    connector.connect(db_params)
    mock_psycopg2.connect.assert_called_once_with(
        host="localhost", port=5432, user="admin", password="pw", dbname="testdb"
    )
    assert connector.schema_name == "public"


@patch("data_scribe.components.db_connectors.postgres_connector.psycopg2.connect")
def test_postgres_connector_connect_fails(mock_connect):
    """Tests that PostgresConnector raises ConnectorError on connection failure."""
    mock_connect.side_effect = psycopg2.Error("Connection failed")
    connector = PostgresConnector()
    with pytest.raises(ConnectorError, match="Failed to connect to PostgreSQL"):
        connector.connect({})


# --- MariaDBConnector Tests (mocking mysql.connector) ---

@patch("data_scribe.components.db_connectors.mariadb_connector.mysql.connector")
def test_mariadb_connector_connect(mock_mysql_connector):
    """Tests that MariaDBConnector calls mysql.connector.connect with correct params."""
    connector = MariaDBConnector()
    db_params = {
        "host": "remotehost", "user": "user",
        "password": "pw", "dbname": "proddb",
    }
    connector.connect(db_params)
    mock_mysql_connector.connect.assert_called_once_with(
        host="remotehost", user="user", password="pw",
        database="proddb", port=3306
    )


# --- DuckDBConnector Tests (mocking duckdb) ---

@patch("data_scribe.components.db_connectors.duckdb_connector.duckdb")
def test_duckdb_connector_connect_to_db_file(mock_duckdb):
    """Tests that DuckDBConnector connects to a .db file correctly."""
    connector = DuckDBConnector()
    db_params = {"path": "test.db"}
    connector.connect(db_params)
    mock_duckdb.connect.assert_called_once_with(database="test.db", read_only=True)


@patch("data_scribe.components.db_connectors.duckdb_connector.duckdb")
def test_duckdb_connector_connect_to_other_file(mock_duckdb):
    """Tests that DuckDBConnector connects to in-memory for other file types."""
    connector = DuckDBConnector()
    db_params = {"path": "data.parquet"}
    connector.connect(db_params)
    mock_duckdb.connect.assert_called_once_with(database=":memory:", read_only=False)


@patch("data_scribe.components.db_connectors.duckdb_connector.duckdb")
def test_duckdb_get_tables_from_db_file(mock_duckdb):
    """Tests that DuckDBConnector's get_tables queries for tables from a db file."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("table1",), ("table2",)]
    mock_duckdb.connect.return_value.cursor.return_value = mock_cursor

    connector = DuckDBConnector()
    connector.connect({"path": "analytics.db"}) # .db file path
    tables = connector.get_tables()

    assert tables == ["table1", "table2"]
    mock_cursor.execute.assert_called_once_with("SHOW ALL TABLES;")


@patch("data_scribe.components.db_connectors.duckdb_connector.duckdb")
def test_duckdb_get_tables_from_pattern(mock_duckdb):
    """Tests that DuckDBConnector's get_tables returns the pattern for non-db files."""
    connector = DuckDBConnector()
    connector.connect({"path": "data/*.csv"}) # non-.db file path
    tables = connector.get_tables()
    assert tables == ["data/*.csv"]


# --- SnowflakeConnector Tests (mocking snowflake.connector) ---

@patch("data_scribe.components.db_connectors.snowflake_connector.snowflake.connector")
def test_snowflake_connector_connect(mock_snowflake_connector):
    """Tests that SnowflakeConnector calls snowflake.connector.connect correctly."""
    connector = SnowflakeConnector()
    db_params = {
        "user": "sf_user", "password": "sf_password", "account": "sf_account",
        "database": "sf_db", "schema": "sf_schema", "warehouse": "sf_wh"
    }
    connector.connect(db_params)
    mock_snowflake_connector.connect.assert_called_once_with(
        user="sf_user", password="sf_password", account="sf_account",
        database="sf_db", schema="sf_schema", warehouse="sf_wh"
    )
    assert connector.schema_name == "sf_schema"
    assert connector.dbname == "sf_db"
