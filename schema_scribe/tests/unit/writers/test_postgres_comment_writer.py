"""
Unit tests for the PostgresCommentWriter.
"""

import pytest
from unittest.mock import MagicMock

from schema_scribe.components.writers import PostgresCommentWriter
from schema_scribe.components.db_connectors import PostgresConnector
from schema_scribe.core.exceptions import (
    ConfigError,
    ConnectorError,
    WriterError,
)


@pytest.fixture
def mock_postgres_connector():
    """Provides a mock PostgresConnector instance."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit = MagicMock()
    mock_conn.rollback = MagicMock()

    mock_connector = MagicMock(spec=PostgresConnector)
    mock_connector.connection = mock_conn
    mock_connector.cursor = mock_cursor
    mock_connector.schema_name = "public"
    return mock_connector


@pytest.fixture
def mock_catalog_data():
    """Provides mock catalog data for testing."""
    return {
        "views": [
            {
                "name": "my_view",
                "ai_summary": "Summary of my_view.",
                "definition": "SELECT * FROM some_table",
            }
        ],
        "tables": [
            {
                "name": "my_table",
                "columns": [
                    {
                        "name": "col1",
                        "type": "TEXT",
                        "description": "Description for col1.",
                    },
                    {
                        "name": "col2",
                        "type": "INT",
                        "description": "Description for col2 with 'quotes'.",
                    },
                ],
            }
        ],
    }


def test_postgres_comment_writer_write_success(
    mock_postgres_connector, mock_catalog_data
):
    """Tests successful writing of comments to PostgreSQL."""
    writer = PostgresCommentWriter()
    writer.write(mock_catalog_data, db_connector=mock_postgres_connector)

    # Assert view comment
    mock_postgres_connector.cursor.execute.assert_any_call(
        'COMMENT ON VIEW "public"."my_view" IS %s;', ("Summary of my_view.",)
    )

    # Assert column comments
    mock_postgres_connector.cursor.execute.assert_any_call(
        'COMMENT ON COLUMN "public"."my_table"."col1" IS %s;',
        ("Description for col1.",),
    )
    mock_postgres_connector.cursor.execute.assert_any_call(
        'COMMENT ON COLUMN "public"."my_table"."col2" IS %s;',
        ("Description for col2 with ''quotes''.",),
    )

    mock_postgres_connector.connection.commit.assert_called_once()
    mock_postgres_connector.connection.rollback.assert_not_called()


def test_postgres_comment_writer_missing_db_connector():
    """Tests that ConfigError is raised if db_connector is missing."""
    writer = PostgresCommentWriter()
    with pytest.raises(
        ConfigError, match="PostgresCommentWriter requires 'db_connector'"
    ):
        writer.write({}, some_other_arg="value")


def test_postgres_comment_writer_wrong_db_connector_type():
    """Tests that ConfigError is raised if db_connector is not PostgresConnector."""
    writer = PostgresCommentWriter()
    mock_connector = MagicMock(spec=PostgresConnector)
    mock_connector.connection = None  # Simulate not connected
    with pytest.raises(
        ConfigError,
        match="PostgresCommentWriter is only compatible with 'postgres'",
    ):
        writer.write({}, db_connector=MagicMock())  # Pass a generic mock


def test_postgres_comment_writer_db_not_connected(mock_postgres_connector):
    """Tests that ConnectorError is raised if the provided db_connector is not connected."""
    mock_postgres_connector.connection = None  # Simulate not connected
    writer = PostgresCommentWriter()
    with pytest.raises(ConnectorError, match="db_connector is not connected."):
        writer.write({}, db_connector=mock_postgres_connector)


def test_postgres_comment_writer_db_error_rollback(
    mock_postgres_connector, mock_catalog_data
):
    """Tests that changes are rolled back on database error."""
    mock_postgres_connector.cursor.execute.side_effect = Exception(
        "DB write error"
    )

    writer = PostgresCommentWriter()
    with pytest.raises(
        WriterError, match="Error writing comments to PostgreSQL"
    ):
        writer.write(mock_catalog_data, db_connector=mock_postgres_connector)

    mock_postgres_connector.connection.commit.assert_not_called()
    mock_postgres_connector.connection.rollback.assert_called_once()
