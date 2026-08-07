"""
Unit tests for the PostgresConnector.
"""

import pytest
from unittest.mock import patch
import psycopg2

from schema_scribe.components.db_connectors import PostgresConnector
from schema_scribe.core.exceptions import ConnectorError


@patch("schema_scribe.components.db_connectors.postgres_connector.psycopg2")
def test_postgres_connector_connect(mock_psycopg2):
    """Tests that PostgresConnector calls psycopg2.connect with correct params."""
    connector = PostgresConnector()
    db_params = {
        "host": "localhost",
        "port": 5432,
        "user": "admin",
        "password": "pw",
        "dbname": "testdb",
        "schema": "public",
    }
    connector.connect(db_params)
    mock_psycopg2.connect.assert_called_once_with(
        host="localhost",
        port=5432,
        user="admin",
        password="pw",
        dbname="testdb",
        options="-c default_transaction_read_only=on",
    )
    assert connector.schema_name == "public"


def test_connect_enforces_read_only(mocker):
    mock_connect = mocker.patch("psycopg2.connect")
    connector = PostgresConnector()
    connector.connect({"host": "h", "user": "u", "password": "p", "dbname": "d"})
    kwargs = mock_connect.call_args.kwargs
    assert kwargs["options"] == "-c default_transaction_read_only=on"


@patch(
    "schema_scribe.components.db_connectors.postgres_connector.psycopg2.connect"
)
def test_postgres_connector_connect_fails(mock_connect):
    """Tests that PostgresConnector raises ConnectorError on connection failure."""
    mock_connect.side_effect = psycopg2.Error("Connection failed")
    connector = PostgresConnector()
    with pytest.raises(ConnectorError, match="Failed to connect to PostgreSQL"):
        connector.connect({})
