"""
Unit tests for the MariaDBConnector.
"""

from unittest.mock import patch

import mysql.connector

from schema_scribe.components.db_connectors import MariaDBConnector


@patch(
    "schema_scribe.components.db_connectors.mariadb_connector.mysql.connector"
)
def test_mariadb_connector_connect(mock_mysql_connector):
    """Tests that MariaDBConnector calls mysql.connector.connect with correct params."""
    connector = MariaDBConnector()
    db_params = {
        "host": "remotehost",
        "user": "user",
        "password": "pw",
        "dbname": "proddb",
    }
    connector.connect(db_params)
    mock_mysql_connector.connect.assert_called_once_with(
        host="remotehost",
        user="user",
        password="pw",
        database="proddb",
        port=3306,
    )


def test_connect_enforces_read_only(mocker):
    mock_cursor = mocker.MagicMock()
    mock_conn = mocker.MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mocker.patch("mysql.connector.connect", return_value=mock_conn)
    connector = MariaDBConnector()
    connector.connect({"host": "h", "user": "u", "password": "p", "dbname": "d"})
    mock_cursor.execute.assert_any_call("SET SESSION TRANSACTION READ ONLY")
