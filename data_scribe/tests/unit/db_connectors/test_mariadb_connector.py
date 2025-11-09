"""
Unit tests for the MariaDBConnector.
"""

from unittest.mock import patch

from data_scribe.components.db_connectors import MariaDBConnector


@patch("data_scribe.components.db_connectors.mariadb_connector.mysql.connector")
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
