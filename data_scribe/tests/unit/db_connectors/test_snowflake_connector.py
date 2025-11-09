"""
Unit tests for the SnowflakeConnector.
"""
from unittest.mock import patch

from data_scribe.components.db_connectors import SnowflakeConnector


@patch(
    "data_scribe.components.db_connectors.snowflake_connector.snowflake.connector"
)
def test_snowflake_connector_connect(mock_snowflake_connector):
    """Tests that SnowflakeConnector calls snowflake.connector.connect correctly."""
    connector = SnowflakeConnector()
    db_params = {
        "user": "sf_user",
        "password": "sf_password",
        "account": "sf_account",
        "database": "sf_db",
        "schema": "sf_schema",
        "warehouse": "sf_wh",
    }
    connector.connect(db_params)
    mock_snowflake_connector.connect.assert_called_once_with(
        user="sf_user",
        password="sf_password",
        account="sf_account",
        database="sf_db",
        schema="sf_schema",
        warehouse="sf_wh",
    )
    assert connector.schema_name == "sf_schema"
    assert connector.dbname == "sf_db"
