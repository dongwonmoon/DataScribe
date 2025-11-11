"""
This module provides a concrete implementation of the `SqlBaseConnector` for
PostgreSQL databases.
"""

import psycopg2
from typing import Dict, Any

from .sql_base_connector import SqlBaseConnector
from schema_scribe.core.exceptions import ConnectorError
from schema_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class PostgresConnector(SqlBaseConnector):
    """
    A concrete connector for PostgreSQL databases.

    This class extends `SqlBaseConnector` and implements the `connect` method
    specific to PostgreSQL using the `psycopg2` library. It relies on the
    parent class for all `information_schema`-based metadata retrieval.
    """

    def __init__(self):
        """Initializes the PostgresConnector."""
        super().__init__()

    def connect(self, db_params: Dict[str, Any]):
        """
        Connects to a PostgreSQL database using the provided parameters.

        Args:
            db_params: A dictionary of connection parameters. Expected keys
                       include `host`, `port`, `user`, `password`, `dbname`,
                       and an optional `schema`.

        Raises:
            ConnectorError: If the connection to the database fails.
        """
        logger.info(
            f"Connecting to PostgreSQL database with params: {db_params}"
        )
        try:
            self.schema_name = db_params.get("schema", "public")
            self.dbname = db_params.get("dbname")

            self.connection = psycopg2.connect(
                host=db_params.get("host", "localhost"),
                port=db_params.get("port", 5432),
                user=db_params.get("user"),
                password=db_params.get("password"),
                dbname=self.dbname,
            )
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to PostgreSQL database.")
        except psycopg2.Error as e:
            logger.error(
                f"Failed to connect to PostgreSQL database: {e}", exc_info=True
            )
            raise ConnectorError(
                f"Failed to connect to PostgreSQL database: {e}"
            ) from e
