"""
This module provides a concrete implementation of the BaseConnector for PostgreSQL databases.

It handles the connection to a PostgreSQL database, extraction of table and column metadata,
and closing the connection.
"""

import psycopg2
from typing import List, Dict, Any

from .sql_base_connector import SqlBaseConnector
from data_scribe.core.exceptions import ConnectorError
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class PostgresConnector(SqlBaseConnector):
    """Connector for PostgreSQL databases.

    This class extends SqlBaseConnector and implements the `connect` method
    specific to PostgreSQL databases using the `psycopg2` library.
    """

    def __init__(self):
        """Initializes the PostgresConnector, setting connection and cursor to None."""
        super().__init__()

    def connect(self, db_params: Dict[str, Any]):
        """Connects to the PostgreSQL database using the provided parameters.

        Args:
            db_params: A dictionary containing connection parameters like
                       host, port, user, password, and dbname.

        Raises:
            ConnectionError: If the connection to the database fails.
        """
        logger.info(f"Connecting to PostgreSQL database with params: {db_params}")
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
