"""
This module provides a concrete implementation of the `SqlBaseConnector` for
PostgreSQL databases.

Design Rationale:
This class demonstrates the extensibility of `SqlBaseConnector`. It acts as a
thin wrapper, only needing to implement the `connect` method using the `psycopg2`
library. All other metadata extraction methods (`get_tables`, `get_columns`, etc.)
are inherited directly from the base class, as PostgreSQL has excellent support
for the standard `information_schema`.
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

    This class extends `SqlBaseConnector` and provides a PostgreSQL-specific
    implementation of the `connect` method using the `psycopg2` library.
    It inherits all metadata extraction logic from its parent.
    """

    def __init__(self):
        """Initializes the PostgresConnector by calling the parent constructor."""
        super().__init__()

    def connect(self, db_params: Dict[str, Any]):
        """
        Connects to a PostgreSQL database using the provided parameters.

        This method fulfills the contract required by `SqlBaseConnector` by
        setting the `self.connection`, `self.cursor`, `self.dbname`, and
        `self.schema_name` attributes upon a successful connection.

        Args:
            db_params: A dictionary of connection parameters. Expected keys
                       include `user`, `password`, `dbname`, and optionally:
                       - `host` (defaults to 'localhost')
                       - `port` (defaults to 5432)
                       - `schema` (defaults to 'public')

        Raises:
            ConnectorError: If the connection to the database fails.
        """
        logger.info("Connecting to PostgreSQL database...")
        try:
            # Set schema and dbname for the base class methods to use
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
