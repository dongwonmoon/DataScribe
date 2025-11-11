"""
This module provides a concrete implementation of the `SqlBaseConnector` for
MariaDB and MySQL databases.

It uses the `mysql-connector-python` library to handle the connection and
relies on the parent class for `information_schema`-based metadata extraction.
"""

import mysql.connector
from typing import Dict, Any

from .sql_base_connector import SqlBaseConnector
from schema_scribe.core.exceptions import ConnectorError
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class MariaDBConnector(SqlBaseConnector):
    """
    A concrete connector for MariaDB and MySQL databases.

    This class extends `SqlBaseConnector` and implements the `connect` method
    specific to MariaDB/MySQL using the `mysql-connector-python` library.
    For these databases, the 'schema' is synonymous with the 'database'.
    """

    def __init__(self):
        """Initializes the MariaDBConnector."""
        super().__init__()

    def connect(self, db_params: Dict[str, Any]):
        """
        Connects to a MariaDB/MySQL database using the provided parameters.

        Args:
            db_params: A dictionary of connection parameters. Expected keys
                       include `host`, `port`, `user`, `password`, and `dbname`.

        Raises:
            ValueError: If the 'dbname' parameter is missing.
            ConnectorError: If the database connection fails.
        """
        try:
            self.dbname = db_params.get("dbname")
            self.schema_name = self.dbname
            if not self.dbname:
                raise ValueError(
                    "'dbname' (database name) parameter is required."
                )

            self.connection = mysql.connector.connect(
                host=db_params.get("host", "localhost"),
                port=db_params.get("port", 3306),
                user=db_params.get("user"),
                password=db_params.get("password"),
                database=self.dbname,  # Use 'database' key
            )
            self.cursor = self.connection.cursor()
            logger.info(
                f"Successfully connected to MariaDB/MySQL DB '{self.dbname}'."
            )
        except mysql.connector.Error as e:
            logger.error(f"MariaDB/MySQL connection failed: {e}", exc_info=True)
            raise ConnectorError(f"MariaDB/MySQL connection failed: {e}") from e
