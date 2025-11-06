"""
This module provides a concrete implementation of the BaseConnector for MariaDB and MySQL databases.

It uses the `mysql-connector-python` library to handle the connection, schema extraction,
and other database interactions.
"""

import mysql.connector
from typing import List, Dict, Any

from .sql_base_connector import SqlBaseConnector
from data_scribe.core.exceptions import ConnectorError
from data_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class MariaDBConnector(SqlBaseConnector):
    """Connector for MariaDB and MySQL databases.

    This class extends SqlBaseConnector and implements the `connect` method
    specific to MariaDB/MySQL databases using the `mysql-connector-python` library.
    """

    def __init__(self):
        """Initializes the MariaDBConnector."""
        super().__init__()

    def connect(self, db_params: Dict[str, Any]):
        """
        Connects to the MariaDB/MySQL database using the provided parameters.

        Args:
            db_params: A dictionary of connection parameters, including 'host',
                       'port', 'user', 'password', and 'dbname'.

        Raises:
            ValueError: If the 'dbname' parameter is missing.
            ConnectionError: If the database connection fails.
        """
        try:
            self.dbname = db_params.get("dbname")
            self.schema_name = self.dbname
            if not self.dbname:
                raise ValueError("'dbname' (database name) parameter is required.")

            self.connection = mysql.connector.connect(
                host=db_params.get("host", "localhost"),
                port=db_params.get("port", 3306),
                user=db_params.get("user"),
                password=db_params.get("password"),
                database=self.dbname,  # Use 'database' key
            )
            self.cursor = self.connection.cursor()
            logger.info(f"Successfully connected to MariaDB/MySQL DB '{self.dbname}'.")
        except mysql.connector.Error as e:
            logger.error(f"MariaDB/MySQL connection failed: {e}", exc_info=True)
            raise ConnectorError(f"MariaDB/MySQL connection failed: {e}") from e
