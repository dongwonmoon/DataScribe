"""
This module provides a concrete implementation of the BaseConnector for PostgreSQL databases.

It handles the connection to a PostgreSQL database, extraction of table and column metadata,
and closing the connection.
"""

import psycopg2
from typing import List, Dict, Any

from data_scribe.core.interfaces import BaseConnector
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class PostgresConnector(BaseConnector):
    """Connector for PostgreSQL databases.

    This class implements the BaseConnector interface to provide
    connectivity and schema extraction for PostgreSQL databases.
    """

    def __init__(self):
        """Initializes the PostgresConnector, setting connection and cursor to None."""
        self.connection: psycopg2.Connection | None = None
        self.cursor: psycopg2.Cursor | None = None

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
            self.connection = psycopg2.connect(
                host=db_params.get("host", "localhost"),
                port=db_params.get("port", 5432),
                user=db_params.get("user"),
                password=db_params.get("password"),
                dbname=db_params.get("dbname"),
            )
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to PostgreSQL database.")
        except psycopg2.Error as e:
            logger.error(
                f"Failed to connect to PostgreSQL database: {e}", exc_info=True
            )
            raise ConnectionError(
                f"Failed to connect to PostgreSQL database: {e}"
            ) from e

    def get_tables(self) -> List[str]:
        """Retrieves a list of all table names in the 'public' schema.

        Returns:
            A list of strings, where each string is a table name.

        Raises:
            RuntimeError: If the database connection has not been established.
        """
        if not self.cursor:
            logger.error("get_tables called before establishing a database connection.")
            raise RuntimeError(
                "Database connection not established. Call connect() first."
            )

        logger.info("Fetching table names from the 'public' schema.")
        self.cursor.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        )
        tables = [table[0] for table in self.cursor.fetchall()]
        logger.info(f"Found {len(tables)} tables: {tables}")
        return tables

    def get_columns(self, table_name: str) -> List[Dict[str, str]]:
        """Retrieves column information (name and type) for a given table in the 'public' schema.

        Args:
            table_name: The name of the table to inspect.

        Returns:
            A list of dictionaries, where each dictionary represents a column
            and contains its name and data type.

        Raises:
            RuntimeError: If the database connection has not been established.
        """
        if not self.cursor:
            logger.error(f"get_columns called for table '{table_name}' before establishing a database connection.")
            raise RuntimeError(
                "Database connection not established. Call connect() first."
            )

        logger.info(f"Fetching columns for table: {table_name}")
        self.cursor.execute(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'public' AND table_name = %s;
        """,
            (table_name,),
        )
        columns = [{"name": col[0], "type": col[1]} for col in self.cursor.fetchall()]
        logger.info(f"Found {len(columns)} columns in table '{table_name}'.")
        return columns

    def close(self):
        """Closes the database cursor and connection if they are open."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None
        logger.info("PostgreSQL database connection closed.")
