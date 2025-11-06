"""
This module defines a base class for SQL-based database connectors that use an information schema.

It abstracts the common logic for fetching metadata (tables, columns, views, foreign keys)
that is shared across many SQL databases like PostgreSQL, MySQL, and MariaDB.
"""
from abc import abstractmethod
from typing import List, Dict, Any

from data_scribe.core.interfaces import BaseConnector
from data_scribe.core.exceptions import ConnectorError
from data_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class SqlBaseConnector(BaseConnector):
    """
    An abstract base class for connectors that rely on an `information_schema`.

    This class provides default implementations for `get_tables`, `get_columns`,
    `get_views`, and `get_foreign_keys` based on standard `information_schema`
    queries. Connectors for specific SQL databases can inherit from this class
    and only need to implement the `connect` method and `close` if necessary.
    """

    def __init__(self):
        """Initializes the SqlBaseConnector."""
        self.connection = None
        self.cursor = None
        self.dbname: str | None = None
        self.schema_name: str | None = None

    @abstractmethod
    def connect(self, db_params: Dict[str, Any]):
        """
        Abstract method for establishing a database connection.
        Subclasses must implement this method.
        """
        pass

    def get_tables(self) -> List[str]:
        """
        Retrieves a list of table names from the information_schema.

        Returns:
            A list of table names in the current schema.

        Raises:
            ConnectorError: If the database connection is not established.
        """
        if not self.cursor or not self.schema_name:
            raise ConnectorError("Must connect to the DB first")

        logger.info(f"Fetching tables from schema: {self.schema_name}")

        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE';
        """

        self.cursor.execute(query, (self.schema_name,))
        tables = [table[0] for table in self.cursor.fetchall()]
        logger.info(f"Found {len(tables)} tables.")
        return tables

    def get_columns(self, table_name: str) -> List[Dict[str, str]]:
        """
        Retrieves column information for a given table from the information_schema.

        Args:
            table_name: The name of the table to inspect.

        Returns:
            A list of dictionaries, each representing a column with its name and type.

        Raises:
            ConnectorError: If the database connection is not established.
        """
        if not self.cursor or not self.schema_name:
            raise ConnectorError("Must connect to the DB first and set schema_name.")

        logger.info(f"Fetching columns for table: {self.schema_name}.{table_name}")

        query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s;
        """

        self.cursor.execute(query, (self.schema_name, table_name))
        columns = [{"name": col[0], "type": col[1]} for col in self.cursor.fetchall()]
        logger.info(f"Found {len(columns)} columns in table {table_name}.")
        return columns

    def get_views(self) -> List[Dict[str, str]]:
        """
        Retrieves a list of views and their definitions from the information_schema.

        Returns:
            A list of dictionaries, each representing a view with its name and definition.

        Raises:
            ConnectorError: If the database connection is not established.
        """
        if not self.cursor or not self.schema_name:
            raise ConnectorError("Must connect to the DB first and set schema_name.")

        logger.info(f"Fetching views from schema: {self.schema_name}")

        query = """
            SELECT table_name, view_definition
            FROM information_schema.views
            WHERE table_schema = %s;
        """

        self.cursor.execute(query, (self.schema_name,))
        views = [
            {"name": view[0], "definition": view[1]} for view in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(views)} views.")
        return views

    def get_foreign_keys(self) -> List[Dict[str, str]]:
        """
        Retrieves all foreign key relationships from the information_schema.

        Returns:
            A list of dictionaries, each representing a foreign key relationship.

        Raises:
            ConnectorError: If the database connection is not established.
        """
        if not self.cursor or not self.schema_name:
            raise ConnectorError("Must connect to the DB first and set schema_name.")

        logger.info(
            f"Fetching foreign key relationships for schema: {self.schema_name}"
        )

        query = """
        SELECT
            kcu.table_name AS from_table,
            kcu.column_name AS from_column,
            ccu.table_name AS to_table,
            ccu.column_name AS to_column
        FROM
            information_schema.table_constraints AS tc
        JOIN
            information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN
            information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE
            tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = %s;
        """

        self.cursor.execute(query, (self.schema_name,))
        foreign_keys = [
            {
                "from_table": fk[0],
                "from_column": fk[1],
                "to_table": fk[2],
                "to_column": fk[3],
            }
            for fk in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(foreign_keys)} foreign key relationships.")
        return foreign_keys

    def close(self):
        """Closes the database cursor and connection."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None
        logger.info(f"{self.__class__.__name__} connection closed.")
