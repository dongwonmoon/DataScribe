"""
This module defines a base class for SQL connectors using an `information_schema`.

It abstracts the common logic for fetching metadata (tables, columns, views,
foreign keys) that is shared across many standard SQL databases (e.g., PostgreSQL,
MySQL, MariaDB). Subclasses can inherit from `SqlBaseConnector` to reuse this
logic, only needing to provide a concrete `connect` implementation.
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
    queries.

    Subclasses are required to implement the `connect` method. They can also
    override any of the metadata methods if their SQL dialect differs from the
    standard ANSI SQL implementation provided here.
    """

    def __init__(self):
        """Initializes the connector, setting the connection state to `None`."""
        self.connection = None
        self.cursor = None
        self.dbname: str | None = None
        self.schema_name: str | None = None

    @abstractmethod
    def connect(self, db_params: Dict[str, Any]):
        """
        Abstract method for establishing a database connection.

        Subclasses must implement this method to handle the specifics of
        connecting to their target database (e.g., using `psycopg2` or `mysql-connector`).
        This method should set `self.connection`, `self.cursor`, `self.dbname`,
        and `self.schema_name`.
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

        logger.info(f"Fetching tables from schema: '{self.schema_name}'")

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
            raise ConnectorError(
                "Must connect to the DB first and set schema_name."
            )

        logger.info(
            f"Fetching columns for table: '{self.schema_name}.{table_name}'"
        )

        query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s;
        """

        self.cursor.execute(query, (self.schema_name, table_name))
        columns = [
            {"name": col[0], "type": col[1]} for col in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(columns)} columns in table '{table_name}'.")
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
            raise ConnectorError(
                "Must connect to the DB first and set schema_name."
            )

        logger.info(f"Fetching views from schema: '{self.schema_name}'")

        query = """
            SELECT table_name, view_definition
            FROM information_schema.views
            WHERE table_schema = %s;
        """

        self.cursor.execute(query, (self.schema_name,))
        views = [
            {"name": view[0], "definition": view[1]}
            for view in self.cursor.fetchall()
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
            raise ConnectorError(
                "Must connect to the DB first and set schema_name."
            )

        logger.info(
            f"Fetching foreign key relationships for schema: '{self.schema_name}'"
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

    def get_column_profile(
        self, table_name: str, column_name: str
    ) -> Dict[str, Any]:
        """
        Generates profile stats for a column using standard ANSI SQL.

        This method calculates total rows, null ratio, distinct values, and
        uniqueness. Subclasses can override this if a more efficient,
        dialect-specific implementation is available.

        Args:
            table_name: The name of the table containing the column.
            column_name: The name of the column to profile.

        Returns:
            A dictionary of statistics, e.g.,
            `{'null_ratio': 0.1, 'distinct_count': 150, 'is_unique': False}`.
            Returns 'N/A' for stats if profiling fails.
        """
        if not self.cursor or not self.schema_name:
            raise ConnectorError(
                "Must connect to the DB first and set schema_name."
            )

        query = f"""
        SELECT
            COUNT(*) AS total_count,
            SUM(CASE WHEN "{column_name}" IS NULL THEN 1 ELSE 0 END) AS null_count,
            COUNT(DISTINCT "{column_name}") AS distinct_count
        FROM "{self.schema_name}"."{table_name}"
        """

        try:
            self.cursor.execute(query)
            row = self.cursor.fetchone()

            total_count = row[0]
            null_count = row[1] if row[1] is not None else 0
            distinct_count = row[2] if row[2] is not None else 0

            if total_count == 0:
                logger.info(
                    f"  - Profile for '{table_name}.{column_name}': Table is empty."
                )
                return {
                    "null_ratio": 0,
                    "distinct_count": 0,
                    "is_unique": True,
                    "total_count": 0,
                }

            null_ratio = null_count / total_count
            is_unique = (distinct_count == total_count) and (null_count == 0)

            stats = {
                "total_count": total_count,
                "null_ratio": round(null_ratio, 2),
                "distinct_count": distinct_count,
                "is_unique": is_unique,
            }
            logger.info(
                f"  - Profile for '{table_name}.{column_name}': {stats}"
            )
            return stats

        except Exception as e:
            # Log error but don't crash the whole scan
            logger.warning(
                f"Failed to profile column '{table_name}.{column_name}': {e}",
                exc_info=True,
            )
            return {
                "null_ratio": "N/A",
                "distinct_count": "N/A",
                "is_unique": False,
                "total_count": "N/A",
            }

    def close(self):
        """Closes the database cursor and connection and resets state."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None
        logger.info(f"{self.__class__.__name__} connection closed.")
