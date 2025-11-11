"""
This module defines `SqlBaseConnector`, a reusable and extensible base class
for connectors that interact with SQL databases supporting a standard
`information_schema`.

Design Rationale:
The purpose of this class is to abstract away the common, boilerplate SQL
queries required to fetch metadata (tables, columns, views, foreign keys).
By providing default implementations based on ANSI SQL standards for the
`information_schema`, it allows developers to add support for a new SQL-based
database with minimal effort. Often, a developer only needs to subclass this
and implement the database-specific `connect` method, inheriting all other
functionality.
"""

from abc import abstractmethod
from typing import List, Dict, Any

from schema_scribe.core.interfaces import BaseConnector
from schema_scribe.core.exceptions import ConnectorError
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class SqlBaseConnector(BaseConnector):
    """
    An abstract base class for connectors that rely on an `information_schema`.

    This class inherits from `BaseConnector` and provides default, ANSI SQL
    implementations for `get_tables`, `get_columns`, `get_views`, and
    `get_foreign_keys`.

    Subclasses are required to implement the `connect` method. They can also
    override any of the metadata methods if their specific SQL dialect differs
    from the standard implementation provided here.
    """

    def __init__(self):
        """
        Initializes the connector's state, which will be populated by `connect`.
        """
        self.connection = None
        self.cursor = None
        self.dbname: str | None = None
        self.schema_name: str | None = None

    @abstractmethod
    def connect(self, db_params: Dict[str, Any]):
        """
        Abstract method for establishing a database connection.

        Subclasses MUST implement this method to handle the specifics of
        connecting to their target database (e.g., using `psycopg2` or
        `mysql-connector`).

        A valid implementation of this method MUST set the following instance
        attributes for the other base methods to function correctly:
        - `self.connection`: The active database connection object.
        - `self.cursor`: The database cursor for executing queries.
        - `self.dbname`: The name of the database being connected to.
        - `self.schema_name`: The name of the schema to be scanned.
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
            raise ConnectorError(
                "Connection not established. The 'connect' method must be called first."
            )

        logger.info(f"Fetching tables from schema: '{self.schema_name}'")
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE';
        """
        self.cursor.execute(query, (self.schema_name,))
        tables = [row[0] for row in self.cursor.fetchall()]
        logger.info(f"Found {len(tables)} tables.")
        return tables

    def get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Retrieves column metadata for a table from the information_schema.

        This implementation fetches column name, data type, nullability, and
        primary key status.

        Args:
            table_name: The name of the table to inspect.

        Returns:
            A list of dictionaries, each representing a column, conforming to
            the `BaseConnector` interface contract.

        Raises:
            ConnectorError: If the database connection is not established.
        """
        if not self.cursor or not self.schema_name:
            raise ConnectorError(
                "Connection not established. The 'connect' method must be called first."
            )

        logger.info(
            f"Fetching columns for table: '{self.schema_name}.{table_name}'"
        )
        # This query joins columns with table constraints to identify primary keys.
        # LEFT JOINs are used because not all columns are part of a constraint.
        query = """
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable,
                CASE
                    WHEN tc.constraint_type = 'PRIMARY KEY' THEN TRUE
                    ELSE FALSE
                END AS is_pk
            FROM
                information_schema.columns c
            LEFT JOIN
                information_schema.key_column_usage kcu
                ON c.table_schema = kcu.table_schema
                AND c.table_name = kcu.table_name
                AND c.column_name = kcu.column_name
            LEFT JOIN
                information_schema.table_constraints tc
                ON kcu.constraint_name = tc.constraint_name
                AND kcu.table_schema = tc.table_schema
                AND tc.constraint_type = 'PRIMARY KEY'
            WHERE
                c.table_schema = %s AND c.table_name = %s;
        """
        self.cursor.execute(query, (self.schema_name, table_name))
        columns = [
            {
                "name": row[0],
                "type": row[1],
                "description": "",  # Not available in information_schema
                "is_nullable": row[2] == "YES",
                "is_pk": row[3] or False,
            }
            for row in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(columns)} columns in table '{table_name}'.")
        return columns

    def get_views(self) -> List[Dict[str, str]]:
        """
        Retrieves a list of views and their definitions from the information_schema.

        Returns:
            A list of dictionaries, each representing a view with its name and
            SQL definition.

        Raises:
            ConnectorError: If the database connection is not established.
        """
        if not self.cursor or not self.schema_name:
            raise ConnectorError(
                "Connection not established. The 'connect' method must be called first."
            )

        logger.info(f"Fetching views from schema: '{self.schema_name}'")
        query = """
            SELECT table_name, view_definition
            FROM information_schema.views
            WHERE table_schema = %s;
        """
        self.cursor.execute(query, (self.schema_name,))
        views = [
            {"name": row[0], "definition": row[1]}
            for row in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(views)} views.")
        return views

    def get_foreign_keys(self) -> List[Dict[str, str]]:
        """
        Retrieves all foreign key relationships from the information_schema.

        Returns:
            A list of dictionaries, each representing a foreign key, conforming
            to the `BaseConnector` interface contract.

        Raises:
            ConnectorError: If the database connection is not established.
        """
        if not self.cursor or not self.schema_name:
            raise ConnectorError(
                "Connection not established. The 'connect' method must be called first."
            )

        logger.info(f"Fetching foreign keys for schema: '{self.schema_name}'")
        query = """
            SELECT
                kcu.table_name AS source_table,
                kcu.column_name AS source_column,
                ccu.table_name AS target_table,
                ccu.column_name AS target_column
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
                "source_table": row[0],
                "source_column": row[1],
                "target_table": row[2],
                "target_column": row[3],
            }
            for row in self.cursor.fetchall()
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
            A dictionary of statistics, or 'N/A' for stats if profiling fails.
        """
        if not self.cursor or not self.schema_name:
            raise ConnectorError(
                "Connection not established. The 'connect' method must be called first."
            )

        # Use double quotes for identifiers to be ANSI SQL compliant. This is
        # safer than f-string interpolation for table/column names in the query body.
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
            if not row:
                raise ConnectorError(
                    "Column profiling query returned no results."
                )

            total_count, null_count, distinct_count = row
            total_count = total_count or 0
            null_count = null_count or 0
            distinct_count = distinct_count or 0

            if total_count == 0:
                return {
                    "null_ratio": 0.0,
                    "distinct_count": 0,
                    "is_unique": True,
                }

            null_ratio = null_count / total_count
            # A column is unique if distinct count equals total rows, and no nulls
            is_unique = (distinct_count == total_count) and (null_count == 0)

            stats = {
                "null_ratio": round(null_ratio, 2),
                "distinct_count": distinct_count,
                "is_unique": is_unique,
            }
            logger.info(
                f"  - Profile for '{table_name}.{column_name}': {stats}"
            )
            return stats
        except Exception as e:
            logger.warning(
                f"Could not profile column '{table_name}.{column_name}': {e}"
            )
            return {
                "null_ratio": "N/A",
                "distinct_count": "N/A",
                "is_unique": "N/A",
            }

    def close(self):
        """
        Safely closes the database cursor and connection if they are open.
        This method is idempotent and can be called multiple times.
        """
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None
        logger.info(f"{self.__class__.__name__} connection closed.")
