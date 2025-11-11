"""
This module provides a concrete implementation of the BaseConnector for SQLite databases.

It handles the connection to a SQLite database file, extraction of table and column metadata,
and closing the connection.
"""

import sqlite3
from typing import List, Dict, Any, Optional

from schema_scribe.core.interfaces import BaseConnector
from schema_scribe.core.exceptions import ConnectorError
from schema_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class SQLiteConnector(BaseConnector):
    """
    Connector for SQLite databases.

    This class implements the BaseConnector interface to provide
    connectivity and schema extraction for SQLite databases. It uses SQLite's
    built-in `PRAGMA` commands for efficient metadata retrieval.
    """

    def __init__(self):
        """Initializes the connector, setting the connection state to `None`."""
        self.connection: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None

    def connect(self, db_params: Dict[str, Any]):
        """Connects to the SQLite database using the provided file path.

        Args:
            db_params: A dictionary containing the database path.
                       Example: {"path": "my_database.db"}

        Raises:
            ValueError: If the 'path' parameter is missing from db_params.
            ConnectorError: If the connection to the database fails.
        """
        db_path = db_params.get("path")
        if not db_path:
            logger.error("Missing 'path' parameter for SQLiteConnector.")
            raise ValueError("Missing 'path' parameter for SQLiteConnector.")

        try:
            logger.info(f"Connecting to SQLite database at: {db_path}")
            # Establish the database connection
            self.connection = sqlite3.connect(db_path)
            # Create a cursor for executing queries
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to SQLite database.")
        except sqlite3.Error as e:
            logger.error(
                f"Failed to connect to SQLite database: {e}", exc_info=True
            )
            raise ConnectorError(
                f"Failed to connect to SQLite database: {e}"
            ) from e
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)
            raise ConnectorError(f"An unexpected error occurred: {e}") from e

    def get_tables(self) -> List[str]:
        """Retrieves a list of all table names in the connected database.

        Returns:
            A list of strings, where each string is a table name.

        Raises:
            ConnectorError: If the database connection has not been established.
        """
        if not self.cursor:
            raise ConnectorError(
                "Database connection not established. Call connect() first."
            )

        logger.info("Fetching table names from the database.")
        # Query the sqlite_master table to get the names of all tables
        self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        )
        # Extract the table names from the query result
        tables = [table[0] for table in self.cursor.fetchall()]
        logger.info(f"Found {len(tables)} tables.")
        return tables

    def get_columns(self, table_name: str) -> List[Dict[str, str]]:
        """
        Retrieves column information (name and type) for a given table.

        It uses the `PRAGMA table_info` command, which returns one row for each
        column in the specified table.

        Args:
            table_name: The name of the table to inspect.

        Returns:
            A list of dictionaries, where each dictionary represents a column
            and contains 'name' and 'type' keys.

        Raises:
            ConnectorError: If the database connection has not been established.
        """
        if not self.cursor:
            raise ConnectorError(
                "Database connection not established. Call connect() first."
            )

        logger.info(f"Fetching columns for table: '{table_name}'")
        # Use the PRAGMA table_info command to get column metadata
        self.cursor.execute(f"PRAGMA table_info('{table_name}');")
        # The result of PRAGMA table_info is a tuple: (cid, name, type, notnull, dflt_value, pk)
        # We extract just the name (index 1) and type (index 2).
        columns = [
            {"name": col[1], "type": col[2]} for col in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(columns)} columns in table '{table_name}'.")
        return columns

    def get_views(self) -> List[Dict[str, str]]:
        """
        Retrieves a list of all views and their SQL definitions.

        Returns:
            A list of dictionaries, where each represents a view and contains
            'name' and 'definition' keys.
        """
        if not self.cursor:
            raise ConnectorError(
                "Database connection not established. Call connect() first."
            )

        logger.info("Fetching views from the database.")
        self.cursor.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='view';"
        )
        views = [
            {"name": view[0], "definition": view[1]}
            for view in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(views)} views.")
        return views

    def get_foreign_keys(self) -> List[Dict[str, str]]:
        """
        Retrieves all foreign key relationships in the database.

        It iterates through each table and uses the `PRAGMA foreign_key_list`
        command to find its foreign key constraints.

        Returns:
            A list of dictionaries, each representing a single foreign key
            relationship with keys: 'from_table', 'from_column', 'to_table', 'to_column'.
        """
        if not self.cursor:
            raise ConnectorError(
                "Database connection not established. Call connect() first."
            )

        logger.info("Fetching foreign key relationships...")
        tables = self.get_tables()
        foreign_keys = []

        for table_name in tables:
            try:
                # PRAGMA foreign_key_list returns one row for each FK constraint.
                # Row format: (id, seq, table, from, to, on_update, on_delete, match)
                self.cursor.execute(f"PRAGMA foreign_key_list('{table_name}');")
                fk_results = self.cursor.fetchall()
                for fk in fk_results:
                    # fk[2] is the target table, fk[3] is the source column, fk[4] is the target column.
                    foreign_keys.append(
                        {
                            "from_table": table_name,
                            "from_column": fk[3],
                            "to_table": fk[2],
                            "to_column": fk[4],
                        }
                    )
            except sqlite3.Error as e:
                logger.warning(
                    f"Failed to get FKs for table '{table_name}': {e}"
                )

        logger.info(f"Found {len(foreign_keys)} foreign key relationships.")
        return foreign_keys

    def get_column_profile(
        self, table_name: str, column_name: str
    ) -> Dict[str, Any]:
        """
        Generates profile stats for a SQLite column using a single, efficient query.

        This method calculates the total row count, null ratio, distinct value count,
        and whether the column is unique.

        Args:
            table_name: The name of the table containing the column.
            column_name: The name of the column to profile.

        Returns:
            A dictionary of statistics, e.g.,
            `{'null_ratio': 0.1, 'distinct_count': 150, 'is_unique': False}`.
            Returns 'N/A' for stats if profiling fails.
        """
        if not self.cursor:
            raise ConnectorError(
                "Database connection not established. Call connect() first."
            )

        # This single query replaces 3 separate ones for efficiency.
        query = f"""
        SELECT
            COUNT(*) AS total_count,
            SUM(CASE WHEN "{column_name}" IS NULL THEN 1 ELSE 0 END) AS null_count,
            COUNT(DISTINCT "{column_name}") AS distinct_count
        FROM "{table_name}"
        """

        try:
            self.cursor.execute(query)
            row = self.cursor.fetchone()

            total_count = row[0]
            null_count = row[1] if row[1] is not None else 0
            distinct_count = row[2] if row[2] is not None else 0

            # Handle case for an empty table to avoid division by zero.
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
            # A column is unique if all values are distinct and there are no nulls.
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
        except sqlite3.Error as e:
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
        """Closes the database connection and resets the connector's state."""
        if self.connection:
            logger.info("Closing SQLite database connection.")
            self.connection.close()
            # Reset connection and cursor attributes to None
            self.connection = None
            self.cursor = None
            logger.info("SQLite database connection closed.")
