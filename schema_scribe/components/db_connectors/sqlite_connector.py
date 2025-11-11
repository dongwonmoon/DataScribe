"""
This module provides a concrete implementation of the `BaseConnector` for
SQLite databases.

Design Rationale:
This connector implements the `BaseConnector` interface directly, rather than
inheriting from `SqlBaseConnector`. This is a deliberate design choice because
SQLite's native `PRAGMA` commands are generally more efficient and direct for
metadata extraction than querying the `information_schema`, which is not as
robustly supported in SQLite as in other SQL databases.
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
    A self-contained connector for SQLite databases.

    This class implements the `BaseConnector` interface, providing connectivity
    and schema extraction for SQLite databases. It uses SQLite's built-in
    `PRAGMA` commands for efficient metadata retrieval.
    """

    def __init__(self):
        """Initializes the connector, setting the connection state to `None`."""
        self.connection: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None

    def connect(self, db_params: Dict[str, Any]):
        """
        Connects to the SQLite database using the provided file path.

        Args:
            db_params: A dictionary containing the database path.
                       Must contain the key `'path'`.
                       Example: `{"path": "my_database.db"}`

        Raises:
            ValueError: If the 'path' parameter is missing from db_params.
            ConnectorError: If the connection to the database fails.
        """
        db_path = db_params.get("path")
        if not db_path:
            raise ValueError("Missing 'path' parameter for SQLiteConnector.")

        try:
            logger.info(f"Connecting to SQLite database at: {db_path}")
            self.connection = sqlite3.connect(db_path)
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to SQLite database.")
        except sqlite3.Error as e:
            logger.error(
                f"Failed to connect to SQLite database: {e}", exc_info=True
            )
            raise ConnectorError(
                f"Failed to connect to SQLite database: {e}"
            ) from e

    def get_tables(self) -> List[str]:
        """
        Retrieves a list of all table names by querying `sqlite_master`.

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
        self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        )
        tables = [row[0] for row in self.cursor.fetchall()]
        logger.info(f"Found {len(tables)} tables.")
        return tables

    def get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Retrieves column metadata for a table using `PRAGMA table_info`.

        This implementation fetches column name, data type, nullability, and
        primary key status to conform to the `BaseConnector` interface.

        Args:
            table_name: The name of the table to inspect.

        Returns:
            A list of dictionaries, each representing a column.

        Raises:
            ConnectorError: If the database connection has not been established.
        """
        if not self.cursor:
            raise ConnectorError(
                "Database connection not established. Call connect() first."
            )

        logger.info(f"Fetching columns for table: '{table_name}'")
        self.cursor.execute(f"PRAGMA table_info('{table_name}');")
        # Row format from PRAGMA table_info:
        # (cid, name, type, notnull, dflt_value, pk)
        columns = [
            {
                "name": row[1],
                "type": row[2],
                "description": "",  # Not available from PRAGMA
                "is_nullable": row[3] == 0,  # 'notnull' is 0 for nullable
                "is_pk": row[5] == 1,  # 'pk' is 1 for primary key
            }
            for row in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(columns)} columns in table '{table_name}'.")
        return columns

    def get_views(self) -> List[Dict[str, str]]:
        """
        Retrieves a list of all views and their SQL definitions from `sqlite_master`.

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
            {"name": row[0], "definition": row[1]}
            for row in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(views)} views.")
        return views

    def get_foreign_keys(self) -> List[Dict[str, str]]:
        """
        Retrieves all foreign key relationships using `PRAGMA foreign_key_list`.

        It iterates through each table in the database to find its foreign key
        constraints and assembles a complete list.

        Returns:
            A list of dictionaries, each representing a foreign key, conforming
            to the `BaseConnector` interface contract.
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
                # Row format from PRAGMA foreign_key_list:
                # (id, seq, table, from, to, on_update, on_delete, match)
                self.cursor.execute(f"PRAGMA foreign_key_list('{table_name}');")
                fk_results = self.cursor.fetchall()
                for fk in fk_results:
                    foreign_keys.append(
                        {
                            "source_table": table_name,
                            "source_column": fk[3],
                            "target_table": fk[2],
                            "target_column": fk[4],
                        }
                    )
            except sqlite3.Error as e:
                logger.warning(
                    f"Could not get FKs for table '{table_name}': {e}"
                )

        logger.info(f"Found {len(foreign_keys)} foreign key relationships.")
        return foreign_keys

    def get_column_profile(
        self, table_name: str, column_name: str
    ) -> Dict[str, Any]:
        """
        Generates profile stats for a SQLite column using a standard SQL query.

        Args:
            table_name: The name of the table containing the column.
            column_name: The name of the column to profile.

        Returns:
            A dictionary of statistics, or 'N/A' for stats if profiling fails.
        """
        if not self.cursor:
            raise ConnectorError(
                "Database connection not established. Call connect() first."
            )

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
        except sqlite3.Error as e:
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
        Safely closes the database connection if it is open.
        This method is idempotent and can be called multiple times.
        """
        if self.connection:
            logger.info("Closing SQLite database connection.")
            self.connection.close()
            self.connection = None
            self.cursor = None
            logger.info("SQLite database connection closed.")
