"""
This module provides a concrete implementation of the BaseConnector for DuckDB.

It handles connecting to a DuckDB database file or an in-memory instance
for reading data from other file types (e.g., Parquet, CSV), including
scanning entire S3 or local directories.
"""

import duckdb
import os
from typing import List, Dict, Any, Optional

from schema_scribe.core.interfaces import BaseConnector
from schema_scribe.core.exceptions import ConnectorError
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class DuckDBConnector(BaseConnector):
    """
    Connector for reading data using DuckDB.

    This connector can connect to:
    1. A persistent DuckDB database file (e.g., 'analytics.db').
    2. A single file or glob pattern (e.g., 'data.csv', 'data/*.parquet').
    3. A directory in S3 or locally (e.g., 's3://my-bucket/data/', './local_data/').

    It intelligently routes its methods (`get_tables`, `get_columns`) based on
    the connection type to provide a unified interface for these different sources.
    """

    def __init__(self):
        """
        Initializes the DuckDBConnector and its state attributes.
        """
        self.connection: Optional[duckdb.DuckDBPyConnection] = None
        self.cursor: Optional[duckdb.DuckDBCursor] = None
        self.base_path: str = ""
        self.is_directory_scan: bool = False
        self.is_s3: bool = False

    def connect(self, db_params: Dict[str, Any]):
        """
        Initializes a DuckDB connection based on the provided path.

        The connection type is determined by the `path` parameter:
        - If it ends in `.db` or `.duckdb`, it connects to a persistent file.
        - Otherwise, it uses an in-memory database to query a file, pattern, or directory.
        - If the path starts with `s3://`, it automatically installs and loads the `httpfs` extension.

        Args:
            db_params: A dictionary containing the 'path' to the database file
                       or file/directory pattern to be read.

        Raises:
            ValueError: If the 'path' parameter is missing.
            ConnectorError: If the connection fails for any reason.
        """
        try:
            path = db_params.get("path")
            if not path:
                raise ValueError(
                    "Missing 'path' parameter for DuckDBConnector."
                )

            self.base_path = path

            # 1. Determine connection type
            db_file = ":memory:"  # Default to in-memory
            read_only = False

            if path.endswith((".db", ".duckdb")):
                db_file = path
                read_only = True
                self.is_directory_scan = False
                logger.info(f"Connecting to persistent DuckDB file: '{path}'")
            else:
                # Path is a file pattern or directory, so use an in-memory DB for querying.
                logger.info(
                    f"Connecting to in-memory DuckDB for path: '{path}'"
                )
                if path.endswith(("/") or "*" in path):
                    self.is_directory_scan = True

            # 2. Establish connection
            self.connection = duckdb.connect(
                database=db_file, read_only=read_only
            )
            self.cursor = self.connection.cursor()

            # 3. Handle S3 paths by installing the httpfs extension.
            if path.startswith("s3://"):
                self.is_s3 = True
                try:
                    self.cursor.execute("INSTALL httpfs; LOAD httpfs;")
                    logger.info("Installed and loaded httpfs for S3 access.")
                except Exception as e:
                    logger.warning(
                        f"Could not install/load httpfs, S3 access may fail: {e}"
                    )

            logger.info("Successfully connected to DuckDB.")
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}", exc_info=True)
            raise ConnectorError(f"Failed to connect to DuckDB: {e}") from e

    def _get_full_path(self, table_name: str) -> str:
        """
        Constructs the full, queryable path for a given file name ("table").

        - In a directory scan, it joins the base directory path with the file name.
        - Otherwise, it returns the original base path, which is already the full path.

        Args:
            table_name: The name of the "table", which is a file name in this context.

        Returns:
            The full path to the file that can be used in a `read_auto` query.
        """
        if self.is_directory_scan:
            # Ensure a single slash between the base path and the file name.
            if self.base_path.endswith("/"):
                return f"{self.base_path}{table_name}"
            else:
                return f"{self.base_path}/{table_name}"

        # If not a directory scan, the base_path is the full file/pattern path.
        return self.base_path

    def get_tables(self) -> List[str]:
        """
        Returns a list of "tables" based on the connection type.

        - If connected to a `.db` file, it returns the actual tables and views.
        - If scanning a directory, it returns the file names within that directory.
        - If given a single file or glob pattern, it returns the pattern itself.

        Returns:
            A list of strings, where each string is a table, view, or file name.
        """
        if not self.cursor:
            raise ConnectorError("Not connected to a DuckDB database.")

        # Case 1: Persistent .db file. Query the schema for tables and views.
        if self.base_path.endswith((".db", ".duckdb")):
            logger.info(
                f"Fetching tables and views from DB: '{self.base_path}'"
            )
            self.cursor.execute("SHOW ALL TABLES;")
            tables = [row[0] for row in self.cursor.fetchall()]
            logger.info(f"Found {len(tables)} tables/views.")
            return tables

        # Case 2: Directory scan. Use glob to find files in the directory.
        if self.is_directory_scan:
            glob_func = "s3_glob" if self.is_s3 else "glob"

            # Ensure path ends with a wildcard for globbing
            glob_path = self.base_path
            if not glob_path.endswith(("*", "*/")):
                if not glob_path.endswith("/"):
                    glob_path += "/"
                glob_path += "*.*"  # Glob for common file types

            query = (
                f"SELECT basename(file_name) FROM {glob_func}('{glob_path}')"
            )
            logger.info(f"Globbing for files using query: {query}")

            try:
                self.cursor.execute(query)
                # Return just the file names, not the full path, as "tables".
                tables = [row[0] for row in self.cursor.fetchall()]
                logger.info(f"Found {len(tables)} files in directory.")
                return tables
            except Exception as e:
                logger.error(
                    f"Failed to glob files at '{self.base_path}': {e}",
                    exc_info=True,
                )
                raise ConnectorError(f"Failed to list files: {e}")

        # Case 3: Single file or pattern. Return the path itself as the "table".
        logger.info(f"Using single file pattern as table: '{self.base_path}'")
        return [self.base_path]

    def get_columns(self, table_name: str) -> List[Dict[str, str]]:
        """
        Describes the columns of a table, view, or file-based dataset.

        - For `.db` files, it uses a standard `DESCRIBE` query.
        - For file/directory scans, it uses `read_auto` to infer the schema from the file.

        Args:
            table_name: The name of the table, view, or file to describe.

        Returns:
            A list of dictionaries, each representing a column with its 'name' and 'type'.
        """
        if not self.cursor:
            raise ConnectorError("Not connected to a DuckDB database.")

        try:
            # If this is a DB file, table_name is a real table.
            if self.base_path.endswith((".db", ".duckdb")):
                query = f'DESCRIBE "{table_name}";'
            else:
                # This is a file scan, so table_name is a file name.
                # We must construct the full path for read_auto.
                full_path = self._get_full_path(table_name)
                logger.info(f"Fetching columns for file: '{full_path}'")
                query = f"DESCRIBE SELECT * FROM read_auto('{full_path}', SAMPLE_SIZE=50000);"

            self.cursor.execute(query)
            result = self.cursor.fetchall()
            columns = [{"name": col[0], "type": col[1]} for col in result]

            logger.info(f"Fetched {len(columns)} columns for: '{table_name}'")
            return columns

        except Exception as e:
            logger.error(
                f"Failed to fetch columns for '{table_name}': {e}",
                exc_info=True,
            )
            raise ConnectorError(
                f"Failed to fetch columns for '{table_name}': {e}"
            ) from e

    def get_column_profile(
        self, table_name: str, column_name: str
    ) -> Dict[str, Any]:
        """
        Generates profile stats for a column in a DuckDB table or file.

        This method dynamically builds a query to calculate statistics, using
        `read_auto` for file-based sources.

        Args:
            table_name: The name of the table, view, or file.
            column_name: The name of the column to profile.

        Returns:
            A dictionary of statistics.
        """
        if not self.cursor:
            raise ConnectorError("Not connected to a DuckDB database.")

        # 1. Determine the source (a table name or a file-based subquery).
        source_query = ""
        if self.base_path.endswith((".db", ".duckdb")):
            source_query = f'"{table_name}"'  # It's a table
        else:
            # It's a file, so create a subquery using read_auto.
            full_path = self._get_full_path(table_name)
            source_query = f"(SELECT * FROM read_auto('{full_path}'))"

        # 2. Build the profiling query.
        # (f-string for column_name is safe as it comes from get_columns).
        query = f"""
        SELECT
            COUNT(*) AS total_count,
            SUM(CASE WHEN "{column_name}" IS NULL THEN 1 ELSE 0 END) AS null_count,
            COUNT(DISTINCT "{column_name}") AS distinct_count
        FROM {source_query} t
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

    def get_views(self) -> List[Dict[str, str]]:
        """
        Retrieves a list of all views and their SQL definitions.
        This is only supported for persistent `.db` file connections.
        """
        if not self.cursor:
            raise ConnectorError("Not connected to a DuckDB database.")

        if not self.base_path.endswith((".db", ".duckdb")):
            logger.info("Views are not supported for file/directory scans.")
            return []

        logger.info("Fetching views from the database.")
        self.cursor.execute("SELECT view_name, sql FROM duckdb_views();")
        views = [
            {"name": view[0], "definition": view[1]}
            for view in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(views)} views.")
        return views

    def get_foreign_keys(self) -> List[Dict[str, str]]:
        """
        Retrieves all foreign key relationships.
        This is only supported for persistent `.db` file connections.
        Includes a fallback for older DuckDB versions.
        """
        if not self.cursor:
            raise ConnectorError("Not connected to a DuckDB database.")

        if not self.base_path.endswith((".db", ".duckdb")):
            logger.info(
                "Foreign keys are not supported for file/directory scans."
            )
            return []

        logger.info("Fetching foreign key relationships...")
        try:
            # DuckDB >= 0.9.0 has a more robust constraints function.
            self.cursor.execute(
                """
                SELECT
                    fk.table_name AS from_table,
                    fk.column_names[1] AS from_column,
                    pk.table_name AS to_table,
                    pk.column_names[1] AS to_column
                FROM duckdb_constraints() fk
                JOIN duckdb_constraints() pk ON fk.primary_key_index = pk.constraint_index
                WHERE fk.constraint_type = 'FOREIGN KEY'
            """
            )
        except duckdb.CatalogException:
            # Fallback for older DuckDB versions.
            logger.warning(
                "Using legacy foreign key query for older DuckDB version."
            )
            self.cursor.execute("SELECT * FROM duckdb_foreign_keys();")

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
        """Closes the database connection if it is open."""
        if self.connection:
            logger.info("Closing DuckDB database connection.")
            self.connection.close()
            self.connection = None
            self.cursor = None
            logger.info("DuckDB database connection closed.")
