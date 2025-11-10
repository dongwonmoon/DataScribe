"""
This module provides a concrete implementation of the `BaseConnector` for DuckDB.

It has a dual purpose:
1.  Connecting to a persistent DuckDB database file (`.db` or `.duckdb`).
2.  Using an in-memory DuckDB instance to read and query other data files
    (e.g., Parquet, CSV, JSON) using DuckDB's `read_auto` function.
"""

import duckdb
from typing import List, Dict, Any

from .sql_base_connector import SqlBaseConnector
from data_scribe.core.exceptions import ConnectorError
from data_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class DuckDBConnector(SqlBaseConnector):
    """
    Connector for reading data using DuckDB.

    This connector can connect to a persistent DuckDB database file or use an
    in-memory database to query other file formats like Parquet and CSV.
    When querying non-DB files, it treats the file path pattern as the "table".

    Note: Some metadata methods (`get_views`, `get_foreign_keys`) are not
    supported and will return empty results.
    """

    def __init__(self):
        """Initializes the DuckDBConnector."""
        super().__init__()
        self.file_path_pattern: str | None = None

    def connect(self, db_params: Dict[str, Any]):
        """
        Initializes a DuckDB connection.

        If the 'path' parameter ends with '.db' or '.duckdb', it connects to
        that file as a persistent database. Otherwise, it initializes an
        in-memory database, assuming the path is a pattern for querying files
        directly (e.g., './data/*.parquet', 's3://bucket/file.csv').

        Args:
            db_params: A dictionary containing the 'path' to the database file
                       or file pattern to be read.

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

            self.file_path_pattern = path

            # For file-based queries (not a persistent .db file), we still use
            # an in-memory DB and query via `read_auto`.
            db_file = path if path.endswith((".db", ".duckdb")) else ":memory:"

            # When querying files directly, read_only should be False to allow
            # extensions like httpfs to be installed if needed.
            read_only = db_file != ":memory:"

            self.connection = duckdb.connect(
                database=db_file, read_only=read_only
            )

            if self.file_path_pattern.startswith("s3://"):
                self.connection.execute("INSTALL httpfs; LOAD httpfs;")

            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to DuckDB.")
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}", exc_info=True)
            raise ConnectorError(f"Failed to connect to DuckDB: {e}") from e

    def get_tables(self) -> List[str]:
        """
        Returns a list of tables and views from the DuckDB database.

        If the connection is for a file pattern (e.g., '*.parquet'), this method
        returns a list containing just that pattern, treating it as the table.

        Returns:
            A list of table/view names or the file pattern.
        """
        if not self.cursor:
            raise ConnectorError("Not connected to a DuckDB database.")

        # If we are in file-query mode, the "table" is the file path pattern
        if not self.file_path_pattern.endswith((".db", ".duckdb")):
            return [self.file_path_pattern]

        logger.info("Fetching tables and views from DuckDB.")
        self.cursor.execute("SHOW ALL TABLES;")
        tables = [row[0] for row in self.cursor.fetchall()]
        logger.info(f"Found {len(tables)} tables/views.")
        return tables

    def get_columns(self, table_name: str) -> List[Dict[str, str]]:
        """
        Describes the columns of a table, view, or file-based dataset.

        If the `table_name` is a file path or pattern, it uses DuckDB's
        `read_auto` function to infer the schema. Otherwise, it uses a
        standard `DESCRIBE` query.

        Args:
            table_name: The name of the table, view, or file pattern to describe.

        Returns:
            A list of dictionaries, each representing a column with its name and type.

        Raises:
            ConnectorError: If the query fails.
        """
        if not self.cursor:
            raise ConnectorError("Not connected to a DuckDB database.")

        try:
            logger.info(f"Fetching columns for: '{table_name}'")

            # If the table_name is a file path, use read_auto for schema inference.
            if not table_name.endswith((".db", ".duckdb")) and (
                "." in table_name or "/" in table_name or "*" in table_name
            ):
                query = f"DESCRIBE SELECT * FROM read_auto('{table_name}');"
            else:  # Otherwise, assume it's a standard table/view name
                query = f'DESCRIBE "{table_name}";'

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

    def get_views(self) -> List[Dict[str, str]]:
        """
        Retrieves a list of views. Not fully supported for all DuckDB modes.
        """
        logger.warning("`get_views` is not fully supported in DuckDBConnector.")
        return []

    def get_foreign_keys(self) -> List[Dict[str, str]]:
        """
        Retrieves foreign keys. Not supported in DuckDBConnector.
        """
        logger.warning(
            "`get_foreign_keys` is not supported in DuckDBConnector."
        )
        return []

    def get_column_profile(
        self, table_name: str, column_name: str
    ) -> Dict[str, Any]:
        """
        Generates profile stats for a column.

        Note: This uses a generic query that may be slow on very large file-based
        datasets, as it requires a full scan.

        Args:
            table_name: The name of the table, view, or file pattern.
            column_name: The name of the column to profile.

        Returns:
            A dictionary of statistics.
        """
        if not self.cursor:
            raise ConnectorError("Not connected to a DuckDB database.")

        # Use read_auto for file patterns, otherwise use table name directly.
        source = (
            f"read_auto('{table_name}')"
            if not table_name.endswith((".db", ".duckdb"))
            and ("." in table_name or "/" in table_name or "*" in table_name)
            else f'"{table_name}"'
        )

        query = f"""
        SELECT
            COUNT(*) AS total_count,
            SUM(CASE WHEN "{column_name}" IS NULL THEN 1 ELSE 0 END) AS null_count,
            COUNT(DISTINCT "{column_name}") AS distinct_count
        FROM {source}
        """

        try:
            self.cursor.execute(query)
            row = self.cursor.fetchone()
            total_count, null_count, distinct_count = (
                row[0],
                row[1] or 0,
                row[2] or 0,
            )

            if total_count == 0:
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
            return stats
        except Exception as e:
            logger.warning(
                f"Failed to profile '{table_name}.{column_name}': {e}"
            )
            return {
                "null_ratio": "N/A",
                "distinct_count": "N/A",
                "is_unique": False,
                "total_count": "N/A",
            }
