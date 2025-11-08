"""
This module provides a concrete implementation of the BaseConnector for DuckDB.

It handles connecting to a DuckDB database file or an in-memory instance
for reading data from other file types (e.g., Parquet, CSV).
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

    This connector can connect to a persistent DuckDB database file or use
    an in-memory database to query other file formats like Parquet and CSV.
    """

    def __init__(self):
        """Initializes the DuckDBConnector."""
        super().__init__()
        self.file_path_pattern: str | None = None

    def connect(self, db_params: Dict[str, Any]):
        """
        Initializes a DuckDB connection.

        If the path ends with '.db' or '.duckdb', it connects to the file.
        Otherwise, it uses an in-memory database, assuming the path is for
        querying files directly (e.g., CSV, Parquet).

        Args:
            db_params: A dictionary containing the 'path' to the database file
                       or file pattern to be read.
        """
        try:
            path = db_params.get("path")
            if not path:
                raise ValueError("Missing 'path' parameter for DuckDBConnector.")

            self.file_path_pattern = path
            
            # For file-based queries (not a persistent .db file), we still use
            # an in-memory DB and query via `read_auto`.
            db_file = path if path.endswith((".db", ".duckdb")) else ":memory:"
            
            # When querying files directly, read_only should be False to allow
            # extensions like httpfs to be installed if needed.
            read_only = db_file != ":memory:"

            self.connection = duckdb.connect(database=db_file, read_only=read_only)

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
        If the connection is for a file pattern, it returns the pattern itself.
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
        """
        if not self.cursor:
            raise ConnectorError("Not connected to a DuckDB database.")

        try:
            logger.info(f"Fetching columns for: {table_name}")

            # If the table_name is a file path, use read_auto for schema inference.
            if not table_name.endswith((".db", ".duckdb")) and (
                "." in table_name or "/" in table_name
            ):
                query = f"DESCRIBE SELECT * FROM read_auto('{table_name}');"
            else: # Otherwise, assume it's a standard table/view name
                query = f"DESCRIBE \"{table_name}\";"
            
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            columns = [{"name": col[0], "type": col[1]} for col in result]

            logger.info(f"Fetched {len(columns)} columns for: {table_name}")
            return columns

        except Exception as e:
            logger.error(f"Failed to fetch columns for {table_name}: {e}", exc_info=True)
            raise ConnectorError(f"Failed to fetch columns for {table_name}: {e}") from e