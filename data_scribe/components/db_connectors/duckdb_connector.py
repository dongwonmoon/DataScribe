"""
This module provides a concrete implementation of the BaseConnector for DuckDB.

It handles connecting to an in-memory DuckDB instance and reading data from
local or remote files (e.g., Parquet, CSV) specified by a path pattern.
"""

import duckdb
from typing import List, Dict, Any

from data_scribe.core.interfaces import BaseConnector
from data_scribe.core.exceptions import ConnectorError
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class DuckDBConnector(BaseConnector):
    """
    Connector for reading data using DuckDB.

    This connector is designed to use DuckDB's ability to directly query
    file-based datasets (like Parquet or CSV files, including from S3)
    without a persistent database server.
    """

    def __init__(self):
        """Initializes the DuckDBConnector."""
        self.connection: duckdb.DuckDBPyConnection | None = None
        self.file_path_pattern: str | None = None

    def connect(self, db_params: Dict[str, Any]):
        """
        Initializes an in-memory DuckDB connection and loads necessary extensions.

        Args:
            db_params: A dictionary containing the 'path' to the file(s) to be read.
                       Example: {"path": "./data/*.parquet"} or {"path": "s3://bucket/data.csv"}

        Raises:
            ValueError: If the 'path' parameter is missing.
            ConnectionError: If the connection to DuckDB fails.
        """
        try:
            self.file_path_pattern = db_params.get("path")
            if not self.file_path_pattern:
                raise ValueError("Missing 'path' parameter for DuckDBConnector.")

            # Connect to an in-memory DuckDB database
            self.connection = duckdb.connect(database=":memory:")

            # If the path points to S3, install and load the httpfs extension
            if self.file_path_pattern.startswith("s3://"):
                self.connection.execute("INSTALL httpfs; LOAD httpfs;")

            logger.info("Successfully connected to DuckDB.")
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            raise ConnectorError(f"Failed to connect to DuckDB: {e}")

    def get_tables(self) -> List[str]:
        """
        Returns the file path pattern as the "table" to be analyzed.

        Since DuckDB is used to query files directly, the concept of a "table"
        in this context is the file path pattern itself.

        Returns:
            A list containing a single string: the file path pattern.
        """
        if not self.file_path_pattern:
            raise ConnectorError("Not connected to a DuckDB database.")

        return [self.file_path_pattern]

    def get_columns(self, table_name: str) -> List[Dict[str, str]]:
        """
        Describes the columns of the dataset specified by the file path pattern.

        It uses DuckDB's `DESCRIBE` and `read_auto` to infer the schema from the file(s).

        Args:
            table_name: The file path pattern to analyze (e.g., "./data/*.parquet").

        Returns:
            A list of dictionaries, each representing a column with its name and type.
        """
        if not self.connection:
            raise ConnectorError("Not connected to a DuckDB database.")

        try:
            logger.info(f"Fetching columns for table: {table_name}")

            # Use DESCRIBE on a read_auto query to get the schema
            query = f"DESCRIBE SELECT * FROM read_auto('{table_name}');"
            result = self.connection.execute(query).fetchall()

            columns = [{"name": col[0], "type": col[1]} for col in result]

            logger.info(f"Fetched columns for table: {table_name}")
            return columns

        except Exception as e:
            logger.error(f"Failed to fetch columns for table {table_name}: {e}")
            raise ConnectorError(f"Failed to fetch columns for table {table_name}: {e}")

        if not self.connection:
            raise ConnectorError(
                "Database connection not established. Call connect() first."
            )

        logger.info("Fetching views from DuckDB.")
        self.connection.execute("SELECT view_name, sql FROM duckdb_views();")
        views = [
            {"name": view[0], "definition": view[1]}
            for view in self.connection.fetchall()
        ]
        logger.info(f"Found {len(views)} views.")
        return views

    def get_foreign_keys(self) -> List[Dict[str, str]]:
        """
        Retrieves all foreign key relationships from the attached database.

        Note: This is often not applicable when querying transient file-based data,
        but is included for completeness.

        Returns:
            A list of dictionaries, each representing a foreign key relationship.
        """
        if not self.connection:
            raise ConnectorError(
                "Database connection not established. Call connect() first."
            )

        logger.info("Fetching foreign key relationships from DuckDB...")
        foreign_keys = []
        try:
            # This pragma works for attached databases but may not for file scans
            self.connection.execute("SELECT * FROM pragma_foreign_keys();")
            fk_results = self.connection.fetchall()

            fk_df = self.connection.fetchdf()

            for _, fk in fk_df.iterrows():
                foreign_keys.append(
                    {
                        "from_table": fk["fk_table"],
                        "from_column": fk["fk_column"],
                        "to_table": fk["pk_table"],
                        "to_column": fk["pk_column"],
                    }
                )
        except Exception as e:
            logger.warning(
                f"FK lookup fails in DuckDB (normal when file-based scanned): {e}"
            )

        logger.info(f"Found {len(foreign_keys)} foreign key relationships.")
        return foreign_keys

    def close(self):
        """Closes the in-memory DuckDB connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("DuckDB connection closed.")
