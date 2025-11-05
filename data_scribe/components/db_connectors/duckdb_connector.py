import duckdb
from typing import List, Dict, Any

from data_scribe.core.interfaces import BaseConnector
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class DuckDBConnector(BaseConnector):
    def __init__(self):
        self.connection = None
        self.file_path_pattern = None

    def connect(self, db_params: Dict[str, Any]):
        try:
            self.file_path_pattern = db_params.get("path")
            if not self.file_path_pattern:
                raise ValueError(
                    "Missing 'path' parameter for DuckDBConnector."
                )

            self.connection = duckdb.connect(database=":memory:")

            if self.file_path_pattern.startwith("s3://"):
                self.connection.execute(f"INSTALL httpfs; LOAD httpfs;")

            logger.info("Successfully connected to DuckDB.")
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            raise ConnectionError(f"Failed to connect to DuckDB: {e}")

    def get_tables(self) -> List[str]:
        if not self.file_path_pattern:
            raise RuntimeError("Not connected to a DuckDB database.")

        return [self.file_path_pattern]

    def get_columns(self, table_name: str) -> List[Dict[str, str]]:
        if not self.connect:
            raise RuntimeError("Not connected to a DuckDB database.")

        try:
            logger.info(f"Fetching columns for table: {table_name}")

            query = f"DESCRIBE SELECT * FROM read_auto('{table_name}');"
            result = self.connection.execute(query).fetchall()

            columns = [{"name": col[0], "type": col[1]} for col in result]

            logger.info(f"Fetched columns for table: {table_name}")
            return columns

        except Exception as e:
            logger.error(f"Failed to fetch columns for table {table_name}: {e}")
            raise RuntimeError(
                f"Failed to fetch columns for table {table_name}: {e}"
            )

    def get_views(self) -> List[Dict[str, str]]:
        """Retrieves a list of all views (in-memory or attached)."""
        if not self.connection:
            raise RuntimeError(
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
        Retrieves all foreign key relationships.
        """
        if not self.connection:
            raise RuntimeError(
                "Database connection not established. Call connect() first."
            )

        logger.info("Fetching foreign key relationships from DuckDB...")
        foreign_keys = []
        try:
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
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("DuckDB connection closed.")
