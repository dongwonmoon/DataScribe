import psycopg2
from typing import List, Dict, Any

from data_scribe.core.interfaces import BaseConnector
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class PostgresConnector(BaseConnector):
    """ """

    def __init__(self):
        self.connection: psycopg2.Connection | None = None
        self.cursor: psycopg2.Cursor | None = None

    def connect(self, db_params: Dict[str, Any]):
        """ """
        try:
            self.connection = psycopg2.connect(
                host=db_params.get("host", "localhost"),
                port=db_params.get("port", 5432),
                user=db_params.get("user"),
                password=db_params.get("password"),
                dbname=db_params.get("dbname"),
            )
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to PostgreSQL database.")
        except psycopg2.Error as e:
            logger.error(
                f"Failed to connect to PostgreSQL database: {e}", exc_info=True
            )
            raise ConnectionError(
                f"Failed to connect to PostgreSQL database: {e}"
            ) from e

    def get_tables(self) -> List[str]:
        if not self.cursor:
            raise RuntimeError(
                "Database connection not established. Call connect() first."
            )

        self.cursor.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        )
        tables = [table[0] for table in self.cursor.fetchall()]
        logger.info(f"Found {len(tables)} tables.")
        return tables

    def get_columns(self, table_name: str) -> List[Dict[str, str]]:
        if not self.cursor:
            raise RuntimeError(
                "Database connection not established. Call connect() first."
            )

        self.cursor.execute(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'public' AND table_name = %s;
        """,
            (table_name,),
        )
        columns = [{"name": col[0], "type": col[1]} for col in self.cursor.fetchall()]
        logger.info(f"Found {len(columns)} columns in table {table_name}.")
        return columns

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("PostgreSQL database connection closed.")
