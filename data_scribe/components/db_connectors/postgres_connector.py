"""
This module provides a concrete implementation of the BaseConnector for PostgreSQL databases.

It handles the connection to a PostgreSQL database, extraction of table and column metadata,
and closing the connection.
"""

import psycopg2
from typing import List, Dict, Any

from data_scribe.core.interfaces import BaseConnector
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class PostgresConnector(BaseConnector):
    """Connector for PostgreSQL databases.

    This class implements the BaseConnector interface to provide
    connectivity and schema extraction for PostgreSQL databases.
    """

    def __init__(self):
        """Initializes the PostgresConnector, setting connection and cursor to None."""
        self.connection: psycopg2.Connection | None = None
        self.cursor: psycopg2.Cursor | None = None
        self.schema_name: str = "public"

    def connect(self, db_params: Dict[str, Any]):
        """Connects to the PostgreSQL database using the provided parameters.

        Args:
            db_params: A dictionary containing connection parameters like
                       host, port, user, password, and dbname.

        Raises:
            ConnectionError: If the connection to the database fails.
        """
        logger.info(
            f"Connecting to PostgreSQL database with params: {db_params}"
        )
        try:
            self.schema_name = db_params.get("schema", "public")

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
        """Retrieves a list of all table names in the 'public' schema.

        Returns:
            A list of strings, where each string is a table name.

        Raises:
            RuntimeError: If the database connection has not been established.
        """
        if not self.cursor:
            logger.error(
                "get_tables called before establishing a database connection."
            )
            raise RuntimeError(
                "Database connection not established. Call connect() first."
            )

        logger.info("Fetching table names from the 'public' schema.")
        self.cursor.execute(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_type = 'BASE TABLE';
        """,
            (self.schema_name,),
        )
        tables = [table[0] for table in self.cursor.fetchall()]
        logger.info(f"Found {len(tables)} tables: {tables}")
        return tables

    def get_columns(self, table_name: str) -> List[Dict[str, str]]:
        """Retrieves column information (name and type) for a given table in the 'public' schema.

        Args:
            table_name: The name of the table to inspect.

        Returns:
            A list of dictionaries, where each dictionary represents a column
            and contains its name and data type.

        Raises:
            RuntimeError: If the database connection has not been established.
        """
        if not self.cursor:
            logger.error(
                f"get_columns called for table '{table_name}' before establishing a database connection."
            )
            raise RuntimeError(
                "Database connection not established. Call connect() first."
            )

        logger.info(
            f"Fetching columns for table: {self.schema_name}.{table_name}"
        )
        self.cursor.execute(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s;
        """,
            (self.schema_name, table_name),
        )
        columns = [
            {"name": col[0], "type": col[1]} for col in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(columns)} columns in table '{table_name}'.")
        return columns

    def get_views(self) -> List[Dict[str, str]]:
        """Retrieves a list of all views and their SQL definitions."""
        if not self.cursor:
            raise RuntimeError(
                "Database connection not established. Call connect() first."
            )

        logger.info(f"Fetching views from schema: {self.schema_name}")
        self.cursor.execute(
            """
            SELECT table_name, view_definition 
            FROM information_schema.views 
            WHERE table_schema = %s;
        """,
            (self.schema_name,),
        )

        views = [
            {"name": view[0], "definition": view[1]}
            for view in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(views)} views.")
        return views

    def get_foreign_keys(self) -> List[Dict[str, str]]:
        """Retrieves all foreign key relationships in the schema."""
        if not self.cursor:
            raise RuntimeError(
                "Database connection not established. Call connect() first."
            )

        logger.info(
            f"Fetching foreign key relationships for schema: {self.schema_name}"
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

    def close(self):
        """Closes the database cursor and connection if they are open."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None
        logger.info("PostgreSQL database connection closed.")
