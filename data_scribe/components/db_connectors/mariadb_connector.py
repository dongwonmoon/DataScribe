import mysql.connector
from typing import List, Dict, Any

from data_scribe.core.interfaces import BaseConnector
from data_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class MariaDBConnector(BaseConnector):
    """Connector for MariaDB/MySQL databases."""

    def __init__(self):
        self.connection = None
        self.cursor = None
        self.dbname = None  # Stores dbname for schema queries

    def connect(self, db_params: Dict[str, Any]):
        """Connects to the MariaDB/MySQL database."""
        try:
            self.dbname = db_params.get("dbname")
            if not self.dbname:
                raise ValueError(
                    "'dbname' (database name) parameter is required."
                )

            self.connection = mysql.connector.connect(
                host=db_params.get("host", "localhost"),
                port=db_params.get("port", 3306),
                user=db_params.get("user"),
                password=db_params.get("password"),
                database=self.dbname,  # Use 'database' key
            )
            self.cursor = self.connection.cursor()
            logger.info(
                f"Successfully connected to MariaDB/MySQL DB '{self.dbname}'."
            )
        except mysql.connector.Error as e:
            logger.error(f"MariaDB/MySQL connection failed: {e}", exc_info=True)
            raise ConnectionError(
                f"MariaDB/MySQL connection failed: {e}"
            ) from e

    def get_tables(self) -> List[str]:
        if not self.cursor or not self.dbname:
            raise RuntimeError("Must connect to the DB first.")

        # Query tables using information_schema
        self.cursor.execute(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_type = 'BASE TABLE';
        """,
            (self.dbname,),
        )

        tables = [table[0] for table in self.cursor.fetchall()]
        logger.info(f"Found {len(tables)} tables in '{self.dbname}' schema.")
        return tables

    def get_columns(self, table_name: str) -> List[Dict[str, str]]:
        """Retrieves column information (name, type) for a specific table."""
        if not self.cursor or not self.dbname:
            raise RuntimeError("Must connect to the DB first.")

        self.cursor.execute(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s;
        """,
            (self.dbname, table_name),
        )

        columns = [
            {"name": col[0], "type": col[1]} for col in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(columns)} columns in table {table_name}.")
        return columns

    def get_views(self) -> List[Dict[str, str]]:
        """Retrieves a list of all views and their SQL definitions."""
        if not self.cursor:
            raise RuntimeError(
                "Database connection not established. Call connect() first."
            )

        logger.info(f"Fetching views from schema: {self.dbname}")
        self.cursor.execute(
            """
            SELECT table_name, view_definition 
            FROM information_schema.views 
            WHERE table_schema = %s;
        """,
            (self.dbname,),
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
            f"Fetching foreign key relationships for schema: {self.dbname}"
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

        self.cursor.execute(query, (self.dbname,))
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
        """Closes the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("MariaDB/MySQL connection closed.")
