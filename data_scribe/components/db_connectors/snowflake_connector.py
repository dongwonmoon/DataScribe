"""
This module provides a concrete implementation of the BaseConnector for Snowflake databases.

It uses the `snowflake-connector-python` library to handle the connection, schema extraction,
and other database interactions.
"""

import snowflake.connector
from typing import List, Dict, Any

from .sql_base_connector import SqlBaseConnector
from data_scribe.core.exceptions import ConnectorError
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class SnowflakeConnector(SqlBaseConnector):
    """Connector for Snowflake data warehouses.

    This class extends SqlBaseConnector and implements the `connect` method
    specific to Snowflake databases using the `snowflake-connector-python` library.
    It also overrides the `get_foreign_keys` method to use Snowflake's specific
    `SHOW IMPORTED KEYS` command.
    """

    def __init__(self):
        """Initializes the SnowflakeConnector."""
        super().__init__()

    def connect(self, db_params: Dict[str, Any]):
        """
        Connects to the Snowflake database using the provided parameters.

        Args:
            db_params: A dictionary of connection parameters, including 'user',
                       'password', 'account', 'warehouse', 'database', and 'schema'.

        Raises:
            ValueError: If the 'database' parameter is missing.
            ConnectionError: If the database connection fails.
        """
        try:
            self.dbname = db_params.get("database")
            self.schema_name = db_params.get("schema", "public")

            if not self.dbname:
                raise ValueError("'database' parameter is required.")

            self.connection = snowflake.connector.connect(
                user=db_params.get("user"),
                password=db_params.get("password"),
                account=db_params.get("account"),
                warehouse=db_params.get("warehouse"),
                database=self.dbname,
                schema=self.schema_name,
            )
            self.cursor = self.connection.cursor()
            logger.info(f"Successfully connected to Snowflake DB '{self.dbname}'.")
        except Exception as e:
            logger.error(f"Snowflake connection failed: {e}", exc_info=True)
            raise ConnectorError(f"Snowflake connection failed: {e}")

    def get_tables(self) -> List[str]:
        """
        Retrieves a list of all table names in the configured schema.

        Returns:
            A list of strings, where each string is a table name.
        """
        if not self.cursor:
            raise ConnectorError("Must connect to the DB first.")

        query = f"""
            SELECT table_name
            FROM "{self.dbname}".information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE';
        """
        self.cursor.execute(query, (self.schema_name,))
        tables = [table[0] for table in self.cursor.fetchall()]
        logger.info(f"Found {len(tables)} tables.")
        return tables

        if not self.cursor:
            raise ConnectorError("Must connect to the DB first.")

        query = f"""
            SELECT column_name, data_type
            FROM "{self.dbname}".information_schema.columns
            WHERE table_schema = %s AND table_name = %s;
        """
        self.cursor.execute(query, (self.schema_name, table_name))
        columns = [{"name": col[0], "type": col[1]} for col in self.cursor.fetchall()]
        logger.info(f"Found {len(columns)} columns in table {table_name}.")
        return columns

    def get_views(self) -> List[Dict[str, str]]:
        """
        Retrieves a list of all views and their SQL definitions from the schema.

        Returns:
            A list of dictionaries, each representing a view with its name and definition.
        if not self.cursor:
            raise ConnectorError("Must connect to the DB first.")

        query = f"""
            SELECT table_name, view_definition
            FROM "{self.dbname}".information_schema.views
            WHERE table_schema = %s;
        """
        self.cursor.execute(query, (self.schema_name,))
        views = [
            {"name": view[0], "definition": view[1]} for view in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(views)} views.")
        return views

    def get_foreign_keys(self) -> List[Dict[str, str]]:
        """Retrieves all foreign key relationships in the schema."""
        if not self.cursor:
            raise ConnectorError("Must connect to the DB first.")

        logger.info("Fetching foreign key relationships from Snowflake...")
        self.cursor.execute(f'USE SCHEMA "{self.dbname}"."{self.schema_name}"')
        self.cursor.execute("SHOW IMPORTED KEYS;")

        foreign_keys = []
        for fk in self.cursor.fetchall():
            foreign_keys.append(
                {
                    "from_table": fk[6],
                    "from_column": fk[7],
                    "to_table": fk[2],
                    "to_column": fk[3],
                }
            )
        logger.info(f"Found {len(foreign_keys)} foreign key relationships.")
        return foreign_keys
