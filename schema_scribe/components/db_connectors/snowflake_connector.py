"""
This module provides a concrete implementation of the `SqlBaseConnector` for
Snowflake data warehouses.

Design Rationale:
While Snowflake supports an `information_schema`, its implementation has quirks
that make the generic queries in `SqlBaseConnector` unreliable. Specifically:
- The `information_schema` is a database-level object, not a schema-level one,
  requiring queries to be fully qualified (e.g., `"my_db".information_schema.tables`).
- Snowflake-specific `SHOW` commands (e.g., `SHOW IMPORTED KEYS`) are often more
  reliable and performant for metadata extraction.

For these reasons, this connector inherits from `SqlBaseConnector` to maintain a
common interface but overrides most of its methods to use a more robust,
Snowflake-specific implementation.
"""

import snowflake.connector
from typing import List, Dict, Any

from .sql_base_connector import SqlBaseConnector
from schema_scribe.core.exceptions import ConnectorError
from schema_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class SnowflakeConnector(SqlBaseConnector):
    """
    A concrete connector for Snowflake data warehouses.

    This class extends `SqlBaseConnector` but overrides most of its metadata
    methods to handle Snowflake's specific SQL dialect and information schema
    structure.
    """

    def __init__(self):
        """Initializes the SnowflakeConnector by calling the parent constructor."""
        super().__init__()

    def connect(self, db_params: Dict[str, Any]):
        """
        Connects to a Snowflake account using the provided parameters.

        This method fulfills the contract required by `SqlBaseConnector` by
        setting the `self.connection`, `self.cursor`, `self.dbname`, and
        `self.schema_name` attributes upon a successful connection.

        Args:
            db_params: A dictionary of connection parameters. Expected keys
                       include `user`, `password`, `account`, `warehouse`,
                       `database`, and `schema`.

        Raises:
            ValueError: If a required parameter like 'database' is missing.
            ConnectorError: If the database connection fails.
        """
        logger.info("Connecting to Snowflake...")
        try:
            self.dbname = db_params.get("database")
            self.schema_name = db_params.get("schema", "public")

            if not self.dbname:
                raise ValueError(
                    "'database' parameter is required for Snowflake."
                )

            self.connection = snowflake.connector.connect(
                user=db_params.get("user"),
                password=db_params.get("password"),
                account=db_params.get("account"),
                warehouse=db_params.get("warehouse"),
                database=self.dbname,
                schema=self.schema_name,
            )
            self.cursor = self.connection.cursor()
            logger.info(
                f"Successfully connected to Snowflake DB '{self.dbname}'."
            )
        except Exception as e:
            logger.error(f"Snowflake connection failed: {e}", exc_info=True)
            raise ConnectorError(f"Snowflake connection failed: {e}") from e

    def get_tables(self) -> List[str]:
        """
        Retrieves a list of all table names in the configured schema.

        Overrides the base implementation to query Snowflake's database-specific
        `information_schema`.
        """
        if not self.cursor or not self.dbname:
            raise ConnectorError("Must connect to the DB first.")

        # Snowflake's information_schema is at the database level.
        query = f"""
            SELECT table_name
            FROM "{self.dbname}".information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE';
        """
        self.cursor.execute(query, (self.schema_name,))
        tables = [row[0] for row in self.cursor.fetchall()]
        logger.info(f"Found {len(tables)} tables.")
        return tables

    def get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Retrieves column metadata for the specified table.

        This method first uses `SHOW PRIMARY KEYS` to reliably identify primary
        key columns, then queries the `information_schema` for general column
        metadata. This two-step process is more robust than a single query.
        """
        if not self.cursor or not self.dbname:
            raise ConnectorError("Must connect to the DB first.")

        # Snowflake requires `SHOW PRIMARY KEYS` to reliably get PK info.
        self.cursor.execute(f'SHOW PRIMARY KEYS IN TABLE "{table_name}"')
        pk_columns = {row[4] for row in self.cursor.fetchall()}

        # Now fetch all column info from the database-level information_schema.
        query = f"""
            SELECT column_name, data_type, is_nullable
            FROM "{self.dbname}".information_schema.columns
            WHERE table_schema = %s AND table_name = %s;
        """
        self.cursor.execute(query, (self.schema_name, table_name))
        columns = [
            {
                "name": row[0],
                "type": row[1],
                "description": "",
                "is_nullable": row[2] == "YES",
                "is_pk": row[0] in pk_columns,
            }
            for row in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(columns)} columns in table '{table_name}'.")
        return columns

    def get_views(self) -> List[Dict[str, str]]:
        """
        Retrieves a list of all views and their SQL definitions from the schema.

        Overrides the base implementation to query Snowflake's database-specific
        `information_schema`.
        """
        if not self.cursor or not self.dbname:
            raise ConnectorError("Must connect to the DB first.")

        # Snowflake's information_schema is at the database level.
        query = f"""
            SELECT table_name, view_definition
            FROM "{self.dbname}".information_schema.views
            WHERE table_schema = %s;
        """
        self.cursor.execute(query, (self.schema_name,))
        views = [
            {"name": row[0], "definition": row[1]}
            for row in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(views)} views.")
        return views

    def get_foreign_keys(self) -> List[Dict[str, str]]:
        """
        Retrieves all foreign key relationships using `SHOW IMPORTED KEYS`.

        This method overrides the base `information_schema` implementation because
        the Snowflake-specific `SHOW IMPORTED KEYS` command is more reliable and
        directly provides the necessary information.
        """
        if not self.cursor or not self.dbname or not self.schema_name:
            raise ConnectorError("Must connect to the DB first.")

        logger.info("Fetching foreign key relationships from Snowflake...")
        self.cursor.execute(f'USE SCHEMA "{self.dbname}"."{self.schema_name}"')
        self.cursor.execute("SHOW IMPORTED KEYS;")

        # Row format from SHOW IMPORTED KEYS:
        # created_on, pk_database_name, pk_schema_name, pk_table_name, pk_column_name,
        # fk_database_name, fk_schema_name, fk_table_name, fk_column_name, ...
        foreign_keys = [
            {
                "source_table": row[7],
                "source_column": row[8],
                "target_table": row[3],
                "target_column": row[4],
            }
            for row in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(foreign_keys)} foreign key relationships.")
        return foreign_keys
