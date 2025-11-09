"""
This module provides a writer for pushing generated data catalog descriptions
back into a PostgreSQL database as comments.

It implements the `BaseWriter` interface and uses `COMMENT ON` SQL statements
to update descriptions for tables, views, and columns directly within the database.
"""

from typing import Dict, Any

from data_scribe.utils.logger import get_logger
from data_scribe.core.interfaces import BaseWriter, BaseConnector
from data_scribe.core.exceptions import WriterError, ConfigError, ConnectorError
from data_scribe.components.db_connectors import PostgresConnector

# Initialize a logger for this module
logger = get_logger(__name__)


class PostgresCommentWriter(BaseWriter):
    """
    Handles writing the generated catalog back to a PostgreSQL database
    using `COMMENT ON` SQL statements.

    This writer updates descriptions for tables, views, and columns directly
    in the database's metadata. It requires an active `PostgresConnector` instance.
    """

    def write(self, catalog_data: Dict[str, Any], **kwargs):
        """
        Writes the catalog data (descriptions) back to the PostgreSQL database
        as table, view, and column comments.

        Args:
            catalog_data: The dictionary containing the structured data catalog.
            **kwargs: Additional writer-specific arguments. Expected to contain:
                      - `db_connector` (PostgresConnector): An initialized and
                        connected instance of `PostgresConnector`.

        Raises:
            ConfigError: If `db_connector` is missing from `kwargs` or is not
                         an instance of `PostgresConnector`.
            ConnectorError: If the provided `db_connector` is not connected.
            WriterError: If an error occurs during the process of writing comments
                         to the database.
        """
        logger.info("Starting to write comments back to PostgreSQL database...")

        # 1. Get the database connector from kwargs
        db_connector: BaseConnector = kwargs.get("db_connector")

        if not db_connector:
            logger.error(
                "PostgresCommentWriter 'write' method missing 'db_connector' in kwargs."
            )
            raise ConfigError("PostgresCommentWriter requires 'db_connector'.")

        if not isinstance(db_connector, PostgresConnector):
            logger.error(
                f"PostgresCommentWriter only works with PostgresConnector, got {type(db_connector)}"
            )
            raise ConfigError(
                "PostgresCommentWriter is only compatible with 'postgres' db_profile."
            )

        if not db_connector.connection or not db_connector.cursor:
            logger.error("The provided db_connector is not connected.")
            raise ConnectorError("db_connector is not connected.")

        cursor = db_connector.cursor
        schema_name = db_connector.schema_name

        try:
            # 2. Write View Comments
            for view in catalog_data.get("views", []):
                view_name = view["name"]
                description = view.get("ai_summary", "").replace(
                    "'", "''"
                )  # Basic SQL escaping

                logger.info(
                    f"  - Writing comment for VIEW: {schema_name}.{view_name}"
                )
                query = f'COMMENT ON VIEW "{schema_name}"."{view_name}" IS %s;'
                cursor.execute(query, (description,))

            # 3. Write Table and Column Comments
            for table in catalog_data.get("tables", []):
                table_name = table["name"]

                for column in table.get("columns", []):
                    col_name = column["name"]
                    description = column.get("description", "").replace(
                        "'", "''"
                    )  # Basic SQL escaping

                    logger.info(
                        f"  - Writing comment for COLUMN: {schema_name}.{table_name}.{col_name}"
                    )
                    query = f'COMMENT ON COLUMN "{schema_name}"."{table_name}"."{col_name}" IS %s;'
                    cursor.execute(query, (description,))

            # 4. Commit the changes to the database
            db_connector.connection.commit()
            logger.info("Successfully wrote all comments to PostgreSQL.")

        except Exception as e:
            logger.error(
                f"Error writing comments to PostgreSQL: {e}", exc_info=True
            )
            db_connector.connection.rollback()  # Rollback on failure
            raise WriterError(
                f"Error writing comments to PostgreSQL: {e}"
            ) from e
