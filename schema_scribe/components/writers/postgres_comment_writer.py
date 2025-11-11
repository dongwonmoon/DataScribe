"""
This module provides `PostgresCommentWriter`, a `BaseWriter` implementation
that pushes generated data catalog descriptions back into a PostgreSQL database
as native database comments.

Design Rationale:
The `PostgresCommentWriter` offers a unique approach to data documentation by
integrating descriptions directly into the database's metadata. This has several
advantages:
- **Discoverability**: Comments are visible directly within database tools (e.g.,
  `psql`, DBeaver, DataGrip), making documentation easily accessible to data users.
- **Consistency**: Documentation lives alongside the schema, reducing the risk
  of drift between code and documentation.
- **Integration**: It leverages PostgreSQL's native `COMMENT ON` SQL statements,
  ensuring compatibility and robustness.
The entire operation is performed within a single database transaction to
guarantee atomicity.
"""

from typing import Dict, Any

from schema_scribe.utils.logger import get_logger
from schema_scribe.core.interfaces import BaseWriter, BaseConnector
from schema_scribe.core.exceptions import (
    WriterError,
    ConfigError,
    ConnectorError,
)
from schema_scribe.components.db_connectors import PostgresConnector

# Initialize a logger for this module
logger = get_logger(__name__)


class PostgresCommentWriter(BaseWriter):
    """
    Implements `BaseWriter` to write the catalog back to a PostgreSQL database
    using `COMMENT ON` SQL statements.

    This specialized writer requires an active `PostgresConnector` instance to
    execute SQL commands that update the metadata for tables, views, and columns
    directly in the database. The entire operation is performed within a single
    database transaction to ensure atomicity and data integrity.
    """

    def write(self, catalog_data: Dict[str, Any], **kwargs):
        """
        Writes catalog descriptions back to the PostgreSQL database as comments.

        This method orchestrates the process:
        1.  Validates that a connected `PostgresConnector` is provided via `kwargs`.
        2.  Iterates through views and tables in the `catalog_data`.
        3.  Constructs and executes `COMMENT ON` statements for each asset.
        4.  Commits the transaction upon success or rolls back on failure.

        Args:
            catalog_data: The dictionary containing the structured data catalog.
            **kwargs: Must contain `db_connector`, which must be an initialized
                      and connected instance of `PostgresConnector`.

        Raises:
            ConfigError: If `db_connector` is missing or is not a `PostgresConnector`.
            ConnectorError: If the provided `db_connector` is not connected.
            WriterError: If an error occurs during the database transaction.
        """
        logger.info("Starting to write comments back to PostgreSQL database...")

        db_connector: BaseConnector = kwargs.get("db_connector")
        if not isinstance(db_connector, PostgresConnector):
            raise ConfigError(
                "PostgresCommentWriter requires a 'postgres' db_profile and its active connector."
            )
        if not db_connector.connection or not db_connector.cursor:
            raise ConnectorError(
                "The provided PostgresConnector is not connected."
            )

        cursor = db_connector.cursor
        schema_name = db_connector.schema_name

        try:
            # Process Views: Add comments to views
            for view in catalog_data.get("views", []):
                description = view.get("ai_summary", "").replace("'", "''")
                logger.info(
                    f"  - Writing comment for VIEW: '{schema_name}.{view['name']}'"
                )
                # Use parameterized query to prevent SQL injection and handle quotes
                query = (
                    f'COMMENT ON VIEW "{schema_name}"."{view["name"]}" IS %s;'
                )
                cursor.execute(query, (description,))

            # Process Tables and Columns
            for table in catalog_data.get("tables", []):
                table_name = table["name"]
                # Write table-level comment if available
                if table.get("ai_summary"):
                    table_desc = table["ai_summary"].replace("'", "''")
                    logger.info(
                        f"  - Writing comment for TABLE: '{schema_name}.{table_name}'"
                    )
                    # Use parameterized query for table comment
                    query = f'COMMENT ON TABLE "{schema_name}"."{table_name}" IS %s;'
                    cursor.execute(query, (table_desc,))

                # Write column-level comments
                for column in table.get("columns", []):
                    col_name = column["name"]
                    col_desc = column.get("description", "").replace("'", "''")
                    logger.info(
                        f"  - Writing comment for COLUMN: '{schema_name}.{table_name}.{col_name}'"
                    )
                    # Use parameterized query for column comment
                    query = f'COMMENT ON COLUMN "{schema_name}"."{table_name}"."{col_name}" IS %s;'
                    cursor.execute(query, (col_desc,))

            db_connector.connection.commit()
            logger.info("Successfully wrote all comments to PostgreSQL.")

        except Exception as e:
            logger.error(
                f"Error writing comments to PostgreSQL: {e}", exc_info=True
            )
            db_connector.connection.rollback()
            raise WriterError(
                f"Error writing comments to PostgreSQL: {e}"
            ) from e
