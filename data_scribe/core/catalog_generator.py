"""
This module contains the core logic for generating a data catalog from a database.

It defines the CatalogGenerator class, which orchestrates the process of connecting to a database,
fetching schema information, and using an LLM to generate descriptions for each column.
"""

from typing import List, Dict, Any

from data_scribe.core.interfaces import BaseConnector, BaseLLMClient
from data_scribe.prompts import COLUMN_DESCRIPTION_PROMPT, VIEW_SUMMARY_PROMPT
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class CatalogGenerator:
    """
    Handles the core logic of generating a data catalog from a database.

    This class orchestrates the process of scanning a database schema,
    fetching table and column information, and using an LLM to generate
    business-friendly descriptions for each column.
    """

    def __init__(self, db_connector: BaseConnector, llm_client: BaseLLMClient):
        """
        Initializes the CatalogGenerator with a database connector and an LLM client.

        Args:
            db_connector: An instance of a class that implements the BaseConnector interface.
            llm_client: An instance of a class that implements the BaseLLMClient interface.
        """
        self.db_connector = db_connector
        self.llm_client = llm_client

    def generate_catalog(
        self, db_profile_name: str
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Generates a data catalog by fetching schema information from the database
        and enriching it with descriptions from an LLM.

        Args:
            db_profile_name: The name of the database profile being used, for logging purposes.

        Returns:
            A dictionary representing the data catalog. The keys are table names,
            and the values are lists of dictionaries, where each dictionary
            represents a column with its name, type, and AI-generated description.
        """
        catalog_data = {"tables": [], "views": [], "foreign_keys": []}
        logger.info(f"Fetching tables for database profile: {db_profile_name}")
        # Retrieve the list of table names from the database
        tables = self.db_connector.get_tables()
        logger.info(f"Found {len(tables)} tables: {tables}")

        # Iterate over each table to process its columns
        for table_name in tables:
            logger.info(f"Processing table: {table_name}")
            # Get the list of columns for the current table
            columns = self.db_connector.get_columns(table_name)
            enriched_columns = []

            # For each column, generate a description using the LLM
            for column in columns:
                col_name = column["name"]
                col_type = column["type"]
                logger.info(
                    f"  - Generating description for column: {col_name} ({col_type})"
                )

                # Format the prompt with table and column details
                prompt = COLUMN_DESCRIPTION_PROMPT.format(
                    table_name=table_name, col_name=col_name, col_type=col_type
                )

                # Get the column description from the LLM client
                description = self.llm_client.get_description(
                    prompt, max_tokens=50
                )

                # Append the enriched column data to the list
                enriched_columns.append(
                    {
                        "name": col_name,
                        "type": col_type,
                        "description": description,
                    }
                )
            # Add the table and its enriched columns to the catalog
            catalog_data["tables"].append(
                {"name": table_name, "columns": enriched_columns}
            )
            logger.info(f"Finished processing table: {table_name}")

        logger.info("Fetching views...")
        views = self.db_connector.get_views()
        enriched_views = []

        for view in views:
            view_name = view["name"]
            view_sql = view["definition"]
            logger.info(f"  - Generating description for view: {view_name}")

            prompt = VIEW_SUMMARY_PROMPT.format(
                view_name=view_name, view_definition=view_sql
            )
            summary = self.llm_client.get_description(prompt, max_tokens=200)

            enriched_views.append(
                {
                    "name": view_name,
                    "definition": view_sql,
                    "ai_summary": summary,
                }
            )
        catalog_data["views"] = enriched_views

        logger.info("Fetching foreign keys...")
        foreign_keys = self.db_connector.get_foreign_keys()
        catalog_data["foreign_keys"] = foreign_keys

        logger.info("Catalog generation completed.")

        return catalog_data
