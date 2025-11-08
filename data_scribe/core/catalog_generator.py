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

    def _format_profile_stats(self, profile_stats: Dict[str, Any]) -> str:
        """
        Helper function to format column profiling stats into a string for the LLM prompt.
        This provides the LLM with context about the column's data distribution.
        """
        context_lines = [
            f"- Null Ratio: {profile_stats.get('null_ratio', 'N/A')} (0.0 = no nulls)",
            f"- Is Unique: {profile_stats.get('is_unique', 'N/A')}",
            f"- Distinct Count: {profile_stats.get('distinct_count', 'N/A')}",
        ]
        return "\n".join(context_lines)

    def generate_catalog(self, db_profile_name: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Generates a data catalog by fetching schema information from the database
        and enriching it with descriptions from an LLM.

        Args:
            db_profile_name: The name of the database profile being used, for logging purposes.

        Returns:
            A dictionary representing the data catalog with the following structure:
            {
                "tables": [
                    {
                        "name": "table_name",
                        "columns": [
                            {
                                "name": "col_name",
                                "type": "col_type",
                                "description": "AI-generated description.",
                                "profile_stats": { ... }
                            },
                            ...
                        ]
                    },
                    ...
                ],
                "views": [
                    {
                        "name": "view_name",
                        "definition": "CREATE VIEW ...",
                        "ai_summary": "AI-generated summary."
                    },
                    ...
                ],
                "foreign_keys": [ { ... } ]
            }
        """
        catalog_data = {"tables": [], "views": [], "foreign_keys": []}
        logger.info(f"Fetching tables for database profile: {db_profile_name}")

        # --- 1. Process Tables and Columns ---
        tables = self.db_connector.get_tables()
        logger.info(f"Found {len(tables)} tables: {tables}")

        for table_name in tables:
            logger.info(f"Processing table: {table_name}")
            columns = self.db_connector.get_columns(table_name)
            enriched_columns = []

            # For each column, profile it and generate a description using the LLM
            for column in columns:
                col_name = column["name"]
                col_type = column["type"]

                # Profile the column to get statistics for better context.
                logger.info(f"  - Profiling column: {table_name}.{col_name}...")
                profile_stats = self.db_connector.get_column_profile(
                    table_name, col_name
                )
                profile_context = self._format_profile_stats(profile_stats)

                logger.info(
                    f"  - Generating description for column: {col_name} ({col_type})"
                )

                # Format the prompt with table, column, and profiling details.
                prompt = COLUMN_DESCRIPTION_PROMPT.format(
                    table_name=table_name,
                    col_name=col_name,
                    col_type=col_type,
                    profile_context=profile_context,
                )

                # Get the column description from the LLM client.
                description = self.llm_client.get_description(prompt, max_tokens=50)

                enriched_columns.append(
                    {
                        "name": col_name,
                        "type": col_type,
                        "description": description,
                        "profile_stats": profile_stats,
                    }
                )
            catalog_data["tables"].append(
                {"name": table_name, "columns": enriched_columns}
            )
            logger.info(f"Finished processing table: {table_name}")

        # --- 2. Process Views ---
        logger.info("Fetching views...")
        views = self.db_connector.get_views()
        enriched_views = []

        for view in views:
            view_name = view["name"]
            view_sql = view["definition"]
            logger.info(f"  - Generating summary for view: {view_name}")

            # Format the prompt with view details.
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

        # --- 3. Process Foreign Keys ---
        logger.info("Fetching foreign keys...")
        foreign_keys = self.db_connector.get_foreign_keys()
        catalog_data["foreign_keys"] = foreign_keys

        logger.info("Catalog generation completed.")
        return catalog_data
