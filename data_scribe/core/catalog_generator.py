"""
This module provides the core engine for generating data catalogs.

The `CatalogGenerator` class uses a database connector to fetch schema metadata
(tables, columns, views) and an LLM client to generate descriptive content,
creating a comprehensive, enriched data catalog.
"""

from typing import List, Dict, Any

from data_scribe.core.interfaces import BaseConnector, BaseLLMClient
from data_scribe.prompts import (
    COLUMN_DESCRIPTION_PROMPT,
    VIEW_SUMMARY_PROMPT,
    TABLE_SUMMARY_PROMPT,
)
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class CatalogGenerator:
    """
    Orchestrates the generation of a data catalog from a database schema.

    This class connects to a database via a `BaseConnector`, inspects its tables,
    columns, and views, profiles the data, and then uses a `BaseLLMClient` to
    generate business-friendly summaries and descriptions for these assets.
    """

    def __init__(self, db_connector: BaseConnector, llm_client: BaseLLMClient):
        """
        Initializes the CatalogGenerator.

        Args:
            db_connector: An initialized connector for the target database.
            llm_client: An initialized client for the desired LLM provider.
        """
        self.db_connector = db_connector
        self.llm_client = llm_client

    def _format_profile_stats(self, profile_stats: Dict[str, Any]) -> str:
        """
        Formats column profiling statistics into a string for an LLM prompt.

        This creates a concise, readable summary of a column's data profile
        to give the LLM better context for generating a description.

        Args:
            profile_stats: A dictionary of statistics from `get_column_profile`.

        Returns:
            A formatted string summarizing the column's profile.
        """
        context_lines = [
            f"- Null Ratio: {profile_stats.get('null_ratio', 'N/A')} (0.0 = no nulls)",
            f"- Is Unique: {profile_stats.get('is_unique', 'N/A')}",
            f"- Distinct Count: {profile_stats.get('distinct_count', 'N/A')}",
        ]
        return "\n".join(context_lines)

    def generate_catalog(self, db_profile_name: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Generates a complete data catalog for the connected database.

        This method fetches all tables, views, and foreign keys, then iterates
        through them to generate AI-powered summaries and descriptions.

        Args:
            db_profile_name: The name of the database profile being scanned,
                             used for logging and context.

        Returns:
            A dictionary representing the complete, enriched data catalog.
            The structure is as follows:
            ```
            {
                "tables": [
                    {
                        "name": "table_name",
                        "ai_summary": "AI-generated summary of the table.",
                        "columns": [
                            {
                                "name": "column_name",
                                "type": "data_type",
                                "description": "AI-generated column description.",
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
                        "ai_summary": "AI-generated summary of the view."
                    },
                    ...
                ],
                "foreign_keys": [ { ... } ]
            }
            ```
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

            logger.info(f"  - Generating summary for table: {table_name}")
            column_list_str = ", ".join([c["name"] for c in columns])

            table_prompt = TABLE_SUMMARY_PROMPT.format(
                table_name=table_name, column_list_str=column_list_str
            )
            table_summary = self.llm_client.get_description(
                table_prompt, max_tokens=200
            )

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
                {
                    "name": table_name,
                    "ai_summary": table_summary,
                    "columns": enriched_columns,
                }
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
