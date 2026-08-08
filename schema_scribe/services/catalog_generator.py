"""
This module provides the `CatalogGenerator`, the core engine for transforming
raw database schema information into an enriched, human-readable data catalog.

It uses a database connector to fetch metadata and an LLM client to generate
descriptive content, effectively turning technical details into valuable
business-level documentation.
"""

from typing import List, Dict, Any, Optional

from schema_scribe.core.interfaces import BaseConnector, BaseLLMClient
from schema_scribe.prompts import (
    COLUMN_DESCRIPTION_PROMPT,
    VIEW_SUMMARY_PROMPT,
    TABLE_SUMMARY_PROMPT,
)
from schema_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class CatalogGenerator:
    """
    Orchestrates the generation of a data catalog from a live database schema.

    This class is the heart of the `db` workflow. It manages a multi-step
    process:
    1.  Connects to a database via a `BaseConnector`.
    2.  Inspects its tables, columns, and views.
    3.  Profiles the data in each column to gather statistics.
    4.  Uses a `BaseLLMClient` to generate business-friendly summaries and
        descriptions for these assets by creating tailored prompts.
    5.  Assembles the final, enriched catalog dictionary.
    """

    def __init__(
        self,
        db_connector: BaseConnector,
        llm_client: BaseLLMClient,
        provider_name: Optional[str] = None,
    ):
        """
        Initializes the CatalogGenerator.

        Args:
            db_connector: An initialized connector for the target database.
            llm_client: An initialized client for the desired LLM provider.
            provider_name: The name of the LLM provider, disclosed in the
                run-start transmission log line.
        """
        self.db_connector = db_connector
        self.llm_client = llm_client
        self.provider_name = provider_name

    def _format_profile_stats(self, profile_stats: Dict[str, Any]) -> str:
        """
        Formats column profiling statistics into a string for an LLM prompt.

        This creates a concise, readable summary of a column's data profile
        to give the LLM better context for generating a description.

        Example Output:
        ```
        - Null Ratio: 0.0 (0.0 = no nulls)
        - Is Unique: True
        - Distinct Count: 150
        ```

        Args:
            profile_stats: A dictionary of statistics from `get_column_profile`.

        Returns:
            A formatted string summarizing the column's profile.
        """
        null_ratio = profile_stats.get("null_ratio")
        is_unique = profile_stats.get("is_unique")
        distinct_count = profile_stats.get("distinct_count")
        context_lines = [
            f"- Null Ratio: {null_ratio if null_ratio is not None else 'N/A'} (0.0 = no nulls)",
            f"- Is Unique: {is_unique if is_unique is not None else 'N/A'}",
            f"- Distinct Count: {distinct_count if distinct_count is not None else 'N/A'}",
        ]
        return "\n".join(context_lines)

    def generate_catalog(
        self, db_profile_name: str
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Generates a complete, enriched data catalog for the connected database.

        This method executes the main logic in these stages:
        1.  **Collect Metadata**: Fetches all tables, their columns, and all
            views up front, with no LLM calls.
        2.  **Disclose**: Logs the payload counts and provider before the
            first LLM call.
        3.  **Process Tables**: For each table, generates an AI summary and
            then, per column, gathers profile stats and generates an AI
            description.
        4.  **Process Views**: Generates an AI summary for each view based
            on its name and SQL definition.
        5.  **Process Foreign Keys**: Fetches all foreign key relationships
            to provide lineage information.

        Args:
            db_profile_name: The name of the database profile being scanned,
                             used for logging and context.

        Returns:
            A dictionary representing the complete data catalog.
            The structure is as follows:
            ```
            {
                "tables": [
                    {
                        "name": "table_name",
                        "ai_summary": "AI-generated summary...",
                        "columns": [
                            {
                                "name": "column_name",
                                "type": "data_type",
                                "description": "AI-generated description...",
                                "profile_stats": { ... },
                                "is_pk": true
                            },
                            ...
                        ]
                    },
                    ...
                ],
                "views": [ { ... } ],
                "foreign_keys": [ { ... } ]
            }
            ```
        """
        catalog_data = {"tables": [], "views": [], "foreign_keys": []}
        logger.info(f"Fetching tables for database profile: {db_profile_name}")

        # --- 1. Collect all metadata (no LLM calls yet) ---
        tables = self.db_connector.get_tables()
        logger.info(f"Found {len(tables)} tables: {tables}")

        tables_with_columns: List[tuple] = []
        total_columns = 0
        for table_name in tables:
            columns = self.db_connector.get_columns(table_name)
            tables_with_columns.append((table_name, columns))
            total_columns += len(columns)

        logger.info("Fetching views...")
        views = self.db_connector.get_views()

        # FKs are fetched here (before the LLM loop) so column prompts can
        # carry relationship context — eval lever 2, 2026-08-09. This only
        # reorders an existing call; the catalog's foreign_keys output is
        # unchanged.
        logger.info("Fetching foreign keys...")
        foreign_keys = self.db_connector.get_foreign_keys()
        fk_lookup = {
            (fk["source_table"], fk["source_column"]): (
                fk["target_table"],
                fk["target_column"],
            )
            for fk in foreign_keys
        }

        # --- 2. Disclose the payload before the first LLM call ---
        provider = self.provider_name or "unknown"
        logger.info(
            f"Sending {len(tables)} table summaries and {total_columns} "
            f"column descriptions to provider '{provider}'. Metadata: "
            f"tables={len(tables)}, columns={total_columns}, views={len(views)}."
        )

        # --- 3. Process Tables and Columns ---
        for table_name, columns in tables_with_columns:
            logger.info(f"Processing table: {table_name}")
            enriched_columns = []

            logger.info(f"  - Generating summary for table: {table_name}")
            column_list_str = ", ".join([c["name"] for c in columns])

            table_prompt = TABLE_SUMMARY_PROMPT.format(
                table_name=table_name, column_list_str=column_list_str
            )
            table_summary = self.llm_client.get_description(
                table_prompt, max_tokens=512
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
                # Sibling columns (already fetched) give the model table-level
                # context at zero cost (eval lever 1, 2026-08-08).
                sibling_columns = ", ".join(
                    c["name"] for c in columns if c["name"] != col_name
                )
                fk_target = fk_lookup.get((table_name, col_name))
                relationship_context = ""
                if fk_target:
                    relationship_context = (
                        f"Relationship: {col_name} is a foreign key to "
                        f"{fk_target[0]}.{fk_target[1]}."
                    )
                prompt = COLUMN_DESCRIPTION_PROMPT.format(
                    table_name=table_name,
                    col_name=col_name,
                    col_type=col_type,
                    profile_context=profile_context,
                    sibling_columns=sibling_columns,
                    relationship_context=relationship_context,
                )

                # 512 (not 200): reasoning models (e.g. gemma-4-26b) emit a
                # verbose thought part before the answer; 200 left only the
                # thought and no answer text (verified live 2026-08-07).
                description = self.llm_client.get_description(
                    prompt, max_tokens=1024
                )

                enriched_columns.append(
                    {
                        "name": col_name,
                        "type": col_type,
                        "description": description,
                        "profile_stats": profile_stats,
                        "is_pk": column.get("is_pk", False),
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

        # --- 4. Process Views ---
        enriched_views = []

        for view in views:
            view_name = view["name"]
            view_sql = view["definition"]
            logger.info(f"  - Generating summary for view: {view_name}")

            # Format the prompt with view details.
            prompt = VIEW_SUMMARY_PROMPT.format(
                view_name=view_name, view_definition=view_sql
            )
            summary = self.llm_client.get_description(prompt, max_tokens=512)

            enriched_views.append(
                {
                    "name": view_name,
                    "definition": view_sql,
                    "ai_summary": summary,
                }
            )
        catalog_data["views"] = enriched_views

        catalog_data["foreign_keys"] = foreign_keys

        logger.info("Catalog generation completed.")
        return catalog_data
