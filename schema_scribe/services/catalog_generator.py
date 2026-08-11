"""
This module provides the `CatalogGenerator`, the core engine for transforming
raw database schema information into an enriched, human-readable data catalog.

It uses a database connector to fetch metadata and an LLM client to generate
descriptive content, effectively turning technical details into valuable
business-level documentation.
"""

from typing import List, Dict, Any, Optional
import re

from schema_scribe.core.interfaces import BaseConnector, BaseLLMClient
from schema_scribe.prompts import (
    COLUMN_DESCRIPTION_PROMPT,
    TABLE_BATCH_PROMPT,
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

    @staticmethod
    def _parse_batch_response(
        response: str, column_count: int
    ) -> tuple[str, List[str]]:
        """Parses the batched LLM response into (summary, per-column list).

        Contract (TABLE_BATCH_PROMPT): a "SUMMARY:" line plus numbered
        "N: <description>" lines. "N." delimiters are also accepted (a
        model echoing the input's "1. name" style must not zero out the
        whole table — panel C6, 2026-08-11). Missing column lines and a
        missing SUMMARY line are logged as warnings — the failure is never
        silent (panel C5). Descriptions for missing lines fall back to
        empty strings (drafts); extra/out-of-range lines are ignored.
        Model-agnostic: no JSON schema dependency.
        """
        summary = ""
        by_index: Dict[int, str] = {}
        for line in response.splitlines():
            stripped = line.strip()
            if stripped.upper().startswith("SUMMARY:"):
                summary = stripped[len("SUMMARY:"):].strip()
                continue
            match = re.match(r"^(\d+)\s*[:.]\s*(.+)$", stripped)
            if match:
                idx = int(match.group(1))
                if 1 <= idx <= column_count:
                    by_index[idx] = match.group(2).strip()
        descriptions = [
            by_index.get(i, "") for i in range(1, column_count + 1)
        ]
        missing = [
            i for i in range(1, column_count + 1) if i not in by_index
        ]
        if missing:
            logger.warning(
                f"Batch response missing {len(missing)} column line(s): "
                f"{['#' + str(i) for i in missing]} — empty drafts."
            )
        if not summary:
            logger.warning(
                "Batch response missing SUMMARY line — empty table summary."
            )
        return summary, descriptions

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

        # --- 3. Process Tables and Columns (batched: one LLM call per table) ---
        for table_name, columns in tables_with_columns:
            logger.info(f"Processing table: {table_name}")
            enriched_columns = []

            # 3a. Profile every column first (DB calls unchanged; only the
            # LLM calls are batched — 13 -> 3 on the clean fixture, the
            # benchmark baseline, 2026-08-11).
            profiled = []
            for column in columns:
                col_name = column["name"]
                col_type = column["type"]
                logger.info(f"  - Profiling column: {table_name}.{col_name}...")
                profile_stats = self.db_connector.get_column_profile(
                    table_name, col_name
                )
                profile_context = self._format_profile_stats(profile_stats)
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
                profiled.append(
                    (
                        column,
                        profile_stats,
                        profile_context,
                        sibling_columns,
                        relationship_context,
                    )
                )

            # 3b. One batch LLM call: table summary + all column descriptions.
            logger.info(f"  - Generating summary+descriptions for table: {table_name}")
            column_entries = "\n".join(
                f"{i}. {col['name']} ({col['type']}) | {profile_context}"
                f" | siblings: {sibling_columns}"
                f" {relationship_context or '(not a foreign key)'}"
                for i, (col, _, profile_context, sibling_columns,
                        relationship_context) in enumerate(profiled, 1)
            )
            batch_prompt = TABLE_BATCH_PROMPT.format(
                table_name=table_name, column_entries=column_entries
            )
            batch_response = self.llm_client.get_description(
                batch_prompt, max_tokens=1024
            )
            table_summary, descriptions = self._parse_batch_response(
                batch_response, len(profiled)
            )

            for i, (column, profile_stats, _, _, _) in enumerate(profiled, 1):
                enriched_columns.append(
                    {
                        "name": column["name"],
                        "type": column["type"],
                        "description": descriptions[i - 1],
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
