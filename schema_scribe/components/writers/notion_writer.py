"""
This module provides an implementation of the `BaseWriter` for Notion.

It allows writing the generated data catalog (from either a database or a dbt project)
to a new page within a specified parent page in Notion. It handles connecting to
the Notion API, dynamically transforming different catalog data structures into
appropriate Notion blocks, and creating the page.
"""

import os
from typing import Dict, Any, List, Optional
from notion_client import Client, APIErrorCode, APIResponseError

from schema_scribe.core.interfaces import BaseWriter
from schema_scribe.core.exceptions import WriterError, ConfigError
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class NotionWriter(BaseWriter):
    """
    Implements `BaseWriter` to write a data catalog to a new Notion page.

    This writer connects to the Notion API and constructs a new page with the
    catalog content, including views, tables, dbt models, and lineage, formatted
    as Notion blocks. It dynamically adapts to both traditional database
    catalogs and dbt project catalogs.

    Attributes:
        notion (Optional[Client]): The initialized Notion client instance.
        params (Dict[str, Any]): The configuration parameters for the writer.
    """

    def __init__(self):
        """Initializes the NotionWriter."""
        self.notion: Optional[Client] = None
        self.params: Dict[str, Any] = {}
        logger.info("NotionWriter initialized")

    def _connect(self):
        """
        Initializes the connection to the Notion API using the provided token.

        It resolves the API token, which can be provided directly or as an
        environment variable reference (e.g., `${NOTION_API_KEY}`).

        Raises:
            ConfigError: If the API token is missing or the referenced
                         environment variable is not set.
            ConnectionError: If the Notion client fails to initialize.
        """
        token = self.params.get("api_token")

        # Resolve token if it's an environment variable reference
        if token and token.startswith("${basedir}") and token.endswith("}"):
            env_var = token[2:-1]
            token = os.getenv(env_var)
            if not token:
                raise ConfigError(
                    f"The environment variable '{env_var}' is required but not set."
                )

        if not token:
            raise ConfigError(
                "'api_token' (or env var) is required for NotionWriter."
            )

        try:
            self.notion = Client(auth=token)
            logger.info("Successfully connected to Notion API.")

        except Exception as e:
            logger.error(f"Failed to connect to Notion: {e}", exc_info=True)
            raise ConnectionError(f"Failed to connect to Notion: {e}")

    def write(self, catalog_data: Dict[str, Any], **kwargs):
        """
        Writes the catalog data to a new Notion page.

        This is the main entry point that orchestrates the connection, block
        generation, and page creation process. It dynamically detects the
        structure of `catalog_data` (DB or dbt) and formats the Notion page accordingly.

        Args:
            catalog_data: The structured data catalog to be written. This can be
                          a DB catalog (containing 'tables', 'views', 'foreign_keys')
                          or a dbt catalog (containing model names as top-level keys).
            **kwargs: Configuration parameters for the writer. Must include:
                      - `api_token` (str): The Notion API integration token.
                      - `parent_page_id` (str): The ID of the parent page under
                        which the new catalog page will be created.
                      - `project_name` (str, optional): The name of the project,
                        used in the page title.

        Raises:
            ConfigError: If required configuration is missing.
            WriterError: If there's an error generating blocks or creating the page.
        """
        self.params = kwargs
        self._connect()

        parent_page_id = self.params.get("parent_page_id")
        if not parent_page_id:
            raise ConfigError("'parent_page_id' is required for NotionWriter.")

        project_name = kwargs.get("project_name", "Data Catalog")
        page_title = f"Data Catalog - {project_name}"

        # 1. Generate a list of Notion blocks from the catalog data.
        try:
            blocks = self._generate_notion_blocks(catalog_data)
        except Exception as e:
            logger.error(
                f"Failed to generate Notion blocks: {e}", exc_info=True
            )
            raise WriterError(f"Failed to generate Notion blocks: {e}")

        # 2. Create the new page in Notion with the generated blocks.
        try:
            logger.info(f"Creating new Notion page: '{page_title}'")

            new_page_props = {
                "title": [{"type": "text", "text": {"content": page_title}}]
            }
            parent_data = {"page_id": parent_page_id}

            page = self.notion.pages.create(
                parent=parent_data,
                properties=new_page_props,
                children=blocks,
            )
            logger.info(f"Successfully created Notion page: {page.get('url')}")

        except APIResponseError as e:
            logger.error(f"Failed to create Notion page: {e}", exc_info=True)
            raise WriterError(
                f"Failed to create Notion page. Check API key and Page ID permissions: {e}"
            )
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)
            raise WriterError(f"An unexpected error occurred: {e}")

    def _text_cell(self, content: str) -> Dict[str, Any]:
        """
        Creates a Notion table cell with plain text content.

        Args:
            content: The text content for the cell.

        Returns:
            A dictionary representing a Notion table cell.
        """
        return [{"type": "text", "text": {"content": content or ""}}]

    def _H2(self, text: str) -> Dict[str, Any]:
        """
        Creates a Notion Heading 2 block.

        Args:
            text: The text content for the heading.

        Returns:
            A dictionary representing a Notion Heading 2 block.
        """
        return {
            "object": "block",
            "type": "heading_2",
            "heading_2": {"rich_text": [{"text": {"content": text}}]},
        }

    def _H3(self, text: str) -> Dict[str, Any]:
        """
        Creates a Notion Heading 3 block.

        Args:
            text: The text content for the heading.

        Returns:
            A dictionary representing a Notion Heading 3 block.
        """
        return {
            "object": "block",
            "type": "heading_3",
            "heading_3": {"rich_text": [{"text": {"content": text}}]},
        }

    def _Para(self, text: str) -> Dict[str, Any]:
        """
        Creates a Notion Paragraph block.

        Args:
            text: The text content for the paragraph.

        Returns:
            A dictionary representing a Notion Paragraph block.
        """
        return {
            "object": "block",
            "type": "paragraph",
            "paragraph": {"rich_text": [{"text": {"content": text}}]},
        }

    def _Code(self, text: str, lang: str = "sql") -> Dict[str, Any]:
        """
        Creates a Notion Code block.

        Args:
            text: The code content.
            lang: The language for syntax highlighting (e.g., "sql", "python", "mermaid").

        Returns:
            A dictionary representing a Notion Code block.
        """
        return {
            "object": "block",
            "type": "code",
            "code": {
                "rich_text": [{"text": {"content": text}}],
                "language": lang,
            },
        }

    def _clean_mermaid_code(self, code: str) -> str:
        """
        Removes Mermaid code fences (```mermaid ... ```) if they exist in the string.

        Args:
            code: The Mermaid code string, potentially with fences.

        Returns:
            The cleaned Mermaid code string without fences.
        """
        return code.replace("```mermaid", "").replace("```", "").strip()

    def _generate_notion_blocks(
        self,
        catalog_data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Dynamically generates Notion blocks by detecting the catalog structure
        as either a 'db' workflow or a 'dbt' workflow.

        Args:
            catalog_data: The structured data catalog.

        Returns:
            A list of dictionaries, where each dictionary is a valid Notion block.
        """
        # 'db' catalog typically has 'tables', 'views', 'foreign_keys' as top-level keys.
        if "tables" in catalog_data and "views" in catalog_data:
            logger.info(
                "Detected 'db' catalog structure. Generating DB blocks."
            )
            return self._generate_db_blocks(catalog_data)
        # 'dbt' catalog typically has model names as top-level keys.
        elif any(
            isinstance(v, dict) and "columns" in v
            for v in catalog_data.values()
        ):
            logger.info(
                "Detected 'dbt' catalog structure. Generating dbt blocks."
            )
            return self._generate_dbt_blocks(catalog_data)
        else:
            logger.warning(
                "Unknown catalog structure. Generating basic blocks."
            )
            return [self._Para("Unknown catalog structure provided.")]

    def _create_column_table(
        self,
        columns: List[Dict[str, Any]],
        is_dbt: bool = False,
    ) -> Dict[str, Any]:
        """
        Creates a Notion Table block to display column details.

        This method dynamically constructs a Notion table with headers for
        'Column Name', 'Data Type', and 'AI-Generated Description'. It adapts
        to the structure of column data from either DB or dbt catalogs.

        Args:
            columns: A list of column dictionaries.
            is_dbt: A boolean flag indicating if the columns are from a dbt catalog.
                    If True, it expects column descriptions to be nested under 'ai_generated'.

        Returns:
            A dictionary representing a Notion Table block.
        """
        header = {
            "type": "table_row",
            "table_row": {
                "cells": [
                    self._text_cell("Column Name"),
                    self._text_cell("Data Type"),
                    self._text_cell("AI-Generated Description"),
                ]
            },
        }

        rows = [header]
        for col in columns:
            if is_dbt:
                # dbt column data has description nested in 'ai_generated'
                desc = col.get("ai_generated", {}).get("description", "(N/A)")
            else:
                # DB column data has description directly
                desc = col.get("description", "N/A")

            row = {
                "type": "table_row",
                "table_row": {
                    "cells": [
                        self._text_cell(col.get("name")),
                        self._text_cell(col.get("type")),
                        self._text_cell(desc),
                    ],
                },
            }
            rows.append(row)

        # Notion table block structure
        return {
            "object": "block",
            "type": "table",
            "table": {
                "table_width": 3,
                "has_column_header": True,
                "children": rows,
            },
        }

    def _generate_mermaid_erd(self, foreign_keys: List[Dict[str, str]]) -> str:
        """
        Generates Mermaid ERD (Entity Relationship Diagram) code from foreign key data.

        This creates a `graph TD` (Top-Down) Mermaid diagram representing the
        relationships between tables based on foreign key constraints.

        Args:
            foreign_keys: A list of dictionaries, where each dictionary represents
                          a foreign key relationship with keys like 'from_table',
                          'to_table', 'from_column', and 'to_column'.

        Returns:
            A string containing the Mermaid ERD code. Returns a placeholder
            if no foreign keys are found.
        """
        if not foreign_keys:
            return "graph TD;\n  A[No foreign key relationships found]"

        code = ["graph TD;"]  # Top-Down graph
        tables = set()
        for fk in foreign_keys:
            tables.add(fk["from_table"])
            tables.add(fk["to_table"])

        for table in tables:
            code.append(f"    {table}[{table}]")  # Define table nodes
        code.append("")

        for fk in foreign_keys:
            label = f"{fk['from_column']} → {fk['to_column']}"
            code.append(
                f'  {fk["from_table"]} --> {fk["to_table"]} : "{label}"'
            )

        return "\n".join(code)

    def _generate_db_blocks(
        self,
        catalog_data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Generates a list of Notion blocks for a traditional database catalog.

        This includes sections for ERD, Views, and Tables, with column details
        rendered in Notion table blocks.

        Args:
            catalog_data: The structured database catalog.

        Returns:
            A list of dictionaries, each representing a valid Notion block.
        """
        blocks = []

        # 1. ERD Section
        blocks.append(self._H2("🚀 Entity Relationship Diagram (ERD)"))
        mermaid_code = self._generate_mermaid_erd(
            catalog_data.get("foreign_keys", [])
        )
        blocks.append(self._Code(mermaid_code, "mermaid"))

        # 2. Views Section
        blocks.append(self._H2("🔎 Views"))
        views = catalog_data.get("views", [])
        if not views:
            blocks.append(self._Para("No views found in this database."))
        else:
            for view in views:
                blocks.append(self._H3(f"View: {view['name']}"))
                blocks.append(
                    self._Para(f"AI Summary: {view.get('ai_summary', 'N/A')}")
                )
                blocks.append(
                    self._Code(view.get("definition", "N/A"), lang="sql")
                )

        # 3. Tables Section
        blocks.append(self._H2("🗂️ Tables"))
        tables = catalog_data.get("tables", [])
        if not tables:
            blocks.append(self._Para("No tables found in this database."))
        else:
            for table in tables:
                blocks.append(self._H3(f"Table: {table['name']}"))
                # Add table summary if available
                if table.get("ai_summary"):
                    blocks.append(
                        self._Para(f"AI Summary: {table['ai_summary']}")
                    )
                blocks.append(
                    self._create_column_table(
                        table.get("columns", []), is_dbt=False
                    )
                )

        return blocks

    def _generate_dbt_blocks(
        self,
        catalog_data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Generates a list of Notion blocks for a dbt project catalog.

        This includes sections for each dbt model, with its AI-generated summary,
        Mermaid lineage chart, and column details rendered in a Notion table block.

        Args:
            catalog_data: The structured dbt project catalog.

        Returns:
            A list of dictionaries, each representing a valid Notion block.
        """
        blocks = []
        for model_name, model_data in catalog_data.items():
            blocks.append(self._H2(f"🧬 Model: {model_name}"))

            # 1. Model Summary
            blocks.append(self._H3("AI-Generated Model Summary"))
            blocks.append(
                self._Para(
                    model_data.get(
                        "model_description", "(No summary available)"
                    )
                )
            )

            # 2. Lineage Chart
            blocks.append(self._H3("AI-Generated Lineage (Mermaid)"))
            mermaid_code = model_data.get(
                "model_lineage_chart", "graph TD; A[N/A];"
            )
            cleaned_code = self._clean_mermaid_code(mermaid_code)
            blocks.append(self._Code(cleaned_code, "mermaid"))

            # 3. Columns Table
            blocks.append(self._H3("Column Details"))
            blocks.append(
                self._create_column_table(
                    model_data.get("columns", []), is_dbt=True
                )
            )

        return blocks
