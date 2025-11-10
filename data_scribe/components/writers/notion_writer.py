"""
This module provides an implementation of the `BaseWriter` for Notion.

It allows writing the generated data catalog to a new page within a specified
parent page in Notion. It handles connecting to the Notion API, transforming
the catalog data into Notion blocks, and creating the page.
"""

import os
from typing import Dict, Any, List, Optional
from notion_client import Client, APIErrorCode, APIResponseError

from data_scribe.core.interfaces import BaseWriter
from data_scribe.core.exceptions import WriterError, ConfigError
from data_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class NotionWriter(BaseWriter):
    """
    Implements `BaseWriter` to write a data catalog to a new Notion page.

    This writer connects to the Notion API and constructs a new page with the
    catalog content, including views and tables, formatted as Notion blocks.

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
        if token and token.startswith("${") and token.endswith("}"):
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
        generation, and page creation process.

        Args:
            catalog_data: The structured data catalog to be written.
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

    def _generate_notion_blocks(
        self, catalog_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Converts the catalog data dictionary into a list of Notion API block objects.

        This implementation creates a simple layout with headings, paragraphs,
        and code blocks. Note that it does not create Notion database tables,
        as the API for table creation is complex. Instead, table columns are
        rendered as a simple list within a paragraph block.

        Args:
            catalog_data: The structured data catalog.

        Returns:
            A list of dictionaries, where each dictionary is a valid Notion block.
        """
        blocks = []

        # --- Block-level helper functions for readability ---
        def H2(text):
            return {
                "object": "block",
                "type": "heading_2",
                "heading_2": {"rich_text": [{"text": {"content": text}}]},
            }

        def H3(text):
            return {
                "object": "block",
                "type": "heading_3",
                "heading_3": {"rich_text": [{"text": {"content": text}}]},
            }

        def Para(text):
            return {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"text": {"content": text}}]},
            }

        def Code(text, lang="sql"):
            return {
                "object": "block",
                "type": "code",
                "code": {
                    "rich_text": [{"text": {"content": text}}],
                    "language": lang,
                },
            }

        # --- Section 1: Views ---
        blocks.append(H2("🔎 Views"))
        views = catalog_data.get("views", [])
        if not views:
            blocks.append(Para("No views found."))
        else:
            for view in views:
                blocks.append(H3(f"View: {view['name']}"))
                blocks.append(
                    Para(f"AI Summary: {view.get('ai_summary', 'N/A')}")
                )
                blocks.append(Code(view.get("definition", "N/A"), lang="sql"))

        # --- Section 2: Tables ---
        blocks.append(H2("🗂️ Tables"))
        tables = catalog_data.get("tables", [])
        if not tables:
            blocks.append(Para("No tables found."))
        else:
            for table in tables:
                blocks.append(H3(f"Table: {table['name']}"))
                # Note: The Notion API for creating tables is complex.
                # A simple bulleted list in a paragraph is used instead.
                col_list = []
                for col in table.get("columns", []):
                    col_list.append(
                        f"  • {col['name']} ({col['type']}): {col['description']}"
                    )
                blocks.append(Para("\n".join(col_list)))

        return blocks
