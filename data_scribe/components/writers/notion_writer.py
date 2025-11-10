import os
from typing import Dict, Any, List
from notion_client import Client, APIErrorCode, APIResponseError

from data_scribe.core.interfaces import BaseWriter
from data_scribe.core.exceptions import WriterError, ConfigError
from data_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class NotionWriter(BaseWriter):
    def __init__(self):
        self.notion: Client | None = None
        self.params: Dict[str, Any] = {}
        logger.info("NotionWriter initialized")

    def _connect(self):
        token = self.params.get("api_token")

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
            # This now correctly catches only Client() initialization errors
            logger.error(f"Failed to connect to Notion: {e}", exc_info=True)
            raise ConnectionError(f"Failed to connect to Notion: {e}")
        
    def write(self, catalog_data: Dict[str, Any], **kwargs):
        self.params = kwargs
        self._connect()

        parent_page_id = self.params.get("parent_page_id")
        if not parent_page_id:
            raise ConfigError("'parent_page_id' is required for NotionWriter.")

        project_name = kwargs.get(
            "project_name", "Data Catalog"
        )
        page_title = f"Data Catalog - {project_name}"

        # --- 1. Generate Notion Blocks ---
        # (This can be simple or very complex, we'll start simple)
        try:
            blocks = self._generate_notion_blocks(catalog_data)
        except Exception as e:
            logger.error(
                f"Failed to generate Notion blocks: {e}", exc_info=True
            )
            raise WriterError(f"Failed to generate Notion blocks: {e}")

        # --- 2. Create the new Page ---
        try:
            logger.info(f"Creating new Notion page: '{page_title}'")

            # Define Page properties (Title)
            new_page_props = {
                "title": [{"type": "text", "text": {"content": page_title}}]
            }

            # Define Page parent
            parent_data = {"page_id": parent_page_id}

            page = self.notion.pages.create(
                parent=parent_data,
                properties=new_page_props,
                children=blocks,  # Add the content blocks
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
        Helper function to convert catalog data into a list of Notion blocks.
        (This is a simplified implementation for demonstration)
        """
        blocks = []

        # --- Helper for text blocks ---
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

        # --- 1. Views ---
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

        # --- 2. Tables ---
        blocks.append(H2("🗂️ Tables"))
        tables = catalog_data.get("tables", [])
        if not tables:
            blocks.append(Para("No tables found."))
        else:
            for table in tables:
                blocks.append(H3(f"Table: {table['name']}"))
                # Note: Notion API for creating tables is complex (requires child blocks).
                # A simple paragraph list is easier for this example.
                col_list = []
                for col in table.get("columns", []):
                    col_list.append(
                        f"  • {col['name']} ({col['type']}): {col['description']}"
                    )
                blocks.append(Para("\n".join(col_list)))

        return blocks
