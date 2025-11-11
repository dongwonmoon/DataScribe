"""
This module provides `ConfluenceWriter`, an implementation of `BaseWriter` that
uploads a generated data catalog to a Confluence page.

Design Rationale:
The `ConfluenceWriter` is designed to integrate Schema Scribe's generated
documentation directly into Confluence, a popular knowledge management platform.
It leverages the `atlassian-python-api` library to interact with the Confluence
API. A key aspect of its design is the conversion of structured catalog data
into Confluence Storage Format (essentially HTML with Confluence-specific macros),
allowing for rich content display, including embedded Mermaid diagrams.
It handles both creating new pages and updating existing ones.
"""

import os
from typing import Dict, Any, List
from atlassian import Confluence

from schema_scribe.core.interfaces import BaseWriter
from schema_scribe.core.exceptions import WriterError, ConfigError
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class ConfluenceWriter(BaseWriter):
    """
    Implements `BaseWriter` to write a data catalog to a Confluence page.

    This writer orchestrates the entire process:
    1.  Connects to the Confluence API using securely resolved credentials.
    2.  Generates a Confluence Storage Format (HTML) string from the catalog data.
    3.  Checks if the target page already exists based on its title.
    4.  Creates a new page or updates an existing one accordingly.
    """

    def __init__(self):
        """Initializes the ConfluenceWriter."""
        self.confluence: Confluence | None = None
        self.params: Dict[str, Any] = {}
        logger.info("ConfluenceWriter initialized.")

    def _connect(self):
        """
        Connects to the Confluence instance using parameters from the config.

        Design Rationale:
        API tokens are sensitive. This method securely resolves the API token,
        allowing it to be provided directly or as an environment variable
        reference (e.g., `${CONFLUENCE_API_TOKEN}`). This promotes secure
        configuration practices.

        Raises:
            ConfigError: If an environment variable for the token is required
                         but not set.
            ConnectionError: If the connection to Confluence fails.
        """
        token = self.params.get("api_token")
        if token and token.startswith("${basedir}") and token.endswith("}"):
            env_var = token[2:-1]
            token = os.getenv(env_var)
            if not token:
                raise ConfigError(
                    f"Environment variable '{env_var}' is required but not set."
                )

        try:
            self.confluence = Confluence(
                url=self.params["url"],
                username=self.params["username"],
                password=token,  # The 'password' field takes the API token
            )
            logger.info(
                f"Successfully connected to Confluence at '{self.params['url']}'."
            )
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to Confluence: {e}"
            ) from e

    def write(self, catalog_data: Dict[str, Any], **kwargs):
        """
        Converts catalog data to HTML and creates or updates a Confluence page.

        This is the main entrypoint. It connects, generates HTML, and handles
        the page creation/update logic.

        Args:
            catalog_data: The dictionary containing the generated data catalog.
            **kwargs: Configuration parameters. Expected keys include `url`,
                      `username`, `api_token`, `space_key`, `parent_page_id`,
                      and optional `page_title_prefix` and `project_name`.

        Raises:
            WriterError: If writing to the Confluence page fails.
        """
        self.params = kwargs
        self._connect()

        space_key = self.params["space_key"]
        parent_page_id = self.params["parent_page_id"]
        project_name = kwargs.get("project_name", "DB")
        page_title_prefix = self.params.get("page_title_prefix", "Data Catalog")
        page_title = f"{page_title_prefix} - {project_name}"

        html_body = self._generate_html(catalog_data, project_name)

        try:
            page_id = self.confluence.get_page_id(space_key, page_title)
            if page_id:
                logger.info(
                    f"Updating existing Confluence page: '{page_title}' (ID: {page_id})"
                )
                self.confluence.update_page(
                    page_id=page_id,
                    title=page_title,
                    body=html_body,
                    representation="storage",
                )
            else:
                logger.info(f"Creating new Confluence page: '{page_title}'")
                self.confluence.create_page(
                    space=space_key,
                    title=page_title,
                    body=html_body,
                    parent_id=parent_page_id,
                    representation="storage",
                )
            logger.info("Successfully updated the Confluence page.")
        except Exception as e:
            raise WriterError(f"Failed to write to Confluence page: {e}") from e

    def _generate_html(
        self,
        catalog_data: Dict[str, Any],
        project_name: str,
    ) -> str:
        """
        Routes to the correct HTML generator based on the catalog type.

        Design Rationale:
        This method uses a simple heuristic to determine if the input `catalog_data`
        is from a traditional database scan (containing `db_profile_name` in `self.params`)
        or a dbt project. This allows the ConfluenceWriter to adapt its output
        format to the source of the catalog.

        Args:
            catalog_data: The dictionary containing the catalog data.
            project_name: The name of the project, used for titles.

        Returns:
            A string containing the full HTML for the Confluence page body.
        """
        if "db_profile_name" in self.params:
            return self._generate_db_html(
                catalog_data, self.params["db_profile_name"]
            )
        return self._generate_dbt_html(catalog_data, project_name)

    def _generate_erd_mermaid_confluence(
        self,
        foreign_keys: List[Dict[str, str]],
    ) -> str:
        """
        Generates raw Mermaid ERD code for the Confluence Mermaid macro.

        This helper function takes a list of foreign key relationships and
        constructs a string containing Mermaid graph syntax, suitable for
        embedding within Confluence's Mermaid macro.

        Args:
            foreign_keys: A list of foreign key relationships.

        Returns:
            A string of raw Mermaid code (without code fences).
        """
        if not foreign_keys:
            return "graph TD;\n  A[No foreign key relationships found]"

        code = ["erDiagram"]
        for fk in foreign_keys:
            source_table = fk["source_table"]
            target_table = fk["target_table"]
            label = f"{fk['source_column']} to {fk['target_column']}"
            code.append(
                f'    "{source_table}" ||--o{{ "{target_table}" : "{label}"'
            )
        return "\n".join(code)

    def _generate_db_html(
        self,
        catalog_data: Dict[str, Any],
        db_profile_name: str,
    ) -> str:
        """
        Generates the HTML body for a traditional database catalog.

        This includes sections for ERD (using a Mermaid macro), views, and tables.

        Args:
            catalog_data: The structured data catalog.
            db_profile_name: The name of the database profile.

        Returns:
            A string containing the HTML content for the Confluence page.
        """
        html = f"<h1>📁 Data Catalog for {db_profile_name}</h1>"
        html += "<h2>🚀 Entity Relationship Diagram (ERD)</h2>"
        mermaid_code = self._generate_erd_mermaid_confluence(
            catalog_data.get("foreign_keys", [])
        )
        html += f'<ac:structured-macro ac:name="mermaid"><ac:plain-text-body><![CDATA[{mermaid_code}]]></ac:plain-text-body></ac:structured-macro>'

        html += "<h2>🔎 Views</h2>"
        views = catalog_data.get("views", [])
        if not views:
            html += "<p>No views found in this database.</p>"
        else:
            for view in views:
                html += f"<h3>📄 View: <code>{view['name']}</code></h3>"
                html += "<h4>AI-Generated Summary</h4>"
                html += (
                    f"<p>{view.get('ai_summary', '(No summary available)')}</p>"
                )
                html += "<h4>SQL Definition</h4>"
                html += f'<ac:structured-macro ac:name="code" ac:parameters-language="sql"><ac:plain-text-body><![CDATA[{view.get("definition", "N/A")}]]></ac:plain-text-body></ac:structured-macro>'

        html += "<h2>🗂️ Tables</h2>"
        tables = catalog_data.get("tables", [])
        if not tables:
            html += "<p>No tables found in this database.</p>"
        else:
            for table in tables:
                html += f"<h3>📄 Table: <code>{table['name']}</code></h3>"
                html += "<table><thead><tr><th>Column Name</th><th>Data Type</th><th>AI-Generated Description</th></tr></thead><tbody>"
                for col in table.get("columns", []):
                    html += f"<tr><td><code>{col['name']}</code></td><td>{col['type']}</td><td>{col['description']}</td></tr>"
                html += "</tbody></table>"
        return html

    def _generate_dbt_html(
        self,
        catalog_data: Dict[str, Any],
        project_name: str,
    ) -> str:
        """
        Generates the HTML body for a dbt project catalog.

        This includes sections for model summaries, lineage charts (using a
        Mermaid macro), and column details.

        Args:
            catalog_data: The structured data catalog.
            project_name: The name of the dbt project.

        Returns:
            A string containing the HTML content for the Confluence page.
        """
        html = f"<h1>🧬 Data Catalog for {project_name} (dbt)</h1>"
        for model_name, model_data in catalog_data.items():
            html += f"<h2>🚀 Model: <code>{model_name}</code></h2>"
            html += "<h3>AI-Generated Model Summary</h3>"
            html += f"<p>{model_data.get('model_description', '(No summary available)')}</p>"
            html += "<h3>AI-Generated Lineage (Mermaid)</h3>"
            mermaid_code = (
                model_data.get("model_lineage_chart", "graph TD; A[N/A];")
                .replace("```mermaid", "")
                .replace("```", "")
                .strip()
            )
            html += f'<ac:structured-macro ac:name="mermaid"><ac:plain-text-body><![CDATA[{mermaid_code}]]></ac:plain-text-body></ac:structured-macro>'
            html += "<h3>Column Details</h3>"
            html += "<table><thead><tr><th>Column Name</th><th>Data Type</th><th>AI-Generated Description</th></tr></thead><tbody>"
            for col in model_data.get("columns", []):
                description = col.get("ai_generated", {}).get(
                    "description", "(N/A)"
                )
                html += f"<tr><td><code>{col['name']}</code></td><td>{col['type']}</td><td>{description}</td></tr>"
            html += "</tbody></table>"
        return html
