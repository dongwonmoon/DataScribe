"""
This module provides a writer that uploads a generated data catalog to Confluence.

It implements the `BaseWriter` interface, connecting to a Confluence instance,
converting the catalog data into Confluence-friendly HTML (including Mermaid
charts), and then creating or updating a page with this content.
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

    This writer transforms the catalog dictionary into a Confluence Storage
    Format (HTML) string and uses the REST API to create or update a page.
    It supports both database and dbt project catalogs.
    """

    def __init__(self):
        """Initializes the ConfluenceWriter."""
        self.confluence: Confluence | None = None
        self.params: Dict[str, Any] = {}
        logger.info("ConfluenceWriter initialized.")

    def _connect(self):
        """
        Connects to the Confluence instance using parameters from the config.

        This method uses the `atlassian-python-api` library to establish a
        connection. It supports resolving API tokens from environment variables
        if they are specified in the format `${VAR_NAME}` in the config file.

        Raises:
            ConfigError: If an environment variable for the API token is
                         specified but not set.
            ConnectionError: If the connection to the Confluence instance fails.
        """
        try:
            token = self.params.get("api_token")

            # Resolve the API token if it's specified as an environment variable
            # in the format `${VAR_NAME}`.
            if token and token.startswith("${basedir}") and token.endswith("}"):
                env_var = token[
                    2:-1
                ]  # Extract the variable name (e.g., CONFLUENCE_API_TOKEN)
                token = os.getenv(env_var)
                if not token:
                    raise ConfigError(
                        f"The environment variable '{env_var}' is required but not set."
                    )

            self.confluence = Confluence(
                url=self.params["url"],
                username=self.params["username"],
                password=token,  # The 'password' argument takes the API token
            )
            logger.info(
                f"Successfully connected to Confluence at '{self.params['url']}'."
            )
        except Exception as e:
            logger.error(f"Failed to connect to Confluence: {e}", exc_info=True)
            raise ConnectionError(f"Failed to connect to Confluence: {e}")

    def write(self, catalog_data: Dict[str, Any], **kwargs):
        """
        Converts catalog data to HTML and creates or updates a Confluence page.

        This is the main entrypoint for the writer. It orchestrates the connection,
        HTML generation, and page creation/update process.

        Args:
            catalog_data: The dictionary containing the generated data catalog.
            **kwargs: A dictionary of parameters from the `output_profiles`
                      section of the config. Expected keys include `url`,
                      `username`, `api_token`, `space_key`, `parent_page_id`,
                      and optional `page_title_prefix` and `project_name`.

        Raises:
            WriterError: If writing to the Confluence page fails.
        """
        self.params = kwargs
        self._connect()  # Establish the connection to Confluence

        space_key = self.params["space_key"]
        parent_page_id = self.params["parent_page_id"]
        project_name = kwargs.get("project_name", "DB")
        page_title_prefix = self.params.get("page_title_prefix", "Data Catalog")
        page_title = f"{page_title_prefix} - {project_name}"

        # Generate the HTML content for the Confluence page body
        html_body = self._generate_html(catalog_data, project_name)

        try:
            # Check if a page with the same title already exists
            page_id = self.confluence.get_page_id(space_key, page_title)

            if page_id:
                # If the page exists, update it with the new content
                logger.info(
                    f"Updating existing Confluence page: '{page_title}' (ID: {page_id})"
                )
                self.confluence.update_page(
                    page_id=page_id,
                    title=page_title,
                    body=html_body,
                    representation="storage",  # Use 'storage' format for HTML
                )
            else:
                # If the page does not exist, create a new one
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
            logger.error(
                f"Failed to write to Confluence page: {e}", exc_info=True
            )
            raise WriterError(f"Failed to write to Confluence page: {e}")

    def _generate_html(
        self, catalog_data: Dict[str, Any], project_name: str
    ) -> str:
        """
        Routes to the correct HTML generator based on the catalog type.

        This function acts as a router, calling the appropriate HTML generation
        method based on whether the catalog is for a database or a dbt project.

        Args:
            catalog_data: The dictionary containing the catalog data.
            project_name: The name of the project, used for titles.

        Returns:
            A string containing the full HTML for the Confluence page body.
        """
        # The presence of 'db_profile_name' indicates a database scan
        if "db_profile_name" in self.params:
            return self._generate_db_html(
                catalog_data, self.params["db_profile_name"]
            )
        else:
            return self._generate_dbt_html(catalog_data, project_name)

    def _generate_erd_mermaid_confluence(
        self, foreign_keys: List[Dict[str, str]]
    ) -> str:
        """
        Generates raw Mermaid ERD code for the Confluence Mermaid macro.

        Args:
            foreign_keys: A list of foreign key relationships.

        Returns:
            A string of raw Mermaid code (without code fences).
        """
        if not foreign_keys:
            return "graph TD;\n  A[No foreign key relationships found]"

        code = ["graph TD;"]  # Top-Down graph
        tables = set()
        for fk in foreign_keys:
            tables.add(fk["from_table"])
            tables.add(fk["to_table"])

        for table in tables:
            code.append(f"  {table}[{table}]")

        code.append("")
        for fk in foreign_keys:
            label = f"{fk['from_column']} → {fk['to_column']}"
            code.append(
                f'  {fk["from_table"]} --> {fk["to_table"]} : "{label}"'
            )

        return "\n".join(code)

    def _generate_db_html(
        self, catalog_data: Dict[str, Any], db_profile_name: str
    ) -> str:
        """
        Generates the HTML body for a database catalog.

        Args:
            catalog_data: The catalog data for the database.
            db_profile_name: The name of the database profile, used in the title.

        Returns:
            An HTML string representing the database catalog.
        """
        html = f"<h1>📁 Data Catalog for {db_profile_name}</h1>"

        html += "<h2>🚀 Entity Relationship Diagram (ERD)</h2>"
        foreign_keys = catalog_data.get("foreign_keys", [])
        mermaid_code = self._generate_erd_mermaid_confluence(foreign_keys)
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
                table_name = table["name"]
                columns = table["columns"]
                html += f"<h3>📄 Table: <code>{table_name}</code></h3>"
                html += "<table><thead><tr>"
                html += "<th>Column Name</th><th>Data Type</th><th>AI-Generated Description</th>"
                html += "</tr></thead><tbody>"
                for col in columns:
                    html += f"<tr><td><code>{col['name']}</code></td><td>{col['type']}</td><td>{col['description']}</td></tr>"
                html += "</tbody></table>"

        return html

    def _generate_dbt_html(
        self, catalog_data: Dict[str, Any], project_name: str
    ) -> str:
        """
        Generates the HTML body for a dbt project catalog.

        Args:
            catalog_data: The catalog data for the dbt project.
            project_name: The name of the dbt project, used in the title.

        Returns:
            An HTML string representing the dbt catalog.
        """
        html = f"<h1>🧬 Data Catalog for {project_name} (dbt)</h1>"
        for model_name, model_data in catalog_data.items():
            html += f"<h2>🚀 Model: <code>{model_name}</code></h2>"

            # Section for the AI-generated model summary
            html += "<h3>AI-Generated Model Summary</h3>"
            html += f"<p>{model_data.get('model_description', '(No summary available)')}</p>"

            # Section for the AI-generated lineage chart using the Mermaid macro
            html += "<h3>AI-Generated Lineage (Mermaid)</h3>"
            mermaid_code = model_data.get(
                "model_lineage_chart", "graph TD; A[N/A];"
            )
            # The Confluence macro requires the raw Mermaid code without fences
            mermaid_code = (
                mermaid_code.replace("```mermaid", "")
                .replace("```", "")
                .strip()
            )
            # Embed the Mermaid code within the Confluence macro structure
            html += f'<ac:structured-macro ac:name="mermaid"><ac:plain-text-body><![CDATA[{mermaid_code}]]></ac:plain-text-body></ac:structured-macro>'

            # Section for the column details in a table
            html += "<h3>Column Details</h3>"
            html += "<table><thead><tr>"
            html += "<th>Column Name</th><th>Data Type</th><th>AI-Generated Description</th>"
            html += "</tr></thead><tbody>"
            for col in model_data.get("columns", []):
                description = col.get("ai_generated", {}).get(
                    "description", "(N/A)"
                )
                html += f"<tr><td><code>{col['name']}</code></td><td>{col['type']}</td><td>{description}</td></tr>"
            html += "</tbody></table>"
        return html
