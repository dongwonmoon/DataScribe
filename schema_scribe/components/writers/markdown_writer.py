"""
This module provides a writer for generating a data catalog in Markdown format.

It implements the `BaseWriter` interface and converts the structured catalog
data into a human-readable Markdown file, including tables for columns, view
definitions, and a Mermaid.js Entity Relationship Diagram (ERD).
"""

from typing import Dict, List, Any

from schema_scribe.utils.logger import get_logger
from schema_scribe.core.interfaces import BaseWriter
from schema_scribe.core.exceptions import WriterError, ConfigError


# Initialize a logger for this module
logger = get_logger(__name__)


class MarkdownWriter(BaseWriter):
    """
    Implements `BaseWriter` to write a database catalog to a Markdown file.
    """

    def _generate_erd_mermaid(self, foreign_keys: List[Dict[str, str]]) -> str:
        """
        Generates a Mermaid ERD chart from foreign key data.

        This helper function takes a list of foreign key relationships and
        constructs a string containing Mermaid graph syntax.

        Args:
            foreign_keys: A list of dictionaries, each representing a
                          foreign key relationship.

        Returns:
            A string containing the Mermaid ERD code block, or a message
            if no foreign keys were provided.
        """
        if not foreign_keys:
            return "No foreign key relationships found to generate a diagram."

        # Mermaid syntax reference: https://mermaid.js.org/syntax/entityRelationshipDiagram.html
        code = ["```mermaid", "erDiagram"]

        # Add all relationships to the diagram definition.
        for fk in foreign_keys:
            # Example syntax: "users" ||--o{ "orders" : "has"
            code.append(
                f'    {fk["from_table"]} ||--o{{ {fk["to_table"]} : "{fk["from_column"]} to {fk["to_column"]}"'
            )

        code.append("```")
        return "\n".join(code)

    def write(self, catalog_data: Dict[str, List[Dict[str, Any]]], **kwargs):
        """
        Writes the catalog data to a Markdown file.

        The generated file includes a title, an ERD chart, and sections for
        all database views and tables.

        Args:
            catalog_data: A dictionary containing the structured catalog data.
            **kwargs: Additional writer-specific arguments. Expects
                      `output_filename` and `db_profile_name`.

        Raises:
            ConfigError: If required `kwargs` are missing.
            WriterError: If an error occurs during file writing.
        """
        output_filename = kwargs.get("output_filename")
        db_profile_name = kwargs.get("db_profile_name")
        if not output_filename or not db_profile_name:
            logger.error(
                "MarkdownWriter 'write' method missing 'output_filename' or 'db_profile_name'."
            )
            raise ConfigError("Missing required kwargs for MarkdownWriter.")

        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                logger.info(
                    f"Writing data catalog for '{db_profile_name}' to '{output_filename}'."
                )
                # --- 1. Main Title ---
                f.write(f"# 📁 Data Catalog for {db_profile_name}\n")

                # --- 2. ERD Section ---
                f.write("\n## 🚀 Entity Relationship Diagram (ERD)\n\n")
                foreign_keys = catalog_data.get("foreign_keys", [])
                mermaid_code = self._generate_erd_mermaid(foreign_keys)
                f.write(mermaid_code + "\n")

                # --- 3. Views Section ---
                f.write("\n## 🔎 Views\n\n")
                views = catalog_data.get("views", [])
                if not views:
                    f.write("No views found in this database.\n")
                else:
                    for view in views:
                        f.write(f"### 📄 View: `{view['name']}`\n\n")
                        f.write("**AI-Generated Summary:**\n")
                        f.write(
                            f"> {view.get('ai_summary', '(No summary available)')}\n\n"
                        )
                        f.write("**SQL Definition:**\n")
                        f.write(
                            f"```sql\n{view.get('definition', 'N/A')}\n```\n\n"
                        )

                # --- 4. Tables Section ---
                f.write("\n## 🗂️ Tables\n\n")
                tables = catalog_data.get("tables", [])
                if not tables:
                    f.write("No tables found in this database.\n")
                else:
                    for table in tables:
                        table_name = table["name"]
                        columns = table["columns"]

                        f.write(f"### 📄 Table: `{table_name}`\n\n")
                        f.write(
                            "| Column Name | Data Type | AI-Generated Description |\n"
                        )
                        f.write("| :--- | :--- | :--- |\n")

                        for column in columns:
                            col_name = column["name"]
                            col_type = column["type"]
                            description = column["description"]
                            f.write(
                                f"| `{col_name}` | `{col_type}` | {description} |\n"
                            )
            logger.info(f"Successfully wrote catalog to '{output_filename}'.")
        except IOError as e:
            logger.error(
                f"Error writing to file '{output_filename}': {e}", exc_info=True
            )
            raise WriterError(
                f"Error writing to file '{output_filename}': {e}"
            ) from e
