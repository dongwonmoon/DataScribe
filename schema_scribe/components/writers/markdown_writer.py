"""
This module provides `MarkdownWriter`, an implementation of `BaseWriter` for
generating a data catalog in Markdown format.

Design Rationale:
The `MarkdownWriter` is designed to produce a human-readable and easily shareable
representation of a database's schema. It translates the structured output of
the `CatalogGenerator` into a Markdown document, including:
- A high-level overview.
- An Entity Relationship Diagram (ERD) generated using Mermaid.js syntax.
- Detailed sections for database views and tables, complete with AI-generated
  summaries and column descriptions.
This format is ideal for documentation that can be version-controlled and
rendered in various platforms (e.g., GitHub, Confluence).
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

    This class transforms the abstract catalog dictionary into a rich,
    human-readable Markdown document, creating dedicated sections for ERD,
    views, and tables.
    """

    def _generate_erd_mermaid(self, foreign_keys: List[Dict[str, str]]) -> str:
        """
        Generates a Mermaid.js ERD chart from foreign key data.

        This helper function takes a list of foreign key relationships and
        constructs a string containing Mermaid graph syntax. It uses the
        `source_table` and `target_table` keys from the foreign key dictionaries.

        Args:
            foreign_keys: A list of dictionaries, each representing a
                          foreign key relationship.

        Returns:
            A string containing the Mermaid ERD code block, or a message
            if no foreign keys were provided.
        """
        if not foreign_keys:
            return "No foreign key relationships found to generate a diagram."

        code = ["```mermaid", "erDiagram"]
        for fk in foreign_keys:
            # Mermaid syntax: "users" ||--o{ "orders" : "has"
            source_table = fk["source_table"]
            target_table = fk["target_table"]
            source_column = fk["source_column"]
            target_column = fk["target_column"]
            code.append(
                f'    "{source_table}" ||--o{{ "{target_table}" : "{source_column} to {target_column}"'
            )
        code.append("```")
        return "\n".join(code)

    def write(self, catalog_data: Dict[str, List[Dict[str, Any]]], **kwargs):
        """
        Writes the catalog data to a Markdown file.

        The generated file has the following structure:
        1.  A main title for the database profile.
        2.  An Entity Relationship Diagram (ERD) generated with Mermaid.js.
        3.  A section for all database views with their AI-generated summaries and SQL code.
        4.  A section for all database tables with their AI-generated summaries and column details.

        Args:
            catalog_data: A dictionary containing the structured catalog data.
            **kwargs: Must contain `output_filename` and `db_profile_name`.

        Raises:
            ConfigError: If required `kwargs` are missing.
            WriterError: If an error occurs during file writing.
        """
        output_filename = kwargs.get("output_filename")
        db_profile_name = kwargs.get("db_profile_name")
        if not output_filename or not db_profile_name:
            raise ConfigError(
                "MarkdownWriter requires 'output_filename' and 'db_profile_name' in kwargs."
            )

        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                logger.info(
                    f"Writing data catalog for '{db_profile_name}' to '{output_filename}'."
                )
                # 1. Main Title
                f.write(f"# 📁 Data Catalog for {db_profile_name}\n")

                # 2. ERD Section
                f.write("\n## 🚀 Entity Relationship Diagram (ERD)\n\n")
                foreign_keys = catalog_data.get("foreign_keys", [])
                mermaid_code = self._generate_erd_mermaid(foreign_keys)
                f.write(mermaid_code + "\n")

                # 3. Views Section
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

                # 4. Tables Section
                f.write("\n## 🗂️ Tables\n\n")
                tables = catalog_data.get("tables", [])
                if not tables:
                    f.write("No tables found in this database.\n")
                else:
                    for table in tables:
                        f.write(f"### 📄 Table: `{table['name']}`\n\n")
                        f.write("**AI-Generated Summary:**\n")
                        f.write(
                            f"> {table.get('ai_summary', '(No summary available)')}\n\n"
                        )
                        f.write(
                            "| Column Name | Data Type | AI-Generated Description |\n"
                        )
                        f.write("| :--- | :--- | :--- |\n")
                        for column in table.get("columns", []):
                            f.write(
                                f"| `{column['name']}` | `{column['type']}` | {column['description']} |\n"
                            )
                        f.write("\n")
            logger.info(f"Successfully wrote catalog to '{output_filename}'.")
        except IOError as e:
            raise WriterError(
                f"Error writing to file '{output_filename}': {e}"
            ) from e
