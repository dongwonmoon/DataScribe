"""
This module provides `DbtMarkdownWriter`, an implementation of `BaseWriter` for
generating a dbt project catalog in Markdown format.

Design Rationale:
The `DbtMarkdownWriter` is specifically tailored to present the rich metadata
extracted from a dbt project. It translates the structured output of the
`DbtCatalogGenerator` (which includes model descriptions, column details, and
Mermaid.js lineage charts) into a human-readable Markdown document. This allows
dbt project documentation to be easily shared, version-controlled, and rendered
in various platforms (e.g., GitHub, Confluence).
"""

from typing import Dict, Any

from schema_scribe.utils.logger import get_logger
from schema_scribe.core.interfaces import BaseWriter
from schema_scribe.core.exceptions import WriterError, ConfigError


# Initialize a logger for this module
logger = get_logger(__name__)


class DbtMarkdownWriter(BaseWriter):
    """
    Implements `BaseWriter` to write a dbt project catalog to a Markdown file.

    This class transforms the dbt-specific catalog dictionary into a Markdown
    document, creating a dedicated section for each dbt model with its
    summary, lineage, and column details.
    """

    def write(self, catalog_data: Dict[str, Any], **kwargs):
        """
        Writes the dbt catalog data to a Markdown file.

        The generated file contains a main title for the project and then a
        dedicated section for each dbt model. Each model section includes:
        1.  An AI-generated model summary.
        2.  An AI-generated Mermaid.js lineage chart for the model's parents.
        3.  A table of the model's columns with their data types and
            AI-generated descriptions.

        Args:
            catalog_data: A dictionary containing the dbt catalog data, keyed
                          by model name.
            **kwargs: Must contain `output_filename` and `project_name`.

        Raises:
            ConfigError: If required `kwargs` are missing.
            WriterError: If an error occurs during file writing.
        """
        output_filename = kwargs.get("output_filename")
        project_name = kwargs.get("project_name")
        if not output_filename or not project_name:
            raise ConfigError(
                "DbtMarkdownWriter requires 'output_filename' and 'project_name' in kwargs."
            )

        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                logger.info(
                    f"Writing dbt catalog for '{project_name}' to '{output_filename}'."
                )
                f.write(f"# 🧬 Data Catalog for {project_name} (dbt)\n")

                for model_name, model_data in catalog_data.items():
                    f.write(f"\n## 🚀 Model: `{model_name}`\n\n")

                    # 1. Model Summary
                    f.write("### AI-Generated Model Summary\n")
                    f.write(
                        f"> {model_data.get('model_description', '(No summary available)')}\n\n"
                    )

                    # 2. Model Lineage
                    f.write("### AI-Generated Lineage (Mermaid)\n")
                    mermaid_chart = model_data.get(
                        "model_lineage_chart",
                        "*(Lineage chart generation failed)*",
                    )
                    f.write(f"{mermaid_chart}\n\n")

                    # 3. Column Details
                    f.write("### Column Details\n")
                    f.write(
                        "| Column Name | Data Type | AI-Generated Description |\n"
                    )
                    f.write("| :--- | :--- | :--- |\n")

                    columns = model_data.get("columns", [])
                    if not columns:
                        f.write("| (No columns found) | | |\n")
                        continue

                    for column in columns:
                        ai_data = column.get("ai_generated", {})
                        description = ai_data.get(
                            "description", "(AI description failed)"
                        )
                        f.write(
                            f"| `{column['name']}` | `{column['type']}` | {description} |\n"
                        )

            logger.info("Finished writing dbt catalog file.")
        except IOError as e:
            raise WriterError(
                f"Error writing to file '{output_filename}': {e}"
            ) from e
