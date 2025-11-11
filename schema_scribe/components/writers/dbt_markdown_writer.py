"""
This module provides a writer for generating a dbt data catalog in Markdown format.

It implements the `BaseWriter` interface and converts the structured dbt catalog
data into a human-readable Markdown file. This includes model descriptions,
column details, and Mermaid.js lineage charts.
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
    """

    def write(self, catalog_data: Dict[str, Any], **kwargs):
        """
        Writes the dbt catalog data to a Markdown file.

        This method generates a file containing model summaries, Mermaid lineage
        charts, and tables with column-level details.

        Args:
            catalog_data: A dictionary containing the dbt catalog data, keyed
                          by model name.
            **kwargs: Additional writer-specific arguments. Expects
                      `output_filename` and `project_name`.

        Raises:
            ConfigError: If required `kwargs` are missing.
            WriterError: If an error occurs during file writing.
        """
        output_filename = kwargs.get("output_filename")
        project_name = kwargs.get("project_name")
        if not output_filename or not project_name:
            logger.error(
                "DbtMarkdownWriter 'write' method missing 'output_filename' or 'project_name'."
            )
            raise ConfigError("Missing required kwargs for DbtMarkdownWriter.")

        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                logger.info(
                    f"Writing dbt catalog for '{project_name}' to '{output_filename}'."
                )
                # Write the main title of the Markdown file
                f.write(f"# 🧬 Data Catalog for {project_name} (dbt)\n")

                # Iterate over each model in the catalog data
                for model_name, model_data in catalog_data.items():
                    f.write(f"\n## 🚀 Model: `{model_name}`\n\n")

                    # Write the AI-generated summary for the model
                    f.write("### AI-Generated Model Summary\n")
                    f.write(
                        f"{model_data.get('model_description', '(No summary available)')}\n\n"
                    )

                    # Write the AI-generated Mermaid Lineage chart for the model
                    f.write("### AI-Generated Lineage (Mermaid)\n")
                    mermaid_chart = model_data.get(
                        "model_lineage_chart",
                        "*(Lineage chart generation failed)*",
                    )
                    f.write(f"{mermaid_chart}\n\n")

                    # Write the header for the column details table
                    f.write("### Column Details\n")
                    f.write(
                        "| Column Name | Data Type | AI-Generated Description |\n"
                    )
                    f.write("| :--- | :--- | :--- |\n")

                    columns = model_data.get("columns", [])
                    if not columns:
                        f.write("| (No columns found) | | |\n")
                        continue

                    # Write a row for each column in the model
                    for column in columns:
                        col_name = column["name"]
                        col_type = column["type"]
                        ai_data = column.get("ai_generated", {})
                        description = ai_data.get(
                            "description", "(AI description failed)"
                        )
                        f.write(
                            f"| `{col_name}` | `{col_type}` | {description} |\n"
                        )

            logger.info("Finished writing dbt catalog file.")
        except IOError as e:
            logger.error(
                f"Error writing to file '{output_filename}': {e}", exc_info=True
            )
            # Re-raise the exception to be handled by the CLI
            raise WriterError(
                f"Error writing to file '{output_filename}': {e}"
            ) from e
