from typing import Dict, List, Any

from data_scribe.utils.logger import get_logger
from data_scribe.core.interfaces import BaseWriter

# Initialize a logger for this module
logger = get_logger(__name__)


class MarkdownWriter(BaseWriter):
    """
    Handles writing the generated database catalog to a Markdown file.
    """

    def write(self, catalog_data: Dict[str, List[Dict[str, Any]]], **kwargs):
        """
        Writes the catalog data to a Markdown file in a structured table format.

        Args:
            catalog_data: A dictionary where keys are table names and values are lists of column dictionaries.
            output_filename: The name of the file to write the catalog to.
            db_profile_name: The name of the database profile used, for the report title.
        """
        output_filename = kwargs.get("output_filename")
        db_profile_name = kwargs.get("db_profile_name")
        if not output_filename or not db_profile_name:
            logger.error(
                "MarkdownWriter 'write' method missing 'output_filename' or 'db_profile_name'."
            )
            raise ValueError("Missing required kwargs for MarkdownWriter.")

        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                logger.info(
                    f"Writing data catalog for '{db_profile_name}' to '{output_filename}'."
                )
                # Write the main title of the Markdown file
                f.write(f"# 📁 Data Catalog for {db_profile_name}\n")

                # Iterate over each table in the catalog data
                for table_name, columns in catalog_data.items():
                    f.write(f"\n## 📄 Table: `{table_name}`\n\n")
                    # Write the header of the column table
                    f.write("| Column Name | Data Type | AI-Generated Description |\n")
                    f.write("| :--- | :--- | :--- |\n")

                    # Write a row for each column
                    for column in columns:
                        col_name = column["name"]
                        col_type = column["type"]
                        description = column["description"]
                        f.write(f"| `{col_name}` | `{col_type}` | {description} |\n")
            logger.info("Finished writing catalog file.")
        except IOError as e:
            logger.error(
                f"Error writing to file '{output_filename}': {e}", exc_info=True
            )
            # Re-raise the exception to be handled by the CLI
            raise
