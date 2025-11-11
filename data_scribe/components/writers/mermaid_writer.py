"""
This module provides a specialized writer for saving a Mermaid.js graph
string to a Markdown file, formatted for rendering.
"""

from typing import Dict, Any

from data_scribe.utils.logger import get_logger
from data_scribe.core.interfaces import BaseWriter
from data_scribe.core.exceptions import WriterError, ConfigError

logger = get_logger(__name__)


class MermaidWriter(BaseWriter):
    """
    Handles writing a single Mermaid graph string to a Markdown file.

    This writer is designed to take a complete Mermaid graph definition
    and save it within a Markdown code block, ready for rendering in
    supported platforms like GitHub or GitLab.
    """

    def write(self, catalog_data: Dict[str, Any], **kwargs):
        """
        Writes the Mermaid graph from catalog_data to a Markdown file.

        Args:
            catalog_data: A dictionary expected to contain the key
                          `"mermaid_graph"` with the full Mermaid string.
            **kwargs: Expects the `output_filename` key, which specifies
                      the path to the output `.md` file.

        Raises:
            ConfigError: If `output_filename` is not provided in kwargs.
            WriterError: If there is an error writing the file to disk.
        """
        output_filename = kwargs.get("output_filename")
        if not output_filename:
            logger.error(
                "MermaidWriter 'write' method missing 'output_filename'."
            )
            raise ConfigError(
                "Missing required kwarg 'output_filename' for MermaidWriter."
            )

        mermaid_graph = catalog_data.get("mermaid_graph")
        if not mermaid_graph:
            logger.warning(
                "No 'mermaid_graph' key found in catalog data. Writing empty file."
            )
            mermaid_graph = "graph TD;\n  A[No lineage data found];"

        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                logger.info(f"Writing global lineage to '{output_filename}'...")
                f.write("# 🌐 Global Data Lineage\n\n")
                f.write("```mermaid\n")
                f.write(mermaid_graph)
                f.write("\n```\n")
            logger.info("Finished writing lineage file.")
        except IOError as e:
            logger.error(
                f"Error writing to file '{output_filename}': {e}", exc_info=True
            )
            raise WriterError(
                f"Error writing to file '{output_filename}': {e}"
            ) from e
