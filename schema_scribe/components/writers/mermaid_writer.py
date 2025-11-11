"""
This module provides `MermaidWriter`, a specialized `BaseWriter` implementation
used by the `lineage` workflow to save a Mermaid.js graph string to a
Markdown file.

Design Rationale:
The `MermaidWriter` is designed for a very specific purpose: to output a
pre-generated Mermaid graph definition into a Markdown file. This is primarily
used by the `lineage` workflow, which constructs a comprehensive lineage graph
and then passes it to this writer. The writer's role is minimal, focusing
solely on formatting the Mermaid code within a Markdown code block, making it
ready for rendering in platforms that support Mermaid.js (e.g., GitHub, Confluence).
"""

from typing import Dict, Any

from schema_scribe.utils.logger import get_logger
from schema_scribe.core.interfaces import BaseWriter
from schema_scribe.core.exceptions import WriterError, ConfigError

logger = get_logger(__name__)


class MermaidWriter(BaseWriter):
    """
    Implements `BaseWriter` to write a Mermaid graph to a Markdown file.

    This writer is designed for a single purpose: to take a complete,
    pre-generated Mermaid graph definition from the catalog data and save it
    within a Markdown code block, ready for rendering in supported platforms
    like GitHub.
    """

    def write(self, catalog_data: Dict[str, Any], **kwargs):
        """
        Writes the Mermaid graph from catalog_data to a Markdown file.

        It expects the `catalog_data` dictionary to contain a key
        `"mermaid_graph"` with the full Mermaid string. If this key is not
        found, it writes a placeholder graph.

        Args:
            catalog_data: A dictionary expected to contain the key
                          `"mermaid_graph"` with the full Mermaid string.
            **kwargs: Must contain the `output_filename` key, which specifies
                      the path to the output `.md` file.

        Raises:
            ConfigError: If `output_filename` is not provided in kwargs.
            WriterError: If there is an error writing the file to disk.
        """
        output_filename = kwargs.get("output_filename")
        if not output_filename:
            raise ConfigError(
                "MermaidWriter requires 'output_filename' in kwargs."
            )

        mermaid_graph = catalog_data.get("mermaid_graph")
        if not mermaid_graph:
            logger.warning(
                "No 'mermaid_graph' key found in catalog data. Writing an empty graph."
            )
            mermaid_graph = "graph TD;\n  A[No lineage data found]"

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
