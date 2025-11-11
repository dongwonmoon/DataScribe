"""
This module provides a writer for generating a data catalog in JSON format.

It implements the `BaseWriter` interface and serializes the catalog data into
a nicely formatted JSON file.
"""

from typing import Dict, Any
import json

from schema_scribe.utils.logger import get_logger
from schema_scribe.core.interfaces import BaseWriter
from schema_scribe.core.exceptions import WriterError, ConfigError


# Initialize a logger for this module
logger = get_logger(__name__)


class JsonWriter(BaseWriter):
    """
    Implements the `BaseWriter` interface to write the data catalog to a JSON file.
    """

    def write(self, catalog_data: Dict[str, Any], **kwargs):
        """
        Writes the catalog data to a JSON file.

        Args:
            catalog_data: The dictionary containing the structured data catalog.
            **kwargs: Additional writer-specific arguments. Expects
                      `output_filename` to be provided.

        Raises:
            ConfigError: If the `output_filename` is not provided in kwargs.
            WriterError: If an error occurs during file writing.
        """
        output_filename = kwargs.get("output_filename")
        if not output_filename:
            logger.error("JsonWriter 'write' method missing 'output_filename'.")
            raise ConfigError(
                "Missing required 'output_filename' argument for JsonWriter."
            )

        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                logger.info(f"Writing data catalog to '{output_filename}'.")
                json.dump(catalog_data, f, indent=2)
            logger.info(f"Successfully wrote catalog to '{output_filename}'.")
        except IOError as e:
            logger.error(
                f"Error writing to JSON file '{output_filename}': {e}",
                exc_info=True,
            )
            raise WriterError(
                f"Error writing to JSON file '{output_filename}': {e}"
            ) from e
