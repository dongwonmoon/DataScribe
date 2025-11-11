"""
This module provides `JsonWriter`, an implementation of `BaseWriter` for
generating a data catalog in JSON format.

Design Rationale:
The `JsonWriter` serves as a fundamental output option for the Schema Scribe
application. Its primary purpose is to serialize the structured catalog data
into a standard, machine-readable JSON format. This is particularly useful for:
- **Programmatic Access**: Allowing other applications or scripts to easily
  consume and process the generated metadata.
- **Intermediate Format**: Serving as a robust intermediate representation
  before further transformation or loading into other systems.
- **Debugging**: Providing a clear, human-readable (when formatted) view of
  the raw catalog data.
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
    Implements `BaseWriter` to write the data catalog to a JSON file.

    This writer provides a straightforward way to dump the raw, structured
    catalog data into a machine-readable format, suitable for programmatic
    consumption or as an intermediate data representation.
    """

    def write(self, catalog_data: Dict[str, Any], **kwargs):
        """
        Writes the catalog data to a JSON file with an indent of 2.

        Args:
            catalog_data: The dictionary containing the structured data catalog.
            **kwargs: Must contain `output_filename`.

        Raises:
            ConfigError: If the `output_filename` is not provided in kwargs.
            WriterError: If an error occurs during file writing.
        """
        output_filename = kwargs.get("output_filename")
        if not output_filename:
            raise ConfigError(
                "JsonWriter requires 'output_filename' in kwargs."
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
