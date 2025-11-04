from typing import Dict, List, Any
import json

from data_scribe.utils.logger import get_logger
from data_scribe.core.interfaces import BaseWriter

# Initialize a logger for this module
logger = get_logger(__name__)


class JsonWriter(BaseWriter):
    """
    Handles writing the generated data catalog to a JSON file.
    """

    def write(self, catalog_data: Dict[str, Any], **kwargs):
        """
        Writes the catalog data to a JSON file

        kwargs required:
            output_filename (str): The name of the file to write the catalog to.
        """
        output_filename = kwargs.get("output_filename")
        if not output_filename:
            logger.error("JsonWriter 'write' method missing 'output_filename'.")
            raise ValueError("Missing required kwargs for JsonWriter.")

        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                logger.info(f"Writing data catalog to '{output_filename}'.")
                json.dump(catalog_data, f, indent=2)
            logger.info(f"Successfully wrote catalog to '{output_filename}'.")
        except IOError as e:
            logger.error(
                f"Error writing to JSON file '{output_filename}': {e}", exc_info=True
            )
            raise
