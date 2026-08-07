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
import os
import stat
import tempfile

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

    def _atomic_write(self, output_filename: str, content: str) -> None:
        """
        Writes `content` to `output_filename` atomically.

        The full content is written to a temp file in the target directory and
        then moved onto the target with `os.replace`, so a mid-write failure
        never leaves a truncated or partially written target file. The temp
        file is removed on any failure.

        The temp file is created mode 0600 by `tempfile.mkstemp`; the target's
        existing permission bits are preserved when it already exists,
        otherwise the umask-derived default for a new file is applied.
        """
        target_dir = os.path.dirname(os.path.abspath(output_filename))
        fd, tmp_name = tempfile.mkstemp(dir=target_dir, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(content)
            if os.path.exists(output_filename):
                os.chmod(tmp_name, stat.S_IMODE(os.stat(output_filename).st_mode))
            else:
                current_umask = os.umask(0)
                os.umask(current_umask)
                os.chmod(tmp_name, 0o666 & ~current_umask)
            os.replace(tmp_name, output_filename)
        except BaseException:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
            raise

    def render(self, catalog_data: Dict[str, Any], **kwargs) -> str:
        """
        Serializes the catalog to a JSON string without writing.

        This is the pure, in-memory half of `write()` so callers like
        `DbWorkflow.check()` can diff a fresh render against the existing
        output file without touching disk.
        """
        return json.dumps(catalog_data, indent=2)

    def write(self, catalog_data: Dict[str, Any], **kwargs):
        """
        Writes the catalog data to a JSON file with an indent of 2.

        Args:
            catalog_data: The dictionary containing the structured catalog data.
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
            logger.info(f"Writing data catalog to '{output_filename}'.")
            self._atomic_write(output_filename, self.render(catalog_data))
            logger.info(f"Successfully wrote catalog to '{output_filename}'.")
        except IOError as e:
            logger.error(
                f"Error writing to JSON file '{output_filename}': {e}",
                exc_info=True,
            )
            raise WriterError(
                f"Error writing to JSON file '{output_filename}': {e}"
            ) from e
