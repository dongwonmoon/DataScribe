"""
Unit tests for the JsonWriter.
"""

import pytest
import json

from schema_scribe.components.writers import JsonWriter
from schema_scribe.core.exceptions import WriterError


@pytest.fixture
def mock_db_catalog_data():
    """Provides a mock catalog data structure for standard DB connections."""
    return {
        "tables": [
            {
                "name": "users",
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "User ID"},
                    {
                        "name": "email",
                        "type": "TEXT",
                        "description": "User email",
                    },
                ],
            }
        ],
        "views": [
            {
                "name": "user_views",
                "ai_summary": "A summary of the view.",
                "definition": "SELECT * FROM users",
            }
        ],
        "foreign_keys": [
            {
                "from_table": "orders",
                "to_table": "users",
                "from_column": "user_id",
                "to_column": "id",
            }
        ],
    }


def test_json_writer_write(tmp_path, mock_db_catalog_data):
    """Tests that JsonWriter correctly writes a catalog to a .json file."""
    output_file = tmp_path / "catalog.json"
    writer = JsonWriter()
    writer.write(mock_db_catalog_data, output_filename=str(output_file))

    assert output_file.exists()
    with open(output_file, "r") as f:
        data = json.load(f)

    assert data == mock_db_catalog_data


def test_failed_write_preserves_previous_output(tmp_path, monkeypatch, mock_db_catalog_data):
    """A mid-write failure must leave the previous output intact (atomic write)."""
    import os
    import builtins

    target = tmp_path / "catalog.json"
    target.write_text("PREVIOUS CONTENT")

    real_open = builtins.open
    real_fdopen = os.fdopen

    class FailingFile:
        def __init__(self, real):
            self._real = real

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self._real.close()

        def write(self, s):
            raise IOError("disk full")

        def __getattr__(self, name):
            return getattr(self._real, name)

    def failing_open(*args, **kwargs):
        return FailingFile(real_open(*args, **kwargs))

    def failing_fdopen(*args, **kwargs):
        return FailingFile(real_fdopen(*args, **kwargs))

    monkeypatch.setattr(builtins, "open", failing_open)
    monkeypatch.setattr(os, "fdopen", failing_fdopen)

    writer = JsonWriter()
    with pytest.raises(WriterError):
        writer.write(mock_db_catalog_data, output_filename=str(target))

    assert target.read_text() == "PREVIOUS CONTENT"
