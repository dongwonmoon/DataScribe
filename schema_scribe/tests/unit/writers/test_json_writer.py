"""
Unit tests for the JsonWriter.
"""

import pytest
import json

from schema_scribe.components.writers import JsonWriter


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
