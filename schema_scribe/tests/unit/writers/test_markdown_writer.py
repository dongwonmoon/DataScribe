"""
Unit tests for the MarkdownWriter.
"""

import pytest

from schema_scribe.components.writers import MarkdownWriter


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


def test_markdown_writer_write(tmp_path, mock_db_catalog_data):
    """Tests that MarkdownWriter correctly writes a catalog to a .md file."""
    output_file = tmp_path / "catalog.md"
    writer = MarkdownWriter()
    writer.write(
        mock_db_catalog_data,
        output_filename=str(output_file),
        db_profile_name="test_db",
    )

    assert output_file.exists()
    content = output_file.read_text()

    assert "# 📁 Data Catalog for test_db" in content
    assert "## 🚀 Entity Relationship Diagram (ERD)" in content
    assert "orders ||--o{ users" in content  # Mermaid erDiagram syntax
    assert "## 🔎 Views" in content
    assert "### 📄 View: `user_views`" in content
    assert "> A summary of the view." in content
    assert "## 🗂️ Tables" in content
    assert "### 📄 Table: `users`" in content
    assert "| `id` | `INTEGER` | User ID |" in content
    assert "| `email` | `TEXT` | User email |" in content
