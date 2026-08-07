"""
Unit tests for the MarkdownWriter.
"""

import pytest

from schema_scribe.components.writers import MarkdownWriter
from schema_scribe.core.exceptions import WriterError


@pytest.fixture
def mock_db_catalog_data():
    """Provides a mock catalog data structure for standard DB connections."""
    return {
        "tables": [
            {
                "name": "users",
                "columns": [
                    {
                        "name": "id",
                        "type": "INTEGER",
                        "description": "User ID",
                        "is_pk": True,
                    },
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
                "source_table": "orders",
                "target_table": "users",
                "source_column": "user_id",
                "target_column": "id",
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
    assert '"orders" ||--o{ "users"' in content  # Mermaid erDiagram syntax
    assert "## 🔎 Views" in content
    assert "### 📄 View: `user_views`" in content
    assert "> A summary of the view." in content
    assert "## 🗂️ Tables" in content
    assert "### 📄 Table: `users`" in content
    assert "| 🔑 `id` | `INTEGER` | User ID |" in content
    assert "| `email` | `TEXT` | User email |" in content


def test_failed_write_preserves_previous_output(
    tmp_path, monkeypatch, mock_db_catalog_data
):
    """A mid-write failure must leave the previous output intact (atomic write)."""
    import os
    import builtins

    target = tmp_path / "catalog.md"
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

    writer = MarkdownWriter()
    with pytest.raises(WriterError):
        writer.write(
            mock_db_catalog_data,
            output_filename=str(target),
            db_profile_name="test_db",
        )

    assert target.read_text() == "PREVIOUS CONTENT"
