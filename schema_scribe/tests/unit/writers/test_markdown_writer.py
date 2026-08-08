"""
Unit tests for the MarkdownWriter.
"""

import pytest

from schema_scribe.components.writers import MarkdownWriter
from schema_scribe.core.exceptions import ConfigError, WriterError


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
    assert '"users" ||--o{ "orders"' in content  # parent (FK target) on the left, one-to-many
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


def _synthetic_landscape():
    """A minimal landscape dict in the build_landscape contract shape."""
    return {
        "scale": {
            "tables": 4,
            "columns": 6,
            "column_types": {"INTEGER": 4, "TEXT": 2},
        },
        "clusters": {
            "dim": ["dim_customer", "dim_product"],
            "other": ["users", "events"],
        },
        "core_tables": [
            {"table": "orders", "degree": 3},
            {"table": "users", "degree": 1},
        ],
        "relationship_map": {
            "nodes": ["dim_customer", "dim_product", "events", "orders", "users"],
            "edges": [
                {
                    "source": "orders",
                    "source_column": "user_id",
                    "target": "users",
                    "target_column": "id",
                }
            ],
        },
    }


def test_landscape_render_includes_all_sections():
    """The landscape render produces Scale, Clusters, Core tables, and Relationship map."""
    writer = MarkdownWriter()
    out = writer.render_landscape(_synthetic_landscape(), db_profile_name="test_db")

    assert "# 🏞️ Landscape Report for test_db" in out
    assert "## Scale" in out
    assert "## Clusters" in out
    assert "## Core tables" in out
    assert "## Relationship map" in out
    assert "| Tables | 4 |" in out
    assert "| Columns | 6 |" in out
    assert "| Column types | INTEGER (4), TEXT (2) |" in out
    assert "### `dim` (2 tables)" in out
    assert "- `dim_customer`" in out
    assert "| 1 | `orders` | 3 |" in out
    assert "Nodes (5):" in out
    assert "| `orders` | `user_id` | `users` | `id` |" in out


def test_landscape_render_includes_hints():
    """Cluster and core-table hints render inline when provided."""
    hints = {
        "clusters": {"dim": "Dimension tables."},
        "core_tables": {"orders": "Order header records."},
    }
    out = MarkdownWriter().render_landscape(
        _synthetic_landscape(), hints=hints, db_profile_name="test_db"
    )

    assert (
        "> **Hint (AI-generated draft — unverified):** Dimension tables." in out
    )
    assert "**Hints (AI-generated drafts — unverified):**" in out
    assert "`orders` — Order header records." in out


def test_landscape_hints_labeled_as_unverified_ai_drafts():
    """
    Both hint forms must carry explicit draft/unverified framing: hints
    are AI-generated drafts until a human accepts them (PRODUCT.md trust
    boundary), never verified business facts.
    """
    hints = {
        "clusters": {"dim": "Dimension tables."},
        "core_tables": {"orders": "Order header records."},
    }
    out = MarkdownWriter().render_landscape(
        _synthetic_landscape(), hints=hints, db_profile_name="test_db"
    )

    assert "AI-generated draft" in out
    assert "unverified" in out
    assert "> **Hint (AI-generated draft — unverified):** Dimension tables." in out
    assert "**Hints (AI-generated drafts — unverified):**" in out
    assert "- `orders` — Order header records." in out


def test_landscape_render_without_hints_omits_hint_sections():
    """No hints passed, no hint lines rendered."""
    out = MarkdownWriter().render_landscape(_synthetic_landscape(), db_profile_name="t")
    assert "**Hint:**" not in out
    assert "Hints:" not in out
    assert "AI-generated draft" not in out
    assert "unverified" not in out


def test_landscape_render_empty_database():
    """Empty landscape renders every section with an explicit empty state."""
    empty = {
        "scale": {"tables": 0, "columns": 0, "column_types": {}},
        "clusters": {"other": []},
        "core_tables": [],
        "relationship_map": {"nodes": [], "edges": []},
    }
    out = MarkdownWriter().render_landscape(empty, db_profile_name="t")
    assert "| Tables | 0 |" in out
    assert "No tables found" in out
    assert "No core tables found" in out
    assert "No edges (no foreign keys)" in out


def test_landscape_render_requires_db_profile_name():
    with pytest.raises(ConfigError):
        MarkdownWriter().render_landscape(_synthetic_landscape())


def test_landscape_write_writes_file(tmp_path):
    output = tmp_path / "landscape.md"
    MarkdownWriter().write_landscape(
        _synthetic_landscape(),
        output_filename=str(output),
        db_profile_name="test_db",
    )
    assert output.exists()
    content = output.read_text()
    assert "## Core tables" in content
    assert "## Relationship map" in content


def test_landscape_write_requires_output_filename(tmp_path):
    with pytest.raises(ConfigError):
        MarkdownWriter().write_landscape(
            _synthetic_landscape(), db_profile_name="test_db"
        )
