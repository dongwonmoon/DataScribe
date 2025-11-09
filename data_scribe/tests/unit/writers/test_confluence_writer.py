"""
Unit tests for the ConfluenceWriter.
"""
import pytest
from unittest.mock import patch, MagicMock

from data_scribe.components.writers import ConfluenceWriter


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


@pytest.fixture
def mock_dbt_catalog_data():
    """Provides a mock catalog data structure for dbt projects."""
    return {
        "customers": {
            "model_description": "This model represents customer data.",
            "model_lineage_chart": "```mermaid\ngraph TD;\n  A-->B;\n```",
            "columns": [
                {
                    "name": "customer_id",
                    "type": "int",
                    "ai_generated": {"description": "Primary key for customers."},
                }
            ],
        }
    }


@patch("data_scribe.components.writers.confluence_writer.Confluence")
def test_confluence_writer_db_write(mock_confluence_constructor, mock_db_catalog_data):
    """Tests that ConfluenceWriter correctly handles standard DB catalog data."""
    mock_confluence_instance = MagicMock()
    mock_confluence_instance.get_page_id.return_value = "123456"  # Simulate page exists
    mock_confluence_constructor.return_value = mock_confluence_instance

    writer = ConfluenceWriter()
    writer.write(
        mock_db_catalog_data,
        url="https://test.atlassian.net",
        username="user",
        api_token="token",
        space_key="SPACE",
        parent_page_id="12345",
        db_profile_name="test_db_profile",
    )

    mock_confluence_instance.update_page.assert_called_once()
    call_args, call_kwargs = mock_confluence_instance.update_page.call_args
    assert call_kwargs["page_id"] == "123456"
    body = call_kwargs["body"]
    assert "<h1>📁 Data Catalog for test_db_profile</h1>" in body
    assert "<h2>🚀 Entity Relationship Diagram (ERD)</h2>" in body


@patch("data_scribe.components.writers.confluence_writer.Confluence")
def test_confluence_writer_dbt_write(
    mock_confluence_constructor,
    mock_dbt_catalog_data,
):
    """Tests that ConfluenceWriter correctly handles dbt catalog data."""
    mock_confluence_instance = MagicMock()
    mock_confluence_instance.get_page_id.return_value = "789012"  # Simulate page exists
    mock_confluence_constructor.return_value = mock_confluence_instance

    writer = ConfluenceWriter()
    writer.write(
        mock_dbt_catalog_data,
        url="https://test.atlassian.net",
        username="user",
        api_token="token",
        space_key="SPACE",
        parent_page_id="12345",
        project_name="test_dbt_project",
    )

    mock_confluence_instance.update_page.assert_called_once()
    call_args, call_kwargs = mock_confluence_instance.update_page.call_args
    assert call_kwargs["page_id"] == "789012"
    body = call_kwargs["body"]
    assert "<h1>🧬 Data Catalog for test_dbt_project (dbt)</h1>" in body
    assert "<h2>🚀 Model: <code>customers</code></h2>" in body
