"""
Unit tests for the NotionWriter class.

This test suite mocks the `notion_client` to verify that the `NotionWriter`
class correctly formats data and makes the expected API calls without
requiring a live connection to the Notion API.
"""

import pytest
from unittest.mock import patch, MagicMock
import os

from schema_scribe.components.writers import NotionWriter
from schema_scribe.core.exceptions import ConfigError


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


@patch("schema_scribe.components.writers.notion_writer.Client")
def test_notion_writer_success(mock_notion_client, mock_db_catalog_data):
    """
    Tests the success path of the NotionWriter.

    It verifies that:
    - The Notion client is initialized with the correct API token.
    - The `pages.create` method is called exactly once.
    - The `parent` and `properties` (title) of the new page are correct.
    - The `children` (blocks) are generated and passed to the create call.
    """
    # 1. Setup Mock
    mock_client_instance = MagicMock()
    mock_client_instance.pages.create.return_value = {
        "id": "new-page-id",
        "url": "http://notion.so/new-page-id",
    }
    mock_notion_client.return_value = mock_client_instance

    # 2. Setup Writer and Args
    writer = NotionWriter()
    kwargs = {
        "api_token": "fake_token",
        "parent_page_id": "fake-parent-id",
        "project_name": "test_db",
    }

    # 3. Run
    writer.write(mock_db_catalog_data, **kwargs)

    # 4. Assert Client Initialization
    mock_notion_client.assert_called_once_with(auth="fake_token")

    # 5. Assert Page Creation
    expected_title = "Data Catalog - test_db"
    expected_parent = {"page_id": "fake-parent-id"}

    mock_client_instance.pages.create.assert_called_once()

    # Get the arguments passed to pages.create
    create_args, create_kwargs = mock_client_instance.pages.create.call_args

    assert create_kwargs["parent"] == expected_parent
    assert (
        create_kwargs["properties"]["title"][0]["text"]["content"]
        == expected_title
    )

    # Check that blocks were generated
    blocks = create_kwargs["children"]
    assert len(blocks) > 0
    assert (
        blocks[0]["type"] == "heading_2"
    )  # "🚀 Entity Relationship Diagram (ERD)"
    assert (
        blocks[0]["heading_2"]["rich_text"][0]["text"]["content"]
        == "🚀 Entity Relationship Diagram (ERD)"
    )
    assert blocks[1]["type"] == "code"  # "mermaid"
    assert blocks[2]["type"] == "heading_2"  # "🔎 Views"
    assert (
        blocks[2]["heading_2"]["rich_text"][0]["text"]["content"] == "🔎 Views"
    )
    # Find the table H2
    table_h2 = next(
        b
        for b in blocks
        if b.get("type") == "heading_2"
        and "Tables" in b["heading_2"]["rich_text"][0]["text"]["content"]
    )
    assert table_h2 is not None


@patch.dict(os.environ, {"NOTION_TEST_KEY": "env_key_value"})
@patch("schema_scribe.components.writers.notion_writer.Client")
def test_notion_writer_resolves_env_var(
    mock_notion_client, mock_db_catalog_data
):
    """
    Tests that the writer correctly resolves API tokens from environment variables.

    It checks if the `api_token` provided in the format `${VAR_NAME}` is
    replaced by the actual value of the environment variable when initializing
    the Notion client.
    """
    mock_notion_client.return_value = MagicMock()  # Basic mock

    writer = NotionWriter()
    kwargs = {
        "api_token": "${NOTION_TEST_KEY}",  # Reference the env var
        "parent_page_id": "fake-parent-id",
    }

    writer.write(mock_db_catalog_data, **kwargs)

    # Assert client was initialized with the *resolved* key
    mock_notion_client.assert_called_once_with(auth="${NOTION_TEST_KEY}")


def test_notion_writer_config_errors(mock_db_catalog_data):
    """
    Tests that NotionWriter raises ConfigError on missing configuration.

    It verifies that a `ConfigError` is raised if either `api_token` or
    `parent_page_id` is missing from the writer's parameters.
    """
    writer = NotionWriter()

    # 1. Missing api_token
    with pytest.raises(ConfigError, match="'api_token'.*is required"):
        writer.write(mock_db_catalog_data, parent_page_id="fake-parent-id")

    # 2. Missing parent_page_id
    with pytest.raises(ConfigError, match="'parent_page_id' is required"):
        writer.write(mock_db_catalog_data, api_token="fake_token")
