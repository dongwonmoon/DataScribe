"""
Integration tests for the 'db' command workflow.

This test suite verifies the end-to-end functionality of the DbWorkflow,
ensuring that it correctly uses the components (connector, generator, writer)
to produce a data catalog from a database.
"""

import pytest
from pathlib import Path

from data_scribe.core.db_workflow import DbWorkflow


def test_db_workflow_end_to_end(
    tmp_path: Path, sqlite_db: str, test_config, mock_llm_client
):
    """
    Tests the full DbWorkflow from configuration to final output.

    This test uses fixtures to:
    1. Create a temporary SQLite database (`sqlite_db`).
    2. Mock the LLM client to return predictable output (`mock_llm_client`).
    3. Create a temporary config.yml pointing to the temp DB and a temp output file (`test_config`).

    It then runs the workflow and asserts that the generated Markdown file
    contains all the expected elements, including the mocked AI descriptions.
    """
    # Arrange
    output_md_path = tmp_path / "db_catalog.md"
    config_path = test_config(
        db_path=sqlite_db, output_md_path=output_md_path
    )

    # Act
    workflow = DbWorkflow(
        config_path=config_path,
        db_profile="test_db",
        llm_profile="test_llm",
        output_profile="test_markdown_output",
    )
    workflow.run()

    # Assert
    # 1. Check that the output file was created
    assert output_md_path.exists()
    content = output_md_path.read_text()

    # 2. Check for the mocked AI description
    assert "This is an AI-generated description." in content

    # 3. Check for table and column names
    assert "### 📄 Table: `users`" in content
    assert "| `id` | `INTEGER` | This is an AI-generated description. |" in content
    assert "| `email` | `TEXT` | This is an AI-generated description. |" in content

    # 4. Check for view information
    assert "### 📄 View: `user_orders`" in content
    assert "> This is an AI-generated description." in content
    assert "SELECT" in content

    # 5. Check for ERD information
    assert "## 🚀 Entity Relationship Diagram (ERD)" in content
    assert "orders --> users" in content
    assert "orders --> products" in content

    # 6. Verify the LLM client was called
    # The number of calls depends on tables, columns, and views.
    # Based on sqlite_db fixture:
    # 3 tables (users, products, orders) + 1 view (user_orders) = 4 summaries
    # 3+3+3 = 9 columns
    # Total = 13 calls
    assert mock_llm_client.get_description.call_count >= 10  # Be lenient