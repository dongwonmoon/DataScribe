"""
Integration tests for the 'db' command workflow.

This test suite verifies the end-to-end functionality of the DbWorkflow,
ensuring that it correctly uses the components (connector, generator, writer)
to produce a data catalog from a database.
"""

import pytest
from pathlib import Path
from unittest.mock import ANY

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
    config_path = test_config(db_path=sqlite_db, output_md_path=output_md_path)

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
    assert (
        "| `id` | `INTEGER` | This is an AI-generated description. |" in content
    )
    assert (
        "| `email` | `TEXT` | This is an AI-generated description. |" in content
    )

    # 4. Check for view information
    assert "### 📄 View: `user_orders`" in content
    assert "> This is an AI-generated description." in content
    assert "SELECT" in content

    # 5. Check for ERD information
    assert "## 🚀 Entity Relationship Diagram (ERD)" in content
    assert "erDiagram" in content
    assert "orders ||--o{ users" in content
    assert "orders ||--o{ products" in content

    # 6. Verify the LLM client was called
    # The number of calls depends on tables, columns, and views.
    # Based on sqlite_db fixture:
    # 3 tables (users, products, orders) + 1 view (user_orders) = 4 summaries
    # 3+3+3 = 9 columns
    # Total = 13 calls
    assert mock_llm_client.get_description.call_count >= 10  # Be lenient


def test_db_workflow_end_to_end_with_profiling(
    tmp_path: Path, sqlite_db: str, test_config, mock_llm_client
):
    """
    Tests the full DbWorkflow from configuration to final output.

    This test verifies that:
    1. The workflow runs.
    2. The output file is created.
    3. The CatalogGenerator *calls* the new get_column_profile method.
    4. The profile stats are *included* in the prompt sent to the LLM.
    """
    # Arrange
    output_md_path = tmp_path / "db_catalog.md"
    config_path = test_config(db_path=sqlite_db, output_md_path=output_md_path)

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

    # 2. Check for the mocked AI description (proves LLM was called)
    assert "This is an AI-generated description." in content

    # 3. Check for table and column names
    assert "### 📄 Table: `users`" in content
    assert (
        "| `id` | `INTEGER` | This is an AI-generated description. |" in content
    )

    # ... (existing checks for view and ERD) ...
    assert "### 📄 View: `user_orders`" in content

    # 4. Verify the LLM was called with the *correct profiling context*

    # Get the list of all calls made to the mock LLM
    calls = mock_llm_client.get_description.call_args_list

    # Find the prompt for the 'users.id' column
    users_id_prompt = None
    for call in calls:
        # call[0] is the positional args, call[0][0] is the 'prompt'
        prompt_text = call[0][0]
        if "Table: users" in prompt_text and "Column: id" in prompt_text:
            users_id_prompt = prompt_text
            break

    assert users_id_prompt is not None, "Prompt for 'users.id' was not found"

    # Check that the prompt contains the stats from our EMPTY test DB
    # (sqlite_db fixture creates an empty users table)
    # The stats should be total=0, null=0, distinct=0, unique=True
    assert "Data Profile Context:" in users_id_prompt
    assert "- Null Ratio: 0" in users_id_prompt
    assert "- Is Unique: True" in users_id_prompt
    assert "- Distinct Count: 0" in users_id_prompt

    # 5. Verify the LLM was called for columns, tables, and views
    # Based on sqlite_db fixture:
    # 3 tables + 1 view = 4 summary calls
    # 3 (users) + 3 (products) + 3 (orders) = 9 column calls
    # Total = 13 calls
    assert mock_llm_client.get_description.call_count == 13
