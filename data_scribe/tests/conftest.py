"""
This module contains shared fixtures for pytest.

Fixtures defined here are accessible to all tests in the 'tests/' directory and its subdirectories.
This helps in creating a consistent testing setup and reducing code duplication.
"""

import pytest
import sqlite3
import yaml
from unittest.mock import MagicMock


@pytest.fixture
def mock_llm_client(mocker):
    """
    Mocks the LLM client initialization and returns a mock object.

    This fixture patches the `init_llm` function in both db_workflow and dbt_workflow
    modules to prevent actual LLM API calls during tests. The mock client's
    `get_description` method is set to return a predictable, fixed string.
    """
    mock_client = MagicMock()
    mock_client.get_description.return_value = (
        "This is an AI-generated description."
    )

    # Patch the init_llm function where it's used in the workflows
    mocker.patch(
        "data_scribe.core.db_workflow.init_llm", return_value=mock_client
    )
    mocker.patch(
        "data_scribe.core.dbt_workflow.init_llm", return_value=mock_client
    )

    return mock_client


@pytest.fixture
def test_config(tmp_path):
    """
    Creates a temporary, isolated config.yaml for testing.

    This fixture generates a config file in a temporary directory provided by pytest's
    `tmp_path` fixture. This ensures that tests do not interfere with each other or
    with a real user config file.

    Returns:
        A function that can be called to get a config path with custom content.
    """

    def _create_config(db_path, output_md_path=None, output_json_path=None):
        config_path = tmp_path / "config.yml"
        config = {
            "default": {"db": "test_db", "llm": "test_llm"},
            "db_connections": {
                "test_db": {"type": "sqlite", "path": str(db_path)},
                "duckdb_test": {
                    "type": "duckdb",
                    "path": "s3://bucket/some.csv",
                },
            },
            "llm_providers": {
                "test_llm": {"provider": "openai", "model": "gpt-4"},
            },
            "output_profiles": {},
        }
        if output_md_path:
            config["output_profiles"]["test_markdown_output"] = {
                "type": "markdown",
                "output_filename": str(output_md_path),
            }
        if output_json_path:
            config["output_profiles"]["test_json_output"] = {
                "type": "json",
                "output_filename": str(output_json_path),
            }

        with open(config_path, "w") as f:
            yaml.dump(config, f)
        return str(config_path)

    return _create_config


@pytest.fixture
def sqlite_db(tmp_path):
    """
    Sets up a temporary SQLite database with sample data for integration tests.

    This fixture creates a new SQLite database file in a temporary directory for each
    test function. It populates the database with tables, columns, views, and foreign keys
    to enable realistic integration testing of the database scanning workflow.

    The database file is automatically removed after the test completes.

    Returns:
        The path to the temporary SQLite database file.
    """
    db_path = tmp_path / "test_database.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create tables
    cursor.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)"
    )
    cursor.execute(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)"
    )
    cursor.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product_id INTEGER, "
        "FOREIGN KEY(user_id) REFERENCES users(id), "
        "FOREIGN KEY(product_id) REFERENCES products(id))"
    )

    # Create a view
    cursor.execute(
        """
        CREATE VIEW user_orders AS
        SELECT u.name as user_name, p.name as product_name
        FROM orders o
        JOIN users u ON o.user_id = u.id
        JOIN products p ON o.product_id = p.id
        """
    )

    conn.commit()
    conn.close()

    return str(db_path)
