"""
Unit tests for the writers.

This test suite verifies that the writer classes in `data_scribe.components.writers`
correctly format the catalog data and write it to the intended output (e.g., file, API).
File I/O and external API calls are mocked or handled using temporary files.
"""

import pytest
import json
import typer
from ruamel.yaml import YAML
from unittest.mock import patch, MagicMock

from data_scribe.components.writers import (
    MarkdownWriter,
    JsonWriter,
    DbtYamlWriter,
    ConfluenceWriter,
)
from data_scribe.core.exceptions import WriterError


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


# --- MarkdownWriter Tests ---


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
    # assert "orders --> users" in content  # Mermaid syntax <-- [OLD] Fails
    assert (
        "orders ||--o{ users" in content
    )  # Mermaid erDiagram syntax <-- [NEW] Correct
    assert "## 🔎 Views" in content
    assert "### 📄 View: `user_views`" in content
    assert "> A summary of the view." in content
    assert "## 🗂️ Tables" in content
    assert "### 📄 Table: `users`" in content
    assert "| `id` | `INTEGER` | User ID |" in content
    assert "| `email` | `TEXT` | User email |" in content


# --- JsonWriter Tests ---


def test_json_writer_write(tmp_path, mock_db_catalog_data):
    """Tests that JsonWriter correctly writes a catalog to a .json file."""
    output_file = tmp_path / "catalog.json"
    writer = JsonWriter()
    writer.write(mock_db_catalog_data, output_filename=str(output_file))

    assert output_file.exists()
    with open(output_file, "r") as f:
        data = json.load(f)

    assert data == mock_db_catalog_data


# --- DbtYamlWriter Tests ---


@pytest.fixture
def dbt_project(tmp_path):
    """Creates a temporary dbt project structure with a schema.yml file."""
    models_path = tmp_path / "models"
    models_path.mkdir()
    schema_yml_path = models_path / "schema.yml"

    initial_yaml_content = {
        "version": 2,
        "models": [
            {
                "name": "customers",
                "columns": [{"name": "customer_id", "tests": ["unique", "not_null"]}],
            }
        ],
    }
    yaml = YAML()
    with open(schema_yml_path, "w") as f:
        yaml.dump(initial_yaml_content, f)

    return str(tmp_path)


def test_dbt_yaml_writer_update(dbt_project):
    """Tests that DbtYamlWriter correctly updates a schema.yml file."""
    writer = DbtYamlWriter(dbt_project_dir=dbt_project)
    catalog_to_update = {
        "customers": {
            "columns": [
                {
                    "name": "customer_id",
                    "ai_generated": {
                        "description": "This is a new column description."
                    },
                }
            ],
            "model_description": "This is a new model description.",
        }
    }

    writer.update_yaml_files(catalog_to_update)

    yaml = YAML()
    with open(f"{dbt_project}/models/schema.yml", "r") as f:
        updated_data = yaml.load(f)

    model_def = updated_data["models"][0]
    col_def = model_def["columns"][0]

    assert model_def["description"] == "This is a new model description."
    assert col_def["description"] == "This is a new column description."
    assert "unique" in col_def["tests"]  # Ensures existing keys are preserved


def test_dbt_yaml_writer_check_mode_no_changes(dbt_project):
    """Tests check mode when no changes are needed."""
    writer = DbtYamlWriter(dbt_project_dir=dbt_project, mode="check")
    # Catalog data matches the existing schema.yml (no descriptions to add)
    catalog = {
        "customers": {
            "model_description": None,
            "columns": [{"name": "customer_id", "ai_generated": {}}],
        }
    }
    updates_needed = writer.update_yaml_files(catalog)
    assert not updates_needed


def test_dbt_yaml_writer_check_mode_changes_needed(dbt_project):
    """Tests check mode when changes are needed."""
    writer = DbtYamlWriter(dbt_project_dir=dbt_project, mode="check")
    # Catalog data has new descriptions to add
    catalog = {"customers": {"model_description": "A new description.", "columns": []}}
    updates_needed = writer.update_yaml_files(catalog)
    assert updates_needed


def test_dbt_yaml_writer_malformed_yaml(dbt_project):
    """Tests that WriterError is raised for a malformed schema.yml."""
    # Overwrite the existing schema.yml with invalid content
    schema_file = f"{dbt_project}/models/schema.yml"
    with open(schema_file, "w") as f:
        f.write(
            "models: - name: customers\n  - name: orders"
        )  # Invalid YAML indentation

    writer = DbtYamlWriter(dbt_project_dir=dbt_project)
    catalog = {"customers": {"model_description": "A description.", "columns": []}}

    with pytest.raises(WriterError, match=f"Failed to parse YAML file: {schema_file}"):
        writer.update_yaml_files(catalog)


# --- ConfluenceWriter Tests ---


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
    mock_confluence_constructor, mock_dbt_catalog_data
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


@patch("data_scribe.components.writers.dbt_yaml_writer.typer.prompt")
def test_dbt_yaml_writer_interactive_accept(mock_prompt, dbt_project):
    """
    Tests that interactive mode correctly ADDS a description when the user
    accepts the AI suggestion (by pressing Enter).
    """
    # Simulate the user accepting the default.
    # The lambda function simulates typer.prompt returning the 'default' value.
    mock_prompt.side_effect = lambda prompt, default: default

    writer = DbtYamlWriter(dbt_project_dir=dbt_project, mode="interactive")
    catalog_to_update = {
        "customers": {
            "columns": [],
            "model_description": "AI model description",
        }
    }

    writer.update_yaml_files(catalog_to_update)

    # Verify the file was updated with the AI's description
    yaml = YAML()
    with open(f"{dbt_project}/models/schema.yml", "r") as f:
        updated_data = yaml.load(f)

    model_def = updated_data["models"][0]
    assert model_def["description"] == "AI model description"
    mock_prompt.assert_called_once()  # Ensure the user was actually prompted


@patch("data_scribe.components.writers.dbt_yaml_writer.typer.prompt")
def test_dbt_yaml_writer_interactive_edit(mock_prompt, dbt_project):
    """
    Tests that interactive mode correctly ADDS an EDITED description when
    the user types a new value.
    """
    # Simulate the user typing a new, custom value
    mock_prompt.return_value = "User edited description"

    writer = DbtYamlWriter(dbt_project_dir=dbt_project, mode="interactive")
    catalog_to_update = {
        "customers": {
            "columns": [],
            "model_description": "AI model description",  # This is the AI default
        }
    }

    writer.update_yaml_files(catalog_to_update)

    # Verify the file was updated with the USER'S description
    yaml = YAML()
    with open(f"{dbt_project}/models/schema.yml", "r") as f:
        updated_data = yaml.load(f)

    model_def = updated_data["models"][0]
    assert model_def["description"] == "User edited description"
    mock_prompt.assert_called_once()


@patch("data_scribe.components.writers.dbt_yaml_writer.typer.prompt")
def test_dbt_yaml_writer_interactive_skip(mock_prompt, dbt_project):
    """
    Tests that interactive mode does NOT add a description if the user skips (inputs 's').
    """
    # Simulate the user pressing 's'
    mock_prompt.return_value = "s"

    writer = DbtYamlWriter(dbt_project_dir=dbt_project, mode="interactive")
    catalog_to_update = {
        "customers": {
            "columns": [],
            "model_description": "AI model description",
        }
    }

    # Check initial state (no description)
    yaml = YAML()
    with open(f"{dbt_project}/models/schema.yml", "r") as f:
        initial_data = yaml.load(f)
    assert "description" not in initial_data["models"][0]

    writer.update_yaml_files(catalog_to_update)

    # Verify the file is unchanged
    with open(f"{dbt_project}/models/schema.yml", "r") as f:
        updated_data = yaml.load(f)

    model_def = updated_data["models"][0]
    assert "description" not in model_def  # Still no description
    mock_prompt.assert_called_once()
