"""
Unit tests for the DbtYamlWriter.
"""

import pytest
import os
from ruamel.yaml import YAML
from unittest.mock import patch

from schema_scribe.components.writers import DbtYamlWriter
from schema_scribe.core.exceptions import WriterError


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
                "columns": [
                    {"name": "customer_id", "tests": ["unique", "not_null"]}
                ],
            }
        ],
    }
    yaml = YAML()
    with open(schema_yml_path, "w") as f:
        yaml.dump(initial_yaml_content, f)

    return str(tmp_path)


@pytest.fixture
def dbt_project_for_stub_tests(tmp_path):
    """
    Creates a complex dbt project for stub generation tests.
    It includes:
    1. models/customers.sql (documented in models/schema.yml)
    2. models/new_model.sql (undocumented, for testing 'append' logic)
    3. models/staging/stg_orders.sql (undocumented, for testing 'create new file' logic)
    """
    # 1. Create models/customers.sql and its schema.yml
    models_path = tmp_path / "models"
    models_path.mkdir(parents=True)

    (models_path / "customers.sql").touch()  # Empty .sql file

    schema_content_root = {
        "version": 2,
        "models": [
            {
                "name": "customers",
                "description": "This is an old description.",  # Will be updated
                "columns": [{"name": "customer_id"}],  # Missing description
            }
        ],
    }
    yaml = YAML()
    with open(models_path / "schema.yml", "w") as f:
        yaml.dump(schema_content_root, f)

    # 2. Create models/new_model.sql (in same dir, but undocumented)
    (models_path / "new_model.sql").touch()

    # 3. Create models/staging/stg_orders.sql (in new dir, undocumented)
    staging_path = models_path / "staging"
    staging_path.mkdir()
    (staging_path / "stg_orders.sql").touch()

    return str(tmp_path)


@pytest.fixture
def mock_catalog_for_stubs(dbt_project_for_stub_tests):
    """
    Provides mock AI catalog data corresponding to dbt_project_for_stub_tests.
    """
    base_path = dbt_project_for_stub_tests
    return {
        # 1. 'customers': Should trigger an UPDATE
        "customers": {
            "model_description": "AI desc for customers",
            "model_lineage_chart": "...",
            "original_file_path": os.path.join(
                base_path, "models", "customers.sql"
            ),
            "columns": [
                {
                    "name": "customer_id",
                    "type": "int",
                    "ai_generated": {"description": "AI desc for id"},
                }
            ],
        },
        # 2. 'new_model': Should trigger an APPEND to models/schema.yml
        "new_model": {
            "model_description": "AI desc for new_model",
            "model_lineage_chart": "...",
            "original_file_path": os.path.join(
                base_path, "models", "new_model.sql"
            ),
            "columns": [
                {
                    "name": "new_id",
                    "type": "int",
                    "ai_generated": {"description": "AI desc for new_id"},
                }
            ],
        },
        # 3. 'stg_orders': Should trigger a CREATE of models/staging/schema.yml
        "stg_orders": {
            "model_description": "AI desc for stg_orders",
            "model_lineage_chart": "...",
            "original_file_path": os.path.join(
                base_path, "models", "staging", "stg_orders.sql"
            ),
            "columns": [
                {
                    "name": "order_id",
                    "type": "int",
                    "ai_generated": {"description": "AI desc for order_id"},
                }
            ],
        },
    }


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
    catalog = {
        "customers": {"model_description": "A new description.", "columns": []}
    }
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
    catalog = {
        "customers": {"model_description": "A description.", "columns": []}
    }

    with pytest.raises(
        WriterError, match=f"Failed to parse YAML file: {schema_file}"
    ):
        writer.update_yaml_files(catalog)


def test_dbt_writer_stub_creation_update_mode(
    dbt_project_for_stub_tests, mock_catalog_for_stubs
):
    """
    Tests mode='update' for all 3 scenarios:
    1. 'customers' (existing model) gets its column description updated.
    2. 'new_model' (new model) gets APPENDED to 'models/schema.yml'.
    3. 'stg_orders' (new model) gets CREATED in 'models/staging/schema.yml'.
    """
    project_dir = dbt_project_for_stub_tests
    writer = DbtYamlWriter(dbt_project_dir=project_dir, mode="update")

    # Run the update
    writer.update_yaml_files(mock_catalog_for_stubs)

    yaml = YAML()

    # --- Assert 1 & 2: Check 'models/schema.yml' (Update + Append) ---
    root_schema_path = os.path.join(project_dir, "models", "schema.yml")
    assert os.path.exists(root_schema_path)

    with open(root_schema_path, "r") as f:
        root_data = yaml.load(f)

    assert (
        len(root_data["models"]) == 2
    ), "Should contain 'customers' and 'new_model'"

    # Assert 1 (Update)
    customers_def = root_data["models"][0]
    assert customers_def["name"] == "customers"
    assert (
        customers_def["description"] == "This is an old description."
    )  # Not overwritten
    assert (
        customers_def["columns"][0]["description"] == "AI desc for id"
    )  # Was added

    # Assert 2 (Append)
    new_model_def = root_data["models"][1]
    assert new_model_def["name"] == "new_model"
    assert new_model_def["description"] == "AI desc for new_model"
    assert new_model_def["columns"][0]["name"] == "new_id"
    assert new_model_def["columns"][0]["description"] == "AI desc for new_id"

    # --- Assert 3: Check 'models/staging/schema.yml' (Create) ---
    staging_schema_path = os.path.join(
        project_dir, "models", "staging", "schema.yml"
    )
    assert os.path.exists(staging_schema_path), "New schema.yml was not created"

    with open(staging_schema_path, "r") as f:
        staging_data = yaml.load(f)

    assert staging_data["version"] == 2
    assert len(staging_data["models"]) == 1

    stg_orders_def = staging_data["models"][0]
    assert stg_orders_def["name"] == "stg_orders"
    assert stg_orders_def["description"] == "AI desc for stg_orders"
    assert stg_orders_def["columns"][0]["description"] == "AI desc for order_id"


def test_dbt_writer_stub_creation_check_mode(
    dbt_project_for_stub_tests, mock_catalog_for_stubs
):
    """
    Tests mode='check'. It should detect all missing items (update, append, create)
    and return True, but should NOT write any files.
    """
    project_dir = dbt_project_for_stub_tests
    writer = DbtYamlWriter(dbt_project_dir=project_dir, mode="check")

    # Run the check
    updates_needed = writer.update_yaml_files(mock_catalog_for_stubs)

    # --- Assert 1: Changes are needed ---
    assert (
        updates_needed is True
    ), "Should detect missing descriptions and models"

    # --- Assert 2: No files were created ---
    staging_schema_path = os.path.join(
        project_dir, "models", "staging", "schema.yml"
    )
    assert not os.path.exists(
        staging_schema_path
    ), "File should not be created in check mode"

    # --- Assert 3: No files were modified ---
    yaml = YAML()
    root_schema_path = os.path.join(project_dir, "models", "schema.yml")
    with open(root_schema_path, "r") as f:
        root_data = yaml.load(f)

    # Check that 'customers' column is still missing description
    assert "description" not in root_data["models"][0]["columns"][0]
    # Check that 'new_model' was not appended
    assert len(root_data["models"]) == 1


@patch("schema_scribe.components.writers.dbt_yaml_writer.typer.prompt")
def test_dbt_writer_stub_creation_interactive_mode(
    mock_prompt, dbt_project_for_stub_tests, mock_catalog_for_stubs
):
    """
    Tests mode='interactive'. It should prompt for all 3 models.
    We simulate accepting all suggestions.
    """
    project_dir = dbt_project_for_stub_tests

    # Simulate user accepting all (Enter key)
    mock_prompt.side_effect = lambda prompt, default: default

    writer = DbtYamlWriter(dbt_project_dir=project_dir, mode="interactive")

    # Run the interactive update
    writer.update_yaml_files(mock_catalog_for_stubs)

    # --- Asserts ---
    # The assertions should be identical to the 'update' mode test,
    # as we accepted all suggestions.

    # Check 'models/schema.yml'
    yaml = YAML()
    root_schema_path = os.path.join(project_dir, "models", "schema.yml")
    with open(root_schema_path, "r") as f:
        root_data = yaml.load(f)
    assert len(root_data["models"]) == 2
    assert (
        root_data["models"][0]["columns"][0]["description"] == "AI desc for id"
    )
    assert root_data["models"][1]["name"] == "new_model"

    # Check 'models/staging/schema.yml'
    staging_schema_path = os.path.join(
        project_dir, "models", "staging", "schema.yml"
    )
    assert os.path.exists(staging_schema_path)
    with open(staging_schema_path, "r") as f:
        staging_data = yaml.load(f)
    assert staging_data["models"][0]["name"] == "stg_orders"

    # --- Assert 4: Check that all prompts were made ---
    # 1. customers.customer_id.description
    # 2. new_model.description
    # 3. new_model.new_id.description
    # 4. stg_orders.description
    # 5. stg_orders.order_id.description
    assert mock_prompt.call_count == 5, "Should prompt for all 5 missing fields"


@patch("schema_scribe.components.writers.dbt_yaml_writer.typer.prompt")
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


@patch("schema_scribe.components.writers.dbt_yaml_writer.typer.prompt")
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


@patch("schema_scribe.components.writers.dbt_yaml_writer.typer.prompt")
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
