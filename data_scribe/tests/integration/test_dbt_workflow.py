"""
Integration tests for the 'dbt' command workflow.

This test suite verifies the end-to-end functionality of the DbtWorkflow,
ensuring that it can parse a dbt project, generate descriptions, and
correctly perform actions like updating YAML files or running CI checks.
"""

import pytest
import os
from pathlib import Path
from ruamel.yaml import YAML
import typer

from data_scribe.core.dbt_workflow import DbtWorkflow

# A minimal manifest.json structure needed for the tests
MINIMAL_MANIFEST = {
    "metadata": {},
    "nodes": {
        "model.jaffle_shop.customers": {
            "resource_type": "model",
            "path": "customers.sql",
            "original_file_path": "models/customers.sql",
            "name": "customers",
            "description": "",
            "columns": {
                "customer_id": {"name": "customer_id", "description": ""},
                "first_name": {"name": "first_name", "description": ""},
            },
        }
    },
}


@pytest.fixture
def dbt_project(tmp_path: Path):
    """Creates a minimal, temporary dbt project for testing."""
    project_dir = tmp_path / "dbt_project"
    models_dir = project_dir / "models"
    target_dir = project_dir / "target"
    models_dir.mkdir(parents=True)
    target_dir.mkdir(parents=True)

    # Create a dummy model file
    (models_dir / "customers.sql").write_text("select 1")

    # Create a dummy schema.yml
    schema_content = {
        "version": 2,
        "models": [{"name": "customers", "columns": [{"name": "customer_id"}]}],
    }
    yaml = YAML()
    with open(models_dir / "schema.yml", "w") as f:
        yaml.dump(schema_content, f)

    # Create a dummy manifest.json
    import json

    (target_dir / "manifest.json").write_text(json.dumps(MINIMAL_MANIFEST))

    return str(project_dir)


@pytest.fixture
def config_for_dbt(tmp_path: Path):
    """Creates a minimal config file for dbt tests."""
    config_path = tmp_path / "config.yml"
    config_content = """
default:
  llm: test_llm
llm_providers:
  test_llm:
    provider: "openai"
    model: "gpt-test"
"""
    config_path.write_text(config_content)
    return str(config_path)


def test_dbt_workflow_update(dbt_project, config_for_dbt, mock_llm_client):
    """Tests the dbt workflow with the --update flag."""
    # Act
    workflow = DbtWorkflow(
        dbt_project_dir=dbt_project,
        llm_profile="test_llm",
        config_path=config_for_dbt,
        output_profile=None,
        update_yaml=True,
        check=False,
    )
    workflow.run()

    # Assert
    schema_path = Path(dbt_project) / "models" / "schema.yml"
    assert schema_path.exists()

    yaml = YAML()
    with open(schema_path, "r") as f:
        data = yaml.load(f)

    model_def = data["models"][0]
    assert model_def["description"] == "This is an AI-generated description."
    assert (
        model_def["columns"][0]["description"]
        == "This is an AI-generated description."
    )
    # Check that the LLM was called for the model and its columns
    assert mock_llm_client.get_description.call_count > 0


def test_dbt_workflow_check_fails(dbt_project, config_for_dbt, mock_llm_client):
    """Tests the --check flag when documentation is missing and expects failure."""
    # Act & Assert
    workflow = DbtWorkflow(
        dbt_project_dir=dbt_project,
        llm_profile="test_llm",
        config_path=config_for_dbt,
        output_profile=None,
        update_yaml=False,
        check=True,
    )
    with pytest.raises(typer.Exit) as e:
        workflow.run()
    assert e.value.exit_code == 1


def test_dbt_workflow_check_succeeds(
    dbt_project, config_for_dbt, mock_llm_client
):
    """Tests the --check flag when documentation is already up-to-date."""
    # Arrange: First, update the YAML to be compliant.
    update_workflow = DbtWorkflow(
        dbt_project_dir=dbt_project,
        llm_profile="test_llm",
        config_path=config_for_dbt,
        output_profile=None,
        update_yaml=True,
        check=False,
    )
    update_workflow.run()

    # Act & Assert: Now, run the check and expect it to pass.
    check_workflow = DbtWorkflow(
        dbt_project_dir=dbt_project,
        llm_profile="test_llm",
        config_path=config_for_dbt,
        output_profile=None,
        update_yaml=False,
        check=True,
    )

    # This should run without raising an exception
    try:
        check_workflow.run()
    except typer.Exit as e:
        pytest.fail(
            f"--check mode failed unexpectedly with exit code {e.exit_code}"
        )
