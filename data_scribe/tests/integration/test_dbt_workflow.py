"""
Integration tests for the 'dbt' command workflow.

This test suite verifies the end-to-end functionality of the DbtWorkflow,
ensuring that it can parse a dbt project, generate descriptions, and
correctly perform actions like updating YAML files or running CI checks.
"""

import pytest
import os
from pathlib import Path
from typing import List, Dict, Any
from ruamel.yaml import YAML
import typer
from unittest.mock import MagicMock, patch

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

DRIFT_MANIFEST = {
    "metadata": {},
    "nodes": {
        "model.jaffle_shop.customers": {
            "resource_type": "model",
            "path": "customers.sql",
            "original_file_path": "models/customers.sql",
            "name": "customers",
            "description": "This is an old description.", # Existing doc
            "columns": {
                "customer_id": {
                    "name": "customer_id",
                    "description": "Unique ID for the customer", # Existing doc
                    "data_type": "int"
                },
            },
            "raw_sql": "SELECT * FROM customers",
        }
    },
}

@pytest.fixture
def mock_parsed_models_list() -> List[Dict[str, Any]]:
    """
    Provides a mock of the parsed models list from DbtManifestParser.

    This simulates the *output* of the `DbtManifestParser.models` property,
    transforming the raw manifest dictionary into the list of dictionaries
    that the catalog generator expects. This makes the test more robust by

    decoupling it from the parser's internal logic.
    """
    node_data = DRIFT_MANIFEST["nodes"]["model.jaffle_shop.customers"]
    
    parsed_columns = [] # This will be a LIST
    for col_name, col_data in node_data.get("columns", {}).items():
        parsed_columns.append(
            {
                "name": col_name,
                "description": col_data.get("description", ""),
                "type": col_data.get("data_type", "N/A"),
            }
        )

    return [
        {
            "name": node_data.get("name"),
            "unique_id": "model.jaffle_shop.customers",
            "description": node_data.get("description", ""),
            "raw_sql": node_data.get("raw_code", ""),
            "columns": parsed_columns,
            "path": node_data.get("path"),
            "original_file_path": node_data.get("original_file_path"),
        }
    ]


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
    """
    Creates a minimal config file for dbt tests.

    Includes a dummy 'db_connections' section, which is required for the
    `--drift` mode to successfully load its configuration, even though the
    connector itself is mocked.
    """
    config_path = tmp_path / "config.yml"
    config_content = """
default:
  llm: test_llm
  
llm_providers:
  test_llm:
    provider: "openai"
    model: "gpt-test"

db_connections:
  test_db:
    type: "postgres" # Type doesn't matter, it will be mocked
    user: "test"
"""
    config_path.write_text(config_content)
    return str(config_path)

@pytest.fixture
def mock_db_connector(mocker):
    """
    Mocks the get_db_connector factory function in the dbt_workflow module.

    This prevents a real database connection and allows tests to control the
    output of `get_column_profile` to simulate different data scenarios.
    """
    mock_connector = MagicMock()
    # Configure the mock connector to return predictable profile stats
    mock_connector.get_column_profile.return_value = {
        "total_count": 100,
        "null_ratio": 0.0,
        "distinct_count": 100,
        "is_unique": True, # This matches "Unique ID"
    }
    
    # Patch the factory function
    mocker.patch(
        "data_scribe.core.dbt_workflow.get_db_connector",
        return_value=mock_connector
    )
    return mock_connector

@pytest.fixture
def dbt_project_with_drift_docs(tmp_path: Path):
    """
    Creates a dbt project with pre-existing documentation.

    This fixture sets up a `schema.yml` file that has descriptions,
    matching the `DRIFT_MANIFEST` mock data. This is the necessary
    precondition for running a drift check.
    """
    project_dir = tmp_path / "dbt_project"
    models_dir = project_dir / "models"
    models_dir.mkdir(parents=True)
    
    # Create a dummy model file
    (models_dir / "customers.sql").write_text("select 1")

    # Create a schema.yml that *has* the descriptions
    schema_content = {
        "version": 2,
        "models": [
            {
                "name": "customers",
                "description": "This is an old description.", # <--- MATCHES MANIFEST
                "columns": [
                    {
                        "name": "customer_id",
                        "description": "Unique ID for the customer" # <--- MATCHES MANIFEST
                    }
                ],
            }
        ],
    }
    yaml = YAML()
    with open(models_dir / "schema.yml", "w") as f:
        yaml.dump(schema_content, f)

    return str(project_dir)


def test_dbt_workflow_update(dbt_project, config_for_dbt, mock_llm_client):
    """Tests the dbt workflow with the --update flag."""
    # Act
    workflow = DbtWorkflow(
        dbt_project_dir=dbt_project,
        llm_profile="test_llm",
        db_profile=None,
        config_path=config_for_dbt,
        output_profile=None,
        update_yaml=True,
        check=False,
        interactive=False,
        drift=False
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
        db_profile=None,
        config_path=config_for_dbt,
        output_profile=None,
        update_yaml=False,
        check=True,
        interactive=False,
        drift=False
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
        db_profile=None,
        config_path=config_for_dbt,
        output_profile=None,
        update_yaml=True,
        check=False,
        interactive=False,
        drift=False
    )
    update_workflow.run()

    # Act & Assert: Now, run the check and expect it to pass.
    check_workflow = DbtWorkflow(
        dbt_project_dir=dbt_project,
        llm_profile="test_llm",
        db_profile=None,
        config_path=config_for_dbt,
        output_profile=None,
        update_yaml=False,
        check=True,
        interactive=False,
        drift=False
    )

    # This should run without raising an exception
    try:
        check_workflow.run()
    except typer.Exit as e:
        pytest.fail(
            f"--check mode failed unexpectedly with exit code {e.exit_code}"
        )

@patch(
    "data_scribe.core.dbt_catalog_generator.DbtManifestParser",
)
def test_dbt_workflow_drift_mode_no_drift(
    mock_parser_constructor,
    dbt_project_with_drift_docs,
    config_for_dbt,
    mock_llm_client,
    mock_db_connector,
    mock_parsed_models_list
):
    """
    Tests the --drift mode when documentation MATCHES the live data.

    It mocks the DB connector to return a data profile that is consistent
    with the existing documentation. It also mocks the LLM to return "MATCH".
    The test asserts that the workflow runs and exits successfully (code 0).
    """
    # 1. Arrange
    # Configure parser to return the manifest with existing docs
    mock_parser_instance = MagicMock()
    mock_parser_instance.models = mock_parsed_models_list
    mock_parser_constructor.return_value = mock_parser_instance
    
    # Configure LLM to return "MATCH" for the auditor prompt
    mock_llm_client.get_description.side_effect = lambda p, max_tokens: "MATCH" if "Auditor" in p else "AI Desc"
    
    # 2. Act
    workflow = DbtWorkflow(
        dbt_project_dir=dbt_project_with_drift_docs,
        db_profile="test_db", # Required for drift
        llm_profile="test_llm",
        config_path=config_for_dbt,
        output_profile=None,
        update_yaml=False,
        check=False,
        interactive=False,
        drift=True, # <--- Enable drift mode
    )

    # 3. Assert
    try:
        workflow.run()
    except typer.Exit as e:
        pytest.fail(
            f"--drift mode failed unexpectedly with exit code {e.exit_code}"
        )

    # Verify that the LLM was called with the drift check prompt
    drift_prompt_call = next(
        call for call in mock_llm_client.get_description.call_args_list
        if "Auditor" in call[0][0]
    )
    assert drift_prompt_call is not None
    assert "Unique ID for the customer" in drift_prompt_call[0][0] # Existing doc
    assert "Is Unique: True" in drift_prompt_call[0][0] # Profile stat


@patch(
    "data_scribe.core.dbt_catalog_generator.DbtManifestParser",
)
def test_dbt_workflow_drift_mode_drift_detected(
    mock_parser_constructor,
    dbt_project_with_drift_docs,
    config_for_dbt,
    mock_llm_client,
    mock_db_connector,
    mock_parsed_models_list
):
    """
    Tests the --drift mode when documentation conflicts with the live data.

    It mocks the DB connector to return a data profile that is inconsistent
    with the existing documentation (e.g., is_unique=False). It also mocks
    the LLM to return "DRIFT". The test asserts that the workflow fails
    with a typer.Exit(code=1).
    """
    # 1. Arrange
    # Configure parser
    mock_parser_instance = MagicMock()
    mock_parser_instance.models = mock_parsed_models_list
    mock_parser_constructor.return_value = mock_parser_instance
    
    # Configure DB profile to return CONFLICTING data
    mock_db_connector.get_column_profile.return_value = {
        "total_count": 100,
        "null_ratio": 0.5,
        "distinct_count": 10,
        "is_unique": False, # <--- CONFLICTS with "Unique ID"
    }

    # Configure LLM to see the conflict and return "DRIFT"
    mock_llm_client.get_description.side_effect = lambda p, max_tokens: "DRIFT" if "Auditor" in p else "AI Desc"

    # 2. Act
    workflow = DbtWorkflow(
        dbt_project_dir=dbt_project_with_drift_docs,
        db_profile="test_db",
        llm_profile="test_llm",
        config_path=config_for_dbt,
        output_profile=None,
        update_yaml=False,
        check=False,
        interactive=False,
        drift=True,
    )

    # 3. Assert
    with pytest.raises(typer.Exit) as e:
        workflow.run()
    
    # Check that it exited with the CI failure code
    assert e.value.exit_code == 1

    # Verify that the LLM was called with the drift check prompt
    drift_prompt_call = next(
        call for call in mock_llm_client.get_description.call_args_list
        if "Auditor" in call[0][0]
    )
    assert drift_prompt_call is not None
    assert "Unique ID for the customer" in drift_prompt_call[0][0] # Existing doc
    assert "Is Unique: False" in drift_prompt_call[0][0] # Conflicting stat