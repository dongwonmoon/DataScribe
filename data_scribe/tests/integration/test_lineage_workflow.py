"""
Integration test for the 'lineage' command workflow.
"""

import pytest
from unittest.mock import patch, MagicMock
from ruamel.yaml import YAML

# Import the class we are testing
from data_scribe.core.lineage_workflow import LineageWorkflow


@pytest.fixture
def lineage_config(tmp_path):
    """
    Creates a minimal config.yaml in a temporary path for lineage tests.
    """
    config_path = tmp_path / "config.yml"
    config_content = """
db_connections:
  test_db:
    type: "postgres" # Type doesn't matter, get_db_connector will be mocked
output_profiles:
  test_output:
    type: "mermaid"
    output_filename: "lineage.md"
"""
    config_path.write_text(config_content)
    return str(config_path)


@patch("data_scribe.core.lineage_workflow.get_writer")
@patch("data_scribe.core.lineage_workflow.DbtManifestParser")
@patch("data_scribe.core.lineage_workflow.get_db_connector")
def test_lineage_workflow_e2e(
    mock_get_connector: MagicMock,
    mock_parser_constructor: MagicMock,
    mock_get_writer: MagicMock,
    lineage_config: str,
    tmp_path: str,
):
    """
    Tests the end-to-end lineage workflow by mocking all external
    dependencies (DB, Parser, Writer).

    Validates that:
    1. The correct components are called.
    2. Physical (FK) and Logical (dbt) dependencies are merged
       into a single Mermaid graph.
    """
    # 1. ARRANGE

    # --- Mock DB Connector (Physical Lineage) ---
    mock_db = MagicMock()
    mock_db.get_foreign_keys.return_value = [
        {
            "from_table": "stg_orders",  # This is also a dbt model
            "from_column": "order_id",
            "to_table": "raw_orders",  # This is just a DB table
            "to_column": "id",
        }
    ]
    mock_get_connector.return_value = mock_db

    # --- Mock dbt Parser (Logical Lineage) ---
    mock_dbt_models = [
        {
            "name": "fct_orders",
            # Depends on two models
            "dependencies": ["stg_orders", "stg_customers"],
        },
        {
            "name": "stg_orders",
            # Depends on a dbt source
            "dependencies": ["jaffle_shop.raw_orders"],
        },
        {
            "name": "stg_customers",
            # Depends on a dbt source
            "dependencies": ["jaffle_shop.raw_customers"],
        },
    ]
    mock_parser_instance = MagicMock()
    mock_parser_instance.models = mock_dbt_models
    mock_parser_constructor.return_value = mock_parser_instance

    # --- Mock Writer ---
    mock_writer = MagicMock()
    mock_get_writer.return_value = mock_writer

    dummy_dbt_dir = str(tmp_path / "dbt_project")

    # 2. ACT
    workflow = LineageWorkflow(
        config_path=lineage_config,
        db_profile="test_db",
        dbt_project_dir=dummy_dbt_dir,
        output_profile="test_output",
    )
    workflow.run()

    # 3. ASSERT

    # Assert components were called correctly
    mock_get_connector.assert_called_once()
    mock_db.get_foreign_keys.assert_called_once()
    mock_parser_constructor.assert_called_once_with(dummy_dbt_dir)
    mock_get_writer.assert_called_once_with("mermaid")
    mock_writer.write.assert_called_once()

    # Assert the generated graph is correct
    # Get the data passed to the writer's 'write' method
    captured_data = mock_writer.write.call_args[0][0]
    assert "mermaid_graph" in captured_data

    mermaid_graph = captured_data["mermaid_graph"]

    # Check for the graph type
    assert "graph TD;" in mermaid_graph

    # Check for all nodes with correct styling
    # DB Table (parentheses)
    assert '    raw_orders[("raw_orders")]' in mermaid_graph
    # dbt Sources (double parentheses)
    assert (
        '    jaffle_shop.raw_orders(("jaffle_shop.raw_orders"))'
        in mermaid_graph
    )
    assert (
        '    jaffle_shop.raw_customers(("jaffle_shop.raw_customers"))'
        in mermaid_graph
    )
    # dbt Models (box)
    assert '    stg_orders["stg_orders"]' in mermaid_graph
    assert '    stg_customers["stg_customers"]' in mermaid_graph
    assert '    fct_orders["fct_orders"]' in mermaid_graph

    # Check for all edges (Physical + Logical)
    # Physical FK
    assert '    stg_orders -- "FK" --> raw_orders' in mermaid_graph
    # Logical (dbt refs and sources)
    assert "    jaffle_shop.raw_orders --> stg_orders" in mermaid_graph
    assert "    jaffle_shop.raw_customers --> stg_customers" in mermaid_graph
    assert "    stg_orders --> fct_orders" in mermaid_graph
    assert "    stg_customers --> fct_orders" in mermaid_graph
