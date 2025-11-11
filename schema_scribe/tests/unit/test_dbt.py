"""
Unit tests for dbt-related components.

This test suite verifies the functionality of the DbtManifestParser,
ensuring it correctly parses dbt artifacts and handles potential errors
like malformed or missing files.
"""

import pytest
import json
from pathlib import Path

from schema_scribe.core.dbt_parser import DbtManifestParser
from schema_scribe.core.exceptions import DbtParseError

# A minimal, valid manifest.json for successful parsing tests
MINIMAL_MANIFEST = {
    "metadata": {"dbt_version": "1.0.0"},
    "nodes": {
        "model.jaffle_shop.customers": {
            "resource_type": "model",
            "path": "customers.sql",
            "original_file_path": "models/customers.sql",
            "name": "customers",
            "description": "Existing model description",
            "columns": {
                "customer_id": {
                    "name": "customer_id",
                    "description": "Existing column description",
                },
            },
        }
    },
}


@pytest.fixture
def dbt_project_dir(tmp_path: Path):
    """Creates a temporary dbt project directory structure."""
    project_dir = tmp_path / "dbt_project"
    target_path = project_dir / "target"
    target_path.mkdir(parents=True)
    return str(project_dir)


def test_dbt_parser_success(dbt_project_dir: str):
    """Tests that the DbtManifestParser successfully parses a valid manifest.json."""
    manifest_path = Path(dbt_project_dir) / "target" / "manifest.json"
    manifest_path.write_text(json.dumps(MINIMAL_MANIFEST))

    parser = DbtManifestParser(dbt_project_dir)
    models = parser.models

    assert len(models) == 1
    assert models[0]["name"] == "customers"
    assert models[0]["description"] == "Existing model description"
    assert models[0]["columns"][0]["name"] == "customer_id"


def test_dbt_parser_manifest_not_found(dbt_project_dir: str):
    """Tests that DbtParseError is raised if manifest.json is not found."""
    with pytest.raises(DbtParseError, match="manifest.json not found"):
        DbtManifestParser(dbt_project_dir)


def test_dbt_parser_manifest_malformed_json(dbt_project_dir: str):
    """
    Tests that DbtParseError is raised if manifest.json is not valid JSON.
    """
    manifest_path = Path(dbt_project_dir) / "target" / "manifest.json"
    manifest_path.write_text("{'key': 'value',}")  # Invalid JSON

    with pytest.raises(DbtParseError, match="Failed to parse manifest.json"):
        DbtManifestParser(dbt_project_dir)
