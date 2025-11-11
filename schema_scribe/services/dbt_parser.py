"""
This module provides `DbtManifestParser`, a foundational component for all
dbt-related workflows in the application.

The parser is responsible for reading the complex `manifest.json` file generated
by dbt and transforming it into a simplified, structured format that other parts
of the application can easily consume.
"""

import json
import os
from typing import List, Dict, Any
from functools import cached_property

from schema_scribe.core.exceptions import DbtParseError
from schema_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class DbtManifestParser:
    """
    Parses a dbt `manifest.json` file to extract model and column information.

    This class acts as an adapter, locating and loading the raw manifest created
    by a `dbt compile` or `dbt run` command, then providing a clean, structured
    list of all dbt models found within it via the `models` cached property.
    """

    def __init__(self, dbt_project_dir: str):
        """
        Initializes the DbtManifestParser.

        Args:
            dbt_project_dir: The absolute path to the root of the dbt project.
                             This directory should contain the `dbt_project.yml`
                             file and a `target` directory with `manifest.json`.

        Raises:
            DbtParseError: If the `manifest.json` file cannot be found or parsed.
        """
        self.manifest_path = os.path.join(
            dbt_project_dir, "target", "manifest.json"
        )
        self.manifest_data = self._load_manifest()

    def _load_manifest(self) -> Dict[str, Any]:
        """
        Loads the `manifest.json` file from the project's `target` directory.

        Note: `dbt compile` or a similar command must be run first to ensure
        this file exists.

        Returns:
            A dictionary containing the parsed JSON data from the manifest file.

        Raises:
            DbtParseError: If the manifest file is not found or is malformed.
        """
        logger.info(f"Loading manifest from: {self.manifest_path}")
        try:
            with open(self.manifest_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Manifest file not found at: {self.manifest_path}")
            raise DbtParseError(
                f"manifest.json not found in '{os.path.dirname(self.manifest_path)}'. "
                "Please run 'dbt compile' or 'dbt run' in your dbt project first."
            )
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse manifest.json: {e}", exc_info=True)
            raise DbtParseError(f"Failed to parse manifest.json: {e}") from e

    @cached_property
    def models(self) -> List[Dict[str, Any]]:
        """
        Parses all 'model' nodes in the manifest into a structured list.

        This method is a `cached_property`, so it performs the parsing work only
        once on the first access. It iterates through all nodes in the manifest,
        filters for `resource_type: 'model'`, and extracts key details.

        Returns:
            A list of dictionaries, where each dictionary represents a dbt model
            with the following structure:
            ```
            {
                "name": str,              # The name of the model.
                "unique_id": str,         # The unique ID from the manifest.
                "description": str,       # The model's description.
                "raw_sql": str,           # The raw SQL code of the model.
                "columns": [              # A list of column dictionaries.
                    {
                        "name": str,
                        "description": str,
                        "type": str,      # The data type, if available.
                    },
                    ...
                ],
                "dependencies": [str],    # A list of parent model/source names.
                "path": str,              # The relative path to the model file.
                "original_file_path": str # The full path to the model file.
            }
            ```
        """
        parsed_models = []
        nodes = self.manifest_data.get("nodes", {})
        logger.info(f"Parsing {len(nodes)} nodes from manifest...")

        for node_name, node_data in nodes.items():
            if node_data.get("resource_type") == "model":
                # The description can be in the 'description' field or under 'config'
                description = node_data.get("description") or node_data.get(
                    "config", {}
                ).get("description", "")

                parsed_columns = []
                for col_name, col_data in node_data.get("columns", {}).items():
                    parsed_columns.append(
                        {
                            "name": col_name,
                            "description": col_data.get("description", ""),
                            "type": col_data.get("data_type", "N/A"),
                        }
                    )

                # Parse dependencies (parents of the current model)
                depends_on_nodes = node_data.get("depends_on", {}).get(
                    "nodes", []
                )
                dependencies = []
                for dep_key in depends_on_nodes:
                    dep_node = nodes.get(dep_key, {})
                    dep_type = dep_node.get("resource_type")
                    if dep_type in ["model", "seed"]:
                        dependencies.append(dep_node.get("name"))
                    elif dep_type == "source":
                        # For sources, format as 'source_name.name'
                        source_name = dep_node.get("source_name")
                        table_name = dep_node.get("name")
                        dependencies.append(f"{source_name}.{table_name}")

                parsed_models.append(
                    {
                        "name": node_data.get("name"),
                        "unique_id": node_name,
                        "description": description,
                        "raw_sql": node_data.get("raw_code")
                        or node_data.get(
                            "raw_sql", "-- SQL code not available --"
                        ),
                        "columns": parsed_columns,
                        "dependencies": dependencies,
                        "path": node_data.get("path"),
                        "original_file_path": node_data.get(
                            "original_file_path"
                        ),
                    }
                )

        logger.info(f"Found and parsed {len(parsed_models)} models.")
        return parsed_models
