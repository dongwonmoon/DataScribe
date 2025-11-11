"""
This module provides a parser for dbt (data build tool) `manifest.json` files.

The `DbtManifestParser` class is responsible for loading the manifest, finding
all model nodes, and extracting relevant information like SQL code, columns,
and descriptions. This data is then used by other parts of the application to
generate a data catalog.
"""

import json
import os
from typing import List, Dict, Any
from functools import cached_property

from data_scribe.core.exceptions import DbtParseError
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class DbtManifestParser:
    """
    Parses a dbt `manifest.json` file to extract model and column information.

    This class locates and loads the manifest created by a `dbt compile` or
    `dbt run` command, then provides a structured list of all dbt models
    found within it.
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

        Returns:
            A dictionary containing the parsed JSON data from the manifest file.

        Raises:
            DbtParseError: If the manifest file cannot be found (e.g., because
                           `dbt compile` has not been run) or if it is malformed.
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
        Parses all 'model' nodes in the manifest and extracts key information.

        This method is a `cached_property`, so it only performs the parsing work
        the first time it is accessed.

        Returns:
            A list of dictionaries, where each dictionary represents a dbt model
            with keys such as `name`, `unique_id`, `description`, `raw_sql`,
            `columns`, and `original_file_path`.
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

                depends_on_nodes = node_data.get("depends_on", {}).get(
                    "nodes", []
                )
                dependencies = []
                for dep_key in depends_on_nodes:
                    dep_node = nodes.get(dep_key, {})
                    dep_type = dep_node.get("resource_type")
                    if dep_type == "model" or dep_type == "seed":
                        dependencies.append(dep_node.get("name"))
                    elif dep_type == "source":
                        # For sources, get 'source_name.name' (e.g., 'jaffle_shop.customers')
                        dependencies.append(
                            f"{dep_node.get('source_name')}.{dep_node.get('name')}"
                        )

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
                        "path": node_data.get("path"),
                        "original_file_path": node_data.get(
                            "original_file_path"
                        ),
                    }
                )

        logger.info(f"Found and parsed {len(parsed_models)} models.")
        return parsed_models
