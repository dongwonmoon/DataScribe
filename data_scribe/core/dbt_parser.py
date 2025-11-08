"""
This module provides a parser for dbt (data build tool) manifest files.

The DbtManifestParser class is responsible for loading the `manifest.json` file
and extracting relevant information about models, including their SQL code and columns.
This information is then used to generate a data catalog.
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
    Parses the dbt 'manifest.json' file to extract model and column information.
    """

    def __init__(self, dbt_project_dir: str):
        """
        Initializes the DbtManifestParser.

        Args:
            dbt_project_dir: The root directory of the dbt project, where the 'target'
                             directory and 'manifest.json' are located.
        
        Raises:
            DbtParseError: If the manifest.json file cannot be found or parsed.
        """
        self.manifest_path = os.path.join(
            dbt_project_dir, "target", "manifest.json"
        )
        self.manifest_data = self._load_manifest()

    def _load_manifest(self) -> Dict[str, Any]:
        """
        Loads the 'manifest.json' file from the specified path.

        Returns:
            A dictionary containing the parsed JSON data from the manifest file.

        Raises:
            DbtParseError: If the manifest file cannot be found or is malformed.
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

        This method is a cached property, so it only parses the manifest once.

        Returns:
            A list of dictionaries, where each dictionary represents a dbt model.
        """
        parsed_models = []
        nodes = self.manifest_data.get("nodes", {})
        logger.info(f"Parsing {len(nodes)} nodes from manifest...")

        for node_name, node_data in nodes.items():
            if node_data.get("resource_type") == "model":
                # The description can be in the 'description' field or under 'config'
                description = node_data.get("description") or node_data.get("config", {}).get("description", "")

                parsed_columns = []
                for col_name, col_data in node_data.get("columns", {}).items():
                    parsed_columns.append(
                        {
                            "name": col_name,
                            "description": col_data.get("description", ""),
                            "type": col_data.get("data_type", "N/A"),
                        }
                    )

                parsed_models.append(
                    {
                        "name": node_data.get("name"),
                        "unique_id": node_name,
                        "description": description,
                        "raw_sql": node_data.get("raw_code") or node_data.get("raw_sql", "-- SQL code not available --"),
                        "columns": parsed_columns,
                        "path": node_data.get("path"),
                        "original_file_path": node_data.get("original_file_path"),
                    }
                )

        logger.info(f"Found and parsed {len(parsed_models)} models.")
        return parsed_models