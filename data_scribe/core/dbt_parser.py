"""
This module provides a parser for dbt (data build tool) manifest files.

The DbtManifestParser class is responsible for loading the `manifest.json` file
and extracting relevant information about models, including their SQL code and columns.
This information is then used to generate a data catalog.
"""

import json
import os
from typing import List, Dict, Any
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
        """
        # Construct the full path to the manifest.json file
        self.manifest_path = os.path.join(dbt_project_dir, "target", "manifest.json")
        # Load the manifest data upon initialization
        self.manifest_data = self._load_manifest()

    def _load_manifest(self) -> Dict[str, Any]:
        """
        Loads the 'manifest.json' file from the specified path.

        Returns:
            A dictionary containing the parsed JSON data from the manifest file.

        Raises:
            FileNotFoundError: If the manifest file cannot be found at the expected path.
        """
        logger.info(f"Loading manifest from: {self.manifest_path}")
        try:
            with open(self.manifest_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Manifest file not found at: {self.manifest_path}")
            # Re-raise the exception to be handled by the caller
            raise

    def parse_models(self) -> List[Dict[str, Any]]:
        """
        Parses all 'model' nodes in the manifest and extracts key information.

        This method filters for nodes that are models and are not ephemeral,
        as ephemeral models are not materialized in the database.

        Returns:
            A list of dictionaries, where each dictionary represents a dbt model
            and contains its name, raw SQL code, and a list of its columns.
            Example:
            [
                {
                    "name": "my_model",
                    "raw_sql": "SELECT * FROM source_table",
                    "columns": [
                        {"name": "id", "type": "integer"},
                        {"name": "name", "type": "text"}
                    ]
                }
            ]
        """
        models = []
        # The manifest contains all nodes (models, sources, tests, etc.) in the 'nodes' dictionary
        nodes = self.manifest_data.get("nodes", {})
        logger.info(f"Parsing {len(nodes)} nodes from manifest...")

        for node in nodes.values():
            # We are interested only in nodes that are models and are materialized as tables or views.
            if (
                node["resource_type"] == "model"
                and node["config"].get("materialized") != "ephemeral"
            ):
                model_name = node["name"]
                # Get the raw SQL code for the model
                raw_code = node.get("raw_code", "-- SQL code not available --")

                # The columns are stored in a dictionary within the node
                column_nodes = node.get("columns", {})

                parsed_columns = []
                for col_name, col_info in column_nodes.items():
                    parsed_columns.append(
                        {
                            "name": col_name,
                            "type": col_info.get("data_type", "N/A"),
                        }
                    )

                models.append(
                    {
                        "name": model_name,
                        "raw_sql": raw_code,
                        "columns": parsed_columns,
                    }
                )

        logger.info(f"Found and parsed {len(models)} models.")
        return models
