from typing import Dict, List, Any
import os
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.error import YAMLError

from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class DbtYamlWriter:
    """Handles reading, updating, and writing dbt schema.yml files.

    This class can be used to:
    - Update schema.yml files with AI-generated descriptions.
    - Run in a check mode to verify if the documentation is up-to-date.
    """

    def __init__(self, dbt_project_dir: str, check_mode: bool = False):
        """Initializes the DbtYamlWriter.

        Args:
            dbt_project_dir: The root directory of the dbt project.
            check_mode: If True, the writer will not modify files but will check for outdated documentation.
        """
        self.dbt_project_dir = dbt_project_dir
        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.yaml.indent(mapping=2, sequence=4, offset=2)
        self.check_mode = check_mode
        if self.check_mode:
            logger.info("DbtYamlWriter initialized in check mode.")
        else:
            logger.info("DbtYamlWriter initialized.")

    def find_schema_files(self) -> List[str]:
        """Finds all 'schema.yml' (or '.yml') files in the dbt 'models', 'seeds', and 'snapshots' directories."""
        model_paths = [
            os.path.join(self.dbt_project_dir, "models"),
            os.path.join(self.dbt_project_dir, "seeds"),
            os.path.join(self.dbt_project_dir, "snapshots"),
        ]
        schema_files = []
        for path in model_paths:
            if not os.path.exists(path):
                continue
            for root, _, files in os.walk(path):
                for file in files:
                    if file.endswith((".yml", ".yaml")) and "dbt_project" not in file:
                        schema_files.append(os.path.join(root, file))

        logger.info(f"Found schema files to check: {schema_files}")
        return schema_files

    def update_yaml_files(self, catalog_data: Dict[str, Any]) -> bool:
        """Finds and updates all relevant schema.yml files with the catalog data.

        If in check mode, this method will not write to the files but will return
        True if any file is outdated.

        Args:
            catalog_data: The AI-generated catalog data.

        Returns:
            True if any file is outdated (in check mode), otherwise False.
        """
        schema_files = self.find_schema_files()
        if not schema_files:
            logger.warning(
                "No .yml files found in 'models', 'seeds', or 'snapshots' directories."
            )
            return False

        total_updates_needed = False

        for file_path in schema_files:
            try:
                file_needs_update = self._update_single_file(file_path, catalog_data)
                if file_needs_update:
                    total_updates_needed = True
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}", exc_info=True)

        return total_updates_needed

    def _update_single_file(self, file_path: str, catalog_data: Dict[str, Any]) -> bool:
        """Updates a single schema.yml file with AI-generated descriptions.

        This method reads a schema.yml file, finds the models defined in it,
        and updates the column descriptions with the AI-generated content from the catalog.
        It only updates fields that are not already present in the schema.yml file.

        Args:
            file_path: The path to the schema.yml file to update.
            catalog_data: The AI-generated catalog data.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = self.yaml.load(f)
        except YAMLError as e:
            logger.error(f"Failed to load YAML file {file_path}: {e}")
            return False

        if not data:
            logger.info(f"Skipping empty YAML file: {file_path}")
            return False

        file_updated = False
        for node_type in ["models", "sources", "seeds", "snapshots"]:
            if node_type not in data:
                continue

            for node_config in data.get(node_type, []):
                if not isinstance(node_config, CommentedMap):
                    continue

                node_name = node_config.get("name")
                if node_type == "models" and node_name in catalog_data:
                    logger.info(f" -> Found model '{node_name}' in YAML.")
                    ai_model_data = catalog_data[node_name]

                    if "columns" in node_config:
                        for column_config in node_config["columns"]:
                            column_name = column_config.get("name")
                            ai_column = next(
                                (
                                    c
                                    for c in ai_model_data["columns"]
                                    if c["name"] == column_name
                                ),
                                None,
                            )

                            if ai_column:
                                ai_data_dict = ai_column.get("ai_generated", {})
                                for key, ai_value in ai_data_dict.items():
                                    if not column_config.get(key):
                                        logger.info(
                                            f"    - Updating '{node_name}.{column_name}' with new key: '{key}'"
                                        )
                                        column_config[key] = ai_value
                                        file_updated = True

        if self.check_mode:
            if file_updated:
                logger.warning(f"CI CHECK: '{file_path}' is outdated.")
            else:
                logger.info(f"CI CHECK: '{file_path}' is up-to-date.")
            return file_updated

        if file_updated:
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    self.yaml.dump(data, f)
                logger.info(f"Successfully updated '{file_path}' with AI descriptions.")
            except IOError as e:
                logger.error(f"Failed to write updates to '{file_path}': {e}")
        else:
            logger.info(
                f"No missing descriptions found in '{file_path}'. No changes made."
            )

        return file_updated
