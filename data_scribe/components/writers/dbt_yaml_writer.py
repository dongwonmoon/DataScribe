"""
This module provides a writer for updating dbt schema.yml files.

It can be used to enrich existing dbt documentation with AI-generated content
or to check if the documentation is up-to-date.
"""

from typing import Dict, List, Any
import os
import typer
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.error import YAMLError

from data_scribe.core.exceptions import WriterError, ConfigError
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class DbtYamlWriter:
    """Handles reading, updating, and writing dbt schema.yml files.

    This class can be used to:
    - Update schema.yml files with AI-generated descriptions.
    - Run in a check mode to verify if the documentation is up-to-date.
    """

    def __init__(self, dbt_project_dir: str, mode: str = "update"):
        """
        Initializes the DbtYamlWriter.

        Args:
            dbt_project_dir: The root directory of the dbt project.
            mode: The operation mode. One of 'update' (overwrite),
                  'check' (CI mode), or 'interactive' (prompt user).
        """
        self.dbt_project_dir = dbt_project_dir
        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.yaml.indent(mapping=2, sequence=4, offset=2)
        if mode not in ["update", "check", "interactive"]:
            raise ValueError(f"Invalid mode: {mode}")
        self.mode = mode

        logger.info(f"DbtYamlWriter initialized in '{self.mode}' mode.")

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
                raise WriterError(f"Error processing {file_path}: {e}") from e

        return total_updates_needed

    def _prompt_user_for_change(
        self, node_log_name: str, key: str, ai_value: str
    ) -> str | None:
        """
        Prompts the user to Accept, Edit, or Skip an AI-generated suggestion.
        Returns the value to save (str) or None to skip.
        """
        target = f"'{key}' on '{node_log_name}'"
        prompt_title = typer.style(f"Suggestion for {target}:", fg=typer.colors.CYAN)

        # Display the AI suggestion clearly
        typer.echo(prompt_title)
        typer.echo(typer.style(f'  AI: "{ai_value}"', fg=typer.colors.GREEN))

        # Ask the user for input
        final_value = typer.prompt(
            "  [Enter] to Accept, type to Edit, or [s] + [Enter] to Skip",
            default=ai_value,
        )

        if final_value.lower() == "s":
            logger.info(f"  - User skipped {key} for {node_log_name}")
            return None

        # Handle the case where user just hits Enter (accepting default)
        if final_value == ai_value:
            logger.info(f"  - User accepted {key} for {node_log_name}")
        else:
            logger.info(f"  - User edited {key} for {node_log_name}")

        return final_value

    def _process_update(
        self,
        config_node: CommentedMap,
        key: str,
        ai_value: str,
        node_log_name: str,
    ) -> bool:
        """
        Handles the logic for a single missing key based on the writer's mode.
        Returns True if a change was made or is needed.
        """
        log_target = f"'{key}' on '{node_log_name}'"

        if self.mode == "check":
            logger.warning(f"CI CHECK: Missing {log_target}")
            return True  # A change is needed

        elif self.mode == "interactive":
            final_value = self._prompt_user_for_change(node_log_name, key, ai_value)
            if final_value:
                config_node[key] = final_value
                return True  # A change was made
            return False  # User skipped

        else:  # self.mode == "update"
            logger.info(f"- Updating {log_target}")
            config_node[key] = ai_value
            return True  # A change was made

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
            logger.error(f"Failed to parse YAML file {file_path}: {e}")
            raise WriterError(f"Failed to parse YAML file: {file_path}") from e

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
                    logger.info(f" -> Checking model: '{node_name}' in {file_path}")
                    ai_model_data = catalog_data[node_name]

                    # --- 1. Update model-level description ---
                    ai_model_desc = ai_model_data.get("model_description")
                    if ai_model_desc and not node_config.get("description"):
                        if self._process_update(
                            config_node=node_config,
                            key="description",
                            ai_value=ai_model_desc,
                            node_log_name=f"model '{node_name}'",
                        ):
                            file_updated = True

                    # --- 2. Update column-level descriptions ---
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
                                        col_log_name = (
                                            f"column '{node_name}.{column_name}'"
                                        )
                                        if self._process_update(
                                            config_node=column_config,
                                            key=key,
                                            ai_value=ai_value,
                                            node_log_name=col_log_name,
                                        ):
                                            file_updated = True

        if self.mode == "check":
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
