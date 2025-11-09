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
    """
    Handles reading, updating, and writing dbt schema YAML files.

    This writer is specialized for dbt projects. It can enrich existing
    `schema.yml` files with AI-generated content, check if documentation is
    up-to-date, or interactively prompt the user for changes.

    Note: This class does not implement the `BaseWriter` interface, as its
    functionality is specific to the dbt workflow and requires special
    parameters and methods (`update_yaml_files` instead of `write`).
    """

    def __init__(self, dbt_project_dir: str, mode: str = "update"):
        """
        Initializes the DbtYamlWriter.

        Args:
            dbt_project_dir: The absolute path to the root of the dbt project.
            mode: The operation mode. Must be one of 'update' (overwrite changes),
                  'check' (fail if changes are needed), or 'interactive'
                  (prompt user for each change).

        Raises:
            ValueError: If an invalid mode is provided.
        """
        self.dbt_project_dir = dbt_project_dir
        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.yaml.indent(mapping=2, sequence=4, offset=2)
        if mode not in ["update", "check", "interactive"]:
            raise ValueError(f"Invalid mode: {mode}")
        self.mode = mode

        logger.info(f"DbtYamlWriter initialized in '{self.mode}' mode.")

        # Stores loaded YAML data {file_path: data}
        self.yaml_files: Dict[str, Any] = {}
        # Stores a map of {model_name: file_path}
        self.model_to_file_map: Dict[str, str] = {}
        # Tracks which files have been modified
        self.files_to_write: set[str] = set()

    def find_schema_files(self) -> List[str]:
        """
        Finds all dbt schema YAML files in the project.

        It searches for `.yml` or `.yaml` files within the standard dbt
        subdirectories: 'models', 'seeds', and 'snapshots'.

        Returns:
            A list of absolute paths to the found schema YAML files.
        """
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

    def _load_and_map_existing_yamls(self):
        """
        Loads all found schema.yml files into memory and builds a map
        of which file documents which model.
        """
        schema_files = self.find_schema_files()
        for file_path in schema_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = self.yaml.load(f)
                    if not data:
                        logger.info(f"Skipping empty YAML file: {file_path}")
                        continue
                    self.yaml_files[file_path] = data

                    # Map models to this file
                    for node_type in ["models", "sources", "seeds", "snapshots"]:
                        for node_config in data.get(node_type, []):
                            if isinstance(node_config, CommentedMap):
                                model_name = node_config.get("name")
                                if model_name:
                                    self.model_to_file_map[model_name] = file_path

            except YAMLError as e:
                logger.error(f"Failed to parse YAML file {file_path}: {e}")
                raise WriterError(f"Failed to parse YAML file: {file_path}") from e

    def update_yaml_files(self, catalog_data: Dict[str, Any]) -> bool:
        """
        Updates dbt `schema.yml` files based on the generated catalog.

        This is the main entrypoint for the writer. It orchestrates finding,
        parsing, and updating the YAML files. The exact behavior depends on the
        mode the writer was initialized with ('update', 'check', 'interactive').

        Args:
            catalog_data: The AI-generated catalog data, keyed by model name.

        Returns:
            `True` if any documentation was missing or outdated (especially
            relevant for 'check' mode), otherwise `False`.
        """
        self._load_and_map_existing_yamls()

        catalog_models = set(catalog_data.keys())
        documented_models = set(self.model_to_file_map.keys())

        models_to_update = catalog_models.intersection(documented_models)
        models_to_create = catalog_models.difference(documented_models)

        total_updates_needed = False

        logger.info(f"Updating {len(models_to_update)} existing models...")
        for model_name in models_to_update:
            file_path = self.model_to_file_map[model_name]
            if self._update_existing_model(
                file_path, model_name, catalog_data[model_name]
            ):
                total_updates_needed = True

        if models_to_create:
            logger.info(f"Creating stubs for {len(models_to_create)} new models...")
            for model_name in models_to_create:
                if self._create_new_model_stub(model_name, catalog_data[model_name]):
                    total_updates_needed = True

        if self.mode != "check" and self.files_to_write:
            logger.info(f"Writing changes to {len(self.files_to_write)} file(s)...")
            for file_path in self.files_to_write:
                try:
                    with open(file_path, "w", encoding="utf-8") as f:
                        self.yaml.dump(self.yaml_files[file_path], f)
                    logger.info(f"Successfully updated '{file_path}'")
                except IOError as e:
                    logger.error(f"Failed to write updates to '{file_path}': {e}")

        elif self.mode == "check" and total_updates_needed:
            logger.warning("CI CHECK: Changes are needed (see warnings above).")
        elif not total_updates_needed:
            logger.info("All dbt documentation is up-to-date. No changes made.")

        return total_updates_needed

    def _update_existing_model(
        self, file_path: str, model_name: str, ai_model_data: Dict[str, Any]
    ) -> bool:
        file_changed = False
        data = self.yaml_files.get(file_path)
        if not data:
            return False  # Should not happen

        node_config = None
        for node in data.get("models", []):
            if node.get("name") == model_name:
                node_config = node
                break

        if not node_config:
            logger.warning(f"Could not find model '{model_name}' in {file_path}")
            return False

        logger.info(f" -> Checking model: '{model_name}' in {file_path}")

        ai_model_desc = ai_model_data.get("model_description")
        if ai_model_desc and not node_config.get("description"):
            if self._process_update(
                config_node=node_config,
                key="description",
                ai_value=ai_model_desc,
                node_log_name=f"model '{model_name}'",
            ):
                file_changed = True

        if "columns" in node_config:
            for column_config in node_config["columns"]:
                column_name = column_config.get("name")
                ai_column = next(
                    (c for c in ai_model_data["columns"] if c["name"] == column_name),
                    None,
                )
                if ai_column:
                    ai_data_dict = ai_column.get("ai_generated", {})
                    for key, ai_value in ai_data_dict.items():
                        if not column_config.get(key):
                            col_log_name = f"column '{model_name}.{column_name}'"
                            if self._process_update(
                                config_node=column_config,
                                key=key,
                                ai_value=ai_value,
                                node_log_name=col_log_name,
                            ):
                                file_changed = True

        if file_changed:
            self.files_to_write.add(file_path)

        return file_changed

    def _create_new_model_stub(
        self, model_name: str, ai_model_data: Dict[str, Any]
    ) -> bool:
        """
        Creates a new model stub (dictionary) and adds it to the
        appropriate schema.yml file.
        """
        if self.mode == "check":
            logger.warning(
                f"CI CHECK: Missing documentation for new model '{model_name}'"
            )
            return True  # A change is needed

        logger.info(f" -> Generating new stub for model: '{model_name}'")

        # --- 1. Build the new YAML stub from AI data ---
        new_model_stub = CommentedMap()
        new_model_stub["name"] = model_name

        # Process model description
        ai_desc = ai_model_data["model_description"]
        if self._process_update(
            new_model_stub, "description", ai_desc, f"model '{model_name}'"
        ):
            pass  # Value is added by _process_update

        # Process columns
        ai_columns = ai_model_data["columns"]
        new_columns_list = []
        for col in ai_columns:
            new_col_stub = CommentedMap()
            new_col_stub["name"] = col["name"]
            ai_data_dict = col.get("ai_generated", {})
            for key, ai_value in ai_data_dict.items():
                col_log_name = f"column '{model_name}.{col['name']}'"
                self._process_update(new_col_stub, key, ai_value, col_log_name)
            new_columns_list.append(new_col_stub)

        new_model_stub["columns"] = new_columns_list

        # --- 2. Find or create the target YAML file ---
        sql_path = ai_model_data.get("original_file_path")
        if not sql_path:
            logger.error(
                f"Cannot create stub for '{model_name}': missing 'original_file_path'."
            )
            return False

        # Place schema.yml in the same directory as the .sql file
        target_yaml_path = os.path.join(os.path.dirname(sql_path), "schema.yml")

        # --- 3. Add stub to the file (in memory) ---
        if target_yaml_path in self.yaml_files:
            # File exists, append model to it
            logger.info(f"   -> Appending stub to existing file: {target_yaml_path}")
            if "models" not in self.yaml_files[target_yaml_path]:
                self.yaml_files[target_yaml_path]["models"] = []
            self.yaml_files[target_yaml_path]["models"].append(new_model_stub)

        else:
            # File does not exist, create new
            logger.info(f"   -> Creating new file for stub: {target_yaml_path}")
            new_yaml_data = CommentedMap()
            new_yaml_data["version"] = 2
            new_yaml_data["models"] = [new_model_stub]
            self.yaml_files[target_yaml_path] = new_yaml_data

        self.files_to_write.add(target_yaml_path)
        return True

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
