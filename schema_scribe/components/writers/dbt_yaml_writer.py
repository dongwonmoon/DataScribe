"""
This module provides `DbtYamlWriter`, the core engine for managing dbt
`schema.yml` documentation across various operational modes.

Design Rationale:
The `DbtYamlWriter` is a critical component for automating dbt documentation.
Its design prioritizes:
- **Fidelity**: It uses `ruamel.yaml` to ensure that existing comments,
  formatting, and ordering within `schema.yml` files are preserved during
  updates. This is crucial for maintaining code quality and developer experience.
- **Flexibility**: It supports multiple operational modes (`update`, `check`,
  `interactive`, `drift`), allowing it to be used in diverse scenarios, from
  fully automated CI/CD pipelines to interactive documentation generation.
- **Extensibility**: It dynamically discovers `schema.yml` files across the
  dbt project, adapting to various project structures.
"""

from typing import Dict, List, Any
import os
import typer
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.error import YAMLError

from schema_scribe.core.exceptions import WriterError
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class DbtYamlWriter:
    """
    Handles reading, updating, and writing dbt schema YAML files.

    This class acts as a stateful orchestrator for dbt YAML modifications.
    Its lifecycle involves:
    1.  **Discovery**: Finding all relevant `schema.yml` files within the dbt project.
    2.  **Loading & Mapping**: Loading these files into memory using `ruamel.yaml`
        and building a map to quickly locate which file documents which dbt model.
    3.  **In-Memory Update**: Iterating through an AI-generated catalog and
        applying updates or creating new documentation stubs in memory, based
        on the selected operational mode.
    4.  **Persistence**: Finally, writing the modified YAML data back to the files
        if the mode permits.
    """

    def __init__(self, dbt_project_dir: str, mode: str = "update"):
        """
        Initializes the DbtYamlWriter.

        Args:
            dbt_project_dir: The absolute path to the root of the dbt project.
            mode: The operational mode. This dictates how changes are handled.
                  Must be one of 'update', 'check', 'interactive', or 'drift'.

        Raises:
            ValueError: If an invalid mode is provided.
        """
        self.dbt_project_dir = dbt_project_dir
        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.yaml.indent(mapping=2, sequence=4, offset=2)

        if mode not in ["update", "check", "interactive", "drift"]:
            raise ValueError(f"Invalid mode for DbtYamlWriter: {mode}")
        self.mode = mode

        logger.info(f"DbtYamlWriter initialized in '{self.mode}' mode.")

        self.yaml_files: Dict[str, Any] = {}
        self.model_to_file_map: Dict[str, str] = {}
        self.files_to_write: set[str] = set()

    def write(self, catalog_data: Dict[str, Any], **kwargs) -> bool:
        """
        Updates dbt `schema.yml` files based on the generated catalog.

        This is the main entrypoint for the writer. It orchestrates finding,
        parsing, and updating the YAML files. The exact behavior depends on the
        mode the writer was initialized with.

        The process involves:
        1.  Loading all existing `schema.yml` files into memory.
        2.  Identifying models that need updates (already documented) and models
            that need new documentation stubs (undocumented).
        3.  Applying changes in memory based on the `mode`.
        4.  If in 'update' or 'interactive' mode, persisting the changes to disk.

        Args:
            catalog_data: The AI-generated catalog data, keyed by model name.
            **kwargs: Not used by this writer, but included for interface
                      compatibility.

        Returns:
            `True` if any documentation was missing or outdated (especially
            relevant for 'check' and 'drift' modes), otherwise `False`.
        """
        self._load_and_map_existing_yamls()

        catalog_models = set(catalog_data.keys())
        documented_models = set(self.model_to_file_map.keys())

        models_to_update = catalog_models.intersection(documented_models)
        models_to_create = catalog_models.difference(documented_models)

        is_outdated = False

        logger.info(f"Checking {len(models_to_update)} existing models...")
        for model_name in models_to_update:
            file_path = self.model_to_file_map[model_name]
            if self._update_existing_model_in_memory(
                file_path, model_name, catalog_data[model_name]
            ):
                is_outdated = True

        if models_to_create:
            logger.info(
                f"Found {len(models_to_create)} models missing documentation..."
            )
            for model_name in models_to_create:
                if self._create_new_model_stub_in_memory(
                    model_name, catalog_data[model_name]
                ):
                    is_outdated = True

        # In 'update' or 'interactive' mode, write the changes to disk.
        if self.mode not in ["check", "drift"] and self.files_to_write:
            self._write_modified_files_to_disk()

        if not is_outdated:
            logger.info("All dbt documentation is up-to-date. No changes made.")

        return is_outdated

    def _find_schema_files(self) -> List[str]:
        """
        Finds all dbt schema YAML files in the project's model, seed, and snapshot paths.

        Design Rationale:
        This method ensures that the writer can discover all relevant `schema.yml`
        files, regardless of their location within the standard dbt project
        structure. It explicitly excludes `dbt_project.yml` to avoid parsing
        the main project configuration as a schema file.
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
                    # Standard dbt convention is schema.yml, but can be anything
                    if (
                        file.endswith((".yml", ".yaml"))
                        and "dbt_project" not in file
                    ):
                        schema_files.append(os.path.join(root, file))
        logger.info(f"Found schema files to check: {schema_files}")
        return schema_files

    def _load_and_map_existing_yamls(self):
        """
        Loads all found `schema.yml` files into memory and builds a map
        from dbt model names to their respective file paths.

        Design Rationale:
        This mapping (`self.model_to_file_map`) is crucial for efficiently
        locating and updating the correct YAML file when processing an
        AI-generated catalog. It avoids redundant file searches.
        """
        schema_files = self._find_schema_files()
        for file_path in schema_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = self.yaml.load(f)
                    if not data:
                        continue
                    self.yaml_files[file_path] = data
                    for node_type in [
                        "models",
                        "sources",
                        "seeds",
                        "snapshots",
                    ]:
                        for node_config in data.get(node_type, []):
                            if isinstance(node_config, dict):
                                model_name = node_config.get("name")
                                if model_name:
                                    self.model_to_file_map[model_name] = (
                                        file_path
                                    )
            except YAMLError as e:
                raise WriterError(
                    f"Failed to parse YAML file: {file_path}"
                ) from e

    def _update_existing_model_in_memory(
        self, file_path: str, model_name: str, ai_model_data: Dict[str, Any]
    ) -> bool:
        """
        Updates a single model's documentation within a loaded YAML file in memory.

        This method handles:
        - Updating model-level descriptions.
        - Updating column-level details (descriptions, meta, tags, tests).
        - Performing drift checks if the writer is in 'drift' mode.

        Args:
            file_path: The path to the `schema.yml` file containing the model.
            model_name: The name of the dbt model to update.
            ai_model_data: The AI-generated data for the model.

        Returns:
            `True` if a change was made or is needed (e.g., in 'check' mode),
            otherwise `False`.
        """
        file_changed = False
        data = self.yaml_files.get(file_path)
        if not data:
            return False

        node_config = next(
            (n for n in data.get("models", []) if n.get("name") == model_name),
            None,
        )
        if not node_config:
            return False

        logger.info(f" -> Checking model: '{model_name}' in '{file_path}'")

        # Update model-level description if missing
        if not node_config.get("description") and ai_model_data.get(
            "model_description"
        ):
            if self._process_update(
                node_config,
                "description",
                ai_model_data["model_description"],
                f"model '{model_name}'",
            ):
                file_changed = True

        # Update column-level details
        for column_config in node_config.get("columns", []):
            col_name = column_config.get("name")
            ai_column = next(
                (
                    c
                    for c in ai_model_data.get("columns", [])
                    if c["name"] == col_name
                ),
                None,
            )
            if not ai_column:
                continue

            # In 'drift' mode, check for inconsistencies
            if (
                self.mode == "drift"
                and column_config.get("description")
                and ai_column.get("drift_status") == "DRIFT"
            ):
                logger.warning(
                    f"DRIFT DETECTED: Doc for '{model_name}.{col_name}' conflicts with live data."
                )
                file_changed = True
                continue

            # In other modes, fill in missing AI-generated data
            for key, ai_value in ai_column.get("ai_generated", {}).items():
                if not column_config.get(key):
                    if self._process_update(
                        column_config,
                        key,
                        ai_value,
                        f"column '{model_name}.{col_name}'",
                    ):
                        file_changed = True

        if file_changed:
            self.files_to_write.add(file_path)
        return file_changed

    def _create_new_model_stub_in_memory(
        self, model_name: str, ai_model_data: Dict[str, Any]
    ) -> bool:
        """
        Creates a new model stub (documentation block) and adds it to the
        appropriate `schema.yml` file in memory.

        Design Rationale:
        This method intelligently determines where to place new model documentation.
        If a `schema.yml` file already exists in the same directory as the model's
        SQL file, the stub is appended to it. Otherwise, a new `schema.yml` file
        is created.

        Args:
            model_name: The name of the new dbt model.
            ai_model_data: The AI-generated data for the new model.

        Returns:
            `True` if a change is needed (e.g., in 'check' mode) or if a stub
            was created/appended, otherwise `False`.
        """
        if self.mode == "check":
            logger.warning(
                f"CI CHECK: Missing documentation for new model '{model_name}'"
            )
            return True

        logger.info(f" -> Generating new stub for model: '{model_name}'")
        new_model_stub = CommentedMap()
        new_model_stub["name"] = model_name

        if self._process_update(
            new_model_stub,
            "description",
            ai_model_data["model_description"],
            f"model '{model_name}'",
        ):
            pass

        new_columns_list = []
        for col in ai_model_data.get("columns", []):
            new_col_stub = CommentedMap()
            new_col_stub["name"] = col["name"]
            for key, ai_value in col.get("ai_generated", {}).items():
                self._process_update(
                    new_col_stub,
                    key,
                    ai_value,
                    f"column '{model_name}.{col['name']}'",
                )
            new_columns_list.append(new_col_stub)
        new_model_stub["columns"] = new_columns_list

        sql_path = ai_model_data.get("original_file_path")
        if not sql_path:
            logger.error(
                f"Cannot create stub for '{model_name}': missing 'original_file_path'."
            )
            return False

        target_yaml_path = os.path.join(os.path.dirname(sql_path), "schema.yml")

        if target_yaml_path in self.yaml_files:
            logger.info(
                f"   -> Appending stub to existing file: '{target_yaml_path}'"
            )
            self.yaml_files[target_yaml_path].setdefault("models", []).append(
                new_model_stub
            )
        else:
            logger.info(
                f"   -> Creating new file for stub: '{target_yaml_path}'"
            )
            self.yaml_files[target_yaml_path] = CommentedMap(
                {"version": 2, "models": [new_model_stub]}
            )

        self.files_to_write.add(target_yaml_path)
        return True

    def _process_update(
        self,
        config_node: CommentedMap,
        key: str,
        ai_value: Any,
        node_log_name: str,
    ) -> bool:
        """
        Handles the logic for a single missing key based on the writer's mode.

        This method encapsulates the core decision-making for how to apply
        AI-generated documentation:
        - In 'check' mode, it only logs a warning and indicates a change is needed.
        - In 'interactive' mode, it prompts the user for confirmation/editing.
        - In 'update' or 'drift' mode, it directly applies the change.

        Args:
            config_node: The YAML node (model or column) to update.
            key: The key within the node to update (e.g., 'description').
            ai_value: The AI-generated value for the key.
            node_log_name: A descriptive name for logging purposes (e.g., "model 'my_model'").

        Returns:
            `True` if a change was made or is needed, otherwise `False`.
        """
        log_target = f"'{key}' on {node_log_name}"
        if self.mode == "check":
            logger.warning(f"CI CHECK: Missing {log_target}")
            return True

        if self.mode == "interactive":
            final_value = self._prompt_user_for_change(
                node_log_name, key, str(ai_value)
            )
            if final_value is not None:
                config_node[key] = final_value
                return True
            return False

        # 'update' or 'drift' mode (drift just logs, update writes)
        logger.info(f"- Updating {log_target}")
        config_node[key] = ai_value
        return True

    def _prompt_user_for_change(
        self, node_log_name: str, key: str, ai_value: str
    ) -> str | None:
        """
        Prompts the user to Accept, Edit, or Skip an AI-generated suggestion.

        This method is used exclusively in 'interactive' mode.

        Args:
            node_log_name: A descriptive name for logging purposes.
            key: The key being updated (e.g., 'description').
            ai_value: The AI-generated value to suggest to the user.

        Returns:
            The value to save, or `None` if the user chose to skip.
        """
        typer.echo(
            typer.style(
                f"Suggestion for '{key}' on {node_log_name}:",
                fg=typer.colors.CYAN,
            )
        )
        typer.echo(typer.style(f'  AI: "{ai_value}"', fg=typer.colors.GREEN))
        final_value = typer.prompt(
            "  [Enter] to Accept, type to Edit, or [s] + [Enter] to Skip",
            default=ai_value,
        )

        if final_value.lower() == "s":
            logger.info(f"  - User skipped {key} for {node_log_name}")
            return None
        elif final_value == ai_value:
            logger.info(f"  - User accepted {key} for {node_log_name}")
        else:
            logger.info(f"  - User edited {key} for {node_log_name}")
        return final_value

    def _write_modified_files_to_disk(self):
        """
        Writes all in-memory YAML objects that have been modified to disk.

        This method is called only in 'update' or 'interactive' modes.
        """
        logger.info(f"Writing changes to {len(self.files_to_write)} file(s)...")
        for file_path in self.files_to_write:
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    self.yaml.dump(self.yaml_files[file_path], f)
                logger.info(f"Successfully updated '{file_path}'")
            except IOError as e:
                logger.error(f"Failed to write updates to '{file_path}': {e}")
                raise WriterError(
                    f"Failed to write updates to '{file_path}': {e}"
                ) from e
