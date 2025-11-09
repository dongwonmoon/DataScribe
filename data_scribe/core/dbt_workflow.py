"""
This module defines the workflow for the 'dbt' command.

It encapsulates the logic for parsing a dbt project, generating a catalog,
and writing or updating dbt documentation, orchestrated by the `DbtWorkflow` class.
"""

from typing import Optional
import typer

from data_scribe.core.factory import get_writer
from data_scribe.core.dbt_catalog_generator import DbtCatalogGenerator
from data_scribe.components.writers import DbtYamlWriter
from data_scribe.core.workflow_helpers import (
    load_and_validate_config,
    init_llm,
)
from data_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class DbtWorkflow:
    """
    Manages the entire workflow for the 'dbt' command.

    This class is responsible for loading configuration, initializing components,
    generating the dbt catalog, and handling the different output modes like
    writing to a file, updating dbt YAML files, or running a CI check.
    """

    def __init__(
        self,
        dbt_project_dir: str,
        llm_profile: Optional[str],
        config_path: str,
        output_profile: Optional[str],
        update_yaml: bool,
        check: bool,
        interactive: bool,
    ):
        """
        Initializes the DbtWorkflow with parameters from the CLI.

        Args:
            dbt_project_dir: The path to the dbt project directory.
            llm_profile: The name of the LLM profile to use.
            config_path: The path to the configuration file.
            output_profile: The name of the output profile to use.
            update_yaml: Flag to update dbt schema.yml files directly.
            check: Flag to run in CI mode to check for outdated documentation.
            interactive: Flag to prompt the user for each AI-generated change.
        """
        self.dbt_project_dir = dbt_project_dir
        self.llm_profile_name = llm_profile
        self.config_path = config_path
        self.output_profile_name = output_profile
        self.update_yaml = update_yaml
        self.check = check
        self.interactive = interactive
        self.config = load_and_validate_config(self.config_path)

    def run(self):
        """
        Executes the dbt scanning and documentation workflow.

        This method orchestrates the following steps:
        1. Initializes the LLM client.
        2. Generates a catalog of the dbt project.
        3. Executes one of the output modes based on CLI flags:
           - `--check`: Verifies if dbt documentation is up-to-date and exits with an error if not.
           - `--update`: Directly updates the dbt `schema.yml` files with generated content.
           - `--output`: Writes the generated catalog to a file (e.g., Markdown) using a specified profile.
        """
        # 1. Initialize the LLM client from the specified or default profile.
        llm_profile_name = self.llm_profile_name or self.config.get(
            "default", {}
        ).get("llm")
        llm_client = init_llm(self.config, llm_profile_name)

        # 2. Generate the dbt project catalog.
        logger.info(
            f"Generating dbt catalog for project: {self.dbt_project_dir}"
        )
        catalog = DbtCatalogGenerator(llm_client).generate_catalog(
            self.dbt_project_dir
        )

        # Determine the action mode based on flags
        action_mode = None
        if self.check:
            action_mode = "check"
        elif self.interactive:
            action_mode = "interactive"
        elif self.update_yaml:
            action_mode = "update"

        # 3. Handle dbt YAML writing modes (check, interactive, update)
        if action_mode:
            logger.info(f"Running in --{action_mode} mode...")
            writer = DbtYamlWriter(
                dbt_project_dir=self.dbt_project_dir, mode=action_mode
            )
            updates_needed = writer.update_yaml_files(catalog)

            if action_mode == "check":
                if updates_needed:
                    logger.error(
                        "CI CHECK FAILED: dbt documentation is outdated or missing."
                    )
                    logger.error(
                        "Run 'data-scribe dbt --project-dir ... --update' or --interactive to fix."
                    )
                    raise typer.Exit(code=1)
                else:
                    logger.info(
                        "CI CHECK PASSED: All dbt documentation is up-to-date."
                    )
            else:
                logger.info(f"dbt schema.yml {action_mode} process complete.")

        # The --output flag writes the catalog to an external file (e.g., Markdown).
        # This is skipped if --update is used.
        elif self.output_profile_name:
            try:
                logger.info(
                    f"Using output profile: '{self.output_profile_name}'"
                )
                writer_params = self.config["output_profiles"][
                    self.output_profile_name
                ]
                writer_type = writer_params.pop("type")
                writer = get_writer(writer_type)

                writer_kwargs = {
                    "project_name": self.dbt_project_dir,
                    **writer_params,
                }
                writer.write(catalog, **writer_kwargs)
                logger.info(
                    f"dbt catalog written successfully using profile: '{self.output_profile_name}'."
                )
            except (KeyError, ValueError, IOError) as e:
                logger.error(
                    f"Failed to write catalog using profile '{self.output_profile_name}': {e}"
                )
                raise typer.Exit(code=1)

        # If no output-related flags are provided, log a message and exit.
        else:
            logger.info(
                "Catalog generated. No output specified (--output, --update, or --check)."
            )
