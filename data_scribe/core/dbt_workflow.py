"""
This module defines the workflow for the 'dbt' command.

It encapsulates the logic for parsing a dbt project, generating a catalog,
and writing or updating dbt documentation, orchestrated by the `DbtWorkflow` class.
"""

from typing import Optional
import typer

from data_scribe.core.factory import get_writer, get_db_connector
from data_scribe.core.dbt_catalog_generator import DbtCatalogGenerator
from data_scribe.components.writers.dbt_yaml_writer import DbtYamlWriter
from data_scribe.core.workflow_helpers import (
    load_config_from_path,
    init_llm,
)
from data_scribe.core.exceptions import CIError
from data_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class DbtWorkflow:
    """
    Manages the end-to-end workflow for the `data-scribe dbt` command.

    This class is responsible for loading configuration, initializing components,
    generating the dbt catalog, and handling the different output modes like
    writing to a file, updating dbt YAML files, or running a CI check.
    """

    def __init__(
        self,
        dbt_project_dir: str,
        db_profile: str | None,
        llm_profile: Optional[str],
        config_path: str,
        output_profile: Optional[str],
        update_yaml: bool,
        check: bool,
        interactive: bool,
        drift: bool,
    ):
        """
        Initializes the DbtWorkflow with parameters from the CLI.

        Args:
            dbt_project_dir: The path to the dbt project directory.
            db_profile: The name of the database profile to use (required for drift).
            llm_profile: The name of the LLM profile to use.
            config_path: The path to the configuration file.
            output_profile: The name of the output profile to use.
            update_yaml: Flag to update dbt schema.yml files directly.
            check: Flag to run in CI mode to check for outdated documentation.
            interactive: Flag to prompt the user for each AI-generated change.
            drift: Flag to run in drift detection mode.
        """
        self.dbt_project_dir = dbt_project_dir
        self.db_profile_name = db_profile
        self.llm_profile_name = llm_profile
        self.config_path = config_path
        self.output_profile_name = output_profile
        self.update_yaml = update_yaml
        self.check = check
        self.interactive = interactive
        self.drift = drift
        self.config = load_config_from_path(self.config_path)

    def run(self):
        """
        Executes the dbt scanning and documentation workflow.

        This method orchestrates the following steps:
        1. Initializes the LLM client.
        2. If in drift mode, initializes a DB connector.
        3. Generates a catalog of the dbt project using `DbtCatalogGenerator`.
        4. Executes one of the output modes based on CLI flags:
           - `--drift`: Checks for inconsistencies between docs and live data.
           - `--check`: Verifies if dbt documentation is missing.
           - `--update` or `--interactive`: Updates `schema.yml` files.
           - `--output`: Writes the catalog to a file (e.g., Markdown).
        """
        # 1. Initialize the LLM client from the specified or default profile.
        llm_profile_name = self.llm_profile_name or self.config.get(
            "default", {}
        ).get("llm")
        llm_client = init_llm(self.config, llm_profile_name)

        db_connector = None
        if self.drift:
            if not self.db_profile_name:
                logger.error("Drift mode requires a --db profile")
                raise typer.Exit(code=1)

            try:
                logger.info(
                    f"Initializing DB connection '{self.db_profile_name}' for drift check..."
                )
                db_params = self.config["db_connections"][self.db_profile_name]
                db_type = db_params.pop("type")
                db_connector = get_db_connector(db_type, db_params)
            except Exception as e:
                logger.error(
                    f"Failed to connect to database for drift check: {e}"
                )
                raise typer.Exit(code=1)

        # 2. Generate the dbt project catalog.
        logger.info(
            f"Generating dbt catalog for project: {self.dbt_project_dir}"
        )
        catalog_gen = DbtCatalogGenerator(
            llm_client=llm_client, db_connector=db_connector
        )
        catalog = catalog_gen.generate_catalog(
            dbt_project_dir=self.dbt_project_dir, run_drift_check=self.drift
        )

        # Determine the action mode based on flags
        action_mode = None
        if self.drift:
            action_mode = "drift"
        elif self.check:
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

            if action_mode in ["check", "drift"]:
                if updates_needed:
                    log_msg = (
                        "documentation is outdated"
                        if self.check
                        else "documentation drift was detected"
                    )
                    logger.error(f"CI CHECK FAILED: {log_msg}.")
                    raise CIError(f"CI CHECK FAILED: {log_msg}.")
                else:
                    log_msg = "is up-to-date" if self.check else "has no drift"
                    logger.info(
                        f"CI CHECK PASSED: All dbt documentation {log_msg}."
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
