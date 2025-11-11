"""
This module defines the `DbtWorkflow` class, which encapsulates the complex,
multi-mode logic for the `schema-scribe dbt` command.

Design Rationale:
The `DbtWorkflow` is designed as a central orchestrator for dbt documentation
tasks. It adheres to the principle of dependency injection, meaning it receives
fully initialized components (LLM client, DB connector, writer) rather than
creating them itself. This promotes:
- **Modularity**: Components can be easily swapped or tested independently.
- **Testability**: The workflow logic can be tested in isolation with mock
  dependencies.
- **Flexibility**: The same workflow can operate with different implementations
  of connectors, LLM clients, or writers.
The workflow supports various modes (`--update`, `--check`, `--interactive`,
`--drift`) to cater to different use cases, from automated CI checks to
interactive documentation generation.
"""

from typing import Dict, Any, Optional
import typer

from schema_scribe.core.interfaces import (
    BaseConnector,
    BaseLLMClient,
    BaseWriter,
)
from schema_scribe.services.dbt_catalog_generator import DbtCatalogGenerator
from schema_scribe.components.writers.dbt_yaml_writer import DbtYamlWriter
from schema_scribe.core.exceptions import CIError
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class DbtWorkflow:
    """
    Manages the end-to-end workflow for the `schema-scribe dbt` command.

    This class acts as a state machine, orchestrating the dbt documentation
    process based on the combination of CLI flags provided by the user. It
    receives initialized components (connectors, clients, writers) and focuses
    purely on the workflow logic, delegating component creation and configuration
    to the `ConfigManager`.
    """

    def __init__(
        self,
        llm_client: BaseLLMClient,
        db_connector: Optional[BaseConnector],
        writer: Optional[BaseWriter],
        dbt_project_dir: str,
        update_yaml: bool,
        check: bool,
        interactive: bool,
        drift: bool,
        db_profile_name: Optional[str],
        output_profile_name: Optional[str],
        writer_params: Dict[str, Any],
    ):
        """
        Initializes the DbtWorkflow with component instances and CLI flags.

        Args:
            llm_client: An initialized LLM client instance.
            db_connector: An initialized DB connector instance, required for `--drift` mode.
            writer: An initialized writer instance for file output (e.g., Markdown, JSON).
            dbt_project_dir: The absolute path to the root of the dbt project directory.
            update_yaml: Boolean flag indicating if `--update` mode is active.
            check: Boolean flag indicating if `--check` mode is active.
            interactive: Boolean flag indicating if `--interactive` mode is active.
            drift: Boolean flag indicating if `--drift` mode is active.
            db_profile_name: The name of the database profile used, for logging.
            output_profile_name: The name of the output profile used, for logging.
            writer_params: Additional parameters to pass to the writer's `write` method.
        """
        self.llm_client = llm_client
        self.db_connector = db_connector
        self.writer = writer
        self.dbt_project_dir = dbt_project_dir
        self.update_yaml = update_yaml
        self.check = check
        self.interactive = interactive
        self.drift = drift
        self.db_profile_name = db_profile_name
        self.output_profile_name = output_profile_name
        self.writer_params = writer_params

    def run(self):
        """
        Executes the dbt scanning and documentation workflow using the injected components.

        The workflow proceeds through the following stages:
        1.  **Validation**: Checks for necessary conditions (e.g., `db_connector` for `--drift` mode).
        2.  **Catalog Generation**: Uses the injected `llm_client` and `db_connector` to
            generate the dbt catalog, optionally including drift detection.
        3.  **Action Mode Determination**: Based on CLI flags, decides whether to:
            a.  Update `schema.yml` files (`--update`, `--check`, `--interactive`, `--drift`).
            b.  Output the catalog to a file using the injected `writer`.
        4.  **Resource Cleanup**: Closes the database connection if one was used.

        Raises:
            typer.Exit: If required conditions for a mode are not met.
            CIError: If a CI check (`--check` or `--drift`) fails.
        """
        # Validation for --drift mode
        if self.drift and not self.db_connector:
            logger.error(
                "Drift mode requires a --db profile, "
                "but no db_connector was provided."
            )
            raise typer.Exit(code=1)

        # 1. Generate Catalog using injected components
        logger.info(
            f"Generating dbt catalog for project: {self.dbt_project_dir}"
        )
        catalog_gen = DbtCatalogGenerator(
            llm_client=self.llm_client, db_connector=self.db_connector
        )
        catalog = catalog_gen.generate_catalog(
            dbt_project_dir=self.dbt_project_dir, run_drift_check=self.drift
        )

        # 2. Determine and execute action mode
        action_mode = None
        if self.drift:
            action_mode = "drift"
        elif self.check:
            action_mode = "check"
        elif self.interactive:
            action_mode = "interactive"
        elif self.update_yaml:
            action_mode = "update"

        if action_mode:
            # DbtYamlWriter is specific to dbt workflow logic, so it's instantiated here.
            self._handle_yaml_update(action_mode, catalog)
        elif self.writer:
            # Perform file output only if an external writer is injected.
            self._handle_file_output(catalog)
        else:
            logger.info(
                "Catalog generated. No output specified (--output, --update, or --check)."
            )

        # 3. Resource cleanup
        if self.db_connector:
            logger.info(f"Closing DB connection for {self.db_profile_name}...")
            self.db_connector.close()

    def _handle_yaml_update(self, mode: str, catalog: dict):
        """
        Handles modes that interact with dbt `schema.yml` files.

        This method instantiates `DbtYamlWriter` with the appropriate mode
        and processes the catalog. It also manages the CI check logic for
        `--check` and `--drift` modes, raising `CIError` if issues are found.

        Args:
            mode: The operational mode ('update', 'check', 'interactive', 'drift').
            catalog: The generated dbt catalog data.

        Raises:
            CIError: If a CI check fails (e.g., documentation is outdated or drift detected).
        """
        logger.info(f"Running in --{mode} mode...")
        writer = DbtYamlWriter(dbt_project_dir=self.dbt_project_dir, mode=mode)
        updates_needed = writer.write(catalog)

        if mode in ["check", "drift"]:
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
            logger.info(f"dbt schema.yml {mode} process complete.")

    def _handle_file_output(self, catalog: dict):
        """
        Writes the catalog to a file using the injected writer.

        This method prepares the necessary `kwargs` for the injected `writer`
        and calls its `write` method.

        Args:
            catalog: The generated dbt catalog data.

        Raises:
            typer.Exit: If writing to the file fails.
        """
        try:
            logger.info(f"Using output profile: '{self.output_profile_name}'")

            writer_kwargs = {
                "project_name": self.dbt_project_dir,
                **self.writer_params,
            }
            self.writer.write(catalog, **writer_kwargs)
            logger.info(
                f"dbt catalog written successfully using profile: '{self.output_profile_name}'."
            )
        except (KeyError, ValueError, IOError) as e:
            logger.error(
                f"Failed to write catalog using profile '{self.output_profile_name}': {e}"
            )
            raise typer.Exit(code=1)
