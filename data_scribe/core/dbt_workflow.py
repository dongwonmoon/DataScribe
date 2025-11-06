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

    def __init__(
        self,
        dbt_project_dir: str,
        llm_profile: str | None,
        config_path: str,
        output_profile: str | None,
        update_yaml: bool,
        check: bool,
    ):
        self.dbt_project_dir = dbt_project_dir
        self.llm_profile_name = llm_profile
        self.config_path = config_path
        self.output_profile_name = output_profile
        self.update_yaml = update_yaml
        self.check = check
        self.config = load_and_validate_config(self.config_path)

    def run(self):
        llm_profile_name = self.llm_profile_name or self.config.get("default", {}).get(
            "llm"
        )
        llm_client = init_llm(self.config, llm_profile_name)

        logger.info(f"Generating dbt catalog for project: {self.dbt_project_dir}")
        catalog = DbtCatalogGenerator(llm_client).generate_catalog(self.dbt_project_dir)

        if self.check:
            logger.info("Running in --check mode (CI mode)...")
            writer = DbtYamlWriter(self.dbt_project_dir, check_mode=True)
            updates_needed = writer.update_yaml_files(catalog)

            if updates_needed:
                logger.error(
                    "CI CHECK FAILED: dbt documentation is outdated or missing."
                )
                logger.error(
                    "Run 'data-scribe dbt --project-dir ... --update' to fix this."
                )
                raise typer.Exit(code=1)
            else:
                logger.info("CI CHECK PASSED: All dbt documentation is up-to-date.")

        if self.update_yaml:
            logger.info("Updating dbt schema.yml files with AI-generated content...")
            DbtYamlWriter(self.dbt_project_dir).update_yaml_files(catalog)
            logger.info("dbt schema.yml update process complete.")
        elif self.output_profile_name:
            try:
                logger.info(f"Using output profile: '{self.output_profile_name}'")
                writer_params = self.config["output_profiles"][self.output_profile_name]
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
        else:
            logger.info(
                "Catalog generated. No output specified (--output, --update, or --check)."
            )
