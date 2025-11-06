import typer

from data_scribe.core.factory import get_db_connector, get_writer
from data_scribe.core.catalog_generator import CatalogGenerator
from data_scribe.core.workflow_helpers import load_and_validate_config, init_llm
from data_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class DbWorkflow:
    def __init__(
        self,
        config_path: str,
        db_profile: str | None,
        llm_profile: str | None,
        output_profile: str | None,
    ):
        self.config_path = config_path
        self.db_profile_name = db_profile
        self.llm_profile_name = llm_profile
        self.output_profile_name = output_profile
        self.config = load_and_validate_config(self.config_path)

    def run(self):
        # Determine which database and LLM profiles to use
        db_profile_name = self.db_profile_name or self.config.get("default", {}).get(
            "db"
        )
        llm_profile_name = self.llm_profile_name or self.config.get("default", {}).get(
            "llm"
        )

        if not db_profile_name or not llm_profile_name:
            logger.error(
                "Missing profiles. Please specify --db and --llm, or set defaults in config.yaml."
            )
            raise typer.Exit(code=1)

        # Instantiate the database connector
        db_params = self.config["db_connections"][db_profile_name]
        db_type = db_params.pop("type")
        db_connector = get_db_connector(db_type, db_params)

        llm_client = init_llm(self.config, llm_profile_name)

        logger.info("Generating data catalog for the database...")
        catalog = CatalogGenerator(db_connector, llm_client).generate_catalog(
            db_profile_name
        )

        if not self.output_profile_name:
            logger.info(
                "Catalog generated. No --output profile specified, so not writing to a file."
            )
            db_connector.close()
            return

        try:
            writer_params = self.config["output_profiles"][self.output_profile_name]
            writer_type = writer_params.pop("type")
            writer = get_writer(writer_type)

            writer_kwargs = {"db_profile_name": db_profile_name, **writer_params}
            writer.write(catalog, **writer_kwargs)
            logger.info(
                f"Catalog written successfully using output profile: '{self.output_profile_name}'."
            )
        except (KeyError, ValueError, IOError) as e:
            logger.error(
                f"Failed to write catalog using profile '{self.output_profile_name}': {e}"
            )
            raise typer.Exit(code=1)
        finally:
            db_connector.close()
