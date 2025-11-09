"""
This module defines the workflow for the 'db' command.

It encapsulates the logic for connecting to a database, generating a catalog,
and writing the output, orchestrated by the `DbWorkflow` class.
"""

from typing import Optional
import typer

from data_scribe.core.factory import get_db_connector, get_writer
from data_scribe.core.catalog_generator import CatalogGenerator
from data_scribe.core.workflow_helpers import load_config_from_path, init_llm
from data_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class DbWorkflow:
    """
    Manages the end-to-end workflow for the `data-scribe db` command.

    This class is responsible for loading configuration, initializing components
    (database connector, LLM client, writer), orchestrating the catalog
    generation process, and handling the final output.
    """

    def __init__(
        self,
        config_path: str,
        db_profile: Optional[str],
        llm_profile: Optional[str],
        output_profile: Optional[str],
    ):
        """
        Initializes the DbWorkflow with parameters from the CLI.

        Args:
            config_path: The path to the configuration file.
            db_profile: The name of the database profile to use.
            llm_profile: The name of the LLM profile to use.
            output_profile: The name of the output profile to use.
        """
        self.config_path = config_path
        self.db_profile_name = db_profile
        self.llm_profile_name = llm_profile
        self.output_profile_name = output_profile
        self.config = load_config_from_path(self.config_path)

    def run(self):
        """
        Executes the database scanning and documentation workflow.

        This method orchestrates the following steps:
        1. Determines the correct database and LLM profiles to use.
        2. Initializes the database connector and LLM client via the factory.
        3. Instantiates the `CatalogGenerator` and runs it to produce the data catalog.
        4. Initializes and uses a writer to save the catalog if an output
           profile is specified.
        5. Ensures the database connection is closed after the operation.
        """
        # 1. Determine which database and LLM profiles to use.
        # Priority is given to CLI arguments, falling back to defaults in the config file.
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

        # 2. Instantiate the database connector and LLM client.
        db_params = self.config["db_connections"][db_profile_name]
        db_type = db_params.pop("type")
        db_connector = get_db_connector(db_type, db_params)

        llm_client = init_llm(self.config, llm_profile_name)

        # 3. Generate the data catalog.
        logger.info("Generating data catalog for the database...")
        catalog = CatalogGenerator(db_connector, llm_client).generate_catalog(
            db_profile_name
        )

        # 4. Write the catalog to the specified output, if provided.
        if not self.output_profile_name:
            logger.info(
                "Catalog generated. No --output profile specified, so not writing to a file."
            )
            db_connector.close()
            return

        try:
            # Initialize the writer based on the output profile.
            writer_params = self.config["output_profiles"][self.output_profile_name]
            writer_type = writer_params.pop("type")
            writer = get_writer(writer_type)

            # Prepare arguments for the writer.
            writer_kwargs = {
                "db_profile_name": db_profile_name,
                "db_connector": db_connector,
                **writer_params,
            }
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
            # 5. Ensure the database connection is closed.
            db_connector.close()
