"""
This module defines the command-line interface (CLI) for the Data Scribe application.

It uses the Typer library to create a simple and intuitive CLI for scanning databases
and dbt projects. The main commands, `db` and `dbt`, orchestrate the process of
connecting to data sources, generating documentation with an LLM, and writing the
output to various formats.
"""

import typer
import yaml
import sys
import functools

from data_scribe.core.factory import get_db_connector, get_llm_client, get_writer
from data_scribe.core.catalog_generator import CatalogGenerator
from data_scribe.core.dbt_catalog_generator import DbtCatalogGenerator
from data_scribe.utils.writers import DbtYamlWriter
from data_scribe.utils.utils import load_config
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)

# Create a Typer application instance, which serves as the main entry point for the CLI.
app = typer.Typer()


def load_and_validate_config(config_path: str):
    """
    Loads and validates the YAML configuration file from the given path.

    Args:
        config_path: The path to the `config.yaml` file.

    Returns:
        A dictionary containing the loaded and parsed configuration.

    Raises:
        typer.Exit: If the file is not found or if there is an error parsing the YAML.
    """
    try:
        logger.info(f"Loading configuration from '{config_path}'...")
        config = load_config(config_path)
        logger.info("Configuration loaded successfully.")
        return config
    except FileNotFoundError:
        logger.error(f"Configuration file not found at '{config_path}'.")
        raise typer.Exit(code=1)
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file: {e}")
        raise typer.Exit(code=1)


def init_llm(config, llm_profile_name: str):
    """
    Initializes the LLM client based on the specified profile in the configuration.

    Args:
        config: The application configuration dictionary.
        llm_profile_name: The name of the LLM profile to use (e.g., 'openai_default').

    Returns:
        An instance of a class that implements the BaseLLMClient interface.

    Raises:
        typer.Exit: If the specified LLM profile or its configuration is missing or invalid.
    """
    try:
        # Get the parameters for the specified LLM provider from the config
        llm_params = config["llm_providers"][llm_profile_name]
        # The 'provider' key determines which client class to instantiate
        llm_provider = llm_params.pop("provider")
        logger.info(f"Initializing LLM provider '{llm_provider}'...")
        # Use the factory to get an instance of the LLM client
        llm_client = get_llm_client(llm_provider, llm_params)
        logger.info("LLM client initialized successfully.")
        return llm_client
    except KeyError as e:
        logger.error(f"Missing LLM configuration key: {e}")
        raise typer.Exit(code=1)


def handle_exceptions(func):
    """
    A decorator that provides centralized error handling for CLI commands.

    This wrapper catches common exceptions (e.g., configuration errors, connection
    issues) and logs them in a user-friendly format before exiting the application.
    This avoids repetitive try/except blocks in each command function.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            # Execute the decorated command function
            return func(*args, **kwargs)
        except (KeyError, ValueError, ConnectionError) as e:
            # Handle expected application errors gracefully
            logger.error(f"{type(e).__name__}: {e}")
            raise typer.Exit(code=1)
        except Exception as e:
            # Handle any unexpected errors to prevent unhandled stack traces
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)
            raise typer.Exit(code=1)

    return wrapper


@app.command(name="db")
@handle_exceptions
def scan_db(
    db_profile: str = typer.Option(
        None,
        "--db",
        help="The name of the database profile to use from config.yaml.",
    ),
    llm_profile: str = typer.Option(
        None,
        "--llm",
        help="The name of the LLM profile to use from config.yaml.",
    ),
    config_path: str = typer.Option(
        "config.yaml", "--config", help="The path to the configuration file."
    ),
    output_profile: str = typer.Option(
        None, "--output", help="The output profile name from config.yaml to use for writing the catalog."
    ),
):
    """
    Scans a database schema, generates a data catalog using an LLM, and writes it to a specified output.
    """
    config = load_and_validate_config(config_path)

    # Determine which database and LLM profiles to use, falling back to defaults if not provided
    db_profile_name = db_profile or config.get("default", {}).get("db")
    llm_profile_name = llm_profile or config.get("default", {}).get("llm")
    if not db_profile_name or not llm_profile_name:
        logger.error(
            "Missing profiles. Please specify --db and --llm, or set defaults in config.yaml."
        )
        raise typer.Exit(code=1)

    # Instantiate the database connector using the factory
    db_params = config["db_connections"][db_profile_name]
    db_type = db_params.pop("type")
    db_connector = get_db_connector(db_type, db_params)

    llm_client = init_llm(config, llm_profile_name)

    logger.info("Generating data catalog for the database...")
    catalog = CatalogGenerator(db_connector, llm_client).generate_catalog(
        db_profile_name
    )

    if not output_profile:
        logger.info("Catalog generated. No --output profile specified, so not writing to a file.")
        db_connector.close()
        return

    try:
        writer_params = config["output_profiles"][output_profile]
        writer_type = writer_params.pop("type")
        writer = get_writer(writer_type)

        # Pass necessary context and parameters to the writer
        writer_kwargs = {"db_profile_name": db_profile_name, **writer_params}
        writer.write(catalog, **writer_kwargs)
        logger.info(f"Catalog written successfully using output profile: '{output_profile}'.")
    except (KeyError, ValueError, IOError) as e:
        logger.error(f"Failed to write catalog using profile '{output_profile}': {e}")
        raise typer.Exit(code=1)
    finally:
        db_connector.close()


@app.command(name="dbt")
@handle_exceptions
def scan_dbt(
    dbt_project_dir: str = typer.Option(
        ..., "--project-dir", help="The path to the dbt project directory."
    ),
    llm_profile: str = typer.Option(
        None,
        "--llm",
        help="The name of the LLM profile to use from config.yaml.",
    ),
    config_path: str = typer.Option(
        "config.yaml", "--config", help="The path to the configuration file."
    ),
    output_profile: str = typer.Option(
        None, "--output", help="The output profile name from config.yaml for writing the catalog."
    ),
    update_yaml: bool = typer.Option(
        False,
        "--update",
        help="Update the dbt schema.yml files directly with AI-generated content.",
    ),
    check: bool = typer.Option(
        False,
        "--check",
        help="Run in CI mode. Fails if dbt documentation is outdated or missing.",
    ),
):
    """
    Scans a dbt project, generates a data catalog, and manages dbt documentation.

    This command can:
    - Generate a catalog file in various formats (e.g., Markdown, Confluence).
    - Directly update dbt `schema.yml` files with AI-generated content.
    - Run in a CI check mode to verify if documentation is up-to-date.
    """
    config = load_and_validate_config(config_path)

    llm_profile_name = llm_profile or config.get("default", {}).get("llm")
    llm_client = init_llm(config, llm_profile_name)

    logger.info(f"Generating dbt catalog for project: {dbt_project_dir}")
    catalog = DbtCatalogGenerator(llm_client).generate_catalog(dbt_project_dir)

    if check:
        logger.info("Running in --check mode (CI mode)...")
        writer = DbtYamlWriter(dbt_project_dir, check_mode=True)
        updates_needed = writer.update_yaml_files(catalog)

        if updates_needed:
            logger.error("CI CHECK FAILED: dbt documentation is outdated or missing.")
            logger.error("Run 'data-scribe dbt --project-dir ... --update' to fix this.")
            raise typer.Exit(code=1)
        else:
            logger.info("CI CHECK PASSED: All dbt documentation is up-to-date.")

    if update_yaml:
        logger.info("Updating dbt schema.yml files with AI-generated content...")
        DbtYamlWriter(dbt_project_dir).update_yaml_files(catalog)
        logger.info("dbt schema.yml update process complete.")
    elif output_profile:
        try:
            logger.info(f"Using output profile: '{output_profile}'")
            # 1. Retrieve the writer parameters from the specified output profile in config.yaml
            writer_params = config["output_profiles"][output_profile]
            writer_type = writer_params.pop("type")  # e.g., "dbt-markdown", "confluence"

            # 2. Instantiate the appropriate writer using the factory
            writer = get_writer(writer_type)

            # 3. Prepare the arguments for the writer's `write` method.
            #    This includes common context like the project name and the specific
            #    parameters from the output profile (e.g., filename, URL).
            writer_kwargs = {
                "project_name": dbt_project_dir,
                **writer_params,
            }
            writer.write(catalog, **writer_kwargs)
            logger.info(
                f"dbt catalog written successfully using profile: '{output_profile}'."
            )
        except (KeyError, ValueError, IOError) as e:
            logger.error(
                f"Failed to write catalog using profile '{output_profile}': {e}"
            )
            raise typer.Exit(code=1)
    else:
        logger.info(
            "Catalog generated. No output specified (--output, --update, or --check)."
        )
