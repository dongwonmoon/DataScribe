"""
This module defines the command-line interface (CLI) for Data Scribe.

It uses the Typer library to create a simple and intuitive CLI for scanning databases and dbt projects.
The main commands are `db` and `dbt`, which correspond to scanning a database and a dbt project, respectively.
"""

import typer
import yaml
import sys
import functools

from data_scribe.core.factory import get_db_connector, get_llm_client
from data_scribe.core.catalog_generator import CatalogGenerator
from data_scribe.core.dbt_catalog_generator import DbtCatalogGenerator
from data_scribe.utils.writers import (
    MarkdownWriter,
    DbtMarkdownWriter,
    DbtYamlWriter,
)
from data_scribe.utils.utils import load_config
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)

# Create a Typer application instance. This is the main entry point for the CLI.
app = typer.Typer()


def load_and_validate_config(config_path: str):
    """
    Loads and validates the YAML configuration file.

    Args:
        config_path: The path to the configuration file.

    Returns:
        A dictionary containing the loaded configuration.

    Raises:
        typer.Exit: If the file is not found or if there is a parsing error.
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
    Initializes the LLM client based on the provided configuration.

    Args:
        config: The application configuration dictionary.
        llm_profile_name: The name of the LLM profile to use.

    Returns:
        An instance of a class that implements the BaseLLMClient interface.

    Raises:
        typer.Exit: If the LLM configuration is missing or invalid.
    """
    try:
        # Get the parameters for the specified LLM provider
        llm_params = config["llm_providers"][llm_profile_name]
        # The 'provider' key is used to determine which client to instantiate
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
    A decorator to handle common exceptions in a centralized way.

    This removes the need for repetitive try/except blocks in each command function.
    It catches common errors like configuration issues and connection problems.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            # Execute the decorated function
            return func(*args, **kwargs)
        except (KeyError, ValueError, ConnectionError) as e:
            # Handle expected errors gracefully
            logger.error(f"{type(e).__name__}: {e}")
            raise typer.Exit(code=1)
        except Exception as e:
            # Handle any unexpected errors
            logger.error(f"Unexpected error: {e}", exc_info=True)
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
    output_filename: str = typer.Option(
        "db_catalog.md", "--output", help="The name of the output file."
    ),
):
    """
    Scans a database schema, generates a data catalog using an LLM, and writes it to a Markdown file.
    """
    # Load and validate the main configuration file
    config = load_and_validate_config(config_path)

    # Determine which database and LLM profiles to use, either from the command line or the config file's defaults
    db_profile_name = db_profile or config.get("default", {}).get("db")
    llm_profile_name = llm_profile or config.get("default", {}).get("llm")
    if not db_profile_name or not llm_profile_name:
        logger.error(
            "Missing profiles: specify --db/--llm or set defaults in config.yaml."
        )
        raise typer.Exit(code=1)

    # Get the database connection parameters and instantiate the connector
    db_params = config["db_connections"][db_profile_name]
    db_type = db_params.pop("type")
    db_connector = get_db_connector(db_type, db_params)

    # Initialize the LLM client
    llm_client = init_llm(config, llm_profile_name)

    logger.info("Generating data catalog...")
    # Create a CatalogGenerator and generate the catalog
    catalog = CatalogGenerator(db_connector, llm_client).generate_catalog(
        db_profile_name
    )
    # Write the generated catalog to a Markdown file
    MarkdownWriter().write(catalog, output_filename, db_profile_name)
    logger.info(f"Catalog written to '{output_filename}'.")
    # Ensure the database connection is closed
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
    output_filename: str = typer.Option(
        "dbt_catalog.md", "--output", help="The name of the output file."
    ),
    update_yaml: bool = typer.Option(
        False,
        "--update",
        help="Update the dbt schema.yml file directly with the AI description.",
    ),
    check: bool = typer.Option(
        False,
        "--check",
        help="Run in CI mode. Fails if documentation is outdated or missing.",
    ),
):
    """
    Scans a dbt project, generates a data catalog using an LLM, and writes it to a Markdown file.
    """
    # Load and validate the main configuration file
    config = load_and_validate_config(config_path)

    # Determine which LLM profile to use
    llm_profile_name = llm_profile or config.get("default", {}).get("llm")
    llm_client = init_llm(config, llm_profile_name)

    logger.info("Generating dbt catalog...")
    # Create a DbtCatalogGenerator and generate the catalog
    catalog = DbtCatalogGenerator(llm_client).generate_catalog(dbt_project_dir)

    if check:
        logger.info("Running in --check mode (CI mode)...")
        writer = DbtYamlWriter(dbt_project_dir, check_mode=True)
        updates_needed = writer.update_yaml_files(catalog)

        if updates_needed:
            logger.error("CI CHECK FAILED: Documentation is outdated or missing.")
            logger.error("Run 'data-scribe dbt --project-dir ... --update' to fix.")
            raise typer.Exit(code=1)
        else:
            logger.info("CI CHECK PASSED: All dbt documentation is up-to-date.")

    if update_yaml:
        logger.info("Starting dbt schema.yml update process...")
        DbtYamlWriter(dbt_project_dir).update_yaml_files(catalog)
        logger.info("dbt schema.yml update complete.")
    else:
        # Write the generated catalog to a Markdown file
        DbtMarkdownWriter().write(catalog, output_filename, dbt_project_dir)
        logger.info(f"DBT catalog written to '{output_filename}'.")
