"""
This module defines the command-line interface (CLI) for the Data Scribe application.

It uses the Typer library to create a simple and intuitive CLI for scanning databases
and dbt projects. The main commands, `db` and `dbt`, orchestrate the process of
connecting to data sources, generating documentation with an LLM, and writing the
output to various formats.
"""

import typer
import functools

from data_scribe.core.db_workflow import DbWorkflow
from data_scribe.core.dbt_workflow import DbtWorkflow
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)

# Create a Typer application instance, which serves as the main entry point for the CLI.
app = typer.Typer()


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
        None,
        "--output",
        help="The output profile name from config.yaml to use for writing the catalog.",
    ),
):
    """
    Scans a database schema, generates a data catalog using an LLM, and writes it to a specified output.
    """
    DbtWorkflow(
        config_path=config_path,
        db_profile=db_profile,
        llm_profile=llm_profile,
        output_profile=output_profile,
    ).run()


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
        None,
        "--output",
        help="The output profile name from config.yaml for writing the catalog.",
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
    DbtWorkflow(
        dbt_project_dir=dbt_project_dir,
        llm_profile=llm_profile,
        config_path=config_path,
        output_profile=output_profile,
        update_yaml=update_yaml,
        check=check,
    ).run()
