"""
This module defines the command-line interface (CLI) for the Data Scribe application.

It uses the Typer library to create a simple and intuitive CLI for scanning databases
and dbt projects. The main commands, `db` and `dbt`, orchestrate the process of
connecting to data sources, generating documentation with an LLM, and writing the
output to various formats.
"""

import typer
import functools
import os
import yaml
from typing import Dict, Any

from data_scribe.core.db_workflow import DbWorkflow
from data_scribe.core.dbt_workflow import DbtWorkflow
from data_scribe.core.exceptions import (
    DataScribeError,
    ConnectorError,
    ConfigError,
    LLMClientError,
    WriterError,
)
from data_scribe.core.factory import DB_CONNECTOR_REGISTRY, LLM_CLIENT_REGISTRY
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)

# Create a Typer application instance, which serves as the main entry point for the CLI.
app = typer.Typer()

CONFIG_FILE = "config.yaml"
ENV_FILE = ".env"


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
            return func(*args, **kwargs)
        except (ConfigError, ConnectorError, LLMClientError, WriterError) as e:
            logger.error(f"{type(e).__name__}: {e}")
            raise typer.Exit(code=1)
        except DataScribeError as e:
            logger.error(
                f"An unexpected application error occurred: {e}", exc_info=True
            )
            raise typer.Exit(code=1)
        except Exception as e:
            # Handle any unexpected (non-DataScribe) errors
            logger.error(f"An unknown unexpected error occurred: {e}", exc_info=True)
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
    DbWorkflow(
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


@app.command(name="init")
@handle_exceptions
def init_config():
    if os.path.exists(CONFIG_FILE):
        overwrite = typer.confirm(
            f"'{CONFIG_FILE}' already exists. Overwrite?", default=False
        )
        if not overwrite:
            logger.info("Aborting configuration initialization.")
            raise typer.Exit(code=0)

    logger.info(f"Creating '{CONFIG_FILE}'...")

    config_data: Dict[str, Any] = {
        "default": {},
        "db_connections": {},
        "llm_providers": {},
        "output_profiles": {},
    }
    env_data: Dict[str, str] = {}

    # DB Connections
    logger.info(
        typer.style(
            "\n--- 1. Database Connections ---", fg=typer.colors.CYAN, bold=True
        )
    )
    db_types_to_add = []
    available_db_types = list(DB_CONNECTOR_REGISTRY.keys())

    for db_type in available_db_types:
        if typer.confirm(f"Would you like to add a '{db_type}' connection?"):
            db_types_to_add.append(db_type)

    for db_type in db_types_to_add:
        profile_name = typer.prompt(
            f"\nProfile name for the '{db_type}' connection (e.g. dev_{db_type})"
        )
        params = {"type": db_type}

        if db_type == "sqlite":
            params["path"] = typer.prompt(
                "Path to the SQLite database file", default="test.db"
            )

        elif db_type in ["postgres", "mariadb", "mysql"]:
            params["host"] = typer.prompt("Host", default="localhost")
            params["port"] = typer.prompt(
                "Port", default=5432 if db_type == "postgres" else 3306
            )
            params["user"] = typer.prompt("User")
            pw = typer.prompt(
                f"Password (sensitive information, stored in .env)", hide_input=True
            )
            env_key = f"{profile_name.upper()}_PASSWORD"
            params["password"] = f"${{{env_key}}}"
            env_data[env_key] = pw
            params["dbname"] = typer.prompt("Database (dbname)")
            if db_type == "postgres":
                params["schema"] = typer.prompt("Schema (optional)", default="public")

        elif db_type == "snowflake":
            params["account"] = typer.prompt("Account (예: xy12345.ap-northeast-2.aws)")
            params["user"] = typer.prompt("User")
            pw = typer.prompt(
                f"Password (sensitive information, stored in .env)", hide_input=True
            )
            env_key = f"{profile_name.upper()}_PASSWORD"
            params["password"] = f"${{{env_key}}}"
            env_data[env_key] = pw
            params["warehouse"] = typer.prompt("Warehouse")
            params["database"] = typer.prompt("Database")
            params["schema"] = typer.prompt("Schema", default="PUBLIC")

        elif db_type == "duckdb":
            params["path"] = typer.prompt(
                "DB file or file pattern path (e.g. ./logs/*.parquet, s3://...)",
                default="local.db",
            )

        config_data["db_connections"][profile_name] = params

    # LLM Providers
    logger.info(
        typer.style("\n--- 2. LLM Providers ---", fg=typer.colors.CYAN, bold=True)
    )
    llm_types_to_add = []
    avalible_llm_types = list(LLM_CLIENT_REGISTRY.keys())

    for llm_type in avalible_llm_types:
        if typer.confirm(f"Would you like to add a '{llm_type}' provider?"):
            llm_types_to_add.append(llm_type)

    for llm_type in llm_types_to_add:
        profile_name = typer.prompt(
            f"\n'{llm_type}' provider's profile name (e.g. {llm_type}_prod)"
        )
        params = {"provider": llm_type}

        if llm_type == "openai":
            params["model"] = typer.prompt("Model", default="gpt-3.5-turbo")
            if "OPENAI_API_KEY" not in env_data:
                key = typer.prompt(
                    "OpenAI API Key (sensitive information, stored in .env)",
                    hide_input=True,
                )
                env_data["OPENAI_API_KEY"] = key

        elif llm_type == "google":
            params["model"] = typer.prompt("Model", default="gemini-2.5-flash")
            if "GOOGLE_API_KEY" not in env_data:
                key = typer.prompt(
                    "Google API Key (sensitive information, stored in .env)",
                    hide_input=True,
                )
                env_data["GOOGLE_API_KEY"] = key

        elif llm_type == "ollama":
            params["model"] = typer.prompt("Model (e.g: llama3)")
            params["host"] = typer.prompt(
                "Ollama Host URL", default="http://localhost:11434"
            )

        config_data["llm_providers"][profile_name] = params

    # Default output profile
    logger.info(
        typer.style(
            "\n--- 3. Default output profile ---", fg=typer.colors.CYAN, bold=True
        )
    )
    if typer.confirm(
        "Would you like to add a default Markdown output profile?", default=True
    ):
        filename = typer.prompt("Markdown output file name", default="db_catalog.md")
        config_data["output_profiles"]["default_markdown"] = {
            "type": "markdown",
            "output_filename": filename,
        }
