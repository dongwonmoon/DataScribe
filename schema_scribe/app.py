"""
This module defines the command-line interface (CLI) for the Schema Scribe application.

It uses the Typer library to create a simple and intuitive CLI for scanning databases
and dbt projects. The main commands, `db` and `dbt`, orchestrate the process of
connecting to data sources, generating documentation with an LLM, and writing the
output to various formats.
"""

import typer
import functools
import os
import yaml
from typing import Dict, Any, Optional

try:
    import uvicorn
except:
    uvicorn = None

from schema_scribe.core.db_workflow import DbWorkflow
from schema_scribe.core.dbt_workflow import DbtWorkflow
from schema_scribe.core.lineage_workflow import LineageWorkflow
from schema_scribe.core.exceptions import (
    DataScribeError,
    ConnectorError,
    ConfigError,
    LLMClientError,
    WriterError,
    CIError,
)
from schema_scribe.core.factory import (
    DB_CONNECTOR_REGISTRY,
    LLM_CLIENT_REGISTRY,
    WRITER_REGISTRY,
)
from schema_scribe.utils.logger import get_logger
from schema_scribe.server.main import app as fastapi_app

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
        except CIError as e:
            logger.error(str(e))
            raise typer.Exit(code=1)
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
            logger.error(
                f"An unknown unexpected error occurred: {e}", exc_info=True
            )
            raise typer.Exit(code=1)

    return wrapper


# --- Helper function for dynamic menus ---


def _select_from_registry(registry: dict, title: str) -> Optional[str]:
    """
    Displays a dynamic, numbered menu from a registry and validates the choice.

    Args:
        registry: A dictionary (e.g., DB_CONNECTOR_REGISTRY).
        title: The title for the menu (e.g., "Database Connection").

    Returns:
        The selected registry key (e.g., "sqlite") or None if the user skips.
    """
    logger.info(
        typer.style(f"\n--- 1. Select a {title} ---", fg=typer.colors.CYAN)
    )

    available_types = list(registry.keys())
    for i, key in enumerate(available_types, 1):
        print(f"  {i}: {key}")
    print(
        typer.style(f"  0: [Skip this section]", fg=typer.colors.BRIGHT_BLACK)
    )

    # Internal loop for input validation
    while True:
        choice_str = typer.prompt("Select an option (or 0 to skip)")

        try:
            choice_int = int(choice_str)
            if choice_int == 0:
                return None  # User chose to skip
            if 1 <= choice_int <= len(available_types):
                return available_types[
                    choice_int - 1
                ]  # User made a valid choice
        except ValueError:
            pass  # Fall through to the error message

        logger.warning(
            f"Invalid selection. Please enter a number between 0 and {len(available_types)}."
        )


def _prompt_db_params(
    db_type: str, profile_name: str, env_data: Dict[str, str]
) -> Dict[str, Any]:
    """
    Prompts the user for connection parameters for a specific database type.

    It dynamically asks for information like host, user, and password based on the
    selected `db_type`. Sensitive information (like passwords) is added to the
    `env_data` dictionary to be stored in a .env file.

    Args:
        db_type: The selected database type (e.g., "postgres", "sqlite").
        profile_name: The name for the new connection profile.
        env_data: A dictionary to store sensitive data for the .env file.

    Returns:
        A dictionary of connection parameters for the config.yaml file.
    """
    params = {"type": db_type}

    if db_type == "sqlite":
        params["path"] = typer.prompt(
            "Path to the SQLite database file", default="demo.db"
        )

    elif db_type in ["postgres", "mariadb", "mysql"]:
        params["host"] = typer.prompt("Host", default="localhost")
        params["port"] = typer.prompt(
            "Port", default=5432 if db_type == "postgres" else 3306
        )
        params["user"] = typer.prompt("User")
        pw = typer.prompt(
            f"Password (sensitive, will be stored in .env)", hide_input=True
        )
        env_key = f"{profile_name.upper()}_PASSWORD"
        params["password"] = f"$ {{{env_key}}}"
        env_data[env_key] = pw
        params["dbname"] = typer.prompt("Database (dbname)")
        if db_type == "postgres":
            params["schema"] = typer.prompt(
                "Schema (optional)", default="public"
            )

    elif db_type == "snowflake":
        params["account"] = typer.prompt(
            "Account (e.g. xy12345.ap-northeast-2.aws)"
        )
        params["user"] = typer.prompt("User")
        pw = typer.prompt(
            f"Password (sensitive, will be stored in .env)", hide_input=True
        )
        env_key = f"{profile_name.upper()}_PASSWORD"
        params["password"] = f"$ {{{env_key}}}"
        env_data[env_key] = pw
        params["warehouse"] = typer.prompt("Warehouse")
        params["database"] = typer.prompt("Database")
        params["schema"] = typer.prompt("Schema", default="PUBLIC")

    elif db_type == "duckdb":
        params["path"] = typer.prompt(
            "DB file or file pattern path (e.g. ./logs/*.parquet, s3://...)",
            default="local.db",
        )

    return params


def _prompt_llm_params(
    llm_type: str, profile_name: str, env_data: Dict[str, str]
) -> Dict[str, Any]:
    """
    Prompts the user for parameters for a specific LLM provider.

    It asks for details like model name and API keys. Sensitive information
    (API keys) is added to the `env_data` dictionary for storage in a .env file.

    Args:
        llm_type: The selected LLM provider (e.g., "openai", "google").
        profile_name: The name for the new LLM profile.
        env_data: A dictionary to store sensitive data for the .env file.

    Returns:
        A dictionary of LLM parameters for the config.yaml file.
    """
    params = {"provider": llm_type}

    if llm_type == "openai":
        params["model"] = typer.prompt("Model", default="gpt-3.5-turbo")
        if "OPENAI_API_KEY" not in env_data:
            key = typer.prompt(
                "OpenAI API Key (sensitive, will be stored in .env)",
                hide_input=True,
            )
            env_data["OPENAI_API_KEY"] = key

    elif llm_type == "google":
        params["model"] = typer.prompt("Model", default="gemini-2.5-flash")
        if "GOOGLE_API_KEY" not in env_data:
            key = typer.prompt(
                "Google API Key (sensitive, will be stored in .env)",
                hide_input=True,
            )
            env_data["GOOGLE_API_KEY"] = key

    elif llm_type == "ollama":
        params["model"] = typer.prompt("Model (e.g: llama3)")
        params["host"] = typer.prompt(
            "Ollama Host URL", default="http://localhost:11434"
        )

    return params


def _prompt_writer_params(
    writer_type: str, profile_name: str, env_data: Dict[str, str]
) -> Dict[str, Any]:
    """
    Prompts the user for parameters for a specific output writer.

    Based on the `writer_type`, it asks for relevant details like output filenames
    or Confluence connection info. Sensitive data is added to `env_data`.

    Args:
        writer_type: The selected writer type (e.g., "markdown", "confluence").
        profile_name: The name for the new output profile.
        env_data: A dictionary to store sensitive data for the .env file.

    Returns:
        A dictionary of writer parameters for the config.yaml file.
    """
    params = {"type": writer_type}

    if writer_type in ["markdown", "dbt-markdown", "json"]:
        default_name = f"catalog.{'json' if writer_type == 'json' else 'md'}"
        params["output_filename"] = typer.prompt(
            "Output file name", default=default_name
        )

    elif writer_type == "confluence":
        params["url"] = typer.prompt(
            "Confluence URL (e.g. https://your-domain.atlassian.net)"
        )
        params["space_key"] = typer.prompt("Confluence Space Key (e.g. DS)")
        params["parent_page_id"] = typer.prompt(
            "Confluence Parent Page ID (numeric)"
        )
        params["page_title_prefix"] = typer.prompt(
            "Page title prefix", default="Schema Scribe Catalog"
        )
        params["username"] = typer.prompt("Confluence Username (email)")

        if "CONFLUENCE_API_TOKEN" not in env_data:
            token = typer.prompt(
                "Confluence API Token (sensitive)", hide_input=True
            )
            env_data["CONFLUENCE_API_TOKEN"] = token
        params["api_token"] = "${CONFLUENCE_API_TOKEN}"

    elif writer_type == "notion":
        params["parent_page_id"] = typer.prompt("Notion Parent Page ID")
        params["project_name"] = typer.prompt("Project name")
        if "NOTION_API_TOKEN" not in env_data:
            token = typer.prompt(
                "Notion API Token (sensitive)", hide_input=True
            )
            env_data["NOTION_API_TOKEN"] = token
        params["api_token"] = "${NOTION_API_TOKEN}"

    return params


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
    Scans a database, generates documentation, and writes it to an output.

    This command connects to a database specified by a profile, inspects its
    schema, uses an LLM to generate descriptions for tables and columns, and
    then writes the resulting data catalog to a specified output (e.g., a
    Markdown file).
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
    db_profile: str = typer.Option(
        None,
        "--db",
        help="The database profile from config.yaml. Required for --drift mode.",
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
    interactive: bool = typer.Option(
        False,
        "--interactive",
        help="Run in interactive mode. Prompts user for each AI-generated change.",
    ),
    drift: bool = typer.Option(
        False,
        "--drift",
        help="Run in drift detection mode. Fails if docs are outdated. Requires --db.",
    ),
):
    """
    Scans a dbt project, generates a data catalog, and manages dbt documentation.

    This command can:
    - Generate a catalog file in various formats (e.g., Markdown, Confluence).
    - Directly update dbt `schema.yml` files with AI-generated content.
    - Run in a CI check mode to verify if documentation is up-to-date.
    """
    mode_flags = sum([update_yaml, check, interactive])
    if mode_flags > 1:
        logger.error(
            "Error: --update, --check, and --interactive are mutually exclusive. Please choose one."
        )
        raise typer.Exit(code=1)

    if drift and not db_profile:
        logger.error("Error: --drift mode requires --db to be specified.")
        raise typer.Exit(code=1)

    DbtWorkflow(
        dbt_project_dir=dbt_project_dir,
        db_profile=db_profile,
        llm_profile=llm_profile,
        config_path=config_path,
        output_profile=output_profile,
        update_yaml=update_yaml,
        check=check,
        interactive=interactive,
        drift=drift,
    ).run()


@app.command(name="lineage")
@handle_exceptions
def generate_lineage(
    dbt_project_dir: str = typer.Option(
        ..., "--project-dir", help="The path to the dbt project directory."
    ),
    db_profile: str = typer.Option(
        ...,
        "--db",
        help="The database profile to scan for physical Foreign Keys.",
    ),
    output_profile: str = typer.Option(
        ...,
        "--output",
        help="The output profile (type 'mermaid') to write the .md file to.",
    ),
    config_path: str = typer.Option(
        "config.yaml", "--config", help="The path to the configuration file."
    ),
):
    """
    Generates a global end-to-end lineage graph for a dbt project.

    This command creates a comprehensive, project-wide lineage graph by
    combining two sources of information:
    1.  **Physical Lineage**: Foreign key relationships from the live database.
    2.  **Logical Lineage**: `ref()` and `source()` dependencies from the dbt project.

    The resulting output is a single Mermaid.js graph, saved to a Markdown file,
    that shows the complete data flow from source tables to final models.
    """
    LineageWorkflow(
        config_path=config_path,
        db_profile=db_profile,
        dbt_project_dir=dbt_project_dir,
        output_profile=output_profile,
    ).run()


@app.command(name="init")
@handle_exceptions
def init_config():
    """
    Runs an interactive wizard to create configuration files.

    This command guides the user through a series of prompts to create the
    `config.yaml` and a `.env` file for sensitive data. It helps set up
    database connections, LLM providers, and output profiles without needing
    to write YAML manually.
    """
    if os.path.exists(CONFIG_FILE):
        overwrite = typer.confirm(
            f"'{CONFIG_FILE}' already exists. Overwrite?", default=False
        )
        if not overwrite:
            logger.info("Aborting configuration initialization.")
            raise typer.Exit(code=0)

    logger.info(f"Starting the Schema Scribe configuration wizard...")
    logger.info(
        f"This will create '{CONFIG_FILE}' and '{ENV_FILE}' in the current directory."
    )

    config_data: Dict[str, Any] = {
        "default": {},
        "db_connections": {},
        "llm_providers": {},
        "output_profiles": {},
    }
    env_data: Dict[str, str] = {}

    # 1. DB Connections
    logger.info(
        typer.style(
            "\n--- 1. Database Connections ---", fg=typer.colors.CYAN, bold=True
        )
    )
    db_type = _select_from_registry(
        DB_CONNECTOR_REGISTRY, "Database Connection"
    )
    if db_type:
        profile_name = typer.prompt(
            f"\nProfile name for '{db_type}' (e.g. dev_{db_type})"
        )
        params = _prompt_db_params(db_type, profile_name, env_data)
        config_data["db_connections"][profile_name] = params
        config_data["default"]["db"] = profile_name  # Set as default
        logger.info(f"Added DB profile: '{profile_name}' (set as default)")
    else:
        logger.info("Skipping Database Connection setup.")

    # 2. LLM Providers
    logger.info(
        typer.style(
            "\n--- 2. LLM Providers ---", fg=typer.colors.CYAN, bold=True
        )
    )
    llm_type = _select_from_registry(LLM_CLIENT_REGISTRY, "LLM Provider")
    if llm_type:
        profile_name = typer.prompt(
            f"\nProfile name for '{llm_type}' (e.g. {llm_type}_prod)"
        )
        params = _prompt_llm_params(llm_type, profile_name, env_data)
        config_data["llm_providers"][profile_name] = params
        config_data["default"]["llm"] = profile_name  # Set as default
        logger.info(f"Added LLM profile: '{profile_name}' (set as default)")
    else:
        logger.info("Skipping LLM Provider setup.")

    # 3. Output Profiles
    logger.info(
        typer.style(
            "\n--- 3. Output Profiles ---", fg=typer.colors.CYAN, bold=True
        )
    )
    writer_type = _select_from_registry(WRITER_REGISTRY, "Output Profile")
    if writer_type:
        profile_name = typer.prompt(
            f"\nProfile name for '{writer_type}' (e.g. my_{writer_type})"
        )
        params = _prompt_writer_params(writer_type, profile_name, env_data)
        config_data["output_profiles"][profile_name] = params
        logger.info(f"Added Output profile: '{profile_name}'")
    else:
        logger.info("Skipping Output Profile setup.")

    # 4. Write Files
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            yaml.dump(
                config_data,
                f,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
            )
        logger.info(
            typer.style(
                f"\nSuccessfully created '{CONFIG_FILE}'.",
                fg=typer.colors.GREEN,
                bold=True,
            )
        )

        if env_data:
            # Use 'a' (append) mode to avoid overwriting existing .env variables
            with open(ENV_FILE, "a", encoding="utf-8") as f:
                f.write("\n# Added by schema-scribe init\n")
                for key, value in env_data.items():
                    f.write(f'{key}"={value}"\n')
            logger.info(
                typer.style(
                    f"Sensitive information added to '{ENV_FILE}'.",
                    fg=typer.colors.GREEN,
                    bold=True,
                )
            )
            logger.info(
                f"Remember to keep your .env file secure and out of git."
            )

    except Exception as e:
        logger.error(
            f"Failed to write configuration file(s): {e}", exc_info=True
        )
        raise typer.Exit(code=1)


@app.command(name="serve")
@handle_exceptions
def serve_app(
    host: str = typer.Option(
        "127.0.0.1", help="The host to bind the server to."
    ),
    port: int = typer.Option(8000, help="The port to run the server on."),
):
    """
    Launches the Schema Scribe web server (FastAPI).

    This command starts a Uvicorn server to run the FastAPI application,
    providing a web-based API for the Schema Scribe workflows.

    Note: This feature requires optional dependencies. Install them with:
    `pip install \"schema-scribe[server]"`
    """
    if uvicorn is None:
        logger.error("Failed to import 'uvicorn' or 'fastapi'.")
        logger.error(
            "Please run 'pip install schema-scribe[server]' to install server dependencies."
        )
        raise typer.Exit(code=1)

    logger.info(f"Starting Schema Scribe server at http://{host}:{port}")
    logger.info("Go to http://{host}:{port}/docs for API documentation.")

    uvicorn.run(
        fastapi_app, host=host, port=port  # The FastAPI app we imported
    )
