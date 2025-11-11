"""
This module provides the `ConfigManager` class, responsible for loading
configuration files and instantiating components.
"""

import typer
import yaml
from typing import Dict, Any, Optional

from schema_scribe.core.factory import (
    get_db_connector,
    get_llm_client,
    get_writer,
)
from schema_scribe.core.interfaces import (
    BaseConnector,
    BaseLLMClient,
    BaseWriter,
)
from schema_scribe.utils.utils import load_config
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class ConfigManager:
    """
    A central manager class for loading configuration files and creating
    and managing dependencies (components).
    """

    def __init__(self, config_path: str):
        self.config_path = config_path
        try:
            logger.info(f"Loading configuration from '{config_path}'...")
            self.config = load_config(config_path)
            logger.info("Configuration loaded successfully.")
        except FileNotFoundError:
            logger.error(f"Configuration file not found at '{config_path}'.")
            logger.error(
                "Please run 'schema-scribe init' or create the file manually."
            )
            raise typer.Exit(code=1)
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file: {e}")
            raise typer.Exit(code=1)

    def _get_profile_name(
        self, cli_profile: Optional[str], default_key: str
    ) -> str:
        """Determines the profile name to use from CLI options or default settings."""
        profile_name = cli_profile or self.config.get("default", {}).get(
            default_key
        )
        if not profile_name:
            raise typer.Exit(
                f"Missing profile. Please specify --{default_key} "
                f"or set a default in {self.config_path}"
            )
        return profile_name

    def get_db_connector(
        self, cli_profile: Optional[str]
    ) -> tuple[BaseConnector, str]:
        """Returns a DB connector instance based on the configuration file."""
        profile_name = self._get_profile_name(cli_profile, "db")
        try:
            db_params = self.config["db_connections"][profile_name]
            db_type = db_params.pop("type")
            connector = get_db_connector(db_type, db_params)
            return connector, profile_name
        except KeyError:
            logger.error(
                f"DB profile '{profile_name}' not found in config.yaml."
            )
            raise typer.Exit(code=1)

    def get_llm_client(
        self, cli_profile: Optional[str]
    ) -> tuple[BaseLLMClient, str]:
        """Returns an LLM client instance based on the configuration file."""
        profile_name = self._get_profile_name(cli_profile, "llm")
        try:
            llm_params = self.config["llm_providers"][profile_name].copy()
            llm_provider = llm_params.pop("provider")
            client = get_llm_client(llm_provider, llm_params)
            return client, profile_name
        except KeyError:
            logger.error(
                f"LLM profile '{profile_name}' not found in config.yaml."
            )
            raise typer.Exit(code=1)

    def get_writer(
        self, cli_profile: Optional[str]
    ) -> tuple[Optional[BaseWriter], Optional[str], Dict[str, Any]]:
        """Returns a Writer instance and associated parameters based on the configuration file."""
        if not cli_profile:
            return None, None, {}

        profile_name = (
            cli_profile  # Output profile name must be provided directly
        )
        try:
            writer_params = self.config["output_profiles"][profile_name].copy()
            writer_type = writer_params.pop("type")
            writer = get_writer(writer_type)
            return writer, profile_name, writer_params
        except KeyError:
            logger.error(
                f"Output profile '{profile_name}' not found in config.yaml."
            )
            raise typer.Exit(code=1)
