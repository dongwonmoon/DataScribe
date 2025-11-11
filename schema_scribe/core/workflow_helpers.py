"""
This module provides helper functions shared across different workflows.

It includes functionality for loading the main application configuration and for
initializing the LLM client based on that configuration.
"""

import typer
import yaml
from typing import Dict, Any

from schema_scribe.core.factory import get_llm_client
from schema_scribe.core.interfaces import BaseLLMClient
from schema_scribe.utils.utils import load_config
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


def load_config_from_path(config_path: str) -> Dict[str, Any]:
    """
    Loads the YAML configuration file from the given path.

    Args:
        config_path: The path to the `config.yaml` file.

    Returns:
        A dictionary containing the loaded and parsed configuration.

    Raises:
        typer.Exit: If the file is not found or if there is an error parsing it.
    """
    try:
        logger.info(f"Loading configuration from '{config_path}'...")
        config = load_config(config_path)
        logger.info("Configuration loaded successfully.")
        return config
    except FileNotFoundError:
        logger.error(f"Configuration file not found at '{config_path}'.")
        logger.error(
            "Please run 'schema-scribe init' or create the file manually."
        )
        raise typer.Exit(code=1)
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file: {e}")
        raise typer.Exit(code=1)


def init_llm(config: Dict[str, Any], llm_profile_name: str) -> BaseLLMClient:
    """
    Initializes the LLM client based on the specified profile.

    Args:
        config: The application configuration dictionary.
        llm_profile_name: The name of the LLM profile to use from the config
                          (e.g., 'openai_default').

    Returns:
        An initialized instance of a class that implements `BaseLLMClient`.

    Raises:
        typer.Exit: If the specified LLM profile or its configuration is
                    missing or invalid.
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
        logger.error(
            f"Missing LLM configuration key: {e}. Check that the '{llm_profile_name}' "
            "profile is correctly defined in 'llm_providers'."
        )
        raise typer.Exit(code=1)
    except Exception as e:
        logger.error(f"Failed to initialize LLM client: {e}", exc_info=True)
        raise typer.Exit(code=1)
