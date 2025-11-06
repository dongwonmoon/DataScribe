import typer
import yaml

from data_scribe.core.factory import get_llm_client
from data_scribe.utils.utils import load_config
from data_scribe.utils.logger import get_logger

logger = get_logger(__name__)


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
