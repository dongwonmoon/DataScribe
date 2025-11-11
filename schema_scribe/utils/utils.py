"""
This module contains general utility functions for the Schema Scribe application,
including configuration loading and environment variable expansion.
"""

import os
import re
import yaml
from typing import Dict, Any
from schema_scribe.core.exceptions import ConfigError


def expand_env_vars(content: str) -> str:
    """
    Expands environment variables of the form `${VAR}` in a string.

    This allows for dynamic configuration values to be pulled from the environment,
    which is useful for sensitive data like API keys or passwords.

    Args:
        content: The string content in which to expand environment variables.

    Returns:
        The string with all `${VAR}` placeholders replaced by their
        corresponding environment variable values.

    Raises:
        ConfigError: If an environment variable referenced in the string is not set.
    """
    pattern = re.compile(r"\$\{([A-Za-z0-9_]+)\}")
    matches = pattern.finditer(content)

    for match in matches:
        var_name = match.group(1)
        var_value = os.getenv(var_name)
        if var_value is None:
            raise ConfigError(
                f"Configuration error: Environment variable '{var_name}' is not set, "
                "but is referenced in the config file."
            )
        content = content.replace(match.group(0), var_value)

    return content


def load_config(config_file: str) -> Dict[str, Any]:
    """
    Loads a configuration from a YAML file and expands environment variables.

    This function first reads the raw YAML file, then expands any `${VAR}`
    placeholders using environment variables, and finally parses the resulting
    string as YAML.

    Args:
        config_file: The path to the YAML configuration file.

    Returns:
        A dictionary containing the loaded and parsed configuration.

    Raises:
        FileNotFoundError: If the configuration file is not found.
        yaml.YAMLError: If there is an error parsing the YAML file.
        ConfigError: If a referenced environment variable is not set.
    """
    with open(config_file, "r") as file:
        raw_content = file.read()

    # Expand environment variables before parsing YAML
    expanded_content = expand_env_vars(raw_content)

    # Use yaml.safe_load to parse the YAML content safely
    config = yaml.safe_load(expanded_content)
    return config
