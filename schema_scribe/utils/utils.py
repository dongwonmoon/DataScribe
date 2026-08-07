"""
This module contains low-level utility functions for the application.

Design Rationale:
The primary focus of this module is to provide robust and secure configuration
management. By centralizing functions for expanding environment variables within
configuration files, it enables:
- **Security**: Sensitive information (e.g., API keys, database passwords) can
  be kept out of version control and injected at runtime.
- **Flexibility**: Configuration can be easily adapted to different environments
  (development, staging, production) without modifying the core configuration files.
"""

import os
import re
import yaml
from typing import Dict, Any
from schema_scribe.core.exceptions import ConfigError


def quote_identifier(name: str) -> str:
    """
    Quotes a SQL identifier using ANSI double-quote rules.

    The name is wrapped in double quotes and embedded double quotes are
    doubled. NUL bytes are rejected because many drivers truncate strings
    at the first NUL, which would silently change the identifier.

    Args:
        name: The identifier to quote.

    Returns:
        The quoted identifier, e.g. `tab"le` -> `"tab""le"`.

    Raises:
        ValueError: If the name contains a NUL byte.
    """
    if "\x00" in name:
        raise ValueError("Identifier must not contain NUL bytes")
    return '"' + name.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    """
    Quotes a string literal using SQL single-quote rules.

    The value is wrapped in single quotes and embedded single quotes are
    doubled. NUL bytes are rejected because many drivers truncate strings
    at the first NUL, which would silently change the value.

    Args:
        value: The string value to quote.

    Returns:
        The quoted literal, e.g. `tab'le` -> `'tab''le'`.

    Raises:
        ValueError: If the value contains a NUL byte.
    """
    if "\x00" in value:
        raise ValueError("Literal must not contain NUL bytes")
    return "'" + value.replace("'", "''") + "'"


def expand_env_vars(content: str) -> str:
    """
    Expands environment variables of the form `${VAR}` in a string.

    Design Rationale:
    This function is crucial for implementing secure and flexible configuration.
    It allows configuration files to contain placeholders for sensitive data
    or environment-specific values. At runtime, these placeholders are replaced
    by values from the operating system's environment variables, ensuring that
    secrets are not hardcoded or committed to version control.

    Example:
        If `os.getenv("DB_PASSWORD")` is "mysecret", the input string
        `"password: ${DB_PASSWORD}"` would become `"password: mysecret"`.

    Args:
        content: The string content in which to expand environment variables.

    Returns:
        The string with all `${VAR}` placeholders replaced by their
        corresponding environment variable values.

    Raises:
        ConfigError: If an environment variable referenced in the string is not set.
    """
    pattern = re.compile(r"\$\{([A-Za-z0-9_]+)\}")

    def replacer(match):
        var_name = match.group(1)
        var_value = os.getenv(var_name)
        if var_value is None:
            raise ConfigError(
                f"Configuration error: Environment variable '{var_name}' is not set, "
                "but is referenced in the config file."
            )
        return var_value

    return pattern.sub(replacer, content)


def load_config(config_file: str) -> Dict[str, Any]:
    """
    Loads a configuration from a YAML file and expands environment variables.

    Design Rationale:
    This function implements a secure and dynamic configuration loading strategy.
    It first reads the raw YAML content as a string, then uses `expand_env_vars`
    to replace any environment variable placeholders, and finally parses the
    expanded string as YAML. This two-step process ensures that sensitive
    information is injected *before* parsing, preventing YAML parsers from
    potentially misinterpreting placeholder syntax.

    Args:
        config_file: The path to the YAML configuration file.

    Returns:
        A dictionary containing the loaded and parsed configuration.

    Raises:
        FileNotFoundError: If the configuration file is not found.
        yaml.YAMLError: If there is an error parsing the YAML file.
        ConfigError: If a referenced environment variable is not set.
    """
    with open(config_file, "r", encoding="utf-8") as file:
        raw_content = file.read()

    expanded_content = expand_env_vars(raw_content)
    config = yaml.safe_load(expanded_content)
    return config
