"""
This module defines custom exception classes for the Data Scribe application.

Using custom exceptions allows for more specific error handling and provides
clearer, more actionable error messages to the end-user. All custom exceptions
inherit from the base `DataScribeError`.
"""


class DataScribeError(Exception):
    """Base class for all custom exceptions in the Data Scribe application."""

    pass


class ConnectorError(DataScribeError):
    """
    Raised for errors related to database connectors.

    This can occur during connection attempts (e.g., bad credentials, host not
    found) or during query execution (e.g., syntax errors, permissions issues).
    """

    pass


class LLMClientError(DataScribeError):
    """
    Raised for errors related to LLM clients.

    This typically occurs when an API call to an LLM provider (like OpenAI or
    Google) fails due to network issues, authentication problems, or invalid
    requests.
    """

    pass


class WriterError(DataScribeError):
    """
    Raised for errors related to output writers.

    This can occur during file I/O operations (e.g., permission denied) or when
    an API call for a writer (like Confluence) fails.
    """

    pass


class ConfigError(DataScribeError):
    """
    Raised for errors related to application configuration.

    This can be due to a malformed `config.yaml` file, missing required
    configuration profiles, or missing environment variables for sensitive data.
    """

    pass


class DbtParseError(DataScribeError):
    """
    Raised for errors related to parsing dbt artifacts.

    This typically occurs if the `manifest.json` file cannot be found (e.g.,
    `dbt compile` was not run) or if the file is corrupted and cannot be parsed.
    """

    pass


class CIError(DataScribeError):
    """
    Raised specifically when a CI check (--check or --drift) fails.
    This allows different interfaces (CLI vs. Server) to handle it properly.
    """

    pass
