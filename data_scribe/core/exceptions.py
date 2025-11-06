"""
This module defines custom exception classes for the Data Scribe application.

Having custom exceptions allows for more specific error handling and clearer
differentiation between different types of runtime errors.
"""

class DataScribeError(Exception):
    """Base class for all custom exceptions in the Data Scribe application."""
    pass


class ConnectorError(DataScribeError):
    """Raised for errors related to database connectors."""
    pass


class LLMClientError(DataScribeError):
    """Raised for errors related to LLM clients."""
    pass


class WriterError(DataScribeError):
    """Raised for errors related to output writers."""
    pass


class ConfigError(DataScribeError):
    """Raised for errors related to application configuration."""
    pass
