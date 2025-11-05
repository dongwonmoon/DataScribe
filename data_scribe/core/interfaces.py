"""
This module defines the abstract base classes (interfaces) for the core components of Data Scribe.

These interfaces (`BaseLLMClient` and `BaseConnector`) ensure that different implementations
of LLM clients and database connectors adhere to a common contract. This makes the system
pluggable and easy to extend.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any


class BaseLLMClient(ABC):
    """Abstract base class for LLM clients.

    All LLM client implementations should inherit from this class and implement the
    `get_description` method.
    """

    @abstractmethod
    def get_description(self, prompt: str, max_tokens: int) -> str:
        """Generates a description for a given prompt.

        Args:
            prompt: The prompt to send to the language model.
            max_tokens: The maximum number of tokens to generate.

        Returns:
            The AI-generated description as a string.
        """
        pass


class BaseConnector(ABC):
    """Abstract base class for database connectors.

    All database connector implementations should inherit from this class and implement
    the `connect`, `get_tables`, `get_columns`, and `close` methods.
    """

    @abstractmethod
    def connect(self, db_params: Dict[str, Any]):
        """
        Establishes a connection to the database.

        Args:
            db_params: A dictionary of connection parameters, such as host,
                         user, password, etc. The specific parameters will
                         vary depending on the database type.
        """
        pass

    @abstractmethod
    def get_tables(self) -> List[str]:
        """
        Retrieves a list of all table names in the connected database.

        Returns:
            A list of strings, where each string is a table name.
        """
        pass

    @abstractmethod
    def get_columns(self, table_name: str) -> List[Dict[str, str]]:
        """
        Retrieves the column details for a specific table.

        Args:
            table_name: The name of the table to inspect.

        Returns:
            A list of dictionaries, where each dictionary represents a column
            and contains keys like 'name' and 'type'.
        """
        pass

    @abstractmethod
    def get_views(self) -> List[Dict[str, str]]:
        """
        Retrieves a list of views and their definitions from the database.

        Returns:
            A list of dictionaries, where each dictionary represents a view
            and contains keys like 'name' and 'definition'.
        """
        pass

    @abstractmethod
    def get_foreign_keys(self) -> List[Dict[str, str]]:
        """
        Retrieves all foreign key relationships in the database.

        Returns:
            A list of dictionaries, each representing a foreign key constraint.
            The dictionary structure may vary but should include information
            like the source and target tables/columns.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Closes the active database connection and releases any resources.
        """
        pass


class BaseWriter(ABC):
    """
    Abstract base class for content writers.

    This interface defines the contract for classes that write the generated
    data catalog to a specific output format, such as a file or a
    collaboration platform.
    """

    @abstractmethod
    def write(self, catalog_data: Dict[str, Any], **kwargs):
        """
        Writes the provided catalog data to the target output.

        Args:
            catalog_data: A dictionary containing the structured data catalog
                          to be written.
            **kwargs: Additional keyword arguments that may be required by a
                      specific writer implementation, such as 'filename' or
                      'api_token'.
        """
        pass
