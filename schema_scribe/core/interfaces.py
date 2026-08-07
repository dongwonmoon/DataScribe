"""
This module defines the abstract base classes (interfaces) for the core
components of the Schema Scribe application.

These interfaces (`BaseConnector`, `BaseLLMClient`, `BaseWriter`) ensure that
different implementations adhere to a common contract. This makes the system
pluggable and easy to extend with new databases, LLM providers, or output formats.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any


class BaseLLMClient(ABC):
    """
    Abstract base class for Large Language Model (LLM) clients.

    All LLM client implementations should inherit from this class and implement the
    `get_description` method.
    """

    @abstractmethod
    def get_description(self, prompt: str, max_tokens: int) -> str:
        """
        Generates a description for a given prompt using the LLM.

        This method is typically used to generate business-friendly descriptions
        for database assets like tables and columns.

        Args:
            prompt: The prompt to send to the language model.
            max_tokens: The maximum number of tokens for the generated description.

        Returns:
            The AI-generated description as a string.
        """
        pass


class BaseConnector(ABC):
    """
    Abstract base class for database connectors.

    This interface defines a standard set of methods for interacting with
    different database systems.
    """

    @abstractmethod
    def connect(self, db_params: Dict[str, Any]):
        """
        Establishes a connection to the database.

        This method should handle authentication and session setup.

        Args:
            db_params: A dictionary of connection parameters, such as host,
                         user, password, etc. The specific parameters will
                         vary depending on the database type.
        """
        pass

    @abstractmethod
    def get_tables(self) -> List[str]:
        """
        Retrieves a list of all table names in the connected database/schema.

        Returns:
            A list of strings, where each string is a table name.
        """
        pass

    @abstractmethod
    def get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Retrieves the column details for a specific table.

        Args:
            table_name: The name of the table to inspect.

        Returns:
            A list of dictionaries, where each dictionary represents a column.
            The dictionary should conform to the following structure:
            {
                'name': str,          # The name of the column.
                'type': str,          # The data type of the column.
                'description': str,   # An existing description, if any.
                'is_nullable': bool,  # True if the column can be null.
                'is_pk': bool,        # True if the column is part of the primary key.
            }
        """
        pass

    @abstractmethod
    def get_views(self) -> List[Dict[str, str]]:
        """
        Retrieves a list of views and their definitions from the database.

        Returns:
            A list of dictionaries, where each dictionary represents a view
            and contains the following keys:
            {
                'name': str,        # The name of the view.
                'definition': str   # The SQL definition of the view.
            }
        """
        pass

    @abstractmethod
    def get_foreign_keys(self) -> List[Dict[str, str]]:
        """
        Retrieves all foreign key relationships in the database/schema.

        Returns:
            A list of dictionaries, each representing a foreign key constraint.
            Each dictionary should have the following structure:
            {
                'source_table': str,  # The table containing the foreign key.
                'source_column': str, # The column that is the foreign key.
                'target_table': str,  # The table the foreign key points to.
                'target_column': str, # The column the foreign key points to.
            }
        """
        pass

    @abstractmethod
    def get_column_profile(
        self, table_name: str, column_name: str
    ) -> Dict[str, Any]:
        """
        Retrieves profiling statistics for a specific column.

        Args:
            table_name: The name of the table.
            column_name: The name of the column to profile.

        Returns:
            A dictionary of statistics with exactly these keys:
            {
                'null_ratio': float | None,
                'distinct_count': int | None,
                'is_unique': bool | None
            }
            Each stat is None if profiling fails (rendered as "N/A" in
            prompts).
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
    collaboration platform like Confluence or Notion.
    """

    @abstractmethod
    def write(self, catalog_data: Dict[str, Any], **kwargs):
        """
        Writes the provided catalog data to the target output.

        Args:
            catalog_data: A dictionary containing the structured data catalog
                          to be written.
            **kwargs: Additional keyword arguments required by the specific
                      writer. For example:
                      - 'output_filename' for file-based writers.
                      - 'space_key', 'parent_page_id' for Confluence.
                      - 'parent_page_id' for Notion.
        """
        pass
