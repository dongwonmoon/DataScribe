"""
This module implements the factory pattern for creating database connectors,
LLM clients, and output writers.

The factory functions (`get_db_connector`, `get_llm_client`, `get_writer`) use
registries to look up and instantiate the correct class based on a string
identifier. This allows for an extensible architecture where new components
can be added by simply registering them.
"""

from typing import Dict, Type, Any

from schema_scribe.core.interfaces import (
    BaseConnector,
    BaseLLMClient,
    BaseWriter,
)
from schema_scribe.components.db_connectors import (
    SQLiteConnector,
    PostgresConnector,
    MariaDBConnector,
    DuckDBConnector,
    SnowflakeConnector,
)
from schema_scribe.components.llm_clients import (
    OpenAIClient,
    OllamaClient,
    GoogleGenAIClient,
)
from schema_scribe.components.writers import (
    MarkdownWriter,
    JsonWriter,
    DbtMarkdownWriter,
    ConfluenceWriter,
    PostgresCommentWriter,
    NotionWriter,
    MermaidWriter,
)
from schema_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)

# Registry for database connectors.
# This dictionary maps a string identifier (e.g., "sqlite") to a connector class.
# To add a new connector, import it and add it to this registry.
DB_CONNECTOR_REGISTRY: Dict[str, Type[BaseConnector]] = {
    "sqlite": SQLiteConnector,
    "postgres": PostgresConnector,
    "mariadb": MariaDBConnector,
    "mysql": MariaDBConnector,
    "duckdb": DuckDBConnector,
    "snowflake": SnowflakeConnector,
}

# Registry for LLM clients.
# This dictionary maps a string identifier (e.g., "openai") to a client class.
# To add a new client, import it and add it to this registry.
LLM_CLIENT_REGISTRY: Dict[str, Type[BaseLLMClient]] = {
    "openai": OpenAIClient,
    "ollama": OllamaClient,
    "google": GoogleGenAIClient,
}

# Registry for output writers.
# This dictionary maps a string identifier (e.g., "markdown") to a writer class.
WRITER_REGISTRY: Dict[str, Type[BaseWriter]] = {
    "markdown": MarkdownWriter,
    "dbt-markdown": DbtMarkdownWriter,
    "json": JsonWriter,
    "confluence": ConfluenceWriter,
    "postgres-comment": PostgresCommentWriter,
    "notion": NotionWriter,
    "mermaid": MermaidWriter,
}


def get_db_connector(type_name: str, params: Dict[str, Any]) -> BaseConnector:
    """
    Instantiates a database connector based on the provided type name.

    This factory function looks up the connector class in the DB_CONNECTOR_REGISTRY
    and returns an initialized and connected instance.

    Args:
        type_name: The type of the database connector to create (e.g., 'sqlite').
        params: A dictionary of parameters to pass to the connector's connect method.

    Returns:
        An instance of a class that implements the BaseConnector interface.

    Raises:
        ValueError: If the specified connector type is not found in the registry.
    """
    logger.info(f"Looking up database connector for type: {type_name}")
    # Look up the connector class in the registry
    connector_class = DB_CONNECTOR_REGISTRY.get(type_name)

    if not connector_class:
        logger.error(f"Unsupported database connector type: {type_name}")
        raise ValueError(f"Unsupported database connector type: {type_name}")

    logger.info(f"Instantiating {connector_class.__name__}...")
    # Create an instance of the connector
    connector = connector_class()
    # Establish the connection using the provided parameters
    connector.connect(params)
    return connector


def get_llm_client(provider_name: str, params: Dict[str, Any]) -> BaseLLMClient:
    """
    Instantiates an LLM client based on the provided provider name.

    This factory function looks up the client class in the LLM_CLIENT_REGISTRY
    and returns an initialized instance.

    Args:
        provider_name: The name of the LLM provider (e.g., 'openai').
        params: A dictionary of parameters to pass to the client's constructor.

    Returns:
        An instance of a class that implements the BaseLLMClient interface.

    Raises:
        ValueError: If the specified LLM provider is not found in the registry.
    """
    logger.info(f"Looking up LLM client for provider: {provider_name}")
    # Look up the client class in the registry
    client_class = LLM_CLIENT_REGISTRY.get(provider_name)

    if not client_class:
        logger.error(f"Unsupported LLM provider: {provider_name}")
        raise ValueError(f"Unsupported LLM provider: {provider_name}")

    logger.info(f"Instantiating {client_class.__name__}...")
    # Create an instance of the client, passing the parameters to its constructor
    return client_class(**params)


def get_writer(type_name: str) -> BaseWriter:
    """
    Instantiates an output writer based on the provided type name.

    This factory function looks up the writer class in the WRITER_REGISTRY
    and returns an uninitialized instance.

    Args:
        type_name: The type of the writer to create (e.g., 'markdown').

    Returns:
        An instance of a class that implements the BaseWriter interface.

    Raises:
        ValueError: If the specified writer type is not found in the registry.
    """
    logger.info(f"Looking up writer for type: {type_name}")
    writer_class = WRITER_REGISTRY.get(type_name)

    if not writer_class:
        logger.error(f"Unsupported writer type: {type_name}")
        raise ValueError(f"Unsupported writer type: {type_name}")

    logger.info(f"Instantiating {writer_class.__name__}...")
    return writer_class()
