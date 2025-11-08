"""
Unit tests for the factory module.

This test suite verifies that the factory functions in `data_scribe.core.factory`
correctly create and return instances of connectors, clients, and writers based on
the provided type names. It also tests the error-handling for unsupported types.
"""

import pytest
from unittest.mock import patch, MagicMock

from data_scribe.core.factory import (
    get_db_connector,
    get_llm_client,
    get_writer,
)
from data_scribe.components.db_connectors import SQLiteConnector
from data_scribe.components.llm_clients import OpenAIClient
from data_scribe.components.writers import MarkdownWriter
from data_scribe.core.interfaces import BaseConnector, BaseLLMClient, BaseWriter


def test_get_db_connector_supported():
    """
    Tests that get_db_connector returns a correct connector instance for a supported type.
    """
    # Mock the connect method to prevent actual database connections
    with patch.object(SQLiteConnector, "connect", return_value=None) as mock_connect:
        connector = get_db_connector("sqlite", {"path": ":memory:"})
        assert isinstance(connector, SQLiteConnector)
        assert isinstance(connector, BaseConnector)
        mock_connect.assert_called_once_with({"path": ":memory:"})


def test_get_db_connector_unsupported():
    """
    Tests that get_db_connector raises a ValueError for an unsupported type.
    """
    with pytest.raises(ValueError, match="Unsupported database connector type: athena"):
        get_db_connector("athena", {})


def test_get_llm_client_supported():
    """
    Tests that get_llm_client returns a correct client instance for a supported type.
    """
    # Mock the OpenAI client's internal initialization
    with patch("data_scribe.components.llm_clients.openai_client.OpenAI") as mock_openai:
        # Mock the settings to provide a dummy API key
        with patch(
            "data_scribe.components.llm_clients.openai_client.settings"
        ) as mock_settings:
            mock_settings.openai_api_key = "dummy_key"
            client = get_llm_client("openai", {"model": "gpt-test"})
            assert isinstance(client, OpenAIClient)
            assert isinstance(client, BaseLLMClient)
            mock_openai.assert_called_once_with(api_key="dummy_key")


def test_get_llm_client_unsupported():
    """
    Tests that get_llm_client raises a ValueError for an unsupported provider.
    """
    with pytest.raises(ValueError, match="Unsupported LLM provider: cohere"):
        get_llm_client("cohere", {})


def test_get_writer_supported():
    """
    Tests that get_writer returns a correct writer instance for a supported type.
    """
    writer = get_writer("markdown")
    assert isinstance(writer, MarkdownWriter)
    assert isinstance(writer, BaseWriter)


def test_get_writer_unsupported():
    """
    Tests that get_writer raises a ValueError for an unsupported type.
    """
    with pytest.raises(ValueError, match="Unsupported writer type: html"):
        get_writer("html")
