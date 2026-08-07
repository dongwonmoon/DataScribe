"""
Unit tests for the GoogleGenAIClient (google.genai SDK).
"""

import pytest
from unittest.mock import patch, MagicMock

from schema_scribe.components.llm_clients import GoogleGenAIClient
from schema_scribe.core.exceptions import ConfigError, LLMClientError


@patch("schema_scribe.components.llm_clients.google_client.genai")
def test_google_client_initialization(mock_genai, mocker):
    """Tests successful initialization of GoogleGenAIClient."""
    mock_settings = mocker.patch(
        "schema_scribe.components.llm_clients.google_client.settings"
    )
    mock_settings.google_api_key = "fake_api_key"

    client = GoogleGenAIClient(model="gemini-test")

    mock_genai.Client.assert_called_once_with(api_key="fake_api_key")
    assert client.model == "gemini-test"


def test_google_client_missing_api_key(mocker):
    """Tests that GoogleGenAIClient raises ConfigError if API key is missing."""
    mocker.patch(
        "schema_scribe.components.llm_clients.google_client.settings"
    ).google_api_key = None
    with pytest.raises(ConfigError, match="GOOGLE_API_KEY must be set"):
        GoogleGenAIClient(model="gemini-test")


@patch("schema_scribe.components.llm_clients.google_client.genai")
def test_google_client_get_description(mock_genai, mocker):
    """Tests the get_description method of GoogleGenAIClient."""
    mocker.patch(
        "schema_scribe.components.llm_clients.google_client.settings"
    ).google_api_key = "fake_key"

    mock_client = MagicMock()
    mock_client.models.generate_content.return_value.text = "Google response"
    mock_genai.Client.return_value = mock_client

    client = GoogleGenAIClient(model="gemini-test")
    description = client.get_description("test prompt", 150)

    assert description == "Google response"
    mock_client.models.generate_content.assert_called_once_with(
        model="gemini-test",
        contents="test prompt",
        config={"max_output_tokens": 150},
    )


@patch("schema_scribe.components.llm_clients.google_client.genai")
def test_google_client_empty_response_raises_llm_error(mock_genai, mocker):
    """A None response.text must raise LLMClientError, not AttributeError."""
    mocker.patch(
        "schema_scribe.components.llm_clients.google_client.settings"
    ).google_api_key = "fake_key"

    mock_client = MagicMock()
    mock_client.models.generate_content.return_value.text = None
    mock_genai.Client.return_value = mock_client

    client = GoogleGenAIClient(model="gemini-test")
    with pytest.raises(LLMClientError, match="returned no text"):
        client.get_description("test prompt", 50)
