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


@patch("schema_scribe.components.llm_clients.google_client.genai")
def test_google_client_retries_once_with_doubled_budget(mock_genai, mocker):
    """Thought-only responses retry once with a doubled output budget."""
    mocker.patch(
        "schema_scribe.components.llm_clients.google_client.settings"
    ).google_api_key = "fake_key"

    mock_client = MagicMock()
    first = MagicMock()
    first.text = None
    second = MagicMock()
    second.text = "retried answer"
    mock_client.models.generate_content.side_effect = [first, second]
    mock_genai.Client.return_value = mock_client

    client = GoogleGenAIClient(model="gemini-test")
    description = client.get_description("test prompt", 100)

    assert description == "retried answer"
    calls = mock_client.models.generate_content.call_args_list
    assert calls[0].kwargs["config"] == {"max_output_tokens": 100}
    assert calls[1].kwargs["config"] == {"max_output_tokens": 200}


@patch("schema_scribe.components.llm_clients.google_client.genai")
def test_google_client_retry_still_empty_raises(mock_genai, mocker):
    """Two empty responses in a row still raise LLMClientError."""
    mocker.patch(
        "schema_scribe.components.llm_clients.google_client.settings"
    ).google_api_key = "fake_key"

    mock_client = MagicMock()
    empty = MagicMock()
    empty.text = None
    mock_client.models.generate_content.return_value = empty
    mock_genai.Client.return_value = mock_client

    client = GoogleGenAIClient(model="gemini-test")
    with pytest.raises(LLMClientError, match="returned no text"):
        client.get_description("test prompt", 50)
    assert mock_client.models.generate_content.call_count == 2
