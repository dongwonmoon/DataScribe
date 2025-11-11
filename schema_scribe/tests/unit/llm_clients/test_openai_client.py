"""
Unit tests for the OpenAIClient.
"""

import pytest
from unittest.mock import patch, MagicMock

from schema_scribe.components.llm_clients import OpenAIClient
from schema_scribe.core.exceptions import ConfigError, LLMClientError


def test_openai_client_initialization(mocker):
    """Tests successful initialization of OpenAIClient."""
    mock_settings = mocker.patch(
        "schema_scribe.components.llm_clients.openai_client.settings"
    )
    mock_settings.openai_api_key = "fake_api_key"
    mock_openai_constructor = mocker.patch(
        "schema_scribe.components.llm_clients.openai_client.OpenAI"
    )

    client = OpenAIClient(model="gpt-test")
    assert client.model == "gpt-test"
    mock_openai_constructor.assert_called_once_with(api_key="fake_api_key")


def test_openai_client_missing_api_key(mocker):
    """Tests that OpenAIClient raises ConfigError if API key is missing."""
    mock_settings = mocker.patch(
        "schema_scribe.components.llm_clients.openai_client.settings"
    )
    mock_settings.openai_api_key = None
    with pytest.raises(
        ConfigError, match="OPENAI_API_KEY environment variable not set"
    ):
        OpenAIClient()


def test_openai_client_get_description(mocker):
    """Tests the get_description method of OpenAIClient."""
    mock_settings = mocker.patch(
        "schema_scribe.components.llm_clients.openai_client.settings"
    )
    mock_settings.openai_api_key = "fake_api_key"

    mock_openai_instance = MagicMock()
    mock_openai_instance.chat.completions.create.return_value.choices[
        0
    ].message.content = "  OpenAI response  "
    mocker.patch(
        "schema_scribe.components.llm_clients.openai_client.OpenAI",
        return_value=mock_openai_instance,
    )

    client = OpenAIClient(model="gpt-test")
    description = client.get_description("test prompt", 100)

    assert description == "OpenAI response"
    mock_openai_instance.chat.completions.create.assert_called_once_with(
        model="gpt-test",
        messages=[{"role": "system", "content": "test prompt"}],
        max_tokens=100,
    )


def test_llm_client_api_error(mocker):
    """Tests that a generic LLMClientError is raised on API failure."""
    mocker.patch(
        "schema_scribe.components.llm_clients.openai_client.settings"
    ).openai_api_key = "fake_key"

    # Make the create call itself raise the error
    mock_completions = MagicMock()
    mock_completions.create.side_effect = Exception("API is down")

    mock_openai_instance = MagicMock()
    mock_openai_instance.chat.completions = mock_completions

    mocker.patch(
        "schema_scribe.components.llm_clients.openai_client.OpenAI",
        return_value=mock_openai_instance,
    )

    client = OpenAIClient(model="gpt-test")

    with pytest.raises(
        LLMClientError, match="OpenAI API call failed: API is down"
    ):
        client.get_description("prompt", 100)
