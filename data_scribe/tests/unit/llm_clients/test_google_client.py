"""
Unit tests for the GoogleGenAIClient.
"""
import pytest
from unittest.mock import patch, MagicMock

from data_scribe.components.llm_clients import GoogleGenAIClient
from data_scribe.core.exceptions import ConfigError


@patch("data_scribe.components.llm_clients.google_client.genai")
def test_google_client_initialization(mock_genai, mocker):
    """Tests successful initialization of GoogleGenAIClient."""
    mock_settings = mocker.patch(
        "data_scribe.components.llm_clients.google_client.settings"
    )
    mock_settings.google_api_key = "fake_api_key"

    client = GoogleGenAIClient(model="gemini-test")

    mock_genai.configure.assert_called_once_with(api_key="fake_api_key")
    mock_genai.GenerativeModel.assert_called_once_with("gemini-test")
    assert client.model is not None


def test_google_client_missing_api_key(mocker):
    """Tests that GoogleGenAIClient raises ConfigError if API key is missing."""
    mocker.patch(
        "data_scribe.components.llm_clients.google_client.settings"
    ).google_api_key = None
    with pytest.raises(ConfigError, match="GOOGLE_API_KEY must be set"):
        GoogleGenAIClient(model="gemini-test")


@patch("data_scribe.components.llm_clients.google_client.genai")
def test_google_client_get_description(mock_genai, mocker):
    """Tests the get_description method of GoogleGenAIClient."""
    mocker.patch(
        "data_scribe.components.llm_clients.google_client.settings"
    ).google_api_key = "fake_key"

    mock_model_instance = MagicMock()
    mock_model_instance.generate_content.return_value.text = "Google response"
    mock_genai.GenerativeModel.return_value = mock_model_instance

    client = GoogleGenAIClient(model="gemini-test")
    description = client.get_description("test prompt", 150)

    assert description == "Google response"
    mock_model_instance.generate_content.assert_called_once_with("test prompt")
